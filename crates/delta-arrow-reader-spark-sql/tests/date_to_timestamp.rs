//! CAST, TRY_CAST and implicit conversions from DATE to TIMESTAMP read midnight in the session
//! time zone like Spark, moving a midnight skipped by a DST change forward and taking the earlier
//! instant of a repeated one.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Fields, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
    TimestampNanosecondType,
};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
use spark_inputs::decode_recorded_batch;

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, validate_batch};

fn date_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/date-to-timestamp/date-to-timestamp-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 430);
    Ok(records)
}

/// The Arrow type family DAR uses for a recorded Spark type.
fn spark_family(spark_type: &Value) -> TestResult<String> {
    let spark_type = spark_type.as_str().ok_or("expected a primitive type")?;
    Ok(match spark_type {
        "integer" => "int32".to_owned(),
        "long" => "int64".to_owned(),
        "boolean" => "boolean".to_owned(),
        "date" => "date32".to_owned(),
        "string" => "string".to_owned(),
        "timestamp" => "timestamp".to_owned(),
        "timestamp_ntz" => "timestamp_ntz".to_owned(),
        // A retained expectation for DAR's native nanosecond coercion (#1122).
        "dar_timestamp_ns" => "unexpected Timestamp(ns)".to_owned(),
        other => return Err(format!("unexpected Spark type {other}").into()),
    })
}

/// A TIMESTAMP result must be annotated with the session zone, as Spark's Arrow output is.
fn dar_family(data_type: &DataType, timezone: &str) -> String {
    match data_type {
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Boolean => "boolean".to_owned(),
        DataType::Date32 => "date32".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == timezone => {
            "timestamp".to_owned()
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz".to_owned(),
        other => format!("unexpected {other}"),
    }
}

fn spark_fields(schema: &Value) -> TestResult<Value> {
    schema["fields"]
        .as_array()
        .ok_or("missing recorded fields")?
        .iter()
        .map(|field| {
            Ok(
                json!({"name": field["name"], "type": spark_family(&field["type"])?,
                "nullable": field["nullable"], "metadata": field["metadata"]}),
            )
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

fn dar_fields(fields: &Fields, names: Option<&[String]>, timezone: &str) -> Value {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = names.map_or(field.name().as_str(), |names| names[index].as_str());
            json!({"name": name, "type": dar_family(field.data_type(), timezone),
                "nullable": field.is_nullable(), "metadata": field.metadata()})
        })
        .collect()
}

/// Encode a value as the capture does: TIMESTAMP as UTC-epoch and TIMESTAMP_NTZ as
/// wall-clock microseconds, DOUBLE as its IEEE 754 bits and DECIMAL as plain text.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Boolean => json!(column.as_boolean().value(row).to_string()),
        // DAR's native coercions produce nanoseconds; compare them as Spark's microseconds.
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = column.as_primitive::<TimestampNanosecondType>().value(row);
            if nanos % 1_000 != 0 {
                return Err(format!("{nanos} nanoseconds are not whole microseconds").into());
            }
            json!((nanos / 1_000).to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Utf8 => json!(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(column.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(column.as_string_view().value(row)),
        other => return Err(format!("unexpected result type {other}").into()),
    })
}

fn options(case: &Value) -> TestResult<SparkSqlOptions> {
    Ok(SparkSqlOptions {
        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        session_timezone: case["timezone"].as_str().ok_or("missing time zone")?.into(),
        ..SparkSqlOptions::default()
    })
}

/// Observe analyzed (public names), optimized and executed fields, typed rows and the
/// failing phase in the record's session time zone.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    // Spark records an analysis failure as the `sql` stage.
    let mut phase = "sql";
    let result: TestResult<Vec<Vec<Value>>> = async {
        let timezone = case["timezone"].as_str().ok_or("missing time zone")?;
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &options(case)?,
        )
        .await?;
        observation["analyzed"] = dar_fields(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
            timezone,
        );
        phase = "optimized";
        let optimized = optimize(session, &query.logical_plan)?;
        observation["optimized"] = dar_fields(
            optimized.schema().fields(),
            Some(&query.column_names),
            timezone,
        );
        let physical = query.create_physical_plan(session).await?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = dar_fields(schema.fields(), None, timezone);
        let mut rows = Vec::new();
        for batch in batches {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?,
                );
            }
        }
        rows.sort_by_key(|row| json!(row).to_string());
        Ok(rows)
    }
    .await;
    match result {
        Ok(rows) => observation["rows"] = json!(rows),
        Err(error) => {
            observation["error_phase"] = json!(phase);
            observation["error"] = json!(error.to_string());
        }
    }
    observation
}

fn optimize(session: &SessionContext, plan: &LogicalPlan) -> TestResult<LogicalPlan> {
    let mut state = session.state();
    let extensions = &mut state.config_mut().options_mut().extensions;
    extensions.insert(SparkSqlExpressionSemantics::default());
    extensions.insert(PreserveAggregateStatisticsSchema::default());
    Ok(state.optimize(plan)?)
}

/// The text DAR's error must contain for a recorded Spark failure: a retained DAR message,
/// Spark's error condition, or Java's `ArithmeticException` message for an overflow.
fn dar_message(case: &Value) -> Option<String> {
    if let Some(message) = case["expected"]["dar_message"].as_str() {
        return Some(message.to_owned());
    }
    match case["expected"]["condition"].as_str() {
        Some(condition) => Some(format!("[{condition}]")),
        None if case["expected"]["exception"] == "java.lang.ArithmeticException" => {
            Some("long overflow".to_owned())
        }
        None => None,
    }
}

/// The recorded stages and outcome that differ from the observation. A failure must occur
/// at Spark's stage with DAR's message for it.
fn differing_parts(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    let mut differing = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        if let Some(expected) = case["schema_stages"].get(stage)
            && observation[stage] != spark_fields(expected)?
        {
            differing.push(stage);
        }
    }
    let outcome_matches = match case["expected"]["status"].as_str() {
        Some("ok") => observation["rows"] == case["expected"]["rows"],
        Some("error") => {
            observation["error_phase"] == case["spark_error_phase"]
                && dar_message(case)
                    .zip(observation["error"].as_str())
                    .is_some_and(|(message, error)| error.contains(&message))
        }
        _ => false,
    };
    if !outcome_matches {
        differing.push("outcome");
    }
    Ok(differing)
}

/// Differences outside this conversion, each named for DAR's current behavior and owned by
/// an issue. A record can have several.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1090: GREATEST and LEAST are always nullable, where Spark declares them NOT NULL when
    /// any argument is NOT NULL.
    NullableExtremum,
    /// #1122: arrays, DATE_TRUNC and IN subqueries coerce a DATE to a TIMESTAMP in nanoseconds
    /// without a zone, which fails for years 1 and 9999, and VALUES gives its UTC midnight that
    /// way.
    NanosecondDateCoercion,
    /// #1122: an IN list of TIMESTAMP values fails with Arrow's cast of a DATE in a zone that
    /// skips a midnight.
    ArrowCastInList,
    /// #1122: HOUR reads a DATE as its midnight, not its first local instant.
    MidnightDateFields,
}

/// DAR's failure for a DATE of year 1 or 9999 coerced to a nanosecond TIMESTAMP; which one
/// fails first depends on the batch.
const NANOSECOND_RANGE: &str = "to Timestamp(ns): converted value exceeds the representable";
/// 2024-03-10 00:00 UTC, the midnight DAR's VALUES gives `DATE '2024-03-10'`.
const VALUES_DATE_MIDNIGHT: &str = "1710028800000000";

/// Set `key` of the result field `r` in each recorded schema stage.
fn set_result_field(case: &mut Value, key: &str, value: &Value) {
    for stage in ["analyzed", "optimized", "executed"] {
        if let Some(fields) = case["schema_stages"][stage]["fields"].as_array_mut() {
            for field in fields.iter_mut().filter(|field| field["name"] == "r") {
                field[key] = value.clone();
            }
        }
    }
}

/// The record failing while collecting rows with `message`, keeping its earlier stages.
fn fail_while_collecting(case: &mut Value, message: &str) {
    case["expected"] = json!({"status": "error", "dar_message": message});
    case["spark_error_phase"] = json!("collect");
    if let Some(stages) = case["schema_stages"].as_object_mut() {
        stages.remove("executed");
    }
}

/// DAR's current result for a record: Spark's record with each retained part replaced by
/// DAR's result, and the differences that changed it.
fn current_expectation(case: &Value) -> TestResult<(Value, Vec<RetainedDifference>)> {
    let mut current = case.clone();
    let mut retained = Vec::new();
    let id = case["id"].as_str().ok_or("missing id")?;
    if matches!(
        id,
        "column_greatest" | "column_least" | "column_greatest_ntz" | "column_greatest_mixed"
    ) {
        let mut changed = false;
        for stage in ["analyzed", "optimized", "executed"] {
            if let Some(fields) = current["schema_stages"][stage]["fields"].as_array_mut() {
                for field in fields.iter_mut().filter(|field| field["name"] == "r") {
                    if field["nullable"] == false {
                        field["nullable"] = json!(true);
                        changed = true;
                    }
                }
            }
        }
        if changed {
            retained.push(RetainedDifference::NullableExtremum);
        }
    }
    let zone = case["timezone"].as_str().ok_or("missing time zone")?;
    let skips_midnight = matches!(
        zone,
        "America/Havana" | "America/Santiago" | "America/Toronto"
    );
    match id {
        "column_array" => {
            set_result_field(&mut current, "type", &json!("dar_timestamp_ns"));
            fail_while_collecting(&mut current, NANOSECOND_RANGE);
            retained.push(RetainedDifference::NanosecondDateCoercion);
        }
        "column_date_trunc_hour" => {
            set_result_field(&mut current, "type", &json!("timestamp_ntz"));
            fail_while_collecting(&mut current, NANOSECOND_RANGE);
            retained.push(RetainedDifference::NanosecondDateCoercion);
        }
        "in_subquery" => {
            fail_while_collecting(&mut current, NANOSECOND_RANGE);
            retained.push(RetainedDifference::NanosecondDateCoercion);
        }
        "values" => {
            // The TIMESTAMP literal, 01:30 local, follows the day's start in every zone.
            let rows = current["expected"]["rows"]
                .as_array()
                .ok_or("missing rows")?;
            let literal = rows
                .iter()
                .map(|row| row[0].clone())
                .max_by_key(|value| value.as_str().and_then(|text| text.parse::<i64>().ok()));
            let mut rows = vec![json!([VALUES_DATE_MIDNIGHT]), json!([literal])];
            rows.sort_by_key(|row| row.to_string());
            current["expected"]["rows"] = json!(rows);
            set_result_field(&mut current, "type", &json!("dar_timestamp_ns"));
            retained.push(RetainedDifference::NanosecondDateCoercion);
        }
        "column_in_list" if skips_midnight => {
            fail_while_collecting(&mut current, "Cannot cast timezone to different timezone");
            retained.push(RetainedDifference::ArrowCastInList);
        }
        "column_hour" if skips_midnight => {
            let mut changed = false;
            for row in current["expected"]["rows"]
                .as_array_mut()
                .ok_or("missing rows")?
            {
                let Some(value) = row[1].as_str() else {
                    continue;
                };
                let midnight = match value.rsplit_once(' ') {
                    Some((day, _)) => format!("{day} 00:00"),
                    None => "0".to_owned(),
                };
                if midnight != value {
                    row[1] = json!(midnight);
                    changed = true;
                }
            }
            if changed {
                retained.push(RetainedDifference::MidnightDateFields);
            }
        }
        _ => {}
    }
    Ok((current, retained))
}

fn session_context(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(1),
                ),
        )
        .build(),
    )
}

fn register(
    session: &SessionContext,
    name: &str,
    batch: &RecordBatch,
    batch_size: usize,
) -> TestResult {
    let mut partitions = vec![Vec::new(), Vec::new()];
    for (chunk, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
        partitions[chunk % 2].push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
    }
    session.register_table(
        name,
        Arc::new(MemTable::try_new(batch.schema(), partitions)?),
    )?;
    Ok(())
}

fn register_inputs(session: &SessionContext, metadata: &Value, batch_size: usize) -> TestResult {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let name = input["name"].as_str().ok_or("missing table name")?;
        register(session, name, &decode_recorded_batch(input)?, batch_size)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dates_convert_to_timestamps_like_spark() -> TestResult {
    let records = date_references()?;
    let mut retained = BTreeMap::new();
    let mut expectations = Vec::new();
    for case in &records[1..] {
        let (expectation, differences) = current_expectation(case)?;
        for difference in differences {
            *retained.entry(difference).or_insert(0) += 1;
        }
        expectations.push(expectation);
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &records[0], batch_size)?;
        for (case, expectation) in records[1..].iter().zip(&expectations) {
            let observation = observe(&session, case).await;
            if case["id"] == "column_date_format" {
                println!(
                    "date_format_day_start_observation={}",
                    json!({"id":case["id"],"timezone":case["timezone"],
                        "ansi":case["ansi"],"batch":batch_size,"actual":observation})
                );
            }
            let differing = differing_parts(expectation, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                    "batch": batch_size, "differing": differing,
                    "expected": expectation["expected"],
                    "schema_stages": expectation["schema_stages"],
                    "observed": observation,
                }));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(
        retained,
        BTreeMap::from([
            (RetainedDifference::NullableExtremum, 40),
            (RetainedDifference::NanosecondDateCoercion, 40),
            (RetainedDifference::ArrowCastInList, 6),
            (RetainedDifference::MidnightDateFields, 4),
        ])
    );
    Ok(())
}
