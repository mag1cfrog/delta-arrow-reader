//! TIMESTAMP day, week, month, quarter and year arithmetic works on local date-times in the
//! session time zone like Spark, across DST changes and local mean time, and subtracting
//! TIMESTAMP values gives the difference of their local date-times.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Fields, Int32Type, Int64Type, TimeUnit,
    TimestampMicrosecondType,
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

fn arithmetic_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/local-day-arithmetic/local-day-arithmetic-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 768);
    Ok(records)
}

/// Differences outside local TIMESTAMP arithmetic, each owned by an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1052: in Los Angeles, row 16, 9999-12-31 23:59:59.999999 UTC, plus a quarter is
    /// 10000-03-31 15:59:59.999999 local time, which Java's recurring rules read in PDT and
    /// the date library, whose Los Angeles rules end in 2099, in PST, an hour later.
    RecurringDstAfter2099,
    /// #952: DATE plus an HOUR or DAY TO HOUR interval stays a DATE, the DATE plus the
    /// interval's whole days, where Spark gives a TIMESTAMP.
    DateHourArithmetic,
    /// #1131: MAKE_INTERVAL is nullable where Spark declares it NOT NULL under ANSI, so a
    /// TIMESTAMP minus it is too. These records fail during optimization, after analysis.
    NullableMakeInterval,
}

/// Records that add a quarter to the `instants` column.
const QUARTER_RECORDS: [&str; 3] = [
    "column_timestampadd_quarter",
    "column_timestampadd_quarter_column",
    "column_dateadd_quarter",
];
/// DATE '2024-03-10' plus INTERVAL '1' HOUR and DATE '2024-03-09' plus INTERVAL '1 02' DAY
/// TO HOUR.
const DATE_HOUR_RECORDS: [&str; 2] = ["literal_date_plus_hour", "literal_date_plus_day_to_hour"];
/// TIMESTAMP minus MAKE_INTERVAL records, whose negation overflows.
const MAKE_INTERVAL_RECORDS: [&str; 2] = [
    "literal_minus_smallest_calendar_months",
    "literal_minus_smallest_calendar_days",
];
/// 2024-03-10, the DATE DAR gives for both DATE plus hour records.
const MARCH_10_2024: &str = "19792";
const MICROS_PER_HOUR: i64 = 3_600_000_000;

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let id = case["id"].as_str()?;
    if QUARTER_RECORDS.contains(&id) && case["timezone"] == "America/Los_Angeles" {
        Some(RetainedDifference::RecurringDstAfter2099)
    } else if DATE_HOUR_RECORDS.contains(&id) {
        Some(RetainedDifference::DateHourArithmetic)
    } else if MAKE_INTERVAL_RECORDS.contains(&id) && case["ansi"] == true {
        Some(RetainedDifference::NullableMakeInterval)
    } else {
        None
    }
}

/// Replace only the retained part of Spark's expectation with DAR's current result.
fn current_expectation(case: &Value, difference: RetainedDifference) -> TestResult<Value> {
    let mut current = case.clone();
    let expected_status = match difference {
        RetainedDifference::NullableMakeInterval => "error",
        _ => "ok",
    };
    if case["expected"]["status"] != expected_status {
        return Err(format!("expected a Spark {expected_status}: {case}").into());
    }
    match difference {
        RetainedDifference::RecurringDstAfter2099 => {
            let mut changed = 0;
            for row in current["expected"]["rows"]
                .as_array_mut()
                .ok_or("missing rows")?
            {
                if row[0] == "16" {
                    let spark = row[1].as_str().ok_or("missing result")?.parse::<i64>()?;
                    row[1] = json!((spark + MICROS_PER_HOUR).to_string());
                    changed += 1;
                }
            }
            if changed != 1 {
                return Err(format!("expected row 16 once: {case}").into());
            }
        }
        RetainedDifference::DateHourArithmetic => {
            for stage in ["analyzed", "optimized", "executed"] {
                let field = &mut current["schema_stages"][stage]["fields"][0];
                if field["type"] != "timestamp" || field["nullable"] != false {
                    return Err(format!("expected Spark's required TIMESTAMP: {case}").into());
                }
                field["type"] = json!("date");
            }
            current["expected"]["rows"] = json!([[MARCH_10_2024]]);
        }
        RetainedDifference::NullableMakeInterval => {
            let stages = current["schema_stages"]
                .as_object_mut()
                .ok_or("missing schema stages")?;
            if stages.len() != 1 || !stages.contains_key("analyzed") {
                return Err(format!("expected only an analyzed schema: {case}").into());
            }
            let field = &mut stages["analyzed"]["fields"][0];
            if field["nullable"] != false {
                return Err(format!("expected Spark's required TIMESTAMP: {case}").into());
            }
            field["nullable"] = json!(true);
        }
    }
    Ok(current)
}

/// The Arrow type family DAR uses for a recorded Spark type.
fn spark_family(spark_type: &Value) -> TestResult<Value> {
    if spark_type["type"] == "array" {
        return Ok(json!({"array": spark_family(&spark_type["elementType"])?,
            "containsNull": spark_type["containsNull"]}));
    }
    let spark_type = spark_type
        .as_str()
        .ok_or("expected a primitive or ARRAY type")?;
    Ok(json!(match spark_type {
        "integer" => "int32",
        "long" => "int64",
        "date" => "date32",
        "string" => "string",
        "timestamp" => "timestamp",
        "timestamp_ntz" => "timestamp_ntz",
        "interval day to second" => "interval day to second",
        other => return Err(format!("unexpected Spark type {other}").into()),
    }))
}

/// A TIMESTAMP result must be annotated with the session zone, as Spark's Arrow output is.
fn dar_family(data_type: &DataType, timezone: &str) -> Value {
    json!(match data_type {
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Date32 => "date32".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == timezone => {
            "timestamp".to_owned()
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz".to_owned(),
        DataType::Duration(TimeUnit::Microsecond) => "interval day to second".to_owned(),
        DataType::List(item) => {
            return json!({"array": dar_family(item.data_type(), timezone),
                "containsNull": item.is_nullable()});
        }
        other => format!("unexpected {other}"),
    })
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
/// wall-clock microseconds, DAY TO SECOND as microseconds and ARRAY as a list.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Duration(TimeUnit::Microsecond) => json!(
            column
                .as_primitive::<DurationMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Utf8 => json!(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(column.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(column.as_string_view().value(row)),
        DataType::List(_) => {
            let items = column.as_list::<i32>().value(row);
            json!(
                (0..items.len())
                    .map(|item| encode_value(items.as_ref(), item))
                    .collect::<TestResult<Vec<_>>>()?
            )
        }
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

/// The text DAR's error must contain for a recorded Spark failure: Spark's error condition,
/// or the recorded message of Java's `ArithmeticException` for an overflow.
fn dar_message(case: &Value) -> Option<String> {
    match case["expected"]["condition"].as_str() {
        Some(condition) => Some(format!("[{condition}]")),
        None if case["expected"]["exception"] == "java.lang.ArithmeticException" => {
            case["expected"]["java_message"].as_str().map(str::to_owned)
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
async fn timestamp_days_add_in_local_time_like_spark() -> TestResult {
    let records = arithmetic_references()?;
    let mut retained = BTreeMap::new();
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
    }
    assert_eq!(
        retained,
        BTreeMap::from([
            (RetainedDifference::RecurringDstAfter2099, 6),
            (RetainedDifference::DateHourArithmetic, 12),
            (RetainedDifference::NullableMakeInterval, 6),
        ])
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session_context(batch_size);
        register_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let expectation = match retained_difference(case) {
                Some(difference) => current_expectation(case, difference)?,
                None => case.clone(),
            };
            let observation = observe(&session, case).await;
            let differing = differing_parts(&expectation, &observation)?;
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
    Ok(())
}
