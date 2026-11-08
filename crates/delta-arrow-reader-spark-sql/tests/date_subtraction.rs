//! Subtracting two DATE values, with `-` or TRY_SUBTRACT, gives the DAY interval between
//! them in microseconds, like Spark, and the difference's consumers see a day-time interval.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Fields, Int32Type, Int64Type, TimeUnit,
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

fn subtraction_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/date-subtraction/date-subtraction-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 192);
    Ok(records)
}

/// Differences in consumers of the difference, each owned by an issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #953: a CAST to STRING formats the DAY interval as DAY TO SECOND text, since Arrow's
    /// duration carries no qualifier.
    StringQualifier,
    /// #1152: a CAST to BIGINT counts the seconds, where Spark counts the DAY interval's days.
    IntegerUnit,
    /// #952: a DATE plus a difference spanning years 1 to 9999 fails in Arrow's cast to a
    /// nanosecond calendar interval while collecting rows, where Spark gives the DATE.
    DateIntervalRange,
}

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    match case["id"].as_str()? {
        "column_cast_string" => Some(RetainedDifference::StringQualifier),
        "column_cast_bigint" => Some(RetainedDifference::IntegerUnit),
        "column_plus_date_far" => Some(RetainedDifference::DateIntervalRange),
        _ => None,
    }
}

/// Map each `[k, value]` row's value, keeping NULL.
fn map_values(case: &Value, map: impl Fn(&str) -> TestResult<String>) -> TestResult<Vec<Value>> {
    let mut rows = case["expected"]["rows"]
        .as_array()
        .ok_or("expected Spark rows")?
        .iter()
        .map(|row| {
            Ok(match row[1].as_str() {
                Some(value) => json!([row[0], map(value)?]),
                None => json!([row[0], null]),
            })
        })
        .collect::<TestResult<Vec<_>>>()?;
    rows.sort_by_key(|row| row.to_string());
    Ok(rows)
}

/// Replace only the retained part of Spark's expectation with DAR's current result.
fn current_expectation(case: &Value, difference: RetainedDifference) -> TestResult<Value> {
    let mut current = case.clone();
    match difference {
        RetainedDifference::StringQualifier => {
            let rows = map_values(case, |text| {
                let days = text
                    .strip_prefix("INTERVAL '")
                    .and_then(|text| text.strip_suffix("' DAY"))
                    .ok_or_else(|| format!("expected Spark's DAY text: {text}"))?;
                Ok(format!("INTERVAL '{days} 00:00:00' DAY TO SECOND"))
            })?;
            current["expected"]["rows"] = json!(rows);
        }
        RetainedDifference::IntegerUnit => {
            let rows = map_values(case, |days| Ok((days.parse::<i64>()? * 86_400).to_string()))?;
            current["expected"]["rows"] = json!(rows);
        }
        RetainedDifference::DateIntervalRange => {
            // Row 2 is 9999-12-31 minus 0001-01-01, added back to 0001-01-01.
            if case["expected"]["rows"] != json!([["2", "2932896"]]) {
                return Err(format!("expected Spark's DATE 9999-12-31: {case}").into());
            }
            current["expected"] = json!({"status": "error", "dar_message":
                "Cannot cast to Interval(MonthDayNano). Overflowing on 315537811200000000"});
            current["spark_error_phase"] = json!("collect");
            current["schema_stages"]
                .as_object_mut()
                .ok_or("missing schema stages")?
                .remove("executed");
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
        "boolean" => "boolean",
        "integer" => "int32",
        "long" => "int64",
        "date" => "date32",
        "string" => "string",
        // Arrow's microsecond durations carry no qualifier, so every day-time qualifier is one
        // family.
        other if other.starts_with("interval day") || other.starts_with("interval hour") => {
            "day_time_interval"
        }
        other => return Err(format!("unexpected Spark type {other}").into()),
    }))
}

fn dar_family(data_type: &DataType) -> Value {
    json!(match data_type {
        DataType::Boolean => "boolean".to_owned(),
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Date32 => "date32".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "string".to_owned(),
        DataType::Duration(TimeUnit::Microsecond) => "day_time_interval".to_owned(),
        DataType::List(item) => {
            return json!({"array": dar_family(item.data_type()),
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

fn dar_fields(fields: &Fields, names: Option<&[String]>) -> Value {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = names.map_or(field.name().as_str(), |names| names[index].as_str());
            json!({"name": name, "type": dar_family(field.data_type()),
                "nullable": field.is_nullable(), "metadata": field.metadata()})
        })
        .collect()
}

/// Encode a value as the capture does: DATE as days since the epoch, a day-time interval as
/// microseconds and ARRAY as a list.
fn encode_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Boolean => json!(column.as_boolean().value(row)),
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
/// failing phase.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    // Spark records an analysis failure as the `sql` stage.
    let mut phase = "sql";
    let result: TestResult<Vec<Vec<Value>>> = async {
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &options(case)?,
        )
        .await?;
        observation["analyzed"] = dar_fields(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
        );
        phase = "optimized";
        let optimized = optimize(session, &query.logical_plan)?;
        observation["optimized"] =
            dar_fields(optimized.schema().fields(), Some(&query.column_names));
        let physical = query.create_physical_plan(session).await?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = dar_fields(schema.fields(), None);
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

/// The text DAR's error must contain for a recorded Spark failure: a retained difference's
/// pinned text, Spark's error condition, or the recorded message of Java's
/// `ArithmeticException` for an overflow.
fn dar_message(case: &Value) -> Option<String> {
    if let Some(message) = case["expected"]["dar_message"].as_str() {
        return Some(message.to_owned());
    }
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
async fn dates_subtract_like_spark() -> TestResult {
    let records = subtraction_references()?;
    let mut retained = std::collections::BTreeMap::new();
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
    }
    assert_eq!(
        retained,
        std::collections::BTreeMap::from([
            (RetainedDifference::StringQualifier, 4),
            (RetainedDifference::IntegerUnit, 4),
            (RetainedDifference::DateIntervalRange, 4),
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
