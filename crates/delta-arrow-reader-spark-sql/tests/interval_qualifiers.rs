//! Interval literals and their consumers retain Spark's generated names, fields and values.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, DurationMicrosecondType, Int32Type, Int64Type, IntervalMonthDayNanoType,
    IntervalUnit, IntervalYearMonthType, TimeUnit, TimestampMicrosecondType,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
use spark_inputs::register_recorded_inputs;

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, validate_batch};

fn interval_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/interval-qualifiers/interval-qualifiers-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"]
            .as_str()
            .is_some_and(|build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8"))
    );
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 448);
    Ok(records)
}

/// The Arrow type family Spark itself exports for a recorded Spark type.
///
/// Spark's Arrow conversion maps every day-time interval range to
/// `Duration(Microsecond)` and every year-month range to `Interval(YearMonth)`,
/// so the start and end fields are compared through generated names instead.
fn spark_arrow_family(spark_type: &Value) -> TestResult<&'static str> {
    Ok(match spark_type.as_str().ok_or("expected a primitive Spark type")? {
        "integer" => "int32",
        "long" => "int64",
        "date" => "date32",
        "timestamp" => "timestamp",
        "timestamp_ntz" => "timestamp_ntz",
        "void" => "null",
        "interval" => "calendar_interval",
        "interval year" | "interval year to month" | "interval month" => "year_month_interval",
        other if other.starts_with("interval ") => "day_time_interval",
        other => return Err(format!("unexpected Spark type {other}").into()),
    })
}

fn dar_arrow_family(data_type: &DataType) -> TestResult<&'static str> {
    Ok(match data_type {
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::Date32 => "date32",
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp_ntz",
        DataType::Null => "null",
        DataType::Interval(IntervalUnit::MonthDayNano) => "calendar_interval",
        DataType::Interval(IntervalUnit::YearMonth) => "year_month_interval",
        DataType::Duration(TimeUnit::Microsecond) => "day_time_interval",
        other => return Err(format!("unexpected DAR type {other}").into()),
    })
}

/// Describe fields as Spark records them, with types reduced to Arrow families.
fn spark_fields(schema: &Value) -> TestResult<Value> {
    schema["fields"]
        .as_array()
        .ok_or("missing recorded fields")?
        .iter()
        .map(|field| {
            Ok(json!({"name": field["name"], "type": spark_arrow_family(&field["type"])?,
                "nullable": field["nullable"], "metadata": field["metadata"]}))
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

fn dar_fields(fields: &arrow::datatypes::Fields, names: Option<&[String]>) -> TestResult<Value> {
    fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let name = names.map_or(field.name().as_str(), |names| names[index].as_str());
            Ok(json!({"name": name, "type": dar_arrow_family(field.data_type())?,
                "nullable": field.is_nullable(), "metadata": field.metadata()}))
        })
        .collect::<TestResult<Vec<_>>>()
        .map(Value::from)
}

/// Encode one value by its Arrow representation, matching the capture's encoding.
fn encode_typed_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Interval(IntervalUnit::YearMonth) => json!(
            column
                .as_primitive::<IntervalYearMonthType>()
                .value(row)
                .to_string()
        ),
        DataType::Duration(TimeUnit::Microsecond) => json!(
            column
                .as_primitive::<DurationMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let value = column
                .as_primitive::<IntervalMonthDayNanoType>()
                .value(row);
            if value.nanoseconds % 1_000 != 0 {
                return Err(format!("calendar interval below microseconds: {value:?}").into());
            }
            json!([
                value.months.to_string(),
                value.days.to_string(),
                (value.nanoseconds / 1_000).to_string()
            ])
        }
        other => return Err(format!("unexpected interval result type: {other}").into()),
    })
}

/// Observe analyzed (public names), optimized and executed fields, typed rows,
/// and the failing phase.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    let mut phase = "resolve";
    let result: TestResult<Vec<Vec<Value>>> = async {
        let options = SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        };
        let query = plan_query(session, case["sql"].as_str().ok_or("missing SQL")?, &options)
            .await?;
        observation["analyzed"] =
            dar_fields(query.logical_plan.schema().fields(), Some(&query.column_names))?;
        phase = "optimized";
        let mut state = session.state();
        let extensions = &mut state.config_mut().options_mut().extensions;
        extensions.insert(SparkSqlExpressionSemantics::default());
        extensions.insert(PreserveAggregateStatisticsSchema::default());
        let optimized = state.optimize(&query.logical_plan)?;
        observation["optimized"] =
            dar_fields(optimized.schema().fields(), Some(&query.column_names))?;
        let physical = query.create_physical_plan(session).await?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = dar_fields(schema.fields(), None)?;
        let mut rows = Vec::new();
        for batch in batches {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_typed_value(column.as_ref(), row))
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

fn matches_expected_error(case: &Value, observation: &Value) -> bool {
    let expected_phase = match case["spark_error_phase"].as_str() {
        Some("sql") => "resolve",
        Some(phase) => phase,
        None => return false,
    };
    let (Some(error), Some(condition)) = (
        observation["error"].as_str(),
        case["expected"]["condition"].as_str(),
    ) else {
        return false;
    };
    observation["error_phase"] == expected_phase && error.contains(condition)
}

/// Compare every recorded stage, typed rows and the required error.
fn compare(case: &Value, observation: &Value) -> TestResult<Vec<&'static str>> {
    let mut differences = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        if let Some(expected) = case["schema_stages"].get(stage)
            && observation[stage] != spark_fields(expected)?
        {
            differences.push(stage);
        }
    }
    let values_match = match case["expected"]["status"].as_str() {
        Some("ok") => observation["rows"] == case["expected"]["rows"],
        Some("error") => matches_expected_error(case, observation),
        _ => false,
    };
    if !values_match {
        differences.push("values");
    }
    Ok(differences)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interval_qualifiers_match_spark() -> TestResult {
    let records = interval_references()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
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
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let observation = observe(&session, case).await;
            let differing = compare(case, &observation)?;
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "context": case["context"], "ansi": case["ansi"],
                    "batch": batch_size, "sql": case["sql"], "differing": differing,
                    "expected": case["expected"], "spark_error_phase": case["spark_error_phase"],
                    "schema_stages": case["schema_stages"], "observed": observation,
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
