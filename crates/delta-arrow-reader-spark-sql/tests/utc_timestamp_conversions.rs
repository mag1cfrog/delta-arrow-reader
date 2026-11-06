//! TO_UTC_TIMESTAMP and FROM_UTC_TIMESTAMP match Spark around DST transitions.

use std::collections::BTreeMap;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType,
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
use spark_results::{TestResult, capture_schema, validate_batch};

fn conversion_references() -> TestResult<Vec<Value>> {
    let records =
        include_str!("fixtures/utc-timestamp-conversions/utc-timestamp-conversions-spark.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 4356);
    Ok(records)
}

/// Encode one value by its typed Arrow representation, as the capture encodes Spark's
/// internal rows: DATE days, TIMESTAMP UTC-epoch and TIMESTAMP_NTZ wall-clock microseconds.
fn encode_typed_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Int64 => json!(column.as_primitive::<Int64Type>().value(row).to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Utf8 => json!(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(column.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(column.as_string_view().value(row)),
        other => return Err(format!("unexpected temporal result type: {other}").into()),
    })
}

fn sort_rows(rows: &mut [Value]) {
    rows.sort_by_key(Value::to_string);
}

/// Observe analyzed (public names), optimized and executed schemas, typed rows and the
/// failing phase in the record's session time zone.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    let mut phase = "resolve";
    let result: TestResult<Vec<Value>> = async {
        let options = SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: case["timezone"].as_str().ok_or("missing time zone")?.into(),
            ..SparkSqlOptions::default()
        };
        let query = plan_query(
            session,
            case["sql"].as_str().ok_or("missing SQL")?,
            &options,
        )
        .await?;
        observation["analyzed"] = capture_schema(
            query.logical_plan.schema().fields(),
            Some(&query.column_names),
        )?;
        phase = "optimized";
        let mut state = session.state();
        let extensions = &mut state.config_mut().options_mut().extensions;
        extensions.insert(SparkSqlExpressionSemantics::default());
        extensions.insert(PreserveAggregateStatisticsSchema::default());
        let optimized = state.optimize(&query.logical_plan)?;
        observation["optimized"] =
            capture_schema(optimized.schema().fields(), Some(&query.column_names))?;
        let physical = query.create_physical_plan(session).await?;
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        let schema = physical.schema();
        observation["executed"] = capture_schema(schema.fields(), None)?;
        observation["executed_zones"] = json!(
            schema
                .fields()
                .iter()
                .map(|field| match field.data_type() {
                    DataType::Timestamp(_, zone) => zone.as_deref().map(str::to_owned),
                    _ => None,
                })
                .collect::<Vec<_>>()
        );
        let mut rows = Vec::new();
        for batch in batches {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(Value::from(
                    batch
                        .columns()
                        .iter()
                        .map(|column| encode_typed_value(column.as_ref(), row))
                        .collect::<TestResult<Vec<_>>>()?,
                ));
            }
        }
        sort_rows(&mut rows);
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
/// Match the expected failing stage and Spark's error condition.
fn matches_expected_error(expectation: &Value, observation: &Value) -> bool {
    let expected_phase = match expectation["spark_error_phase"].as_str() {
        Some("sql") => "resolve",
        Some(phase) => phase,
        None => return false,
    };
    let (Some(error), Some(condition)) = (
        observation["error"].as_str(),
        expectation["expected"]["condition"].as_str(),
    ) else {
        return false;
    };
    observation["error_phase"] == expected_phase && error.contains(&format!("[{condition}]"))
}

/// The zone each executed field carries: the session zone for Spark's TIMESTAMP, none
/// otherwise.
fn expected_zones(expectation: &Value) -> Option<Value> {
    let fields = expectation["schema_stages"]["executed"]["fields"].as_array()?;
    Some(json!(
        fields
            .iter()
            .map(|field| (field["type"] == "timestamp").then(|| expectation["timezone"].clone()))
            .collect::<Vec<_>>()
    ))
}

/// The recorded stages and outcome parts of an expectation that differ from the observation.
fn differing_parts(expectation: &Value, observation: &Value) -> Vec<&'static str> {
    let mut differing = Vec::new();
    for stage in ["analyzed", "optimized", "executed"] {
        if expectation["schema_stages"]
            .get(stage)
            .is_some_and(|expected| observation[stage] != *expected)
        {
            differing.push(stage);
        }
    }
    if expected_zones(expectation).is_some_and(|zones| observation["executed_zones"] != zones) {
        differing.push("executed_zones");
    }
    let outcome_matches = match expectation["expected"]["status"].as_str() {
        Some("ok") => observation["rows"] == expectation["expected"]["rows"],
        Some("error") => matches_expected_error(expectation, observation),
        _ => false,
    };
    if !outcome_matches {
        differing.push("outcome");
    }
    differing
}

/// Existing differences from Spark, each named for DAR's current behavior and owned by an
/// issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1024: a zone offset such as `+05:30` fails with INVALID_TIMEZONE while collecting rows.
    RejectedZoneOffsets,
    /// #1025: an invalid constant zone fails while collecting rows instead of during
    /// optimization.
    LateConstantZoneErrors,
}

/// Whether a record converts with a zone offset: its literal zone, or the per-row zones of
/// every registered row, one of which is an offset.
fn uses_zone_offset(case: &Value, header: &Value) -> TestResult<bool> {
    if let Some(zone) = case["zone"].as_str() {
        return Ok(zone.starts_with(['+', '-']));
    }
    let sql = case["sql"].as_str().ok_or("missing SQL")?;
    if !sql.contains("(ntz, z)") || sql.contains(" WHERE ") {
        return Ok(false);
    }
    let rows = header["inputs"][0]["rows"]
        .as_array()
        .ok_or("missing registered rows")?;
    Ok(rows.iter().any(|row| {
        row[4]
            .as_str()
            .is_some_and(|zone| zone.starts_with(['+', '-']))
    }))
}

/// Replace only the retained part of Spark's expectation with the current DAR result.
fn current_expectation(
    case: &Value,
    header: &Value,
) -> TestResult<(Value, Option<RetainedDifference>)> {
    let mut current = case.clone();
    let literal = !case["sql"]
        .as_str()
        .ok_or("missing SQL")?
        .contains(" FROM ");
    if case["expected"]["status"] == "ok" && uses_zone_offset(case, header)? {
        // The whole query fails while collecting rows, after its optimized stage.
        current["expected"] = json!({"status": "error", "condition": "INVALID_TIMEZONE"});
        current["spark_error_phase"] = json!("collect");
        current["schema_stages"]
            .as_object_mut()
            .ok_or("missing schema stages")?
            .remove("executed");
        return Ok((current, Some(RetainedDifference::RejectedZoneOffsets)));
    }
    if literal
        && case["expected"]["condition"] == "INVALID_TIMEZONE"
        && case["spark_error_phase"] == "optimized"
    {
        current["spark_error_phase"] = json!("collect");
        return Ok((current, Some(RetainedDifference::LateConstantZoneErrors)));
    }
    Ok((current, None))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn utc_timestamp_conversions_match_spark() -> TestResult {
    let records = conversion_references()?;
    let mut expectations = Vec::new();
    let mut retained = BTreeMap::new();
    for case in &records[1..] {
        let (expectation, difference) = current_expectation(case, &records[0])?;
        if let Some(difference) = difference {
            *retained.entry(difference).or_insert(0) += 1;
        }
        expectations.push(expectation);
    }
    let retained = retained.into_iter().collect::<Vec<_>>();
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
        for (case, expectation) in records[1..].iter().zip(&expectations) {
            let observation = observe(&session, case).await;
            let differing = differing_parts(expectation, &observation);
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "timezone": case["timezone"], "ansi": case["ansi"],
                    "batch": batch_size, "sql": case["sql"], "differing": differing,
                    "spark": {"expected": case["expected"],
                        "spark_error_phase": case["spark_error_phase"],
                        "schema_stages": case["schema_stages"]},
                    "current": {"expected": expectation["expected"],
                        "spark_error_phase": expectation["spark_error_phase"]},
                    "observed": observation,
                }));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "retained {retained:?}; {} differences:\n{}",
        differences.len(),
        differences
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    );
    assert_eq!(
        retained,
        [
            (RetainedDifference::RejectedZoneOffsets, 1068),
            (RetainedDifference::LateConstantZoneErrors, 24),
        ]
    );
    Ok(())
}
