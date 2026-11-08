//! Timestamp strings and typed literals accept only Spark's date/time separators.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Date32Type, TimeUnit, TimestampMicrosecondType};
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

fn separator_references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/timestamp-separators/timestamp-separators-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    Ok(records)
}

/// Encode one result value by its typed Arrow representation.
///
/// TIMESTAMP values are UTC-epoch microseconds, TIMESTAMP_NTZ values are
/// wall-clock microseconds and DATE values are epoch days, matching the
/// Spark internal rows recorded by the capture script.
fn encode_typed_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => json!(
            column
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_string()
        ),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Boolean => json!(column.as_boolean().value(row).to_string()),
        DataType::Utf8 => json!(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(column.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(column.as_string_view().value(row)),
        other => return Err(format!("unexpected separator result type: {other}").into()),
    })
}

/// Observe analyzed (public names), optimized and executed schemas, typed rows
/// and the failing phase.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    let mut phase = "resolve";
    let result: TestResult<Vec<Vec<Value>>> = async {
        let options = SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: case["timezone"].as_str().ok_or("missing timezone")?.into(),
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
        let mut rows = Vec::new();
        for batch in batches {
            validate_batch(schema.as_ref(), &batch)?;
            for row in 0..batch.num_rows() {
                rows.push(vec![encode_typed_value(batch.column(0).as_ref(), row)?]);
            }
        }
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

/// Match Spark's failing stage and the DAR form of its error condition.
///
/// DAR reports a typed-literal or timestamp parse failure with the shared
/// parser's message instead of Spark's error class.
fn matches_expected_error(case: &Value, observation: &Value) -> bool {
    let expected_phase = match case["spark_error_phase"].as_str() {
        Some("sql") => "resolve",
        Some(phase) => phase,
        None => return false,
    };
    let Some(error) = observation["error"].as_str() else {
        return false;
    };
    observation["error_phase"] == expected_phase
        && match case["expected"]["condition"].as_str() {
            Some("INVALID_TYPED_LITERAL") => error.contains("invalid argument: found"),
            Some("CAST_INVALID_INPUT") => {
                error.contains("error in SQL parser: found")
                    || error.contains("[CAST_INVALID_INPUT]")
            }
            _ => false,
        }
}

/// Existing differences outside the separator contract, each with its owner.
///
/// Some inputs join a class only because their separator is now rejected: at
/// the staging base they returned a wrong value instead of failing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #938: ANSI DATE_FORMAT returns NULL for a string Spark rejects.
    DateFormatAnsiNull,
    /// #939: DATE_FORMAT prints year -1 as year of era 2.
    DateFormatNegativeYear,
}

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    match case["operator"].as_str()? {
        "date_format" if case["value"] == "negative_year_upper_t" => {
            Some(RetainedDifference::DateFormatNegativeYear)
        }
        "date_format" if case["expected"]["status"] == "error" => {
            Some(RetainedDifference::DateFormatAnsiNull)
        }
        _ => None,
    }
}

/// Replace only the retained part of Spark's expectation with the current DAR result.
fn current_expectation(case: &Value, difference: RetainedDifference) -> TestResult<Value> {
    let mut current = case.clone();
    match difference {
        RetainedDifference::DateFormatAnsiNull => {
            let rows = if case["form"] == "literal" { 1 } else { 2 };
            current["expected"] = json!({"status": "ok", "rows": vec![[Value::Null]; rows]});
        }
        RetainedDifference::DateFormatNegativeYear => {
            let rows = serde_json::to_string(&case["expected"]["rows"])?;
            if !rows.contains("\"-0001-01-02 ") {
                return Err(format!("unexpected negative-year rows: {rows}").into());
            }
            current["expected"]["rows"] =
                serde_json::from_str(&rows.replace("\"-0001-01-02 ", "\"0002-01-02 "))?;
        }
    }
    Ok(current)
}

fn matches_reference(case: &Value, observation: &Value) -> bool {
    let stages = &case["schema_stages"];
    let stages_match = ["analyzed", "optimized", "executed"].iter().all(|stage| {
        stages
            .get(stage)
            .is_none_or(|expected| observation[stage] == *expected)
    });
    stages_match
        && match case["expected"]["status"].as_str() {
            Some("ok") => observation["rows"] == case["expected"]["rows"],
            Some("error") => matches_expected_error(case, observation),
            _ => false,
        }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamp_separators_match_spark() -> TestResult {
    let records = separator_references()?;
    let mut retained = std::collections::BTreeMap::new();
    for case in &records[1..] {
        if let Some(difference) = retained_difference(case) {
            *retained.entry(difference).or_insert(0) += 1;
        }
    }
    assert_eq!(records.len() - 1, 3348);
    assert_eq!(
        retained.into_iter().collect::<Vec<_>>(),
        [
            (RetainedDifference::DateFormatAnsiNull, 92),
            (RetainedDifference::DateFormatNegativeYear, 8),
        ]
    );
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
            let expectation = match retained_difference(case) {
                Some(difference) => current_expectation(case, difference)?,
                None => case.clone(),
            };
            if !matches_reference(&expectation, &observation) {
                differences.push(json!({
                    "id": case["id"], "ansi": case["ansi"], "timezone": case["timezone"],
                    "batch": batch_size, "sql": case["sql"],
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
