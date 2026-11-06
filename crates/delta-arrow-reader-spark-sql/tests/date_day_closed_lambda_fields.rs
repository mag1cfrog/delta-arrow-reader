//! DATE arithmetic with closed higher-order day operands keeps Spark's fields at every stage.

use std::collections::BTreeMap;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Date32Type, Int32Type};
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

fn closed_lambda_references() -> TestResult<Vec<Value>> {
    let records = include_str!(
        "fixtures/date-day-closed-lambda-fields/date-day-closed-lambda-fields-spark.jsonl"
    )
    .lines()
    .map(serde_json::from_str)
    .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(
        records[0]["spark_build"].as_str().is_some_and(
            |build| build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
        )
    );
    assert_eq!(records.len() - 1, 362);
    Ok(records)
}

/// Encode one value by its typed Arrow representation, as the capture encodes Spark's
/// internal rows: INT and DATE days, BOOLEAN values, STRING text and ARRAY lists.
fn encode_typed_value(column: &dyn Array, row: usize) -> TestResult<Value> {
    if column.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match column.data_type() {
        DataType::Boolean => json!(column.as_boolean().value(row)),
        DataType::Int32 => json!(column.as_primitive::<Int32Type>().value(row).to_string()),
        DataType::Date32 => json!(column.as_primitive::<Date32Type>().value(row).to_string()),
        DataType::Utf8 => json!(column.as_string::<i32>().value(row)),
        DataType::LargeUtf8 => json!(column.as_string::<i64>().value(row)),
        DataType::Utf8View => json!(column.as_string_view().value(row)),
        DataType::List(_) => {
            let values = column.as_list::<i32>().value(row);
            json!(
                (0..values.len())
                    .map(|index| encode_typed_value(values.as_ref(), index))
                    .collect::<TestResult<Vec<_>>>()?
            )
        }
        other => return Err(format!("unexpected result type: {other}").into()),
    })
}

fn sort_rows(rows: &mut [Value]) {
    rows.sort_by_key(Value::to_string);
}

/// Observe analyzed (public names), optimized and executed schemas, typed rows and the
/// failing phase.
async fn observe(session: &SessionContext, case: &Value) -> Value {
    let mut observation = json!({});
    let mut phase = "resolve";
    let result: TestResult<Vec<Value>> = async {
        let options = SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
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
/// Match the expected failing stage and Spark's error condition, or the DAR message that a
/// retained expectation names.
fn matches_expected_error(expectation: &Value, observation: &Value) -> bool {
    let expected_phase = match expectation["spark_error_phase"].as_str() {
        Some("sql") => "resolve",
        Some(phase) => phase,
        None => return false,
    };
    let Some(error) = observation["error"].as_str() else {
        return false;
    };
    let expected = &expectation["expected"];
    let message_matches = match (
        expected["condition"].as_str(),
        expected["dar_message"].as_str(),
    ) {
        (Some(condition), None) => error.contains(&format!("[{condition}]")),
        (None, Some(message)) => error.contains(message),
        _ => false,
    };
    observation["error_phase"] == expected_phase && message_matches
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

/// Existing differences outside the closed-lambda contract, each with its owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDifference {
    /// #1033: an out-of-bounds ELEMENT_AT index fails while collecting with DAR's message
    /// instead of Spark's INVALID_ARRAY_INDEX_IN_ELEMENT_AT condition.
    ElementAtIndexMessage,
    /// #1038: ARRAY_SORT without a comparator folds during optimization, narrowing its
    /// optimized and executed fields to nonnullable.
    DefaultSortNarrowedField,
    /// #1038: legacy DATE_ADD evaluates a folded ARRAY_SORT day instead of rejecting it
    /// with DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE during analysis.
    DefaultSortLegacyDay,
    /// #1039: a LIMIT over a lambda fails during physical planning instead of
    /// INVALID_LIMIT_LIKE_EXPRESSION.IS_UNFOLDABLE during analysis.
    UnfoldableLimit,
}

const ELEMENT_AT_INDEX_MESSAGE: &str = "element_at: the index is out of bounds";
const UNSUPPORTED_LIMIT_MESSAGE: &str = "Unsupported LIMIT expression";

fn retained_difference(case: &Value) -> Option<RetainedDifference> {
    let expected = &case["expected"];
    match case["id"].as_str()? {
        _ if expected["condition"] == "INVALID_ARRAY_INDEX_IN_ELEMENT_AT"
            && case["spark_error_phase"] == "collect" =>
        {
            Some(RetainedDifference::ElementAtIndexMessage)
        }
        "control_sort_default" | "control_sort_default_day" if expected["status"] == "ok" => {
            Some(RetainedDifference::DefaultSortNarrowedField)
        }
        "control_sort_default_day"
            if expected["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
                && case["spark_error_phase"] == "sql" =>
        {
            Some(RetainedDifference::DefaultSortLegacyDay)
        }
        "control_limit_lambda"
            if expected["condition"] == "INVALID_LIMIT_LIKE_EXPRESSION.IS_UNFOLDABLE"
                && case["spark_error_phase"] == "sql" =>
        {
            Some(RetainedDifference::UnfoldableLimit)
        }
        _ => None,
    }
}

/// Replace only the retained part of Spark's expectation with the current DAR result.
fn current_expectation(case: &Value, difference: RetainedDifference) -> TestResult<Value> {
    let mut current = case.clone();
    match difference {
        RetainedDifference::ElementAtIndexMessage => {
            current["expected"] =
                json!({"status": "error", "dar_message": ELEMENT_AT_INDEX_MESSAGE});
        }
        RetainedDifference::DefaultSortNarrowedField => {
            for stage in ["optimized", "executed"] {
                let nullable = current
                    .pointer_mut(&format!("/schema_stages/{stage}/fields/0/nullable"))
                    .ok_or("missing recorded field")?;
                if *nullable != json!(true) {
                    return Err(format!("unexpected {stage} nullability: {nullable}").into());
                }
                *nullable = json!(false);
            }
        }
        RetainedDifference::DefaultSortLegacyDay => {
            current["expected"] = json!({"status": "ok", "rows": [["1"]]});
            current
                .as_object_mut()
                .ok_or("record is not an object")?
                .remove("spark_error_phase");
        }
        RetainedDifference::UnfoldableLimit => {
            current["expected"] =
                json!({"status": "error", "dar_message": UNSUPPORTED_LIMIT_MESSAGE});
            current["spark_error_phase"] = json!("optimized");
        }
    }
    Ok(current)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn date_day_closed_lambda_fields_match_spark() -> TestResult {
    let records = closed_lambda_references()?;
    let mut retained = BTreeMap::new();
    let mut expectations = Vec::new();
    for case in &records[1..] {
        expectations.push(match retained_difference(case) {
            Some(difference) => {
                *retained.entry(difference).or_insert(0) += 1;
                current_expectation(case, difference)?
            }
            None => case.clone(),
        });
    }
    assert_eq!(
        retained.into_iter().collect::<Vec<_>>(),
        [
            (RetainedDifference::ElementAtIndexMessage, 9),
            (RetainedDifference::DefaultSortNarrowedField, 3),
            (RetainedDifference::DefaultSortLegacyDay, 1),
            (RetainedDifference::UnfoldableLimit, 2),
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
        for (case, expectation) in records[1..].iter().zip(&expectations) {
            let observation = observe(&session, case).await;
            let differing = differing_parts(expectation, &observation);
            if !differing.is_empty() {
                differences.push(json!({
                    "id": case["id"], "ansi": case["ansi"], "batch": batch_size,
                    "sql": case["sql"], "differing": differing,
                    "spark": {"expected": case["expected"],
                        "spark_error_phase": case["spark_error_phase"],
                        "schema_stages": case["schema_stages"]},
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
