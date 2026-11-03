//! CASE validation and explicit SQL errors retain their Spark condition identifiers.

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    )
}

#[tokio::test]
async fn original_case_errors_keep_identifiers_bodies_and_phases() -> TestResult {
    let records = include_str!(
        "fixtures/conditional-error-identifiers/original-case-errors-spark.jsonl"
    )
    .lines()
    .map(serde_json::from_str::<Value>)
    .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 10);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = spark_results::compare_exact_spark_query_stages(&session, case).await?;
            if actual["matches"] != true {
                differences.push(format!("batch={batch_size}, expected={case}, actual={actual}"));
            }
            if let Some(error) = actual["error"].as_str() {
                let body = if case["id"] == "required_raise_else" {
                    "[USER_RAISED_EXCEPTION] case-required"
                } else {
                    "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] CASE WHEN condition must be BOOLEAN, got Null"
                };
                if !error.ends_with(body) {
                    differences.push(format!("batch={batch_size}, expected body={body:?}, actual={actual}"));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_conditions_and_errors_stay_unchanged_after_spark_planning() -> TestResult {
    let native = SessionContext::new();
    let configured = session(1);
    for after_spark in [false, true] {
        if after_spark {
            let query = plan_query(
                &configured,
                "SELECT CASE WHEN true THEN 7 ELSE RAISE_ERROR('dead') END AS r",
                &SparkSqlOptions::default(),
            )
            .await?;
            let physical = query.create_physical_plan(&configured).await?;
            collect(physical, configured.task_ctx()).await?;
        }
        for sql in [
            "SELECT CASE WHEN false THEN 1 ELSE 7 END AS r",
            "SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN 1 END AS r",
            "SELECT CASE WHEN true THEN 1 WHEN NULL THEN 2 ELSE 3 END AS r",
            "SELECT 1 / 0 AS r",
            "SELECT ARRAY[7][3] AS r",
        ] {
            let expected = spark_results::capture_native_query_observation(&native, sql).await?;
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn conditional_error_identifiers_match_independent_spark() -> TestResult {
    let records = include_str!(
        "fixtures/conditional-error-identifiers/conditional-error-identifiers-spark.jsonl"
    )
    .lines()
    .map(serde_json::from_str::<Value>)
    .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 92);
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let comparison = spark_results::compare_exact_spark_query_stages(&session, case).await?;
            eprintln!("CONDITIONAL_ERROR_OBSERVATION {}", json!({
                "id":case["id"], "input":case["input"], "ansi":case["ansi"],
                "batch":batch_size, "observation":comparison,
            }));
            // These capture probes belong to CASE admission and internal index diagnostics.
            if matches!(case["id"].as_str(), Some("invalid_condition_before_results" | "internal_element" | "internal_insert")) {
                continue;
            }
            if comparison["matches"] != true {
                differences.push(format!("batch={batch_size}, expected={case}, actual={comparison}"));
            }
            if let Some(error) = comparison["error"].as_str() {
                if case["expected"]["condition"] == "USER_RAISED_EXCEPTION" {
                    let (expected_body, _) = case["spark_error"]
                        .as_str()
                        .ok_or("missing independent error body")?
                        .split_once(" SQLSTATE:")
                        .ok_or("missing independent SQLSTATE boundary")?;
                    if !error.ends_with(expected_body) {
                        differences.push(format!("expected body={expected_body:?}, actual={comparison}"));
                    }
                } else if error.contains("[USER_RAISED_EXCEPTION]") {
                    differences.push(format!("internal failure became a user error: {comparison}"));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn public_raise_error_keeps_supported_arrow_strings_and_empty_inputs() -> TestResult {
    use arrow::array::StringArray;
    use arrow::compute::cast;
    use arrow::datatypes::DataType;
    use arrow::record_batch::RecordBatch;

    for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        for message in [Some("arrow-message"), Some(""), None] {
            let values = cast(&StringArray::from(vec![message]), &data_type)?;
            for rows in [0, 1] {
                let session = session(32);
                session.register_batch(
                    "arrow_messages",
                    RecordBatch::try_from_iter([("message", values.slice(0, rows))])?,
                )?;
                for ansi_mode in [false, true] {
                    let query = plan_query(
                        &session,
                        "SELECT RAISE_ERROR(message) AS r FROM arrow_messages",
                        &SparkSqlOptions { ansi_mode, ..SparkSqlOptions::default() },
                    )
                    .await?;
                    let public = query.logical_plan.schema().field(0);
                    assert_eq!(public.data_type(), &DataType::Null);
                    assert!(public.is_nullable());
                    let physical = query.create_physical_plan(&session).await?;
                    assert_eq!(physical.schema().field(0).data_type(), &DataType::Null);
                    assert!(physical.schema().field(0).is_nullable());
                    let actual = collect(physical, session.task_ctx()).await;
                    if rows == 0 {
                        assert_eq!(actual?.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
                    } else {
                        let error = actual.err().ok_or("required Arrow message did not raise")?;
                        assert!(error.to_string().ends_with(&format!(
                            "Execution error: [USER_RAISED_EXCEPTION] {}", message.unwrap_or("null")
                        )), "{data_type:?}, ANSI={ansi_mode}: {error}");
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn internal_raise_error_retains_original_causes_for_supported_strings() -> TestResult {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use sail_function::scalar::misc::raise_error::RaiseError;

    let function = RaiseError::new();
    for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let message = "[DIVIDE_BY_ZERO] retained internal cause";
        let scalar = ScalarValue::Utf8(Some(message.into())).cast_to(&data_type)?;
        let array = cast(&StringArray::from(vec![Some(message)]), &data_type)?;
        for value in [ColumnarValue::Scalar(scalar), ColumnarValue::Array(array)] {
            let actual = function.invoke_with_args(ScalarFunctionArgs {
                args: vec![value],
                arg_fields: vec![Arc::new(Field::new("message", data_type.clone(), false))],
                number_rows: 1,
                return_field: Arc::new(Field::new("raise_error", DataType::Null, true)),
                config_options: Default::default(),
            });
            let error = actual.err().ok_or("internal error did not raise")?;
            assert_eq!(error.to_string(), format!("Execution error: {message}"));
        }
    }
    Ok(())
}
