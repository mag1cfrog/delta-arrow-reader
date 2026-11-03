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
    let records =
        include_str!("fixtures/conditional-error-identifiers/original-case-errors-spark.jsonl")
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
                differences.push(format!(
                    "batch={batch_size}, expected={case}, actual={actual}"
                ));
            }
            if let Some(error) = actual["error"].as_str() {
                let body = if case["id"] == "required_raise_else" {
                    "[USER_RAISED_EXCEPTION] case-required"
                } else {
                    "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] CASE WHEN condition must be BOOLEAN, got Null"
                };
                if !error.ends_with(body) {
                    differences.push(format!(
                        "batch={batch_size}, expected body={body:?}, actual={actual}"
                    ));
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
    let mut before: Vec<(Value, Value)> = Vec::new();
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
        for (index, sql) in [
            "SELECT CASE WHEN false THEN 1 ELSE 7 END AS r",
            "SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN 1 END AS r",
            "SELECT CASE WHEN true THEN 1 WHEN NULL THEN 2 ELSE 3 END AS r",
            "SELECT 1 / 0 AS r",
            "SELECT ARRAY[7][3] AS r",
        ]
        .into_iter()
        .enumerate()
        {
            let expected = spark_results::capture_native_query_observation(&native, sql).await?;
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            eprintln!(
                "CONDITIONAL_ERROR_NATIVE_OBSERVATION {}",
                json!({
                    "sql":sql, "after_spark":after_spark, "native":expected, "configured":actual,
                })
            );
            if after_spark {
                assert_eq!(
                    (&expected, &actual),
                    (&before[index].0, &before[index].1),
                    "{sql}"
                );
            } else {
                before.push((expected.clone(), actual.clone()));
            }
            // #861: configured native division already preserves errors during folding.
            if sql != "SELECT 1 / 0 AS r" {
                assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn conditional_error_identifiers_and_internal_guards() -> TestResult {
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
        register_ordered_null_first_messages(&session)?;
        for case in &records[1..] {
            let comparison =
                spark_results::compare_exact_spark_query_stages(&session, case).await?;
            eprintln!(
                "CONDITIONAL_ERROR_OBSERVATION {}",
                json!({
                    "id":case["id"], "input":case["input"], "ansi":case["ansi"],
                    "batch":batch_size, "observation":comparison,
                })
            );
            match preserves_inherited_diagnostic(case, &comparison, batch_size) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    differences.push(format!("internal guard: {case}, {error}"));
                    continue;
                }
            }
            if comparison["matches"] != true {
                differences.push(format!(
                    "batch={batch_size}, expected={case}, actual={comparison}"
                ));
            }
            if let Some(error) = comparison["error"].as_str() {
                if case["expected"]["condition"] == "USER_RAISED_EXCEPTION" {
                    let (expected_body, _) = case["spark_error"]
                        .as_str()
                        .ok_or("missing independent error body")?
                        .split_once(" SQLSTATE:")
                        .ok_or("missing independent SQLSTATE boundary")?;
                    if !error.ends_with(expected_body) {
                        differences.push(format!(
                            "expected body={expected_body:?}, actual={comparison}"
                        ));
                    }
                } else if error.contains("[USER_RAISED_EXCEPTION]") {
                    differences.push(format!(
                        "internal failure became a user error: {comparison}"
                    ));
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn register_ordered_null_first_messages(session: &SessionContext) -> TestResult {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;

    // A single partition makes the required first message deterministic.
    session.deregister_table("messages_null_first")?;
    session.register_batch(
        "messages_null_first",
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![0, 1])) as ArrayRef),
            (
                "message",
                Arc::new(StringArray::from(vec![None, Some("after-null")])) as ArrayRef,
            ),
        ])?,
    )?;
    Ok(())
}

// #863-#865 preserve internal diagnostics separately from raw Spark comparisons.
fn preserves_inherited_diagnostic(case: &Value, actual: &Value, batch: usize) -> TestResult<bool> {
    // #862 keeps the existing condition-before-result validation order.
    if case["id"] == "invalid_condition_before_results" {
        let expected = json!({
            "native_phase":"resolve",
            "error":"External error: invalid argument: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] CASE WHEN condition must be BOOLEAN, got Int32",
            "public_schema":null, "before_collect":null, "physical_schema":null, "rows":null,
        });
        for (field, expected) in expected.as_object().ok_or("missing admission guard")? {
            if actual[field] != *expected {
                return Err(format!("{field}: expected {expected}, got {}", actual[field]).into());
            }
        }
        return Ok(true);
    }
    if !matches!(
        case["id"].as_str(),
        Some("internal_division" | "internal_index" | "internal_element" | "internal_insert")
    ) {
        return Ok(false);
    }
    let records =
        include_str!("fixtures/conditional-error-identifiers/internal-error-controls-before.jsonl")
            .lines()
            .skip(1)
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
    let before = records
        .iter()
        .find(|before| {
            before["id"] == case["id"]
                && before["input"] == case["input"]
                && before["ansi"] == case["ansi"]
                && before["batch"] == batch
        })
        .ok_or("missing internal baseline guard")?;
    for (field, expected) in before["observation"]
        .as_object()
        .ok_or("missing baseline observation")?
    {
        if actual[field] != *expected {
            return Err(format!("{field}: expected {expected}, got {}", actual[field]).into());
        }
    }
    Ok(true)
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
                        &SparkSqlOptions {
                            ansi_mode,
                            ..SparkSqlOptions::default()
                        },
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
                        assert!(
                            error.to_string().ends_with(&format!(
                                "Execution error: [USER_RAISED_EXCEPTION] {}",
                                message.unwrap_or("null")
                            )),
                            "{data_type:?}, ANSI={ansi_mode}: {error}"
                        );
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

#[tokio::test]
async fn public_assertions_keep_null_conditions_messages_and_empty_inputs() -> TestResult {
    let records = include_str!(
        "fixtures/conditional-error-identifiers/assertion-and-internal-controls-spark.jsonl"
    )
    .lines()
    .map(serde_json::from_str::<Value>)
    .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 76);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        // One populated partition preserves the mixed provider's first required error.
        spark_inputs::register_recorded_inputs(&session, &records[0], 32)?;
        for case in &records[1..] {
            let actual = spark_results::compare_exact_spark_query_stages(&session, case).await?;
            eprintln!(
                "ASSERTION_ERROR_OBSERVATION {}",
                json!({
                    "id":case["id"], "input":case["input"], "ansi":case["ansi"],
                    "batch":batch_size, "observation":actual,
                })
            );
            // The two internal SQL probes are retained as raw diagnostics for separate owners.
            if case["id"]
                .as_str()
                .ok_or("missing ID")?
                .starts_with("assert_")
            {
                if actual["matches"] != true {
                    differences.push(format!(
                        "batch={batch_size}, expected={case}, actual={actual}"
                    ));
                }
                if case["expected"]["condition"] == "USER_RAISED_EXCEPTION" {
                    let (body, _) = case["spark_error"]
                        .as_str()
                        .ok_or("missing independent error body")?
                        .split_once(" SQLSTATE:")
                        .ok_or("missing independent SQLSTATE boundary")?;
                    if !actual["error"]
                        .as_str()
                        .is_some_and(|error| error.ends_with(body))
                    {
                        differences.push(format!("expected body={body:?}, actual={actual}"));
                    }
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[test]
fn user_raise_error_uses_the_first_null_and_accepts_zero_rows() -> TestResult {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    use sail_function::scalar::misc::raise_error::RaiseError;

    let function = RaiseError::new_user_error();
    for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let array = cast(
            &StringArray::from(vec![None, Some("after-null")]),
            &data_type,
        )?;
        let scalar = ScalarValue::Utf8(None).cast_to(&data_type)?;
        for (value, rows) in [
            (ColumnarValue::Array(array.clone()), 2),
            (ColumnarValue::Scalar(scalar.clone()), 1),
            (ColumnarValue::Array(array.slice(0, 0)), 0),
            (ColumnarValue::Scalar(scalar), 0),
        ] {
            let actual = function.invoke_with_args(ScalarFunctionArgs {
                args: vec![value],
                arg_fields: vec![Arc::new(Field::new("message", data_type.clone(), true))],
                number_rows: rows,
                return_field: Arc::new(Field::new("raise_error", DataType::Null, true)),
                config_options: Default::default(),
            });
            if rows == 0 {
                let empty = actual?.into_array(0)?;
                assert_eq!(empty.data_type(), &DataType::Null);
                assert!(empty.is_empty());
                empty.to_data().validate_full()?;
            } else {
                let error = actual.err().ok_or("required NULL message did not raise")?;
                assert_eq!(
                    error.to_string(),
                    "Execution error: [USER_RAISED_EXCEPTION] null"
                );
            }
        }
    }
    let invalid = function.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Scalar(ScalarValue::Int32(None))],
        arg_fields: vec![Arc::new(Field::new("message", DataType::Int32, true))],
        number_rows: 0,
        return_field: Arc::new(Field::new("raise_error", DataType::Null, true)),
        config_options: Default::default(),
    });
    assert!(
        matches!(invalid, Err(datafusion::common::DataFusionError::Internal(message))
        if message == "raise_error expects a single UTF-8 string argument")
    );
    Ok(())
}
