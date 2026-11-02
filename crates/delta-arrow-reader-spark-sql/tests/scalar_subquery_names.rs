//! Scalar subqueries keep Spark's generated names, explicit aliases and full result fields.

use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::Value;

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
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
async fn scalar_subquery_names_match_independent_metadata_evidence() -> TestResult {
    replay_fixture(
        include_str!("fixtures/scalar-subquery-names-metadata-spark.jsonl"),
        13,
    )
    .await
}

#[tokio::test]
async fn scalar_subquery_names_fields_and_errors_match_spark() -> TestResult {
    replay_fixture(
        include_str!("fixtures/scalar-subquery-names-spark.jsonl"),
        55,
    )
    .await
}

#[tokio::test]
async fn correlated_scalar_subquery_names_match_spark_occurrences() -> TestResult {
    replay_fixture(
        include_str!("fixtures/scalar-subquery-names-correlated-spark.jsonl"),
        19,
    )
    .await
}

#[tokio::test]
async fn scalar_subquery_sort_names_fields_and_values_match_spark() -> TestResult {
    replay_fixture(
        include_str!("fixtures/scalar-subquery-sort-names-spark.jsonl"),
        7,
    )
    .await
}

#[tokio::test]
async fn scalar_subquery_sort_lowering_fields_values_and_restrictions_match_spark() -> TestResult {
    replay_fixture(
        include_str!("fixtures/scalar-subquery-sort-lowering-spark.jsonl"),
        13,
    )
    .await
}

#[tokio::test]
async fn scalar_subquery_sort_planned_names_match_spark() -> TestResult {
    for (fixture, record_count, accepted_count) in [
        (
            include_str!("fixtures/scalar-subquery-sort-names-spark.jsonl"),
            7,
            6,
        ),
        (
            include_str!("fixtures/scalar-subquery-sort-lowering-spark.jsonl"),
            13,
            8,
        ),
    ] {
        replay_planned_sort_names(fixture, record_count, accepted_count).await?;
    }
    Ok(())
}

async fn replay_planned_sort_names(
    fixture: &str,
    record_count: usize,
    accepted_count: usize,
) -> TestResult {
    let records = fixture
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records.len(), record_count);
    let accepted = records[1..]
        .iter()
        .filter(|case| case["expected"]["status"] == "ok")
        .collect::<Vec<_>>();
    assert_eq!(accepted.len(), accepted_count);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        for case in &accepted {
            let query = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    session_timezone: "UTC".into(),
                    ..SparkSqlOptions::default()
                },
            )
            .await?;
            let expected = case["expected"]["schema"]["fields"]
                .as_array()
                .ok_or("missing fields")?
                .iter()
                .map(|field| field["name"].as_str().ok_or("missing field name"))
                .collect::<Result<Vec<_>, _>>()?;
            if query.column_names != expected {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected:?}, got {:?}",
                    case["id"], case["ansi"], query.column_names
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn replay_fixture(fixture: &str, record_count: usize) -> TestResult {
    let records = fixture
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records.len(), record_count);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = spark_results::capture_spark_query_result(&session, case).await;
            let expected = &case["expected"];
            let matches = match &actual {
                Ok(actual) if expected["status"] != "error" => {
                    actual["schema"] == expected["schema"] && actual["rows"] == expected["rows"]
                }
                Err(error) if expected["status"] == "error" => {
                    let error = error.to_string();
                    match expected["condition"].as_str() {
                        Some("SCALAR_SUBQUERY_TOO_MANY_ROWS") => {
                            error.contains("Scalar subquery returned more than one row")
                        }
                        Some(
                            "INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN",
                        ) => error
                            .contains("Scalar subquery must return exactly one column, found 2"),
                        Some("CAST_INVALID_INPUT") => {
                            error.contains("Cannot cast string 'bad' to value of Int32 type")
                        }
                        Some("UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.CORRELATED_REFERENCE") => {
                            error.contains(
                                "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.CORRELATED_REFERENCE",
                            )
                        }
                        _ => false,
                    }
                }
                _ => false,
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_scalar_subquery_names_and_fields_remain_native() -> TestResult {
    let native = SessionContext::new();
    let configured = session(32);
    let mut differences = Vec::new();
    for sql in [
        "SELECT (SELECT 1)",
        "SELECT NULL + (SELECT 1)",
        "SELECT (SELECT 1) AS value",
    ] {
        let expected = async { native.sql(sql).await?.collect().await }.await;
        let actual = async { configured.sql(sql).await?.collect().await }.await;
        let matches = match (&actual, &expected) {
            (Ok(actual), Ok(expected)) => actual == expected,
            (Err(actual), Err(expected)) => {
                let expected = native_projection_optimizer_failure(expected);
                expected.is_some() && native_projection_optimizer_failure(actual) == expected
            }
            _ => false,
        };
        if !matches {
            differences.push(format!("{sql}: native {expected:?}, configured {actual:?}"));
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn native_projection_optimizer_failure(error: &DataFusionError) -> Option<(&str, &str, String)> {
    let DataFusionError::Context(rule, invariant) = error else {
        return None;
    };
    let DataFusionError::Context(check, cause) = invariant.as_ref() else {
        return None;
    };
    let DataFusionError::Internal(message) = cause.as_ref() else {
        return None;
    };
    (rule == "Optimizer rule 'optimize_projections' failed"
        && check
            == "Check optimizer-specific invariants after optimizer rule: optimize_projections"
        && message
            .starts_with("Assertion failed: compatible: Failed due to a difference in schemas:"))
    .then(|| (rule.as_str(), check.as_str(), cause.strip_backtrace()))
}
