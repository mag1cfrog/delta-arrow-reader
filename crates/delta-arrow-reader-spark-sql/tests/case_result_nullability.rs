//! CASE result declarations and executed schemas follow independent Spark observations.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_observation};

#[tokio::test]
async fn case_results_match_spark_declarations_execution_and_errors() -> TestResult {
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for (group, source, count) in [
        ("base", include_str!("fixtures/case-result-nullability-spark.jsonl"), 324),
        ("parents", include_str!("fixtures/case-result-nullability-parents-spark.jsonl"), 128),
        ("pruning", include_str!("fixtures/case-result-nullability-pruning-spark.jsonl"), 64),
    ] {
    let records = source.lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
    assert_eq!(records.len() - 1, count);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        if case["expected"].get("condition").is_none() {
            assert_eq!(case["expected"]["schema"], case["schema_stages"]["analyzed"]);
            assert_eq!(case["schema_stages"]["executed_before_collect"], case["schema_stages"]["executed"]);
        }
    }
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new().with_default_features().with_config(
                    SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2),
                ),
            ).build(),
        );
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let actual = capture_spark_query_observation(&session, case).await;
            let matches = match &actual {
                Ok(actual) => actual["result"] == case["expected"]
                    && actual["physical_schema"] == case["schema_stages"]["executed"],
                Err(error) => match case["expected"]["condition"].as_str() {
                    Some("CAST_INVALID_INPUT") => error.to_string().contains("[CAST_INVALID_INPUT]")
                        || error.to_string().contains("Cannot cast string 'bad'"),
                    Some("USER_RAISED_EXCEPTION") => error.to_string().contains("case-required"),
                    Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE") => error.to_string().contains("CASE WHEN condition must be BOOLEAN"),
                    Some("INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION") => error.to_string().contains("[CAST_INVALID_INPUT]"),
                    _ => false,
                },
            };
            eprintln!("CASE_SCHEMA_OBSERVATION {}", json!({
                "group":group, "id":case["id"], "input":case["input"], "ansi":case["ansi"],
                "batch":batch_size, "observation":actual.as_ref().ok(),
                "error":actual.as_ref().err().map(ToString::to_string), "matches":matches,
            }));
            if !matches {
                differences.push(format!("{group}: {} input={} ANSI={} batch={batch_size}: expected {} physical={}, got {actual:?}",
                    case["id"], case["input"], case["ansi"], case["expected"], case["schema_stages"]["executed"]));
            }
        }
    }
    }
    assert_eq!(comparisons, 1032);
    assert!(differences.is_empty(), "{} differences:\n{}", differences.len(), differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_native_case_semantics() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT CASE WHEN true THEN 1 ELSE CAST(NULL AS INT) END AS r",
        "SELECT CASE WHEN false THEN CAST(NULL AS INT) ELSE 1 END AS r",
        "SELECT CASE 1 WHEN 1 THEN 1 ELSE CAST(NULL AS INT) END AS r",
        "SELECT CASE WHEN v IS NOT NULL THEN v ELSE 2 END AS r FROM (VALUES (1),(NULL)) t(v)",
    ] {
        let expected = native.sql(sql).await?;
        let actual = configured.sql(sql).await?;
        assert_eq!(actual.schema(), expected.schema(), "{sql}");
        assert_eq!(actual.collect().await?, expected.collect().await?, "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn case_result_types_and_analyzer_scan_match_spark() -> TestResult {
    let records = include_str!("fixtures/case-result-nullability-review-spark.jsonl")
        .lines().map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
    assert_eq!(records.len() - 1, 12);
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(configure_spark_sql(
            SessionStateBuilder::new().with_default_features().with_config(
                SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2),
            ),
        ).build());
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let actual = capture_spark_query_observation(&session, case).await;
            let matches = match &actual {
                Ok(actual) => actual["result"] == case["expected"]
                    && actual["physical_schema"] == case["schema_stages"]["executed"],
                Err(error) if case["expected"]["condition"] == "DATATYPE_MISMATCH.DATA_DIFF_TYPES" => {
                    let message = error.to_string();
                    message.contains("[DATATYPE_MISMATCH.DATA_DIFF_TYPES]")
                        || (message.contains("Failed to coerce then (")
                            && message.contains("to common types in CASE WHEN expression"))
                }
                _ => false,
            };
            eprintln!("CASE_721_REVIEW_OBSERVATION {}", json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "observation":actual.as_ref().ok(), "error":actual.as_ref().err().map(ToString::to_string),
                "matches":matches,
            }));
            if !matches {
                differences.push(format!("{} ANSI={} batch={batch_size}: expected {} physical={}, got {actual:?}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]["executed"]));
            }
        }
    }
    assert_eq!(comparisons, 24);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
