//! STRUCT preserves selected child names separately from construction labels.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_result};

fn records() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/struct-expression-names-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 150);
    Ok(records)
}

async fn compare_spark_cases(records: &[Value], generated: Option<bool>) -> TestResult {
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(
            case["expected"].get("schema").is_some()
                || case["expected"]["condition"] == "FIELD_NOT_FOUND"
        );
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
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
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let id = case["id"].as_str().ok_or("missing ID")?;
            if generated.is_some_and(|generated| id.contains("generated") != generated) {
                continue;
            }
            let actual = match capture_spark_query_result(&session, case).await {
                Ok(actual) => actual,
                Err(error) => {
                    let error = error.to_string();
                    if error.contains("[FIELD_NOT_FOUND]") {
                        json!({"condition":"FIELD_NOT_FOUND"})
                    } else {
                        json!({"error":error})
                    }
                }
            };
            if actual != case["expected"] {
                differences.push(format!(
                    "{id} input={} ANSI={} batch={batch_size}: expected {}, got {actual}",
                    case["input"], case["ansi"], case["expected"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn struct_child_names_values_and_metadata_match_spark() -> TestResult {
    compare_spark_cases(&records()?, Some(false)).await
}

#[tokio::test]
async fn struct_generated_labels_and_metadata_match_spark() -> TestResult {
    compare_spark_cases(&records()?, Some(true)).await
}

#[tokio::test]
async fn struct_identifier_names_values_and_metadata_match_spark() -> TestResult {
    let identifier_records = include_str!("fixtures/struct-identifier-names-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(identifier_records[0], records()?[0]);
    assert_eq!(identifier_records.len() - 1, 48);
    assert_eq!(
        identifier_records[1..]
            .iter()
            .filter(|case| case["expected"]["condition"] == "FIELD_NOT_FOUND")
            .count(),
        6
    );
    compare_spark_cases(&identifier_records, None).await
}

#[tokio::test]
async fn struct_identifier_positions_after_wildcards_match_spark() -> TestResult {
    let wildcard_records = include_str!("fixtures/struct-identifier-wildcard-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(wildcard_records.len() - 1, 4);
    assert_eq!(wildcard_records[0]["inputs"].as_array().unwrap().len(), 1);
    let records = records()?;
    let empty_input = records[0]["inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .iter()
        .find(|input| input["name"] == "struct_empty")
        .ok_or("missing empty input")?;
    assert_eq!(&wildcard_records[0]["inputs"][0], empty_input);
    compare_spark_cases(&wildcard_records, None).await
}

#[tokio::test]
async fn struct_anonymous_positions_after_wildcards_match_spark() -> TestResult {
    let wildcard_records = include_str!("fixtures/review-757-ordinary-wildcard-spark-v1.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(wildcard_records.len() - 1, 8);
    assert!(
        wildcard_records[0]["spark_build"]
            .as_str()
            .ok_or("missing Spark build")?
            .contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
    );
    compare_spark_cases(&wildcard_records, None).await
}

#[tokio::test]
async fn native_struct_results_are_unchanged() -> TestResult {
    let records = records()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &records[0], 2)?;
    }
    let mut expected = Vec::new();
    for sql in [
        "SELECT STRUCT(nested.item, v), NAMED_STRUCT('renamed', nested.item) AS s FROM struct_mixed",
        "SELECT STRUCT(nested.item, v), NAMED_STRUCT('renamed', nested.item) AS s FROM struct_empty",
    ] {
        let frame = native.sql(sql).await?;
        expected.push((sql, frame.schema().clone(), frame.collect().await?));
    }
    for after_spark in [false, true] {
        if after_spark {
            capture_spark_query_result(
                &configured,
                &json!({"ansi":true,
                    "sql":"SELECT STRUCT(nested.item AS renamed) AS s FROM struct_mixed"}),
            )
            .await?;
        }
        for (sql, schema, batches) in &expected {
            let actual = configured.sql(sql).await?;
            assert_eq!(actual.schema(), schema, "{sql}, after_spark={after_spark}");
            assert_eq!(
                &actual.collect().await?,
                batches,
                "{sql}, after_spark={after_spark}"
            );
        }
    }
    Ok(())
}
