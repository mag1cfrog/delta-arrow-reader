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
    Ok(include_str!("fixtures/struct-expression-names-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?)
}

async fn compare_spark_cases(generated: bool) -> TestResult {
    let records = records()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 150);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(case["expected"].get("schema").is_some());
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
            if id.contains("generated") != generated {
                continue;
            }
            let difference = match capture_spark_query_result(&session, case).await {
                Ok(actual) if actual == case["expected"] => None,
                Ok(actual) => Some(format!("expected {}, got {actual}", case["expected"])),
                Err(error) => Some(error.to_string()),
            };
            if let Some(difference) = difference {
                differences.push(format!(
                    "{id} input={} ANSI={} batch={batch_size}: {difference}",
                    case["input"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn struct_child_names_values_and_metadata_match_spark() -> TestResult {
    compare_spark_cases(false).await
}

#[tokio::test]
async fn struct_generated_labels_and_metadata_match_spark() -> TestResult {
    compare_spark_cases(true).await
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
