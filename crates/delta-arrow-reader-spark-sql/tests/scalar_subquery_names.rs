//! Scalar subqueries keep Spark's generated names, explicit aliases and full result fields.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::Value;

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
    let records = include_str!("fixtures/scalar-subquery-names-metadata-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records.len(), 13);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        for case in &records[1..] {
            let actual = spark_results::capture_spark_query_result(&session, case).await;
            let expected = &case["expected"];
            if !actual.as_ref().is_ok_and(|actual| actual == expected) {
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
    for sql in [
        "SELECT (SELECT 1)",
        "SELECT NULL + (SELECT 1)",
        "SELECT (SELECT 1) AS value",
    ] {
        let expected = native.sql(sql).await?.collect().await?;
        let actual = configured.sql(sql).await?.collect().await?;
        assert_eq!(actual, expected, "{sql}");
    }
    Ok(())
}
