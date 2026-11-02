//! PIVOT compares floating keys without changing aggregate inputs or native SQL.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/float_inputs.rs"]
mod float_inputs;
use float_inputs::register_float_inputs;

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_result};

#[tokio::test]
async fn pivot_float_keys_match_spark() -> TestResult {
    let records = include_str!("fixtures/pivot-float-keys-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 128);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
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
        register_float_inputs(&session, &records[0], batch_size, 2)?;
        for case in &records[1..] {
            let actual = capture_spark_query_result(&session, case).await;
            if actual.as_ref().ok() != Some(&case["expected"]) {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {actual:?}",
                    case["id"], case["ansi"], case["expected"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_pivot_planning_does_not_change_native_float_equality() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/pivot-float-keys-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_float_inputs(session, &metadata, 32, 1)?;
    }
    for kind in ["float", "double"] {
        capture_spark_query_result(&configured, &json!({"ansi":true,"sql":format!(
            "SELECT * FROM (SELECT k FROM {kind}_mixed) PIVOT(COUNT(*) FOR (k) IN (CAST('-0.0' AS {kind})))"
        )})).await?;
        for operator in ["=", "IS NOT DISTINCT FROM"] {
            let sql = format!(
                "SELECT COUNT(*) AS n FROM {kind}_mixed WHERE k {operator} CAST('-0.0' AS {kind})"
            );
            let expected = native.sql(&sql).await?.collect().await?;
            let actual = configured.sql(&sql).await?.collect().await?;
            assert_eq!(actual, expected, "{sql}");
        }
    }
    Ok(())
}
