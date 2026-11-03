//! Independent Spark references for nested conditional common types and admission.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

#[tokio::test]
async fn nested_conditional_results_match_spark() -> TestResult {
    let mut records = include_str!("fixtures/conditional-nested-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, 12);
    for (fixture, count) in [
        (include_str!("fixtures/conditional-nested-supplement-spark.jsonl"), 110),
        (include_str!("fixtures/conditional-nested-order-spark.jsonl"), 16),
        (include_str!("fixtures/conditional-nested-boundaries-spark.jsonl"), 10),
        (include_str!("fixtures/conditional-nested-set-operations-spark.jsonl"), 10),
    ] {
        let supplement = fixture.lines().map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(supplement.len() - 1, count);
        assert_eq!(supplement[0]["spark_version"], records[0]["spark_version"]);
        assert_eq!(supplement[0]["spark_revision"], records[0]["spark_revision"]);
        assert_eq!(supplement[0]["inputs"], records[0]["inputs"]);
        records.extend(supplement.into_iter().skip(1));
    }
    assert_eq!(records.len() - 1, 158);
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI")?,
        )));
        if case["expected"]["condition"].is_null() {
            assert_eq!(case["expected"]["schema"], case["schema_stages"]["analyzed"]);
        }
    }
    let mut failures = Vec::new();
    let mut comparisons = 0;
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
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let mut comparison =
                spark_results::compare_exact_spark_query_stages(&session, case).await?;
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("NESTED_CONDITIONAL_OBSERVATION {comparison}");
            if comparison["matches"] != true {
                failures.push(format!(
                    "{} ANSI={} batch={batch_size}: expected={}, stages={}, actual={comparison}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]
                ));
            }
        }
    }
    assert_eq!(comparisons, 316);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
