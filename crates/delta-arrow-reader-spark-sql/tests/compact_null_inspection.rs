//! NULL inspection preserves Spark's local-input and required-value boundaries.

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
async fn compact_null_inspection_boundaries_match_spark() -> TestResult {
    let mut completed = std::collections::BTreeSet::new();
    let mut differences = Vec::new();
    for (manifest_json, fixture, record_count) in [
        (
            include_str!("fixtures/array-compact-null-inspection/manifest.json"),
            include_str!("fixtures/array-compact-null-inspection/spark-reference.jsonl"),
            21,
        ),
        (
            include_str!(
                "fixtures/array-compact-null-inspection/subquery-preparation/manifest.json"
            ),
            include_str!(
                "fixtures/array-compact-null-inspection/subquery-preparation/spark-reference.jsonl"
            ),
            3,
        ),
    ] {
        let manifest: Value = serde_json::from_str(manifest_json)?;
        let records = fixture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records.len(), record_count);
        assert_eq!(records[0]["group"], manifest["group"]);
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(
            records[0]["spark_revision"],
            "32f7299601108917fb01920a54e084595b7b3bf8"
        );
        assert_eq!(records[0]["inputs"], manifest["inputs"]);
        assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
        assert_eq!(
            records[1..]
                .iter()
                .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
                .collect::<Vec<_>>(),
            manifest["identities"]
                .as_array()
                .ok_or("missing identities")?
                .clone()
        );
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
                let mut comparison =
                    spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                        .await?;
                comparison["cast_input_matches"] = json!(
                    spark_results::matches_spark_int32_cast_input(case, &comparison)
                );
                comparison["group"] = records[0]["group"].clone();
                comparison["id"] = case["id"].clone();
                comparison["ansi"] = case["ansi"].clone();
                comparison["batch"] = json!(batch_size);
                assert!(completed.insert((
                    records[0]["group"].to_string(),
                    case["id"].to_string(),
                    case["ansi"].to_string(),
                    batch_size
                )));
                eprintln!("ARRAY_COMPACT_NULL_OBSERVATION {comparison}");
                if comparison["matches"] != true
                    || comparison["optimized_matches"] != true
                    || comparison["cast_input_matches"] != true
                {
                    differences.push(format!(
                        "{} ANSI={} batch={batch_size}: expected={} stages={}, actual={comparison}",
                        case["id"], case["ansi"], case["expected"], case["schema_stages"]
                    ));
                }
            }
        }
    }
    assert_eq!(completed.len(), 44);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
