//! Hidden ORDER BY output projections retain their nested Spark declarations.

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
use spark_results::TestResult;

const PACKAGES: &str =
    include_str!("fixtures/case-hidden-sort-declarations/delivery-packages.json");
const REFERENCES: &[&str] = &[
    include_str!("fixtures/case-hidden-sort-declarations/references/original-parents.jsonl"),
    include_str!("fixtures/case-hidden-sort-declarations/references/scalar-case-literal.jsonl"),
    include_str!("fixtures/case-hidden-sort-declarations/references/scalar-case-folded.jsonl"),
    include_str!("fixtures/case-hidden-sort-declarations/references/absent-scalar-original.jsonl"),
    include_str!("fixtures/case-hidden-sort-declarations/references/numeric-pruning-original.jsonl"),
    include_str!("fixtures/case-hidden-sort-declarations/references/new-boundaries.jsonl"),
];

#[tokio::test]
async fn hidden_sort_output_declarations_match_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(PACKAGES)?;
    let packages = manifest["packages"].as_array().ok_or("missing packages")?;
    assert_eq!(packages.len(), REFERENCES.len());
    let mut identities = BTreeSet::new();
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for (package, reference) in packages.iter().zip(REFERENCES) {
        let records = reference
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], manifest["spark_version"]);
        assert_eq!(records[0]["spark_revision"], manifest["spark_revision"]);
        assert_eq!(json!(records.len() - 1), package["source_records"]);
        let cases = records[1..]
            .iter()
            .filter(|case| {
                package["select_ids"]
                    .as_array()
                    .is_none_or(|ids| ids.contains(&case["id"]))
            })
            .collect::<Vec<_>>();
        assert_eq!(json!(cases.len()), package["records"]);
        for case in &cases {
            assert!(identities.insert((package["package"].to_string(), case.to_string())));
            for (stage, alias) in [
                ("analyzed", "analyzed_schema"),
                ("executed_before_collect", "executed_schema_before_collect"),
                ("executed", "executed_schema_after_collect"),
            ] {
                assert!(
                    case["schema_stages"]
                        .get(stage)
                        .or_else(|| case.get(alias))
                        .is_some()
                );
            }
            assert!(case["expected"]["rows"].is_array());
        }
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
            for case in &cases {
                comparisons += 1;
                let mut comparison =
                    spark_results::compare_exact_spark_query_stages(&session, case).await?;
                comparison["package"] = package["package"].clone();
                comparison["id"] = case["id"].clone();
                comparison["input"] = case["input"].clone();
                comparison["ansi"] = case["ansi"].clone();
                comparison["batch"] = json!(batch_size);
                eprintln!("CASE_HIDDEN_SORT_DECLARATIONS_OBSERVATION {comparison}");
                if comparison["matches"] != true {
                    differences.push(format!(
                        "package={}, batch={batch_size}, expected={case}, actual={comparison}",
                        package["package"]
                    ));
                }
            }
        }
    }
    assert_eq!(identities.len(), 86);
    assert_eq!(comparisons, 172);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
