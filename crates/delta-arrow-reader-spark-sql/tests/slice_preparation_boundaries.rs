//! SLICE preparation boundaries match independently captured Spark plans.

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
async fn slice_preparation_boundaries_match_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/slice-parameters/reachability/manifest.json"
    ))?;
    let records = include_str!("fixtures/slice-parameters/reachability/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(manifest["scenario_count"], 12);
    assert_eq!(manifest["observation_count"], 24);
    assert_eq!(records.len(), 25);
    for key in ["spark_version", "spark_revision", "timezone", "group"] {
        assert_eq!(records[0][key], manifest[key], "{key}");
    }
    assert_eq!(records[0]["capture_sha256"], manifest["capture_script_sha256"]);
    assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
    let identities = records[1..].iter()
        .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
        .collect::<Vec<_>>();
    assert_eq!(json!(identities), manifest["identities"]);
    for case in &records[1..] {
        let definition = manifest["scenarios"].as_array().ok_or("missing scenarios")?
            .iter().find(|definition| definition["id"] == case["id"])
            .ok_or("missing scenario")?;
        for (key, value) in definition.as_object().ok_or("missing definition")? {
            assert_eq!(&case[key], value, "{} {key}", case["id"]);
        }
    }
    let mut completed = std::collections::BTreeSet::new();
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_batch_size(batch)),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs(&session, &records[0], batch)?;
        for case in &records[1..] {
            let mut observation =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await
                    .unwrap_or_else(|error| json!({"observer_error":error.to_string()}));
            observation["group"] = records[0]["group"].clone();
            observation["id"] = case["id"].clone();
            observation["sql"] = case["sql"].clone();
            observation["ansi"] = case["ansi"].clone();
            observation["batch"] = json!(batch);
            eprintln!("SLICE_PREPARATION_BOUNDARY_OBSERVATION {observation}");
            if [
                "matches", "optimized_matches", "public_matches", "before_matches",
                "physical_matches", "rows_match", "ordered_rows_match", "error_matches",
            ].iter().any(|flag| observation[flag] != true) {
                differences.push(observation.to_string());
            }
            assert!(completed.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch,
            )));
        }
    }
    assert_eq!(completed.len(), 48);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
