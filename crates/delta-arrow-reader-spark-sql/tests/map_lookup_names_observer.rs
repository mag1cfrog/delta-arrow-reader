//! Root-only baseline observations for the unchanged independent #823 references.
//!
//! Completion records harness coverage, not Spark compatibility acceptance.

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use futures_util::FutureExt;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const REFERENCE: &str = include_str!("fixtures/map-lookup-names/spark-reference.jsonl");
const CASES: &str = include_str!("fixtures/map-lookup-names/823-preparation-v2-cases.json");
const PROVENANCE: &str = include_str!("fixtures/map-lookup-names/provenance.json");

const NATIVE_CONTROLS: [(&str, &str); 4] = [
    (
        "literal_and_dynamic_map_keys",
        "SELECT id,m['key'] AS literal_key,m[key] AS dynamic_key FROM map_names_mixed ORDER BY id",
    ),
    (
        "nested_struct_and_map",
        "SELECT id,sm['leaf'] AS leaf,sm['m']['key'] AS nested_key FROM map_names_mixed ORDER BY id",
    ),
    (
        "missing_and_null_keys",
        "SELECT id,m['missing'] AS absent,m[NULL] AS null_key FROM map_names_mixed ORDER BY id",
    ),
    (
        "missing_struct_field",
        "SELECT (named_struct('leaf',1)).missing AS missing_field",
    ),
];

/// Record every query and native guard through the existing shared observers.
#[tokio::test]
async fn record_map_lookup_name_baseline() -> TestResult {
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let cases: Value = serde_json::from_str(CASES)?;
    let provenance: Value = serde_json::from_str(PROVENANCE)?;
    assert_eq!(records.len(), 89);
    assert_eq!(cases["scenario_count"], 44);
    assert_eq!(cases["spark_observation_count"], 88);
    assert_eq!(cases["native_observation_count_per_source"], 176);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["case_sensitive"], false);
    let originals = cases["original_controls"]
        .as_array()
        .ok_or("missing original controls")?;
    assert_eq!(originals.len(), 4);
    let original_keys = originals
        .iter()
        .map(|control| {
            json!({"id":control["id"], "input":control["input"],
                "ansi":control["ansi"], "batch_size":control["batch_size"]})
        })
        .collect::<Vec<_>>();
    for case in &records[1..] {
        let definition = cases["scenarios"]
            .as_array()
            .ok_or("missing scenarios")?
            .iter()
            .find(|definition| definition["id"] == case["id"])
            .ok_or("reference has no declared scenario")?;
        for (key, value) in definition.as_object().ok_or("invalid scenario")? {
            assert_eq!(&case[key], value, "{} {key}", case["id"]);
        }
    }
    eprintln!(
        "MAP_LOOKUP_NAME_BASELINE {}",
        json!({"kind":"header", "compatibility_acceptance":false,
            "provenance":provenance, "reference_header":records[0],
            "original_control_keys":original_keys})
    );

    let mut completed = BTreeSet::new();
    let mut difference_records = 0;
    let mut error_review_records = 0;
    let mut native_difference_records = 0;
    for batch_size in [1, 32] {
        let config = SessionConfig::new()
            .with_batch_size(batch_size)
            .with_target_partitions(2);
        let native = SessionContext::new_with_config(config.clone());
        let configured = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(config),
            )
            .build(),
        );
        for session in [&native, &configured] {
            spark_inputs::register_recorded_inputs(session, &records[0], batch_size)?;
        }
        let config_before = format!("{:?}", configured.state().config_options());
        let mut native_controls = Vec::new();
        for (id, sql) in NATIVE_CONTROLS {
            let mut observations = Vec::new();
            for session in [&native, &configured] {
                observations.push(
                    match spark_results::capture_native_query_observation(session, sql).await {
                        Ok(observation) => observation,
                        Err(error) => json!({"observer_error":error.to_string()}),
                    },
                );
            }
            native_controls.push(json!({"id":id, "sql":sql,
                "reference":observations[0], "configured_before":observations[1]}));
        }

        for case in &records[1..] {
            let id = case["id"].as_str().ok_or("missing case ID")?;
            let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
            assert!(completed.insert((id.to_string(), ansi, batch_size)));
            let attempt = AssertUnwindSafe(
                spark_results::compare_exact_spark_query_stages_with_optimized(&configured, case),
            )
            .catch_unwind()
            .await;
            let observation = match attempt {
                Ok(Ok(observation)) => observation,
                Ok(Err(error)) => json!({"observer_error":error.to_string()}),
                Err(payload) => {
                    let message = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|text| (*text).to_string()))
                        .unwrap_or_else(|| "non-string panic payload".to_string());
                    json!({"panic":message})
                }
            };
            let mut mismatches = [
                "public_matches",
                "before_matches",
                "physical_matches",
                "rows_match",
                "ordered_rows_match",
                "error_matches",
                "optimized_matches",
            ]
            .into_iter()
            .filter(|key| observation[*key] == false)
            .collect::<Vec<_>>();
            for key in ["observer_error", "panic"] {
                if observation.get(key).is_some() {
                    mismatches.push(key);
                }
            }
            let cast_input_matches =
                spark_results::matches_spark_int32_cast_input(case, &observation);
            if !cast_input_matches {
                mismatches.push("cast_input_matches");
            }
            difference_records += usize::from(!mismatches.is_empty());
            let error_review_required = case["expected"].get("condition").is_some()
                || observation["error"].is_string();
            error_review_records += usize::from(error_review_required);
            let original = originals.iter().find(|control| {
                control["id"] == case["id"]
                    && control["input"] == case["input"]
                    && control["ansi"] == case["ansi"]
                    && control["batch_size"] == batch_size
            });
            if id == "original_unaliased" {
                let control = original.ok_or("missing original batch control")?;
                assert_eq!(case["expected"], control["reference"]["expected"]);
                for (stage, field) in control["reference"]["schema_stages"]
                    .as_object()
                    .ok_or("missing original schema stages")?
                {
                    assert_eq!(&case["schema_stages"][stage], field);
                }
            }
            eprintln!(
                "MAP_LOOKUP_NAME_BASELINE {}",
                json!({"kind":"query_observation", "id":id, "input":case["input"],
                    "ansi":ansi, "batch_size":batch_size, "reference":case,
                    "observation":observation, "diagnostic_mismatch_fields":mismatches,
                    "cast_input_matches":cast_input_matches,
                    "exact_error_cause_review_required":error_review_required,
                    "historical_control":original, "compatibility_acceptance":false})
            );
        }

        let config_after = format!("{:?}", configured.state().config_options());
        eprintln!(
            "MAP_LOOKUP_NAME_BASELINE {}",
            json!({"kind":"session_observation", "batch_size":batch_size,
                "config_before":config_before, "config_after":config_after,
                "config_unchanged":config_before == config_after})
        );
        for mut control in native_controls {
            let sql = control["sql"].as_str().ok_or("missing native SQL")?;
            let after =
                match spark_results::capture_native_query_observation(&configured, sql).await {
                    Ok(observation) => observation,
                    Err(error) => json!({"observer_error":error.to_string()}),
                };
            let before_matches = control["configured_before"] == control["reference"];
            let after_matches = after == control["reference"];
            native_difference_records += usize::from(!before_matches || !after_matches);
            control["kind"] = json!("native_control");
            control["batch_size"] = json!(batch_size);
            control["configured_after"] = after;
            control["before_matches"] = json!(before_matches);
            control["after_matches"] = json!(after_matches);
            control["compatibility_acceptance"] = json!(false);
            eprintln!("MAP_LOOKUP_NAME_BASELINE {control}");
        }
    }
    assert_eq!(completed.len(), 176);
    eprintln!(
        "MAP_LOOKUP_NAME_BASELINE {}",
        json!({"kind":"summary", "query_observations":completed.len(),
            "diagnostic_difference_records":difference_records,
            "exact_error_cause_review_records":error_review_records,
            "native_controls":8, "native_difference_records":native_difference_records,
            "compatibility_acceptance":false})
    );
    Ok(())
}
