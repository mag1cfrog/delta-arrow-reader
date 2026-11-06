//! Map names match independent Spark; other recorded gaps keep exact native contracts.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
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
const RETAINED: &str = include_str!("fixtures/map-lookup-names/retained-native-contracts.jsonl");
const STAGES: [(&str, &str); 4] = [
    ("public_schema", "analyzed"),
    ("optimized_schema", "optimized"),
    ("before_collect", "executed_before_collect"),
    ("physical_schema", "executed"),
];
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

#[tokio::test]
async fn map_lookup_names_match_recorded_contracts() -> TestResult {
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let retained = RETAINED
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let cases: Value = serde_json::from_str(CASES)?;
    assert_eq!(records.len(), 89);
    assert_eq!(retained.len(), 73);
    assert_eq!(cases["scenario_count"], 44);
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

    let mut completed = BTreeSet::new();
    let mut preserved = BTreeSet::new();
    let mut differences = Vec::new();
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
            let reference = spark_results::capture_native_query_observation(&native, sql).await?;
            let before = spark_results::capture_native_query_observation(&configured, sql).await?;
            if before != reference {
                differences.push(format!("native {id} batch={batch_size}: before={before}"));
            }
            native_controls.push((id, sql, reference, before));
        }

        for case in &records[1..] {
            let id = case["id"].as_str().ok_or("missing case ID")?;
            let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
            assert!(completed.insert((id.to_string(), ansi, batch_size)));
            let observed =
                spark_results::compare_exact_spark_query_stages_with_optimized(&configured, case)
                    .await?;
            let spark_success_matches = observed["error"].is_null()
                && case["expected"].get("rows").is_some()
                && observed["public_schema"] == case["expected"]["schema"]
                && STAGES.iter().all(|(actual, expected)| {
                    observed[*actual] == case["schema_stages"][*expected]
                })
                && observed["optimized_observation"]["names_match"] == true
                && observed["rows_raw"] == case["expected"]["rows"];
            let control = retained[1..].iter().find(|control| {
                control["id"] == case["id"]
                    && control["input"] == case["input"]
                    && control["ansi"] == case["ansi"]
                    && control["batch_size"] == batch_size
            });
            let contract_matches = if let Some(control) = control {
                assert_eq!(control["reference"], *case);
                assert!(preserved.insert((id.to_string(), ansi, batch_size)));
                observed == control["retained_observation"]
            } else {
                spark_success_matches
            };
            // These independent assertions keep the Map repair strict inside owned old gaps.
            let map_contribution_matches = STAGES.iter().all(|(actual, expected)| {
                let actual_schema = &observed[*actual];
                let expected_schema = &case["schema_stages"][*expected];
                match id {
                    "numeric_dotted_key" => {
                        actual_schema["fields"].is_array()
                            && expected_schema["fields"].is_array()
                            && actual_schema == expected_schema
                    }
                    "typed_null_map" | "typed_empty_array" => {
                        let component =
                            "lambdafunction(namedlambdavariable()[key], namedlambdavariable())";
                        actual_schema["fields"][0]["name"]
                            .as_str()
                            .is_some_and(|name| name.contains(component))
                            && expected_schema["fields"][0]["name"]
                                .as_str()
                                .is_some_and(|name| name.contains(component))
                    }
                    "null_keys" | "nested_consumers" | "struct_paths" => {
                        let (Some(actual_fields), Some(expected_fields)) = (
                            actual_schema["fields"].as_array(),
                            expected_schema["fields"].as_array(),
                        ) else {
                            return false;
                        };
                        actual_fields.len() == expected_fields.len()
                            && actual_fields.iter().zip(expected_fields).enumerate().all(
                                |(index, (actual, expected))| {
                                    if id == "null_keys" && index == 2 {
                                        return ["name", "type", "nullable"]
                                            .iter()
                                            .all(|key| actual[*key] == expected[*key])
                                            && actual["metadata"] == json!({})
                                            && expected["metadata"]
                                                == json!({"__autoGeneratedAlias":"true"});
                                    }
                                    let struct_index = if id == "nested_consumers" { 3 } else { 6 };
                                    if id != "null_keys" && index == struct_index {
                                        let child = &actual["type"]["fields"][0];
                                        let expected_child = &expected["type"]["fields"][0];
                                        return ["name", "nullable", "metadata"]
                                            .iter()
                                            .all(|key| actual[*key] == expected[*key])
                                            && actual["type"]["fields"].as_array().map(Vec::len)
                                                == Some(1)
                                            && expected["type"]["fields"].as_array().map(Vec::len)
                                                == Some(1)
                                            && child["name"] == "col1"
                                            && expected_child["name"]
                                                == if id == "nested_consumers" {
                                                    "key"
                                                } else {
                                                    "leaf"
                                                }
                                            && ["type", "nullable", "metadata"]
                                                .iter()
                                                .all(|key| child[*key] == expected_child[*key]);
                                    }
                                    actual == expected
                                },
                            )
                    }
                    _ => true,
                }
            });
            let cast_input_matches = spark_results::matches_spark_int32_cast_input(case, &observed);
            if !contract_matches || !map_contribution_matches || !cast_input_matches {
                differences.push(format!(
                    "{id} ANSI={ansi} batch={batch_size}: contract={contract_matches}, Map={map_contribution_matches}, cast={cast_input_matches}: {observed}"
                ));
            }
            if id == "original_unaliased" {
                let original = originals
                    .iter()
                    .find(|control| control["ansi"] == ansi && control["batch_size"] == batch_size)
                    .ok_or("missing original batch control")?;
                assert_eq!(case["expected"], original["reference"]["expected"]);
                for (stage, field) in original["reference"]["schema_stages"]
                    .as_object()
                    .ok_or("missing original schema stages")?
                {
                    assert_eq!(&case["schema_stages"][stage], field);
                }
                assert!(control.is_none());
            }
            eprintln!(
                "MAP_LOOKUP_NAME_RESULT {}",
                json!({"id":id, "input":case["input"], "ansi":ansi, "batch_size":batch_size,
                    "reference":case, "observation":observed, "retained_native_contract":control,
                    "independent_spark_success_matches":spark_success_matches,
                    "independent_spark_condition":case["expected"].get("condition"),
                    "contract_matches":contract_matches,
                    "map_contribution_matches":map_contribution_matches,
                    "cast_input_matches":cast_input_matches})
            );
        }
        let config_after = format!("{:?}", configured.state().config_options());
        if config_before != config_after {
            differences.push(format!(
                "native session config changed at batch={batch_size}"
            ));
        }
        for (id, sql, reference, before) in native_controls {
            let after = spark_results::capture_native_query_observation(&configured, sql).await?;
            if after != reference {
                differences.push(format!("native {id} batch={batch_size}: after={after}"));
            }
            eprintln!(
                "MAP_LOOKUP_NAME_NATIVE {}",
                json!({"id":id, "sql":sql, "batch_size":batch_size,
                    "reference":reference, "before":before, "after":after,
                    "config_unchanged":config_before == config_after})
            );
        }
    }
    assert_eq!(completed.len(), 176);
    assert_eq!(preserved.len(), 72);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
