//! Unary negation fields, NULLs, overflow, and pruning match captured Spark plans.

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

fn unary_negate_matches_spark_or_inherited_control(
    case: &Value,
    comparison: &Value,
    controls: &[Value],
) -> bool {
    if comparison["group"] != "unary-negate-fields-current-v1"
        || comparison["id"] != case["id"]
        || comparison["ansi"] != case["ansi"]
        || case["ansi"].as_bool().is_none()
        || !matches!(comparison["batch"].as_u64(), Some(1 | 32))
        || comparison.get("panic").is_some()
    {
        return false;
    }
    if comparison["matches"] == true && comparison["optimized_matches"] == true {
        return true;
    }
    let Some(control) = controls.iter().find(|control| {
        control["issue"] == 896 && control["id"] == case["id"] && control["ansi"] == case["ansi"]
    }) else {
        return false;
    };
    spark_results::matches_inherited_query_observation(case, comparison, control)
}

#[tokio::test]
async fn unary_negate_fields_match_spark() -> TestResult {
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/unary-negate-fields/manifest.json"))?;
    let records = include_str!("fixtures/unary-negate-fields/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(manifest["scenario_count"], 36);
    assert_eq!(manifest["observation_count"], 72);
    assert_eq!(records.len(), 73);
    for key in ["spark_version", "spark_revision", "timezone", "group"] {
        assert_eq!(records[0][key], manifest[key], "{key}");
    }
    assert_eq!(
        records[0]["capture_sha256"],
        manifest["capture_script_sha256"]
    );
    assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
    let identities = records[1..]
        .iter()
        .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
        .collect::<Vec<_>>();
    assert_eq!(json!(identities), manifest["identities"]);
    for case in &records[1..] {
        let definition = manifest["scenarios"]
            .as_array()
            .ok_or("missing scenarios")?
            .iter()
            .find(|definition| definition["id"] == case["id"])
            .ok_or("missing scenario")?;
        for (key, value) in definition.as_object().ok_or("missing definition")? {
            assert_eq!(&case[key], value, "{} {key}", case["id"]);
        }
    }
    let controls = include_str!("fixtures/unary-negate-fields/inherited-controls.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(controls.len(), 13);
    assert_eq!(controls[0]["issues"], json!([896]));
    assert_eq!(controls[0]["batch_objects_identical_except_batch"], true);
    let mut differences = Vec::new();
    let mut completed = std::collections::BTreeSet::new();
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
            comparison["group"] = records[0]["group"].clone();
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            assert!(completed.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch_size,
            )));
            comparison["owned_matches"] = json!(unary_negate_matches_spark_or_inherited_control(
                case,
                &comparison,
                &controls[1..],
            ));
            eprintln!("UNARY_NEGATE_FIELDS_OBSERVATION {comparison}");
            if comparison["owned_matches"] != true {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected={} stages={}, actual={comparison}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]
                ));
            }
        }
    }
    assert_eq!(completed.len(), 144);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn historical_unary_negate_fields_match_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/unary-negate-fields/historical/manifest.json"
    ))?;
    let records = include_str!("fixtures/unary-negate-fields/historical/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(manifest["observation_count"], 5);
    assert_eq!(records.len(), 6);
    for key in ["spark_version", "spark_revision", "timezone", "group"] {
        assert_eq!(records[0][key], manifest[key], "{key}");
    }
    assert_eq!(records[0]["capture_sha256"], manifest["capture_script_sha256"]);
    assert_eq!(json!(records[1..].iter().map(|case| {
        json!({"id":case["id"],"ansi":case["ansi"]})
    }).collect::<Vec<_>>()), manifest["identities"]);
    for (case, definition) in records[1..].iter().zip(
        manifest["scenarios"].as_array().ok_or("missing scenarios")?,
    ) {
        for (key, value) in definition.as_object().ok_or("missing definition")? {
            assert_eq!(&case[key], value, "{} {key}", case["id"]);
        }
    }
    let mut completed = std::collections::BTreeSet::new();
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2)),
            )
            .build(),
        );
        for case in &records[1..] {
            let mut comparison = spark_results::compare_exact_spark_query_stages_with_optimized(&session, case).await?;
            comparison["group"] = records[0]["group"].clone();
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            assert!(completed.insert((case["id"].as_str().ok_or("missing ID")?, batch_size)));
            eprintln!("UNARY_NEGATE_HISTORICAL_OBSERVATION {comparison}");
            if comparison["matches"] != true || comparison["optimized_matches"] != true {
                differences.push(format!("{} batch={batch_size}: {comparison}", case["id"]));
            }
        }
    }
    assert_eq!(completed.len(), 10);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[test]
fn inherited_unary_overflow_controls_reject_new_differences() -> TestResult {
    let references = include_str!("fixtures/unary-negate-fields/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let controls = include_str!("fixtures/unary-negate-fields/inherited-controls.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let controls = &controls[1..];
    let mut identities = std::collections::BTreeSet::new();
    for control in controls {
        assert_eq!(control["issue"], 896);
        assert_eq!(control["ansi"], true);
        let id = control["id"].as_str().ok_or("missing ID")?;
        assert!(identities.insert(id));
        let case = references
            .iter()
            .find(|case| case["id"] == control["id"] && case["ansi"] == control["ansi"])
            .ok_or("missing inherited reference")?;
        assert_eq!(control["reference"], *case);
        assert_eq!(case["expected"]["condition"], "ARITHMETIC_OVERFLOW");
        assert_eq!(control["observation"]["matches"], false);
        assert_eq!(control["observation"]["error_matches"], false);
        for batch_size in [1, 32] {
            let mut comparison = control["observation"].clone();
            comparison["batch"] = json!(batch_size);
            assert!(unary_negate_matches_spark_or_inherited_control(
                case,
                &comparison,
                controls
            ));
            for key in comparison.as_object().ok_or("missing observation")?.keys() {
                let mut wrong = comparison.clone();
                wrong[key] = json!("new difference");
                assert!(
                    !unary_negate_matches_spark_or_inherited_control(case, &wrong, controls),
                    "{key}"
                );
            }
            for (key, value) in [
                ("batch", json!(2)),
                ("ansi", json!(false)),
                ("panic", Value::Null),
                ("extra", json!(true)),
                ("rows", json!([["wrong value"]])),
                ("rows_raw", json!([["wrong value"]])),
                ("error", json!("[ARITHMETIC_OVERFLOW] different failure")),
                ("native_phase", json!("resolve")),
            ] {
                let mut wrong = comparison.clone();
                wrong[key] = value;
                assert!(
                    !unary_negate_matches_spark_or_inherited_control(case, &wrong, controls),
                    "{key}"
                );
            }
            for stage in [
                "/public_schema",
                "/before_collect",
                "/physical_schema",
                "/optimized_schema",
                "/optimized_observation/schema",
                "/optimized_observation/raw_schema",
            ] {
                for field_path in ["fields/0", "fields/0/type/fields/0"] {
                    for (field, value) in [
                        ("name", json!("wrong")),
                        ("type", json!("string")),
                        ("nullable", json!("wrong")),
                        ("metadata", json!({"new":"difference"})),
                    ] {
                        let pointer = format!("{stage}/{field_path}/{field}");
                        let mut wrong = comparison.clone();
                        if let Some(actual) = wrong.pointer_mut(&pointer) {
                            *actual = if field == "nullable" {
                                json!(!actual.as_bool().ok_or("invalid field nullability")?)
                            } else {
                                value
                            };
                            assert!(
                                !unary_negate_matches_spark_or_inherited_control(
                                    case, &wrong, controls
                                ),
                                "{pointer}"
                            );
                        }
                    }
                }
            }
            for key in case.as_object().ok_or("missing reference")?.keys() {
                let mut wrong = case.clone();
                wrong[key] = json!("different reference");
                assert!(
                    !unary_negate_matches_spark_or_inherited_control(&wrong, &comparison, controls),
                    "{key}"
                );
            }
        }
    }
    assert_eq!(
        identities,
        std::collections::BTreeSet::from([
            "required_operator_int_min_direct",
            "required_operator_int_min_struct",
            "required_function_int_min_direct",
            "required_function_int_min_struct",
            "nullable_operator_int_min_direct",
            "nullable_operator_int_min_is_null",
            "nullable_operator_int_min_struct",
            "nullable_function_int_min_direct",
            "nullable_function_int_min_is_null",
            "nullable_function_int_min_struct",
            "constant_int_min_operator",
            "constant_int_min_function",
        ])
    );
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_native_unary_negation() -> TestResult {
    let records = include_str!("fixtures/unary-negate-fields/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
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
        for sql in [
            "SELECT -required_int AS r FROM unary_input WHERE k IN (0, 1, 3) ORDER BY k",
            "SELECT -nullable_int AS r FROM unary_input WHERE k IN (0, 1, 3) ORDER BY k",
            "SELECT -CAST(required_int AS BIGINT) AS r FROM unary_input ORDER BY k",
            "SELECT -required_int AS r FROM unary_empty",
            "SELECT -CAST(NULL AS INT) AS r",
        ] {
            let expected = spark_results::capture_native_query_observation(&native, sql).await?;
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            let observation =
                json!({"sql":sql,"batch":batch_size,"native":expected,"configured":actual});
            eprintln!("UNARY_NEGATE_NATIVE_OBSERVATION {observation}");
            assert!(observation["native"]["error"].is_null(), "{observation}");
            assert_eq!(observation["native"]["phase"], "encode", "{observation}");
            for stage in ["public_schema", "before_collect", "physical_schema"] {
                assert!(observation["native"][stage].is_object(), "{observation}");
            }
            assert!(observation["native"]["rows"].is_array(), "{observation}");
            assert_eq!(
                observation["configured"], observation["native"],
                "{observation}"
            );
        }
    }
    Ok(())
}
