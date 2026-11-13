//! Exact issue 970 references remain independent of the native property regressions.

use std::collections::BTreeSet;
use std::path::Path;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_native_query_observation, compare_exact_spark_query_stages_with_optimized,
};

fn session(batch_size: usize, target_partitions: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(target_partitions),
                ),
        )
        .build(),
    )
}

fn records(text: &str) -> TestResult<Vec<Value>> {
    Ok(text
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn comparison_matches(case: &Value, actual: &Value) -> bool {
    let public_matches = case["expected"]
        .get("schema")
        .is_none_or(|schema| actual["public_schema"] == *schema);
    let cause_matches = if case["expected"]["condition"] == "CAST_INVALID_INPUT" {
        let target = if case["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("float_"))
        {
            "Float32"
        } else {
            "Float64"
        };
        actual["error"].as_str().is_some_and(|error| {
            error.contains(&format!(
                "[CAST_INVALID_INPUT] Cannot cast string \"bad\" to {target}"
            ))
        })
    } else {
        true
    };
    actual["matches"] == true
        && actual["optimized_matches"] == true
        && public_matches
        && cause_matches
}

#[tokio::test]
async fn original_constant_array_grouping_sets_keep_exact_counts_and_fields() -> TestResult {
    let records = records(include_str!(
        "fixtures/grouping-set-properties-original-spark.jsonl"
    ))?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, 12);
    let identities = records[1..]
        .iter()
        .filter(|case| {
            case["id"]
                .as_str()
                .is_some_and(|id| id.ends_with("array_grouping_sets"))
        })
        .map(|case| (case["id"].as_str(), case["ansi"].as_bool()))
        .collect::<BTreeSet<_>>();
    assert_eq!(identities.len(), 4);
    let mut differences = Vec::new();
    for (batch_size, target_partitions, input_batch_size, provider_partitions) in [
        (1, 2, 1, 2),
        (32, 2, 32, 2),
        (1, 1, 1, 1),
        (1, 4, 1, 4),
        (32, 1, 32, 1),
        (32, 4, 32, 4),
        // Keep multiple nonempty source partitions at a large execution batch size.
        (32, 2, 1, 2),
    ] {
        let session = session(batch_size, target_partitions);
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &records[0],
            input_batch_size,
            provider_partitions,
        )?;
        for case in &records[1..] {
            let actual = compare_exact_spark_query_stages_with_optimized(&session, case).await?;
            let matches = comparison_matches(case, &actual);
            eprintln!(
                "GROUPING_SET_ORIGINAL {}",
                json!({
                    "id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "target_partitions":target_partitions,"input_batch_size":input_batch_size,
                    "provider_partitions":provider_partitions,"observation":actual,"matches":matches,
                })
            );
            if !matches {
                differences.push(format!("{} ANSI={} batch={batch_size} target={target_partitions} input_batch={input_batch_size} providers={provider_partitions}: {actual}",case["id"],case["ansi"]));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn captured_records() -> TestResult<(Vec<Value>, Value)> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/grouping-set-properties-spark.jsonl");
    let text = std::fs::read_to_string(&path).map_err(|error| {
        format!(
            "independent root capture required at {}: {error}",
            path.display()
        )
    })?;
    let records = records(&text)?;
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/grouping-set-properties-cases.json"))?;
    assert_eq!(records[0]["spark_version"], manifest["spark_version"]);
    assert_eq!(records[0]["spark_revision"], manifest["spark_revision"]);
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["inputs"], manifest["inputs"]);
    assert_eq!(records.len() - 1, 46);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI")?;
        assert!(identities.insert((id.to_owned(), ansi)));
        let definition = manifest["cases"]
            .as_array()
            .ok_or("missing cases")?
            .iter()
            .find(|definition| definition["id"] == id)
            .ok_or("unrecorded identity")?;
        for key in ["sql", "category"] {
            assert_eq!(case[key], definition[key], "{id}: {key}");
        }
        if case["expected"].get("rows").is_some() {
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"].get(stage).is_some(), "{id}: {stage}");
            }
            assert_eq!(case["public_schema"], case["expected"]["schema"]);
        } else {
            assert_eq!(case["category"], "required_cast_error");
            assert_eq!(case["expected"]["condition"], "CAST_INVALID_INPUT");
            assert!(
                case["spark_error"]
                    .as_str()
                    .is_some_and(|error| error.contains("bad"))
            );
        }
    }
    Ok((records, manifest))
}

#[tokio::test]
async fn captured_grouping_masks_match_each_spark_stage() -> TestResult {
    let (records, _) = captured_records()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size, 2);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = compare_exact_spark_query_stages_with_optimized(&session, case).await?;
            let matches = comparison_matches(case, &actual);
            eprintln!(
                "GROUPING_SET_CAPTURE {}",
                json!({
                    "id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "observation":actual,"matches":matches,
                })
            );
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_grouping_set_counts_and_fields_survive_spark_execution() -> TestResult {
    let records = records(include_str!(
        "fixtures/grouping-set-properties-original-spark.jsonl"
    ))?;
    let spark_control = records[1..]
        .iter()
        .find(|case| case["id"] == "float_mixed_array" && case["ansi"] == true)
        .ok_or("missing ordinary Spark control")?;
    let sql = "SELECT ARRAY[1] AS key,COUNT(*) AS n FROM float_scalar_mixed GROUP BY GROUPING SETS ((ARRAY[1]),())";
    for batch_size in [1, 32] {
        let plain = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(2),
                )
                .build(),
        );
        let configured = session(batch_size, 2);
        for session in [&plain, &configured] {
            spark_inputs::register_recorded_inputs(session, &records[0], batch_size)?;
        }
        let before = capture_native_query_observation(&plain, sql).await?;
        eprintln!(
            "GROUPING_SET_NATIVE_SQL {}",
            json!({"batch":batch_size,"sql":sql,"observation":before})
        );
        for after_spark in [false, true] {
            if after_spark {
                let actual =
                    compare_exact_spark_query_stages_with_optimized(&configured, spark_control)
                        .await?;
                assert!(comparison_matches(spark_control, &actual), "{actual}");
            }
            let actual = capture_native_query_observation(&configured, sql).await?;
            eprintln!(
                "GROUPING_SET_NATIVE_SQL {}",
                json!({"batch":batch_size,"after_spark":after_spark,"sql":sql,"observation":actual})
            );
            assert_eq!(
                actual, before,
                "batch={batch_size} after_spark={after_spark}"
            );
            assert_eq!(actual["error"], Value::Null);
            assert_eq!(actual["rows"], json!([[["1"], "13"], [null, "13"]]));
        }
    }
    Ok(())
}
