//! Independent Spark controls for make_ym_interval's BIGINT argument boundaries.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::datasource::MemTable;
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

const MANIFEST: &str =
    include_str!("fixtures/bigint-int-narrowing/make-ym-interval-arguments-manifest.json");
const INPUTS: &str = include_str!("fixtures/bigint-int-narrowing/manifest.json");
const REFERENCE: &str =
    include_str!("fixtures/bigint-int-narrowing/make-ym-interval-arguments-spark.jsonl");

fn references() -> TestResult<(Value, Vec<Value>)> {
    let manifest: Value = serde_json::from_str(MANIFEST)?;
    let inputs: Value = serde_json::from_str(INPUTS)?;
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records.len(), 33);
    let header = &records[0];
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(
        header["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    for key in [
        "scope",
        "spark_version",
        "spark_revision",
        "timezone",
        "observations",
    ] {
        assert_eq!(header[key], manifest[key], "{key}");
    }
    for key in ["inputs", "native"] {
        assert_eq!(header[key], inputs[key], "{key}");
    }
    let registered = inputs["inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .iter()
        .map(|table| json!({"name":table["name"],"schema":table["schema"],"rows":table["rows"]}))
        .collect::<Vec<_>>();
    assert_eq!(header["registered_inputs"], json!(registered));
    let definitions = manifest["cases"].as_array().ok_or("missing cases")?;
    assert_eq!(definitions.len(), 16);
    let mut expected_ids = BTreeSet::new();
    for source in definitions {
        assert_eq!(source["ansi_modes"], json!([true, false]));
        for ansi in source["ansi_modes"].as_array().ok_or("missing modes")? {
            assert!(expected_ids.insert((source["id"].to_string(), ansi.to_string())));
        }
    }
    let mut actual_ids = BTreeSet::new();
    for case in &records[1..] {
        assert!(actual_ids.insert((case["id"].to_string(), case["ansi"].to_string())));
        let source = definitions
            .iter()
            .find(|source| source["id"] == case["id"])
            .ok_or("unexpected captured ID")?;
        for key in ["sql", "purpose", "domain", "ordered", "input_tables"] {
            assert_eq!(case[key], source[key], "{key}: {case}");
        }
        for (key, value) in manifest["settings"].as_object().ok_or("missing settings")? {
            assert_eq!(&case["settings"][key], value, "{key}: {case}");
        }
        assert_eq!(
            case["settings"]["spark.sql.ansi.enabled"],
            case["ansi"].to_string()
        );
        if case["expected"]["condition"].is_null() || case["spark_error_phase"] == "collect" {
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"][stage].is_object(), "{stage}: {case}");
            }
        }
        if case["expected"]["condition"].is_null() {
            assert!(case["expected"]["rows"].is_array());
            assert_eq!(
                case["expected"]["schema"],
                case["schema_stages"]["analyzed"]
            );
        } else {
            assert!(case["spark_error_details"]["parameters"].is_object());
            assert!(case["spark_error_details"]["message"].is_string());
            assert_eq!(
                case["spark_error_details"]["condition"],
                case["expected"]["condition"]
            );
        }
    }
    assert_eq!(actual_ids, expected_ids);
    Ok((inputs, records[1..].to_vec()))
}

fn register_inputs(session: &SessionContext, manifest: &Value, batch_size: usize) -> TestResult {
    for input in manifest["inputs"].as_array().ok_or("missing inputs")? {
        let mut storage = input.clone();
        let visible_rows = input["rows"].as_array().ok_or("missing rows")?.len();
        if input["native_slice"] == true {
            let rows = storage["rows"].as_array_mut().ok_or("missing rows")?;
            rows.insert(0, manifest["native"]["sliced_hidden_rows"][0].clone());
            rows.push(manifest["native"]["sliced_hidden_rows"][1].clone());
        }
        let storage = spark_inputs::decode_recorded_batch(&storage)?;
        let batch = storage.slice(usize::from(input["native_slice"] == true), visible_rows);
        let mut partitions = vec![Vec::new(), Vec::new()];
        for (chunk, offset) in (0..visible_rows).step_by(batch_size).enumerate() {
            partitions[chunk % 2].push(batch.slice(offset, batch_size.min(visible_rows - offset)));
        }
        session.register_table(
            input["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(batch.schema(), partitions)?),
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn bigint_int_argument_boundaries_match_spark() -> TestResult {
    let (manifest, cases) = references()?;
    let mut count = 0;
    let mut failures = Vec::new();
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
        register_inputs(&session, &manifest, batch_size)?;
        for case in &cases {
            count += 1;
            let observation =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await;
            let exact_error_message_matches = observation.as_ref().is_ok_and(|observation| {
                if case["expected"]["condition"].is_null() {
                    observation["error"].is_null()
                } else {
                    observation["error"] == case["spark_error_details"]["message"]
                }
            });
            let strict_matches = exact_error_message_matches
                && observation.as_ref().is_ok_and(|observation| {
                    observation["matches"] == true && observation["optimized_matches"] == true
                });
            eprintln!(
                "BIGINT_INT_ARGUMENT_BOUNDARY {}",
                json!({
                    "id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "reference":case,"observation":observation.as_ref().ok(),
                    "capture_error":observation.as_ref().err().map(ToString::to_string),
                    "exact_error_message_matches":exact_error_message_matches,
                    "strict_matches":strict_matches,
                })
            );
            if !strict_matches {
                failures.push(format!(
                    "{} ansi={} batch={batch_size}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert_eq!(count, 64);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
