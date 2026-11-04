//! Signed SLICE coordinates retain the original independent Spark observations.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use futures_util::FutureExt;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const SCENARIO_IDS: &[&str] = &[
    "literal_min_start_zero_length",
    "literal_last_max_length",
    "provider_valid_boundaries",
    "literal_negative_start_cross_end",
    "literal_negative_start_past_end",
];

fn coordinate_references() -> TestResult<(Value, Vec<Value>)> {
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/slice-parameters/manifest.json"))?;
    let mut records = include_str!("fixtures/slice-parameters/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 309);
    let header = records.remove(0);
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(
        header["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(header["timezone"], "UTC");
    assert_eq!(header["group"], "slice-parameters-880-current-v1");
    assert_eq!(header["provider_input_roundtrip_checked"], true);
    assert_eq!(header["inputs"], manifest["inputs"]);
    assert_eq!(header["capture_sha256"], manifest["capture_script_sha256"]);
    records.retain(|case| {
        case["id"]
            .as_str()
            .is_some_and(|id| SCENARIO_IDS.contains(&id))
    });
    let identities = records
        .iter()
        .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
        .collect::<Vec<_>>();
    let expected = [true, false]
        .into_iter()
        .flat_map(|ansi| SCENARIO_IDS.iter().map(move |id| json!({"id":id,"ansi":ansi})))
        .collect::<Vec<_>>();
    assert_eq!(identities, expected);
    for case in &records {
        let definition = manifest["scenarios"]
            .as_array()
            .ok_or("missing scenarios")?
            .iter()
            .find(|definition| definition["id"] == case["id"])
            .ok_or("missing coordinate scenario")?;
        for (key, value) in definition.as_object().ok_or("missing definition")? {
            assert_eq!(&case[key], value, "{} {key}", case["id"]);
        }
        assert_eq!(case["timezone"], "UTC");
        assert!(case.get("spark_error").is_none());
        for stage in [
            "public",
            "analyzed",
            "optimized",
            "executed_before_collect",
            "executed",
        ] {
            assert_eq!(
                case["schema_stages"][stage], case["expected"]["schema"],
                "{} {stage}", case["id"]
            );
        }
        assert_eq!(
            case["schema_stages"].as_object().map(|stages| stages.len()),
            Some(5)
        );
    }
    Ok((header, records))
}

#[test]
fn coordinate_references_preserve_spark_discriminators() -> TestResult {
    let (_, records) = coordinate_references()?;
    for case in &records {
        let expected = match case["id"].as_str() {
            Some("literal_min_start_zero_length" | "literal_last_max_length") => json!([[[]]]),
            Some("literal_negative_start_cross_end" | "literal_negative_start_past_end") => {
                json!([[["2", "3"]]])
            }
            Some("provider_valid_boundaries") => {
                assert_eq!(case["ordered"], true);
                let rows = case["expected"]["rows"].as_array().ok_or("missing rows")?;
                assert_eq!(rows.len(), 19);
                for (key, expected) in [
                    ("24", json!(["24", [], "false", [[]]])),
                    ("28", json!(["28", ["3"], "false", [["3"]]])),
                    ("50", json!(["50", ["2", "3"], "false", [["2", "3"]]])),
                    ("51", json!(["51", ["2", "3"], "false", [["2", "3"]]])),
                ] {
                    assert_eq!(rows.iter().find(|row| row[0] == key), Some(&expected));
                }
                continue;
            }
            _ => return Err("unexpected coordinate reference".into()),
        };
        assert_eq!(case["expected"]["rows"], expected, "{}", case["id"]);
        assert_eq!(
            case["expected"]["schema"],
            json!({"type":"struct","fields":[{"name":"r",
                "type":{"type":"array","elementType":"integer","containsNull":false},
                "nullable":false,"metadata":{}}]})
        );
    }
    Ok(())
}

#[tokio::test]
async fn slice_coordinate_results_match_original_spark() -> TestResult {
    let (header, records) = coordinate_references()?;
    let mut completed = std::collections::BTreeSet::new();
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch)
                            .with_target_partitions(2),
                    ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs(&session, &header, batch)?;
        for case in &records {
            let captured = std::panic::AssertUnwindSafe(
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case),
            )
            .catch_unwind()
            .await;
            let mut observation = match captured {
                Ok(Ok(observation)) => observation,
                Ok(Err(error)) => json!({"observer_error":error.to_string()}),
                Err(payload) => {
                    let panic = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| {
                            payload
                                .downcast_ref::<&str>()
                                .map(|message| (*message).to_owned())
                        })
                        .unwrap_or_else(|| "non-string panic payload".into());
                    json!({"panic":panic})
                }
            };
            observation["group"] = header["group"].clone();
            observation["id"] = case["id"].clone();
            observation["sql"] = case["sql"].clone();
            observation["ansi"] = case["ansi"].clone();
            observation["batch"] = json!(batch);
            eprintln!("SLICE_COORDINATE_OBSERVATION {observation}");
            if [
                "matches",
                "optimized_matches",
                "public_matches",
                "before_matches",
                "physical_matches",
                "rows_match",
                "ordered_rows_match",
                "error_matches",
            ]
            .iter()
            .any(|flag| observation[flag] != true)
            {
                differences.push(observation.to_string());
            }
            assert!(completed.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch,
            )));
        }
    }
    assert_eq!(completed.len(), 20);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
