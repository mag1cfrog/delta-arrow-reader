//! Compare ordinary aggregate and window results across independent execution layouts.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

fn load_spark_reference_records() -> TestResult<Vec<Value>> {
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/aggregate-window-results/cases.json"))?;
    let records = include_str!("fixtures/aggregate-window-results/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["inputs"], manifest["inputs"]);
    let definitions = manifest["cases"].as_array().ok_or("missing cases")?;
    let mut expected = BTreeSet::new();
    for definition in definitions {
        let id = definition["id"].as_str().ok_or("missing case ID")?;
        for ansi in [false, true] {
            assert!(
                expected.insert((id.to_owned(), ansi)),
                "duplicate case: {id}"
            );
        }
    }
    let mut captured = BTreeSet::new();
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing reference ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(
            captured.insert((id.to_owned(), ansi)),
            "duplicate reference: {id}"
        );
        let definition = definitions
            .iter()
            .find(|definition| definition["id"] == id)
            .ok_or("unexpected reference ID")?;
        for key in ["sql", "ordered"] {
            assert_eq!(case[key], definition[key], "{id}: {key}");
        }
        assert!(
            case["schema"]["fields"].is_array(),
            "{id}: missing executed types"
        );
        assert!(case["rows"].is_array(), "{id}: missing rows");
    }
    assert_eq!(captured, expected);
    Ok(records)
}

#[tokio::test]
async fn ordinary_aggregates_and_windows_match_spark_results() -> TestResult {
    let references = load_spark_reference_records()?;
    let mut differences = Vec::new();
    // Change each input/execution setting alone, then exercise a distributed combination.
    for (provider_partitions, input_batch_size, target_partitions, output_batch_size) in [
        (1, 1, 1, 1),
        (3, 1, 1, 1),
        (1, 4, 1, 1),
        (1, 1, 3, 1),
        (1, 1, 1, 32),
        (2, 3, 3, 4),
    ] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(output_batch_size)
                            .with_target_partitions(target_partitions),
                    ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &references[0],
            input_batch_size,
            provider_partitions,
        )?;
        for case in &references[1..] {
            let mut actual = spark_results::capture_executed_query_result(&session, case)
                .await
                .unwrap_or_else(|error| json!({"error":error.to_string()}));
            let mut expected = json!({"rows":case["rows"],"schema":case["schema"]});
            spark_results::remove_nullability_and_metadata(&mut actual);
            spark_results::remove_nullability_and_metadata(&mut expected);
            println!(
                "AGGREGATE_WINDOW_RESULT {}",
                json!({"id":case["id"],"ansi":case["ansi"],
                    "provider_partitions":provider_partitions,"input_batch_size":input_batch_size,
                    "target_partitions":target_partitions,"output_batch_size":output_batch_size,
                    "actual":actual,"matches":actual==expected})
            );
            if actual != expected {
                differences.push(format!(
                    "id={} ANSI={} providers={provider_partitions} input_batch={input_batch_size} target={target_partitions} output_batch={output_batch_size}: expected {expected}; actual {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
