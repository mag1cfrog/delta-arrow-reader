//! ELEMENT_AT respects ANSI mode while other array and map access keeps its contract.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "../support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "../support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_executed_query_result, capture_native_query_observation,
    remove_nullability_and_metadata,
};

fn references() -> TestResult<Vec<Value>> {
    let records = include_str!("../fixtures/element-at-ansi/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert!(records[0]["spark_build"].as_str().is_some_and(|build| {
        build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
    }));
    let definitions = records[0]["cases"].as_array().ok_or("missing cases")?;
    assert_eq!(definitions.len(), 54);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(identities.insert((id, ansi)), "duplicate {id} ANSI={ansi}");
        let definition = definitions
            .iter()
            .find(|definition| definition["id"] == id)
            .ok_or("unknown case")?;
        assert_eq!(case["sql"], definition["sql"], "{id}");
        assert_eq!(case["scope"], definition["scope"], "{id}");
        if !ansi && case["scope"] == "element_at" {
            assert_eq!(case["status"], "ok", "ordinary ANSI-off input: {case}");
        }
    }
    assert_eq!(identities.len(), definitions.len() * 2);
    Ok(records)
}

fn result_types(schema: &Value) -> TestResult<Vec<Value>> {
    schema["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| {
            let mut data_type = field["type"].clone();
            remove_nullability_and_metadata(&mut data_type);
            Ok(data_type)
        })
        .collect()
}

#[test]
fn raw_bigint_index_rejections_are_separate_from_valid_value_cases() -> TestResult {
    let records = references()?;
    let rejected = records[1..]
        .iter()
        .filter(|case| case["scope"] == "spark_rejected")
        .collect::<Vec<_>>();
    assert_eq!(rejected.len(), 8);
    for case in rejected {
        assert_eq!(case["status"], "error", "{case}");
        assert_eq!(
            case["condition"], "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
            "{case}"
        );
        assert!(case.get("rows").is_none(), "{case}");
    }
    Ok(())
}

#[tokio::test]
async fn element_at_matches_spark_values_and_executed_types() -> TestResult {
    let records = references()?;
    let native_sql = [
        "SELECT id, a[1] AS first, a[-1] AS last, array_element(a, 3) AS missing FROM element_inputs ORDER BY id",
        "SELECT id, array_element(a, 0) AS zero FROM element_inputs ORDER BY id",
    ];
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let config = SessionConfig::new()
            .with_batch_size(batch_size)
            .with_target_partitions(1);
        let native = SessionContext::new_with_config(config.clone());
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(config),
            )
            .build(),
        );
        for context in [&native, &session] {
            spark_inputs::register_recorded_inputs_with_partition_count(
                context,
                &records[0],
                batch_size,
                1,
            )?;
        }
        let mut native_before = Vec::new();
        for sql in native_sql {
            let expected = capture_native_query_observation(&native, sql).await?;
            let before = capture_native_query_observation(&session, sql).await?;
            assert_eq!(before, expected, "native before Spark: {sql}");
            assert!(before["error"].is_null(), "{before}");
            native_before.push(before);
        }
        assert_eq!(
            native_before[0]["rows"],
            json!([
                ["1", "7", "9", null],
                ["2", "7", null, null],
                ["3", "7", "7", null],
                ["4", null, null, null],
                ["5", null, null, null],
                ["6", null, null, null]
            ])
        );
        for case in records[1..]
            .iter()
            .filter(|case| case["scope"] != "spark_rejected")
        {
            let result = capture_executed_query_result(&session, case).await;
            println!(
                "ELEMENT_AT_OBSERVATION {}",
                json!({"id":case["id"], "ansi":case["ansi"], "batch_size":batch_size,
                    "actual":result.as_ref().map_err(ToString::to_string)})
            );
            let matches = match (case["status"].as_str(), &result) {
                (Some("ok"), Ok(actual)) => {
                    actual["rows"] == case["rows"]
                        && result_types(&actual["schema"])? == result_types(&case["schema"])?
                }
                (Some("error"), Err(error)) => {
                    let id = case["id"].as_str().ok_or("missing ID")?;
                    let message = if id == "try_zero" {
                        "try_element_at: the index 0 is invalid"
                    } else if id.contains("zero") {
                        "element_at: the index 0 is invalid"
                    } else {
                        "out of bounds"
                    };
                    error.to_string().contains(message)
                }
                _ => false,
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {case}, got {result:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
        for (sql, before) in native_sql.iter().zip(native_before) {
            let after = capture_native_query_observation(&session, sql).await?;
            assert_eq!(after, before, "native after both Spark ANSI modes: {sql}");
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
