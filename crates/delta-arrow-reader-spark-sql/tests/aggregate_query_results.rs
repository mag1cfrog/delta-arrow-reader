//! Compare grouped and global aggregates with Spark across empty, NULL and duplicate inputs.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, compare_exact_spark_query_stages_with_optimized};

const CASE_IDS: [&str; 18] = [
    "aggregate_mixed",
    "grouped_mixed",
    "aggregate_empty",
    "grouped_empty",
    "aggregate_all_null",
    "grouped_all_null",
    "aggregate_labels",
    "having_count",
    "having_null_sum",
    "having_alias",
    "having_count_if",
    "distinct_mixed",
    "distinct_grouped_mixed",
    "distinct_empty",
    "distinct_grouped_empty",
    "distinct_all_null",
    "distinct_grouped_all_null",
    "distinct_nested_arguments",
];
const SCHEMA_STAGES: [&str; 4] = [
    "analyzed",
    "optimized",
    "executed_before_collect",
    "executed",
];
const REFERENCE: &str = include_str!("fixtures/aggregate-query-results-spark.jsonl");

fn is_complete_aggregate_schema(schema: &Value) -> bool {
    schema["type"] == "struct"
        && schema["fields"].as_array().is_some_and(|fields| {
            !fields.is_empty()
                && fields.iter().all(|field| {
                    field["name"].is_string()
                        && matches!(field["type"].as_str(), Some("integer" | "long" | "double"))
                        && field["nullable"].is_boolean()
                        && field["metadata"].is_object()
                })
        })
}

fn parse_aggregate_fixture(source: &str) -> TestResult<Vec<Value>> {
    let records = source
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/aggregate-query-results-cases.json"))?;
    if records.len() != 37 {
        return Err(format!(
            "expected 36 aggregate references, got {} records",
            records.len()
        )
        .into());
    }
    let header = &records[0];
    for (key, expected) in manifest["capture"]
        .as_object()
        .ok_or("missing capture metadata")?
    {
        if header[key] != *expected {
            return Err(format!("aggregate capture metadata differs: {key}").into());
        }
    }
    if header["legacy_source"]["sha256"] != manifest["legacy_source"]["sha256"]
        || header["inputs"] != manifest["inputs"]
    {
        return Err("aggregate provenance or original provider inventory differs".into());
    }
    let definitions = manifest["cases"]
        .as_array()
        .ok_or("missing original query definitions")?;
    if definitions.len() != CASE_IDS.len() {
        return Err("expected 18 original aggregate definitions".into());
    }
    let expected_identities = CASE_IDS
        .into_iter()
        .flat_map(|id| [true, false].map(move |ansi| (id.to_owned(), ansi)))
        .collect::<BTreeSet<_>>();
    let mut identities = BTreeSet::new();
    let availability = json!({
        "public": true, "analyzed": true, "optimized": true,
        "executed_before_collect": true, "executed": true,
    });
    for case in &records[1..] {
        let id = case["id"].as_str().ok_or("missing aggregate ID")?;
        let ansi = case["ansi"]
            .as_bool()
            .ok_or("missing aggregate ANSI mode")?;
        if !identities.insert((id.to_owned(), ansi)) {
            return Err(format!("duplicate aggregate identity: {id} ANSI={ansi}").into());
        }
        let definition = definitions
            .iter()
            .find(|definition| definition["id"] == id)
            .ok_or("unknown aggregate query identity")?;
        for key in ["sql", "ordered", "expected"] {
            if case[key] != definition[key] {
                return Err(format!("original aggregate {key} differs: {id} ANSI={ansi}").into());
            }
        }
        if case["status"] != "ok" || case["timezone"] != "UTC" {
            return Err(format!("incomplete aggregate capture: {id} ANSI={ansi}").into());
        }
        if case["stage_availability"] != availability {
            return Err(
                format!("incomplete aggregate stage availability: {id} ANSI={ansi}").into(),
            );
        }
        let stages = case["schema_stages"]
            .as_object()
            .ok_or("missing aggregate schema stages")?;
        if stages.len() != SCHEMA_STAGES.len()
            || SCHEMA_STAGES
                .iter()
                .any(|stage| !is_complete_aggregate_schema(&case["schema_stages"][*stage]))
            || !is_complete_aggregate_schema(&case["public_schema"])
            || case["expected"]["schema"] != case["public_schema"]
        {
            return Err(format!("incomplete aggregate schemas: {id} ANSI={ansi}").into());
        }
        let width = case["public_schema"]["fields"]
            .as_array()
            .ok_or("missing public aggregate fields")?
            .len();
        let rows = case["expected"]["rows"]
            .as_array()
            .ok_or("missing aggregate rows")?;
        if rows.iter().any(|row| {
            !row.as_array().is_some_and(|row| {
                row.len() == width && row.iter().all(|cell| cell.is_null() || cell.is_string())
            })
        }) {
            return Err(format!("malformed aggregate rows: {id} ANSI={ansi}").into());
        }
        let mut requested = header["requested_spark_sql_settings"].clone();
        let mut effective = header["effective_spark_sql_settings"].clone();
        requested["spark.sql.ansi.enabled"] = json!(ansi.to_string());
        effective["spark.sql.ansi.enabled"] = json!(ansi.to_string());
        if case["requested_spark_sql_settings"] != requested
            || case["effective_spark_sql_settings"] != effective
        {
            return Err(format!("aggregate settings differ: {id} ANSI={ansi}").into());
        }
    }
    if identities != expected_identities {
        return Err("aggregate query/ANSI identities differ from the original 36".into());
    }
    Ok(records)
}

#[test]
fn aggregate_fixture_rejects_missing_and_malformed_contracts() -> TestResult {
    let records = parse_aggregate_fixture(REFERENCE)?;
    let source = |records: &[Value]| {
        records
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    };
    for (object, key) in [
        ("/schema_stages", "analyzed"),
        ("/stage_availability", "public"),
        ("/expected", "rows"),
    ] {
        let mut malformed = records.clone();
        malformed[1]
            .pointer_mut(object)
            .and_then(Value::as_object_mut)
            .ok_or("missing mutation object")?
            .remove(key);
        assert!(
            parse_aggregate_fixture(&source(&malformed)).is_err(),
            "accepted missing {object}/{key}"
        );
    }
    for pointer in [
        "/schema_stages/executed/fields/0/nullable",
        "/expected/schema/fields/0/metadata",
    ] {
        let mut malformed = records.clone();
        *malformed[1]
            .pointer_mut(pointer)
            .ok_or("missing mutation field")? = Value::Null;
        assert!(
            parse_aggregate_fixture(&source(&malformed)).is_err(),
            "accepted malformed {pointer}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn aggregate_query_results_match_independent_spark() -> TestResult {
    let records = parse_aggregate_fixture(REFERENCE)?;
    let mut expected_contexts = BTreeSet::new();
    for id in CASE_IDS {
        for ansi in [true, false] {
            for batch_size in [1, 32] {
                expected_contexts.insert((id, ansi, batch_size, 2));
            }
        }
    }
    let mut completed = BTreeSet::new();
    let mut differences = Vec::new();
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
        for input in records[0]["inputs"].as_array().ok_or("missing inputs")? {
            let input_metadata = json!({"inputs": [input]});
            match input["name"].as_str() {
                Some("inputs") => {
                    spark_inputs::register_recorded_inputs(&session, &input_metadata, batch_size)
                }
                Some("empty_inputs") => {
                    spark_inputs::register_recorded_inputs_with_partition_count(
                        &session,
                        &input_metadata,
                        batch_size,
                        1,
                    )
                }
                other => return Err(format!("unexpected aggregate provider: {other:?}").into()),
            }?;
        }
        for case in &records[1..] {
            let id = case["id"].as_str().ok_or("missing ID")?;
            let ansi = case["ansi"].as_bool().ok_or("missing ANSI")?;
            assert!(
                completed.insert((id, ansi, batch_size, 2)),
                "duplicate context: {id} ANSI={ansi} batch={batch_size}"
            );
            let result = compare_exact_spark_query_stages_with_optimized(&session, case).await;
            let matches = result.as_ref().is_ok_and(|actual| {
                actual["matches"] == true
                    && actual["optimized_matches"] == true
                    && actual["public_schema"] == case["expected"]["schema"]
            });
            let actual = match result {
                Ok(actual) => actual,
                Err(error) => json!({"observer_error": error.to_string()}),
            };
            eprintln!(
                "AGGREGATE_QUERY_RESULTS_OBSERVATION {}",
                json!({
                    "id": id, "ansi": ansi, "batch_size": batch_size,
                    "target_partitions": 2,
                    "provider_partitions": {"inputs": 2, "empty_inputs": 1},
                    "observation": actual, "matches": matches,
                })
            );
            if !matches {
                differences.push(format!(
                    "{id} ANSI={ansi} batch={batch_size}: expected={case}, actual={actual}"
                ));
            }
        }
    }
    assert_eq!(completed, expected_contexts);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
