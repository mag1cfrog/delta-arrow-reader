//! Shared observation checks for the remaining original C06 contracts.

use std::collections::{BTreeMap, HashSet};

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

use crate::string_binary::{TestResult, capture_observation, register_inputs};

pub(crate) fn complete_match(actual: &Value, expected: &Value) -> bool {
    expected["status"] == "ok"
        && actual.get("error").is_none()
        && actual["analyzed_schema"] == expected["analyzed_schema"]
        && actual["physical_schema"] == expected["physical_schema"]
        && actual["rows"] == expected["rows"]
}

// Each field must retain every property. Only the explicitly supplied, observed
// metadata alternative is allowed alongside Spark's metadata; no fields are edited.
pub(crate) fn metadata_fields_match(
    actual: &Value,
    expected: &Value,
    observed_metadata: &[Value],
) -> bool {
    let (Some(actual_fields), Some(expected_fields)) =
        (actual["fields"].as_array(), expected["fields"].as_array())
    else {
        return false;
    };
    actual.as_object().is_some_and(|object| object.len() == 2)
        && actual["type"] == expected["type"]
        && actual_fields.len() == expected_fields.len()
        && actual_fields.len() == observed_metadata.len()
        && actual_fields
            .iter()
            .zip(expected_fields)
            .zip(observed_metadata)
            .all(|((actual, expected), observed)| {
                actual.as_object().is_some_and(|object| object.len() == 4)
                    && actual["name"] == expected["name"]
                    && actual["type"] == expected["type"]
                    && actual["nullable"] == expected["nullable"]
                    && (actual["metadata"] == expected["metadata"]
                        || actual["metadata"] == *observed)
            })
}

pub(crate) fn error_is(actual: &Value, stage: &str, error: &str) -> bool {
    actual["error_stage"] == stage && actual["error"] == error && actual.get("rows").is_none()
}

fn completed_stages_are_retained(actual: &Value) -> bool {
    let analyzed =
        actual["analyzed_schema"].is_object() && actual["analyzed_arrow_schema"].is_string();
    let physical =
        actual["physical_schema"].is_object() && actual["physical_arrow_schema"].is_string();
    let no_analyzed =
        actual["analyzed_schema"].is_null() && actual["analyzed_arrow_schema"].is_null();
    let no_physical =
        actual["physical_schema"].is_null() && actual["physical_arrow_schema"].is_null();
    match actual["error_stage"].as_str() {
        Some("resolve") => no_analyzed && no_physical && actual.get("rows").is_none(),
        Some("physical") => analyzed && no_physical && actual.get("rows").is_none(),
        Some("execute") => analyzed && physical && actual.get("rows").is_none(),
        None => analyzed && physical && actual["rows"].is_array(),
        Some(_) => false,
    }
}

pub(crate) async fn check_fixture(
    capture: &str,
    expected_count: usize,
    classify_open: fn(&Value, &Value, usize, usize) -> Option<&'static str>,
) -> TestResult {
    let records = capture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, expected_count);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    let mut keys = HashSet::new();
    let mut counts = BTreeMap::new();
    let mut differences = Vec::new();
    for representation in 0..3 {
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
            register_inputs(&session, &records[0], representation, batch_size)?;
            for case in &records[1..] {
                assert!(keys.insert((
                    case["id"].as_str().ok_or("missing ID")?.to_owned(),
                    case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    representation,
                    batch_size,
                )));
                let actual = capture_observation(&session, case).await?;
                let status = if !completed_stages_are_retained(&actual) {
                    "missing_completed_stage"
                } else if complete_match(&actual, &case["expected"]) {
                    "full_spark_match"
                } else {
                    classify_open(case, &actual, representation, batch_size)
                        .unwrap_or("unexpected_difference")
                };
                *counts.entry(status).or_insert(0usize) += 1;
                eprintln!(
                    "c06 contracts observation: {}",
                    json!({"id":case["id"],"sql":case["sql"],"ansi":case["ansi"],
                        "representation":representation,"batch_size":batch_size,
                        "status":status,"actual":actual})
                );
                if matches!(status, "missing_completed_stage" | "unexpected_difference") {
                    differences.push(format!(
                        "{} ANSI={} representation={representation} batch={batch_size}: expected {}, observed {actual}",
                        case["id"], case["ansi"], case["expected"],
                    ));
                }
            }
        }
    }
    assert_eq!(keys.len(), expected_count * 6);
    eprintln!(
        "C06 contracts retained {} observations: {counts:?}; unexpected {}",
        keys.len(),
        differences.len()
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

pub(crate) fn invalid_utf8_error(actual: &Value, stage: &str, cause: &str) -> bool {
    let prefix = match stage {
        "physical" => "physical: Optimizer rule 'simplify_expressions' failed\ncaused by\n",
        "execute" => "execute: ",
        _ => return false,
    };
    error_is(
        actual,
        stage,
        &format!(
            "{prefix}Arrow error: Invalid argument error: Encountered non UTF-8 data: {cause}"
        ),
    )
}
