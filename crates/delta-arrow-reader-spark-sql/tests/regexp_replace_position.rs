//! Complete position references with measured native stages and explicit shared gaps.
use std::collections::HashSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, register_inputs};

fn field_comparison(actual: &Value, expected: &Value, key: &str) -> &'static str {
    match (
        actual.get(key).filter(|value| !value.is_null()),
        expected.get(key).filter(|value| !value.is_null()),
    ) {
        (Some(actual), Some(expected)) if actual == expected => "exact",
        (Some(_), Some(_)) => "difference",
        (Some(_), None) => "reference_unavailable",
        (None, Some(_)) => "actual_unavailable",
        (None, None) => "unavailable",
    }
}

fn error_boundary_matches(actual: &Value, expected: &Value) -> bool {
    if actual.get("rows").is_some() {
        return false;
    }
    let null_field = |key| actual.get(key).is_some_and(Value::is_null);
    match expected["phase"].as_str() {
        Some("sql") => {
            actual["error_stage"] == "resolve"
                && actual["error"]
                    .as_str()
                    .is_some_and(|error| error.starts_with("resolve: "))
                && [
                    "analyzed_schema",
                    "analyzed_arrow_schema",
                    "physical_schema",
                    "physical_arrow_schema",
                ]
                .into_iter()
                .all(null_field)
        }
        Some("optimized_schema") => {
            actual["error_stage"] == "physical"
                && actual["error"]
                    .as_str()
                    .is_some_and(|error| error.starts_with("physical: "))
                && actual
                    .get("analyzed_schema")
                    .is_some_and(|field| !field.is_null())
                && actual["analyzed_arrow_schema"].is_string()
                && ["physical_schema", "physical_arrow_schema"]
                    .into_iter()
                    .all(null_field)
        }
        _ => false,
    }
}

fn position_error_cause_matches(actual: &Value, expected: &Value) -> bool {
    let Some(error) = actual["error"].as_str() else {
        return false;
    };
    let parameters = &expected["parameters"];
    match expected["condition"].as_str() {
        Some("DATATYPE_MISMATCH.NON_FOLDABLE_INPUT") => {
            error.contains("[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT]")
                && error.ends_with("regexp_replace position must be a foldable INT expression")
        }
        Some("DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE") => {
            error.contains("[DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE]")
                && parameters["currentValue"].as_str().is_some_and(|value| {
                    error.ends_with(&format!(
                        "regexp_replace position must be in (0, 2147483647], got {value}"
                    ))
                })
        }
        Some("CAST_INVALID_INPUT") => {
            error.contains("[CAST_INVALID_INPUT]")
                && parameters["sourceType"] == "\"STRING\""
                && parameters["targetType"] == "\"INT\""
                && parameters["expression"].as_str().is_some_and(|value| {
                    error.ends_with(&format!(
                        "Arrow error: Cast error: Cannot cast string {value} to value of Int32 type"
                    ))
                })
        }
        Some("CAST_OVERFLOW") if parameters["targetType"] == "\"INT\"" => {
            let Some(value) = parameters["value"].as_str() else {
                return false;
            };
            match parameters["sourceType"].as_str() {
                Some("\"BIGINT\"") => error.ends_with(&format!(
                    "Arrow error: Cast error: Can't cast value {} to type Int32",
                    value.trim_end_matches('L')
                )),
                Some("\"DOUBLE\"") => {
                    let native = if value == "Infinity" { "inf" } else { value };
                    error.ends_with(&format!(
                        "Arrow error: Cast error: Can't cast value {native} to type Int32"
                    ))
                }
                Some(source) if source.starts_with("\"DECIMAL(") => {
                    error.contains("[CAST_OVERFLOW]")
                        && error.ends_with(&format!(
                            "Arrow error: Cast error: value of {} is out of range Int32",
                            value.trim_end_matches("BD")
                        ))
                }
                _ => false,
            }
        }
        _ => false,
    }
}

// Meaningful native cause equality and condition-identifier equality are separate
// facets. A missing condition identifier never becomes a strict Spark match.
fn reference_comparison(actual: &Value, expected: &Value) -> (bool, Option<bool>, bool) {
    if expected["status"] == "ok" {
        let matched = actual.get("error").is_none()
            && ["analyzed_schema", "physical_schema", "rows"]
                .into_iter()
                .all(|key| actual[key] == expected[key]);
        return (matched, None, matched);
    }
    let semantic = error_boundary_matches(actual, expected)
        && position_error_cause_matches(actual, expected)
        && ["analyzed_schema", "physical_schema"]
            .into_iter()
            .all(|key| {
                expected
                    .get(key)
                    .is_none_or(|field| actual.get(key) == Some(field))
            });
    let identifier = expected["condition"].as_str().is_some_and(|condition| {
        actual["error"]
            .as_str()
            .is_some_and(|error| error.contains(&format!("[{condition}]")))
    });
    (semantic, Some(identifier), semantic && identifier)
}

fn read_records(source: &str) -> TestResult<Vec<Value>> {
    Ok(source
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?)
}

#[test]
fn position_error_comparison_rejects_later_stages_and_missing_fields() -> TestResult {
    let references = read_records(include_str!(
        "fixtures/regexp-replace-position-int-spark.jsonl"
    ))?;
    let profiles = read_records(include_str!(
        "fixtures/regexp-replace-position-native.jsonl"
    ))?;
    let mut checked = 0;
    for profile in profiles
        .iter()
        .skip(1)
        .filter(|profile| profile["kind"] == "stage_control")
    {
        let case = references
            .iter()
            .skip(1)
            .find(|case| case["id"] == profile["id"] && case["ansi"] == profile["ansi"])
            .ok_or("missing stage-control reference")?;
        let actual = &profile["actual"];
        assert!(reference_comparison(actual, &case["expected"]).2);
        for stage in ["resolve", "physical", "execute"] {
            if actual["error_stage"] == stage {
                continue;
            }
            // These are synthetic faults applied to a measured native object.
            let mut fault = actual.clone();
            fault["error_stage"] = json!(stage);
            let suffix = fault["error"]
                .as_str()
                .ok_or("missing native error")?
                .split_once(": ")
                .ok_or("missing stage prefix")?
                .1
                .to_owned();
            fault["error"] = json!(format!("{stage}: {suffix}"));
            assert!(
                !reference_comparison(&fault, &case["expected"]).0,
                "accepted later-stage cause: {fault}"
            );
        }
        let mut missing = actual.clone();
        missing
            .as_object_mut()
            .ok_or("missing native object")?
            .remove("analyzed_schema");
        assert!(!reference_comparison(&missing, &case["expected"]).0);
        let mut wrong_target = actual.clone();
        wrong_target["error"] = json!(
            actual["error"]
                .as_str()
                .ok_or("missing error")?
                .replace("Int32", "Int64")
        );
        assert!(!reference_comparison(&wrong_target, &case["expected"]).0);
        checked += 1;
    }
    assert_eq!(checked, 2);
    Ok(())
}

#[tokio::test]
async fn regexp_replace_position_matches_complete_spark_references() -> TestResult {
    let records = read_records(include_str!(
        "fixtures/regexp-replace-position-int-spark.jsonl"
    ))?;
    let native = read_records(include_str!(
        "fixtures/regexp-replace-position-native.jsonl"
    ))?;
    assert_eq!(records.len(), 111);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(native.len(), 25);
    let profiles = &native[1..];
    let keys = profiles
        .iter()
        .map(|profile| (profile["id"].as_str(), profile["ansi"].as_bool()))
        .collect::<HashSet<_>>();
    assert_eq!(keys.len(), profiles.len());
    let mut observations = 0;
    let mut semantic_matches = 0;
    let mut strict_matches = 0;
    let mut differences = Vec::new();
    eprintln!(
        "c06 position regression header: {}",
        json!({"spark_capture_header":records[0],
        "native_profile_header":native[0],"native_representations":["Utf8","LargeUtf8","Utf8View"],
        "native_batch_sizes":[1,32],"native_target_partitions":2,"native_session_timezone":"UTC"})
    );
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
                let actual = capture_observation(&session, case).await?;
                let expected = &case["expected"];
                let (semantic, identifier, strict) = reference_comparison(&actual, expected);
                let profile = profiles
                    .iter()
                    .find(|profile| profile["id"] == case["id"] && profile["ansi"] == case["ansi"]);
                let preserved = profile.is_some_and(|profile| {
                    profile["sql"] == case["sql"]
                        && actual == profile["actual"]
                        && profile["semantic_match"] == semantic
                        && profile["diagnostic_identifier_match"] == json!(identifier)
                        && profile["strict_reference_match"] == strict
                });
                if !preserved && (profile.is_some() || !strict) {
                    differences.push(format!("id={} ANSI={} representation={representation} batch={batch_size}: {actual}", case["id"], case["ansi"]));
                }
                observations += 1;
                semantic_matches += usize::from(semantic);
                strict_matches += usize::from(strict);
                eprintln!(
                    "c06 position regression observation: {}",
                    json!({"id":case["id"],"ansi":case["ansi"],
                    "representation":representation,"batch_size":batch_size,"sql":case["sql"],"expected":expected,"actual":actual,
                    "semantic_match":semantic,"diagnostic_identifier_match":identifier,"strict_reference_match":strict,
                    "native_profile_preserved":preserved,"owner":profile.map(|value| &value["owner"]),
                    "schema_comparison":{"analyzed":field_comparison(&actual,expected,"analyzed_schema"),
                        "physical":field_comparison(&actual,expected,"physical_schema")}})
                );
            }
        }
    }
    assert_eq!(observations, 660);
    assert_eq!(semantic_matches, 588);
    assert_eq!(strict_matches, 528);
    eprintln!(
        "c06 position regression summary: {}",
        json!({"references":110,"observations":observations,
        "semantic_matches":semantic_matches,"semantic_differences":observations-semantic_matches,
        "strict_reference_matches":strict_matches,"strict_reference_differences":observations-strict_matches,
        "full_compatibility_pass":false,"complete_raw_diagnostic_parity":"unverified"})
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
