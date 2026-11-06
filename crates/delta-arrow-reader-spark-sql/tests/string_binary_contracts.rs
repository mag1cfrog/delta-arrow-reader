//! Original cross-family C06 controls, preserving named unresolved outcomes.

use serde_json::{Value, json};

#[path = "support/string_binary_contracts.rs"]
mod contracts;
#[allow(
    dead_code,
    reason = "These tests use capture_observation; the shared module also exports capture_result."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use contracts::{check_fixture, error_is, invalid_utf8_error, metadata_fields_match};
use string_binary::TestResult;

#[allow(
    dead_code,
    reason = "The original contracts share the focused substring matchers."
)]
#[path = "support/substring.rs"]
mod substring;

fn alias_metadata_gap(case: &Value, actual: &Value) -> bool {
    let count = match case["id"].as_str() {
        Some("frozen_nulls_concat_ws" | "frozen_strings_unicode" | "frozen_strings_binary") => 2,
        Some("substr_alias" | "from_for" | "from_only" | "binary_substr") => 1,
        Some("derived_metadata_plain") => 4,
        _ => return false,
    };
    let metadata = vec![json!({}); count];
    actual.get("error").is_none()
        && actual["rows"] == case["expected"]["rows"]
        && ["analyzed_schema", "physical_schema"]
            .into_iter()
            .all(|stage| metadata_fields_match(&actual[stage], &case["expected"][stage], &metadata))
}

fn invalid_binary_outcome(
    case: &Value,
    actual: &Value,
    representation: usize,
    batch_size: usize,
) -> Option<&'static str> {
    let expected = &case["expected"];
    match case["id"].as_str()? {
        "unaliased_binary_names" => (actual["analyzed_schema"] == expected["analyzed_schema"]
            && actual["physical_schema"].is_null()
            && invalid_utf8_error(
                actual,
                "physical",
                "invalid utf-8 sequence of 1 bytes from index 1",
            ))
        .then_some("open_780_invalid_utf8"),
        "invalid_utf8_binary_roundtrip_literal" => (actual["analyzed_schema"]
            == expected["analyzed_schema"]
            && actual["physical_schema"].is_null()
            && invalid_utf8_error(
                actual,
                "physical",
                "invalid utf-8 sequence of 1 bytes from index 1",
            ))
        .then_some("open_780_invalid_utf8_roundtrip"),
        "invalid_utf8_binary_roundtrip_column"
        | "concat_binary_string"
        | "concat_string_binary" => {
            let offset = if case["id"] == "invalid_utf8_binary_roundtrip_column"
                && !(representation == 2 && batch_size == 1)
            {
                26
            } else {
                0
            };
            (actual["analyzed_schema"] == expected["analyzed_schema"]
                && actual["physical_schema"] == expected["physical_schema"]
                && invalid_utf8_error(
                    actual,
                    "execute",
                    &format!("invalid utf-8 sequence of 1 bytes from index {offset}"),
                ))
            .then_some("open_780_invalid_utf8_execution")
        }
        _ => None,
    }
}

fn arity_cause_matches(case: &Value, actual: &Value) -> bool {
    let (function, accepted, count, native_error) = match case["id"].as_str() {
        Some("admission_arity_substring_zero") => (
            "substring", "[2, 3]", "0",
            "resolve: External error: invalid argument: substr requires 2 or 3 arguments".to_owned(),
        ),
        Some("admission_arity_substring_one") => (
            "substring", "[2, 3]", "1",
            "resolve: External error: invalid argument: substr requires 2 or 3 arguments".to_owned(),
        ),
        Some("admission_arity_substring_extra") => (
            "substring", "[2, 3]", "4",
            "resolve: External error: invalid argument: substr requires 2 or 3 arguments".to_owned(),
        ),
        Some(id @ ("admission_arity_regexp_extract_zero"
            | "admission_arity_regexp_extract_one"
            | "admission_arity_regexp_extract_extra")) => {
            let (count, types) = match id {
                "admission_arity_regexp_extract_zero" => ("0", ""),
                "admission_arity_regexp_extract_one" => ("1", "Utf8"),
                _ => ("4", "Utf8, Utf8, Int32, Int32"),
            };
            ("regexp_extract", "[2, 3]", count, format!(
                "resolve: External error: error in DataFusion: Error during planning: Execution error: Function 'regexp_extract' user-defined coercion failed with: Execution error: Unsupported Data Type: Spark `regexp_extract` function expects Expected (STRING, STRING) or (STRING, STRING, INT), got {types}. No function matches the given name and argument types 'regexp_extract({types})'. You might need to add explicit type casts.\n\tCandidate functions:\n\tregexp_extract(UserDefined)"
            ))
        }
        Some("admission_arity_concat_ws_zero") => (
            "concat_ws", "> 0", "0",
            "resolve: External error: error in DataFusion: Error during planning: Execution error: Function 'spark_concat_ws' user-defined coercion failed with: Execution error: Spark `concat_ws` function requires 1 to 2147483647 arguments, got 0. No function matches the given name and argument types 'spark_concat_ws()'. You might need to add explicit type casts.\n\tCandidate functions:\n\tspark_concat_ws(UserDefined)".to_owned(),
        ),
        _ => return false,
    };
    case["expected"]["condition"] == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION"
        && case["expected"]["parameters"]["functionName"] == format!("`{function}`")
        && case["expected"]["parameters"]["expectedNum"] == accepted
        && case["expected"]["parameters"]["actualNum"] == count
        && actual["analyzed_schema"].is_null()
        && actual["physical_schema"].is_null()
        && (error_is(actual, "resolve", &native_error)
            || (actual["error_stage"] == "resolve"
                && actual.get("rows").is_none()
                && case["expected"]["message"].as_str().is_some_and(|message| {
                    actual["error"].as_str().is_some_and(|error| {
                        error.starts_with("resolve:") && error.ends_with(message)
                    })
                })))
}

fn open_contract_outcome(
    case: &Value,
    actual: &Value,
    representation: usize,
    batch_size: usize,
) -> Option<&'static str> {
    if alias_metadata_gap(case, actual) {
        Some("open_710_generated_alias_metadata")
    } else if let Some(status) = invalid_binary_outcome(case, actual, representation, batch_size) {
        Some(status)
    } else if arity_cause_matches(case, actual) {
        // These preserve meaningful admission causes, without claiming identical
        // Spark diagnostics or sharing #795's distinct pre-admission formatter bug.
        Some("arity_cause_only_151")
    } else if case["id"]
        .as_str()
        .is_some_and(|id| id.starts_with("admission_substring_"))
        && substring::matches_reference(case, actual)
    {
        Some("substring_argument_cause")
    } else {
        None
    }
}

#[tokio::test]
async fn original_string_binary_controls_retain_complete_outcomes() -> TestResult {
    check_fixture(
        include_str!("fixtures/string-binary-contracts-spark.jsonl"),
        66,
        open_contract_outcome,
    )
    .await
}

#[test]
fn contract_gap_profiles_reject_new_fields_values_and_causes() -> TestResult {
    let records = include_str!("fixtures/string-binary-contracts-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let case = records
        .iter()
        .find(|case| case["id"] == "from_for" && case["ansi"] == true)
        .ok_or("missing substring alias reference")?;
    let mut actual = json!({"analyzed_schema":case["expected"]["analyzed_schema"],
        "physical_schema":case["expected"]["physical_schema"],"rows":case["expected"]["rows"]});
    actual["analyzed_schema"]["fields"][0]["metadata"] = json!({});
    actual["physical_schema"]["fields"][0]["metadata"] = json!({});
    assert!(alias_metadata_gap(case, &actual));
    for property in ["name", "type", "nullable", "metadata"] {
        let mut changed = actual.clone();
        changed["analyzed_schema"]["fields"][0][property] = json!("unrelated");
        assert!(!alias_metadata_gap(case, &changed));
    }
    let mut changed = actual.clone();
    changed["physical_schema"]["fields"][0]["nullable"] = json!(true);
    assert!(!alias_metadata_gap(case, &changed));
    actual["rows"] = json!([["wrong"]]);
    assert!(!alias_metadata_gap(case, &actual));

    let case = records
        .iter()
        .find(|case| case["id"] == "admission_arity_substring_zero")
        .ok_or("missing arity reference")?;
    let mut actual = json!({"analyzed_schema":null,"physical_schema":null,
        "error_stage":"resolve","error":"resolve: External error: invalid argument: substr requires 2 or 3 arguments"});
    assert!(arity_cause_matches(case, &actual));
    actual["error"] =
        json!("resolve: External error: expected at least one value, but got an empty vector");
    assert!(!arity_cause_matches(case, &actual));

    let case = records
        .iter()
        .find(|case| {
            case["id"] == "admission_substring_bigint_length_above_int" && case["ansi"] == false
        })
        .ok_or("missing width reference")?;
    // Constructed counterfactual with the obsolete wide-length result, not a native capture.
    let mut actual = json!({"analyzed_schema":case["expected"]["analyzed_schema"],
        "physical_schema":case["expected"]["analyzed_schema"],"rows":[["abcdef"]]});
    assert!(!substring::matches_reference(case, &actual));
    assert!(substring::known_difference(case, &actual).is_none());
    assert!(open_contract_outcome(case, &actual, 0, 1).is_none());
    actual["rows"] = json!([["unrelated"]]);
    assert!(open_contract_outcome(case, &actual, 0, 1).is_none());
    Ok(())
}

#[test]
fn uncaptured_substring_errors_reject_the_review_counterexample() -> TestResult {
    let case = include_str!("fixtures/string-binary-contracts-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|case| {
            case["id"] == "admission_substring_bigint_length_above_int" && case["ansi"] == true
        })
        .ok_or("missing overflow reference")?;
    // This copies the review's synthetic predicate counterexample, not a native
    // capture or a schema constructed from Spark's expected fields.
    let reviewed = json!({
        "analyzed_schema":{"type":"struct","fields":[
            {"name":"r","type":"string","nullable":false,"metadata":{}}
        ]},
        "analyzed_arrow_schema":"actual DFSchema from completed planning",
        "physical_schema":{"type":"struct","fields":[
            {"name":"wrong_name","type":"binary","nullable":true,
                "metadata":{"unrelated":"metadata"}}
        ]},
        "physical_arrow_schema":"actual Arrow schema from completed physical planning",
        "error_stage":"execute",
        "error":"execute: [CAST_OVERFLOW] The value 2147483648L of the type \"BIGINT\" cannot be cast to \"INT\" due to an overflow."
    });
    assert!(!contracts::complete_match(&reviewed, &case["expected"]));
    assert!(open_contract_outcome(&case, &reviewed, 0, 1).is_none());
    let mut concrete_cause = reviewed.clone();
    concrete_cause["error"] =
        json!("execute: Arrow error: Cast error: Can't cast value 2147483648 to type Int32");
    assert!(!substring::matches_reference(&case, &concrete_cause));
    assert!(open_contract_outcome(&case, &concrete_cause, 0, 1).is_none());

    let observed_native_fields = json!({"type":"struct","fields":[
        {"name":"r","type":"string","nullable":false,"metadata":{}}
    ]});
    for (property, value) in [
        ("name", json!("wrong_name")),
        ("type", json!("binary")),
        ("nullable", json!(true)),
        ("metadata", json!({"unrelated":"metadata"})),
    ] {
        let mut changed = reviewed.clone();
        changed["physical_schema"] = observed_native_fields.clone();
        changed["physical_schema"]["fields"][0][property] = value;
        assert!(open_contract_outcome(&case, &changed, 0, 1).is_none());
    }
    for stage in ["resolve", "physical", "unknown"] {
        let mut changed = reviewed.clone();
        changed["physical_schema"] = observed_native_fields.clone();
        changed["error_stage"] = json!(stage);
        assert!(open_contract_outcome(&case, &changed, 0, 1).is_none());
    }
    let mut changed = reviewed;
    changed["physical_schema"] = observed_native_fields;
    changed["error"] = json!("execute: [UNRELATED] arbitrary failure");
    assert!(open_contract_outcome(&case, &changed, 0, 1).is_none());
    Ok(())
}
