//! Original STRING/BINARY cast contracts, with precise open #710/#780 outcomes.

use serde_json::{Value, json};

#[path = "support/string_binary_contracts.rs"]
mod contracts;
#[allow(
    dead_code,
    reason = "These tests use capture_observation; the shared module also exports capture_result."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use contracts::{check_fixture, invalid_utf8_error, metadata_fields_match};
use string_binary::TestResult;

fn source_metadata(id: &str) -> Option<Value> {
    match id {
        "cast_string_binary_c06_strings_required"
        | "cast_string_binary_c06_strings_nullable"
        | "cast_string_binary_c06_strings_empty_required"
        | "cast_string_binary_c06_strings_empty_nullable" => {
            Some(json!({"source":"c06","column":"s"}))
        }
        "cast_binary_string_c06_strings_required"
        | "cast_binary_string_c06_strings_nullable"
        | "cast_binary_string_c06_strings_empty_required"
        | "cast_binary_string_c06_strings_empty_nullable" => {
            Some(json!({"source":"c06","column":"b"}))
        }
        _ => None,
    }
}

fn open_cast_outcome(
    case: &Value,
    actual: &Value,
    representation: usize,
    batch_size: usize,
) -> Option<&'static str> {
    let id = case["id"].as_str()?;
    let expected = &case["expected"];
    if let Some(metadata) = source_metadata(id) {
        let fields_match = ["analyzed_schema", "physical_schema"].iter().all(|stage| {
            metadata_fields_match(
                &actual[stage],
                &expected[stage],
                std::slice::from_ref(&metadata),
            )
        });
        if !fields_match {
            return None;
        }
        if actual.get("error").is_none() && actual["rows"] == expected["rows"] {
            return Some("open_710_cast_metadata");
        }
        if matches!(
            id,
            "cast_binary_string_c06_strings_required" | "cast_binary_string_c06_strings_nullable"
        ) {
            // The retained view path validates one value at a time at batch 1;
            // other paths validate the concatenated byte buffer. Both offsets
            // are pinned by the current native capture, not by displayed text.
            let offset = if representation == 2 && batch_size == 1 {
                0
            } else {
                26
            };
            if invalid_utf8_error(
                actual,
                "execute",
                &format!("invalid utf-8 sequence of 1 bytes from index {offset}"),
            ) {
                return Some("open_780_invalid_utf8_and_710_cast_metadata");
            }
        }
        return None;
    }
    let cause = match id {
        "cast_binary_string_nul_ff_a" => "invalid utf-8 sequence of 1 bytes from index 1",
        "cast_binary_string_continuation"
        | "cast_binary_string_bad_second_byte"
        | "cast_binary_string_surrogate"
        | "cast_binary_string_too_large" => "invalid utf-8 sequence of 1 bytes from index 0",
        "cast_binary_string_truncated" => "incomplete utf-8 byte sequence from index 0",
        _ => return None,
    };
    (actual["analyzed_schema"] == expected["analyzed_schema"]
        && actual["physical_schema"].is_null()
        && invalid_utf8_error(actual, "physical", cause))
    .then_some("open_780_invalid_utf8_literal_cast")
}

#[tokio::test]
async fn original_binary_casts_retain_complete_outcomes() -> TestResult {
    check_fixture(
        include_str!("fixtures/binary-casts-spark.jsonl"),
        62,
        open_cast_outcome,
    )
    .await
}

#[test]
fn binary_cast_gap_profiles_reject_unrelated_failures() -> TestResult {
    let records = include_str!("fixtures/binary-casts-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let case = records
        .iter()
        .find(|case| case["id"] == "cast_binary_string_nul_ff_a" && case["ansi"] == true)
        .ok_or("missing literal cast reference")?;
    let mut actual = json!({
        "analyzed_schema":case["expected"]["analyzed_schema"],
        "physical_schema":null,"error_stage":"physical",
        "error":"physical: Optimizer rule 'simplify_expressions' failed\ncaused by\nArrow error: Invalid argument error: Encountered non UTF-8 data: invalid utf-8 sequence of 1 bytes from index 1"
    });
    assert!(open_cast_outcome(case, &actual, 0, 1).is_some());
    actual["analyzed_schema"]["fields"][0]["nullable"] = json!(true);
    assert!(open_cast_outcome(case, &actual, 0, 1).is_none());
    actual["analyzed_schema"] = case["expected"]["analyzed_schema"].clone();
    for error in [
        "physical: CAST_INVALID_INPUT",
        "physical: Arrow error: Invalid argument error: Encountered non UTF-8 data: invalid utf-8 sequence of 1 bytes from index 0",
    ] {
        actual["error"] = json!(error);
        assert!(open_cast_outcome(case, &actual, 0, 1).is_none());
    }
    Ok(())
}
