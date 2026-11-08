//! Exact NULL ROWS diagnostics shared by the original 754 and focused 1085 replays.

use serde_json::Value;

use super::spark_results::TestResult;

pub(crate) fn unsupported_null_bound_location(case: &Value) -> Option<&'static str> {
    let shape = case["coverage"]["shape"]
        .as_str()
        .or_else(|| case["coverage"]["probe"].as_str())?;
    match shape {
        "rows_lower_typed_null"
        | "core_lower_typed_null"
        | "pair_both_typed_int"
        | "expr_folded_int_null_lower" => Some("lower"),
        "rows_upper_typed_null" | "core_upper_typed_null" | "expr_folded_int_null_upper" => {
            Some("upper")
        }
        _ => None,
    }
}

pub(crate) fn expected_native_error(case: &Value) -> TestResult<String> {
    if let Some(location) = unsupported_null_bound_location(case) {
        return Ok(format!(
            "External error: not supported: NULL ROWS frame {location} bound"
        ));
    }
    let detail = match case["expected"]["condition"].as_str() {
        Some("DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE") => {
            let parameters = &case["spark_error_details"]["parameters"];
            let location = parameters["location"].as_str().ok_or("missing bound location")?;
            let source_type = parameters["exprType"].as_str().ok_or("missing source type")?;
            let expected_type = parameters["expectedType"].as_str().ok_or("missing expected type")?;
            if !matches!(location, "lower" | "upper") || expected_type != "\"INT\"" {
                return Err("unexpected NULL ROWS type parameters".into());
            }
            format!(
                "[DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE] The data type of the {location} bound {source_type} does not match the expected data type {expected_type}."
            )
        }
        Some("DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_INVALID_BOUND") => {
            "[DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_INVALID_BOUND] invalid UNBOUNDED frame boundary".into()
        }
        Some("_LEGACY_ERROR_TEMP_1036") => {
            "[WINDOW_FRAME_MISMATCH] ROW_NUMBER requires ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".into()
        }
        Some("_LEGACY_ERROR_TEMP_1035") => {
            "[WINDOW_OFFSET_FRAME_MISMATCH] lag frame must match its offset expression".into()
        }
        Some("WINDOW_FUNCTION_FRAME_NOT_ORDERED") => {
            "[WINDOW_FUNCTION_FRAME_NOT_ORDERED] NTH_VALUE requires ORDER BY in the window definition".into()
        }
        condition => return Err(format!("unclassified NULL ROWS reference: {condition:?}").into()),
    };
    Ok(format!("External error: invalid argument: {detail}"))
}

pub(crate) fn matches_contract(case: &Value, observation: &Value) -> TestResult<bool> {
    let error = expected_native_error(case)?;
    Ok(observation["native_phase"] == "resolve"
        && observation["error"] == error
        && observation["optimized_error"] == error
        && observation.get("observer_error").is_none()
        && observation.get("native_panic").is_none()
        && [
            "public_schema",
            "optimized_schema",
            "before_collect",
            "physical_schema",
            "optimized_observation",
            "rows",
            "rows_raw",
        ]
        .iter()
        .all(|field| observation.get(*field).is_some_and(Value::is_null)))
}
