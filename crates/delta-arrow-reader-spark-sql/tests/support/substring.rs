//! Exact Spark comparisons and separately retained, measured substring differences.

use serde_json::Value;

use crate::contracts::{completed_stages_are_retained, error_is, invalid_utf8_error};

pub(crate) fn meaningful_error(case: &Value, error: &str) -> bool {
    let parameters = &case["expected"]["parameters"];
    match case["expected"]["condition"].as_str() {
        Some("CAST_OVERFLOW") if parameters["targetType"] == "\"INT\"" => parameters["value"]
            .as_str()
            .map(|value| value.trim_end_matches('L'))
            .is_some_and(|value| {
                error.contains(&format!("Can't cast value {value} to type Int32"))
                    || error.contains(&format!("value of {value} is out of range Int32"))
            }),
        Some("CAST_INVALID_INPUT") if parameters["targetType"] == "\"INT\"" => {
            parameters["expression"].as_str().is_some_and(|value| {
                error.contains(&format!(
                    "Cannot cast string {value} to value of Int32 type"
                ))
            })
        }
        Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION")
            if parameters["functionName"] == "`substring`"
                && parameters["expectedNum"] == "[2, 3]" =>
        {
            let count = match case["id"].as_str() {
                Some("admission_arity_substring_zero") => "0",
                Some("admission_arity_substring_one") => "1",
                Some("admission_arity_substring_extra") => "4",
                _ => return false,
            };
            parameters["actualNum"] == count
                && error
                    == "resolve: External error: invalid argument: substr requires 2 or 3 arguments"
        }
        Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
            if parameters["inputType"] == "\"BOOLEAN\""
                && parameters["requiredType"] == "\"INT\"" =>
        {
            let parameter = match parameters["paramIndex"].as_str() {
                Some("second") => "position",
                Some("third") => "length",
                _ => return false,
            };
            error
                == format!(
                    "resolve: External error: invalid argument: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] substring {parameter} requires INT, got Boolean"
                )
        }
        Some("DIVIDE_BY_ZERO") => error == "execute: Arrow error: Divide by zero error",
        _ => false,
    }
}

pub(crate) fn matches_reference(case: &Value, actual: &Value) -> bool {
    let expected = &case["expected"];
    match expected["status"].as_str() {
        Some("ok") => {
            actual.get("error").is_none()
                && ["analyzed_schema", "physical_schema", "rows"]
                    .into_iter()
                    .all(|key| actual[key] == expected[key])
        }
        Some("error") => {
            ["analyzed_schema", "physical_schema"]
                .into_iter()
                .all(|key| {
                    expected
                        .get(key)
                        .is_none_or(|schema| actual[key] == *schema)
                })
                && available_error_fields_are_sound(actual)
                && actual["error"]
                    .as_str()
                    .is_some_and(|error| meaningful_error(case, error))
        }
        _ => false,
    }
}

fn available_error_fields_are_sound(actual: &Value) -> bool {
    if actual["physical_schema"].is_null() {
        return true;
    }
    let (Some(analyzed), Some(physical)) = (
        actual["analyzed_schema"]["fields"].as_array(),
        actual["physical_schema"]["fields"].as_array(),
    ) else {
        return false;
    };
    actual["analyzed_schema"]["type"] == actual["physical_schema"]["type"]
        && analyzed.len() == physical.len()
        && analyzed.iter().zip(physical).all(|(analyzed, physical)| {
            ["name", "type", "metadata"]
                .into_iter()
                .all(|key| analyzed[key] == physical[key])
                && physical["nullable"]
                    .as_bool()
                    .is_some_and(|nullable| !nullable || analyzed["nullable"] == true)
        })
}

// The raw receipt comparison above keeps all measured failures. These four
// literal controls separately establish the same concrete numeric overflow
// despite their observed Infinity/inf or Decimal BD diagnostic spelling.
pub(crate) fn matches_meaningful_contract(case: &Value, actual: &Value) -> bool {
    if !completed_stages_are_retained(actual) {
        return false;
    }
    if matches_reference(case, actual) {
        return true;
    }
    let sql = match case["id"].as_str() {
        Some("double_infinity_direct_cast") => {
            "SELECT CAST(CAST('Infinity' AS DOUBLE) AS INT) AS r"
        }
        Some("double_infinity_substring") => {
            "SELECT substring('abcdef',CAST('Infinity' AS DOUBLE),2) AS r"
        }
        Some("decimal_overflow_direct_cast") => {
            "SELECT CAST(CAST(2147483648 AS DECIMAL(10,0)) AS INT) AS r"
        }
        Some("decimal_overflow_substring") => {
            "SELECT substring('abcdef',CAST(2147483648 AS DECIMAL(10,0)),2) AS r"
        }
        _ => return false,
    };
    case["sql"] == sql
        && case["ansi"] == true
        && matches!(
            known_difference(case, actual),
            Some("strict_172_infinity_literal_notation" | "strict_decimal_literal_notation")
        )
}

// Check the observed native field itself; never alter a reference or an actual schema.
fn single_field(schema: &Value, kind: &str, nullable: bool) -> bool {
    schema.as_object().is_some_and(|object| object.len() == 2)
        && schema["type"] == "struct"
        && schema["fields"].as_array().is_some_and(|fields| {
            matches!(fields.as_slice(), [field] if field.as_object().is_some_and(|object| object.len() == 4)
                && field["name"] == "r" && field["type"] == kind && field["nullable"] == nullable
                && field["metadata"].as_object().is_some_and(|metadata| metadata.is_empty()))
        })
}

pub(crate) fn known_difference(case: &Value, actual: &Value) -> Option<&'static str> {
    let id = case["id"].as_str()?;
    let ansi = case["ansi"].as_bool()?;
    let expected = &case["expected"];
    if id == "unaliased_binary_names"
        && expected["status"] == "ok"
        && actual["analyzed_schema"] == expected["analyzed_schema"]
        && actual["physical_schema"].is_null()
        && invalid_utf8_error(
            actual,
            "physical",
            "invalid utf-8 sequence of 1 bytes from index 1",
        )
    {
        return Some("open_780_invalid_utf8");
    }
    if matches!(
        id,
        "double_infinity_direct_cast" | "double_infinity_substring"
    ) && ansi
    {
        let direct = id.ends_with("direct_cast");
        let kind = if direct { "integer" } else { "string" };
        let value = "inf";
        let expected_matches = expected["condition"] == "CAST_OVERFLOW"
            && expected["parameters"]["value"] == "Infinity"
            && expected["parameters"]["sourceType"] == "\"DOUBLE\""
            && expected["parameters"]["targetType"] == "\"INT\""
            && expected["parameters"]["ansiConfig"] == "\"spark.sql.ansi.enabled\"";
        let (stage, error) = if direct {
            (
                "physical",
                format!(
                    "physical: Optimizer rule 'simplify_expressions' failed\ncaused by\nArrow error: Cast error: Can't cast value {value} to type Int32"
                ),
            )
        } else {
            (
                "execute",
                format!("execute: Arrow error: Cast error: Can't cast value {value} to type Int32"),
            )
        };
        let physical_matches = if direct {
            actual["physical_schema"].is_null()
        } else {
            single_field(&actual["physical_schema"], "string", true)
        };
        if expected_matches
            && single_field(&expected["analyzed_schema"], kind, true)
            && actual["analyzed_schema"] == expected["analyzed_schema"]
            && physical_matches
            && error_is(actual, stage, &error)
        {
            return Some("strict_172_infinity_literal_notation");
        }
    }
    if matches!(
        id,
        "decimal_overflow_direct_cast" | "decimal_overflow_substring"
    ) && ansi
        && expected["condition"] == "CAST_OVERFLOW"
        && expected["parameters"]["value"] == "2147483648BD"
        && expected["parameters"]["sourceType"] == "\"DECIMAL(10,0)\""
        && expected["parameters"]["targetType"] == "\"INT\""
        && expected["parameters"]["ansiConfig"] == "\"spark.sql.ansi.enabled\""
        && actual["analyzed_schema"] == expected["analyzed_schema"]
        && actual["physical_schema"].is_null()
        && error_is(
            actual,
            "physical",
            "physical: preserve_spark_expression_errors\ncaused by\n[CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: value of 2147483648 is out of range Int32",
        )
    {
        return Some("strict_decimal_literal_notation");
    }
    None
}

pub(crate) fn known_first_error_difference(
    case: &Value,
    actual: &Value,
    batch_size: usize,
) -> bool {
    case["id"] == "first_error_forward"
        && case["ansi"] == true
        && batch_size == 32
        && case["expected"]["condition"] == "DIVIDE_BY_ZERO"
        && actual["analyzed_schema"] == case["expected"]["analyzed_schema"]
        && single_field(&actual["physical_schema"], "string", true)
        && error_is(
            actual,
            "execute",
            "execute: [CAST_INVALID_INPUT]\ncaused by\nArrow error: Cast error: Cannot cast string 'bad-pos' to value of Int32 type",
        )
}
