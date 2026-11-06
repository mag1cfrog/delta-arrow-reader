//! Exact diagnostic checks shared by extraction regression consumers.
use serde_json::Value;

pub(crate) fn group_error_suffix_matches(
    error: &str,
    parameters: &Value,
    condition: Option<&str>,
) -> bool {
    if parameters["functionName"] != "`regexp_extract`" {
        return false;
    }
    match (
        parameters["groupCount"].as_str(),
        parameters["groupIndex"].as_str(),
    ) {
        (Some(count), Some(index)) => {
            let condition = condition.map_or(String::new(), |condition| format!("[{condition}] "));
            error.ends_with(&format!(
                "Spark `regexp_extract` function: {condition}Expects group index between 0 and {count}, but got {index}"
            ))
        }
        _ => false,
    }
}

pub(crate) fn error_cause(actual: &Value, expected: &Value) -> bool {
    let Some(error) = actual["error"].as_str() else {
        return false;
    };
    let parameters = &expected["parameters"];
    match expected["condition"].as_str() {
        Some("CAST_INVALID_INPUT") => {
            parameters["sourceType"] == "\"STRING\""
                && parameters["targetType"] == "\"INT\""
                && parameters["expression"].as_str().is_some_and(|value| {
                    error.ends_with(&format!(
                        "Arrow error: Cast error: Cannot cast string {value} to value of Int32 type"
                    ))
                })
        }
        Some("INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION") => parameters["sqlExpr"]
            == "\"CAST(CAST(bad AS INT) AS STRING)\""
            && error.split_once(": ").is_some_and(|(_, cause)| {
                cause
                    == concat!(
                        "External error: error in DataFusion: ",
                        "[INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION]\ncaused by\n",
                        "[CAST_INVALID_INPUT]\ncaused by\n",
                        "Arrow error: Cast error: Cannot cast string 'bad' to value of Int32 type"
                    )
            }),
        Some("CAST_OVERFLOW") if parameters["targetType"] == "\"INT\"" => {
            let Some(value) = parameters["value"].as_str() else {
                return false;
            };
            match parameters["sourceType"].as_str() {
                Some("\"BIGINT\"") => error.ends_with(&format!(
                    "Arrow error: Cast error: Can't cast value {} to type Int32",
                    value.trim_end_matches('L')
                )),
                Some("\"FLOAT\"" | "\"DOUBLE\"") => {
                    let native = if value == "Infinity" { "inf" } else { value };
                    error.ends_with(&format!(
                        "Arrow error: Cast error: Can't cast value {native} to type Int32"
                    ))
                }
                Some(source) if source.starts_with("\"DECIMAL(") => error.ends_with(&format!(
                    "Arrow error: Cast error: value of {} is out of range Int32",
                    value.trim_end_matches("BD")
                )),
                _ => false,
            }
        }
        Some("INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX") => {
            group_error_suffix_matches(error, parameters, None)
                || group_error_suffix_matches(error, parameters, expected["condition"].as_str())
        }
        _ => false,
    }
}

pub(crate) fn availability(actual: &Value, key: &str) -> &'static str {
    match actual.get(key) {
        None => "absent",
        Some(Value::Null) => "null",
        Some(_) => "available",
    }
}
