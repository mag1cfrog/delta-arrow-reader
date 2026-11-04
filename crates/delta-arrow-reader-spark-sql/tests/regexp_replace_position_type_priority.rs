//! Position type admission precedes foldability, including discarded inputs.
use std::collections::HashSet;

use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[allow(
    dead_code,
    reason = "This suite uses the shared observer without its Result-based wrapper."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, register_inputs};

fn reference_matches(actual: &Value, expected: &Value, representation: usize) -> bool {
    if expected["status"] == "ok" {
        return actual.get("error").is_none()
            && actual.get("error_stage").is_none()
            && ["analyzed_schema", "physical_schema", "rows"]
                .into_iter()
                .all(|key| expected.get(key).is_some() && actual.get(key) == expected.get(key))
            && actual["analyzed_arrow_schema"].is_string()
            && actual["physical_arrow_schema"].is_string();
    }
    if expected["status"] != "error"
        || expected["phase"] != "sql"
        || actual["error_stage"] != "resolve"
        || actual.get("rows").is_some()
        || ![
            "analyzed_schema",
            "analyzed_arrow_schema",
            "physical_schema",
            "physical_arrow_schema",
        ]
        .into_iter()
        .all(|key| actual.get(key).is_some_and(Value::is_null))
    {
        return false;
    }
    let parameters = &expected["parameters"];
    let cause = match expected["condition"].as_str() {
        Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
            if parameters["paramIndex"] == "4th" && parameters["requiredType"] == "\"INT\"" =>
        {
            let data_type = match (
                parameters["inputType"].as_str(),
                parameters["inputSql"].as_str(),
            ) {
                (Some("\"BOOLEAN\""), Some("\"true\"" | "\"CAST(NULL AS BOOLEAN)\"" | "\"(k > 0)\"")) => {
                    DataType::Boolean
                }
                (Some("\"BINARY\""), Some("\"pb\"")) => [
                    DataType::Binary,
                    DataType::LargeBinary,
                    DataType::BinaryView,
                ][representation]
                    .clone(),
                (Some("\"BINARY\""), Some("\"X'32'\"" | "\"CAST(NULL AS BINARY)\"")) => {
                    DataType::Binary
                }
                _ => return false,
            };
            format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] regexp_replace position requires INT, got {data_type}"
            )
        }
        Some("DATATYPE_MISMATCH.NON_FOLDABLE_INPUT")
            if parameters["inputName"] == "`position`"
                && parameters["inputType"] == "\"INT\""
                && parameters["inputExpr"] == "\"k\"" =>
        {
            "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] regexp_replace position must be a foldable INT expression".to_owned()
        }
        _ => return false,
    };
    actual["error"] == format!("resolve: External error: invalid argument: {cause}")
}

fn check_error_comparison(actual: &Value, expected: &Value, representation: usize) -> TestResult {
    let error = actual["error"]
        .as_str()
        .ok_or("missing checked native error")?;
    for stage in ["physical", "execute"] {
        let mut fault = actual.clone();
        fault["error_stage"] = json!(stage);
        fault["error"] = json!(error.replacen("resolve: ", &format!("{stage}: "), 1));
        assert!(!reference_matches(&fault, expected, representation));
    }
    for key in [
        "analyzed_schema",
        "analyzed_arrow_schema",
        "physical_schema",
        "physical_arrow_schema",
    ] {
        let mut fault = actual.clone();
        fault
            .as_object_mut()
            .ok_or("missing native observation object")?
            .remove(key);
        assert!(!reference_matches(&fault, expected, representation));
    }
    let mut fault = actual.clone();
    fault["error"] = json!(error.replace("INT", "BIGINT"));
    assert!(!reference_matches(&fault, expected, representation));
    if expected["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" {
        fault["error"] = json!(error.replace(", got ", ", got Wrong"));
        assert!(!reference_matches(&fault, expected, representation));
    }
    fault["error"] = json!("resolve: unrelated error");
    assert!(!reference_matches(&fault, expected, representation));
    Ok(())
}

#[tokio::test]
async fn regexp_replace_position_type_precedes_foldability() -> TestResult {
    let records = include_str!("fixtures/regexp-replace-position-type-priority-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 33);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    let reference_keys = records[1..]
        .iter()
        .map(|case| (case["id"].as_str(), case["ansi"].as_bool()))
        .collect::<HashSet<_>>();
    assert_eq!(reference_keys.len(), 32);
    let mut seen = HashSet::new();
    let mut successes = 0;
    let mut errors = 0;
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
                assert!(seen.insert((
                    case["id"].as_str(),
                    case["ansi"].as_bool(),
                    representation,
                    batch_size,
                )));
                eprintln!(
                    "c06 position type priority observation: {}",
                    json!({"id":case["id"],"ansi":case["ansi"],"representation":representation,
                    "batch_size":batch_size,"sql":case["sql"],"expected":expected,"actual":actual,
                    "native_structured_error_parameters_available":false,"complete_raw_diagnostic_parity":"unverified"})
                );
                assert!(
                    reference_matches(&actual, expected, representation),
                    "id={} ANSI={} representation={representation} batch={batch_size}: {actual}",
                    case["id"],
                    case["ansi"]
                );
                if expected["status"] == "error" {
                    check_error_comparison(&actual, expected, representation)?;
                    errors += 1;
                } else {
                    successes += 1;
                }
            }
        }
    }
    assert_eq!(seen.len(), 192);
    assert_eq!(errors, 180);
    assert_eq!(successes, 12);
    Ok(())
}
