//! Complete index references. Native stage contracts and diagnostic identifiers are separate facets.
use std::collections::HashSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, register_inputs};

fn error_cause(actual: &Value, expected: &Value) -> bool {
    let Some(error) = actual["error"].as_str() else { return false; };
    let parameters = &expected["parameters"];
    match expected["condition"].as_str() {
        Some("CAST_INVALID_INPUT") => parameters["sourceType"] == "\"STRING\""
            && parameters["targetType"] == "\"INT\""
            && parameters["expression"].as_str().is_some_and(|value| {
                error.ends_with(&format!("Arrow error: Cast error: Cannot cast string {value} to value of Int32 type"))
            }),
        Some("CAST_OVERFLOW") if parameters["targetType"] == "\"INT\"" => {
            let Some(value) = parameters["value"].as_str() else { return false; };
            match parameters["sourceType"].as_str() {
                Some("\"BIGINT\"") => error.ends_with(&format!(
                    "Arrow error: Cast error: Can't cast value {} to type Int32", value.trim_end_matches('L'))),
                Some("\"FLOAT\"" | "\"DOUBLE\"") => {
                    let native = if value == "Infinity" { "inf" } else { value };
                    error.ends_with(&format!("Arrow error: Cast error: Can't cast value {native} to type Int32"))
                }
                Some(source) if source.starts_with("\"DECIMAL(") => error.ends_with(&format!(
                    "Arrow error: Cast error: value of {} is out of range Int32", value.trim_end_matches("BD"))),
                _ => false,
            }
        }
        Some("INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX") => parameters["functionName"] == "`regexp_extract`"
            && match (parameters["groupCount"].as_str(), parameters["groupIndex"].as_str()) {
                (Some(count), Some(index)) => error.ends_with(&format!(
                    "Spark `regexp_extract` function: Expects group index between 0 and {count}, but got {index}")),
                _ => false,
            },
        _ => false,
    }
}

fn availability(actual: &Value, key: &str) -> &'static str {
    match actual.get(key) {
        None => "absent",
        Some(Value::Null) => "null",
        Some(_) => "available",
    }
}

fn reviewed_error_boundary(actual: &Value, expected: &Value, contract: Option<&Value>) -> bool {
    let Some(contract) = contract.filter(|contract| contract["reviewed"] == true) else { return false; };
    let Some(stage) = contract["native_stage"].as_str().filter(|stage| matches!(*stage, "resolve" | "physical" | "execute")) else { return false; };
    contract["spark_phase"] == expected["phase"] && contract["condition"] == expected["condition"]
        && actual["error_stage"] == stage
        && actual["error"].as_str().is_some_and(|error| error.starts_with(&format!("{stage}: ")))
        && ["analyzed_schema", "analyzed_arrow_schema", "physical_schema", "physical_arrow_schema", "rows"]
            .into_iter().all(|key| contract["availability"][key] == availability(actual, key))
        && contract["evidence"].is_object() && contract["rationale"].is_string()
}

#[test]
fn extraction_index_cause_keeps_the_exact_value_type_and_function() {
    let expected = json!({"condition":"CAST_OVERFLOW","parameters":{
        "sourceType":"\"BIGINT\"","targetType":"\"INT\"","value":"4294967296L"}});
    let actual = json!({"error":"physical: Arrow error: Cast error: Can't cast value 4294967296 to type Int32"});
    assert!(error_cause(&actual, &expected));
    for message in ["Arrow error: Cast error: Can't cast value 4294967297 to type Int32",
        "Arrow error: Cast error: Can't cast value 4294967296 to type Int64", "arbitrary overflow"] {
        assert!(!error_cause(&json!({"error":message}), &expected));
    }
    let group = json!({"condition":"INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX","parameters":{
        "functionName":"`regexp_extract`","groupCount":"1","groupIndex":"-2147483648"}});
    assert!(error_cause(&json!({"error":"execute: Execution error: Spark `regexp_extract` function: Expects group index between 0 and 1, but got -2147483648"}), &group));
    assert!(!error_cause(&json!({"error":"execute: Execution error: Spark `regexp_extract_all` function: Expects group index between 0 and 1, but got -2147483648"}), &group));
    assert!(!reviewed_error_boundary(&actual, &expected, None));
}

#[tokio::test]
async fn regexp_extract_index_retains_all_complete_references() -> TestResult {
    let stages: Value = serde_json::from_str(include_str!("fixtures/regexp-extract-index-native-stages.json"))?;
    let contracts = stages["contracts"].as_array().ok_or("missing native contracts")?;
    let fixtures = [
        ("original_bigint", "regexp-extract-index-original-spark.jsonl", include_str!("fixtures/regexp-extract-index-original-spark.jsonl"), 10),
        ("expanded", "regexp-extract-index-expanded-spark.jsonl", include_str!("fixtures/regexp-extract-index-expanded-spark.jsonl"), 106),
        ("direct_cast", "regexp-extract-index-cast-controls-spark.jsonl", include_str!("fixtures/regexp-extract-index-cast-controls-spark.jsonl"), 10),
    ];
    let mut references = 0;
    let mut observations = 0;
    let mut capture_failures = 0;
    let mut strict_matches = 0;
    let mut differences = Vec::new();
    for (family, fixture, source, expected_count) in fixtures {
        let records = source.lines().map(serde_json::from_str::<Value>).collect::<Result<Vec<_>, _>>()?;
        let (header, cases) = records.split_first().ok_or("empty reference fixture")?;
        assert_eq!(header["spark_version"], "4.2.0");
        assert_eq!(cases.len(), expected_count);
        let mut identities = HashSet::new();
        for case in cases {
            assert!(identities.insert((case["id"].as_str(), case["ansi"].as_bool())), "duplicate reference");
        }
        references += cases.len();
        eprintln!("c06 index regression header: {}", json!({"family":family,"fixture":fixture,
            "spark_capture_header":header,"native_stage_contracts":stages,"native_representations":["Utf8","LargeUtf8","Utf8View"],
            "native_batch_sizes":[1,32],"native_target_partitions":2,"native_session_timezone":"UTC"}));
        for representation in 0..3 {
            for batch_size in [1,32] {
                let session = SessionContext::new_with_state(configure_spark_sql(
                    SessionStateBuilder::new().with_default_features().with_config(
                        SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2))).build());
                register_inputs(&session, header, representation, batch_size)?;
                for case in cases {
                    let session = session.clone();
                    let input = json!({"sql":case["sql"],"ansi":case["ansi"]});
                    let actual = match tokio::spawn(async move { capture_observation(&session, &input).await }).await {
                        Ok(Ok(actual)) => actual,
                        Ok(Err(error)) => { capture_failures += 1; json!({"capture_error":error.to_string()}) }
                        Err(error) => { capture_failures += 1; json!({"capture_task_error":error.to_string()}) }
                    };
                    let expected = &case["expected"];
                    let contract = contracts.iter().find(|contract| contract["fixture"] == fixture
                        && contract["id"] == case["id"] && contract["ansi"] == case["ansi"]
                        && contract["representation"] == representation && contract["batch_size"] == batch_size);
                    let fields = ["analyzed_schema", "physical_schema"].into_iter().all(|key| {
                        expected.get(key).is_none_or(|field| actual.get(key) == Some(field))
                    });
                    let cause = error_cause(&actual, expected);
                    let boundary = reviewed_error_boundary(&actual, expected, contract);
                    let identifier = expected["condition"].as_str().map(|condition| {
                        actual["error"].as_str().is_some_and(|error| error.contains(&format!("[{condition}]")))
                    });
                    let complete_fields = ["analyzed_schema", "analyzed_arrow_schema", "physical_schema", "physical_arrow_schema"]
                        .into_iter().all(|key| actual.get(key).is_some_and(|field| !field.is_null()));
                    let semantic = if expected["status"] == "ok" {
                        actual.get("error").is_none() && actual.get("capture_error").is_none() && actual.get("capture_task_error").is_none()
                            && complete_fields && fields && actual["rows"] == expected["rows"]
                    } else { fields && cause && boundary };
                    let strict = semantic && identifier != Some(false);
                    observations += 1;
                    strict_matches += usize::from(strict);
                    eprintln!("c06 index regression observation: {}", json!({"family":family,"fixture":fixture,
                        "id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,"sql":case["sql"],
                        "expected":expected,"actual":actual,"meaningful_cause_match":cause,"native_boundary_match":boundary,
                        "stage_review":if contract.is_some_and(|value| value["reviewed"] == true) {"reviewed"} else {"unreviewed"},"diagnostic_identifier_match":identifier,
                        "semantic_match":semantic,"strict_reference_match":strict,"owners":contract.map(|value| &value["owners"]),
                        "full_compatibility_pass":false}));
                    if !strict { differences.push(format!("{fixture}: {} ANSI={} representation={representation} batch={batch_size}: {actual}", case["id"],case["ansi"])); }
                }
            }
        }
    }
    assert_eq!(references, 126);
    assert_eq!(observations, 756);
    eprintln!("c06 index regression summary: {}", json!({"references":references,"observations":observations,
        "capture_failures":capture_failures,"strict_matches":strict_matches,"strict_differences":differences.len(),
        "full_compatibility_pass":false,"complete_raw_diagnostic_parity":"unverified"}));
    assert_eq!(capture_failures, 0);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
