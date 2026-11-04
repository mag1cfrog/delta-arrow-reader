//! LIKE and ILIKE retain Spark's argument-count diagnostic at resolution.

use std::collections::HashSet;

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

#[tokio::test]
async fn like_function_arity_matches_spark_and_preserves_valid_calls() -> TestResult {
    let mut keys = HashSet::new();
    let mut differences = Vec::new();
    let mut arity_contexts = 0;
    let mut valid_contexts = 0;
    for (fixture, capture, ids, arity) in [
        (
            "like-escapes-spark.jsonl",
            include_str!("fixtures/like-escapes-spark.jsonl"),
            &[
                "admission_arity_like_zero",
                "admission_arity_like_one",
                "admission_arity_like_extra",
                "admission_arity_ilike_zero",
                "admission_arity_ilike_one",
                "admission_arity_ilike_extra",
            ][..],
            true,
        ),
        (
            "like-function-spark.jsonl",
            include_str!("fixtures/like-function-spark.jsonl"),
            &[
                "like_function_custom_percent",
                "like_function_uppercase_escape",
                "like_function_nulls_dynamic_plain",
                "like_function_empty_dynamic_plain",
                "ilike_function_custom_percent",
                "ilike_function_uppercase_escape",
                "ilike_function_nulls_dynamic_plain",
                "ilike_function_empty_dynamic_plain",
            ][..],
            false,
        ),
        (
            "like-limit-guard-nullif-spark.jsonl",
            include_str!("fixtures/like-limit-guard-nullif-spark.jsonl"),
            &[
                "like_limit_guard_nullif_provider_like_function_limit0",
                "like_limit_guard_nullif_provider_ilike_function_limit0",
                "like_limit_guard_nullif_one_row_like_function_limit0",
                "like_limit_guard_nullif_one_row_ilike_function_limit0",
            ][..],
            false,
        ),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        let header = &records[0];
        assert_eq!(header["spark_version"], "4.2.0");
        assert_eq!(
            header["spark_build"]["SPARK_REVISION"]["value"],
            "32f7299601108917fb01920a54e084595b7b3bf8"
        );
        eprintln!(
            "c06 arity reference: {}",
            json!({"fixture":fixture,"header":header})
        );
        let selected = records[1..]
            .iter()
            .filter(|case| case["id"].as_str().is_some_and(|id| ids.contains(&id)))
            .collect::<Vec<_>>();
        assert_eq!(selected.len(), ids.len() * 2);
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
                register_inputs(&session, header, representation, batch_size)?;
                for case in &selected {
                    assert!(keys.insert((
                        fixture,
                        case["id"].as_str().ok_or("missing ID")?.to_owned(),
                        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        representation,
                        batch_size,
                    )));
                    let expected = &case["expected"];
                    let actual = capture_observation(&session, case).await?;
                    let matches = if arity {
                        arity_contexts += 1;
                        assert_eq!(expected["status"], "error");
                        assert_eq!(expected["phase"], "sql");
                        assert_eq!(expected["condition"], "WRONG_NUM_ARGS.WITHOUT_SUGGESTION");
                        assert_eq!(expected["parameters"]["expectedNum"], "[2, 3]");
                        // Keep Spark's captured condition, function and counts; omit its help URL.
                        let (diagnostic, _) = expected["message"]
                            .as_str()
                            .and_then(|message| message.split_once(" Please,"))
                            .ok_or("missing captured Spark arity diagnostic")?;
                        actual
                            == json!({
                                "error":format!("resolve: External error: invalid argument: {diagnostic}"),
                                "error_stage":"resolve",
                                "analyzed_schema":null,
                                "physical_schema":null,
                                "analyzed_arrow_schema":null,
                                "physical_arrow_schema":null,
                            })
                    } else {
                        valid_contexts += 1;
                        expected["status"] == "ok"
                            && actual.get("error").is_none()
                            && actual.get("error_stage").is_none()
                            && actual["analyzed_schema"] == expected["analyzed_schema"]
                            && actual["physical_schema"] == expected["physical_schema"]
                            && actual["analyzed_arrow_schema"].is_string()
                            && actual["physical_arrow_schema"].is_string()
                            && actual["rows"] == expected["rows"]
                    };
                    eprintln!(
                        "c06 arity observation: {}",
                        json!({"fixture":fixture,"case":case,"representation":representation,
                            "batch_size":batch_size,"strict_match":matches,"actual":actual})
                    );
                    if !matches {
                        differences.push(format!(
                            "{fixture} {} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {actual}",
                            case["id"], case["ansi"],
                        ));
                    }
                }
            }
        }
    }
    assert_eq!(arity_contexts, 72);
    assert_eq!(valid_contexts, 144);
    assert_eq!(keys.len(), arity_contexts + valid_contexts);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
