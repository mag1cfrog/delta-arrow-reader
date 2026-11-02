//! String codepoint and binary byte lengths match independent Spark observations.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, capture_result, register_inputs};

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
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
    )
}

fn admission_error_matches(id: &str, observed: &Value, expected: &Value) -> bool {
    let (actual_count, cause) = match id {
        "admission_arity_length_zero" => ("0", "one value expected: []"),
        "admission_arity_length_extra" => (
            "2",
            "one value expected: [Literal(Utf8(\"a\"), None), Literal(Utf8(\"b\"), None)]",
        ),
        _ => return false,
    };
    expected["status"] == "error"
        && expected["phase"] == "sql"
        && expected["condition"] == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION"
        && expected["parameters"]["functionName"] == "`length`"
        && expected["parameters"]["expectedNum"] == "1"
        && expected["parameters"]["actualNum"] == actual_count
        && expected["sql_state"] == "42605"
        && expected.get("analyzed_schema").is_none()
        && expected.get("physical_schema").is_none()
        && observed["error_stage"] == "resolve"
        && observed["error"]
            == format!(
                "resolve: External error: error in DataFusion: Error during planning: {cause}"
            )
        && observed.get("analyzed_schema").is_some_and(Value::is_null)
        && observed.get("physical_schema").is_some_and(Value::is_null)
        && observed.get("rows").is_none()
}

#[tokio::test]
async fn string_length_matches_spark() -> TestResult {
    let mut differences = Vec::new();
    let mut exact = 0;
    let mut admission_errors = 0;
    for (capture, count) in [
        (include_str!("fixtures/length-spark.jsonl"), 126),
        (include_str!("fixtures/length-binary-spark.jsonl"), 28),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(records.len() - 1, count);
        let mut keys = BTreeSet::new();
        for case in &records[1..] {
            assert!(keys.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            )));
        }
        for representation in 0..3 {
            for batch_size in [1, 32] {
                let session = session(batch_size);
                register_inputs(&session, &records[0], representation, batch_size)?;
                for case in &records[1..] {
                    let id = case["id"].as_str().ok_or("missing ID")?;
                    let expected = &case["expected"];
                    let observed = capture_observation(&session, case).await?;
                    let status = if expected["status"] == "ok"
                        && observed.get("error").is_none()
                        && observed["analyzed_schema"] == expected["analyzed_schema"]
                        && observed["physical_schema"] == expected["physical_schema"]
                        && observed["rows"] == expected["rows"]
                    {
                        exact += 1;
                        "exact"
                    } else if admission_error_matches(id, &observed, expected) {
                        admission_errors += 1;
                        "required_admission_error"
                    } else {
                        differences.push(format!("{id} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["ansi"]));
                        "difference"
                    };
                    eprintln!(
                        "c06 observation: {}",
                        json!({"id":id,"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,"status":status,"actual":observed})
                    );
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    assert_eq!(exact, (122 + 28) * 6);
    assert_eq!(admission_errors, 4 * 6);
    eprintln!("C06 length: {exact} exact results, {admission_errors} required admission errors");
    Ok(())
}

#[tokio::test]
async fn string_length_preserves_native_functions() -> TestResult {
    let sql = "SELECT length('Aé😀') AS c,octet_length('Aé😀') AS b,concat('a',NULL) AS n";
    let expected = SessionContext::new().sql(sql).await?.collect().await?;
    let session = session(32);
    assert_eq!(session.sql(sql).await?.collect().await?, expected);
    for ansi in [true, false] {
        capture_result(&session, &json!({"sql":sql,"ansi":ansi})).await?;
        assert_eq!(session.sql(sql).await?.collect().await?, expected);
    }
    Ok(())
}
