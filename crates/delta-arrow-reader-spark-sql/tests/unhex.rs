//! UNHEX preserves bytes, NULLs and fields across string representations.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_result, register_inputs};

#[tokio::test]
async fn unhex_matches_spark() -> TestResult {
    let mut differences = Vec::new();
    let mut compared = 0;
    for (capture, count) in [(include_str!("fixtures/unhex-spark.jsonl"), 28)] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(records.len() - 1, count);
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
                    compared += 1;
                    let actual = capture_result(&session, case).await;
                    let observed = match &actual {
                        Ok(value) => value.clone(),
                        Err(error) => json!({"error":error.to_string()}),
                    };
                    eprintln!(
                        "c06 observation: {}",
                        json!({"id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,"actual":observed})
                    );
                    let expected = &case["expected"];
                    let matches = match expected["condition"].as_str() {
                        Some(condition) => actual.as_ref().is_err_and(|error| {
                            error.to_string().contains(&format!("[{condition}]"))
                        }),
                        None => actual.as_ref().is_ok_and(|value| {
                            value["analyzed_schema"] == expected["analyzed_schema"]
                                && value["physical_schema"] == expected["physical_schema"]
                                && value["rows"] == expected["rows"]
                        }),
                    };
                    if !matches {
                        differences.push(format!("{} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["id"],case["ansi"]));
                    }
                }
            }
        }
    }
    assert_eq!(compared, 28 * 6);
    eprintln!("C06 compared {compared}, mismatches {}", differences.len());
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn unhex_preserves_native_encoding_functions() -> TestResult {
    let native_sql = "SELECT decode('00FF61','hex') AS b,encode('Aé','hex') AS s";
    let plain = SessionContext::new();
    let expected = plain.sql(native_sql).await?.collect().await?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
    for ansi in [true, false] {
        let result = capture_result(
            &session,
            &json!({"sql":"SELECT unhex('00FF61') AS r","ansi":ansi}),
        )
        .await?;
        assert_eq!(result["rows"], json!([["00ff61"]]));
        assert_eq!(session.sql(native_sql).await?.collect().await?, expected);
    }
    Ok(())
}
