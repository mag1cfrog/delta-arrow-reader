//! STRUCT child names determine field access and serialized JSON keys.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_executed_query_result, remove_nullability_and_metadata};

fn records() -> TestResult<Vec<Value>> {
    include_str!("fixtures/struct-expression-names-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

#[tokio::test]
async fn struct_child_names_match_spark_values_and_executed_types() -> TestResult {
    let corpora = [
        (
            include_str!("fixtures/struct-expression-names-spark.jsonl"),
            192,
        ),
        (
            include_str!("fixtures/struct-identifier-names-spark.jsonl"),
            48,
        ),
        (
            include_str!("fixtures/struct-identifier-wildcard-spark.jsonl"),
            4,
        ),
        (
            include_str!("fixtures/struct-anonymous-wildcard-spark.jsonl"),
            8,
        ),
        (
            include_str!("fixtures/struct-selected-field-names-spark.jsonl"),
            234,
        ),
    ];
    let mut separate_json_replays = 0;
    let mut differences = Vec::new();
    for (source, count) in corpora {
        let records = source
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(records.len() - 1, count);
        let mut identities = BTreeSet::new();
        for case in &records[1..] {
            assert!(identities.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["input"].as_str().ok_or("missing input")?,
                case["ansi"].as_bool().ok_or("missing ANSI mode")?
            )));
        }
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
            register_recorded_inputs(&session, &records[0], batch_size)?;
            for case in &records[1..] {
                let expected = &case["expected"];
                if expected.get("schema").is_none() {
                    assert_eq!(expected["condition"], "FIELD_NOT_FOUND");
                    continue; // Spark-rejected queries are outside the valid-input contract.
                }
                if case["id"] == "null_json" {
                    // #1284 reproduces VOID NULL omission separately with NAMED_STRUCT.
                    separate_json_replays += 1;
                    continue;
                }
                let result = capture_executed_query_result(&session, case).await;
                println!(
                    "STRUCT_OBSERVATION {}",
                    json!({"id":case["id"],
                    "input":case["input"],"ansi":case["ansi"],"batch_size":batch_size,
                    "actual":result.as_ref().map_err(ToString::to_string)})
                );
                match result {
                    Ok(actual) => {
                        let types = |schema: &Value| -> TestResult<Vec<Value>> {
                            schema["fields"]
                                .as_array()
                                .ok_or("missing fields")?
                                .iter()
                                .map(|field| {
                                    let mut data_type = field["type"].clone();
                                    remove_nullability_and_metadata(&mut data_type);
                                    Ok(data_type)
                                })
                                .collect()
                        };
                        if actual["rows"] != expected["rows"]
                            || types(&actual["schema"])? != types(&expected["schema"])?
                        {
                            differences
                                .push(format!("case={case} batch={batch_size}: got {actual}"));
                        }
                    }
                    Err(error) => {
                        differences.push(format!("case={case} batch={batch_size}: {error}"))
                    }
                }
            }
        }
    }
    assert_eq!(separate_json_replays, 12);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_struct_results_are_unchanged() -> TestResult {
    let records = records()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &records[0], 2)?;
    }
    let mut expected = Vec::new();
    for sql in [
        "SELECT STRUCT(nested.item, v), NAMED_STRUCT('renamed', nested.item) AS s FROM struct_mixed",
        "SELECT STRUCT(nested.item, v), NAMED_STRUCT('renamed', nested.item) AS s FROM struct_empty",
    ] {
        let frame = native.sql(sql).await?;
        expected.push((sql, frame.schema().clone(), frame.collect().await?));
    }
    for after_spark in [false, true] {
        if after_spark {
            capture_executed_query_result(
                &configured,
                &json!({"ansi":true,
                    "sql":"SELECT STRUCT(nested.item AS renamed) AS s FROM struct_mixed"}),
            )
            .await?;
        }
        for (sql, schema, batches) in &expected {
            let actual = configured.sql(sql).await?;
            assert_eq!(actual.schema(), schema, "{sql}, after_spark={after_spark}");
            assert_eq!(
                &actual.collect().await?,
                batches,
                "{sql}, after_spark={after_spark}"
            );
        }
    }
    Ok(())
}
