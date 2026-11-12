//! STRUCT UNION results retain positional values and executable result types.

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
use spark_results::{
    TestResult, capture_executed_query_result, encode_value, remove_nullability_and_metadata,
};

#[tokio::test]
async fn struct_unions_match_spark_values_and_executed_types() -> TestResult {
    let records = include_str!("fixtures/union-struct-planning/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 164);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
        )));
        assert!(case["rows"].is_array());
    }
    let mut differences = Vec::new();
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
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let result = capture_executed_query_result(&session, case).await;
            println!(
                "UNION_OBSERVATION {}",
                json!({"id":case["id"], "ansi":case["ansi"],
                "batch_size":batch_size,"actual":result.as_ref().map_err(ToString::to_string)})
            );
            match result {
                Ok(actual) => {
                    if case["id"]
                        .as_str()
                        .is_some_and(|id| id.starts_with("source_extension_direct_"))
                    {
                        for key in ["ARROW:extension:name", "ARROW:extension:metadata"] {
                            let expected = &case["schema"]["fields"][0]["metadata"][key];
                            assert!(expected.is_string());
                            assert_eq!(
                                &actual["schema"]["fields"][0]["metadata"][key], expected,
                                "UNION must retain its Arrow extension type metadata"
                            );
                        }
                    }
                    let types = |schema: &Value| -> TestResult<Vec<Value>> {
                        schema["fields"]
                            .as_array()
                            .ok_or("missing result fields")?
                            .iter()
                            .map(|field| {
                                let mut data_type = field["type"].clone();
                                remove_nullability_and_metadata(&mut data_type);
                                Ok(data_type)
                            })
                            .collect()
                    };
                    if actual["rows"] != case["rows"]
                        || types(&actual["schema"])? != types(&case["schema"])?
                    {
                        differences.push(format!(
                            "{} ANSI={} batch={batch_size}: expected {case}, got {actual}",
                            case["id"], case["ansi"]
                        ));
                    }
                }
                Err(error) => differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {error}",
                    case["id"], case["ansi"]
                )),
            }
        }
        let native = session
            .sql("SELECT named_struct('x', 1) AS r UNION ALL SELECT named_struct('x', 2) AS r")
            .await?
            .collect()
            .await?;
        let mut native_values = Vec::new();
        for batch in native {
            for row in 0..batch.num_rows() {
                native_values.push(encode_value(batch.column(0).as_ref(), row)?);
            }
        }
        native_values.sort_by_key(Value::to_string);
        assert_eq!(native_values, vec![json!(["1"]), json!(["2"])]);
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
