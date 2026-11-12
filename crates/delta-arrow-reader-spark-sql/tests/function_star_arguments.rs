//! Function arguments expand visible columns without changing COUNT(*) semantics.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_schema, encode_value};

fn remove_nullability_and_metadata(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for key in ["nullable", "metadata", "containsNull", "valueContainsNull"] {
                object.remove(key);
            }
            object
                .values_mut()
                .for_each(remove_nullability_and_metadata);
        }
        Value::Array(values) => values.iter_mut().for_each(remove_nullability_and_metadata),
        _ => {}
    }
}

async fn observe_query_result(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let schema = capture_schema(physical.schema().fields(), None)?;
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| encode_value(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    rows.sort_by_key(|row| json!(row).to_string());
    Ok(json!({"schema":schema,"rows":rows}))
}

#[tokio::test]
async fn function_argument_stars_match_spark() -> TestResult {
    let records = include_str!("fixtures/function-star-arguments/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 82);
    let mut identities = BTreeSet::new();
    for case in records.iter().skip(1) {
        assert!(case["rows"].is_array());
        assert_eq!(
            case["schema"]["fields"]
                .as_array()
                .ok_or("missing fields")?
                .len(),
            1
        );
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?
        )));
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
                            .with_target_partitions(1),
                    ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in records.iter().skip(1) {
            let observation = observe_query_result(&session, case).await;
            println!(
                "STAR_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],
                "batch_size":batch_size,"actual":observation.as_ref().map_err(ToString::to_string)})
            );
            match observation {
                Ok(actual) => {
                    let mut actual_type = actual["schema"]["fields"][0]["type"].clone();
                    let mut expected_type = case["schema"]["fields"][0]["type"].clone();
                    remove_nullability_and_metadata(&mut actual_type);
                    remove_nullability_and_metadata(&mut expected_type);
                    if actual["rows"] != case["rows"] || actual_type != expected_type {
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
            .sql("SELECT count(*) AS n FROM numbers")
            .await?
            .collect()
            .await?;
        assert_eq!(encode_value(native[0].column(0).as_ref(), 0)?, json!("4"));
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
