//! Membership results and required CAST failures match independent Spark results.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Int32Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn in_list_results_and_required_errors_match_spark() -> TestResult {
    let records = include_str!("fixtures/in-list-evaluation-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    let rows: Vec<[Option<i32>; 4]> = serde_json::from_value(records[0]["input_rows"].clone())?;
    let input = RecordBatch::try_from_iter(["k", "a", "b", "c"].into_iter().enumerate().map(
        |(i, name)| {
            (
                name,
                Arc::new(Int32Array::from_iter(rows.iter().map(|row| row[i]))) as ArrayRef,
            )
        },
    ))?;
    for (i, field) in input.schema().fields().iter().enumerate() {
        let expected = &records[0]["input_schema"]["fields"][i];
        assert_eq!(json!(field.name()), expected["name"]);
        assert_eq!(expected["type"], "integer");
        assert_eq!(json!(field.is_nullable()), expected["nullable"]);
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
        let batches = (0..input.num_rows())
            .step_by(batch_size)
            .map(|start| input.slice(start, batch_size.min(input.num_rows() - start)))
            .collect();
        session.register_table(
            "inputs",
            Arc::new(MemTable::try_new(input.schema(), vec![batches])?),
        )?;
        for case in &records[1..] {
            let outcome = execute_in_list_query(&session, case).await;
            let expected = &case["expected"];
            let matches = match &outcome {
                Ok(actual) if expected["status"] == "ok" => actual == &expected["rows"],
                Err(error) if expected["status"] == "error" => {
                    expected["condition"] == "CAST_INVALID_INPUT"
                        && error
                            .to_string()
                            .contains("Cannot cast string 'bad' to value of Int32 type")
                }
                _ => false,
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected}, got {outcome:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

async fn execute_in_list_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            session_timezone: "UTC".into(),
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    assert_eq!(query.column_names, ["r"]);
    assert_eq!(physical.schema().field(0).data_type(), &DataType::Boolean);
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        rows.extend(batch.column(0).as_boolean().iter().map(|value| vec![value]));
    }
    Ok(json!(rows))
}
