//! CSV record validity, quoting and option errors at the public Spark SQL boundary.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

async fn execute_csv_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let logical_schema = query.logical_plan.schema();
    assert_eq!(logical_schema.fields().len(), 1);
    assert_eq!(query.column_names, ["r"]);
    assert_eq!(logical_schema.field(0).data_type(), &DataType::Utf8);
    if case["expected"]["status"] == "ok" {
        assert_eq!(
            logical_schema.field(0).is_nullable(),
            case["expected"]["schema"]["fields"][0]["nullable"] == true
        );
    }
    let plan = query.create_physical_plan(session).await?;
    assert_eq!(plan.schema().field(0).name(), "r");
    assert_eq!(
        plan.schema().field(0).data_type(),
        logical_schema.field(0).data_type()
    );
    let physical_nullable = plan.schema().field(0).is_nullable();
    let batches = collect(plan, session.task_ctx()).await?;
    let mut rows = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        for row in 0..batch.num_rows() {
            rows.push(vec![if column.is_null(row) {
                assert!(physical_nullable && logical_schema.field(0).is_nullable());
                Value::Null
            } else {
                array_value_to_string(column.as_ref(), row)?.into()
            }]);
        }
    }
    Ok(json!(rows))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn match_spark_csv_record_validity() -> TestResult {
    let records = include_str!("fixtures/csv-records-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    let inputs: Vec<(i32, Option<String>)> = serde_json::from_value(records[0]["inputs"].clone())?;
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
        // Padding makes the registered input a real slice with nonzero Arrow offsets.
        let keys = Int32Array::from(
            std::iter::once(-1)
                .chain(inputs.iter().map(|r| r.0))
                .chain([99])
                .collect::<Vec<_>>(),
        );
        let values = StringArray::from(
            std::iter::once(Some("padding".to_string()))
                .chain(inputs.iter().map(|r| r.1.clone()))
                .chain([Some("padding".to_string())])
                .collect::<Vec<_>>(),
        );
        session.register_batch(
            "csv_inputs",
            RecordBatch::try_from_iter([
                ("k", Arc::new(keys.slice(1, inputs.len())) as ArrayRef),
                ("v", Arc::new(values.slice(1, inputs.len())) as ArrayRef),
            ])?,
        )?;
        for case in &records[1..] {
            let actual = execute_csv_query(&session, case).await;
            let expected = &case["expected"];
            let matches = match &actual {
                Ok(rows) => expected["status"] == "ok" && *rows == expected["rows"],
                Err(error) if expected["status"] == "error" => {
                    let message = error.to_string();
                    match case["id"].as_str() {
                        Some("invalid_quote") => {
                            message.contains("quote") && message.contains("one character")
                        }
                        Some("invalid_bool") => {
                            message.contains("quoteAll flag can be true or false")
                        }
                        _ => false,
                    }
                }
                _ => false,
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
