//! Nested NULL text and pivot names preserve Spark's values and SQL NULL validity.

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

async fn execute_nested_text_query(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            ..SparkSqlOptions::default()
        },
    )
    .await?;
    let plan = query.create_physical_plan(session).await?;
    let expected_fields = case["schema"]["fields"]
        .as_array()
        .ok_or("missing reference schema")?;
    assert_eq!(plan.schema().fields().len(), expected_fields.len());
    let names: Vec<_> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, query.column_names);
    for (index, field) in expected_fields.iter().enumerate() {
        let expected_type = match field["type"].as_str() {
            Some("string") => DataType::Utf8,
            Some("long") => DataType::Int64,
            other => return Err(format!("unexpected reference type: {other:?}").into()),
        };
        assert_eq!(plan.schema().field(index).data_type(), &expected_type);
    }
    let batches = collect(plan, session.task_ctx()).await?;
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for (index, column) in batch.columns().iter().enumerate() {
                values.push(if column.is_null(row) {
                    assert!(batch.schema().field(index).is_nullable());
                    assert!(query.logical_plan.schema().field(index).is_nullable());
                    Value::Null
                } else {
                    array_value_to_string(column.as_ref(), row)?.into()
                });
            }
            rows.push(values);
        }
    }
    Ok(json!({"names":names,"rows":rows}))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn match_spark_nested_null_text_and_pivot_names() -> TestResult {
    let records = include_str!("fixtures/nested-null-text-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len(), 49);
    let inputs: Vec<(i32, Option<String>)> = serde_json::from_value(records[0]["inputs"].clone())?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let config = SessionConfig::new()
            .with_batch_size(batch_size)
            .with_target_partitions(2);
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(config.clone()),
            )
            .build(),
        );
        let native = SessionContext::new_with_config(config);
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
        let input = RecordBatch::try_from_iter([
            ("k", Arc::new(keys.slice(1, inputs.len())) as ArrayRef),
            ("v", Arc::new(values.slice(1, inputs.len())) as ArrayRef),
        ])?;
        session.register_batch("nested_inputs", input.clone())?;
        native.register_batch("nested_inputs", input)?;
        for case in &records[1..] {
            let actual = execute_nested_text_query(&session, case).await?;
            let names: Vec<_> = case["schema"]["fields"]
                .as_array()
                .ok_or("missing fields")?
                .iter()
                .map(|f| f["name"].clone())
                .collect();
            let expected = json!({"names":names,"rows":case["rows"]});
            if actual != expected {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected}, got {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
        let sql = "SELECT CAST(v AS VARCHAR) AS r FROM nested_inputs ORDER BY k";
        assert_eq!(
            session.sql(sql).await?.collect().await?,
            native.sql(sql).await?.collect().await?
        );
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
