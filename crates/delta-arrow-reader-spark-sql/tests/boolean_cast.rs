//! Spark boolean parsing accepts whole tokens and trims ASCII control bytes.

use std::error::Error;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

async fn execute_boolean_cast(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let options = SparkSqlOptions {
        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        ..SparkSqlOptions::default()
    };
    let query = plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &options,
    )
    .await?;
    let logical = query.logical_plan.schema().field(0);
    assert_eq!(query.logical_plan.schema().fields().len(), 1);
    assert_eq!(logical.data_type(), &DataType::Boolean);
    assert_eq!(query.column_names, ["r"]);
    let plan = query.create_physical_plan(session).await?;
    let schema = plan.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
    assert_eq!(schema.field(0).name(), "r");
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        for value in batch.column(0).as_boolean() {
            if value.is_none() {
                assert!(logical.is_nullable());
                assert!(schema.field(0).is_nullable());
            }
            rows.push(json!([value]));
        }
    }
    Ok(json!(rows))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn string_boolean_casts_match_spark() -> TestResult {
    let records = include_str!("fixtures/boolean-cast-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 345);
    assert_eq!(records[0]["spark_version"], "4.2.0");
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
        for case in &records[1..] {
            let expected = &case["expected"];
            let actual = execute_boolean_cast(&session, case).await;
            let matches = match &actual {
                Ok(rows) => expected["status"] == "ok" && rows == &expected["rows"],
                Err(error) => {
                    expected["condition"] == "CAST_INVALID_INPUT"
                        && error.to_string().contains("[CAST_INVALID_INPUT]")
                }
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {expected}, got {actual:?}",
                    case["id"], case["ansi"],
                ));
            }
        }
    }
    let native = SessionContext::new();
    let batches = native.sql("SELECT CAST('on' AS BOOLEAN), TRY_CAST('tr' AS BOOLEAN), CAST(2 AS BOOLEAN), CAST(0 AS BOOLEAN), CAST(NULL AS BOOLEAN)")
        .await?.collect().await?;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() == 1)
        .ok_or("missing native row")?;
    assert_eq!(batch.num_columns(), 5);
    for (column, expected) in
        batch
            .columns()
            .iter()
            .zip([Some(true), Some(true), Some(true), Some(false), None])
    {
        assert_eq!(column.data_type(), &DataType::Boolean);
        assert_eq!(column.as_boolean().iter().collect::<Vec<_>>(), [expected]);
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
