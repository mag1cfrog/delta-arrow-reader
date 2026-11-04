//! Execute retained input-sort boundaries with real shared producers and native controls.

use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Volatility, create_udf};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

fn session(batch_size: usize, spark: bool) -> SessionContext {
    let builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(
            SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(1),
        );
    SessionContext::new_with_state(if spark {
        configure_spark_sql(builder).build()
    } else {
        builder.build()
    })
}

fn register(session: &SessionContext, batch_size: usize, keys: &[i32]) -> TestResult {
    let batch = RecordBatch::try_from_iter([
        ("k", Arc::new(Int32Array::from(keys.to_vec())) as ArrayRef),
        (
            "v",
            Arc::new(Int32Array::from_iter_values(
                keys.iter().map(|key| 10 + key),
            )) as ArrayRef,
        ),
    ])?;
    let chunks = (0..keys.len())
        .step_by(batch_size)
        .map(|offset| batch.slice(offset, batch_size.min(keys.len() - offset)))
        .collect::<Vec<_>>();
    session.register_table(
        "sort_values",
        Arc::new(MemTable::try_new(batch.schema(), vec![chunks])?),
    )?;
    Ok(())
}

fn count_nodes(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    usize::from(plan.name() == name)
        + plan
            .children()
            .iter()
            .map(|child| count_nodes(child, name))
            .sum::<usize>()
}

fn count_producer_expressions(plan: &Arc<dyn ExecutionPlan>) -> usize {
    plan.downcast_ref::<ProjectionExec>()
        .map_or(0, |projection| {
            projection
                .expr()
                .iter()
                .filter(|expression| expression.expr.to_string().contains("record_sorted_value"))
                .count()
        })
        + plan
            .children()
            .iter()
            .map(|child| count_producer_expressions(child))
            .sum::<usize>()
}

async fn rows(plan: Arc<dyn ExecutionPlan>, session: &SessionContext) -> TestResult<Value> {
    let schema = plan.schema();
    let mut rows = Vec::new();
    for batch in collect(plan, session.task_ctx()).await? {
        spark_results::validate_batch(schema.as_ref(), &batch)?;
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| spark_results::encode_value(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    Ok(json!(rows))
}

#[tokio::test]
async fn retained_input_sort_preserves_order_fields_and_once_only_shared_callbacks() -> TestResult {
    for batch_size in [1, 32] {
        for keys in [&[0, 1, 2, 3][..], &[3, 2, 1, 0][..]] {
            let session = session(batch_size, true);
            register(&session, batch_size, keys)?;
            let calls = Arc::new(Mutex::new(Vec::<Vec<(i32, i32)>>::new()));
            let callback_calls = calls.clone();
            session.register_udf(create_udf(
                "record_sorted_value",
                vec![DataType::Int32, DataType::Int32],
                DataType::Int32,
                Volatility::Immutable,
                Arc::new(move |args| {
                    let values = args[0].to_array(1)?;
                    let keys = args[1].to_array(1)?;
                    let values = values
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| DataFusionError::Internal("expected Int32 values".into()))?;
                    let keys = keys
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| DataFusionError::Internal("expected Int32 keys".into()))?;
                    if values.null_count() != 0
                        || keys.null_count() != 0
                        || values.len() != keys.len()
                    {
                        return Err(DataFusionError::Internal("invalid callback input".into()));
                    }
                    callback_calls
                        .lock()
                        .map_err(|_| {
                            DataFusionError::Internal("callback trace lock poisoned".into())
                        })?
                        .push(
                            keys.values()
                                .iter()
                                .copied()
                                .zip(values.values().iter().copied())
                                .collect(),
                        );
                    Ok(args[0].clone())
                }),
            ));
            let sql = "SELECT SLICE(shared_array, 1, 1) AS r, shared_array AS repeated_array, k \
                FROM (SELECT k, ARRAY(record_sorted_value(v, k)) AS shared_array \
                FROM sort_values ORDER BY k ASC NULLS LAST) AS renamed_input";
            let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
            let physical = query.create_physical_plan(&session).await?;
            let plan = displayable(physical.as_ref()).indent(true).to_string();
            assert_eq!(count_nodes(&physical, "InputSortBoundaryExec"), 1, "{plan}");
            assert_eq!(count_producer_expressions(&physical), 1, "{plan}");
            assert!(
                calls.lock().map_err(|_| "trace lock poisoned")?.is_empty(),
                "planning evaluated a callback"
            );
            let schema = physical.schema();
            assert_eq!(
                rows(physical, &session).await?,
                json!([
                    [["10"], ["10"], "0"],
                    [["11"], ["11"], "1"],
                    [["12"], ["12"], "2"],
                    [["13"], ["13"], "3"],
                ])
            );
            let expected = keys
                .chunks(batch_size)
                .map(|chunk| chunk.iter().map(|key| (*key, 10 + key)).collect::<Vec<_>>())
                .collect::<Vec<_>>();
            assert_eq!(*calls.lock().map_err(|_| "trace lock poisoned")?, expected);

            // The equivalent output sort keeps the same complete declarations.
            let outer_sql = "SELECT SLICE(shared_array, 1, 1) AS r, shared_array AS repeated_array, k \
                FROM (SELECT k, ARRAY(record_sorted_value(v, k)) AS shared_array \
                FROM sort_values) AS renamed_input ORDER BY k ASC NULLS LAST";
            let outer = plan_query(&session, outer_sql, &SparkSqlOptions::default())
                .await?
                .create_physical_plan(&session)
                .await?;
            assert_eq!(count_nodes(&outer, "InputSortBoundaryExec"), 0);
            assert_eq!(schema, outer.schema());
            assert_eq!(
                rows(outer, &session).await?,
                json!([
                    [["10"], ["10"], "0"],
                    [["11"], ["11"], "1"],
                    [["12"], ["12"], "2"],
                    [["13"], ["13"], "3"],
                ])
            );
            assert_eq!(
                *calls.lock().map_err(|_| "trace lock poisoned")?,
                [expected.clone(), expected].concat()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn configured_native_input_sort_keeps_the_plain_datafusion_path() -> TestResult {
    let sql = "SELECT shared_value AS r, shared_value AS repeated_value, k \
        FROM (SELECT k, CAST(v AS BIGINT) AS shared_value FROM sort_values \
        ORDER BY k DESC NULLS LAST) AS native_input";
    for batch_size in [1, 32] {
        for outer_order in ["", " ORDER BY k DESC NULLS LAST"] {
            let sql = format!("{sql}{outer_order}");
            let mut observations = Vec::new();
            for spark in [false, true] {
                let session = session(batch_size, spark);
                register(&session, batch_size, &[0, 1, 2, 3])?;
                let physical = session.sql(&sql).await?.create_physical_plan().await?;
                assert_eq!(count_nodes(&physical, "InputSortBoundaryExec"), 0);
                let schema = spark_results::capture_schema(physical.schema().fields(), None)?;
                observations.push((schema, rows(physical, &session).await?));
            }
            assert_eq!(observations[0], observations[1]);
            let mut actual_rows = observations[0]
                .1
                .as_array()
                .ok_or("expected native rows")?
                .clone();
            let expected_rows = if outer_order.is_empty() {
                // The subquery's ORDER BY does not request final output order.
                actual_rows.sort_by_key(Value::to_string);
                json!([
                    ["10", "10", "0"],
                    ["11", "11", "1"],
                    ["12", "12", "2"],
                    ["13", "13", "3"],
                ])
            } else {
                json!([
                    ["13", "13", "3"],
                    ["12", "12", "2"],
                    ["11", "11", "1"],
                    ["10", "10", "0"],
                ])
            };
            assert_eq!(Value::Array(actual_rows), expected_rows);
        }
    }
    Ok(())
}
