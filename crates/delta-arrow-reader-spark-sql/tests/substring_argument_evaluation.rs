//! Substring evaluates each required operand once and skips NULL-discarded rows.

use std::sync::{Arc, Mutex};

use arrow::array::Int32Array;
use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::Value;

#[allow(
    dead_code,
    reason = "This test uses input registration from the shared observer."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, register_inputs};

async fn check_counted(
    session: &SessionContext,
    sql: &str,
    ansi_mode: bool,
    observed: &[Arc<Mutex<Vec<i32>>>; 3],
    expected: [Vec<i32>; 3],
    row_count: usize,
) -> TestResult {
    for rows in observed {
        rows.lock().unwrap().clear();
    }
    let query = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    assert!(
        observed.iter().all(|rows| rows.lock().unwrap().is_empty()),
        "planning evaluated a volatile substring operand: {sql}"
    );
    let batches = collect(physical, session.task_ctx()).await?;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        row_count
    );
    for (rows, expected) in observed.iter().zip(expected) {
        let mut rows = rows.lock().unwrap().clone();
        rows.sort_unstable();
        assert_eq!(rows, expected, "{sql}, ANSI={ansi_mode}");
    }
    Ok(())
}

#[tokio::test]
async fn substring_evaluates_each_needed_operand_once() -> TestResult {
    let header: Value = serde_json::from_str(
        include_str!("fixtures/substring-arguments-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing fixture header")?,
    )?;
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
            register_inputs(&session, &header, representation, batch_size)?;
            let string_type = match representation {
                0 => DataType::Utf8,
                1 => DataType::LargeUtf8,
                _ => DataType::Utf8View,
            };
            let binary_type = match representation {
                0 => DataType::Binary,
                1 => DataType::LargeBinary,
                _ => DataType::BinaryView,
            };
            let observed: [Arc<Mutex<Vec<i32>>>; 3] =
                std::array::from_fn(|_| Arc::new(Mutex::new(Vec::new())));
            for (name, index, data_type) in [
                ("count_substring_source", 0, string_type.clone()),
                ("count_substring_binary", 0, binary_type),
                ("count_substring_position", 1, string_type.clone()),
                ("count_substring_length", 2, string_type),
            ] {
                let rows = Arc::clone(&observed[index]);
                session.register_udf(create_udf(
                    name,
                    vec![DataType::Int32, data_type.clone()],
                    data_type,
                    Volatility::Volatile,
                    Arc::new(move |args| {
                        let arrays = ColumnarValue::values_to_arrays(args)?;
                        let ids =
                            arrays[0]
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .ok_or_else(|| {
                                    datafusion::common::DataFusionError::Internal(
                                        "substring counted operand expected INT row IDs".into(),
                                    )
                                })?;
                        rows.lock().unwrap().extend(ids.values().iter().copied());
                        Ok(ColumnarValue::Array(Arc::clone(&arrays[1])))
                    }),
                ));
            }
            for ansi_mode in [true, false] {
                for (source, source_function) in [
                    ("s", "count_substring_source"),
                    ("b", "count_substring_binary"),
                ] {
                    for (relation, expected_ids) in [
                        ("substring_text WHERE id<=6", vec![1, 2, 3, 4, 5, 6]),
                        ("substring_text WHERE id=5", vec![5]),
                        ("substring_text WHERE id=6", vec![6]),
                        ("substring_text_empty", vec![]),
                    ] {
                        let sql = format!(
                            "SELECT substring({source_function}(id,{source}),count_substring_position(id,p),count_substring_length(id,n)) AS r FROM {relation}"
                        );
                        let expected = [
                            expected_ids.clone(),
                            expected_ids.iter().copied().filter(|id| *id != 5).collect(),
                            expected_ids
                                .iter()
                                .copied()
                                .filter(|id| *id != 5 && *id != 6)
                                .collect(),
                        ];
                        check_counted(
                            &session,
                            &sql,
                            ansi_mode,
                            &observed,
                            expected,
                            expected_ids.len(),
                        )
                        .await?;
                    }
                }
                for (sql, expected, row_count) in [
                    (
                        "SELECT substring(count_substring_source(id,CAST(NULL AS STRING)),count_substring_position(id,'bad'),count_substring_length(id,'bad')) AS r FROM substring_text WHERE id=1",
                        [vec![1], vec![], vec![]],
                        1,
                    ),
                    (
                        "SELECT substring(count_substring_source(id,s),count_substring_position(id,CAST(NULL AS STRING)),count_substring_length(id,'bad')) AS r FROM substring_text WHERE id=1",
                        [vec![1], vec![1], vec![]],
                        1,
                    ),
                    (
                        "SELECT CASE WHEN false THEN substring(count_substring_source(id,s),count_substring_position(id,'bad'),count_substring_length(id,'bad')) ELSE 'ok' END AS r FROM substring_text WHERE id=1",
                        [vec![], vec![], vec![]],
                        1,
                    ),
                    (
                        "SELECT COALESCE('ok',substring(count_substring_source(id,s),count_substring_position(id,'bad'),count_substring_length(id,'bad'))) AS r FROM substring_text WHERE id=1",
                        [vec![], vec![], vec![]],
                        1,
                    ),
                    (
                        "SELECT substring(count_substring_source(id,s),count_substring_position(id,p),count_substring_length(id,n)) AS r FROM VALUES (1,'abcdef','2','3'),(2,CAST(NULL AS STRING),'bad','bad'),(3,'abcdef',CAST(NULL AS STRING),'bad') AS t(id,s,p,n)",
                        [vec![1, 2, 3], vec![1, 3], vec![1]],
                        3,
                    ),
                ] {
                    check_counted(&session, sql, ansi_mode, &observed, expected, row_count).await?;
                }
            }
            // A failing later child must not trigger a replay of earlier volatile operands.
            for rows in &observed {
                rows.lock().unwrap().clear();
            }
            let query = plan_query(
                &session,
                "SELECT substring(count_substring_source(id,s),count_substring_position(id,p),count_substring_length(id,n)) AS r FROM substring_text WHERE id=7",
                &SparkSqlOptions { ansi_mode: true, ..Default::default() },
            )
            .await?;
            let physical = query.create_physical_plan(&session).await?;
            assert!(observed.iter().all(|rows| rows.lock().unwrap().is_empty()));
            let error = collect(physical, session.task_ctx())
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("Cannot cast string 'bad' to value of Int32 type"),
                "{error}"
            );
            for rows in &observed {
                assert_eq!(*rows.lock().unwrap(), vec![7]);
            }
        }
    }
    Ok(())
}
