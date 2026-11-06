//! Floating grouping keys share Spark equality without changing aggregate payloads.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
use spark_inputs::register_recorded_inputs;

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_result};

#[tokio::test]
async fn floating_group_keys_match_spark() -> TestResult {
    let records = include_str!("fixtures/floating-group-keys-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 840);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
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
                            .with_target_partitions(2),
                    ),
            )
            .build(),
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = capture_spark_query_result(&session, case).await;
            let matches = if case["expected"]["condition"] == "CAST_INVALID_INPUT" {
                actual.as_ref().is_err_and(|error| {
                    let message = error.to_string();
                    message.contains("[CAST_INVALID_INPUT]") && message.contains("bad")
                })
            } else {
                actual.as_ref().ok() == Some(&case["expected"])
            };
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {actual:?}",
                    case["id"], case["ansi"], case["expected"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn nested_key_normalization_preserves_argument_evaluation() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/floating-group-keys-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    for ansi in [true, false] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
        );
        register_recorded_inputs(&session, &metadata, 2)?;
        let evaluated_rows = Arc::new(AtomicUsize::new(0));
        let observed = evaluated_rows.clone();
        session.register_udf(create_udf(
            "observed_key",
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Immutable,
            Arc::new(move |args| {
                observed.fetch_add(
                    match &args[0] {
                        ColumnarValue::Array(array) => array.len(),
                        ColumnarValue::Scalar(_) => 1,
                    },
                    Ordering::SeqCst,
                );
                Ok(args[0].clone())
            }),
        ));
        let query = plan_query(
            &session,
            "SELECT COUNT(DISTINCT ARRAY(observed_key(k))) AS n FROM double_scalar_mixed",
            &SparkSqlOptions {
                ansi_mode: ansi,
                ..Default::default()
            },
        )
        .await?;
        let physical = query.create_physical_plan(&session).await?;
        assert_eq!(evaluated_rows.load(Ordering::SeqCst), 0);
        let batches = datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(evaluated_rows.load(Ordering::SeqCst), 13);

        let invoked = Arc::new(AtomicUsize::new(0));
        let observed = invoked.clone();
        session.register_udf(create_udf(
            "volatile_key",
            vec![DataType::Float64],
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(move |args| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(args[0].clone())
            }),
        ));
        for expression in [
            "COUNT(DISTINCT ARRAY(volatile_key(k)))",
            "COLLECT_SET(ARRAY(volatile_key(k)))",
        ] {
            let planned = plan_query(
                &session,
                &format!("SELECT {expression} FROM double_scalar_mixed GROUP BY id%2"),
                &SparkSqlOptions {
                    ansi_mode: ansi,
                    ..Default::default()
                },
            )
            .await;
            assert!(
                planned.as_ref().is_err_and(|error| {
                    let message = error.to_string();
                    message.contains("AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION")
                        && message.contains("volatile_key")
                }),
                "{planned:?}"
            );
            assert_eq!(invoked.load(Ordering::SeqCst), 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn spark_planning_preserves_native_distinct_behavior() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/floating-group-keys-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &metadata, 32)?;
    }
    for kind in ["float", "double"] {
        for shape in ["scalar", "array", "struct", "nested", "struct_array"] {
            let sql = format!("SELECT COUNT(DISTINCT k) AS n FROM {kind}_{shape}_mixed");
            capture_spark_query_result(&configured, &json!({"ansi":true,"sql":sql})).await?;
            assert_eq!(
                configured.sql(&sql).await?.collect().await?,
                native.sql(&sql).await?.collect().await?,
                "{sql}"
            );
        }
    }
    Ok(())
}
