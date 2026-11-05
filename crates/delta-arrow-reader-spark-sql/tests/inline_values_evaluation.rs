//! Constant inline VALUES failures survive pruning and keep their original cause.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::{SessionStateBuilder, TaskContext};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, create_udf,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{SendableRecordBatchStream, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use spark_results::TestResult;

const INLINE_CONDITION: &str = "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION";

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
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
    )
}

fn matches_inline_cause(case: &Value, error: &DataFusionError) -> TestResult<bool> {
    let root = error.find_root();
    let root_message = root.to_string();
    if root_message.contains(INLINE_CONDITION) {
        return Ok(false);
    }
    // Spark's parser wrapper can omit its cause; its sqlExpr still pins input order.
    let selected = case["spark_error_details"]["parameters"]["sqlExpr"]
        .as_str()
        .ok_or("missing Spark's selected inline expression")?;
    if let Some(input) = selected
        .strip_prefix("\"CAST(")
        .and_then(|value| value.strip_suffix(" AS INT)\""))
    {
        return Ok(matches!(root, DataFusionError::ArrowError(cause, _)
            if matches!(cause.as_ref(), ArrowError::CastError(message)
                if message == &format!("Cannot cast string '{input}' to value of Int32 type"))));
    }
    if selected == "\"CAST(bad AS DECIMAL(4,2))\"" {
        return Ok(root_message.contains("Cannot cast Some(\"bad\") to DECIMAL(4,2)"));
    }
    if selected == "\"(2147483647 + 1)\"" {
        return Ok(root_message.contains("[ARITHMETIC_OVERFLOW]"));
    }
    let cause = case["spark_error_details"]["java_causes"]
        .as_array()
        .ok_or("missing raw Spark causes")?
        .iter()
        .find(|cause| {
            cause["condition"]
                .as_str()
                .is_some_and(|condition| condition != INLINE_CONDITION)
        })
        .ok_or("missing the underlying Spark inline-expression condition")?;
    Ok(match cause["condition"].as_str() {
        Some("DIVIDE_BY_ZERO") => matches!(root, DataFusionError::ArrowError(cause, _)
            if matches!(cause.as_ref(), ArrowError::DivideByZero)),
        Some(condition) => root_message.contains(&format!("[{condition}]")),
        None => false,
    })
}

#[tokio::test]
async fn inline_values_match_spark_stages_rows_and_causes() -> TestResult {
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/inline-values-evaluation/manifest.json"
    ))?;
    let records = include_str!("fixtures/inline-values-evaluation/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 73);
    assert_eq!(records[0]["source_commit"], manifest["source_commit"]);
    assert_eq!(records[0]["cases"], manifest["cases"]);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], manifest["spark_revision"]);
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(
        json!(
            records[1..]
                .iter()
                .map(|case| json!({"id":case["id"], "ansi":case["ansi"]}))
                .collect::<Vec<_>>()
        ),
        manifest["identities"]
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await?;
            let mut matches = actual["matches"] == true && actual["optimized_matches"] == true;
            if let Some(rows) = case["expected"]["rows"].as_array() {
                matches &= actual["rows"].as_array().map(Vec::len) == Some(rows.len())
                    && case["row_count"].as_u64() == Some(rows.len() as u64)
                    && actual["before_collect"] == case["schema_stages"]["physical"]
                    && case["schema_stages"]["public"] == case["schema_stages"]["analyzed"];
            }
            let mut cause_matches = None;
            if case["expected"]["condition"] == INLINE_CONDITION {
                cause_matches = Some(
                    match plan_query(
                        &session,
                        case["sql"].as_str().ok_or("missing SQL")?,
                        &SparkSqlOptions {
                            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                            ..SparkSqlOptions::default()
                        },
                    )
                    .await
                    {
                        Err(error) => matches_inline_cause(case, &error)?,
                        Ok(_) => false,
                    },
                );
                matches &= cause_matches == Some(true);
            }
            println!(
                "{}",
                json!({"id":case["id"], "ansi":case["ansi"], "sql":case["sql"],
                "batch_size":batch_size, "observation":actual, "cause_matches":cause_matches,
                "matches":matches})
            );
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {actual}; cause_matches={cause_matches:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[derive(Debug)]
struct DeferredInlineValue(Arc<AtomicUsize>);

impl PartialEq for DeferredInlineValue {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for DeferredInlineValue {}

impl std::hash::Hash for DeferredInlineValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::ptr::hash(Arc::as_ptr(&self.0), state);
    }
}

impl ScalarUDFImpl for DeferredInlineValue {
    fn name(&self) -> &str {
        "inline_deferred"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![DataType::Int32], Volatility::Immutable));
        &SIGNATURE
    }

    fn supports_constant_folding(&self) -> bool {
        false
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(args.args[0].clone())
    }
}

#[tokio::test]
async fn inline_validation_does_not_invoke_nonfoldable_functions() -> TestResult {
    for ansi_mode in [true, false] {
        let session = session(1);
        let calls = Arc::new(AtomicUsize::new(0));
        for (name, volatility) in [
            ("inline_volatile", Volatility::Volatile),
            ("inline_stable", Volatility::Stable),
        ] {
            let calls = Arc::clone(&calls);
            session.register_udf(create_udf(
                name,
                vec![DataType::Int32],
                DataType::Int32,
                volatility,
                Arc::new(move |args| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(args[0].clone())
                }),
            ));
        }
        session.register_udf(ScalarUDF::from(DeferredInlineValue(Arc::clone(&calls))));
        for function in ["inline_volatile", "inline_stable", "inline_deferred"] {
            for sql in [
                format!("SELECT x AS r FROM VALUES ({function}(1)) t(x)"),
                format!("SELECT x AS r FROM VALUES ({function}(1)) t(x) WHERE false"),
                format!("SELECT 1 AS r FROM VALUES ({function}(1)) t(x) LIMIT 0"),
            ] {
                plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                assert_eq!(calls.load(Ordering::SeqCst), 0, "{sql} ANSI={ansi_mode}");
            }
        }
        let batches = session
            .sql("SELECT inline_volatile(1) AS r")
            .await?
            .collect()
            .await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[derive(Debug)]
struct ObservedPartition {
    batch: RecordBatch,
    reads: Arc<AtomicUsize>,
}

impl PartitionStream for ObservedPartition {
    fn schema(&self) -> &SchemaRef {
        self.batch.schema_ref()
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(RecordBatchStreamAdapter::new(
            self.batch.schema(),
            futures_util::stream::iter(vec![Ok(self.batch.clone())]),
        ))
    }
}

#[tokio::test]
async fn inline_validation_does_not_read_registered_providers() -> TestResult {
    for ansi_mode in [true, false] {
        let session = session(1);
        let reads = Arc::new(AtomicUsize::new(0));
        let batch = RecordBatch::try_from_iter([(
            "k",
            Arc::new(arrow::array::Int32Array::from(vec![7])) as arrow::array::ArrayRef,
        )])?;
        session.register_table(
            "inline_reads",
            Arc::new(StreamingTable::try_new(
                batch.schema(),
                vec![Arc::new(ObservedPartition {
                    batch,
                    reads: Arc::clone(&reads),
                })],
            )?),
        )?;
        for sql in [
            "SELECT k AS r FROM inline_reads CROSS JOIN VALUES (7) t(x) LIMIT 0",
            "SELECT k AS r FROM inline_reads CROSS JOIN VALUES (INTERVAL '1' MONTH/0) t(x) LIMIT 0",
            "SELECT x AS r FROM VALUES ((SELECT k FROM inline_reads LIMIT 1)) t(x)",
        ] {
            let resolved = plan_query(
                &session,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..SparkSqlOptions::default()
                },
            )
            .await;
            // Subquery admission is separate; resolving an inline cell cannot read it.
            assert_eq!(reads.load(Ordering::SeqCst), 0, "{sql}: {resolved:?}");
            if sql.contains("VALUES (7)") {
                let physical = resolved?.create_physical_plan(&session).await?;
                assert!(
                    collect(physical, session.task_ctx())
                        .await?
                        .iter()
                        .all(|batch| batch.num_rows() == 0)
                );
                assert_eq!(reads.load(Ordering::SeqCst), 0);
            }
        }
        let batches = session
            .sql("SELECT k FROM inline_reads")
            .await?
            .collect()
            .await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[tokio::test]
async fn native_values_sql_is_unchanged_by_spark_configuration() -> TestResult {
    let native = SessionContext::new();
    let configured = session(1);
    for sql in [
        "SELECT x AS r FROM (VALUES (CAST('bad' AS INT)), (1)) t(x)",
        "SELECT x AS r FROM (VALUES (CAST('bad' AS INT)), (1)) t(x) WHERE false",
        "SELECT 1 AS r FROM (VALUES (CAST('bad' AS INT)), (1)) t(x) LIMIT 0",
        "SELECT 1 AS r FROM (VALUES (CAST('bad' AS INT)), (1)) t(x)",
        "SELECT x AS r FROM (VALUES (1 / 0)) t(x) WHERE false",
        "SELECT x AS r FROM (VALUES (1 / 0)) t(x) LIMIT 0",
    ] {
        let expected = spark_results::capture_native_query_observation(&native, sql).await?;
        let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
        println!(
            "{}",
            json!({"native_sql":sql, "unconfigured":expected, "configured":actual})
        );
        assert_eq!(actual, expected, "{sql}");
    }
    Ok(())
}
