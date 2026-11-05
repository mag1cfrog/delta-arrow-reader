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
    let mut observation_count = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await
                    .unwrap_or_else(|error| json!({"observer_error":error.to_string()}));
            let mut matches = actual["matches"] == true && actual["optimized_matches"] == true;
            if let Some(rows) = case["expected"]["rows"].as_array() {
                matches &= actual["rows"].as_array().map(Vec::len) == Some(rows.len())
                    && case["row_count"].as_u64() == Some(rows.len() as u64)
                    && actual["before_collect"] == case["schema_stages"]["physical"]
                    && case["schema_stages"]["public"] == case["schema_stages"]["analyzed"];
            }
            let mut cause_matches = None;
            let mut cause_error = None;
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
                        Err(error) => match matches_inline_cause(case, &error) {
                            Ok(matches) => matches,
                            Err(error) => {
                                cause_error = Some(error.to_string());
                                false
                            }
                        },
                        Ok(_) => false,
                    },
                );
                matches &= cause_matches == Some(true);
            }
            println!(
                "inline_values_observation={}",
                json!({"id":case["id"], "ansi":case["ansi"], "sql":case["sql"],
                "batch_size":batch_size, "observation":actual, "cause_matches":cause_matches,
                "cause_error":cause_error, "matches":matches})
            );
            observation_count += 1;
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {actual}; cause_matches={cause_matches:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert_eq!(observation_count, 144);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn inline_values_null_boundaries_match_spark() -> TestResult {
    check_inline_null_reference(
        include_str!("fixtures/inline-values-evaluation/null-boundaries-spark.jsonl"),
        8,
        "inline_values_null_boundary_observation",
    )
    .await
}

#[tokio::test]
async fn inline_values_null_divisors_match_spark() -> TestResult {
    check_inline_null_reference(
        include_str!("fixtures/inline-values-evaluation/null-divisors-spark.jsonl"),
        10,
        "inline_values_null_divisor_observation",
    )
    .await
}

async fn check_inline_null_reference(fixture: &str, case_count: usize, tag: &str) -> TestResult {
    let records = fixture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), case_count + 1);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["observations"], json!(case_count));
    assert_eq!(
        json!(
            records[1..]
                .iter()
                .map(|case| json!({"id":case["id"], "ansi":case["ansi"]}))
                .collect::<Vec<_>>()
        ),
        records[0]["identities"]
    );
    let cases = records[0]["cases"]
        .as_array()
        .ok_or("missing NULL-boundary SQL seeds")?;
    assert_eq!(cases.len() * 2, case_count);
    for (case, seed) in records[1..].iter().zip(cases.iter().cycle()) {
        for key in ["id", "sql", "ordered", "group"] {
            assert_eq!(case[key], seed[key]);
        }
    }
    let mut differences = Vec::new();
    let mut observation_count = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        for case in &records[1..] {
            let actual =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await
                    .unwrap_or_else(|error| json!({"observer_error":error.to_string()}));
            let mut matches = actual["matches"] == true && actual["optimized_matches"] == true;
            if let Some(rows) = case["expected"]["rows"].as_array() {
                matches &= actual["rows"].as_array().map(Vec::len) == Some(rows.len())
                    && case["row_count"].as_u64() == Some(rows.len() as u64)
                    && actual["before_collect"] == case["schema_stages"]["physical"]
                    && case["schema_stages"]["public"] == case["schema_stages"]["analyzed"];
            }
            let mut cause_matches = None;
            let mut cause_error = None;
            if case["expected"]["condition"] == INLINE_CONDITION {
                let result = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing NULL-boundary SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                cause_matches = Some(match result {
                    Err(error) => match matches_inline_cause(case, &error) {
                        Ok(matches) => matches,
                        Err(error) => {
                            cause_error = Some(error.to_string());
                            false
                        }
                    },
                    Ok(_) => false,
                });
                matches &= cause_matches == Some(true);
            }
            println!(
                "{tag}={}",
                json!({
                "id":case["id"], "ansi":case["ansi"], "sql":case["sql"],
                "batch_size":batch_size, "observation":actual, "cause_matches":cause_matches,
                "cause_error":cause_error, "matches":matches})
            );
            observation_count += 1;
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: {actual}; cause_matches={cause_matches:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert_eq!(observation_count, case_count * 2);
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
    let mut differences = Vec::new();
    let mut observation_count = 0;
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
                format!("SELECT x AS r FROM VALUES ({function}(1)+1) t(x)"),
            ] {
                calls.store(0, Ordering::SeqCst);
                let resolved = plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode,
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                let call_count = calls.load(Ordering::SeqCst);
                println!(
                    "inline_values_nonfoldable_observation={}",
                    json!({
                    "sql":sql, "ansi":ansi_mode, "function":function,
                    "phase":"resolve", "calls":call_count,
                    "error":resolved.as_ref().err().map(ToString::to_string)})
                );
                observation_count += 1;
                if resolved.is_err() || call_count != 0 {
                    differences.push(format!(
                        "{sql} ANSI={ansi_mode}: calls={call_count}, result={resolved:?}"
                    ));
                }
            }
        }
        calls.store(0, Ordering::SeqCst);
        let result = async {
            session
                .sql("SELECT inline_volatile(CAST(1 AS INT)) AS r")
                .await?
                .collect()
                .await
        }
        .await;
        let row_count = result
            .as_ref()
            .ok()
            .map(|batches| batches.iter().map(RecordBatch::num_rows).sum::<usize>());
        let call_count = calls.load(Ordering::SeqCst);
        println!(
            "inline_values_nonfoldable_observation={}",
            json!({
            "sql":"SELECT inline_volatile(CAST(1 AS INT)) AS r", "ansi":ansi_mode,
            "phase":"liveness", "calls":call_count, "row_count":row_count,
            "error":result.as_ref().err().map(ToString::to_string)})
        );
        if row_count != Some(1) || call_count != 1 {
            differences.push(format!(
                "volatile liveness ANSI={ansi_mode}: rows={row_count:?}, calls={call_count}, result={result:?}"
            ));
        }
    }
    assert_eq!(observation_count, 24);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
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
async fn native_values_sql_preserves_each_context_baseline() -> TestResult {
    let mut observations = Vec::new();
    for batch_size in [1, 32] {
        let contexts = [
            (
                "plain",
                SessionContext::new_with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(1),
                ),
            ),
            ("configured", session(batch_size)),
        ];
        for (mode, session) in contexts {
            for (id, sql) in [
                (
                    "cast_read",
                    "SELECT x AS r FROM (VALUES (CAST('bad' AS INT)), (1)) t(x)",
                ),
                (
                    "cast_false_filter",
                    "SELECT x AS r FROM (VALUES (CAST('bad' AS INT)), (1)) t(x) WHERE false",
                ),
                (
                    "cast_unused_limit_zero",
                    "SELECT 1 AS r FROM (VALUES (CAST('bad' AS INT)), (1)) t(x) LIMIT 0",
                ),
                (
                    "cast_unused",
                    "SELECT 1 AS r FROM (VALUES (CAST('bad' AS INT)), (1)) t(x)",
                ),
                (
                    "divide_false_filter",
                    "SELECT x AS r FROM (VALUES (1 / 0)) t(x) WHERE false",
                ),
                (
                    "divide_limit_zero",
                    "SELECT x AS r FROM (VALUES (1 / 0)) t(x) LIMIT 0",
                ),
            ] {
                let actual = spark_results::capture_native_query_observation(&session, sql)
                    .await
                    .unwrap_or_else(|error| json!({"observer_error":error.to_string()}));
                let record = json!({"id":id, "sql":sql, "mode":mode,
                    "batch_size":batch_size, "observation":actual});
                println!("inline_values_native_observation={record}");
                observations.push(record);
            }
        }
    }
    assert_eq!(observations.len(), 24);
    let capture_path = std::env::var_os("DAR_INLINE_VALUES_NATIVE_CAPTURE");
    if let Some(path) = &capture_path {
        use std::io::Write;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        for observation in &observations {
            writeln!(output, "{observation}")?;
        }
    }
    assert!(
        observations
            .iter()
            .all(|record| record["observation"].get("observer_error").is_none()),
        "native observer failed: {observations:?}"
    );
    if capture_path.is_some() {
        return Ok(());
    }
    let baseline = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/inline-values-evaluation/native-baseline.jsonl"
    ))?
    .lines()
    .map(serde_json::from_str::<Value>)
    .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(observations, baseline);
    Ok(())
}
