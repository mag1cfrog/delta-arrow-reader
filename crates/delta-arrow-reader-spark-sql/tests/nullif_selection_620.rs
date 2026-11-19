//! Connected provider/local/error controls for NULLIF's kernel-safety opt-in.

use std::error::Error;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, StringArray};
use arrow::datatypes::{DataType, Float64Type};
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

fn failure(phase: &str, error: DataFusionError) -> Value {
    let root = error.find_root();
    let debug = format!("{root:?}");
    let class = debug.split(['(', '{']).next().unwrap_or_default().trim();
    // Preserve complete messages while excluding optional runtime backtraces.
    json!({"status":"error","phase":phase,"root_class":class,
        "root_message":root.strip_backtrace(),"message":error.strip_backtrace()})
}

async fn observe(session: &SessionContext, sql: &str, ansi_mode: bool) -> Value {
    let query = match plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            ..SparkSqlOptions::default()
        },
    )
    .await
    {
        Ok(query) => query,
        Err(error) => return failure("planning", error),
    };
    let plan = match query.create_physical_plan(session).await {
        Ok(plan) => plan,
        Err(error) => return failure("physical-planning", error),
    };
    let physical_type = format!("{:?}", plan.schema().field(0).data_type());
    assert_eq!(plan.schema().field(0).data_type(), &DataType::Float64);
    let batches = match collect(plan, session.task_ctx()).await {
        Ok(batches) => batches,
        Err(error) => return failure("execution", error),
    };
    let mut executed_types = Vec::new();
    let mut values = Vec::new();
    for batch in batches {
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.column(0).data_type(), &DataType::Float64);
        executed_types.push(format!("{:?}", batch.column(0).data_type()));
        values.extend(
            batch
                .column(0)
                .as_primitive::<Float64Type>()
                .iter()
                .map(|value| value.map(|v| format!("{:016x}", v.to_bits()))),
        );
    }
    executed_types.sort();
    executed_types.dedup();
    json!({"status":"ok","phase":"execution","physical_type":physical_type,"executed_types":executed_types,"values":values})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_local_guards_and_comparability_keep_nullif_outcomes()
-> Result<(), Box<dyn Error + Send + Sync>> {
    let corpus: Value = serde_json::from_str(include_str!(
        "../vendor/sail/crates/sail-plan/tests/fixtures/nullif-selection-620-inputs.json"
    ))?;
    let mut observations = Vec::new();
    for batch_size in [1, 3, 16] {
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
        let left: Vec<Option<f64>> = serde_json::from_value(corpus["fallback_left"].clone())?;
        let strings: Vec<Option<String>> =
            serde_json::from_value(corpus["skipped_bad_strings"].clone())?;
        let bad: Vec<Option<String>> =
            serde_json::from_value(corpus["required_bad_strings"].clone())?;
        session.register_batch(
            "inputs",
            RecordBatch::try_from_iter([
                ("a", Arc::new(Float64Array::from(left)) as ArrayRef),
                ("s", Arc::new(StringArray::from(strings)) as ArrayRef),
                ("bad", Arc::new(StringArray::from(bad)) as ArrayRef),
            ])?,
        )?;
        for ansi_mode in [false, true] {
            for case in corpus["sql_cases"].as_array().ok_or("missing SQL cases")? {
                let outcome = observe(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    ansi_mode,
                )
                .await;
                let mode = if ansi_mode { "on" } else { "off" };
                if let Some(error) = case[format!("{mode}_error")].as_str() {
                    assert_eq!(
                        outcome["status"], "error",
                        "{} ANSI={ansi_mode}",
                        case["name"]
                    );
                    let actual = outcome["message"].as_str().ok_or("missing error message")?;
                    assert!(
                        actual.contains(error),
                        "{} ANSI={ansi_mode}: {actual}",
                        case["name"]
                    );
                } else {
                    assert_eq!(
                        outcome["status"], "ok",
                        "{} ANSI={ansi_mode}: {outcome}",
                        case["name"]
                    );
                    assert_eq!(
                        outcome["values"], case[mode],
                        "{} ANSI={ansi_mode}",
                        case["name"]
                    );
                }
                observations.push(json!({"case":case["name"],"sql":case["sql"],"ansi":ansi_mode,"batch_size":batch_size,"observation":outcome}));
            }
        }
    }
    assert_eq!(observations.len(), 48);
    if let Some(path) = std::env::var_os("DAR_CAPTURE_NULLIF_SQL_BASELINE") {
        let path = PathBuf::from(path);
        assert!(path.is_absolute(), "capture path must be absolute");
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        serde_json::to_writer_pretty(&mut output, &observations)?;
        output.write_all(b"\n")?;
        return Ok(());
    }
    let baseline: Value = serde_json::from_str(&std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/nullif-selection-620-sql-baseline.json"
    ))?)?;
    assert_eq!(
        json!(observations),
        baseline,
        "NULLIF SQL phase/type/value/error observations changed"
    );
    Ok(())
}
