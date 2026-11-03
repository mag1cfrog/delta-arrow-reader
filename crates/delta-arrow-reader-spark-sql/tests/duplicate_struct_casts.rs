//! STRUCT casts and constructor declarations preserve exact Spark fields and values.

use std::collections::BTreeSet;
use std::io::Write;
use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use futures_util::FutureExt;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_schema, encode_value, validate_batch};

#[tokio::test]
async fn duplicate_struct_casts_match_spark() -> TestResult {
    let reference = std::env::var_os("DAR_DUPLICATE_STRUCT_CASTS_REFERENCE").map_or_else(
        || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/duplicate-struct-casts-spark.jsonl"),
        std::path::PathBuf::from,
    );
    constructor_cases_match_spark(reference,
        std::env::var_os("DAR_DUPLICATE_STRUCT_CASTS_OUTPUT"), 68).await
}

#[tokio::test]
async fn array_struct_declarations_match_spark() -> TestResult {
    let reference = std::env::var_os("DAR_ARRAY_STRUCT_DECLARATIONS_REFERENCE").map_or_else(
        || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/array-struct-declarations-spark.jsonl"),
        std::path::PathBuf::from,
    );
    constructor_cases_match_spark(reference,
        std::env::var_os("DAR_ARRAY_STRUCT_DECLARATIONS_OUTPUT"), 54).await
}

#[tokio::test]
async fn array_struct_metadata_matches_spark() -> TestResult {
    let reference = std::env::var_os("DAR_ARRAY_STRUCT_METADATA_REFERENCE").map_or_else(
        || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/array-struct-metadata-spark.jsonl"),
        std::path::PathBuf::from,
    );
    constructor_cases_match_spark(reference,
        std::env::var_os("DAR_ARRAY_STRUCT_METADATA_OUTPUT"), 24).await
}

#[tokio::test]
async fn array_struct_admission_matches_spark() -> TestResult {
    let reference = std::env::var_os("DAR_ARRAY_STRUCT_ADMISSION_REFERENCE").map_or_else(
        || std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/array-struct-admission-spark.jsonl"),
        std::path::PathBuf::from,
    );
    constructor_cases_match_spark(reference,
        std::env::var_os("DAR_ARRAY_STRUCT_ADMISSION_OUTPUT"), 20).await
}

async fn constructor_cases_match_spark(
    reference: std::path::PathBuf,
    output_path: Option<std::ffi::OsString>,
    expected_cases: usize,
) -> TestResult {
    let raw = std::fs::read_to_string(reference)?;
    let records = raw.lines().map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["map_encoding"], "entry_pairs");
    assert_eq!(records.len() - 1, expected_cases);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI")?)));
        assert!(case.get("expected").is_some() ^ case.get("error").is_some());
    }
    let mut output = output_path
        .map(|path| std::fs::OpenOptions::new().write(true).create_new(true).open(path))
        .transpose()?;
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(configure_spark_sql(
            SessionStateBuilder::new().with_default_features().with_config(
                SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2)))
            .build());
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let mut actual = json!({});
            let mut phase = "resolve";
            let observed = std::panic::AssertUnwindSafe(async {
                let query = plan_query(&session, case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions { ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        session_timezone: "UTC".into(), ..SparkSqlOptions::default() }).await?;
                actual["public_schema"] = capture_schema(query.logical_plan.schema().fields(),
                    Some(&query.column_names))?;
                actual["logical_plan"] = json!(format!("{}", query.logical_plan.display_indent_schema()));
                phase = "physical";
                let physical = query.create_physical_plan(&session).await?;
                let schema = physical.schema();
                actual["executed_schema_before_collect"] = capture_schema(schema.fields(), None)?;
                actual["physical_plan"] = json!(format!("{physical:?}"));
                phase = "execute";
                let batches = collect(Arc::clone(&physical), session.task_ctx()).await?;
                actual["executed_schema_after_collect"] = capture_schema(physical.schema().fields(), None)?;
                let mut rows = Vec::new();
                let mut batch_observations = Vec::new();
                for batch in batches {
                    phase = "validate_batch";
                    validate_batch(schema.as_ref(), &batch)?;
                    batch_observations.push(json!({"schema":capture_schema(batch.schema().fields(), None)?,
                        "rows":batch.num_rows()}));
                    for row in 0..batch.num_rows() {
                        rows.push(batch.columns().iter().map(|column| encode_value(column.as_ref(), row))
                            .collect::<TestResult<Vec<_>>>()?);
                    }
                }
                rows.sort_by_key(|row| json!(row).to_string());
                actual["batches"] = json!(batch_observations);
                actual["rows"] = json!(rows);
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }).catch_unwind().await;
            let matches = match &observed {
                Ok(Ok(())) if case.get("error").is_none() => {
                    actual["public_schema"] == case["public_schema"]
                        && actual["executed_schema_before_collect"] == case["executed_schema_before_collect"]
                        && actual["executed_schema_after_collect"] == case["executed_schema_after_collect"]
                        && actual["rows"] == case["expected"]["rows"]
                }
                Ok(Ok(())) => false,
                Ok(Err(error)) => {
                    let engine_error = error.downcast_ref::<DataFusionError>().is_some();
                    actual["error"] = json!({"message":error.to_string(), "phase":phase,
                        "engine_error":engine_error});
                    case["error"]["condition"].as_str().is_some_and(|condition|
                        engine_error && error.to_string().contains(&format!("[{condition}]")))
                }
                Err(panic) => {
                    actual["panic"] = json!({"phase":phase,
                        "message":panic.downcast_ref::<String>().cloned().or_else(||
                            panic.downcast_ref::<&str>().map(|message| (*message).to_owned()))});
                    false
                }
            };
            if let Some(output) = &mut output {
                writeln!(output, "{}", json!({"id":case["id"], "input":case["input"],
                    "ansi":case["ansi"], "batch_size":batch_size, "matches":matches,
                    "expected":case, "actual":actual}))?;
            }
            if !matches {
                differences.push(format!("{} {} ANSI={} batch={batch_size}: expected {case}, got {actual}",
                    case["id"],case["input"],case["ansi"]));
            }
            comparisons += 1;
        }
    }
    assert_eq!(comparisons, expected_cases * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
