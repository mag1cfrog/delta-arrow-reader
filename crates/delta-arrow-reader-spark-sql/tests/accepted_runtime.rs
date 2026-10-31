//! Replay the final accepted runtime captures through the consolidated API.
//!
//! This verifies preservation, including documented unresolved differences. It
//! does not classify those historical outputs as Spark-compatible. Ordering is
//! checked separately in the public API tests; this corpus compares row multisets.

use std::error::Error;
use std::io::BufRead;

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Deserialize)]
struct RecordedQuery {
    id: String,
    sql: String,
    batch_size: usize,
    ansi: bool,
    expected: Value,
}

async fn observe(query: &RecordedQuery) -> TestResult<Value> {
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(query.batch_size)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    );
    let options = SparkSqlOptions {
        ansi_mode: query.ansi,
        ..SparkSqlOptions::default()
    };
    let planned = match plan_query(&session, &query.sql, &options).await {
        Ok(planned) => planned,
        Err(error) => {
            let message = match error {
                DataFusionError::External(error) => error.to_string(),
                error => error.to_string(),
            };
            return Ok(json!({"status": "planning_error", "error": message}));
        }
    };
    let logical_nullable: Vec<_> = planned
        .logical_plan
        .schema()
        .fields()
        .iter()
        .map(|field| field.is_nullable())
        .collect();
    let executed: datafusion::error::Result<_> = async {
        let physical = planned.create_physical_plan(&session).await?;
        let schema = physical.schema();
        let batches = collect(physical, session.task_ctx()).await?;
        Ok((schema, batches))
    }
    .await;
    let (schema, batches) = match executed {
        Ok(executed) => executed,
        Err(error) => {
            return Ok(json!({
                "status": "execution_error",
                "error": error.to_string(),
                "output_names": planned.column_names,
            }));
        }
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let row = batch
                .columns()
                .iter()
                .map(|column| {
                    if column.is_null(row_index) {
                        Ok(Value::Null)
                    } else {
                        array_value_to_string(column.as_ref(), row_index).map(Value::String)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            rows.push(Value::Array(row));
        }
    }
    rows.sort_by_cached_key(Value::to_string);
    Ok(json!({
        "status": "ok",
        "types": schema.fields().iter().map(|field| format!("{:?}", field.data_type())).collect::<Vec<_>>(),
        "rows": rows,
        "output_names": planned.column_names,
        "logical_nullable": logical_nullable,
        "physical_nullable": schema.fields().iter().map(|field| field.is_nullable()).collect::<Vec<_>>(),
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preserve_accepted_query_outcomes() -> TestResult {
    let compressed = include_bytes!("fixtures/accepted-runtime.jsonl.gz");
    let reader = std::io::BufReader::new(GzDecoder::new(&compressed[..]));
    let mut checked = 0;
    let mut failures = Vec::new();
    for line in reader.lines() {
        let mut query: RecordedQuery = serde_json::from_str(&line?)?;
        if let Some(rows) = query.expected.get_mut("rows").and_then(Value::as_array_mut) {
            rows.sort_by_cached_key(Value::to_string);
        }
        let actual = observe(&query).await?;
        let expected = query
            .expected
            .as_object()
            .ok_or("expected outcome object")?;
        for (field, value) in expected {
            let actual_value = actual.get(field);
            // The consolidation renamed this analyzer rule; its failure cause
            // and stage still have to match the archived observation.
            let normalized_error = if field == "error" {
                actual_value.and_then(Value::as_str).map(|message| {
                    Value::String(message.replace(
                        "preserve_spark_expression_errors\n",
                        "spark_decimal_null_propagation\n",
                    ))
                })
            } else {
                None
            };
            if normalized_error.as_ref().or(actual_value) != Some(value) {
                failures.push(format!(
                    "{}: {field}: expected {value}, got {}\nSQL: {}\nObserved: {actual}",
                    query.id,
                    actual.get(field).unwrap_or(&Value::Null),
                    query.sql,
                ));
                break;
            }
        }
        checked += 1;
        if checked % 1000 == 0 {
            eprintln!("Replayed {checked} accepted-runtime observations");
        }
        if failures.len() == 20 {
            return Err(format!(
                "Stopped after {checked} observations with 20 differences:\n{}",
                failures.join("\n")
            )
            .into());
        }
    }
    let manifest: Value = serde_json::from_str(include_str!("fixtures/accepted-runtime.json"))?;
    assert_eq!(
        checked,
        manifest["unique_observations"]
            .as_u64()
            .ok_or("missing corpus count")?,
        "the complete deduplicated corpus must run"
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
