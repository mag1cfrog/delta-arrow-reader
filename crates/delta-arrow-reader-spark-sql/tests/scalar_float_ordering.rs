//! Query comparison keys follow Spark order and leave original float payloads intact.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, DictionaryArray, Int32Array};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_result};

fn reference_cases() -> TestResult<Vec<Value>> {
    Ok(include_str!("fixtures/scalar-float-ordering-spark.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn raw_float_bits(array: &dyn Array, row: usize) -> TestResult<Value> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match array.data_type() {
        DataType::Float32 => json!(format!(
            "{:08x}",
            array.as_primitive::<Float32Type>().value(row).to_bits()
        )),
        DataType::Float64 => json!(format!(
            "{:016x}",
            array.as_primitive::<Float64Type>().value(row).to_bits()
        )),
        other => return Err(format!("unexpected float payload type: {other}").into()),
    })
}

fn assert_order_and_payloads(batches: &[RecordBatch], case: &Value, input: &Value) -> TestResult {
    let mut ids = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let id = batch.column(0).as_primitive::<Int32Type>().value(row);
            ids.push(id.to_string());
            if batch.num_columns() > 1 {
                assert_eq!(
                    raw_float_bits(batch.column(1).as_ref(), row)?,
                    input["rows"][usize::try_from(id)?][0],
                    "{}: id={id}",
                    case["sql"]
                );
            }
        }
    }
    assert_eq!(json!(ids), case["ordered_ids"], "{}", case["sql"]);
    Ok(())
}

#[tokio::test]
async fn scalar_float_order_matches_spark_without_changing_payloads() -> TestResult {
    let records = reference_cases()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records.len() - 1, 204);
    assert_eq!(
        records[0]["inputs"]
            .as_array()
            .ok_or("missing inputs")?
            .len(),
        6
    );
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((case["id"].to_string(), case["ansi"].to_string())));
    }
    let mut differences = Vec::new();
    let mut observations = Vec::new();
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
            let matches = actual.as_ref().ok() == Some(&case["expected"]);
            observations.push(json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                "matches":matches,"actual":actual.as_ref().map_or_else(|error|json!({"error":error.to_string()}),Clone::clone)}));
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {actual:?}",
                    case["id"], case["ansi"], case["expected"]
                ));
                continue;
            }
            let input = records[0]["inputs"]
                .as_array()
                .ok_or("missing inputs")?
                .iter()
                .find(|input| input["name"] == case["table"])
                .ok_or("missing provider")?;
            let query = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing SQL")?,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    ..Default::default()
                },
            )
            .await?;
            let batches = collect(
                query.create_physical_plan(&session).await?,
                session.task_ctx(),
            )
            .await?;
            assert_order_and_payloads(&batches, case, input)?;
        }
    }
    if let Ok(path) = std::env::var("DAR_SCALAR_FLOAT_ORDER_OBSERVATIONS") {
        std::fs::write(path, serde_json::to_string_pretty(&observations)?)?;
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn scalar_dictionary_sort_keys_decode_without_changing_payloads() -> TestResult {
    let records = reference_cases()?;
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
        for input in records[0]["inputs"].as_array().ok_or("missing inputs")? {
            let table = input["name"].as_str().ok_or("missing table")?;
            if !table.ends_with("_mixed") {
                continue;
            }
            let source = session
                .sql(&format!("SELECT k,id FROM {table} ORDER BY id"))
                .await?
                .collect()
                .await?;
            let source = arrow::compute::concat_batches(&source[0].schema(), &source)?;
            let values = source.column(0).clone();
            // A NULL dictionary key and a key referencing a NULL value are both logical NULLs.
            let keys = Int32Array::from(
                (0..values.len())
                    .map(|index| {
                        if index == values.len() - 1 {
                            None
                        } else {
                            Some(index as i32)
                        }
                    })
                    .collect::<Vec<_>>(),
            );
            let dictionary: ArrayRef =
                Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values)?);
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", dictionary.data_type().clone(), true),
                Field::new("id", DataType::Int32, false),
            ]));
            let batch =
                RecordBatch::try_new(schema.clone(), vec![dictionary, source.column(1).clone()])?;
            let mut partitions = vec![Vec::new(); 2];
            for offset in 0..batch.num_rows() {
                partitions[offset % 2].push(batch.slice(offset, 1));
            }
            let dictionary_table = format!("{table}_dictionary");
            session.register_table(
                &dictionary_table,
                Arc::new(MemTable::try_new(schema, partitions)?),
            )?;
            for case in records[1..].iter().filter(|case| {
                case["table"] == table
                    && matches!(
                        case["label"].as_str(),
                        Some("order" | "limit" | "limit_ties")
                    )
            }) {
                let kind = input["schema"]["fields"][0]["type"]
                    .as_str()
                    .ok_or("missing type")?;
                let limit = match case["label"].as_str() {
                    Some("limit") => " LIMIT 1",
                    Some("limit_ties") => " LIMIT 8",
                    _ => "",
                };
                let sql = format!(
                    "SELECT id,CAST(k AS {kind}) AS k FROM {dictionary_table} ORDER BY {dictionary_table}.k {},id{limit}",
                    case["direction"].as_str().ok_or("missing direction")?
                );
                let query = plan_query(
                    &session,
                    &sql,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        ..Default::default()
                    },
                )
                .await?;
                let batches = collect(
                    query.create_physical_plan(&session).await?,
                    session.task_ctx(),
                )
                .await?;
                assert_order_and_payloads(&batches, case, input)?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn spark_scalar_sort_planning_preserves_native_sql() -> TestResult {
    let records = reference_cases()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &records[0], 1)?;
    }
    let mut snapshots = Vec::new();
    for kind in ["float", "double"] {
        for suffix in ["", " LIMIT 1"] {
            let sql = format!(
                "SELECT id,k FROM {kind}_scalar_mixed ORDER BY k DESC NULLS LAST,id{suffix}"
            );
            let before = native.sql(&sql).await?.collect().await?;
            assert_eq!(
                configured.sql(&sql).await?.collect().await?,
                before,
                "{sql}"
            );
            snapshots.push((sql, before));
        }
    }
    for case in &records[1..] {
        capture_spark_query_result(&configured, case).await?;
    }
    for (sql, before) in snapshots {
        assert_eq!(native.sql(&sql).await?.collect().await?, before, "{sql}");
        assert_eq!(
            configured.sql(&sql).await?.collect().await?,
            before,
            "{sql}"
        );
    }
    Ok(())
}
