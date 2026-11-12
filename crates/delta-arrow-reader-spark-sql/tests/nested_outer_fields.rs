//! Compare nested outer references with independently captured Spark execution.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use arrow::array::{Array, AsArray, StructArray};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use futures_util::FutureExt;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, c06_receipts, compare_exact_spark_query_stages, encode_value};

const INPUTS: &str = include_str!("fixtures/nested-outer-fields/inputs.json");
const RESULTS: &str = include_str!("fixtures/nested-outer-fields/spark-results.jsonl");

fn session(batch: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    )
}

fn references(manifest: &Value) -> TestResult<Vec<Value>> {
    assert_eq!(manifest["issue"], 819);
    assert_eq!(manifest["spark_version"], "4.2.0");
    assert_eq!(
        manifest["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(manifest["timezone"], "UTC");
    assert_eq!(manifest["ansi_modes"], json!([false, true]));
    let cases = manifest["cases"].as_array().ok_or("missing cases")?;
    assert_eq!(cases.len(), 8);
    let mut planned = BTreeMap::new();
    for case in cases {
        let id = case["id"].as_str().ok_or("missing planned case ID")?;
        assert!(
            case["ordered"].is_boolean(),
            "missing order policy for {id}"
        );
        for ansi in [false, true] {
            assert!(planned.insert((id, ansi), case).is_none(), "duplicate {id}");
        }
    }
    let references = RESULTS
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    for case in &references {
        let id = case["id"].as_str().ok_or("missing reference ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        let definition = planned
            .remove(&(id, ansi))
            .ok_or_else(|| format!("unexpected Spark reference {id} ANSI={ansi}"))?;
        for key in ["sql", "input", "ordered"] {
            assert_eq!(case[key], definition[key], "{id} ANSI={ansi}: {key}");
        }
        assert_eq!(case["status"], "ok", "{id} ANSI={ansi}");
        assert!(case["rows"].is_array(), "missing Spark rows for {id}");
        assert!(
            case["physical_schema"]["fields"].is_array(),
            "missing Spark executed fields for {id}"
        );
    }
    assert!(planned.is_empty(), "missing Spark references: {planned:?}");
    Ok(references)
}

fn remove_field_annotations(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            for key in ["nullable", "metadata", "containsNull", "valueContainsNull"] {
                fields.remove(key);
            }
            fields.values_mut().for_each(remove_field_annotations);
        }
        Value::Array(values) => values.iter_mut().for_each(remove_field_annotations),
        _ => {}
    }
}

async fn observe(session: &SessionContext, case: &Value, batch: usize, variant: &str) -> bool {
    let mut expected_rows = case["rows"].clone();
    if case["ordered"] == false
        && let Some(rows) = expected_rows.as_array_mut()
    {
        rows.sort_by_cached_key(Value::to_string);
    }
    let query = json!({"sql":case["sql"],"ansi":case["ansi"],"ordered":case["ordered"],
        "expected":{"rows":expected_rows}});
    let actual = match AssertUnwindSafe(compare_exact_spark_query_stages(session, &query))
        .catch_unwind()
        .await
    {
        Ok(Ok(mut observation)) => {
            observation["status"] = json!(if observation["error"].is_null() {
                "ok"
            } else {
                "error"
            });
            observation
        }
        Ok(Err(error)) => json!({"status":"error","error":error.to_string()}),
        Err(panic) => {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied());
            json!({"status":"panic","panic":message})
        }
    };
    let mut spark_schema = case["physical_schema"].clone();
    let mut dar_schema = actual["physical_schema"].clone();
    remove_field_annotations(&mut spark_schema);
    remove_field_annotations(&mut dar_schema);
    let fields_match = dar_schema == spark_schema;
    let rows_match = actual["rows_match"] == true && actual["ordered_rows_match"] == true;
    let matches = actual["status"] == "ok" && fields_match && rows_match;
    let receipt = json!({"id":case["id"],"sql":case["sql"],"input":case["input"],"ansi":case["ansi"],
        "batch":batch,"variant":variant,"matches":matches,"fields_match":fields_match,
        "rows_match":rows_match,"spark":case,"actual":actual});
    eprintln!("NESTED_OUTER_FIELDS {receipt}");
    matches
}

fn register_hidden_payload(session: &SessionContext, input: &Value, batch: usize) -> TestResult {
    let logical = spark_inputs::decode_recorded_batch(input)?;
    let nested_index = logical.schema().index_of("nested")?;
    let mut storage = input.clone();
    let row = input["rows"]
        .as_array()
        .ok_or("missing input rows")?
        .iter()
        .position(|row| row[nested_index].is_null())
        .ok_or("missing NULL nested parent")?;
    storage["rows"][row][nested_index] = json!(["-3", ["-4"]]);
    let storage = spark_inputs::decode_recorded_batch(&storage)?;
    let original_nested = logical.column(nested_index).as_struct();
    let storage_nested = storage.column(nested_index).as_struct();
    let nested = StructArray::try_new(
        storage_nested.fields().clone(),
        storage_nested.columns().to_vec(),
        original_nested.nulls().cloned(),
    )?;
    let item = nested
        .column_by_name("item")
        .ok_or("missing hidden child")?;
    let physical_child = encode_value(item.as_ref(), row)?;
    assert!(nested.is_null(row));
    assert_eq!(physical_child, "-3");
    let mut columns = logical.columns().to_vec();
    columns[nested_index] = Arc::new(nested);
    let provider = RecordBatch::try_new(logical.schema(), columns)?;
    let readback = c06_receipts::batches(std::slice::from_ref(&provider), encode_value)?;
    assert_eq!(readback["rows"], input["rows"]);
    let receipt = json!({"batch":batch,"variant":"hidden_payload","input":input["name"],
        "row":row,"parent_is_null":true,"physical_child":physical_child,"rows":readback["rows"]});
    eprintln!("NESTED_OUTER_FIELDS_INPUT {receipt}");
    let mut partitions = vec![Vec::new(), Vec::new()];
    for (chunk, offset) in (0..provider.num_rows()).step_by(batch).enumerate() {
        partitions[chunk % 2].push(provider.slice(offset, batch.min(provider.num_rows() - offset)));
    }
    session.register_table(
        input["name"].as_str().ok_or("missing provider name")?,
        Arc::new(MemTable::try_new(provider.schema(), partitions)?),
    )?;
    Ok(())
}

#[tokio::test]
async fn nested_outer_fields_match_spark_values_and_executed_types() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    let references = references(&manifest)?;
    let inputs = json!({"inputs":manifest["registered_inputs"]});
    let mixed = inputs["inputs"]
        .as_array()
        .ok_or("missing registered inputs")?
        .iter()
        .find(|input| input["name"] == "struct_mixed")
        .ok_or("missing struct_mixed input")?;
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let recorded = session(batch);
        spark_inputs::register_recorded_inputs(&recorded, &inputs, batch)?;
        let hidden = session(batch);
        register_hidden_payload(&hidden, mixed, batch)?;
        for case in &references {
            for (session, variant) in [(&recorded, "recorded"), (&hidden, "hidden_payload")] {
                if variant == "hidden_payload" && case["id"] != "reported_struct_mixed" {
                    continue;
                }
                if !observe(session, case, batch, variant).await {
                    differences.push(format!(
                        "{} ANSI={} batch={batch} variant={variant}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} nested outer field contexts differ:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
