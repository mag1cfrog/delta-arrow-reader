//! ARRAY and MAP use a common STRUCT declaration while preserving positional values.

use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::Write;
use std::panic::AssertUnwindSafe;

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use futures_util::FutureExt;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, c06_receipts, capture_schema, encode_value, validate_batch};

const INPUTS: &str = include_str!("fixtures/array-struct-declarations/inputs.json");
const RESULTS: &str = include_str!("fixtures/array-struct-declarations/spark-results.jsonl");

fn without_metadata(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            fields.remove("metadata");
            fields.values_mut().for_each(without_metadata);
        }
        Value::Array(values) => values.iter_mut().for_each(without_metadata),
        _ => {}
    }
}

fn result_types(schema: &Value) -> TestResult<Vec<Value>> {
    Ok(schema["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| {
            let mut data_type = field["type"].clone();
            without_metadata(&mut data_type);
            json!({"name": field["name"], "type": data_type})
        })
        .collect())
}

fn comparable_rows(rows: &Value, schema: &Value, ordered: bool) -> TestResult<Vec<Value>> {
    let fields = schema["fields"].as_array().ok_or("missing executed fields")?;
    let mut rows = rows.as_array().ok_or("missing result rows")?.clone();
    for row in &mut rows {
        let values = row.as_array_mut().ok_or("expected positional row")?;
        if values.len() != fields.len() {
            return Err("result row does not match executed field count".into());
        }
        for (value, field) in values.iter_mut().zip(fields) {
            // Compare MAP entries without regard to order. ARRAY and STRUCT positions still matter.
            if field["type"]["type"] == "map" && !value.is_null() {
                value
                    .as_array_mut()
                    .ok_or("expected MAP entry pairs")?
                    .sort_by_key(Value::to_string);
            }
        }
    }
    if !ordered {
        rows.sort_by_key(Value::to_string);
    }
    Ok(rows)
}

async fn observe(session: &SessionContext, sql: &str, ansi_mode: bool) -> Value {
    let mut actual = json!({});
    let mut phase = "plan_query";
    let result = AssertUnwindSafe(async {
        let query = plan_query(
            session,
            sql,
            &SparkSqlOptions {
                ansi_mode,
                session_timezone: "UTC".into(),
                ..SparkSqlOptions::default()
            },
        )
        .await?;
        phase = "create_physical_plan";
        let physical = query.create_physical_plan(session).await?;
        actual["before_collect"] = c06_receipts::schema(physical.schema().as_ref());
        phase = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        phase = "validate_output";
        let schema = physical.schema();
        actual["physical"] = c06_receipts::schema(schema.as_ref());
        actual["executed_schema"] = capture_schema(schema.fields(), None)?;
        for batch in &batches {
            validate_batch(schema.as_ref(), batch)?;
        }
        actual["output"] = c06_receipts::batches(&batches, encode_value)?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .catch_unwind()
    .await;
    match result {
        Ok(Ok(())) => actual["status"] = json!("ok"),
        Ok(Err(error)) => {
            actual["status"] = json!("error");
            actual["error"] = c06_receipts::failure(error.as_ref(), phase);
        }
        Err(panic) => {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied());
            actual["status"] = json!("panic");
            actual["panic"] = json!({"phase": phase, "message": message});
        }
    }
    actual
}

#[tokio::test]
async fn array_struct_declarations_match_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    assert_eq!(manifest["spark_version"], "4.2.0");
    assert_eq!(
        manifest["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(manifest["ansi_modes"], json!([false, true]));
    let definitions = manifest["cases"].as_array().ok_or("missing cases")?;
    let references = RESULTS
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(references.len(), 38);
    let mut identities = BTreeSet::new();
    for case in &references {
        let id = case["id"].as_str().ok_or("missing case ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(identities.insert((id, ansi)), "duplicate reference {id}");
        assert_eq!(case["status"], "ok", "{id}");
        assert_eq!(
            case["effective_settings"]["spark.sql.ansi.enabled"],
            ansi.to_string()
        );
        assert_eq!(
            case["effective_settings"]["spark.sql.session.timeZone"],
            "UTC"
        );
    }
    assert_eq!(identities.len(), definitions.len() * 2);
    for definition in definitions {
        let id = definition["id"].as_str().ok_or("missing case ID")?;
        for ansi in [false, true] {
            assert!(
                references.iter().any(|case| {
                    case["id"] == id && case["ansi"] == ansi && case["sql"] == definition["sql"]
                }),
                "missing Spark reference for {id} ANSI={ansi}"
            );
        }
    }

    let mut output = std::env::var_os("DAR_ARRAY_STRUCT_DECLARATIONS_OUTPUT")
        .map(|path| OpenOptions::new().write(true).create_new(true).open(path))
        .transpose()?;
    let inputs = json!({"inputs": manifest["registered_inputs"]});
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
        spark_inputs::register_recorded_inputs(&session, &inputs, batch_size)?;
        for case in &references {
            let sql = case["sql"].as_str().ok_or("missing SQL")?;
            let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
            let ordered = sql.contains(" ORDER BY ");
            let expected_schema = &case["physical_schema"];
            let expected_types = result_types(expected_schema)?;
            let expected_rows = comparable_rows(&case["rows"], expected_schema, ordered)?;
            let mut actual = observe(&session, sql, ansi).await;
            let mut matches = false;
            if actual["status"] == "ok" {
                let actual_schema = &actual["executed_schema"];
                let actual_types = result_types(actual_schema)?;
                let actual_rows =
                    comparable_rows(&actual["output"]["rows"], actual_schema, ordered)?;
                matches = actual_types == expected_types && actual_rows == expected_rows;
                actual["result_types"] = json!(actual_types);
            }
            if let Some(file) = &mut output {
                writeln!(
                    file,
                    "{}",
                    json!({"id": case["id"], "sql": sql, "ansi": ansi,
                        "batch_size": batch_size, "matches": matches, "spark": case, "actual": actual})
                )?;
            }
            if !matches {
                differences.push(format!(
                    "{} ANSI={ansi} batch={batch_size}: expected types={} rows={}, got types={} rows={} error={} panic={}",
                    case["id"],
                    json!(expected_types),
                    json!(expected_rows),
                    actual["result_types"],
                    actual["output"]["rows"],
                    actual["error"],
                    actual["panic"],
                ));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
