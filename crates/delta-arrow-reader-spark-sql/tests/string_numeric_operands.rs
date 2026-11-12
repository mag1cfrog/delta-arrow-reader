//! Compare Spark arithmetic and numeric aggregates on STRING values and executed result types.

use std::collections::BTreeMap;
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
use spark_results::{
    TestResult, c06_receipts, capture_native_query_observation, capture_schema, encode_value,
    validate_batch,
};

const INPUTS: &str = include_str!("fixtures/string-numeric-operands/inputs.json");
const RESULTS: &str = include_str!("fixtures/string-numeric-operands/spark-results.jsonl");

fn references(manifest: &Value) -> TestResult<Vec<Value>> {
    let mut expected = BTreeMap::new();
    for case in manifest["cases"].as_array().ok_or("missing case definitions")? {
        let id = case["id"].as_str().ok_or("missing case ID")?;
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        let modes = match case.get("ansi_modes") {
            Some(modes) => serde_json::from_value(modes.clone())?,
            None => vec![false, true],
        };
        let error_modes: Vec<bool> = match case.get("error_ansi_modes") {
            Some(modes) => serde_json::from_value(modes.clone())?,
            None => Vec::new(),
        };
        assert!(!modes.is_empty() && error_modes.iter().all(|ansi| modes.contains(ansi)));
        for ansi in modes {
            let succeeds = !error_modes.contains(&ansi);
            assert!(expected.insert((id.to_owned(), ansi), (sql, succeeds)).is_none());
        }
    }
    expected.retain(|_, (_, succeeds)| *succeeds);
    assert!(!expected.is_empty(), "missing Spark query matrix");
    let mut records = Vec::with_capacity(expected.len());
    for line in RESULTS.lines() {
        let case: Value = serde_json::from_str(line)?;
        let id = case["id"].as_str().ok_or("missing reference ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        let (sql, _) = expected
            .remove(&(id.to_owned(), ansi))
            .ok_or_else(|| format!("unexpected Spark reference {id} ANSI={ansi}"))?;
        assert_eq!(case["sql"], sql, "{id} ANSI={ansi}");
        assert_eq!(case["status"], "ok", "{id} ANSI={ansi}");
        assert_eq!(
            case["effective_settings"]["spark.sql.ansi.enabled"],
            ansi.to_string(),
            "{id}"
        );
        assert_eq!(case["effective_settings"]["spark.sql.session.timeZone"], "UTC");
        records.push(case);
    }
    assert!(expected.is_empty(), "missing Spark references: {expected:?}");
    Ok(records)
}

fn result_fields(schema: &Value) -> TestResult<Vec<Value>> {
    schema["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| {
            let name = field["name"].as_str().ok_or("missing field name")?;
            let kind = field["type"].as_str().ok_or("missing scalar type")?;
            Ok(json!({"name": name, "type": kind}))
        })
        .collect()
}

fn spark_rows(case: &Value, fields: &[Value]) -> TestResult<Vec<Value>> {
    let mut rows = case["rows"].as_array().ok_or("missing Spark rows")?.clone();
    for row in &mut rows {
        let cells = row.as_array_mut().ok_or("expected a positional Spark row")?;
        assert_eq!(cells.len(), fields.len());
        for (cell, field) in cells.iter_mut().zip(fields) {
            if cell.is_null() {
                continue;
            }
            let text = cell.as_str().ok_or("expected Spark scalar text")?;
            // The shared Arrow encoder keeps signed zero and all finite floating-point bits.
            *cell = match field["type"].as_str() {
                Some("float") => {
                    let value = text.parse::<f32>()?;
                    json!(if value.is_nan() {
                        "nan".to_owned()
                    } else {
                        format!("{:08x}", value.to_bits())
                    })
                }
                Some("double") => {
                    let value = text.parse::<f64>()?;
                    json!(if value.is_nan() {
                        "nan".to_owned()
                    } else {
                        format!("{:016x}", value.to_bits())
                    })
                }
                _ => continue,
            };
        }
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
        let schema = physical.schema();
        actual["physical"] = c06_receipts::schema(schema.as_ref());
        phase = "collect";
        let batches = collect(physical, session.task_ctx()).await?;
        phase = "encode";
        for batch in &batches {
            validate_batch(schema.as_ref(), batch)?;
        }
        actual["output"] = c06_receipts::batches(&batches, encode_value)?;
        actual["executed_schema"] = capture_schema(schema.fields(), None)?;
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
async fn string_numeric_operands_match_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    assert_eq!(manifest["spark_version"], "4.2.0");
    assert_eq!(
        manifest["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(manifest["ansi_modes"], json!([false, true]));
    let references = references(&manifest)?;
    let inputs = json!({"inputs": manifest["registered_inputs"]});
    let mut output = std::env::var_os("DAR_STRING_NUMERIC_OPERANDS_OUTPUT")
        .map(|path| OpenOptions::new().write(true).create_new(true).open(path))
        .transpose()?;
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
            let actual = observe(&session, sql, ansi).await;
            let expected_fields = result_fields(&case["physical_schema"])?;
            let mut expected_rows = spark_rows(case, &expected_fields)?;
            let (actual_fields, mut actual_rows) = if actual["status"] == "ok" {
                (
                    result_fields(&actual["executed_schema"])?,
                    actual["output"]["rows"].as_array().ok_or("missing executed rows")?.clone(),
                )
            } else {
                (Vec::new(), Vec::new())
            };
            if !sql.contains(" ORDER BY ") {
                expected_rows.sort_by_key(Value::to_string);
                actual_rows.sort_by_key(Value::to_string);
            }
            let matches = actual["status"] == "ok"
                && actual_fields == expected_fields
                && actual_rows == expected_rows;
            if !matches {
                differences.push(format!(
                    "{} ANSI={ansi} batch={batch_size}: expected fields={} rows={}, got fields={} rows={} error={} panic={}",
                    case["id"], json!(expected_fields), json!(expected_rows),
                    json!(actual_fields), json!(actual_rows), actual["error"]["display"], actual["panic"],
                ));
            }
            if let Some(file) = &mut output {
                writeln!(
                    file,
                    "{}",
                    json!({"id": case["id"], "sql": sql, "ansi": ansi, "batch_size": batch_size,
                        "target_partitions": 2, "matches": matches, "spark": case, "actual": actual})
                )?;
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

#[tokio::test]
async fn native_sql_keeps_its_numeric_coercion() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    let inputs = json!({"inputs": manifest["registered_inputs"]});
    for (context, session) in [
        ("default", SessionContext::new()),
        (
            "spark_configured",
            SessionContext::new_with_state(
                configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
            ),
        ),
    ] {
        spark_inputs::register_recorded_inputs(&session, &inputs, 1)?;
        for sql in [
            "SELECT k, n + 1 AS r FROM inputs",
            "SELECT avg(n) AS r FROM inputs",
        ] {
            let observed = capture_native_query_observation(&session, sql).await?;
            println!("{}", json!({"context": context, "sql": sql, "actual": observed}));
            assert!(observed["error"].is_string(), "{sql}: {observed}");
        }
        for (sql, fields, rows) in [
            (
                "SELECT k, CAST(n AS BIGINT) + 1 AS r FROM inputs",
                json!([{"name":"k","type":"integer"}, {"name":"r","type":"long"}]),
                json!([["0", "13"], ["1", "8"]]),
            ),
            (
                "SELECT avg(CAST(n AS DOUBLE)) AS r FROM inputs",
                json!([{"name":"r","type":"double"}]),
                json!([[format!("{:016x}", 9.5_f64.to_bits())]]),
            ),
        ] {
            let observed = capture_native_query_observation(&session, sql).await?;
            println!("{}", json!({"context": context, "sql": sql, "actual": observed}));
            assert!(observed["error"].is_null(), "{sql}: {observed}");
            assert_eq!(
                json!(result_fields(&observed["physical_schema"])?),
                fields,
                "{sql}"
            );
            assert_eq!(observed["rows"], rows, "{sql}");
        }
    }
    Ok(())
}
