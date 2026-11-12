//! Compare ordered TRY arithmetic values and executed field names and types with Spark.

use std::collections::BTreeMap;
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

const INPUTS: &str = include_str!("fixtures/try-numeric-arithmetic/inputs.json");
const SPARK: &str = include_str!("fixtures/try-numeric-arithmetic/spark-results.jsonl");
const TARGET_PARTITIONS: usize = 2;

fn references(manifest: &Value) -> TestResult<Vec<Value>> {
    let mut planned = BTreeMap::new();
    for case in manifest["cases"].as_array().ok_or("missing cases")? {
        let id = case["id"].as_str().ok_or("missing case ID")?;
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        let modes = case
            .get("ansi_modes")
            .unwrap_or(&manifest["ansi_modes"])
            .as_array()
            .ok_or("missing ANSI modes")?;
        assert!(!modes.is_empty(), "missing ANSI modes for {id}");
        for mode in modes {
            let ansi = mode.as_bool().ok_or("expected an ANSI boolean")?;
            assert!(planned.insert((id, ansi), sql).is_none(), "duplicate {id}");
        }
    }
    assert!(!planned.is_empty(), "missing Spark query matrix");
    let records = SPARK
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    for case in &records {
        let id = case["id"].as_str().ok_or("missing reference ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        let sql = planned
            .remove(&(id, ansi))
            .ok_or_else(|| format!("unexpected Spark reference {id} ANSI={ansi}"))?;
        assert_eq!(case["sql"], sql, "{id} ANSI={ansi}");
        assert_eq!(case["status"], "ok", "{id} ANSI={ansi}");
        assert_eq!(
            case["effective_settings"]["spark.sql.ansi.enabled"],
            ansi.to_string(),
            "{id}"
        );
        assert_eq!(
            case["effective_settings"]["spark.sql.session.timeZone"],
            "UTC",
            "{id}"
        );
    }
    assert!(planned.is_empty(), "missing Spark references: {planned:?}");
    Ok(records)
}

fn result_fields(schema: &Value) -> TestResult<Vec<Value>> {
    schema["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| {
            let name = field["name"].as_str().ok_or("missing field name")?;
            let data_type = field["type"].as_str().ok_or("missing scalar type")?;
            Ok(json!({"name": name, "type": data_type}))
        })
        .collect()
}

fn spark_rows(case: &Value, fields: &[Value]) -> TestResult<Value> {
    let mut rows = case["rows"].as_array().ok_or("missing Spark rows")?.clone();
    for row in &mut rows {
        let cells = row.as_array_mut().ok_or("expected a positional Spark row")?;
        assert_eq!(cells.len(), fields.len(), "{}", case["id"]);
        for (cell, field) in cells.iter_mut().zip(fields) {
            if cell.is_null() {
                continue;
            }
            let text = cell.as_str().ok_or("expected Spark scalar text")?;
            // Compare floating point bits, including signed zero, in the shared Arrow encoding.
            *cell = match field["type"].as_str() {
                Some("float") => {
                    let value = text.parse::<f32>()?;
                    json!(if value.is_nan() {
                        "nan".into()
                    } else {
                        format!("{:08x}", value.to_bits())
                    })
                }
                Some("double") => {
                    let value = text.parse::<f64>()?;
                    json!(if value.is_nan() {
                        "nan".into()
                    } else {
                        format!("{:016x}", value.to_bits())
                    })
                }
                _ => continue,
            };
        }
    }
    Ok(json!(rows))
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
        phase = "encode";
        let schema = physical.schema();
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

fn session(manifest: &Value, batch_size: usize, spark: bool) -> TestResult<SessionContext> {
    let builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(
            SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(TARGET_PARTITIONS),
        );
    let builder = if spark {
        configure_spark_sql(builder)
    } else {
        builder
    };
    let session = SessionContext::new_with_state(builder.build());
    spark_inputs::register_recorded_inputs(
        &session,
        &json!({"inputs": manifest["registered_inputs"]}),
        batch_size,
    )?;
    Ok(session)
}

#[tokio::test]
async fn try_numeric_arithmetic_matches_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    assert_eq!(manifest["spark_version"], "4.2.0");
    assert_eq!(
        manifest["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(manifest["ansi_modes"], json!([false, true]));
    let references = references(&manifest)?;
    let expected = references
        .iter()
        .map(|case| {
            let fields = result_fields(&case["physical_schema"])?;
            let rows = spark_rows(case, &fields)?;
            Ok((fields, rows))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(&manifest, batch_size, true)?;
        for (case, (expected_fields, expected_rows)) in references.iter().zip(&expected) {
            let id = case["id"].as_str().ok_or("missing case ID")?;
            let sql = case["sql"].as_str().ok_or("missing SQL")?;
            let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
            let actual = observe(&session, sql, ansi).await;
            let fields_match = result_fields(&actual["executed_schema"])
                .is_ok_and(|fields| &fields == expected_fields);
            let rows_match = actual["output"]["rows"] == *expected_rows;
            let matches = actual["status"] == "ok" && fields_match && rows_match;
            let record = json!({"id": id, "sql": sql, "ansi": ansi, "batch_size": batch_size,
                "target_partitions": TARGET_PARTITIONS, "matches": matches,
                "fields_match": fields_match, "rows_match": rows_match,
                "spark": case, "actual": actual});
            eprintln!("TRY_NUMERIC_ARITHMETIC {record}");
            if !matches {
                differences.push(format!("{id} ANSI={ansi} batch={batch_size}"));
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
async fn native_sql_keeps_its_arithmetic_behavior() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    let native = session(&manifest, 1, false)?;
    let configured = session(&manifest, 1, true)?;
    let mut differences = Vec::new();
    let cases = manifest["native_cases"]
        .as_array()
        .ok_or("missing native cases")?;
    assert!(!cases.is_empty(), "missing native query controls");
    for case in cases {
        let id = case["id"].as_str().ok_or("missing native case ID")?;
        let sql = case["sql"].as_str().ok_or("missing native SQL")?;
        let native = capture_native_query_observation(&native, sql).await?;
        let configured = capture_native_query_observation(&configured, sql).await?;
        let matches = native == configured;
        let record = json!({"id": id, "sql": sql, "matches": matches,
            "native_status": if native["error"].is_null() { "ok" } else { "error" },
            "configured_status": if configured["error"].is_null() { "ok" } else { "error" },
            "native": native, "configured": configured});
        eprintln!("TRY_NUMERIC_NATIVE {record}");
        if !matches {
            differences.push(sql);
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
