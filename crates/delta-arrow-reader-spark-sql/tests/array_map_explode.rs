//! Compare Spark generator rows and executed types for arrays and maps.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, c06_receipts, encode_value};

const INPUTS: &str = include_str!("fixtures/array-map-explode/inputs.json");
const RESULTS: &str = include_str!("fixtures/array-map-explode/spark-results.jsonl");

fn config(batch_size: usize) -> SessionConfig {
    SessionConfig::new()
        .with_batch_size(batch_size)
        .with_target_partitions(1)
}

fn spark_session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(config(batch_size)),
        )
        .build(),
    )
}

fn references(manifest: &Value) -> TestResult<Vec<Value>> {
    assert_eq!(manifest["spark_version"], "4.2.0");
    assert_eq!(
        manifest["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(manifest["ansi_modes"], json!([false, true]));
    let cases = manifest["cases"].as_array().ok_or("missing cases")?;
    let mut identities = BTreeSet::new();
    let mut references = Vec::new();
    for record in RESULTS.lines() {
        let case: Value = serde_json::from_str(record)?;
        let id = case["id"].as_str().ok_or("missing case ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(
            identities.insert((id.to_owned(), ansi)),
            "duplicate reference {id}"
        );
        let definition = cases
            .iter()
            .find(|definition| definition["id"] == id)
            .ok_or("unknown case ID")?;
        assert_eq!(case["sql"], definition["sql"], "{id}");
        assert_eq!(case["order"], definition["order"], "{id}");
        assert_eq!(case["status"], "ok", "{id}");
        assert_eq!(
            case["effective_settings"]["spark.sql.ansi.enabled"],
            ansi.to_string(),
            "{id}"
        );
        assert_eq!(
            case["effective_settings"]["spark.sql.session.timeZone"], "UTC",
            "{id}"
        );
        assert!(matches!(
            case["order"].as_str(),
            Some("ordered" | "multiset")
        ));
        assert!(case["rows"].is_array() && case["fields"].is_array(), "{id}");
        references.push(case);
    }
    assert_eq!(identities.len(), cases.len() * 2);
    assert!(!references.is_empty());
    Ok(references)
}

async fn collect_output(
    session: &SessionContext,
    physical: Arc<dyn ExecutionPlan>,
    receipt: &mut Value,
) -> TestResult {
    let schema = physical.schema();
    receipt["before_collect"] = c06_receipts::schema(schema.as_ref());
    let batches = collect(physical.clone(), session.task_ctx()).await;
    let schema = physical.schema();
    receipt["physical"] = c06_receipts::schema(schema.as_ref());
    let batches = batches?;
    receipt["output"] = c06_receipts::batches(&batches, encode_value)?;
    for batch in &batches {
        spark_results::validate_batch(schema.as_ref(), batch)?;
    }
    receipt["executed_fields"] = spark_results::capture_schema(schema.fields(), None)?["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| json!({"name":field["name"],"type":field["type"]}))
        .collect::<Vec<_>>()
        .into();
    Ok(())
}

async fn observe(session: &SessionContext, sql: &str, ansi_mode: bool) -> Value {
    let mut receipt = c06_receipts::new("public_physical_collect");
    let mut stage = "resolve";
    let result: TestResult = async {
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
        receipt["public_names"] = json!(query.column_names);
        receipt["analyzed"] = c06_receipts::schema(query.logical_plan.schema().as_arrow());
        stage = "create_physical_plan";
        let physical = query.create_physical_plan(session).await?;
        stage = "collect_output";
        collect_output(session, physical, &mut receipt).await
    }
    .await;
    if let Err(error) = result {
        receipt["error"] = c06_receipts::failure(error.as_ref(), stage);
    }
    c06_receipts::finish(&mut receipt);
    receipt
}

fn comparison_rows(rows: &Value, multiset: bool) -> Value {
    let mut rows = rows.clone();
    if multiset && let Some(rows) = rows.as_array_mut() {
        rows.sort_by_cached_key(Value::to_string);
    }
    rows
}

#[tokio::test]
async fn array_map_explode_matches_spark_values_and_executed_types() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    let references = references(&manifest)?;
    let inputs = json!({"inputs":manifest["registered_inputs"]});
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = spark_session(batch_size);
        let providers = spark_inputs::register_recorded_inputs_with_slices_and_receipts(
            &session, &inputs, batch_size,
        )
        .await?;
        c06_receipts::emit(
            "array_map_explode_inputs",
            json!({"batch":batch_size}),
            providers.clone(),
        );
        for provider in providers.as_array().ok_or("missing input receipts")? {
            assert_eq!(
                provider["provider"]["readback"]["rows"], provider["input_definition"]["rows"],
                "registered input {}",
                provider["name"]
            );
        }
        for case in &references {
            let sql = case["sql"].as_str().ok_or("missing SQL")?;
            let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
            let actual = observe(&session, sql, ansi).await;
            let multiset = case["order"] == "multiset";
            let matches = actual["error"].is_null()
                && actual["executed_fields"] == case["fields"]
                && comparison_rows(&actual["output"]["rows"], multiset)
                    == comparison_rows(&case["rows"], multiset);
            c06_receipts::emit(
                "array_map_explode",
                json!({"id":case["id"],"ansi":ansi,"batch":batch_size,"sql":sql,
                    "matches":matches,"spark":case}),
                actual.clone(),
            );
            if !matches {
                differences.push(format!(
                    "{} ANSI={ansi} batch={batch_size}: expected rows={} fields={}, got error={} rows={} fields={}",
                    case["id"], case["rows"], case["fields"], actual["error"],
                    actual["output"]["rows"], actual["executed_fields"],
                ));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} differences:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn native_unnest_keeps_its_values_and_executed_types() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    let inputs = json!({"inputs":manifest["registered_inputs"]});
    let sql = "SELECT id, unnest(a) AS item FROM collections ORDER BY id, item NULLS FIRST";
    for batch_size in [1, 32] {
        let native = SessionContext::new_with_config(config(batch_size));
        let configured = spark_session(batch_size);
        for (session, name) in [(&native, "native"), (&configured, "configured")] {
            spark_inputs::register_recorded_inputs(session, &inputs, batch_size)?;
            let frame = session.sql(sql).await?;
            let mut receipt = c06_receipts::new("native_dataframe_physical_collect");
            receipt["analyzed"] = c06_receipts::schema(frame.schema().as_arrow());
            let physical = frame.create_physical_plan().await?;
            collect_output(session, physical, &mut receipt).await?;
            c06_receipts::finish(&mut receipt);
            c06_receipts::emit(
                "array_map_explode_native",
                json!({"batch":batch_size,"session":name,"sql":sql}),
                receipt.clone(),
            );
            assert_eq!(
                receipt["output"]["rows"],
                json!([["0", null], ["0", "p"], ["0", "q"], ["0", "q"], ["3", null]]),
                "{name} batch={batch_size}: {sql}"
            );
            assert_eq!(
                receipt["executed_fields"],
                json!([{"name":"id","type":"integer"},{"name":"item","type":"string"}]),
                "{name} batch={batch_size}: {sql}"
            );
        }
    }
    Ok(())
}
