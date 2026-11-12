//! Compare row-dependent array subscripts with recorded Spark results.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
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

const INPUTS: &str = include_str!("fixtures/array-subscript-expressions/inputs.json");
const REFERENCE: &str = include_str!("fixtures/array-subscript-expressions/spark-reference.jsonl");

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

fn register_inputs(session: &SessionContext, manifest: &Value, batch_size: usize) -> TestResult {
    spark_inputs::register_recorded_inputs_with_partition_count(
        session,
        &json!({"inputs": manifest["registered_inputs"]}),
        batch_size,
        1,
    )
}

async fn observe(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let sql = case["sql"].as_str().ok_or("missing SQL")?;
    let ansi_mode = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
    let mut actual = json!({"physical_schema":null,"fields":null,"output":null,"error":null});
    let mut stage = "resolve";
    let execution: TestResult<_> = async {
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
        stage = "create_physical_plan";
        let physical = query.create_physical_plan(session).await?;
        actual["physical_schema"] = c06_receipts::schema(physical.schema().as_ref());
        stage = "collect";
        let batches = collect(physical.clone(), session.task_ctx()).await?;
        Ok((physical.schema(), batches))
    }
    .await;
    match execution {
        Ok((schema, batches)) => {
            for batch in &batches {
                spark_results::validate_batch(schema.as_ref(), batch)?;
            }
            let schema = spark_results::capture_schema(schema.fields(), None)?;
            actual["fields"] = json!(
                schema["fields"]
                    .as_array()
                    .ok_or("missing physical fields")?
                    .iter()
                    .map(|field| json!({"name":field["name"],"type":field["type"]}))
                    .collect::<Vec<_>>()
            );
            actual["output"] = c06_receipts::batches(&batches, encode_value)?;
        }
        Err(error) => actual["error"] = c06_receipts::failure(error.as_ref(), stage),
    }
    Ok(actual)
}

async fn replay(bounds: bool) -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    let cases = manifest["cases"].as_array().ok_or("missing cases")?;
    let references = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let mut identities = BTreeSet::new();
    for case in &references {
        let id = case["id"].as_str().ok_or("missing case ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(identities.insert((id, ansi)), "duplicate reference {id}");
        let definition = cases
            .iter()
            .find(|definition| definition["id"] == id)
            .ok_or("unknown case ID")?;
        assert_eq!(case["sql"], definition["sql"], "{id}");
        assert_eq!(case["scope"], definition["scope"], "{id}");
    }
    assert_eq!(identities.len(), cases.len() * 2);

    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = spark_session(batch_size);
        register_inputs(&session, &manifest, batch_size)?;
        for case in references
            .iter()
            .filter(|case| (case["scope"] == "observation") == bounds)
        {
            let actual = match observe(&session, case).await {
                Ok(actual) => actual,
                Err(error) => json!({"capture_error":error.to_string()}),
            };
            let matches = match (case["scope"].as_str(), case["status"].as_str()) {
                (_, Some("ok")) => Some(
                    actual["output"]["rows"] == case["rows"] && actual["fields"] == case["fields"],
                ),
                (Some("boundary"), Some("error")) => None,
                (Some("observation"), Some("error")) => Some(
                    actual["error"]["display"]
                        .as_str()
                        .is_some_and(|error| error.contains("out of bounds")),
                ),
                _ => return Err(format!("unexpected reference outcome: {case}").into()),
            };
            eprintln!(
                "ARRAY_SUBSCRIPT_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "sql":case["sql"],"matches":matches,"actual":actual})
            );
            if matches == Some(false) {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {} rows={} fields={}, got error={} rows={} fields={}",
                    case["id"], case["ansi"], case["status"], case["rows"], case["fields"],
                    actual.get("error").unwrap_or(&actual["capture_error"]),
                    actual["output"]["rows"], actual["fields"],
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn array_subscript_expressions_match_spark_values_and_executed_types() -> TestResult {
    replay(false).await
}

#[tokio::test]
async fn array_subscript_bounds_match_spark_outcomes() -> TestResult {
    replay(true).await
}

#[tokio::test]
async fn native_array_subscripts_keep_their_indexing_rules() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    let native = SessionContext::new_with_config(config(32));
    let configured = spark_session(32);
    for session in [&native, &configured] {
        register_inputs(session, &manifest, 32)?;
    }
    let sql = "SELECT id, ARRAY[7,9][id+1] AS indexed, ARRAY[7,9][-1] AS last FROM input_table ORDER BY id";
    let expected = spark_results::capture_native_query_observation(&native, sql).await?;
    let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
    assert_eq!(
        expected["rows"],
        json!([["0", "7", "9"], ["1", "9", "9"], ["2", null, "9"]]),
        "{expected}"
    );
    assert_eq!(actual, expected, "{sql}");
    Ok(())
}
