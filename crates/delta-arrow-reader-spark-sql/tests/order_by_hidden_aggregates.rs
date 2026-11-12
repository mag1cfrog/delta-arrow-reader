//! Compare hidden aggregate sort keys with Spark's ordered rows and executed types.

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

const INPUTS: &str = include_str!("fixtures/order-by-hidden-aggregates/inputs.json");
const RESULTS: &str = include_str!("fixtures/order-by-hidden-aggregates/spark-results.jsonl");
const FAILURES: &str = include_str!("fixtures/order-by-hidden-aggregates/spark-failures.jsonl");
const SPARK_REJECTIONS: &[&str] = &[
    "group_having",
    "group_having_shared",
    "distinct_group_hidden_probe",
    "distinct_global_hidden_probe",
    "ungrouped_column_with_aggregate_probe",
    "group_order_alias_collision",
    "group_window_hidden_aggregate",
];

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

async fn collect_output(
    session: &SessionContext,
    physical: Arc<dyn ExecutionPlan>,
) -> TestResult<Value> {
    let batches = collect(physical.clone(), session.task_ctx()).await?;
    let schema = physical.schema();
    for batch in &batches {
        spark_results::validate_batch(schema.as_ref(), batch)?;
    }
    let fields = spark_results::capture_schema(schema.fields(), None)?["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| json!({"name":field["name"],"type":field["type"]}))
        .collect::<Vec<_>>();
    Ok(
        json!({"fields":fields,"physical_schema":c06_receipts::schema(schema.as_ref()),
        "output":c06_receipts::batches(&batches, encode_value)?}),
    )
}

async fn observe(session: &SessionContext, sql: &str, ansi_mode: bool) -> Value {
    let mut stage = "resolve";
    let result: TestResult<_> = async {
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
        stage = "collect";
        collect_output(session, physical).await
    }
    .await;
    result.unwrap_or_else(|error| json!({"error":c06_receipts::failure(error.as_ref(), stage)}))
}

#[tokio::test]
async fn hidden_aggregate_sorts_match_spark_values_and_executed_types() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    let cases = manifest["cases"].as_array().ok_or("missing cases")?;
    let references = RESULTS
        .lines()
        .chain(FAILURES.lines())
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
        let status = if SPARK_REJECTIONS.contains(&id) {
            "error"
        } else {
            "ok"
        };
        assert_eq!(case["status"], status, "{id}");
        if status == "ok" {
            assert!(case["rows"].is_array() && case["fields"].is_array(), "{id}");
        }
    }
    assert_eq!(identities.len(), cases.len() * 2);

    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = spark_session(batch_size);
        register_inputs(&session, &manifest, batch_size)?;
        for case in &references {
            let sql = case["sql"].as_str().ok_or("missing SQL")?;
            let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
            let actual = observe(&session, sql, ansi).await;
            let matches = (case["status"] == "ok").then(|| {
                actual["output"]["rows"] == case["rows"] && actual["fields"] == case["fields"]
            });
            let mut observation = json!({"id":case["id"],"ansi":ansi,"batch":batch_size,
                "sql":sql,"matches":matches,"actual":actual});
            if case["status"] == "error" {
                observation["spark"] = case.clone();
                eprintln!("HIDDEN_AGGREGATE_DIAGNOSTIC {observation}");
            } else {
                eprintln!("HIDDEN_AGGREGATE_OBSERVATION {observation}");
            }
            if matches == Some(false) {
                differences.push(format!(
                    "{} ANSI={ansi} batch={batch_size}: expected rows={} fields={}, got error={} rows={} fields={}",
                    case["id"], case["rows"], case["fields"], actual["error"],
                    actual["output"]["rows"], actual["fields"],
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn native_hidden_aggregate_sorts_keep_their_order_and_executed_types() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    let native = SessionContext::new_with_config(config(32));
    let configured = spark_session(32);
    let sql = "SELECT k FROM inputs GROUP BY k ORDER BY MAX(f) ASC NULLS FIRST, k ASC NULLS FIRST";
    for session in [&native, &configured] {
        register_inputs(session, &manifest, 32)?;
        let physical = session.sql(sql).await?.create_physical_plan().await?;
        let actual = collect_output(session, physical).await?;
        assert_eq!(
            actual["output"]["rows"],
            json!([["1"], [null], ["2"], ["3"], ["0"]]),
            "{sql}"
        );
        assert_eq!(
            actual["fields"],
            json!([{"name":"k","type":"integer"}]),
            "{sql}"
        );
    }
    Ok(())
}
