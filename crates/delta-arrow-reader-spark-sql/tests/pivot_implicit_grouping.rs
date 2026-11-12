//! PIVOT groups by source columns unused by the pivot key and aggregates.

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
use spark_results::{TestResult, c06_receipts, capture_schema, encode_value, validate_batch};

const INPUTS: &str = include_str!("fixtures/pivot-implicit-grouping/inputs.json");
const RESULTS: &str = include_str!("fixtures/pivot-implicit-grouping/spark-results.jsonl");

fn names_and_types(schema: &Value) -> TestResult<Vec<Value>> {
    Ok(schema["fields"]
        .as_array()
        .ok_or("missing executed fields")?
        .iter()
        .map(|field| json!({"name":field["name"],"type":field["type"]}))
        .collect())
}

async fn observe(session: &SessionContext, sql: &str, ansi_mode: bool) -> TestResult<Value> {
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
    let physical = query.create_physical_plan(session).await?;
    let batches = collect(physical.clone(), session.task_ctx()).await?;
    let schema = physical.schema();
    for batch in &batches {
        validate_batch(schema.as_ref(), batch)?;
    }
    let executed_schema = capture_schema(schema.fields(), None)?;
    Ok(json!({
        "physical": c06_receipts::schema(schema.as_ref()),
        "fields": names_and_types(&executed_schema)?,
        "output": c06_receipts::batches(&batches, encode_value)?,
    }))
}

#[tokio::test]
async fn pivot_implicit_grouping_matches_spark() -> TestResult {
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
    assert_eq!(references.len(), 36);
    assert_eq!(references.len(), definitions.len() * 2);
    let mut identities = BTreeSet::new();
    for case in &references {
        let id = case["id"].as_str().ok_or("missing case ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(identities.insert((id, ansi)), "duplicate reference {id}");
        let definition = definitions
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
        assert_eq!(
            case["fields"],
            json!(names_and_types(&case["physical_schema"])?),
            "{id}"
        );
        let rows = case["rows"].as_array().ok_or("missing rows")?;
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        assert!(
            rows.len() <= 1 || sql.contains(" ORDER BY "),
            "{id} needs ORDER BY"
        );
    }

    let inputs = json!({"inputs":manifest["registered_inputs"]});
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
            let actual = observe(&session, sql, ansi)
                .await
                .unwrap_or_else(|error| json!({"error":error.to_string()}));
            let matches =
                actual["fields"] == case["fields"] && actual["output"]["rows"] == case["rows"];
            if !matches {
                differences.push(format!(
                    "{} ANSI={ansi} batch={batch_size} expected fields={} rows={}, got fields={} rows={} error={}",
                    case["id"], case["fields"], case["rows"], actual["fields"],
                    actual["output"]["rows"], actual["error"],
                ));
            }
            c06_receipts::emit(
                "pivot_implicit_grouping",
                json!({"id":case["id"],"ansi":ansi,"batch":batch_size,"sql":sql,
                    "matches":matches,"spark":case}),
                actual,
            );
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
