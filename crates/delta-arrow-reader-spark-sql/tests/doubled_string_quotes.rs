//! Compare collected string literals with independently executed Spark queries.
//! The #1009 fixture records default parser settings on Spark 4.2.0
//! (revision 32f7299601108917fb01920a54e084595b7b3bf8).

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(dead_code, reason = "This target only needs the batch encoder.")]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_batches};

fn session() -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    )
}

async fn collect_spark(session: &SessionContext, sql: &str, ansi_mode: bool) -> TestResult<Value> {
    let query = plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        },
    )
    .await?;
    let physical = query.create_physical_plan(session).await?;
    let batches = collect(physical, session.task_ctx()).await?;
    capture_batches(&batches)
}

fn field_types(batch: &Value) -> TestResult<Value> {
    let fields = batch["schema"]["fields"]
        .as_array()
        .ok_or("missing collected fields")?;
    Ok(json!(
        fields
            .iter()
            .map(|field| json!({"name":field["name"],"type":field["type"]}))
            .collect::<Vec<_>>()
    ))
}

#[tokio::test]
async fn spark_string_literal_values_and_executed_types() -> TestResult {
    let reference =
        include_str!("fixtures/doubled-string-quotes/doubled-string-quotes-spark.jsonl");
    let session = session();
    let mut seen = BTreeSet::new();
    let mut differences = 0;
    for line in reference.lines() {
        let case: Value = serde_json::from_str(line)?;
        let id = case["id"].as_str().ok_or("missing case ID")?.to_owned();
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(
            seen.insert((id.clone(), ansi)),
            "duplicate case {id} ANSI={ansi}"
        );
        assert!(
            case["rows"].is_array() && case["fields"].is_array(),
            "{case}"
        );

        let actual = collect_spark(&session, sql, ansi).await;
        let (rows_match, types_match) = match &actual {
            Ok(actual) => {
                let batches = actual["batches"].as_array().ok_or("missing batches")?;
                let actual_fields = batches
                    .iter()
                    .map(field_types)
                    .collect::<TestResult<Vec<_>>>()?;
                (
                    actual["rows"] == case["rows"],
                    !actual_fields.is_empty()
                        && actual_fields.iter().all(|fields| fields == &case["fields"]),
                )
            }
            Err(_) => (false, false),
        };
        let comparison = json!({
            "id":id,"ansi":ansi,"sql":sql,"purpose":case["purpose"],
            "expected":{"rows":case["rows"],"fields":case["fields"]},
            "actual":actual.as_ref().ok(),"error":actual.as_ref().err().map(ToString::to_string),
            "rows_match":rows_match,"types_match":types_match,
        });
        eprintln!("string quote comparison: {comparison}");
        differences += usize::from(!rows_match || !types_match);
    }
    assert_eq!(seen.len(), 46, "missing Spark cases");
    for (id, ansi) in &seen {
        assert!(
            seen.contains(&(id.clone(), !ansi)),
            "missing other ANSI mode for {id}"
        );
    }
    assert_eq!(
        differences,
        0,
        "{differences} of {} Spark comparisons differ",
        seen.len()
    );
    Ok(())
}

#[tokio::test]
async fn spark_setup_preserves_native_doubled_quotes() -> TestResult {
    let sql = "SELECT 'it''s' AS quoted";
    let expected = SessionContext::new().sql(sql).await?.collect().await?;
    let session = session();
    assert_eq!(session.sql(sql).await?.collect().await?, expected);
    for ansi in [false, true] {
        collect_spark(&session, sql, ansi).await?;
        let actual = session.sql(sql).await?.collect().await?;
        assert_eq!(actual, expected, "after Spark query with ANSI={ansi}");
    }
    eprintln!(
        "native string quote control: {}",
        capture_batches(&expected)?
    );
    Ok(())
}
