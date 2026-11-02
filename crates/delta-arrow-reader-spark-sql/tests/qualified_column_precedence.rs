//! Qualified columns bind before unqualified struct access in shared expressions.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::catalog::CatalogProvider;
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::error::PlanError;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_result};

fn records() -> TestResult<Vec<Value>> {
    Ok(
        include_str!("fixtures/qualified-column-precedence-spark.jsonl")
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()?,
    )
}

#[tokio::test]
async fn qualified_column_precedence_matches_spark() -> TestResult {
    let records = records()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["case_sensitive"], false);
    assert_eq!(records.len() - 1, 186);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
    }
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
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let comparison = if let Some(condition) = case["expected"]["condition"].as_str() {
                let id = case["id"].as_str().ok_or("missing ID")?;
                let (expected_condition, parts) = match id {
                    "qualified_duplicate" | "struct_duplicate" => {
                        ("AMBIGUOUS_REFERENCE", vec!["p", "id"])
                    }
                    "qualified_struct_duplicate" => ("AMBIGUOUS_REFERENCE", vec!["p", "p", "id"]),
                    "missing" => ("FIELD_NOT_FOUND", vec!["p", "missing"]),
                    "missing_qualifier" => {
                        ("UNRESOLVED_COLUMN.WITH_SUGGESTION", vec!["missing", "id"])
                    }
                    "invalid_qualified_nested" => {
                        ("INVALID_EXTRACT_BASE_FIELD_TYPE", vec!["p", "id", "item"])
                    }
                    "missing_qualified_nested" => ("FIELD_NOT_FOUND", vec!["p", "x", "id"]),
                    other => return Err(format!("unexpected error case: {other}").into()),
                };
                assert_eq!(condition, expected_condition, "{id}");
                let attribute = format!(
                    "ObjectName([{}])",
                    parts
                        .iter()
                        .map(|part| format!("Identifier({part:?})"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                let cause = if condition == "AMBIGUOUS_REFERENCE" {
                    format!("ambiguous attribute: {attribute}")
                } else {
                    format!(
                        "attribute {attribute} is missing from the schema: cannot resolve attribute"
                    )
                };
                match plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        ..SparkSqlOptions::default()
                    },
                )
                .await
                {
                    Err(DataFusionError::External(error))
                        if matches!(error.downcast_ref::<PlanError>(),
                            Some(PlanError::AnalysisError(message)) if message == &cause) =>
                    {
                        None
                    }
                    Err(error) => Some(format!("expected {condition} ({cause}), got {error}")),
                    Ok(_) => Some(format!("expected planning failure {condition}")),
                }
            } else {
                match capture_spark_query_result(&session, case).await {
                    Ok(actual) if actual == case["expected"] => None,
                    Ok(actual) => Some(format!("expected {}, got {actual}", case["expected"])),
                    Err(error) => Some(error.to_string()),
                }
            };
            if let Some(difference) = comparison {
                differences.push(format!(
                    "{} input={} ANSI={} batch={batch_size}: {difference}",
                    case["id"], case["input"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn qualified_column_precedence_leaves_native_sql_unchanged() -> TestResult {
    let records = records()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &records[0], 2)?;
    }
    let mut expected = Vec::new();
    for sql in [
        "SELECT p.id AS selected_id,p.p.id AS nested_id FROM (SELECT id,named_struct('id',99) AS p FROM inputs) p WHERE p.id<3",
        "SELECT p.id AS selected_id FROM (SELECT id,named_struct('id',99) AS p FROM empty_inputs) p",
    ] {
        let frame = native.sql(sql).await?;
        expected.push((sql, frame.schema().clone(), frame.collect().await?));
    }
    for after_spark in [false, true] {
        if after_spark {
            capture_spark_query_result(
                &configured,
                &json!({"ansi":true,
                    "sql":"SELECT p.id AS selected_id FROM (SELECT id,named_struct('id',99) AS p FROM inputs) p"}),
            )
            .await?;
        }
        for (sql, schema, batches) in &expected {
            let actual = configured.sql(sql).await?;
            assert_eq!(actual.schema(), schema, "{sql}, after_spark={after_spark}");
            assert_eq!(
                &actual.collect().await?,
                batches,
                "{sql}, after_spark={after_spark}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn qualified_namespace_overlap_matches_spark() -> TestResult {
    let records = include_str!("fixtures/qualified-overlap-review-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    assert_eq!(records[0]["case_sensitive"], false);
    assert_eq!(records.len() - 1, 18);
    assert_eq!(
        records[0]["inputs"]
            .as_array()
            .ok_or("missing inputs")?
            .len(),
        1
    );
    assert_eq!(records[0]["inputs"][0]["name"], "p.p");
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert_eq!(case["input"], "p.p");
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
    }
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(2)
                            .with_create_default_catalog_and_schema(false)
                            .with_default_catalog_and_schema("spark_catalog", "p"),
                    ),
            )
            .build(),
        );
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("p", Arc::new(MemorySchemaProvider::new()))?;
        assert!(session.register_catalog("spark_catalog", catalog).is_none());
        register_recorded_inputs(&session, &records[0], batch_size)?;
        let provider = session
            .table_provider(TableReference::full("spark_catalog", "p", "p"))
            .await?;
        assert_eq!(provider.schema().field(0).name(), "p");
        assert_eq!(provider.schema().field(1).name(), "id");
        for case in &records[1..] {
            comparisons += 1;
            let (matched, actual) = if let Some(condition) = case["expected"]["condition"].as_str()
            {
                assert_eq!(case["id"], "alias_invalid_nested");
                assert_eq!(condition, "INVALID_EXTRACT_BASE_FIELD_TYPE");
                let cause = "attribute ObjectName([Identifier(\"p\"), Identifier(\"p\"), Identifier(\"id\")]) is missing from the schema: cannot resolve attribute";
                match plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        session_timezone: "UTC".into(),
                        ..SparkSqlOptions::default()
                    },
                )
                .await
                {
                    Err(error) => {
                        let matched = matches!(&error,
                            DataFusionError::External(source)
                                if matches!(source.downcast_ref::<PlanError>(),
                                    Some(PlanError::AnalysisError(message)) if message == cause));
                        (matched, json!({"planning_error":error.to_string()}))
                    }
                    Ok(_) => (false, json!({"planning_succeeded":true})),
                }
            } else {
                match capture_spark_query_result(&session, case).await {
                    Ok(actual) => (actual == case["expected"], actual),
                    Err(error) => (false, json!({"error":error.to_string()})),
                }
            };
            if !matched {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {}, got {actual}",
                    case["id"], case["ansi"], case["expected"]
                ));
            }
        }
    }
    assert_eq!(comparisons, 36);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
