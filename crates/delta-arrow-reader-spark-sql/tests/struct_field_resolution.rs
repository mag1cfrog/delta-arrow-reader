//! A selected local STRUCT cannot fall through to another field or outer scope.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, SparkSqlQuery, configure_spark_sql, plan_query};
use sail_plan::error::PlanError;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{TestResult, capture_spark_query_observation, capture_spark_query_result};

fn records(source: &str) -> TestResult<Vec<Value>> {
    Ok(source
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn identity(case: &Value) -> TestResult<(&str, &str, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing ID")?,
        case["input"].as_str().ok_or("missing input")?,
        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
    ))
}

fn assert_partition(
    original: &str,
    selected: &str,
    selected_count: usize,
    pending_count: usize,
    pending_ids: &[&str],
) -> TestResult {
    assert_eq!(original.lines().next(), selected.lines().next());
    let original_records = records(original)?;
    let selected_records = records(selected)?;
    assert_eq!(original_records.len() - 1, selected_count + pending_count);
    assert_eq!(selected_records.len() - 1, selected_count);
    let mut original_lines = BTreeMap::new();
    let mut pending = BTreeSet::new();
    for (case, line) in original_records[1..].iter().zip(original.lines().skip(1)) {
        let key = identity(case)?;
        assert!(original_lines.insert(key, line).is_none());
        if pending_ids.contains(&key.0) {
            assert!(pending.insert(key));
        }
    }
    let mut selected_keys = BTreeSet::new();
    for (case, line) in selected_records[1..].iter().zip(selected.lines().skip(1)) {
        let key = identity(case)?;
        assert!(selected_keys.insert(key));
        assert!(!pending.contains(&key));
        assert_eq!(original_lines.get(&key), Some(&line));
    }
    assert_eq!(pending.len(), pending_count);
    assert_eq!(
        selected_keys.union(&pending).copied().collect::<BTreeSet<_>>(),
        original_lines.keys().copied().collect::<BTreeSet<_>>()
    );
    Ok(())
}

#[test]
fn field_resolution_partitions_preserve_all_parent_records() -> TestResult {
    assert_partition(
        include_str!("fixtures/qualified-column-precedence-spark.jsonl"),
        include_str!("fixtures/qualified-column-resolution-spark.jsonl"),
        174,
        12,
        &["using_hidden", "using_stars"],
    )?;
    assert_partition(
        include_str!("fixtures/struct-identifier-names-spark.jsonl"),
        include_str!("fixtures/struct-field-resolution-identifier-spark.jsonl"),
        6,
        42,
        &[
            "identifier_nested",
            "identifier_qualified",
            "identifier_deep",
            "identifier_bracket",
            "identifier_explicit_alias",
            "identifier_plain",
            "identifier_generated",
        ],
    )?;
    Ok(())
}

fn session(batch_size: usize) -> TestResult<SessionContext> {
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
    catalog.register_schema("global_temp", Arc::new(MemorySchemaProvider::new()))?;
    assert!(session.register_catalog("spark_catalog", catalog).is_none());
    Ok(session)
}

fn matches_planning_error(error: &DataFusionError, id: &str, condition: &str) -> bool {
    let DataFusionError::External(source) = error else {
        return false;
    };
    let Some(PlanError::AnalysisError(message)) = source.downcast_ref::<PlanError>() else {
        return false;
    };
    match condition {
        "FIELD_NOT_FOUND"
        | "AMBIGUOUS_REFERENCE_TO_FIELDS"
        | "INVALID_EXTRACT_BASE_FIELD_TYPE"
        | "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" => {
            let child = match id {
                "missing"
                | "missing_direct"
                | "missing_deep"
                | "missing_identifier"
                | "missing_bracket"
                | "missing_computed"
                | "missing_qualified"
                | "missing_array_field"
                | "missing_multipart"
                | "scalar_tail"
                | "scalar_array_field"
                | "scalar_array_bracket" => "missing",
                "missing_qualified_nested"
                | "alias_invalid_nested"
                | "missing_before_lower_priority"
                | "outer_shadow_missing"
                | "outer_shadow_duplicate" => "id",
                "identifier_consumer"
                | "invalid_qualified_nested"
                | "duplicate_direct"
                | "duplicate_case"
                | "duplicate_bracket"
                | "duplicate_computed"
                | "duplicate_array_field" => "item",
                _ => return false,
            };
            message.starts_with(&format!(
                "cannot resolve attribute: [{condition}] field {child:?} "
            ))
        }
        "AMBIGUOUS_REFERENCE" => {
            let parts = match id {
                "qualified_duplicate" | "struct_duplicate" => vec!["p", "id"],
                "qualified_struct_duplicate" => vec!["p", "p", "id"],
                _ => return false,
            };
            let name = parts
                .iter()
                .map(|part| format!("Identifier({part:?})"))
                .collect::<Vec<_>>()
                .join(", ");
            message == &format!("ambiguous attribute: ObjectName([{name}])")
        }
        "UNRESOLVED_COLUMN.WITH_SUGGESTION" => {
            id == "missing_qualifier"
                && message == "attribute ObjectName([Identifier(\"missing\"), Identifier(\"id\")]) is missing from the schema: cannot resolve attribute"
        }
        _ => false,
    }
}

#[test]
fn planning_conditions_require_the_resolver_cause_and_requested_child() {
    let condition = "FIELD_NOT_FOUND";
    let message = "cannot resolve attribute: [FIELD_NOT_FOUND] field \"item\" is absent";
    assert!(!matches_planning_error(
        &DataFusionError::Plan(message.into()),
        "identifier_consumer",
        condition,
    ));
    assert!(!matches_planning_error(
        &DataFusionError::External(Box::new(PlanError::AnalysisError(
            format!("unrelated text: {message}"),
        ))),
        "identifier_consumer",
        condition,
    ));
    assert!(!matches_planning_error(
        &DataFusionError::External(Box::new(PlanError::AnalysisError(
            message.replace("item", "other"),
        ))),
        "identifier_consumer",
        condition,
    ));
}

async fn check_execution_error(
    session: &SessionContext,
    query: SparkSqlQuery,
    condition: &str,
) -> TestResult {
    assert_eq!(condition, "INVALID_ARRAY_INDEX");
    let physical = query.create_physical_plan(session).await?;
    let schema = physical.schema();
    spark_results::validate_physical_schema(
        query.logical_plan.schema().fields(),
        &query.column_names,
        schema.fields(),
    )?;
    match collect(physical, session.task_ctx()).await {
        Err(error) if error.to_string().contains("[INVALID_ARRAY_INDEX]") => Ok(()),
        Err(error) => Err(format!("expected execution {condition}, got {error}").into()),
        Ok(batches) => {
            for batch in batches {
                spark_results::validate_batch(schema.as_ref(), &batch)?;
            }
            Err(format!("expected execution {condition}, collection succeeded").into())
        }
    }
}

async fn compare_cases(source: &str, count: usize) -> TestResult {
    let records = records(source)?;
    assert_eq!(records.len() - 1, count);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert(identity(case)?));
    }
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size)?;
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let id = case["id"].as_str().ok_or("missing ID")?;
            let difference = if let Some(condition) = case["expected"]["condition"].as_str() {
                let result = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        session_timezone: "UTC".into(),
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                match result {
                    Ok(query) if case["phase"] == "execution" => {
                        check_execution_error(&session, query, condition)
                            .await
                            .err()
                            .map(|error| error.to_string())
                    }
                    Err(error) if case["phase"] == "execution" => {
                        Some(format!("expected execution {condition}, planning failed: {error}"))
                    }
                    Err(error) if matches_planning_error(&error, id, condition) => None,
                    Err(error) => Some(format!("expected planning {condition}, got {error}")),
                    Ok(_) => Some(format!("expected planning {condition}, got a logical plan")),
                }
            } else {
                if let Some(stages) = case.get("schema_stages") {
                    assert_eq!(case["expected"]["schema"], stages["analyzed"]);
                }
                match capture_spark_query_observation(&session, case).await {
                    Ok(actual)
                        if case.get("schema_stages").is_some()
                            && actual["physical_schema"] != case["schema_stages"]["executed"] =>
                    {
                        Some(format!(
                            "expected executed schema {}, got {}",
                            case["schema_stages"]["executed"], actual["physical_schema"]
                        ))
                    }
                    Ok(actual) if actual["result"] == case["expected"] => None,
                    Ok(actual) => Some(format!("expected {}, got {actual}", case["expected"])),
                    Err(error) => Some(error.to_string()),
                }
            };
            if let Some(difference) = difference {
                differences.push(format!(
                    "{} input={} ANSI={} batch={batch_size}: {difference}",
                    case["id"], case["input"], case["ansi"]
                ));
            }
        }
    }
    assert_eq!(comparisons, count * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn local_resolution_matches_all_174_scoped_qualified_references() -> TestResult {
    compare_cases(
        include_str!("fixtures/qualified-column-resolution-spark.jsonl"),
        174,
    )
    .await
}

#[tokio::test]
async fn namespace_overlap_matches_all_18_references() -> TestResult {
    compare_cases(
        include_str!("fixtures/qualified-overlap-review-spark.jsonl"),
        18,
    )
    .await
}

#[tokio::test]
async fn known_struct_rejects_all_six_identifier_consumers_during_planning() -> TestResult {
    compare_cases(
        include_str!("fixtures/struct-field-resolution-identifier-spark.jsonl"),
        6,
    )
    .await
}

#[tokio::test]
async fn struct_field_resolution_boundaries_match_all_186_independent_references() -> TestResult {
    let source = include_str!("fixtures/struct-field-resolution-spark.jsonl");
    let captured = records(source)?;
    assert!(captured[0]["spark_build"].as_str().ok_or("missing Spark build")?
        .contains("revision=32f7299601108917fb01920a54e084595b7b3bf8"));
    for (phase, expected_count) in [("success", 71), ("planning", 114), ("execution", 1)] {
        assert_eq!(
            captured[1..].iter().filter(|case| case["phase"] == phase).count(),
            expected_count,
        );
    }
    compare_cases(source, 186).await
}

async fn native_snapshot(
    session: &SessionContext,
    sql: &str,
) -> TestResult<(String, Vec<RecordBatch>)> {
    let frame = session.sql(sql).await?;
    let analyzed = frame.schema().clone();
    let physical = frame.create_physical_plan().await?;
    let declaration = format!(
        "{analyzed:?}\n{:?}\n{}",
        physical.schema(),
        datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
    );
    Ok((declaration, collect(physical, session.task_ctx()).await?))
}

#[tokio::test]
async fn local_resolution_leaves_native_sql_unchanged() -> TestResult {
    let records = records(include_str!("fixtures/qualified-column-resolution-spark.jsonl"))?;
    let native = SessionContext::new();
    let configured = session(2)?;
    for session in [&native, &configured] {
        register_recorded_inputs(session, &records[0], 2)?;
    }
    let mut expected = Vec::new();
    for sql in [
        "SELECT p.id AS selected_id,p.p.id AS nested_id FROM (SELECT id,named_struct('id',99) AS p FROM inputs) p WHERE p.id<3",
        "SELECT p.id AS selected_id FROM (SELECT id,named_struct('id',99) AS p FROM empty_inputs) p",
    ] {
        expected.push((sql, native_snapshot(&native, sql).await?));
    }
    let missing = "SELECT (named_struct('item',1)).missing AS result";
    let expected_error = native.sql(missing).await.unwrap_err().to_string();
    for after_spark in [false, true] {
        if after_spark {
            capture_spark_query_result(
                &configured,
                &json!({"ansi":true,"sql":
                    "SELECT p.id AS selected_id FROM (SELECT id,named_struct('id',99) AS p FROM inputs) p"}),
            )
            .await?;
        }
        for (sql, snapshot) in &expected {
            assert_eq!(
                &native_snapshot(&configured, sql).await?,
                snapshot,
                "{sql}, after_spark={after_spark}"
            );
        }
        assert_eq!(configured.sql(missing).await.unwrap_err().to_string(), expected_error);
    }
    Ok(())
}
