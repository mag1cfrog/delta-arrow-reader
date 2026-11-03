//! CASE result declarations and executed schemas follow independent Spark observations.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_spark_query_observation};

#[tokio::test]
async fn case_results_match_spark_declarations_execution_and_errors() -> TestResult {
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for (group, source, count) in [
        ("base", include_str!("fixtures/case-result-nullability-spark.jsonl"), 324),
        ("parents", include_str!("fixtures/case-result-nullability-parents-spark.jsonl"), 128),
        ("pruning", include_str!("fixtures/case-result-nullability-pruning-spark.jsonl"), 64),
    ] {
    let records = source.lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
    assert_eq!(records.len() - 1, count);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        if case["expected"].get("condition").is_none() {
            assert_eq!(case["expected"]["schema"], case["schema_stages"]["analyzed"]);
            assert_eq!(case["schema_stages"]["executed_before_collect"], case["schema_stages"]["executed"]);
        }
    }
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new().with_default_features().with_config(
                    SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2),
                ),
            ).build(),
        );
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let actual = capture_spark_query_observation(&session, case).await;
            let matches = match &actual {
                Ok(actual) => actual["result"] == case["expected"]
                    && actual["physical_schema"] == case["schema_stages"]["executed"],
                Err(error) => match case["expected"]["condition"].as_str() {
                    Some("CAST_INVALID_INPUT") => error.to_string().contains("[CAST_INVALID_INPUT]")
                        || error.to_string().contains("Cannot cast string 'bad'"),
                    Some("USER_RAISED_EXCEPTION") => error.to_string().contains("case-required"),
                    Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE") => error.to_string().contains("CASE WHEN condition must be BOOLEAN"),
                    Some("INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION") => error.to_string().contains("[CAST_INVALID_INPUT]"),
                    _ => false,
                },
            };
            eprintln!("CASE_SCHEMA_OBSERVATION {}", json!({
                "group":group, "id":case["id"], "input":case["input"], "ansi":case["ansi"],
                "batch":batch_size, "observation":actual.as_ref().ok(),
                "error":actual.as_ref().err().map(ToString::to_string), "matches":matches,
            }));
            if !matches {
                differences.push(format!("{group}: {} input={} ANSI={} batch={batch_size}: expected {} physical={}, got {actual:?}",
                    case["id"], case["input"], case["ansi"], case["expected"], case["schema_stages"]["executed"]));
            }
        }
    }
    }
    assert_eq!(comparisons, 1032);
    assert!(differences.is_empty(), "{} differences:\n{}", differences.len(), differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_native_case_semantics() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT CASE WHEN true THEN 1 ELSE CAST(NULL AS INT) END AS r",
        "SELECT CASE WHEN false THEN CAST(NULL AS INT) ELSE 1 END AS r",
        "SELECT CASE 1 WHEN 1 THEN 1 ELSE CAST(NULL AS INT) END AS r",
        "SELECT CASE WHEN v IS NOT NULL THEN v ELSE 2 END AS r FROM (VALUES (1),(NULL)) t(v)",
    ] {
        let expected = native.sql(sql).await?;
        let actual = configured.sql(sql).await?;
        assert_eq!(actual.schema(), expected.schema(), "{sql}");
        assert_eq!(actual.collect().await?, expected.collect().await?, "{sql}");
    }
    let sql = "SELECT CASE true WHEN false THEN v ELSE 1 END AS r FROM native_case_input";
    for session in [&native, &configured] {
        session.register_batch("native_case_input", arrow::record_batch::RecordBatch::try_from_iter([
            ("v", std::sync::Arc::new(arrow::array::Int32Array::from(vec![Some(1), None]))
                as arrow::array::ArrayRef),
        ])?)?;
        let frame = session.sql(sql).await?;
        assert!(frame.schema().field(0).is_nullable(), "{sql}");
        let physical = frame.create_physical_plan().await?;
        assert!(physical.schema().field(0).is_nullable(), "native Boolean CASE declaration changed: {sql}");
        let batches = datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
        let mut values = Vec::new();
        for batch in &batches {
            let column = batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>()
                .ok_or("expected native LONG result")?;
            values.extend(column.iter());
        }
        assert_eq!(values, vec![Some(1), Some(1)], "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn case_result_types_and_analyzer_scan_match_spark() -> TestResult {
    let records = include_str!("fixtures/case-result-nullability-review-spark.jsonl")
        .lines().map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
    assert_eq!(records.len() - 1, 12);
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(configure_spark_sql(
            SessionStateBuilder::new().with_default_features().with_config(
                SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2),
            ),
        ).build());
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let actual = capture_spark_query_observation(&session, case).await;
            let matches = match &actual {
                Ok(actual) => actual["result"] == case["expected"]
                    && actual["physical_schema"] == case["schema_stages"]["executed"],
                Err(error) if case["expected"]["condition"] == "DATATYPE_MISMATCH.DATA_DIFF_TYPES" => {
                    let message = error.to_string();
                    message.contains("[DATATYPE_MISMATCH.DATA_DIFF_TYPES]")
                        || (message.contains("Failed to coerce then (")
                            && message.contains("to common types in CASE WHEN expression"))
                }
                _ => false,
            };
            eprintln!("CASE_721_REVIEW_OBSERVATION {}", json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "observation":actual.as_ref().ok(), "error":actual.as_ref().err().map(ToString::to_string),
                "matches":matches,
            }));
            if !matches {
                differences.push(format!("{} ANSI={} batch={batch_size}: expected {} physical={}, got {actual:?}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]["executed"]));
            }
        }
    }
    assert_eq!(comparisons, 24);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn case_common_types_and_internal_case_boundaries_match_spark() -> TestResult {
    assert_case_schema_stages_and_rows(
        include_str!("fixtures/case-result-nullability-boundaries-spark.jsonl"),
        6,
        "CASE_721_BOUNDARY_OBSERVATION",
    ).await
}

#[tokio::test]
async fn case_common_types_preserve_live_nested_metadata_and_refinement() -> TestResult {
    assert_case_schema_stages_and_rows(
        include_str!("fixtures/case-result-nullability-common-metadata-spark.jsonl"),
        12,
        "CASE_721_COMMON_METADATA_OBSERVATION",
    ).await
}

#[tokio::test]
async fn case_results_preserve_required_subquery_preparation() -> TestResult {
    assert_case_schema_stages_and_rows(
        include_str!("fixtures/case-result-nullability-true-subquery-spark.jsonl"),
        12,
        "CASE_721_TRUE_SUBQUERY_OBSERVATION",
    ).await
}

#[tokio::test]
async fn case_nested_common_types_and_field_admission_match_spark() -> TestResult {
    assert_case_schema_stages_and_rows(
        include_str!("fixtures/case-result-nullability-nested-common-spark.jsonl"),
        12,
        "CASE_721_NESTED_COMMON_OBSERVATION",
    ).await
}

async fn assert_case_schema_stages_and_rows(
    source: &str,
    expected_cases: usize,
    observation_label: &str,
) -> TestResult {
    use datafusion::physical_plan::collect;
    let records = source.lines().map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["spark_revision"], "32f7299601108917fb01920a54e084595b7b3bf8");
    assert_eq!(records.len() - 1, expected_cases);
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["input"].as_str().ok_or("missing input")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        if case["expected"].get("condition").is_none() {
            assert_eq!(case["expected"]["schema"], case["schema_stages"]["analyzed"]);
            assert_eq!(case["schema_stages"]["executed_before_collect"], case["schema_stages"]["executed"]);
            assert!(case["schema_stages"]["executed"].is_object());
            assert!(case["expected"]["rows"].is_array());
        }
    }
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(configure_spark_sql(
            SessionStateBuilder::new().with_default_features().with_config(
                SessionConfig::new().with_batch_size(batch_size).with_target_partitions(2),
            ),
        ).build());
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let mut phase = "resolve";
            let mut public = None;
            let mut executed = None;
            let actual: TestResult<Vec<Vec<Value>>> = async {
                let query = plan_query(&session, case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions { ansi_mode:case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                        session_timezone:"UTC".into(), ..SparkSqlOptions::default() }).await?;
                phase = "public_schema";
                public = Some(spark_results::capture_schema(query.logical_plan.schema().fields(), Some(&query.column_names))?);
                phase = "create_physical_plan";
                let physical = query.create_physical_plan(&session).await?;
                let physical_schema = physical.schema();
                phase = "physical_schema";
                executed = Some(spark_results::capture_schema(physical_schema.fields(), None)?);
                phase = "collect";
                let mut rows = Vec::new();
                for batch in collect(physical, session.task_ctx()).await? {
                    spark_results::validate_batch(physical_schema.as_ref(), &batch)?;
                    for row in 0..batch.num_rows() {
                        rows.push(batch.columns().iter().map(|column| spark_results::encode_value(column.as_ref(), row))
                            .collect::<TestResult<Vec<_>>>()?);
                    }
                }
                rows.sort_by_key(|row| json!(row).to_string());
                Ok(rows)
            }.await;
            let public_matches = case["schema_stages"].get("analyzed")
                .map(|expected| public.as_ref() == Some(expected));
            let physical_matches = case["schema_stages"].get("executed")
                .map(|expected| executed.as_ref() == Some(expected));
            let rows_matches = case["expected"].get("rows")
                .map(|expected| actual.as_ref().is_ok_and(|rows| json!(rows) == *expected));
            let error_matches = case["expected"]["condition"].as_str().map(|condition| {
                actual.as_ref().err().is_some_and(|error| {
                    let message = error.to_string();
                    match condition {
                        "DIVIDE_BY_ZERO" => phase == "create_physical_plan" && {
                            let message = message.to_ascii_lowercase();
                            message.contains("[divide_by_zero]") || message.contains("divide by zero")
                                || message.contains("division by zero")
                        },
                        "DATATYPE_MISMATCH.DATA_DIFF_TYPES" => phase == "resolve" && (
                            message.contains("[DATATYPE_MISMATCH.DATA_DIFF_TYPES]")
                                || (message.contains("Failed to coerce then (")
                                    && message.contains("to common types in CASE WHEN expression"))
                        ),
                        _ => false,
                    }
                })
            });
            let matches = public_matches.unwrap_or(true) && physical_matches.unwrap_or(true)
                && rows_matches.unwrap_or(true) && error_matches.unwrap_or(true);
            eprintln!("{observation_label} {}", json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "public_schema":public, "physical_schema":executed, "rows":actual.as_ref().ok(),
                "public_matches":public_matches, "physical_matches":physical_matches, "rows_matches":rows_matches,
                "error_matches":error_matches, "native_phase":phase,
                "error":actual.as_ref().err().map(ToString::to_string), "matches":matches,
                "expected":case["expected"], "expected_physical":case["schema_stages"]["executed"],
                "spark_error_phase":case["spark_error_phase"], "spark_error":case["spark_error"],
            }));
            if !matches {
                differences.push(format!("{} ANSI={} batch={batch_size}: public={public_matches:?} physical={physical_matches:?} rows={rows_matches:?} error={error_matches:?}; expected {} executed={}, got public={public:?} executed={executed:?} result={actual:?} phase={phase}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]["executed"]));
            }
        }
    }
    assert_eq!(comparisons, expected_cases * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
