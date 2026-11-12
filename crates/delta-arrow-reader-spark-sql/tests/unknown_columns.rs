//! Unknown-column diagnostics expose usable names from the active query scope.

use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::error::PlanError;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, capture_executed_query_result, remove_nullability_and_metadata};

#[tokio::test]
async fn unknown_columns_list_the_active_scope_without_internal_names() -> TestResult {
    let records = include_str!("fixtures/unknown-columns/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records.len() - 1, 76);
    let mut differences = Vec::new();
    let mut errors_checked = 0;
    let mut known_input_checks = 0;
    let mut controls_checked = 0;
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
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            if let Some(requested) = case["unresolved_name"].as_str() {
                errors_checked += 1;
                let query = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                let error = match query {
                    Ok(_) => {
                        differences.push(format!("unexpected success: {case}"));
                        continue;
                    }
                    Err(error) => error.to_string(),
                };
                println!(
                    "COLUMN_DIAGNOSTIC {}",
                    json!({"id":case["id"],"ansi":case["ansi"],
                    "batch_size":batch_size,"error":error})
                );
                let columns = case["columns"].as_array().ok_or("missing scope columns")?;
                if !error.contains(requested)
                    || !error.contains("available columns:")
                    || error.contains("ObjectName(")
                    || error.contains("Identifier(")
                    || columns
                        .iter()
                        .any(|column| column.as_str().is_none_or(|column| !error.contains(column)))
                    || (columns.is_empty() && !error.contains("available columns: []"))
                    || (case["id"] == "using_outer" && error.contains("outer_only"))
                    || (case["id"] == "closed_cte"
                        && (error.contains("private_column") || error.contains("`inputs`.`hour`")))
                {
                    differences.push(format!("case={case} batch={batch_size}: {error}"));
                }
            } else if case["known_input_error"] == true {
                known_input_checks += 1;
                let result = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                if let Err(DataFusionError::External(source)) = &result {
                    assert!(
                        !matches!(
                            source.downcast_ref::<PlanError>(),
                            Some(PlanError::UnknownColumn { .. })
                        ),
                        "known sort input reported missing: {case}: {source}"
                    );
                }
                println!(
                    "COLUMN_KNOWN_INPUT_DIAGNOSTIC {}",
                    json!({"id":case["id"],
                    "ansi":case["ansi"],"batch_size":batch_size,"accepted":result.is_ok(),
                    "error":result.as_ref().err().map(ToString::to_string)})
                );
            } else {
                controls_checked += 1;
                let actual = capture_executed_query_result(&session, case).await?;
                println!(
                    "COLUMN_VALID_CONTROL {}",
                    json!({"id":case["id"],"ansi":case["ansi"],
                    "batch_size":batch_size,"actual":actual})
                );
                let types = |schema: &Value| -> TestResult<Vec<Value>> {
                    schema["fields"]
                        .as_array()
                        .ok_or("missing fields")?
                        .iter()
                        .map(|field| {
                            let mut data_type = field["type"].clone();
                            remove_nullability_and_metadata(&mut data_type);
                            Ok(data_type)
                        })
                        .collect()
                };
                assert_eq!(actual["rows"], case["rows"], "{case}");
                assert_eq!(types(&actual["schema"])?, types(&case["schema"])?, "{case}");
            }
        }
        let direct = SessionContext::new();
        spark_inputs::register_recorded_inputs(&direct, &records[0], batch_size)?;
        let sql = "SELECT missing FROM inputs";
        let native = spark_results::capture_native_query_observation(&direct, sql).await?;
        assert_eq!(
            spark_results::capture_native_query_observation(&session, sql).await?,
            native
        );
    }
    assert_eq!(errors_checked, 92);
    assert_eq!(controls_checked, 52);
    assert_eq!(known_input_checks, 8);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
