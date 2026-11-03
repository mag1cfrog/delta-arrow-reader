//! Independent Spark references for nested conditional common types and admission.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::Value;

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

#[tokio::test]
async fn nested_conditional_results_match_spark() -> TestResult {
    let records = include_str!("fixtures/conditional-nested-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, 12);
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI")?,
        )));
    }
    let mut failures = Vec::new();
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
            let label = format!(
                "{} ANSI={} batch_size={batch_size}",
                case["id"], case["ansi"]
            );
            if case["expected"]["condition"] == "DATATYPE_MISMATCH.DATA_DIFF_TYPES" {
                let result = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        session_timezone: "UTC".into(),
                        ..SparkSqlOptions::default()
                    },
                )
                .await;
                match result {
                    Err(error)
                        if error
                            .to_string()
                            .contains("[DATATYPE_MISMATCH.DATA_DIFF_TYPES]") => {}
                    Err(error) => failures.push(format!("{label}: wrong analysis error: {error}")),
                    Ok(_) => failures.push(format!("{label}: incompatible fields passed analysis")),
                }
            } else {
                match spark_results::capture_spark_query_observation(&session, case).await {
                    Ok(actual)
                        if actual["result"] == case["expected"]
                            && actual["physical_schema"] == case["schema_stages"]["executed"] => {}
                    Ok(actual) => failures.push(format!(
                        "{label}: expected {} and physical {}, got {actual}",
                        case["expected"], case["schema_stages"]["executed"]
                    )),
                    Err(error) => failures.push(format!("{label}: {error}")),
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
