//! Scalar CASE keeps explicit ELSE fields and native conditional behavior.

use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

fn references() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/case-explicit-else/case-explicit-else-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, 58);
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI")?,
        )));
        if case["expected"]["condition"].is_null() {
            assert_eq!(
                case["expected"]["schema"],
                case["schema_stages"]["analyzed"]
            );
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"][stage].is_object(), "{case}");
            }
            assert!(case["expected"]["rows"].is_array());
        }
    }
    Ok(records)
}

#[tokio::test]
async fn scalar_case_explicit_else_matches_spark() -> TestResult {
    let records = references()?;
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
                            .with_target_partitions(2),
                    ),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            comparisons += 1;
            let mut comparison =
                spark_results::compare_exact_spark_query_stages(&session, case).await?;
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("CASE_EXPLICIT_ELSE_OBSERVATION {comparison}");
            if comparison["matches"] != true {
                differences.push(format!("expected={case}, actual={comparison}"));
            }
        }
    }
    assert_eq!(comparisons, 116);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn configured_native_case_keeps_plain_results() -> TestResult {
    let records = references()?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        spark_inputs::register_recorded_inputs(session, &records[0], 1)?;
    }
    for after_spark in [false, true] {
        if after_spark {
            let query = plan_query(
                &configured,
                "SELECT CASE WHEN n IS NOT NULL THEN n ELSE 0 END AS r FROM case_inputs",
                &SparkSqlOptions::default(),
            )
            .await?;
            collect(
                query.create_physical_plan(&configured).await?,
                configured.task_ctx(),
            )
            .await?;
        }
        for sql in [
            "SELECT CASE WHEN false THEN 1 ELSE 7 END AS r",
            "SELECT CASE WHEN i>0 THEN i ELSE 7 END AS r FROM case_inputs",
            "SELECT CASE WHEN i>0 THEN i END AS r FROM case_inputs",
            "SELECT CASE WHEN n IS NOT NULL THEN n ELSE 0 END AS r FROM case_inputs",
            "SELECT CASE i WHEN -1 THEN 1 WHEN 0 THEN 2 ELSE 3 END AS r FROM case_inputs",
            "SELECT CASE WHEN i>0 THEN n ELSE 7 END AS r FROM case_empty",
        ] {
            let expected = spark_results::capture_native_query_observation(&native, sql).await?;
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            assert!(expected["error"].is_null(), "{sql}: {expected}");
            assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
        }
        let guard = native
            .sql("SELECT CASE WHEN n IS NOT NULL THEN n ELSE 0 END AS r FROM case_inputs")
            .await?;
        assert!(!guard.schema().field(0).is_nullable());
    }
    Ok(())
}
