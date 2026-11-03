//! Spark CASE merges nested declarations and retains value casts while pruning.

use std::collections::BTreeSet;

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

const PACKAGES: &str = include_str!("fixtures/case-nested-declarations/delivery-packages.json");
const REFERENCES: &[&str] = &[
    include_str!("fixtures/case-nested-declarations/spark.jsonl"),
    include_str!("fixtures/case-nested-declarations/pruning-spark.jsonl"),
    include_str!("fixtures/case-nested-declarations/numeric-pruning-spark.jsonl"),
];

#[tokio::test]
async fn nested_case_declarations_match_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(PACKAGES)?;
    let packages = manifest["packages"].as_array().ok_or("missing packages")?;
    assert_eq!(packages.len(), REFERENCES.len());
    let mut identities = BTreeSet::new();
    let mut differences = Vec::new();
    let mut comparisons = 0;
    for (package, reference) in packages.iter().zip(REFERENCES) {
        let records = reference
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], manifest["spark_version"]);
        assert_eq!(records[0]["spark_revision"], manifest["spark_revision"]);
        assert_eq!(records[0]["timezone"], manifest["timezone"]);
        assert_eq!(records[0]["capture_sha256"], manifest["capture_script_sha256"]);
        assert_eq!(records[0]["manifest_sha256"], package["capture_manifest_sha256"]);
        assert_eq!(json!(records.len() - 1), package["records"]);
        for case in &records[1..] {
            assert!(identities.insert((
                package["package"].to_string(),
                case["id"].as_str().ok_or("missing ID")?.to_owned(),
                case["input"].as_str().ok_or("missing input")?.to_owned(),
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            )));
            if case["expected"].get("condition").is_none() {
                for stage in [
                    "analyzed",
                    "optimized",
                    "executed_before_collect",
                    "executed",
                ] {
                    assert!(case["schema_stages"].get(stage).is_some());
                }
                assert_eq!(case["expected"]["schema"], case["schema_stages"]["analyzed"]);
                assert!(case["expected"]["rows"].is_array());
            } else {
                assert!(case["spark_error_phase"].is_string());
            }
        }
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
                comparison["package"] = package["package"].clone();
                comparison["id"] = case["id"].clone();
                comparison["input"] = case["input"].clone();
                comparison["ansi"] = case["ansi"].clone();
                comparison["batch"] = json!(batch_size);
                eprintln!("CASE_NESTED_DECLARATIONS_OBSERVATION {comparison}");
                if comparison["matches"] != true {
                    differences.push(format!("expected={case}, actual={comparison}"));
                }
            }
        }
    }
    assert_eq!(identities.len(), 98);
    assert_eq!(comparisons, 196);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_native_nested_case() -> TestResult {
    let records = REFERENCES[0]
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
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
                "SELECT CASE WHEN FALSE THEN ARRAY(CAST(2 AS BIGINT)) ELSE ARRAY(id) END AS r FROM case_nested_mixed",
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
            "SELECT CASE WHEN true THEN ARRAY(true) ELSE ARRAY(CAST(NULL AS BOOLEAN)) END AS r",
            "SELECT CASE WHEN false THEN ARRAY(CAST(NULL AS BOOLEAN)) ELSE ARRAY(true) END AS r",
            "SELECT CASE WHEN false THEN ARRAY(CAST(2 AS BIGINT)) ELSE ARRAY(id) END AS r FROM case_nested_mixed",
            "SELECT CASE WHEN false THEN named_struct('leaf', id) END AS r FROM case_nested_mixed",
            "SELECT CASE WHEN true THEN ARRAY(1) ELSE ARRAY(CAST('bad' AS INT)) END AS r",
        ] {
            let expected = spark_results::capture_native_query_observation(&native, sql).await?;
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            assert_eq!(expected["phase"], "encode", "{sql}");
            assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
        }
    }
    Ok(())
}
