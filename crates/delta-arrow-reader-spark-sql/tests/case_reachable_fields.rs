//! Spark CASE declarations stop counting results after the first literal TRUE.

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

const REFERENCE: &str =
    include_str!("fixtures/case-reachable-fields/case-result-nullability-spark.jsonl");
const CASE_IDS: &[&str] = &[
    "searched_true",
    "searched_false",
    "searched_typed_null",
    "searched_folded_true",
    "simple_literal_true",
    "true_missing_else",
    "false_missing_else",
    "null_missing_else",
    "column_missing_else",
    "column_required_else",
    "column_nullable_then",
    "column_nullable_else",
    "column_guarded_nullable",
    "true_then_nullable",
    "actual_null",
    "multiple_true_cuts_nullable",
    "nullable_before_true",
    "false_before_true",
    "true_before_nullable",
    "dead_error_else",
    "dead_error_then",
    "required_error_else",
    "column_required_error",
    "metadata_literal",
];

fn references() -> TestResult<Vec<Value>> {
    let records = REFERENCE
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, 324);
    Ok(records)
}

#[tokio::test]
async fn case_reachable_result_fields_match_spark() -> TestResult {
    let records = references()?;
    let cases = records[1..]
        .iter()
        .filter(|case| case["id"].as_str().is_some_and(|id| CASE_IDS.contains(&id)))
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 142);
    let identities = cases
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["input"].as_str().ok_or("missing provider")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            ))
        })
        .collect::<TestResult<std::collections::BTreeSet<_>>>()?;
    assert_eq!(identities.len(), 142);
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
        for case in &cases {
            comparisons += 1;
            let mut comparison =
                spark_results::compare_exact_spark_query_stages(&session, case).await?;
            comparison["id"] = case["id"].clone();
            comparison["input"] = case["input"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("CASE_REACHABLE_FIELDS_OBSERVATION {comparison}");
            if comparison["matches"] != true {
                differences.push(format!("expected={case}, actual={comparison}"));
            }
        }
    }
    assert_eq!(comparisons, 284);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_native_case_fields() -> TestResult {
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
                "SELECT CASE WHEN true THEN 1 ELSE CAST(NULL AS INT) END AS r",
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
            "SELECT CASE WHEN true THEN 1 ELSE CAST(NULL AS INT) END AS r",
            "SELECT CASE WHEN false THEN CAST(NULL AS INT) ELSE 1 END AS r",
            "SELECT CASE true WHEN true THEN 1 ELSE CAST(NULL AS INT) END AS r",
            "SELECT CASE WHEN v IS NOT NULL THEN v ELSE 2 END AS r FROM case_mixed",
            "SELECT CASE WHEN id=0 THEN 1 END AS r FROM case_empty",
            "SELECT CASE WHEN true THEN 1 ELSE CAST('bad' AS INT) END AS r",
        ] {
            let expected = spark_results::capture_native_query_observation(&native, sql).await?;
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
        }
    }
    Ok(())
}
