//! Independent Spark ABS declarations and native DataFusion isolation controls.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const ABS_REFERENCE: &str =
    include_str!("fixtures/abs-result-fields/abs-result-fields-spark.jsonl");
const PREPARATION_REFERENCE: &str =
    include_str!("fixtures/abs-result-fields/preparation/abs-preparation-spark.jsonl");

fn references(fixture: &str, count: usize) -> TestResult<Vec<Value>> {
    let records = fixture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, count);
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
                assert!(
                    case["schema_stages"][stage].is_object(),
                    "missing {stage}: {case}"
                );
            }
            assert!(case["expected"]["rows"].is_array());
        }
    }
    Ok(records)
}

#[tokio::test]
async fn abs_result_fields_match_pinned_spark_stages() -> TestResult {
    replay_references(ABS_REFERENCE, 44, "ABS_RESULT_FIELDS_OBSERVATION").await
}

#[tokio::test]
async fn abs_preparation_matches_pinned_spark_stages() -> TestResult {
    replay_references(PREPARATION_REFERENCE, 24, "ABS_PREPARATION_OBSERVATION").await
}

async fn replay_references(fixture: &str, count: usize, label: &str) -> TestResult {
    let records = references(fixture, count)?;
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
            match spark_results::compare_exact_spark_query_stages(&session, case).await {
                Ok(mut comparison) => {
                    comparison["id"] = case["id"].clone();
                    comparison["ansi"] = case["ansi"].clone();
                    comparison["batch"] = json!(batch_size);
                    eprintln!("{label} {comparison}");
                    if comparison["matches"] != true {
                        differences.push(format!("expected={case}, actual={comparison}"));
                    }
                }
                Err(error) => differences.push(format!(
                    "case={case}, batch={batch_size}, observer error={error}"
                )),
            }
        }
    }
    assert_eq!(comparisons, count * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn configured_native_abs_preserves_plain_datafusion_results() -> TestResult {
    let records = references(ABS_REFERENCE, 44)?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        spark_inputs::register_recorded_inputs(session, &records[0], 1)?;
    }
    for sql in [
        "SELECT abs(b) AS b,abs(s) AS s,abs(i) AS i,abs(l) AS l,abs(f) AS f,abs(d) AS d,abs(n) AS n,abs(w) AS w FROM abs_required",
        "SELECT abs(b) AS b,abs(f) AS f,abs(n) AS n FROM abs_nullable",
        "SELECT abs(i) AS r FROM abs_required_empty",
        "SELECT abs(NULL) AS r",
        "SELECT abs(i) AS r FROM abs_limits",
        "SELECT abs() AS r",
        "SELECT abs(1,2) AS r",
    ] {
        let expected = spark_results::capture_native_query_observation(&native, sql).await?;
        let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
        assert_eq!(actual, expected, "{sql}");
    }
    Ok(())
}
