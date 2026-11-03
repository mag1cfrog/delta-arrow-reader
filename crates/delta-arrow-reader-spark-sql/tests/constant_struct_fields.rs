//! Constant STRUCT fields and values match independently captured Spark plans.

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

#[tokio::test]
async fn constant_struct_fields_match_spark_without_array_leaves() -> TestResult {
    let records = include_str!("fixtures/constant-struct-result-fields-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let expected_cases = 10;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, expected_cases);
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            ))
        })
        .collect::<TestResult<std::collections::BTreeSet<_>>>()?;
    assert_eq!(identities.len(), expected_cases);
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
        if records[0].get("inputs").is_some() {
            spark_inputs::register_recorded_inputs(&session, &records[0], batch_size)?;
        }
        for case in &records[1..] {
            comparisons += 1;
            if case["expected"]["condition"].is_null() {
                assert_eq!(
                    case["expected"]["schema"],
                    case["schema_stages"]["analyzed"]
                );
                assert_eq!(
                    case["schema_stages"]["executed_before_collect"],
                    case["schema_stages"]["executed"]
                );
            }
            let mut comparison =
                spark_results::compare_exact_spark_query_stages(&session, case).await?;
            comparison["group"] = records[0]["group"].clone();
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("CONSTANT_STRUCT_RESULT_FIELDS_OBSERVATION {comparison}");
            if comparison["matches"] != true {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected={} stages={}, actual={comparison}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]
                ));
            }
        }
    }
    assert_eq!(comparisons, expected_cases * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn constant_struct_stage_comparator_rejects_wrong_error_fields_and_phase() -> TestResult {
    let case = include_str!("fixtures/constant-struct-result-fields-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .find(|case| case["id"] == "required_cast_error" && case["ansi"] == true)
        .ok_or("missing required-error reference")?;
    assert_eq!(case["spark_error_phase"], "optimized_schema");
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let mut wrong_fields = case.clone();
    wrong_fields["schema_stages"]["analyzed"]["fields"][0]["nullable"] = json!(false);
    let comparison =
        spark_results::compare_exact_spark_query_stages(&session, &wrong_fields).await?;
    assert_eq!(comparison["public_matches"], false);
    assert_eq!(comparison["error_matches"], true);
    assert_eq!(comparison["matches"], false);
    let mut wrong_phase = case;
    wrong_phase["spark_error_phase"] = json!("collect");
    let comparison =
        spark_results::compare_exact_spark_query_stages(&session, &wrong_phase).await?;
    assert_eq!(comparison["public_matches"], true);
    assert_eq!(comparison["error_matches"], false);
    assert_eq!(comparison["matches"], false);
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_native_constant_structs() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT NAMED_STRUCT('x', CAST('1' AS INT)) AS r",
        "SELECT NAMED_STRUCT('x', COALESCE(CAST(NULL AS INT), 1)) AS r",
        "SELECT NAMED_STRUCT('x', NAMED_STRUCT('y', CAST('1' AS INT))) AS r",
    ] {
        let expected = native.sql(sql).await?;
        let actual = configured.sql(sql).await?;
        assert_eq!(actual.schema(), expected.schema(), "{sql}");
        assert_eq!(actual.collect().await?, expected.collect().await?, "{sql}");
    }
    Ok(())
}
