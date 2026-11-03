//! ARRAY_CONTAINS result fields and values match independently captured Spark plans.

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
async fn array_contains_result_fields_match_spark() -> TestResult {
    let records = include_str!("fixtures/array-contains-result-fields-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, 32);
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            ))
        })
        .collect::<TestResult<std::collections::BTreeSet<_>>>()?;
    assert_eq!(identities.len(), 32);
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
            let matches = array_contains_reference_matches(case, &comparison);
            comparison["reference_matches"] = json!(matches);
            comparison["group"] = records[0]["group"].clone();
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("ARRAY_RESULT_FIELDS_OBSERVATION {comparison}");
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected={} stages={}, actual={comparison}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]
                ));
            }
        }
    }
    assert_eq!(comparisons, 64);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn array_contains_reference_matches(case: &Value, comparison: &Value) -> bool {
    // These two pinned resolver diagnostics have no Spark condition tag.
    let diagnostic = match (
        case["expected"]["condition"].as_str(),
        case["sql"].as_str(),
    ) {
        (
            Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
            Some("SELECT ARRAY_CONTAINS(1, 1) AS r"),
        ) => "array_contains does not support type Int32",
        (
            Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION"),
            Some("SELECT ARRAY_CONTAINS(ARRAY(1)) AS r"),
        ) => "ARRAY_CONTAINS requires two arguments, got 1",
        _ => return comparison["matches"] == true,
    };
    case["spark_error_phase"] == "sql"
        && comparison["native_phase"] == "resolve"
        && [
            "public_matches",
            "before_matches",
            "physical_matches",
            "rows_match",
        ]
        .into_iter()
        .all(|key| comparison[key] == true)
        && comparison["error"]
            .as_str()
            .is_some_and(|error| error.contains(diagnostic))
}

#[tokio::test]
async fn array_contains_diagnostic_aliases_reject_wrong_cause_or_phase() -> TestResult {
    let records = include_str!("fixtures/array-contains-result-fields-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let mut checked = 0;
    for case in records.iter().filter(|case| {
        case["ansi"] == true
            && matches!(case["id"].as_str(), Some("invalid_scalar" | "invalid_arity"))
    }) {
        let comparison = spark_results::compare_exact_spark_query_stages(&session, case).await?;
        assert!(array_contains_reference_matches(case, &comparison));
        for key in [
            "public_matches",
            "before_matches",
            "physical_matches",
            "rows_match",
        ] {
            let mut wrong_fields = comparison.clone();
            wrong_fields[key] = json!(false);
            assert!(!array_contains_reference_matches(case, &wrong_fields));
        }
        let mut wrong_phase = comparison.clone();
        wrong_phase["native_phase"] = json!("collect");
        assert!(!array_contains_reference_matches(case, &wrong_phase));
        let mut wrong_cause = comparison;
        wrong_cause["error"] = json!("unrelated failure");
        assert!(!array_contains_reference_matches(case, &wrong_cause));
        checked += 1;
    }
    assert_eq!(checked, 2);
    Ok(())
}

#[tokio::test]
async fn array_stage_comparator_rejects_wrong_error_fields_and_phase() -> TestResult {
    let case = include_str!("fixtures/array-contains-result-fields-spark.jsonl")
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
    assert!(!array_contains_reference_matches(&wrong_fields, &comparison));
    let mut wrong_phase = case;
    wrong_phase["spark_error_phase"] = json!("collect");
    let comparison =
        spark_results::compare_exact_spark_query_stages(&session, &wrong_phase).await?;
    assert_eq!(comparison["public_matches"], true);
    assert_eq!(comparison["error_matches"], false);
    assert!(!array_contains_reference_matches(&wrong_phase, &comparison));
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_native_array_membership() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT ARRAY_HAS([1], 1) AS r",
        "SELECT ARRAY_HAS([1, NULL], 2) AS r",
        "SELECT ARRAY_HAS([], 1) AS r",
        "SELECT ARRAY_HAS(CAST(NULL AS INT[]), 1) AS r",
    ] {
        let expected = spark_results::capture_native_query_observation(&native, sql).await?;
        let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
        assert_eq!(actual, expected, "{sql}");
    }
    Ok(())
}
