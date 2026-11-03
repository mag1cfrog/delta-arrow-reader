//! Array result fields and values match independently captured Spark plans.

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
async fn flatten_result_fields_match_spark() -> TestResult {
    assert_array_result_fields(
        include_str!("fixtures/flatten-result-fields-spark.jsonl"),
        26,
    )
    .await
}

#[tokio::test]
async fn flatten_untyped_boundaries_match_spark() -> TestResult {
    let records = include_str!("fixtures/array-result-fields-boundaries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 13);
    let flatten = records
        .into_iter()
        .filter(|record| {
            record.get("id").is_none()
                || matches!(
                    record["id"].as_str(),
                    Some("untyped_null" | "untyped_empty")
                )
        })
        .collect::<Vec<_>>();
    assert_array_result_records(&flatten, 4).await
}

async fn assert_array_result_fields(source: &str, expected_cases: usize) -> TestResult {
    let records = source
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_array_result_records(&records, expected_cases).await
}

async fn assert_array_result_records(records: &[Value], expected_cases: usize) -> TestResult {
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
            apply_flatten_diagnostic_alias(case, &mut comparison);
            comparison["group"] = records[0]["group"].clone();
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            eprintln!("ARRAY_RESULT_FIELDS_OBSERVATION {comparison}");
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

// These exact resolver diagnostics predate Spark condition tags.
fn apply_flatten_diagnostic_alias(case: &Value, comparison: &mut Value) {
    let diagnostic = match (
        case["expected"]["condition"].as_str(),
        case["sql"].as_str(),
    ) {
        (
            Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
            Some(
                "SELECT FLATTEN(1) AS r"
                | "SELECT FLATTEN(ARRAY(1)) AS r"
                | "SELECT FLATTEN(NULL) AS r, STRUCT(FLATTEN(NULL) AS x) AS nested",
            ),
        ) => "FLATTEN requires an array of arrays",
        (
            Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION"),
            Some("SELECT FLATTEN(ARRAY(ARRAY(1)), ARRAY(ARRAY(1))) AS r"),
        ) => "FLATTEN requires one argument, got 2",
        _ => return,
    };
    comparison["error_matches"] = json!(
        case["spark_error_phase"] == "sql"
            && comparison["native_phase"] == "resolve"
            && comparison["error"]
                .as_str()
                .is_some_and(|error| error.contains(diagnostic))
    );
    comparison["matches"] = json!(
        ["public_matches", "before_matches", "physical_matches", "rows_match", "error_matches"]
            .iter()
            .all(|key| comparison[key] == true)
    );
}

#[test]
fn flatten_diagnostic_alias_keeps_exact_boundaries() -> TestResult {
    let case = json!({
        "sql":"SELECT FLATTEN(1) AS r",
        "expected":{"condition":"DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"},
        "spark_error_phase":"sql",
    });
    let observation = json!({
        "error":"Invalid argument: FLATTEN requires an array of arrays",
        "native_phase":"resolve",
        "public_matches":true, "before_matches":true, "physical_matches":true,
        "rows_match":true, "error_matches":false, "matches":false,
    });
    let mut comparison = observation.clone();
    apply_flatten_diagnostic_alias(&case, &mut comparison);
    assert_eq!(comparison["matches"], true);
    for (key, wrong) in [
        ("error", json!("unrelated error")),
        ("native_phase", json!("collect")),
        ("public_matches", json!(false)),
        ("before_matches", json!(false)),
        ("physical_matches", json!(false)),
        ("rows_match", json!(false)),
    ] {
        let mut comparison = observation.clone();
        comparison[key] = wrong;
        apply_flatten_diagnostic_alias(&case, &mut comparison);
        assert_eq!(comparison["matches"], false, "{key}");
    }
    for wrong in [
        json!({"sql":"SELECT FLATTEN(2) AS r"}),
        json!({"expected":{"condition":"CAST_INVALID_INPUT"}}),
        json!({"spark_error_phase":"collect"}),
    ] {
        let mut reference = case.clone();
        for (key, value) in wrong.as_object().ok_or("missing negative control")? {
            reference[key] = value.clone();
        }
        let mut comparison = observation.clone();
        apply_flatten_diagnostic_alias(&reference, &mut comparison);
        assert_eq!(comparison["matches"], false, "{wrong}");
    }
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_native_flatten() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT FLATTEN([[1], [2]]) AS r",
        "SELECT FLATTEN([[1], NULL]) AS r",
        "SELECT FLATTEN([1, 2]) AS r",
    ] {
        let expected = native.sql(sql).await?;
        let actual = configured.sql(sql).await?;
        assert_eq!(actual.schema(), expected.schema(), "{sql}");
        assert_eq!(actual.collect().await?, expected.collect().await?, "{sql}");
    }
    Ok(())
}

