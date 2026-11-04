//! SLICE NULL evaluation matches independently captured Spark schemas and errors.

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

const ORIGINAL_IDS: &[&str] = &[
    "null_array_bad_start",
    "bad_array_null_start",
    "bad_array_null_start_is_null",
    "constant_bad_array_null_start",
];

fn cast_input_matches(case: &Value, comparison: &Value) -> bool {
    if case["expected"]["condition"] != "CAST_INVALID_INPUT" {
        return true;
    }
    let Some(input) = case["spark_error_details"]["message_parameters"]["expression"].as_str()
    else {
        return false;
    };
    let expected = format!("Cannot cast string {input} to value of Int32 type");
    comparison["error"].as_str().is_some_and(|error| {
        error
            .lines()
            .filter_map(|line| line.strip_prefix("Arrow error: Cast error: "))
            .any(|cause| cause == expected)
    })
}

#[test]
fn cast_error_priority_rejects_a_different_failing_argument() {
    let case = json!({
        "expected":{"condition":"CAST_INVALID_INPUT"},
        "spark_error_details":{"message_parameters":{"expression":"'array_bad'"}},
    });
    for input in ["array_bad", "start_bad", "length_bad"] {
        let comparison = json!({"error":format!(
            "[CAST_INVALID_INPUT]\nArrow error: Cast error: Cannot cast string '{input}' to value of Int32 type"
        )});
        assert_eq!(cast_input_matches(&case, &comparison), input == "array_bad");
    }
    assert!(!cast_input_matches(
        &case,
        &json!({"error":"[CAST_INVALID_INPUT]"})
    ));
    assert!(!cast_input_matches(
        &case,
        &json!({"error":concat!(
            "SQL: CAST('array_bad' AS INT), CAST('start_bad' AS INT)\n",
            "[CAST_INVALID_INPUT]\n",
            "Arrow error: Cast error: Cannot cast string 'start_bad' to value of Int32 type",
        )})
    ));
}

#[tokio::test]
async fn ordered_observer_rejects_unexpected_success_and_reordered_rows() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let schema = json!({"type":"struct","fields":[{
        "name":"r","type":"integer","nullable":false,"metadata":{},
    }]});
    let mut case = json!({
        "sql":"SELECT CAST(1 AS INT) AS r", "ansi":true, "ordered":true,
        "expected":{"condition":"CAST_INVALID_INPUT"}, "spark_error_phase":"collect",
        "schema_stages":{
            "public":schema, "analyzed":schema, "optimized":schema,
            "executed_before_collect":schema, "executed":schema,
        },
    });
    let unexpected_success =
        spark_results::compare_exact_spark_query_stages_with_optimized(&session, &case).await?;
    assert_eq!(unexpected_success["ordered_rows_match"], true);
    assert_eq!(unexpected_success["error_matches"], false);
    assert_eq!(unexpected_success["matches"], false);

    case["sql"] = json!("SELECT k AS r FROM VALUES (1), (2) t(k) ORDER BY k");
    case["expected"] = json!({"schema":schema,"rows":[["2"],["1"]]});
    let reordered =
        spark_results::compare_exact_spark_query_stages_with_optimized(&session, &case).await?;
    assert_eq!(reordered["rows_match"], true);
    assert_eq!(reordered["ordered_rows_match"], false);
    assert_eq!(reordered["matches"], false);

    case["expected"] = json!({"schema":schema});
    let missing =
        spark_results::compare_exact_spark_query_stages_with_optimized(&session, &case).await?;
    assert_eq!(missing["ordered_rows_match"], false);
    assert_eq!(missing["matches"], false);

    case["sql"] = json!("SELECT CAST(1 AS INT) AS r WHERE false");
    case["expected"] = json!({"schema":schema,"rows":[]});
    let empty =
        spark_results::compare_exact_spark_query_stages_with_optimized(&session, &case).await?;
    assert_eq!(empty["ordered_rows_match"], true);
    Ok(())
}

async fn compare_null_evaluation(records: &[Value], cases: &[&Value]) -> TestResult {
    let mut completed = std::collections::BTreeSet::new();
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_batch_size(batch)),
            )
            .build(),
        );
        spark_inputs::register_recorded_inputs(&session, &records[0], batch)?;
        for case in cases {
            let mut comparison =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await
                    .unwrap_or_else(|error| json!({"observer_error":error.to_string()}));
            comparison["group"] = records[0]["group"].clone();
            comparison["id"] = case["id"].clone();
            comparison["sql"] = case["sql"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch);
            comparison["cast_input_matches"] = json!(cast_input_matches(case, &comparison));
            eprintln!("SLICE_NULL_EVALUATION_OBSERVATION {comparison}");
            if [
                "matches",
                "optimized_matches",
                "public_matches",
                "before_matches",
                "physical_matches",
                "rows_match",
                "ordered_rows_match",
                "error_matches",
                "cast_input_matches",
            ]
            .iter()
            .any(|flag| comparison[flag] != true)
            {
                differences.push(comparison.to_string());
            }
            assert!(completed.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch,
            )));
        }
    }
    assert_eq!(completed.len(), cases.len() * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn slice_null_evaluation_preserves_original_references() -> TestResult {
    let records = include_str!("fixtures/slice-parent-fields-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["group"], "slice-parent-878-current-v1");
    let cases = records[1..]
        .iter()
        .filter(|case| {
            case["id"]
                .as_str()
                .is_some_and(|id| ORIGINAL_IDS.contains(&id))
        })
        .collect::<Vec<_>>();
    let identities = cases
        .iter()
        .map(|case| (case["id"].as_str(), case["ansi"].as_bool()))
        .collect::<Vec<_>>();
    let expected = [true, false]
        .into_iter()
        .flat_map(|ansi| ORIGINAL_IDS.iter().map(move |id| (Some(*id), Some(ansi))))
        .collect::<Vec<_>>();
    assert_eq!(identities, expected);
    compare_null_evaluation(&records, &cases).await
}

#[tokio::test]
async fn slice_null_evaluation_matches_fresh_spark_capture() -> TestResult {
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/slice-null-evaluation/manifest.json"))?;
    let records = include_str!("fixtures/slice-null-evaluation/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(manifest["scenario_count"], 49);
    assert_eq!(manifest["observation_count"], 98);
    assert_eq!(manifest["ansi_modes"], json!([true, false]));
    assert_eq!(records.len(), 99);
    assert_eq!(records[0]["group"], "slice-881-null-evaluation-current-v1");
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(
        records[0]["source_commit"],
        "085986ca2a53d8e249222ecd4052ea884386a203"
    );
    assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
    for key in [
        "spark_version",
        "spark_revision",
        "timezone",
        "group",
        "source_commit",
        "inputs",
        "input_encoding",
    ] {
        assert_eq!(records[0][key], manifest[key], "{key}");
    }
    assert_eq!(
        records[0]["capture_sha256"],
        manifest["capture_script_sha256"]
    );
    let identities = records[1..]
        .iter()
        .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
        .collect::<Vec<_>>();
    assert_eq!(json!(identities), manifest["identities"]);
    let original = include_str!("fixtures/slice-parent-fields-spark.jsonl")
        .lines()
        .skip(1)
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let mut outcomes = (0, 0);
    for case in &records[1..] {
        let definition = manifest["scenarios"]
            .as_array()
            .ok_or("missing scenarios")?
            .iter()
            .find(|definition| definition["id"] == case["id"])
            .ok_or("missing scenario")?;
        for (key, value) in definition.as_object().ok_or("missing definition")? {
            assert_eq!(&case[key], value, "{} {key}", case["id"]);
        }
        let stages = &case["schema_stages"];
        assert!(stages["public"].is_object());
        assert_eq!(stages["public"], stages["resolved"]);
        assert_eq!(stages["public"], stages["analyzed"]);
        if let Some(physical) = stages.get("physical") {
            assert_eq!(physical, &stages["executed_before_collect"]);
        }
        if case["expected"]["condition"].is_string() {
            outcomes.1 += 1;
            assert_eq!(case["query_failure_kind"], "sql_error");
            assert_eq!(
                case["expected"]["condition"],
                case["spark_error_details"]["condition"]
            );
        } else {
            outcomes.0 += 1;
            assert!(case["expected"]["rows"].is_array());
            assert_eq!(case["expected"]["schema"], stages["public"]);
            assert!(case.get("query_failure_kind").is_none());
        }
        if case["id"]
            .as_str()
            .is_some_and(|id| ORIGINAL_IDS.contains(&id))
        {
            let reference = original
                .iter()
                .find(|reference| {
                    reference["id"] == case["id"] && reference["ansi"] == case["ansi"]
                })
                .ok_or("missing original reference")?;
            for key in ["sql", "expected"] {
                assert_eq!(case[key], reference[key], "{} {key}", case["id"]);
            }
            for (stage, schema) in reference["schema_stages"]
                .as_object()
                .ok_or("missing original schema stages")?
            {
                assert_eq!(&stages[stage], schema, "{} {stage}", case["id"]);
            }
        }
    }
    assert_eq!(outcomes, (61, 37));
    let cases = records[1..].iter().collect::<Vec<_>>();
    compare_null_evaluation(&records, &cases).await
}
