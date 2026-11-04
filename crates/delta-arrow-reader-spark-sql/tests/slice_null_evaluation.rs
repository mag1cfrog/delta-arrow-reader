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
