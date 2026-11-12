//! Replay correlated scalar execution against independent Spark observations.

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use futures_util::FutureExt;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{TestResult, compare_exact_spark_query_stages};

const INPUTS: &str = include_str!("fixtures/correlated-scalar-execution/inputs.json");
const RESULTS: &str = include_str!("fixtures/correlated-scalar-execution/spark-results.jsonl");
const FAILURES: &str = include_str!("fixtures/correlated-scalar-execution/spark-failures.jsonl");
const REVIEW_INPUTS: &str = include_str!("fixtures/correlated-scalar-execution/review-inputs.json");
const REVIEW_RESULTS: &str =
    include_str!("fixtures/correlated-scalar-execution/review-results.jsonl");
const REVIEW_FAILURES: &str =
    include_str!("fixtures/correlated-scalar-execution/review-failures.jsonl");
const LEGACY_VALID_RESULTS: &str =
    include_str!("fixtures/correlated-scalar-execution/legacy-valid-results.jsonl");
const ARCHIVE_REVIEW_RESULTS: &str =
    include_str!("fixtures/correlated-scalar-execution/archive-review-results.jsonl");
const OPERATOR_COMPOSITION_RESULTS: &str =
    include_str!("fixtures/correlated-scalar-execution/operator-composition-results.jsonl");
const ROUNDING: &str =
    include_str!("fixtures/correlated-scalar-execution/frozen-rounding-spark.jsonl");
const SINGLE_ROW: &str = include_str!("fixtures/single-row-correlated-spark.jsonl");
const SINGLE_ROW_STAGES: &str = include_str!(
    "fixtures/correlated-scalar-metadata-stages/main-replay/main-scalar-stages-spark.jsonl"
);
const CARDINALITY: &str = "SCALAR_SUBQUERY_TOO_MANY_ROWS";

fn records(source: &str) -> TestResult<Vec<Value>> {
    Ok(source
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn fresh_references(
    manifest: &Value,
    results: &str,
    failures: &str,
    count: usize,
) -> TestResult<Vec<Value>> {
    let mut references = records(results)?;
    references.extend(records(failures)?);
    let mut seen = BTreeSet::new();
    let definitions = manifest["cases"].as_array().ok_or("missing fresh cases")?;
    for case in &references {
        let id = case["id"].as_str().ok_or("missing fresh case ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing fresh ANSI mode")?;
        let definition = definitions
            .iter()
            .find(|definition| definition["id"] == id)
            .ok_or_else(|| format!("unknown fresh case {id}"))?;
        if !seen.insert((id, ansi))
            || ["sql", "order", "expected_condition"]
                .iter()
                .any(|key| case[*key] != definition[*key])
            || !matches!(case["order"].as_str(), Some("ordered" | "multiset"))
            || match case["status"].as_str() {
                Some("ok") => {
                    !case["expected_condition"].is_null()
                        || !case["rows"].is_array()
                        || !case["physical_schema"]["fields"].is_array()
                }
                Some("error") => {
                    case["expected_condition"] != CARDINALITY
                        || case["condition"] != CARDINALITY
                        || case["phase"] != "collect"
                }
                _ => true,
            }
        {
            return Err(format!("invalid fresh case {id} ANSI={ansi}").into());
        }
    }
    if definitions.len() != count || seen.len() != definitions.len() * 2 {
        return Err("incomplete fresh Spark observations".into());
    }
    Ok(references)
}

fn session(batch: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    )
}

fn executed_fields(schema: &Value) -> Option<Vec<(&Value, &Value)>> {
    Some(
        schema["fields"]
            .as_array()?
            .iter()
            .map(|field| (&field["name"], &field["type"]))
            .collect(),
    )
}

async fn observe(
    session: &SessionContext,
    batch: usize,
    corpus: &str,
    case: &Value,
    expected: &Value,
    schema: &Value,
) -> bool {
    let ordered = case["order"] == "ordered";
    let mut expected = expected.clone();
    if !ordered && let Some(rows) = expected.get_mut("rows").and_then(Value::as_array_mut) {
        rows.sort_by_cached_key(Value::to_string);
    }
    let query = json!({"sql":case["sql"],"ansi":case["ansi"],"ordered":ordered,
        "expected":expected,"spark_error_phase":"collect"});
    let actual = match AssertUnwindSafe(compare_exact_spark_query_stages(session, &query))
        .catch_unwind()
        .await
    {
        Ok(Ok(actual)) => actual,
        Ok(Err(error)) => json!({"observation_error":error.to_string()}),
        Err(panic) => {
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied());
            json!({"panic":message})
        }
    };
    let rows_match = actual["rows_match"] == true && actual["ordered_rows_match"] == true;
    let types_match = executed_fields(schema)
        .is_some_and(|fields| Some(fields) == executed_fields(&actual["physical_schema"]));
    let cardinality = expected["condition"] == CARDINALITY;
    let matches = if cardinality {
        actual["native_phase"] == "collect" && actual["error_matches"] == true
    } else {
        actual["error_matches"] == true && rows_match && types_match
    };
    let receipt = json!({"corpus":corpus,"id":case["id"],"sql":case["sql"],"ansi":case["ansi"],
        "batch":batch,"matches":matches,"rows_match":(!cardinality).then_some(rows_match),
        "types_match":(!cardinality).then_some(types_match),"spark":case,"actual":actual});
    eprintln!("CORRELATED_SCALAR_EXECUTION {receipt}");
    matches
}

#[tokio::test]
async fn correlated_scalar_execution_matches_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(INPUTS)?;
    let fresh = fresh_references(&manifest, RESULTS, FAILURES, 9)?;
    let review_manifest: Value = serde_json::from_str(REVIEW_INPUTS)?;
    assert_eq!(
        manifest["registered_inputs"],
        review_manifest["registered_inputs"]
    );
    let review = fresh_references(&review_manifest, REVIEW_RESULTS, REVIEW_FAILURES, 9)?;
    let legacy_valid = records(LEGACY_VALID_RESULTS)?;
    let archive_review = records(ARCHIVE_REVIEW_RESULTS)?;
    let operator_composition = records(OPERATOR_COMPOSITION_RESULTS)?;
    for (references, count) in [
        (&legacy_valid, 48),
        (&archive_review, 17),
        (&operator_composition, 22),
    ] {
        let ids: BTreeSet<_> = references
            .iter()
            .map(|case| (case["id"].as_str(), case["ansi"].as_bool()))
            .collect();
        if references.len() != count
            || ids.len() != count
            || references.iter().any(|case| {
                case["status"] != "ok"
                    || !case["rows"].is_array()
                    || !case["physical_schema"]["fields"].is_array()
            })
        {
            return Err("incomplete archived scalar reference successes".into());
        }
    }
    let rounding = records(ROUNDING)?;
    let single_row = records(SINGLE_ROW)?;
    let historical_stages = records(SINGLE_ROW_STAGES)?;
    let mut rounding_ids = BTreeSet::new();
    for case in &rounding {
        let id = case["id"].as_str().ok_or("missing rounding ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing rounding ANSI mode")?;
        if !rounding_ids.insert((id, ansi))
            || case["failure_stage"] != "execution"
            || case["actual"]["condition"] != CARDINALITY
        {
            return Err(format!("invalid frozen rounding case {id}").into());
        }
    }
    let historical_controls: Vec<_> = historical_stages[1..]
        .iter()
        .filter(|case| {
            case["id"].as_str().is_some_and(|id| {
                ["_plain_", "_nested_alias_", "_nested_reuse_"]
                    .iter()
                    .any(|control| id.contains(control))
            })
        })
        .collect();
    let historical_correlated: Vec<_> = single_row[1..]
        .iter()
        .filter(|case| {
            case["id"]
                .as_str()
                .is_some_and(|id| id.contains("_correlated_"))
        })
        .collect();
    if rounding_ids.len() != 20
        || historical_controls.len() != 36
        || historical_correlated.len() != 18
    {
        return Err("incomplete historical Spark observations".into());
    }

    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = session(batch);
        spark_inputs::register_recorded_inputs(
            &session,
            &json!({"inputs":manifest["registered_inputs"]}),
            batch,
        )?;
        spark_inputs::register_recorded_inputs(&session, &single_row[0], batch)?;
        for (corpus, cases) in [
            ("fresh", fresh.iter().collect::<Vec<_>>()),
            ("review", review.iter().collect()),
            ("legacy_valid", legacy_valid.iter().collect()),
            ("archive_review", archive_review.iter().collect()),
            (
                "operator_composition",
                operator_composition.iter().collect(),
            ),
            ("frozen_rounding", rounding.iter().collect()),
            ("historical_single_row", historical_controls.clone()),
            ("historical_inner_relation", historical_correlated.clone()),
        ] {
            for case in cases {
                let (expected, schema) = match corpus {
                    "fresh"
                    | "review"
                    | "legacy_valid"
                    | "archive_review"
                    | "operator_composition" => (
                        if case["status"] == "ok" {
                            json!({"rows":case["rows"]})
                        } else {
                            json!({"condition":case["condition"]})
                        },
                        &case["physical_schema"],
                    ),
                    "frozen_rounding" => (case["actual"].clone(), &case["schema"]),
                    "historical_single_row" => (
                        case["expected"].clone(),
                        &case["executed_schema_after_collect"],
                    ),
                    _ => (case["expected"].clone(), &case["expected"]["schema"]),
                };
                if !observe(&session, batch, corpus, case, &expected, schema).await {
                    differences.push(format!(
                        "{corpus} {} ANSI={} batch={batch}",
                        case["id"], case["ansi"]
                    ));
                }
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} correlated scalar comparisons differ:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
