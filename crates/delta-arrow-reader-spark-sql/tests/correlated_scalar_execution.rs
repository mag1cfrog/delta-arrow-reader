//! Replay correlated scalar execution against independent Spark observations.

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
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
const BOUNDARY_RESULTS: &str =
    include_str!("fixtures/correlated-scalar-execution/boundary-review-results.jsonl");
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
    let boundary = records(BOUNDARY_RESULTS)?;
    for (references, count) in [
        (&legacy_valid, 48),
        (&archive_review, 17),
        (&operator_composition, 22),
        (&boundary, 10),
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
            ("boundary_review", boundary.iter().collect()),
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
                    | "operator_composition"
                    | "boundary_review" => (
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

#[tokio::test]
async fn correlated_union_all_preserves_values_types_and_cardinality() -> TestResult {
    let references = records(include_str!(
        "fixtures/correlated-scalar-union/spark-results.jsonl"
    ))?;
    assert_eq!(references[0]["spark_version"], "4.2.0");
    assert_eq!(references.len() - 1, 36);
    for input in references[0]["inputs"].as_array().ok_or("missing inputs")? {
        let batch = spark_inputs::decode_recorded_batch(input)?;
        spark_results::validate_batch(batch.schema().as_ref(), &batch)?;
    }
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = session(batch);
        spark_inputs::register_recorded_inputs(&session, &references[0], batch)?;
        for case in &references[1..] {
            if case["cardinality_error"] == true {
                let query = plan_query(
                    &session,
                    case["sql"].as_str().ok_or("missing SQL")?,
                    &SparkSqlOptions {
                        ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                let physical = query.create_physical_plan(&session).await?;
                let result = datafusion::physical_plan::collect(physical, session.task_ctx()).await;
                let error = result.err().ok_or("scalar cardinality guard did not run")?;
                assert!(
                    error
                        .to_string()
                        .contains("Scalar subquery returned more than one row")
                );
                println!(
                    "CORRELATED_UNION {}",
                    json!({"id":case["id"],"ansi":case["ansi"],"batch":batch,
                        "actual":{"execution_error":error.to_string()}})
                );
                continue;
            }
            let result = spark_results::capture_executed_query_result(&session, case).await;
            let actual = match result {
                Ok(mut actual) => {
                    spark_results::remove_nullability_and_metadata(&mut actual);
                    actual
                }
                Err(error) => json!({"error":error.to_string()}),
            };
            println!(
                "CORRELATED_UNION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch,"actual":actual})
            );
            let mut expected = json!({"rows":case["rows"],"schema":case["schema"]});
            spark_results::remove_nullability_and_metadata(&mut expected);
            if actual != expected {
                differences.push(format!("{case} batch={batch}: {actual}"));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn correlated_windows_preserve_values_types_and_cardinality() -> TestResult {
    let references = records(include_str!(
        "fixtures/correlated-scalar-window/spark-results.jsonl"
    ))?;
    assert_eq!(references[0]["spark_version"], "4.2.0");
    assert_eq!(references.len() - 1, 54);
    let mut seen = BTreeSet::new();
    for case in &references[1..] {
        assert!(seen.insert((case["id"].to_string(), case["ansi"].as_bool())));
        if case["id"] == "issue_row_number" {
            assert_eq!(case["rows"], json!([["0", null], ["1", "20"], ["2", "30"]]));
            assert_eq!(
                executed_fields(&case["schema"]),
                Some(vec![
                    (&json!("k"), &json!("integer")),
                    (&json!("r"), &json!("integer"))
                ])
            );
        }
        if case["id"] == "different_window_specs" {
            assert_eq!(
                case["rows"],
                json!([
                    ["0", null],
                    ["1", "940"],
                    ["2", "940"],
                    ["3", "730"],
                    ["4", null],
                    ["5", null]
                ])
            );
        }
    }
    for input in references[0]["inputs"].as_array().ok_or("missing inputs")? {
        let batch = spark_inputs::decode_recorded_batch(input)?;
        spark_results::validate_batch(batch.schema().as_ref(), &batch)?;
    }
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = session(batch);
        spark_inputs::register_recorded_inputs(&session, &references[0], batch)?;
        for case in &references[1..] {
            let result: TestResult<Value> = async {
                if case["cardinality_error"] == true {
                    let query = plan_query(
                        &session,
                        case["sql"].as_str().ok_or("missing SQL")?,
                        &SparkSqlOptions {
                            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                            ..SparkSqlOptions::default()
                        },
                    )
                    .await?;
                    let physical = query.create_physical_plan(&session).await?;
                    let result =
                        datafusion::physical_plan::collect(physical, session.task_ctx()).await;
                    let error = result.err().ok_or("scalar cardinality guard did not run")?;
                    assert!(
                        error
                            .to_string()
                            .contains("Scalar subquery returned more than one row")
                    );
                    return Ok(json!({"cardinality_error":true}));
                }
                spark_results::capture_executed_query_result(&session, case).await
            }
            .await;
            let mut actual = result.unwrap_or_else(|error| json!({"error":error.to_string()}));
            spark_results::remove_nullability_and_metadata(&mut actual);
            let mut expected = if case["cardinality_error"] == true {
                json!({"cardinality_error":true})
            } else {
                json!({"rows":case["rows"],"schema":case["schema"]})
            };
            spark_results::remove_nullability_and_metadata(&mut expected);
            println!(
                "CORRELATED_WINDOW {}",
                json!({"id":case["id"],"ansi":case["ansi"],
                "batch":batch,"matches":actual==expected,"actual":actual})
            );
            if actual != expected {
                differences.push(format!(
                    "{} ANSI={} batch={batch}: {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn nested_live_scalars_preserve_values_types_and_cardinality() -> TestResult {
    let references = records(include_str!(
        "fixtures/correlated-scalar-nested/spark-results.jsonl"
    ))?;
    assert_eq!(references[0]["spark_version"], "4.2.0");
    assert_eq!(references.len() - 1, 50);
    let mut seen = BTreeSet::new();
    for case in &references[1..] {
        assert!(seen.insert((case["id"].to_string(), case["ansi"].as_bool())));
        if case["id"] == "issue_two_live_levels" {
            assert_eq!(case["rows"], json!([["0", null], ["1", "20"], ["2", "30"]]));
            assert_eq!(
                executed_fields(&case["schema"]),
                Some(vec![
                    (&json!("k"), &json!("integer")),
                    (&json!("r"), &json!("integer"))
                ])
            );
        }
    }
    for input in references[0]["inputs"].as_array().ok_or("missing inputs")? {
        let batch = spark_inputs::decode_recorded_batch(input)?;
        spark_results::validate_batch(batch.schema().as_ref(), &batch)?;
    }
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = session(batch);
        spark_inputs::register_recorded_inputs(&session, &references[0], batch)?;
        for case in &references[1..] {
            let result: TestResult<Value> = async {
                if case["id"] == "nested_filter_after_local_filter" {
                    use datafusion::common::tree_node::TreeNode;
                    use datafusion::logical_expr::{LogicalPlan, col, lit};

                    let query = plan_query(
                        &session,
                        case["sql"].as_str().ok_or("missing SQL")?,
                        &SparkSqlOptions {
                            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                            ..SparkSqlOptions::default()
                        },
                    )
                    .await?;
                    let mut state = session.state();
                    state.config_mut().options_mut().extensions.insert(
                        sail_plan::expression_errors::SparkSqlExpressionSemantics::default(),
                    );
                    let optimized = state.optimize(&query.logical_plan)?;
                    let source_filter = optimized.exists(|node| {
                        let LogicalPlan::Filter(filter) = node else {
                            return Ok(false);
                        };
                        Ok(filter.predicate == col("ordinary.v").gt_eq(lit(20_i32))
                            && matches!(filter.input.as_ref(), LogicalPlan::TableScan(scan)
                                    if scan.table_name.table() == "ordinary"))
                    })?;
                    assert!(
                        source_filter,
                        "local filter must reach the source before nested scalar evaluation"
                    );
                }
                if case["cardinality_error"] == true {
                    let query = plan_query(
                        &session,
                        case["sql"].as_str().ok_or("missing SQL")?,
                        &SparkSqlOptions {
                            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                            ..SparkSqlOptions::default()
                        },
                    )
                    .await?;
                    let physical = query.create_physical_plan(&session).await?;
                    let result =
                        datafusion::physical_plan::collect(physical, session.task_ctx()).await;
                    let error = result.err().ok_or("scalar cardinality guard did not run")?;
                    assert!(
                        error
                            .to_string()
                            .contains("Scalar subquery returned more than one row")
                    );
                    return Ok(json!({"cardinality_error":true}));
                }
                spark_results::capture_executed_query_result(&session, case).await
            }
            .await;
            let mut actual = result.unwrap_or_else(|error| json!({"error":error.to_string()}));
            spark_results::remove_nullability_and_metadata(&mut actual);
            let mut expected = if case["cardinality_error"] == true {
                json!({"cardinality_error":true})
            } else {
                json!({"rows":case["rows"],"schema":case["schema"]})
            };
            spark_results::remove_nullability_and_metadata(&mut expected);
            println!(
                "CORRELATED_NESTED {}",
                json!({"id":case["id"],"ansi":case["ansi"],
                    "batch":batch,"matches":actual==expected,"actual":actual})
            );
            if actual != expected {
                differences.push(format!(
                    "{} ANSI={} batch={batch}: {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
