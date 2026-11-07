//! Equal-result Spark CASE tails retain required volatility and preparation errors.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{ScalarValue, ToDFSchema};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Case, Expr, LogicalPlanBuilder, col, lit, scalar_subquery};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
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

const REFERENCE: &str = include_str!("fixtures/case-result-tails-spark.jsonl");
const VOLATILE_SUBQUERIES: &str =
    include_str!("fixtures/case-result-tails-volatile-subqueries-spark.jsonl");
const RANGE_SUBQUERY: &str = include_str!("fixtures/case-result-tails-range-subquery-spark.jsonl");
const VOLATILE_FLOATS: &str =
    include_str!("fixtures/case-result-tails-volatile-floats-spark.jsonl");
const RAW_FLOATS: &str = include_str!("fixtures/case-result-tails-raw-floats-spark.jsonl");
const SPARK_IF: &str = include_str!("fixtures/conditional-nested-if-spark.jsonl");
const ALIAS_LINEAGE: &str = include_str!("fixtures/case-result-tails-alias-lineage-spark.jsonl");
const HOST_CONTEXTS: &str = include_str!("fixtures/case-result-tails-host-contexts-spark.jsonl");

fn references(source: &str, count: usize) -> TestResult<Vec<Value>> {
    let records = source
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records.len() - 1, count);
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["input"].as_str().ok_or("missing input")?,
                case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            ))
        })
        .collect::<TestResult<std::collections::BTreeSet<_>>>()?;
    assert_eq!(identities.len(), count);
    Ok(records)
}

fn condition_id_matches(case: &Value, comparison: &Value) -> Option<bool> {
    case["expected"]["condition"].as_str().map(|condition| {
        comparison["error"]
            .as_str()
            .is_some_and(|error| error.contains(&format!("[{condition}]")))
    })
}

fn complete_spark_matches(case: &Value, comparison: &Value) -> bool {
    comparison["matches"] == true
        && comparison["optimized_matches"] == true
        && condition_id_matches(case, comparison) != Some(false)
}

fn schema_matches_with_nullable_gap(actual: &Value, expected: &Value, allowed: bool) -> bool {
    if actual == expected {
        return true;
    }
    if !allowed
        || expected["fields"]
            .as_array()
            .is_none_or(|fields| fields.len() != 1)
        || expected["fields"][0]["nullable"] != false
    {
        return false;
    }
    let mut inherited = expected.clone();
    inherited["fields"][0]["nullable"] = json!(true);
    actual == &inherited
}

fn case_tail_schemas_match(case: &Value, comparison: &Value, nullable_gaps: &[&str]) -> bool {
    [
        ("analyzed", "public_schema"),
        ("optimized", "optimized_schema"),
        ("executed_before_collect", "before_collect"),
        ("executed", "physical_schema"),
    ]
    .into_iter()
    .all(|(stage, key)| {
        let expected = case["schema_stages"].get(stage).or_else(|| {
            (stage == "executed" && case["spark_error_phase"] == "collect")
                .then(|| case["schema_stages"].get("executed_before_collect"))
                .flatten()
        });
        match (comparison.get(key), expected) {
            (Some(actual), Some(expected)) => {
                schema_matches_with_nullable_gap(actual, expected, nullable_gaps.contains(&stage))
            }
            (Some(Value::Null), None) => true,
            _ => false,
        }
    })
}

// These exclusions preserve measured declarations and missing identifiers owned
// elsewhere. Every other Field, value and error boundary still follows Spark.
fn independent_case_tail_gap(case: &Value, comparison: &Value) -> Option<&'static str> {
    let id = case["id"].as_str()?;
    let input = case["input"].as_str()?;
    let ansi = case["ansi"].as_bool()?;
    if comparison["id"] != case["id"]
        || comparison["input"] != case["input"]
        || comparison["ansi"] != case["ansi"]
        || !matches!(comparison["batch"].as_u64(), Some(1 | 32))
    {
        return None;
    }
    let all_stages = [
        "analyzed",
        "optimized",
        "executed_before_collect",
        "executed",
    ];
    let declaration = match (id, input, ansi) {
        (
            "signed_zero_results" | "signed_zero_results_reversed",
            "case_parent_mixed",
            true | false,
        ) => Some(("#711", &["analyzed"][..])),
        (
            "volatile_signed_zero_results" | "volatile_signed_zero_results_reversed",
            "case_parent_mixed",
            true | false,
        ) => Some(("#711", &all_stages[..])),
        (
            "volatile_results_domain"
            | "alias_separate_volatile_sources"
            | "alias_shared_materialized_volatile_source",
            "case_parent_mixed",
            true | false,
        ) => Some(("#1072", &all_stages[..])),
        ("volatile_subquery_results_domain", "case_parent_mixed", true | false) => {
            Some(("#1072", &all_stages[1..]))
        }
        _ => None,
    };
    let optimizer_succeeded = comparison.get("optimized_error") == Some(&Value::Null)
        && comparison["optimized_observation"]["names_match"] == true
        && comparison["optimized_observation"]["schema"] == comparison["optimized_schema"];
    if let Some((owner, stages)) = declaration {
        return (case["ordered"] == false
            && case["expected"].get("condition").is_none()
            && case["expected"]["rows"].is_array()
            && comparison["native_phase"] == "encode"
            && comparison.get("error") == Some(&Value::Null)
            && comparison.get("rows") == case["expected"].get("rows")
            && comparison.get("rows_raw") == case["expected"].get("rows")
            && optimizer_succeeded
            && case_tail_schemas_match(case, comparison, stages))
        .then_some(owner);
    }
    let error_boundary = match (id, input, ansi) {
        (
            "equal_closed_error_guard"
            | "all_null_closed_error_guard"
            | "subquery_guard_closed_equal",
            "one_row",
            true,
        )
        | ("equal_local_error_guard" | "all_null_local_error_guard", "local_values", true) => {
            Some((
                "#865",
                "DIVIDE_BY_ZERO",
                "optimized_schema",
                "create_physical_plan",
                "preserve_spark_expression_errors\ncaused by\nArrow error: Divide by zero error",
            ))
        }
        (
            "distinct_result_required_guard"
            | "equal_required_result_error"
            | "error_prefix_then_volatile_guard"
            | "subquery_results_error"
            | "alias_case_sum_argument",
            "case_parent_mixed",
            true,
        ) => Some((
            "#865",
            "DIVIDE_BY_ZERO",
            "collect",
            "collect",
            "Arrow error: Divide by zero error",
        )),
        ("subquery_results_multirow", "case_parent_mixed", true | false) => Some((
            "#140",
            "SCALAR_SUBQUERY_TOO_MANY_ROWS",
            "collect",
            "collect",
            "Execution error: Scalar subquery returned more than one row",
        )),
        _ => None,
    }?;
    let (owner, condition, spark_phase, native_phase, cause) = error_boundary;
    let optimizer_matches = if native_phase == "create_physical_plan" {
        comparison["optimized_error"] == cause
            && comparison.get("optimized_observation") == Some(&Value::Null)
    } else {
        optimizer_succeeded
    };
    (case["expected"]["condition"] == condition
        && case["expected"].get("rows").is_none()
        && case["spark_error_phase"] == spark_phase
        && comparison["native_phase"] == native_phase
        && comparison["error"] == cause
        && comparison.get("rows") == Some(&Value::Null)
        && comparison.get("rows_raw") == Some(&Value::Null)
        && optimizer_matches
        && case_tail_schemas_match(case, comparison, &[]))
    .then_some(owner)
}

async fn compare_references(source: &str, count: usize) -> TestResult {
    let records = references(source, count)?;
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
            let mut comparison =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await?;
            comparison["id"] = case["id"].clone();
            comparison["input"] = case["input"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            let complete_match = complete_spark_matches(case, &comparison);
            let gap_owner = (!complete_match)
                .then(|| independent_case_tail_gap(case, &comparison))
                .flatten();
            comparison["condition_id_matches"] = json!(condition_id_matches(case, &comparison));
            comparison["complete_spark_matches"] = json!(complete_match);
            comparison["independent_gap_owner"] = json!(gap_owner);
            comparison["regression_matches"] = json!(complete_match || gap_owner.is_some());
            eprintln!("CASE_RESULT_TAILS_1066_OBSERVATION {comparison}");
            if comparison["regression_matches"] != true {
                differences.push(format!("expected={case}, actual={comparison}"));
            }
        }
    }
    assert_eq!(comparisons, count * 2);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn case_result_tails_match_spark() -> TestResult {
    compare_references(REFERENCE, 44).await
}

#[tokio::test]
async fn case_volatile_subquery_tails_match_spark() -> TestResult {
    compare_references(VOLATILE_SUBQUERIES, 4).await
}

#[tokio::test]
async fn case_deterministic_range_subquery_tails_match_spark() -> TestResult {
    compare_references(RANGE_SUBQUERY, 2).await
}

#[tokio::test]
async fn case_volatile_float_tails_match_spark() -> TestResult {
    compare_references(VOLATILE_FLOATS, 4).await
}

#[tokio::test]
async fn case_raw_float_tails_match_spark() -> TestResult {
    compare_references(RAW_FLOATS, 4).await
}

#[tokio::test]
async fn spark_case_tail_rules_preserve_spark_if() -> TestResult {
    compare_references(SPARK_IF, 4).await
}

#[tokio::test]
async fn case_result_alias_lineage_matches_spark() -> TestResult {
    compare_references(ALIAS_LINEAGE, 6).await
}

#[tokio::test]
async fn case_result_alias_host_contexts_match_spark() -> TestResult {
    compare_references(HOST_CONTEXTS, 6).await
}

#[test]
fn case_tail_algebraic_boundaries_preserve_required_work() -> TestResult {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]).to_dfschema_ref()?;
    let simplifier = ExprSimplifier::new(SimplifyContext::builder().with_schema(schema).build());
    let guard = (lit(1_i32) / col("id")).eq(lit(0_i32));
    let result = col("id");
    let branch = |guard, result| (Box::new(guard), Box::new(result));
    let marked = |branches, fallback| Expr::Case(Case::new_spark_searched(branches, fallback));
    let equal = marked(
        vec![branch(guard.clone(), result.clone())],
        Some(Box::new(result.clone())),
    );
    assert_eq!(simplifier.simplify_algebraic_once(equal)?.data, result);
    let null = lit(ScalarValue::Int32(None));
    let implicit = marked(vec![branch(guard.clone(), null.clone())], None);
    assert_eq!(simplifier.simplify_algebraic_once(implicit)?.data, null);

    let volatile = datafusion::functions::expr_fn::uuid();
    let subquery = scalar_subquery(Arc::new(
        LogicalPlanBuilder::empty(true)
            .project(vec![lit(1_i32)])?
            .build()?,
    ));
    let volatile_subquery = scalar_subquery(Arc::new(
        LogicalPlanBuilder::empty(true)
            .project(vec![volatile.clone()])?
            .build()?,
    ));
    let opaque_subquery = scalar_subquery(Arc::new(
        datafusion::logical_expr::LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(sail_logical_plan::monotonic_id::MonotonicIdNode::try_new(
                Arc::new(LogicalPlanBuilder::empty(true).build()?),
                "sequence".to_owned(),
                sail_logical_plan::monotonic_id::MonotonicIdPurpose::SqlFunction,
            )?),
        }),
    ));
    for protected in [
        volatile.clone().eq(lit("")),
        volatile_subquery.clone().eq(lit("")),
        opaque_subquery.clone().eq(lit(0_i64)),
    ] {
        let prefix = vec![
            branch(guard.clone(), result.clone()),
            branch(protected, result.clone()),
        ];
        let mut branches = prefix.clone();
        branches.push(branch(guard.clone(), result.clone()));
        let expected = marked(prefix, Some(Box::new(result.clone())));
        let case = marked(branches, Some(Box::new(result.clone())));
        assert_eq!(simplifier.simplify_algebraic_once(case)?.data, expected);
    }
    for protected in [volatile, volatile_subquery, opaque_subquery] {
        let case = marked(
            vec![branch(guard.clone(), protected.clone())],
            Some(Box::new(protected)),
        );
        assert_eq!(simplifier.simplify_algebraic_once(case.clone())?.data, case);
    }
    let deterministic_subquery_guard = marked(
        vec![branch(subquery.clone().eq(lit(1_i32)), result.clone())],
        Some(Box::new(result.clone())),
    );
    assert_eq!(
        simplifier
            .simplify_algebraic_once(deterministic_subquery_guard)?
            .data,
        result
    );
    let deterministic_subquery_results = marked(
        vec![branch(guard.clone(), subquery.clone())],
        Some(Box::new(subquery.clone())),
    );
    assert_eq!(
        simplifier
            .simplify_algebraic_once(deterministic_subquery_results)?
            .data,
        subquery
    );
    let distinct = marked(
        vec![branch(guard.clone(), lit(2_i32))],
        Some(Box::new(result.clone())),
    );
    assert_eq!(
        simplifier.simplify_algebraic_once(distinct.clone())?.data,
        distinct
    );
    let native = Expr::Case(Case::new(
        None,
        vec![branch(guard, result.clone())],
        Some(Box::new(result)),
    ));
    assert_eq!(
        simplifier.simplify_algebraic_once(native.clone())?.data,
        native
    );
    Ok(())
}

#[test]
fn case_tail_result_identity_uses_spark_floats_and_preserves_metadata() -> TestResult {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]).to_dfschema_ref()?;
    let simplifier = ExprSimplifier::new(SimplifyContext::builder().with_schema(schema).build());
    let nan = f64::from_bits(0x7ff8_0000_0000_0000);
    let other_nan = f64::from_bits(0x7ff8_0000_0000_0001);
    for (left, right, equal) in [
        (0.0, -0.0, true),
        (-0.0, 0.0, true),
        (nan, nan, true),
        (nan, other_nan, true),
        (1.0, 2.0, false),
    ] {
        let fallback = lit(right);
        let case = Expr::Case(Case::new_spark_searched(
            vec![(Box::new(col("id").eq(lit(0_i32))), Box::new(lit(left)))],
            Some(Box::new(fallback.clone())),
        ));
        let expected = if equal { fallback } else { case.clone() };
        assert_eq!(simplifier.simplify_algebraic_once(case)?.data, expected);
    }
    for (left, right) in [(0.0_f32, -0.0_f32), (f32::NAN, f32::from_bits(0x7fc0_0001))] {
        let fallback = lit(right);
        let case = Expr::Case(Case::new_spark_searched(
            vec![(Box::new(col("id").eq(lit(0_i32))), Box::new(lit(left)))],
            Some(Box::new(fallback.clone())),
        ));
        assert_eq!(simplifier.simplify_algebraic_once(case)?.data, fallback);
    }
    let case = Expr::Case(Case::new_spark_searched(
        vec![(
            Box::new(col("id").eq(lit(0_i32))),
            Box::new(col("id").alias("left")),
        )],
        Some(Box::new(col("id").alias("right"))),
    ));
    assert_eq!(simplifier.simplify_algebraic_once(case.clone())?.data, case);
    let annotated = |source: &str| {
        col("id").alias_with_metadata(
            "value",
            Some(FieldMetadata::from(std::collections::HashMap::from([(
                "source".to_owned(),
                source.to_owned(),
            )]))),
        )
    };
    let case = Expr::Case(Case::new_spark_searched(
        vec![(
            Box::new(col("id").eq(lit(0_i32))),
            Box::new(annotated("left")),
        )],
        Some(Box::new(annotated("right"))),
    ));
    assert_eq!(simplifier.simplify_algebraic_once(case.clone())?.data, case);
    Ok(())
}

#[tokio::test]
async fn spark_case_tail_rules_preserve_native_case() -> TestResult {
    let records = references(REFERENCE, 44)?;
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
                "SELECT CASE WHEN id=0 THEN required ELSE required END AS r FROM case_parent_mixed",
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
            "SELECT CASE WHEN 1/id=0 THEN required ELSE required END AS r FROM case_parent_mixed",
            "SELECT CASE WHEN id=0 THEN CAST(NULL AS INT) END AS r FROM case_parent_mixed",
            "SELECT CASE id WHEN 0 THEN v ELSE v END AS r FROM case_parent_mixed",
        ] {
            let expected = spark_results::capture_native_query_observation(&native, sql).await?;
            let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
            assert_eq!(actual, expected, "{sql}, after Spark={after_spark}");
        }
    }
    Ok(())
}

// Synthetic observations use only the unchanged Spark record's Fields and rows.
// Raw match flags stay false so these checks exercise the narrow fallback itself.
fn reference_observation_for_gap_test(case: &Value) -> Value {
    let stages = &case["schema_stages"];
    let optimized = stages["optimized"].clone();
    let physical = stages
        .get("executed")
        .unwrap_or(&stages["executed_before_collect"]);
    json!({
        "id":case["id"], "input":case["input"], "ansi":case["ansi"], "batch":1,
        "public_schema":stages["analyzed"], "optimized_schema":optimized,
        "before_collect":stages["executed_before_collect"], "physical_schema":physical,
        "rows":case["expected"]["rows"], "rows_raw":case["expected"]["rows"],
        "error":null, "native_phase":"encode", "matches":false, "optimized_matches":false,
        "optimized_error":null,
        "optimized_observation":if optimized.is_null() { Value::Null } else {
            json!({"schema":optimized,"names_match":true})
        },
    })
}

#[test]
fn case_tail_declaration_gates_reject_changed_owned_facets() -> TestResult {
    let records = references(REFERENCE, 44)?;
    for case in records[1..].iter().filter(|case| {
        matches!(
            case["id"].as_str(),
            Some("signed_zero_results" | "signed_zero_results_reversed")
        )
    }) {
        let mut observed = reference_observation_for_gap_test(case);
        observed["public_schema"]["fields"][0]["nullable"] = json!(true);
        assert_eq!(independent_case_tail_gap(case, &observed), Some("#711"));
        for (pointer, wrong) in [
            ("/rows/0/0", json!("wrong zero sign")),
            ("/rows_raw/0/0", json!("wrong zero sign")),
            ("/before_collect/fields/0/nullable", json!(true)),
            ("/physical_schema/fields/0/nullable", json!(true)),
            ("/optimized_schema/fields/0/nullable", json!(true)),
            ("/public_schema/fields/0/name", json!("wrong name")),
            ("/public_schema/fields/0/type", json!("integer")),
            (
                "/public_schema/fields/0/metadata",
                json!({"source":"wrong"}),
            ),
            ("/native_phase", json!("collect")),
            ("/error", json!("Arrow error: Divide by zero error")),
            ("/batch", json!(2)),
            ("/id", json!("unlisted")),
        ] {
            let mut wrong_observation = observed.clone();
            *wrong_observation
                .pointer_mut(pointer)
                .ok_or("missing gate mutation target")? = wrong;
            assert_eq!(
                independent_case_tail_gap(case, &wrong_observation),
                None,
                "{pointer}"
            );
        }
        // A future fully correct declaration passes without requiring the old defect.
        let mut correct = reference_observation_for_gap_test(case);
        correct["matches"] = json!(true);
        correct["optimized_matches"] = json!(true);
        assert!(complete_spark_matches(case, &correct));
    }
    for (source, count, id, keys, owner) in [
        (
            VOLATILE_FLOATS,
            4,
            "volatile_signed_zero_results",
            &[
                "public_schema",
                "optimized_schema",
                "before_collect",
                "physical_schema",
            ][..],
            "#711",
        ),
        (
            REFERENCE,
            44,
            "volatile_results_domain",
            &[
                "public_schema",
                "optimized_schema",
                "before_collect",
                "physical_schema",
            ][..],
            "#1072",
        ),
        (
            VOLATILE_SUBQUERIES,
            4,
            "volatile_subquery_results_domain",
            &["optimized_schema", "before_collect", "physical_schema"][..],
            "#1072",
        ),
    ] {
        let records = references(source, count)?;
        for case in records[1..].iter().filter(|case| case["id"] == id) {
            let mut observed = reference_observation_for_gap_test(case);
            for key in keys {
                observed[*key]["fields"][0]["nullable"] = json!(true);
            }
            observed["optimized_observation"]["schema"] = observed["optimized_schema"].clone();
            assert_eq!(independent_case_tail_gap(case, &observed), Some(owner));
            for key in [
                "public_schema",
                "optimized_schema",
                "before_collect",
                "physical_schema",
            ] {
                let mut wrong = observed.clone();
                wrong[key]["fields"][0]["metadata"] = json!({"source":"wrong"});
                assert_eq!(independent_case_tail_gap(case, &wrong), None, "{id}: {key}");
            }
            let mut wrong = observed.clone();
            wrong["rows"] = json!([["35"], ["36"], ["36"]]);
            assert_eq!(independent_case_tail_gap(case, &wrong), None, "{id}: rows");
        }
    }
    Ok(())
}

#[test]
fn case_tail_error_gates_require_the_original_work_and_identifier_facet() -> TestResult {
    for (source, count, id, owner, cause, phase) in [
        (
            HOST_CONTEXTS,
            6,
            "alias_case_sum_argument",
            "#865",
            "Arrow error: Divide by zero error",
            "collect",
        ),
        (
            REFERENCE,
            44,
            "equal_closed_error_guard",
            "#865",
            "preserve_spark_expression_errors\ncaused by\nArrow error: Divide by zero error",
            "create_physical_plan",
        ),
        (
            REFERENCE,
            44,
            "subquery_results_multirow",
            "#140",
            "Execution error: Scalar subquery returned more than one row",
            "collect",
        ),
    ] {
        let records = references(source, count)?;
        let case = records[1..]
            .iter()
            .find(|case| case["id"] == id && case["ansi"] == true)
            .ok_or("missing error gate reference")?;
        let mut observed = reference_observation_for_gap_test(case);
        observed["error"] = json!(cause);
        observed["native_phase"] = json!(phase);
        if phase == "create_physical_plan" {
            observed["optimized_error"] = json!(cause);
        }
        assert_eq!(independent_case_tail_gap(case, &observed), Some(owner));
        assert_eq!(condition_id_matches(case, &observed), Some(false));
        assert!(!complete_spark_matches(case, &observed));
        for (pointer, wrong) in [
            ("/error", Value::Null),
            ("/error", json!("Arrow error: different failure")),
            (
                "/native_phase",
                json!(if phase == "collect" {
                    "create_physical_plan"
                } else {
                    "collect"
                }),
            ),
            ("/rows", json!([])),
            ("/rows_raw", json!([])),
            (
                "/public_schema/fields/0/metadata",
                json!({"source":"wrong"}),
            ),
        ] {
            let mut wrong_observation = observed.clone();
            *wrong_observation
                .pointer_mut(pointer)
                .ok_or("missing error gate mutation target")? = wrong;
            assert_eq!(
                independent_case_tail_gap(case, &wrong_observation),
                None,
                "{id}: {pointer}"
            );
        }
        for key in ["rows", "rows_raw", "public_schema"] {
            let mut missing = observed.clone();
            missing
                .as_object_mut()
                .ok_or("expected observation object")?
                .remove(key);
            assert_eq!(
                independent_case_tail_gap(case, &missing),
                None,
                "{id}: missing {key}"
            );
        }
        let mut corrected = observed.clone();
        let condition = case["expected"]["condition"]
            .as_str()
            .ok_or("missing error condition")?;
        corrected["error"] = json!(format!("[{condition}] {cause}"));
        corrected["matches"] = json!(true);
        corrected["optimized_matches"] = json!(true);
        assert_eq!(condition_id_matches(case, &corrected), Some(true));
        assert!(complete_spark_matches(case, &corrected));
    }
    Ok(())
}

#[test]
fn scalar_cardinality_cause_does_not_receive_strict_condition_id_credit() -> TestResult {
    let records = references(REFERENCE, 44)?;
    for case in records[1..]
        .iter()
        .filter(|case| case["id"] == "subquery_results_multirow")
    {
        for batch in [1, 32] {
            let mut observed = reference_observation_for_gap_test(case);
            observed["batch"] = json!(batch);
            observed["native_phase"] = json!("collect");
            observed["error"] =
                json!("Execution error: Scalar subquery returned more than one row");
            // The canonical observer matches this cause without a literal condition ID.
            observed["matches"] = json!(true);
            observed["optimized_matches"] = json!(true);
            assert_eq!(condition_id_matches(case, &observed), Some(false));
            assert!(!complete_spark_matches(case, &observed));
            assert_eq!(independent_case_tail_gap(case, &observed), Some("#140"));

            observed["error"] = json!(
                "[SCALAR_SUBQUERY_TOO_MANY_ROWS] Execution error: Scalar subquery returned more than one row"
            );
            assert_eq!(condition_id_matches(case, &observed), Some(true));
            assert!(complete_spark_matches(case, &observed));
        }
    }
    Ok(())
}
