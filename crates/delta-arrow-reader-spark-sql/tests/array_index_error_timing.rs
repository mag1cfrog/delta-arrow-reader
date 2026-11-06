//! ARRAY bounds errors retain Spark's preparation phase and conditional reachability.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
#[allow(dead_code)]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(1),
                ),
        )
        .build(),
    )
}

fn records(text: &str) -> TestResult<Vec<Value>> {
    Ok(text
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn retained_facets() -> TestResult<Value> {
    let mut facets: Value = serde_json::from_str(include_str!(
        "fixtures/array-index-error-timing/retained-baseline-facets.json"
    ))?;
    assert_eq!(facets["source"], "66e35259e841ef0bb4db56d00444fd3c9223e738");
    assert_eq!(
        facets["source_log_sha256"],
        "cd03b1e42c9fa5aa28105ee378cdfa3d8a9c0198d5e3295003dc1a0d5ddcd59f"
    );
    assert_eq!(
        facets["spark_reference_sha256"],
        "5ae12b0ac050c7e44f89d8a7a448260750fe5371845d393dd5df987727553960"
    );
    assert_eq!(facets["is_spark_compatibility_credit"], false);
    assert_eq!(
        facets["field_facets"]
            .as_array()
            .ok_or("missing Field facets")?
            .len(),
        86
    );
    assert_eq!(
        facets["cause_facets"]
            .as_array()
            .ok_or("missing cause facets")?
            .len(),
        4
    );
    let conditional: Value = serde_json::from_str(include_str!(
        "fixtures/array-index-error-timing/conditional-retained-baseline-facets.json"
    ))?;
    assert_eq!(
        conditional["source"],
        "88d89d7bd25f38ac0e6d028a7ca09461b103aa05"
    );
    assert_eq!(conditional["runtime_source"], facets["runtime_source"]);
    assert_eq!(
        conditional["source_log_sha256"],
        "687dc3e4f68141d1a2feedc8b06361503e83b9dc426fbcc6d7e7903064ba923d"
    );
    assert_eq!(
        conditional["spark_reference_sha256"],
        "02a4c392cec9051881471f5489d433e698960509e6f280ea918efc2120b80e68"
    );
    assert_eq!(conditional["is_spark_compatibility_credit"], false);
    let fields = conditional["field_facets"]
        .as_array()
        .ok_or("missing conditional Field facets")?;
    assert_eq!(fields.len(), 26);
    assert!(
        conditional["cause_facets"]
            .as_array()
            .ok_or("missing conditional causes")?
            .is_empty()
    );
    facets["field_facets"]
        .as_array_mut()
        .ok_or("missing mutable Field facets")?
        .extend(fields.iter().cloned());
    let bindings: Value = serde_json::from_str(include_str!(
        "fixtures/array-index-error-timing/binding-retained-baseline-facets.json"
    ))?;
    assert_eq!(
        bindings["source"],
        "e9fa5d8e9341f785f046f2b8fb3203ce61a0c473"
    );
    assert_eq!(bindings["runtime_source"], facets["runtime_source"]);
    assert_eq!(
        bindings["source_log_sha256"],
        "ad152dc389f1b81ec4414049dd98cc6f2f37aa63eeda92fe3ea9e4ca3a3aa72f"
    );
    assert_eq!(
        bindings["spark_reference_sha256"],
        "7683c84bc284d4bb9fb68dcb2124bb6bee8dc99ef6bc719cd7c39403d31bd108"
    );
    assert_eq!(bindings["is_spark_compatibility_credit"], false);
    assert!(
        bindings["field_facets"]
            .as_array()
            .ok_or("missing binding Fields")?
            .is_empty()
    );
    let outcomes = bindings["cause_facets"]
        .as_array()
        .ok_or("missing binding outcomes")?;
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|facet| facet["owner"] == 1004));
    facets["cause_facets"]
        .as_array_mut()
        .ok_or("missing mutable causes")?
        .extend(outcomes.iter().cloned());
    let opaque: Value = serde_json::from_str(include_str!(
        "fixtures/array-index-error-timing/opaque-retained-baseline-facets.json"
    ))?;
    assert_eq!(opaque["source"], "ded20e6f6b783d13d3427680900396cbb7ab7ff3");
    assert_eq!(
        opaque["runtime_source"],
        "cb9f10dcda765cafe81e63eabe7412c94949da1e"
    );
    assert_eq!(
        opaque["source_log_sha256"],
        "b39b9e2d45b19d9b3b308ab5b31bf059a880e7c9c327237750cb98221d8fc811"
    );
    assert_eq!(
        opaque["spark_reference_sha256"],
        "28a922e7c069cd4c25578a87e367e00735f7997c46f541e392fae740bfccda42"
    );
    assert_eq!(opaque["is_spark_compatibility_credit"], false);
    let fields = opaque["field_facets"]
        .as_array()
        .ok_or("missing opaque Field facets")?;
    assert_eq!(fields.len(), 2);
    assert!(fields.iter().all(|facet| facet["owner"] == 1000));
    assert!(
        opaque["cause_facets"]
            .as_array()
            .ok_or("missing opaque causes")?
            .is_empty()
    );
    facets["field_facets"]
        .as_array_mut()
        .ok_or("missing mutable Fields")?
        .extend(fields.iter().cloned());
    assert_eq!(
        facets["field_facets"]
            .as_array()
            .ok_or("missing combined Fields")?
            .len(),
        114
    );
    assert_eq!(
        facets["cause_facets"]
            .as_array()
            .ok_or("missing combined causes")?
            .len(),
        6
    );
    Ok(facets)
}

fn schema_with_retained_facets_matches(
    case: &Value,
    actual: &Value,
    batch: usize,
    fixture: &str,
    stage: &str,
    observation_key: &str,
    facets: &Value,
) -> TestResult<bool> {
    let expected = case["schema_stages"]
        .get(stage)
        .or_else(|| (stage == "analyzed").then(|| &case["public_schema"]))
        .unwrap_or(&Value::Null);
    let mut schema = actual[observation_key].clone();
    for facet in facets["field_facets"]
        .as_array()
        .ok_or("missing Field facets")?
    {
        if facet["fixture"] != fixture
            || facet["id"] != case["id"]
            || facet["ansi"] != case["ansi"]
            || facet["batch"] != batch
            || facet["observation_key"] != observation_key
        {
            continue;
        }
        assert_eq!(facet["sql"], case["sql"]);
        assert_eq!(facet["stage"], stage);
        if facet["owner"] == 1002 {
            assert_eq!(facet["json_pointer"], "/fields/0/metadata");
        } else {
            assert!(matches!(facet["owner"].as_u64(), Some(1000 | 1001)));
            assert_eq!(facet["json_pointer"], "/fields/0/nullable");
        }
        assert_eq!(facet["all_other_schema_parts_match"], true);
        // A preparation error has no later Field to retain.
        if schema.is_null() {
            continue;
        }
        let pointer = facet["json_pointer"]
            .as_str()
            .ok_or("missing Field pointer")?;
        let Some(value) = schema.pointer_mut(pointer) else {
            return Ok(false);
        };
        if *value != facet["spark"] && *value != facet["inherited_dar"] {
            return Ok(false);
        }
        *value = facet["spark"].clone();
    }
    Ok(&schema == expected)
}

fn scoped_contract_matches(
    case: &Value,
    actual: &Value,
    batch: usize,
    fixture: &str,
    facets: &Value,
    cause_matches: bool,
    raw_rows_match: bool,
) -> TestResult<bool> {
    for facet in facets["cause_facets"]
        .as_array()
        .ok_or("missing cause facets")?
    {
        if facet["fixture"] == fixture
            && facet["id"] == case["id"]
            && facet["ansi"] == case["ansi"]
            && facet["batch"] == batch
            && facet["owner"] == 1004
        {
            assert_eq!(facet["sql"], case["sql"]);
            let keys = [
                "public_schema",
                "optimized_schema",
                "before_collect",
                "physical_schema",
                "rows",
                "rows_raw",
                "native_phase",
                "error",
                "optimized_error",
            ];
            assert!(keys.iter().all(|key| facet.get(*key).is_some()));
            // #1004 retains only the complete measured outcome; every value and Field is bound.
            if keys.iter().all(|key| actual[*key] == facet[*key]) {
                return Ok(true);
            }
        }
    }
    for (stage, key) in [
        ("analyzed", "public_schema"),
        ("optimized", "optimized_schema"),
        ("executed_before_collect", "before_collect"),
        ("executed", "physical_schema"),
    ] {
        if !schema_with_retained_facets_matches(case, actual, batch, fixture, stage, key, facets)? {
            return Ok(false);
        }
    }
    if actual["rows_match"] != true || actual["ordered_rows_match"] != true || !raw_rows_match {
        return Ok(false);
    }
    let mut error_matches = actual["error_matches"] == true && cause_matches;
    for facet in facets["cause_facets"]
        .as_array()
        .ok_or("missing cause facets")?
    {
        if facet["fixture"] == fixture
            && facet["id"] == case["id"]
            && facet["ansi"] == case["ansi"]
            && facet["batch"] == batch
        {
            if facet["owner"] == 1004 {
                continue;
            }
            assert_eq!(facet["owner"], 865);
            assert_eq!(facet["sql"], case["sql"]);
            error_matches |= actual["error"] == facet["error"]
                && actual["native_phase"] == facet["native_phase"]
                && actual["optimized_error"] == facet["optimized_error"];
        }
    }
    if !error_matches {
        return Ok(false);
    }
    if case["schema_stages"].get("optimized").is_some() {
        Ok(actual["optimized_observation"]["names_match"] == true
            && actual["optimized_observation"]["schema"] == actual["optimized_schema"])
    } else {
        Ok(actual["native_phase"] == "create_physical_plan"
            && actual["optimized_error"] == actual["error"])
    }
}

async fn replay(records: &[Value], fixture: &str) -> TestResult {
    let facets = retained_facets()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["timezone"], "UTC");
    let mut identities = std::collections::BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
        if case["expected"].get("rows").is_some() {
            assert_eq!(case["public_schema"], case["expected"]["schema"]);
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"].get(stage).is_some(), "{case}");
            }
            assert!(case["rows_raw"].is_array(), "{case}");
        } else {
            assert!(case["expected"]["condition"].is_string(), "{case}");
            assert!(case["spark_error_phase"].is_string(), "{case}");
        }
    }
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        // One input partition keeps competing provider failures deterministic.
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &records[0],
            batch_size,
            1,
        )?;
        for case in &records[1..] {
            let actual =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await?;
            let cause_matches = if case["expected"]["condition"] == "CAST_INVALID_INPUT" {
                spark_results::matches_spark_int32_cast_input(case, &actual)
            } else if let Some(condition) = case["expected"]["condition"].as_str() {
                let prefix = format!("[{condition}] ");
                let (_, cause) = case["spark_error"]
                    .as_str()
                    .ok_or("missing independent cause")?
                    .split_once(&prefix)
                    .ok_or("missing independent condition body")?;
                let (body, _) = cause
                    .split_once(" SQLSTATE:")
                    .ok_or("missing independent SQLSTATE boundary")?;
                actual["error"]
                    .as_str()
                    .is_some_and(|error| error.ends_with(&format!("{prefix}{body}")))
            } else {
                true
            };
            let raw_rows_match = match case.get("rows_raw") {
                Some(expected) => {
                    let mut expected = expected.as_array().ok_or("missing raw rows")?.clone();
                    expected.sort_by_key(Value::to_string);
                    actual["rows"] == json!(expected)
                }
                None => true,
            };
            let scoped_matches = scoped_contract_matches(
                case,
                &actual,
                batch_size,
                fixture,
                &facets,
                cause_matches,
                raw_rows_match,
            )?;
            eprintln!(
                "ARRAY_INDEX_TIMING_OBSERVATION {}",
                json!({"id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "observation":actual,"cause_matches":cause_matches,
                    "raw_rows_match":raw_rows_match,"owned_contract_and_retained_facets_match":scoped_matches})
            );
            if !scoped_matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: cause={cause_matches} raw_rows={raw_rows_match} {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn original_array_bounds_errors_and_agreeing_controls_keep_exact_stages() -> TestResult {
    let records = records(include_str!(
        "fixtures/array-index-error-timing/original-spark.jsonl"
    ))?;
    let provenance: Value = serde_json::from_str(include_str!(
        "fixtures/array-index-error-timing/original-provenance.json"
    ))?;
    assert_eq!(records.len() - 1, 16);
    assert_eq!(provenance["selected_observations"], 16);
    assert_eq!(
        provenance["source_sha256"],
        "bca7979a35a8eca98ae3f5340c87a8c364c184bc7698388357f8958ebe922c2d"
    );
    assert_eq!(
        records[1..]
            .iter()
            .filter(|case| case["expected"]["condition"] == "INVALID_ARRAY_INDEX")
            .count(),
        3
    );
    for id in provenance["selected_ids"]
        .as_array()
        .ok_or("missing original IDs")?
    {
        for ansi in [true, false] {
            assert!(
                records[1..]
                    .iter()
                    .any(|case| case["id"] == *id && case["ansi"] == ansi)
            );
        }
    }
    replay(&records, "original-spark.jsonl").await
}

#[tokio::test]
async fn captured_array_bounds_reachability_and_first_errors_match_each_stage() -> TestResult {
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/array-index-error-timing/manifest.json"
    ))?;
    // Root captures this independent oracle before executing the unchanged runtime.
    let records = records(&std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/array-index-error-timing/supplement-spark.jsonl"
    ))?)?;
    assert_eq!(records.len() - 1, 108);
    assert_eq!(records[0]["inputs"], manifest["inputs"]);
    assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
    for definition in manifest["cases"].as_array().ok_or("missing definitions")? {
        for ansi in [true, false] {
            let case = records[1..]
                .iter()
                .find(|case| case["id"] == definition["id"] && case["ansi"] == ansi)
                .ok_or("missing captured query")?;
            for (field, expected) in definition.as_object().ok_or("invalid definition")? {
                assert_eq!(case[field], *expected, "{field}: {case}");
            }
        }
    }
    replay(&records, "supplement-spark.jsonl").await
}

#[tokio::test]
async fn independently_rechecked_conditional_boundaries_match_each_stage() -> TestResult {
    let records = records(include_str!(
        "fixtures/array-index-error-timing/conditional-boundaries-spark.jsonl"
    ))?;
    assert_eq!(records.len() - 1, 24);
    assert_eq!(records[0]["case_count"], 12);
    assert_eq!(
        records[0]["source_manifest_sha256"],
        "4e032939d6233b003e3621644df5531bf7702296e39e6f3f03784de1e829c13e"
    );
    assert_eq!(
        records[0]["raw_capture_sha256"],
        "d5ea1692b3138492bf4b5cbd1fd3d2bdac19d86398f890e9eca1440b93b9b940"
    );
    assert_eq!(records[0]["after_collect_error_fields_captured"], true);
    replay(&records, "conditional-boundaries-spark.jsonl").await
}

#[tokio::test]
async fn independently_captured_binding_boundaries_match_each_stage() -> TestResult {
    let records = records(include_str!(
        "fixtures/array-index-error-timing/binding-boundaries-spark.jsonl"
    ))?;
    assert_eq!(records.len() - 1, 8);
    assert_eq!(records[0]["case_count"], 4);
    assert_eq!(
        records[0]["source_manifest_sha256"],
        "4e032939d6233b003e3621644df5531bf7702296e39e6f3f03784de1e829c13e"
    );
    assert_eq!(records[0]["after_collect_error_fields_captured"], true);
    replay(&records, "binding-boundaries-spark.jsonl").await
}

#[tokio::test]
async fn independently_captured_opaque_conditional_arms_match_each_stage() -> TestResult {
    let records = records(include_str!(
        "fixtures/array-index-error-timing/opaque-conditional-arms-spark.jsonl"
    ))?;
    assert_eq!(records.len() - 1, 8);
    assert_eq!(records[0]["observations"], 8);
    assert_eq!(records[0]["expected_outcomes_from_spark_only"], true);
    assert_eq!(
        records[0]["source_captures"]
            .as_array()
            .ok_or("missing opaque captures")?
            .len(),
        2
    );
    assert_eq!(
        records[0]["source_captures"][0]["raw_sha256"],
        "f75c81832f7cec3d41b238961125b94637fe80aae4b6fcbdad376c1a0d0a178b"
    );
    assert_eq!(
        records[0]["source_captures"][1]["raw_sha256"],
        "b8aea75b15ada1b6cfb3183bc521c55141ce40d36daadef10aeb2adb58452100"
    );
    replay(&records, "opaque-conditional-arms-spark.jsonl").await
}

#[test]
fn ordinary_internal_and_user_raise_error_simplification_stays_deferred() -> TestResult {
    use std::sync::Arc;

    use datafusion::common::{ScalarValue, config::ConfigOptions};
    use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
    use datafusion::logical_expr::{ScalarUDFImpl, col, lit};
    use sail_function::scalar::misc::raise_error::RaiseError;
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;

    for spark in [false, true] {
        let mut options = ConfigOptions::default();
        if spark {
            options
                .extensions
                .insert(SparkSqlExpressionSemantics::default());
        }
        let context = SimplifyContext::builder()
            .with_config_options(Arc::new(options))
            .build();
        for function in [RaiseError::new(), RaiseError::new_user_error()] {
            for message in [
                lit("[INVALID_ARRAY_INDEX] user supplied text"),
                lit(ScalarValue::LargeUtf8(Some("internal cause".into()))),
                lit(ScalarValue::Utf8View(Some("view cause".into()))),
                lit(ScalarValue::Utf8(None)),
                col("message"),
            ] {
                let args = vec![message];
                match function.simplify(args.clone(), &context)? {
                    ExprSimplifyResult::Original(actual) => assert_eq!(actual, args),
                    other => return Err(format!("generic error policy changed: {other:?}").into()),
                }
            }
        }
    }
    Ok(())
}

#[test]
fn typed_array_error_provenance_survives_wrappers_without_admitting_message_forgery() {
    use arrow::error::ArrowError;
    use datafusion::common::DataFusionError;
    use datafusion::logical_expr::simplify::SparkArrayIndexError;
    use std::sync::Arc;

    let message = "[INVALID_ARRAY_INDEX] exact typed cause";
    let typed = DataFusionError::External(Box::new(SparkArrayIndexError::new(message.into())));
    let wrapped = DataFusionError::ArrowError(
        Box::new(ArrowError::ExternalError(Box::new(
            DataFusionError::Shared(Arc::new(typed.context("array context"))),
        ))),
        None,
    );
    assert!(SparkArrayIndexError::is(&wrapped));
    assert!(wrapped.to_string().ends_with(message));
    assert!(!SparkArrayIndexError::is(&DataFusionError::Execution(
        message.into()
    )));
    assert!(!SparkArrayIndexError::is(&DataFusionError::External(
        Box::new(std::io::Error::other(message),)
    )));
}

#[test]
fn whole_expression_array_errors_keep_native_generic_and_conditional_policies() -> TestResult {
    use arrow::datatypes::DataType;
    use datafusion::common::{ScalarValue, config::ConfigOptions};
    use datafusion::logical_expr::expr_fn::when;
    use datafusion::logical_expr::simplify::{SimplifyContext, SparkArrayIndexError};
    use datafusion::logical_expr::{ScalarUDF, cast, col, lit};
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use sail_function::scalar::misc::raise_error::RaiseError;
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;
    use std::sync::Arc;

    for spark in [false, true] {
        let mut options = ConfigOptions::default();
        if spark {
            options
                .extensions
                .insert(SparkSqlExpressionSemantics::default());
        }
        let info = SimplifyContext::builder()
            .with_config_options(Arc::new(options))
            .build();
        let simplifier = ExprSimplifier::new(info);
        for message in [
            ScalarValue::Utf8(Some("[INVALID_ARRAY_INDEX] typed cause".into())),
            ScalarValue::LargeUtf8(Some("large cause".into())),
            ScalarValue::Utf8View(Some("view cause".into())),
        ] {
            let error =
                ScalarUDF::from(RaiseError::new_array_index_error()).call(vec![lit(message)]);
            match simplifier.simplify(error.clone()) {
                Err(actual) if spark => assert!(SparkArrayIndexError::is(&actual)),
                Ok(actual) if !spark => assert_eq!(actual, error),
                other => return Err(format!("wrong typed-error policy: {other:?}").into()),
            }
        }
        let typed = cast(
            ScalarUDF::from(RaiseError::new_array_index_error())
                .call(vec![lit("[INVALID_ARRAY_INDEX] forged and typed text")]),
            DataType::Int32,
        );
        let bounds = when(lit(true), typed.clone()).otherwise(lit(0_i32))?;
        for function in [RaiseError::new(), RaiseError::new_user_error()] {
            let forged = cast(
                ScalarUDF::from(function)
                    .call(vec![lit("[INVALID_ARRAY_INDEX] forged and typed text")]),
                DataType::Int32,
            );
            assert_eq!(simplifier.simplify(forged.clone())?, forged);
            let selected_forgery = when(lit(true), forged.clone()).otherwise(bounds.clone())?;
            assert_eq!(simplifier.simplify(selected_forgery)?, forged);
        }
        for message in [lit(ScalarValue::Utf8(None)), col("message")] {
            let error = ScalarUDF::from(RaiseError::new_array_index_error()).call(vec![message]);
            assert_eq!(simplifier.simplify(error.clone())?, error);
        }
        let row_condition = when(col("selected"), bounds.clone()).otherwise(lit(2_i32))?;
        let expected = when(
            col("selected"),
            if spark {
                lit(ScalarValue::Int32(None))
            } else {
                typed.clone()
            },
        )
        .otherwise(lit(2_i32))?;
        assert_eq!(simplifier.simplify(row_condition)?, expected);
        let required = when(bounds.clone().eq(lit(0_i32)), lit(1_i32)).otherwise(lit(2_i32))?;
        assert_eq!(simplifier.simplify(required).is_err(), spark);
        assert_eq!(
            simplifier.simplify(lit(ScalarValue::Int32(None)) + bounds)?,
            lit(ScalarValue::Int32(None)),
        );
    }
    Ok(())
}

#[test]
fn string_argument_hof_preserves_reached_array_errors() -> TestResult {
    use arrow::array::{BooleanArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::common::{DFSchema, ScalarValue, config::ConfigOptions, tree_node::TreeNode};
    use datafusion::functions::expr_fn::coalesce;
    use datafusion::logical_expr::execution_props::ExecutionProps;
    use datafusion::logical_expr::expr::{HigherOrderFunction, Lambda};
    use datafusion::logical_expr::expr_fn::when;
    use datafusion::logical_expr::simplify::SimplifyContext;
    use datafusion::logical_expr::{
        ColumnarValue, Expr, HigherOrderUDF, ScalarUDF, Volatility, cast, col, create_udf, lit,
    };
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion::physical_expr::create_physical_expr;
    use sail_function::scalar::misc::raise_error::RaiseError;
    use sail_function::scalar::string::spark_substring::SparkSubstring;
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let position = create_udf(
        "argument_position",
        vec![],
        DataType::Int64,
        Volatility::Volatile,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(0))))
        }),
    );
    let source = cast(
        ScalarUDF::from(RaiseError::new_array_index_error())
            .call(vec![lit("[INVALID_ARRAY_INDEX] reached source")]),
        DataType::Utf8,
    );
    let lambda = |body| Expr::Lambda(Lambda::new(vec!["unused".into()], body));
    let arguments = Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::new(HigherOrderUDF::new_from_impl(SparkSubstring::new())),
        vec![
            lambda(source.clone()),
            lambda(position.call(vec![])),
            lambda(lit(1_i64)),
        ],
    ));
    let schema = Arc::new(DFSchema::try_from(Schema::new(vec![
        Field::new("selected", DataType::Boolean, false),
        Field::new("value", DataType::Utf8, true),
    ]))?);
    let batch = RecordBatch::try_new(
        Arc::new(schema.as_arrow().clone()),
        vec![
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(StringArray::from(vec!["kept"])),
        ],
    )?;
    for spark in [false, true] {
        let mut options = ConfigOptions::default();
        if spark {
            options
                .extensions
                .insert(SparkSqlExpressionSemantics::default());
        }
        let simplifier = ExprSimplifier::new(
            SimplifyContext::builder()
                .with_schema(Arc::clone(&schema))
                .with_config_options(Arc::new(options))
                .build(),
        );
        assert_eq!(simplifier.simplify(arguments.clone())?, arguments);
        for (conditional, expected) in [
            (
                when(col("selected"), arguments.clone()).otherwise(lit("fallback"))?,
                "fallback",
            ),
            (coalesce(vec![col("value"), arguments.clone()]), "kept"),
        ] {
            let actual = simplifier.simplify(conditional)?;
            assert_eq!(actual.exists(|node| Ok(node == &source))?, !spark);
            let physical = create_physical_expr(&actual, &schema, &ExecutionProps::new())?;
            let values = physical.evaluate(&batch)?.into_array(batch.num_rows())?;
            assert_eq!(
                ScalarValue::try_from_array(values.as_ref(), 0)?,
                ScalarValue::Utf8(Some(expected.into()))
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn array_preparation_preserves_callback_boundaries_and_live_errors() -> TestResult {
    use arrow::datatypes::DataType;
    use datafusion::common::{Result, config::ConfigOptions};
    use datafusion::logical_expr::simplify::SimplifyContext;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, cast,
        lit,
    };
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion::physical_plan::collect;
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};
    use sail_function::scalar::misc::raise_error::RaiseError;
    use sail_plan::expression_errors::SparkSqlExpressionSemantics;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    static CALLS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct Producer {
        signature: Signature,
        fold: bool,
    }
    impl ScalarUDFImpl for Producer {
        fn name(&self) -> &str {
            "bounds_producer"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn supports_constant_folding(&self) -> bool {
            self.fold
        }
        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }
        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(args.args[0].clone())
        }
    }
    for (volatility, fold) in [(Volatility::Immutable, false), (Volatility::Volatile, true)] {
        let session = session(1);
        let producer = ScalarUDF::from(Producer {
            signature: Signature::exact(vec![DataType::Int32], volatility),
            fold,
        });
        session.register_udf(producer.clone());
        for function in [
            producer.clone(),
            producer.clone().with_aliases(["bounds_producer_alias"]),
        ] {
            for spark in [false, true] {
                CALLS.store(0, Ordering::SeqCst);
                let mut options = ConfigOptions::default();
                if spark {
                    options
                        .extensions
                        .insert(SparkSqlExpressionSemantics::default());
                }
                let info = SimplifyContext::builder()
                    .with_config_options(Arc::new(options))
                    .build();
                let message = cast(function.call(vec![lit(7_i32)]), DataType::Utf8);
                let error =
                    ScalarUDF::from(RaiseError::new_array_index_error()).call(vec![message]);
                assert_eq!(ExprSimplifier::new(info).simplify(error.clone())?, error);
                assert_eq!(CALLS.load(Ordering::SeqCst), 0);
            }
        }
        for (sql, live) in [
            ("SELECT ARRAY(bounds_producer(7))[1] AS r", true),
            (
                "SELECT ARRAY(v)[1] AS r FROM (SELECT bounds_producer(7) AS v FROM VALUES(1) t(k)) p",
                true,
            ),
            (
                "SELECT ARRAY(v)[1] AS r FROM (SELECT bounds_producer(7) AS v FROM VALUES(1) t(k)) p LIMIT 0",
                false,
            ),
            (
                "SELECT ARRAY(v)[1] AS r FROM (SELECT bounds_producer(7) AS v FROM VALUES(1) t(k)) p WHERE false",
                false,
            ),
            (
                "SELECT CASE WHEN false THEN ARRAY(bounds_producer(7))[1] ELSE 2 END AS r",
                false,
            ),
        ] {
            CALLS.store(0, Ordering::SeqCst);
            let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
            assert_eq!(
                CALLS.load(Ordering::SeqCst),
                0,
                "{sql}, {volatility:?}, fold={fold}"
            );
            let physical = query.create_physical_plan(&session).await?;
            let preparation_calls = CALLS.load(Ordering::SeqCst);
            if matches!(volatility, Volatility::Volatile)
                && fold
                && sql == "SELECT ARRAY(bounds_producer(7))[1] AS r"
            {
                // #1007: native physical CASE schema probing measured 2 calls; see 971-native-case-producer-proof-current-v2.log.
                assert!(
                    preparation_calls <= 2,
                    "physical CASE probe callbacks increased: {preparation_calls}"
                );
            } else {
                assert_eq!(preparation_calls, 0, "{sql}, {volatility:?}, fold={fold}");
            }
            let result = collect(physical, session.task_ctx()).await;
            if live {
                let error = result
                    .err()
                    .ok_or("live ARRAY bounds validation was lost")?;
                assert!(
                    error.to_string().ends_with("[INVALID_ARRAY_INDEX] The index 1 is out of bounds. The array has 1 elements. Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead."),
                    "{error}"
                );
                assert!(CALLS.load(Ordering::SeqCst) > preparation_calls, "{sql}");
            } else {
                result?;
                assert_eq!(
                    CALLS.load(Ordering::SeqCst),
                    0,
                    "{sql}, {volatility:?}, fold={fold}"
                );
            }
        }
        for sql in [
            "SELECT CASE WHEN true THEN ARRAY(7)[1] ELSE bounds_producer(7) END AS r",
            "SELECT CASE WHEN true THEN CASE WHEN true THEN ARRAY(7)[1] ELSE bounds_producer(7) END ELSE bounds_producer(7) END AS r",
            "SELECT CASE WHEN CAST(true AS BOOLEAN) THEN ARRAY(7)[1] ELSE bounds_producer(7) END AS r",
            "SELECT CASE WHEN 1=1 THEN ARRAY(7)[1] ELSE bounds_producer(7) END AS r",
        ] {
            CALLS.store(0, Ordering::SeqCst);
            let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
            assert_eq!(
                CALLS.load(Ordering::SeqCst),
                0,
                "{sql}, {volatility:?}, fold={fold}"
            );
            let physical = query.create_physical_plan(&session).await?;
            assert_eq!(
                CALLS.load(Ordering::SeqCst),
                0,
                "{sql}, {volatility:?}, fold={fold}"
            );
            // Independent Spark captures return NULL even with literal guards and dead RAND arms.
            let batches = collect(physical, session.task_ctx()).await?;
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                1,
                "{sql}"
            );
            assert!(
                batches
                    .iter()
                    .all(|batch| batch.column(0).null_count() == batch.num_rows()),
                "{sql}"
            );
            assert_eq!(
                CALLS.load(Ordering::SeqCst),
                0,
                "{sql}, {volatility:?}, fold={fold}"
            );
        }
    }
    Ok(())
}

#[test]
fn retained_facets_reject_unowned_field_row_and_error_changes() -> TestResult {
    let facets = retained_facets()?;
    let references = records(include_str!(
        "fixtures/array-index-error-timing/supplement-spark.jsonl"
    ))?;
    let case = references
        .iter()
        .find(|case| case["id"] == "literal_valid" && case["ansi"] == true)
        .ok_or("missing literal control")?;
    let mut actual = json!({"public_schema":case["schema_stages"]["analyzed"]});
    actual["public_schema"]["fields"][0]["nullable"] = json!(true);
    assert!(schema_with_retained_facets_matches(
        case,
        &actual,
        1,
        "supplement-spark.jsonl",
        "analyzed",
        "public_schema",
        &facets
    )?);
    actual["public_schema"]["fields"][0]["metadata"] = json!({"unexpected":"metadata"});
    assert!(!schema_with_retained_facets_matches(
        case,
        &actual,
        1,
        "supplement-spark.jsonl",
        "analyzed",
        "public_schema",
        &facets
    )?);
    actual["public_schema"]["fields"][0]["metadata"] = json!({});
    actual["public_schema"]["fields"][0]["type"] = json!("long");
    assert!(!schema_with_retained_facets_matches(
        case,
        &actual,
        1,
        "supplement-spark.jsonl",
        "analyzed",
        "public_schema",
        &facets
    )?);
    let provider = references
        .iter()
        .find(|case| case["id"] == "provider_condition_selected" && case["ansi"] == true)
        .ok_or("missing provider control")?;
    let mut wrong = json!({"public_schema":provider["schema_stages"]["analyzed"],
        "optimized_schema":provider["schema_stages"]["optimized"],
        "before_collect":provider["schema_stages"]["executed_before_collect"],
        "physical_schema":provider["schema_stages"]["executed"],
        "optimized_observation":{"names_match":true,"schema":provider["schema_stages"]["optimized"]},
        "rows_match":true,"ordered_rows_match":true,"error_matches":true});
    assert!(scoped_contract_matches(
        provider,
        &wrong,
        1,
        "supplement-spark.jsonl",
        &facets,
        true,
        true
    )?);
    wrong["rows_match"] = json!(false);
    assert!(!scoped_contract_matches(
        provider,
        &wrong,
        1,
        "supplement-spark.jsonl",
        &facets,
        false,
        false
    )?);
    let cause_case = references
        .iter()
        .find(|case| case["id"] == "first_arithmetic_then_index" && case["ansi"] == true)
        .ok_or("missing tracked numeric cause")?;
    let cause = &facets["cause_facets"][0];
    let mut numeric = json!({"public_schema":cause_case["schema_stages"]["analyzed"],
        "native_phase":cause["native_phase"],"error":cause["error"],
        "optimized_error":cause["optimized_error"],"rows_match":true,
        "ordered_rows_match":true,"error_matches":false});
    assert!(scoped_contract_matches(
        cause_case,
        &numeric,
        1,
        "supplement-spark.jsonl",
        &facets,
        false,
        true
    )?);
    numeric["error"] = json!("a different numeric error");
    assert!(!scoped_contract_matches(
        cause_case,
        &numeric,
        1,
        "supplement-spark.jsonl",
        &facets,
        false,
        true
    )?);
    let conditional = records(include_str!(
        "fixtures/array-index-error-timing/conditional-boundaries-spark.jsonl"
    ))?;
    let metadata_case = conditional
        .iter()
        .find(|case| case["id"] == "provider_coalesce_bounds" && case["ansi"] == false)
        .ok_or("missing inherited metadata control")?;
    let mut metadata = json!({"optimized_schema":metadata_case["schema_stages"]["optimized"]});
    metadata["optimized_schema"]["fields"][0]["metadata"] = json!({});
    assert!(schema_with_retained_facets_matches(
        metadata_case,
        &metadata,
        1,
        "conditional-boundaries-spark.jsonl",
        "optimized",
        "optimized_schema",
        &facets
    )?);
    metadata["optimized_schema"]["fields"][0]["metadata"] = json!({"source":"wrong-source"});
    assert!(!schema_with_retained_facets_matches(
        metadata_case,
        &metadata,
        1,
        "conditional-boundaries-spark.jsonl",
        "optimized",
        "optimized_schema",
        &facets
    )?);
    Ok(())
}

#[test]
fn retained_union_outcome_binds_every_observable_facet() -> TestResult {
    let facets = retained_facets()?;
    let bindings = records(include_str!(
        "fixtures/array-index-error-timing/binding-boundaries-spark.jsonl"
    ))?;
    let case = bindings
        .iter()
        .find(|case| case["id"] == "union_array_binding" && case["ansi"] == true)
        .ok_or("missing independent UNION reference")?;
    let outcome = facets["cause_facets"]
        .as_array()
        .ok_or("missing retained outcomes")?
        .iter()
        .find(|facet| facet["owner"] == 1004 && facet["batch"] == 1)
        .ok_or("missing exact UNION baseline")?;
    assert!(scoped_contract_matches(
        case,
        outcome,
        1,
        "binding-boundaries-spark.jsonl",
        &facets,
        false,
        true
    )?);
    for key in [
        "public_schema",
        "optimized_schema",
        "before_collect",
        "physical_schema",
        "rows",
        "rows_raw",
        "native_phase",
        "error",
        "optimized_error",
    ] {
        let mut changed = outcome.clone();
        changed[key] = json!("an unmeasured change");
        assert!(
            !scoped_contract_matches(
                case,
                &changed,
                1,
                "binding-boundaries-spark.jsonl",
                &facets,
                false,
                false
            )?,
            "{key}"
        );
    }
    let error = case["spark_error"]
        .as_str()
        .ok_or("missing UNION bounds cause")?;
    let improvement = json!({"public_schema":case["schema_stages"]["analyzed"],
        "native_phase":"create_physical_plan","error":error,"optimized_error":error,
        "rows_match":true,"ordered_rows_match":true,"error_matches":true});
    assert!(scoped_contract_matches(
        case,
        &improvement,
        1,
        "binding-boundaries-spark.jsonl",
        &facets,
        true,
        true
    )?);
    Ok(())
}

#[tokio::test]
async fn native_array_bounds_keep_fields_values_and_stages_after_spark_planning() -> TestResult {
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};

    let metadata: Value = serde_json::from_str(include_str!(
        "fixtures/array-index-error-timing/manifest.json"
    ))?;
    for batch_size in [1, 32] {
        let native = SessionContext::new_with_config(
            SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(1),
        );
        let configured = session(batch_size);
        for session in [&native, &configured] {
            spark_inputs::register_recorded_inputs_with_partition_count(
                session, &metadata, batch_size, 1,
            )?;
        }
        for after_spark in [false, true] {
            if after_spark {
                let query = plan_query(
                    &configured,
                    "SELECT ARRAY(7)[0] AS r",
                    &SparkSqlOptions::default(),
                )
                .await?;
                query.create_physical_plan(&configured).await?;
            }
            for sql in [
                "SELECT ARRAY[7][0] AS r",
                "SELECT ARRAY[7][-1] AS r",
                "SELECT ARRAY[7][2] AS r",
                "SELECT CASE WHEN false THEN ARRAY[7][2] ELSE 2 END AS r",
                "SELECT id,a[1] AS r FROM bounds_rows ORDER BY id",
                "SELECT a[2] AS r FROM bounds_empty",
            ] {
                let expected =
                    spark_results::capture_native_query_observation(&native, sql).await?;
                let actual =
                    spark_results::capture_native_query_observation(&configured, sql).await?;
                assert_eq!(expected["error"], Value::Null, "{sql}: {expected}");
                assert_eq!(
                    actual, expected,
                    "batch={batch_size} after_spark={after_spark} {sql}"
                );
            }
        }
    }
    Ok(())
}
