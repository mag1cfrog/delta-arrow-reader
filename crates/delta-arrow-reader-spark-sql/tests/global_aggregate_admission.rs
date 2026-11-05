//! Aggregate admission must reject volatile arguments before any registered callback runs.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::{
    TestResult, capture_schema, compare_exact_spark_query_stages_with_optimized, encode_value,
    validate_batch,
};

const ARGUMENT_CONDITION: &str = "AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION";

fn fresh_spark_references() -> TestResult<Vec<Value>> {
    let fixture = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/global-aggregate-admission-spark.jsonl"
    ))?;
    let references = fixture
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/global-aggregate-admission-cases.json"
    ))?;
    assert_eq!(references[0]["spark_version"], manifest["spark_version"]);
    assert_eq!(references[0]["spark_revision"], manifest["spark_revision"]);
    assert_eq!(references[0]["input_version"], 2);
    assert_eq!(manifest["input_version"], 2);
    assert_eq!(
        references[0]["manifest_sha256"],
        "12a4be1302fe3422cd23884e9611141e2f80bb4b10203ce142996172af23e734"
    );
    assert!(
        references[0]["spark_build"]
            .as_str()
            .ok_or("missing Spark build")?
            .contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
    );
    let cases = manifest["cases"].as_array().ok_or("missing cases")?;
    assert_eq!(cases.len(), 249);
    assert_eq!(references.len() - 1, cases.len() * 2);
    for (mode, ansi) in [true, false].into_iter().enumerate() {
        for (index, case) in cases.iter().enumerate() {
            let reference = &references[1 + mode * cases.len() + index];
            assert_eq!(reference["ansi"], ansi);
            for field in ["id", "scope", "sql", "udf_sql", "success_assertions"] {
                assert_eq!(
                    reference[field], case[field],
                    "reference input mismatch: {field}"
                );
            }
            assert!(
                reference.get("expected").is_some(),
                "capture failed: {reference}"
            );
            if reference["scope"] == "required_argument_rejection" {
                for observed in [reference, &reference["udf_reference"]] {
                    assert_eq!(observed["expected"]["condition"], ARGUMENT_CONDITION);
                    assert_eq!(observed["phase"], "planning");
                    assert_eq!(observed["failure_stage"], "spark.sql");
                }
            }
            assert_eq!(
                reference["schema_stages"]["public"],
                reference["schema_stages"]["analyzed"]
            );
        }
    }
    Ok(references)
}

fn reference_for_stage_observer(case: &Value) -> TestResult<Value> {
    let mut reference = case.clone();
    if let Some(stage) = case["failure_stage"].as_str() {
        reference["spark_error_phase"] = json!(match stage {
            "spark.sql" => "sql",
            "collect" => "collect",
            other => return Err(format!("unclassified Spark failure stage {other}: {case}").into()),
        });
    }
    if let Some(schema) = case["schema_stages"].get("executed") {
        // The shared observer names the before/after-collection schemas separately.
        reference["schema_stages"]["executed_before_collect"] = schema.clone();
        reference["schema_stages"]["executed"] = case["schema_stages"]
            .get("executed_after_collect")
            .unwrap_or(schema)
            .clone();
    }
    Ok(reference)
}

fn has_random_result(case: &Value) -> bool {
    matches!(
        case["id"].as_str(),
        Some(
            "window_rows_sum"
                | "window_rows_sum_nested"
                | "window_nulls_sum"
                | "window_rows_collect_list"
                | "window_rows_collect_list_nested"
                | "window_rows_collect_set"
                | "window_rows_collect_set_nested"
                | "window_nulls_collect_list"
                | "window_nulls_collect_list_nested"
                | "window_nulls_collect_set"
                | "window_nulls_collect_set_nested"
                | "window_count_if_volatile"
        )
    )
}

fn aggregate_staging_baseline_facets() -> TestResult<Vec<Value>> {
    let baseline: Value = serde_json::from_str(include_str!(
        "fixtures/global-aggregate-admission-staging-baseline.json"
    ))?;
    assert_eq!(
        baseline["runtime_source"],
        "b0b98f64981a5fde769e92ba87a3cf9257dd6c24"
    );
    assert_eq!(
        baseline["test_source"],
        "c3a8f4f8db9b9239f964ab11138ce6c2fcb6b420"
    );
    assert_eq!(
        baseline["source_observations_sha256"],
        "9ec022ca12b0abad966b1e1209be34b4f6211b2471154f02a8a94702f79bc15f"
    );
    assert_eq!(baseline["is_spark_compatibility_credit"], false);
    let facets = baseline["facets"]
        .as_array()
        .ok_or("missing staging facets")?
        .clone();
    assert_eq!(facets.len(), 56);
    let mut identities = std::collections::BTreeSet::new();
    for facet in &facets {
        let id = facet["id"].as_str().ok_or("missing facet ID")?;
        let ansi = facet["ansi"].as_bool().ok_or("missing facet ANSI")?;
        let batch = facet["batch"].as_u64().ok_or("missing facet batch")?;
        assert!(matches!(batch, 1 | 32));
        assert!(identities.insert((id, ansi, batch)));
        let schema_stages = matches!(
            id,
            "window_rows_collect_list_nested"
                | "window_rows_collect_set_nested"
                | "window_nulls_collect_list_nested"
                | "window_nulls_collect_set_nested"
                | "window_empty_collect_list_nested"
                | "window_empty_collect_set_nested"
        );
        assert!(
            schema_stages
                || matches!(
                    id,
                    "window_rows_count_false_filter"
                        | "window_rows_sum_false_filter"
                        | "window_rows_collect_list_false_filter"
                        | "window_nulls_count_false_filter"
                        | "window_nulls_sum_false_filter"
                        | "window_nulls_collect_list_false_filter"
                        | "window_count_if_volatile_false_filter"
                        | "retained_grouped_volatile_exists"
                )
        );
        assert_eq!(
            facet["kind"],
            if schema_stages {
                "schema_stages"
            } else {
                "observation"
            }
        );
        assert_eq!(
            facet["owners"],
            if schema_stages {
                json!([142, 156])
            } else {
                json!([142])
            }
        );
        assert_eq!(facet["source_strict_spark_matches"], false);
    }
    Ok(facets)
}

fn matches_aggregate_staging_baseline(
    case: &Value,
    actual: &Value,
    batch_size: usize,
    facets: &[Value],
) -> TestResult<Option<bool>> {
    let Some(facet) = facets.iter().find(|facet| {
        facet["id"] == case["id"] && facet["ansi"] == case["ansi"] && facet["batch"] == batch_size
    }) else {
        return Ok(None);
    };
    assert_eq!(facet["reference"], *case);
    let matches = match facet["kind"].as_str() {
        Some("observation") => facet["observation"] == *actual,
        Some("schema_stages") => {
            let fields_preserved = [
                "public_schema",
                "before_collect",
                "physical_schema",
                "optimized_schema",
            ]
            .into_iter()
            .all(|key| actual[key] == facet["observation"][key]);
            let rows_match = if has_random_result(case) {
                actual["rows"].as_array().map(Vec::len)
                    == case["expected"]["rows"].as_array().map(Vec::len)
            } else {
                actual["rows_match"] == true
            };
            fields_preserved
                && rows_match
                && actual["error_matches"] == true
                && actual["error"].is_null()
                && actual["optimized_error"].is_null()
                && actual["optimized_observation"]["names_match"] == true
        }
        other => return Err(format!("unexpected staging facet kind: {other:?}").into()),
    };
    Ok(Some(matches))
}

#[test]
fn independent_reference_outcomes_are_fully_classified() -> TestResult {
    let references = fresh_spark_references()?;
    let mut exact_successes = 0;
    let mut random_successes = 0;
    let mut planning_rejections = 0;
    let mut internal_execution_failures = 0;
    for case in &references[1..] {
        match case["phase"].as_str() {
            Some("success") => {
                assert_eq!(case["expected"]["schema"], case["schema_stages"]["public"]);
                for stage in [
                    "analyzed",
                    "optimized",
                    "executed",
                    "executed_after_collect",
                ] {
                    assert!(case["schema_stages"].get(stage).is_some());
                }
                assert!(case["raw_rows"].is_array());
                if has_random_result(case) {
                    assert_eq!(
                        case["success_assertions"],
                        "admission_fields_and_cardinality"
                    );
                    random_successes += 1;
                } else {
                    exact_successes += 1;
                }
            }
            Some("planning") => {
                assert_eq!(case["failure_stage"], "spark.sql");
                assert!(matches!(
                    case["expected"]["condition"].as_str(),
                    Some(
                        ARGUMENT_CONDITION
                            | "DISTINCT_WINDOW_FUNCTION_UNSUPPORTED"
                            | "INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC"
                            | "NESTED_AGGREGATE_FUNCTION"
                            | "_LEGACY_ERROR_TEMP_1031"
                            | "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
                    )
                ));
                planning_rejections += 1;
            }
            Some("execution") => {
                assert_eq!(case["scope"], "window_contract");
                assert_eq!(case["expected"]["condition"], "INTERNAL_ERROR");
                assert_eq!(case["failure_stage"], "collect");
                let cause = case["error"]["causes"]
                    .as_array()
                    .ok_or("missing Spark causes")?
                    .last()
                    .ok_or("missing deepest Spark cause")?;
                assert_eq!(cause["type"], "org.apache.spark.SparkException");
                assert_eq!(cause["condition"], "INTERNAL_ERROR");
                assert!(matches!(
                    cause["message"].as_str(),
                    Some(
                        "[INTERNAL_ERROR] Unsupported window function: 0 SQLSTATE: XX000"
                            | "[INTERNAL_ERROR] Unsupported window function: null SQLSTATE: XX000"
                            | "[INTERNAL_ERROR] Unsupported window function: [] SQLSTATE: XX000"
                    )
                ));
                internal_execution_failures += 1;
            }
            other => return Err(format!("unclassified Spark outcome {other:?}: {case}").into()),
        }
    }
    assert_eq!(
        (
            exact_successes,
            random_successes,
            planning_rejections,
            internal_execution_failures
        ),
        (122, 24, 338, 14)
    );
    Ok(())
}

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
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
    )
}

#[tokio::test]
async fn original_global_aggregate_arguments_match_independent_spark_rejections() -> TestResult {
    let references = include_str!("fixtures/global-aggregate-admission-original-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(references.len(), 4);
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        for case in &references {
            assert_eq!(case["expected"]["condition"], ARGUMENT_CONDITION);
            let planned = plan_query(
                &session,
                case["sql"].as_str().ok_or("missing original SQL")?,
                &SparkSqlOptions {
                    ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI mode")?,
                    ..Default::default()
                },
            )
            .await;
            if !planned
                .as_ref()
                .is_err_and(|error| error.to_string().contains(ARGUMENT_CONDITION))
            {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {ARGUMENT_CONDITION} during resolution, got {planned:?}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn rejected_aggregate_arguments_never_invoke_registered_volatile_functions() -> TestResult {
    let references = fresh_spark_references()?;
    let cases = &references[1..];
    assert_eq!(
        cases
            .iter()
            .filter(|case| case.get("udf_sql").is_some())
            .count(),
        282
    );
    let mut differences = Vec::new();
    for batch_size in [1, 32] {
        let session = session(batch_size);
        let callbacks = Arc::new(AtomicUsize::new(0));
        for (name, data_type, value) in [
            (
                "observed_arg",
                DataType::Float64,
                ScalarValue::Float64(Some(0.5)),
            ),
            ("observed_null", DataType::Null, ScalarValue::Null),
        ] {
            let observed = Arc::clone(&callbacks);
            session.register_udf(create_udf(
                name,
                vec![DataType::Int32],
                data_type,
                Volatility::Volatile,
                Arc::new(move |_| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok(ColumnarValue::Scalar(value.clone()))
                }),
            ));
        }
        for ansi in [true, false] {
            for case in cases
                .iter()
                .filter(|case| case["ansi"] == ansi && case.get("udf_sql").is_some())
            {
                assert_eq!(
                    case["udf_reference"]["expected"]["condition"], ARGUMENT_CONDITION,
                    "resolve the independent UDF admission before implementing: {case}"
                );
                assert_eq!(case["udf_reference"]["phase"], "planning");
                assert_eq!(case["udf_reference"]["failure_stage"], "spark.sql");
                callbacks.store(0, Ordering::SeqCst);
                let sql = case["udf_sql"].as_str().ok_or("missing UDF SQL")?;
                let planned = plan_query(
                    &session,
                    sql,
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..Default::default()
                    },
                )
                .await;
                let rejected = planned.as_ref().is_err_and(|error| {
                    let message = error.to_string();
                    message.contains(ARGUMENT_CONDITION) && message.contains("observed_")
                });
                let resolved_callbacks = callbacks.load(Ordering::SeqCst);
                // Execute an incorrectly admitted query so baseline evidence includes real callbacks.
                let mut phase = "resolve";
                let mut schemas = json!({});
                let outcome: TestResult<Value> = async {
                    let query = planned?;
                    phase = "public_schema";
                    if query.column_names.len() != query.logical_plan.schema().fields().len() {
                        return Err("public field count differs from output names".into());
                    }
                    schemas["public"] = capture_schema(
                        query.logical_plan.schema().fields(),
                        Some(&query.column_names),
                    )?;
                    phase = "create_physical_plan";
                    let physical = query.create_physical_plan(&session).await?;
                    let schema = physical.schema();
                    schemas["executed_before_collect"] = capture_schema(schema.fields(), None)?;
                    phase = "collect";
                    let batches = collect(physical.clone(), session.task_ctx()).await;
                    schemas["executed_after_collect"] =
                        capture_schema(physical.schema().fields(), None)?;
                    let batches = batches?;
                    phase = "encode";
                    let mut rows = Vec::new();
                    for batch in batches {
                        validate_batch(schema.as_ref(), &batch)?;
                        for row in 0..batch.num_rows() {
                            rows.push(
                                batch
                                    .columns()
                                    .iter()
                                    .map(|column| encode_value(column.as_ref(), row))
                                    .collect::<TestResult<Vec<_>>>()?,
                            );
                        }
                    }
                    phase = "success";
                    Ok(json!(rows))
                }
                .await;
                let total_callbacks = callbacks.load(Ordering::SeqCst);
                let observation = json!({"id":case["id"], "ansi":ansi, "batch":batch_size,
                    "sql":sql, "phase":phase, "schema_stages":schemas,
                    "rows_raw":outcome.as_ref().ok(),
                    "error":outcome.as_ref().err().map(ToString::to_string),
                    "resolved_callbacks":resolved_callbacks, "total_callbacks":total_callbacks});
                eprintln!("global_aggregate_udf_observation={observation}");
                if !rejected || resolved_callbacks != 0 || total_callbacks != 0 {
                    differences.push(format!(
                        "{} ANSI={ansi} batch={batch_size}: expected {ARGUMENT_CONDITION} during resolution and zero callbacks; got {observation}",
                        case["id"]
                    ));
                }
            }
            for (sql, rows) in [
                (
                    "SELECT observed_arg(v) AS result FROM VALUES(1),(2) t(v)",
                    2,
                ),
                (
                    "SELECT COUNT(*) AS n,observed_arg(1) AS result FROM VALUES(1),(2) t(v)",
                    1,
                ),
            ] {
                callbacks.store(0, Ordering::SeqCst);
                let control = plan_query(
                    &session,
                    sql,
                    &SparkSqlOptions {
                        ansi_mode: ansi,
                        ..Default::default()
                    },
                )
                .await?;
                let physical = control.create_physical_plan(&session).await?;
                assert_eq!(callbacks.load(Ordering::SeqCst), 0);
                let batches = collect(physical, session.task_ctx()).await?;
                assert_eq!(
                    batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    rows
                );
                assert!(
                    callbacks.load(Ordering::SeqCst) > 0,
                    "scalar callback control did not run"
                );
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn aggregate_admission_matches_spark_and_preserves_tracked_staging_facets() -> TestResult {
    let references = fresh_spark_references()?;
    assert_eq!(references.len() - 1, 498);
    let mut differences = Vec::new();
    let mut strict_differences = Vec::new();
    let facets = aggregate_staging_baseline_facets()?;
    let mut tracked = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        for case in &references[1..] {
            let actual = compare_exact_spark_query_stages_with_optimized(
                &session,
                &reference_for_stage_observer(case)?,
            )
            .await?;
            eprintln!(
                "global_aggregate_admission_observation={}",
                json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "scope":case["scope"], "observation":actual})
            );
            let matches = if case["expected"]["condition"] == "INTERNAL_ERROR" {
                // Spark's folded-window execution defect needs a separate support decision.
                strict_differences.push(format!(
                    "{} ANSI={} batch={batch_size}: unresolved Spark execution failure at collect; cause={}; this is not a required native rejection; actual={actual}",
                    case["id"], case["ansi"], case["error"]["causes"]
                        .as_array().and_then(|causes| causes.last()).unwrap_or(&Value::Null)
                ));
                false
            } else if let Some(condition) = case["expected"]["condition"].as_str() {
                let cause = match condition {
                    ARGUMENT_CONDITION if case["id"] == "count_if_argument" => {
                        "COUNT_IF argument requires a deterministic expression"
                    }
                    ARGUMENT_CONDITION => ARGUMENT_CONDITION,
                    "DISTINCT_WINDOW_FUNCTION_UNSUPPORTED" => {
                        "[DISTINCT_WINDOW_FUNCTION_UNSUPPORTED]"
                    }
                    "INVALID_AGGREGATE_FILTER.NON_DETERMINISTIC" => {
                        "COUNT_IF FILTER requires a deterministic expression"
                    }
                    "NESTED_AGGREGATE_FUNCTION" if case["id"] == "count_if_nested" => {
                        "COUNT_IF argument cannot contain an aggregate function"
                    }
                    "NESTED_AGGREGATE_FUNCTION" => "[NESTED_AGGREGATE_FUNCTION]",
                    "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"
                        if case["scope"] == "category_precedence" =>
                    {
                        "No function matches the given name and argument types 'sum(List"
                    }
                    "_LEGACY_ERROR_TEMP_1031" => {
                        "COUNT_IF argument cannot contain a window function"
                    }
                    other => {
                        return Err(format!("unclassified Spark condition {other}: {case}").into());
                    }
                };
                actual["native_phase"] == "resolve"
                    && actual["error"]
                        .as_str()
                        .is_some_and(|error| error.contains(cause))
            } else {
                actual["error_matches"] == true
                    && actual["public_matches"] == true
                    && actual["before_matches"] == true
                    && actual["physical_matches"] == true
                    && actual["optimized_matches"] == true
                    && if has_random_result(case) {
                        actual["rows"].as_array().map(Vec::len)
                            == case["expected"]["rows"].as_array().map(Vec::len)
                    } else {
                        actual["rows_match"] == true
                    }
            };
            if !matches && case["expected"]["condition"] != "INTERNAL_ERROR" {
                strict_differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected {} at phase {}, got {actual}",
                    case["id"], case["ansi"], case["expected"], case["phase"]
                ));
            }
            let baseline_preserved =
                matches_aggregate_staging_baseline(case, &actual, batch_size, &facets)?;
            tracked += usize::from(baseline_preserved.is_some());
            let required_contract_matches = matches || baseline_preserved == Some(true);
            eprintln!(
                "global_aggregate_contract_observation={}",
                json!({
                "id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                "strict_spark_matches":matches,"baseline_preserved":baseline_preserved,
                "required_contract_matches":required_contract_matches,
                "baseline_is_spark_compatibility_credit":false})
            );
            if !required_contract_matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: required contract changed: {actual}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    eprintln!(
        "global_aggregate_strict_differences={}",
        json!(strict_differences)
    );
    assert_eq!(tracked, 52);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn retained_grouped_volatile_exists_matches_spark_or_exact_staging_observation() -> TestResult
{
    let main = fresh_spark_references()?;
    let retained = include_str!("fixtures/global-aggregate-admission-spark.retained.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/global-aggregate-admission-cases.json"
    ))?;
    assert_eq!(retained.len(), 3);
    assert_eq!(retained[0], main[0]);
    let mut differences = Vec::new();
    let facets = aggregate_staging_baseline_facets()?;
    let mut tracked = 0;
    for batch_size in [1, 32] {
        let session = session(batch_size);
        for (case, ansi) in retained[1..].iter().zip([true, false]) {
            for key in ["id", "scope", "sql"] {
                assert_eq!(case[key], manifest["retained_cases"][0][key]);
            }
            assert_eq!(case["ansi"], ansi);
            assert_eq!(case["phase"], "success");
            assert_eq!(case["expected"]["rows"], json!([["true"]]));
            let actual = compare_exact_spark_query_stages_with_optimized(
                &session,
                &reference_for_stage_observer(case)?,
            )
            .await?;
            eprintln!(
                "global_aggregate_retained_142_observation={}",
                json!({
                "id":case["id"], "ansi":ansi, "batch":batch_size, "observation":actual})
            );
            let strict_spark_matches =
                actual["matches"] == true && actual["optimized_matches"] == true;
            let baseline_preserved =
                matches_aggregate_staging_baseline(case, &actual, batch_size, &facets)?;
            tracked += usize::from(baseline_preserved.is_some());
            eprintln!(
                "global_aggregate_retained_contract_observation={}",
                json!({
                "id":case["id"],"ansi":ansi,"batch":batch_size,
                "strict_spark_matches":strict_spark_matches,"baseline_preserved":baseline_preserved,
                "baseline_is_spark_compatibility_credit":false})
            );
            if !strict_spark_matches {
                eprintln!(
                    "retained #142 {} ANSI={ansi} batch={batch_size}: expected {}, got {actual}",
                    case["id"], case["expected"]
                );
            }
            if !strict_spark_matches && baseline_preserved != Some(true) {
                differences.push(format!(
                    "retained #142 {} ANSI={ansi} batch={batch_size}: expected {}, got {actual}",
                    case["id"], case["expected"]
                ));
            }
        }
    }
    assert_eq!(tracked, 4);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn aggregate_argument_checks_preserve_native_sql() -> TestResult {
    for batch_size in [1, 32] {
        let native =
            SessionContext::new_with_config(SessionConfig::new().with_batch_size(batch_size));
        let configured = session(batch_size);
        for session in [&native, &configured] {
            session.register_udf(create_udf(
                "native_observed_arg",
                vec![DataType::Int64],
                DataType::Float64,
                Volatility::Volatile,
                Arc::new(|_| Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(0.5))))),
            ));
        }
        for expression in [
            "COUNT(native_observed_arg(v))",
            "COUNT(DISTINCT native_observed_arg(v))",
            "SUM(native_observed_arg(v))",
            "ARRAY_AGG(native_observed_arg(v))",
            "SUM(native_observed_arg(v)) OVER ()",
        ] {
            let sql = format!("SELECT {expression} AS result FROM (VALUES(1),(2)) t(v)");
            let expected = native.sql(&sql).await?.collect().await?;
            let actual = configured.sql(&sql).await?.collect().await?;
            assert_eq!(actual, expected, "{sql}");
        }
    }
    Ok(())
}
