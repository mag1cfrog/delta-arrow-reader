//! SLICE parent fields and bounded controls match independently captured Spark plans.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use futures_util::FutureExt;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const SCENARIO_IDS: &[&str] = &[
    "a_nn_required",
    "a_nn_nullable_start",
    "a_nn_nullable_length",
    "a_nn_nullable_both",
    "a_ne_required",
    "a_ne_nullable_start",
    "a_ne_nullable_length",
    "a_ne_nullable_both",
    "a_pn_required",
    "a_pn_nullable_start",
    "a_pn_nullable_length",
    "a_pn_nullable_both",
    "a_pe_required",
    "a_pe_nullable_start",
    "a_pe_nullable_length",
    "a_pe_nullable_both",
    "literal_required",
    "literal_null_element",
    "literal_empty",
    "literal_all_null",
    "literal_null_array",
    "literal_null_start",
    "literal_null_length",
    "nested_struct_metadata",
    "nested_array_elements",
    "provider_zero_rows",
    "zero_start_constant",
    "negative_length_constant",
    "zero_start_runtime",
    "negative_length_runtime",
    "negative_start_past_beginning",
    "null_array_bad_start",
    "bad_array_null_start",
    "bad_array_null_start_is_null",
    "constant_bad_array_null_start",
];

async fn capture_slice_observation(
    observation: impl std::future::Future<Output = TestResult<Value>>,
) -> TestResult<Value> {
    match std::panic::AssertUnwindSafe(observation)
        .catch_unwind()
        .await
    {
        Ok(observation) => observation,
        Err(payload) => {
            let panic = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| {
                    payload
                        .downcast_ref::<&str>()
                        .map(|message| (*message).to_owned())
                })
                .unwrap_or_else(|| "non-string panic payload".into());
            Ok(json!({"matches":false,"panic":panic,
                "public_schema":null,"before_collect":null,"physical_schema":null,
                "rows":null,"rows_raw":null,"native_phase":null,"error":null,
                "public_matches":false,"before_matches":false,"physical_matches":false,
                "rows_match":false,"ordered_rows_match":false,"error_matches":false}))
        }
    }
}

fn slice_matches_issue_contract(case: &Value, comparison: &Value, controls: &[Value]) -> bool {
    if !matches!(comparison["group"].as_str(),
        Some("slice-parent-878-current-v1" | "slice-parameters-880-current-v1"))
        || comparison["id"] != case["id"]
        || comparison["ansi"] != case["ansi"]
        || case["ansi"].as_bool().is_none()
        || !matches!(comparison["batch"].as_u64(), Some(1 | 32))
        || comparison.get("panic").is_some()
    {
        return false;
    }
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
    .all(|flag| comparison[flag] == true)
    {
        return true;
    }
    // shortcut: #881/#894 retain these exact inherited observations until their fixes.
    let issue = match (
        comparison["group"].as_str(), case["id"].as_str(), case["ansi"].as_bool(),
    ) {
        (
            Some("slice-parent-878-current-v1"),
            Some(
                "null_array_bad_start"
                | "bad_array_null_start"
                | "bad_array_null_start_is_null"
                | "constant_bad_array_null_start",
            ),
            Some(true),
        ) => 881,
        (
            Some("slice-parameters-880-current-v1"),
            Some("inherited_881_null_array_bad_start"
                | "inherited_881_bad_array_null_start"
                | "inherited_881_bad_array_null_start_is_null"
                | "inherited_881_constant_bad_array_null_start"),
            Some(true),
        ) => 881,
        (
            Some("slice-parameters-880-current-v1"),
            Some("literal_last_max_length"
                | "literal_min_start_zero_length"
                | "literal_negative_start_cross_end"
                | "literal_negative_start_past_end"
                | "provider_valid_boundaries"),
            Some(_),
        ) => 894,
        _ => return false,
    };
    let Some(control) = controls.iter().find(|control| {
        control["issue"] == issue && control["id"] == case["id"] && control["ansi"] == case["ansi"]
    }) else {
        return false;
    };
    let reference = if comparison["group"] == "slice-parameters-880-current-v1" {
        case.clone()
    } else {
        json!({"sql":case["sql"],"family":case["family"],
            "consumer":case["consumer"],"expected":case["expected"],
            "schema_stages":case["schema_stages"],"spark_error_phase":case["spark_error_phase"]})
    };
    spark_results::matches_inherited_query_observation(&reference, comparison, control)
}

#[tokio::test]
async fn slice_parent_fields_match_spark() -> TestResult {
    let records = include_str!("fixtures/slice-parent-fields-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(records[0]["group"], "slice-parent-878-current-v1");
    assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
    assert_eq!(records.len(), 71);
    let identities = records[1..]
        .iter()
        .map(|case| {
            Ok((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            ))
        })
        .collect::<TestResult<Vec<_>>>()?;
    let expected_identities = [true, false]
        .into_iter()
        .flat_map(|ansi| SCENARIO_IDS.iter().map(move |id| (*id, ansi)))
        .collect::<Vec<_>>();
    assert_eq!(identities, expected_identities);
    let controls = include_str!("fixtures/slice-inherited-controls.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(controls.len(), 5);
    assert_eq!(controls[0]["issues"], json!([881]));
    assert_eq!(controls[0]["batch_objects_identical_except_batch"], true);
    let mut differences = Vec::new();
    let mut completed = std::collections::BTreeSet::new();
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
            let mut comparison = capture_slice_observation(
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case),
            )
            .await?;
            comparison["group"] = records[0]["group"].clone();
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            comparison["owned_matches"] = json!(slice_matches_issue_contract(
                case,
                &comparison,
                &controls[1..]
            ));
            assert!(completed.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch_size,
            )));
            eprintln!("SLICE_PARENT_FIELDS_OBSERVATION {comparison}");
            if comparison["owned_matches"] != true {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected={} stages={}, actual={comparison}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]
                ));
            }
        }
    }
    assert_eq!(completed.len(), 140);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn slice_parameters_match_spark() -> TestResult {
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/slice-parameters/manifest.json"))?;
    let records = include_str!("fixtures/slice-parameters/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(manifest["scenario_count"], 154);
    assert_eq!(manifest["observation_count"], 308);
    assert_eq!(records.len(), 309);
    for key in ["spark_version", "spark_revision", "timezone", "group"] {
        assert_eq!(records[0][key], manifest[key], "{key}");
    }
    assert_eq!(
        records[0]["capture_sha256"],
        manifest["capture_script_sha256"]
    );
    assert_eq!(records[0]["provider_input_roundtrip_checked"], true);
    let identities = records[1..]
        .iter()
        .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
        .collect::<Vec<_>>();
    assert_eq!(json!(identities), manifest["identities"]);
    let controls = include_str!("fixtures/slice-parameters/inherited-controls.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(controls.len(), 15);
    assert_eq!(controls[0]["issues"], json!([881, 894]));
    assert_eq!(controls[0]["comparisons"], 28);
    assert_eq!(controls[0]["batch_objects_identical_except_batch"], true);
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
    }
    let mut differences = Vec::new();
    let mut completed = std::collections::BTreeSet::new();
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
            let mut comparison = capture_slice_observation(
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case),
            )
            .await?;
            comparison["group"] = records[0]["group"].clone();
            comparison["id"] = case["id"].clone();
            comparison["ansi"] = case["ansi"].clone();
            comparison["batch"] = json!(batch_size);
            assert!(completed.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch_size,
            )));
            eprintln!("SLICE_PARAMETERS_OBSERVATION {comparison}");
            if !slice_matches_issue_contract(case, &comparison, &controls[1..]) {
                differences.push(format!(
                    "{} ANSI={} batch={batch_size}: expected={} stages={}, actual={comparison}",
                    case["id"], case["ansi"], case["expected"], case["schema_stages"]
                ));
            }
        }
    }
    assert_eq!(completed.len(), 616);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[test]
fn inherited_slice_controls_reject_new_differences() -> TestResult {
    let references = [
        include_str!("fixtures/slice-parent-fields-spark.jsonl"),
        include_str!("fixtures/slice-parameters/spark-reference.jsonl"),
    ].into_iter().flat_map(str::lines)
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let controls = [
        include_str!("fixtures/slice-inherited-controls.jsonl"),
        include_str!("fixtures/slice-parameters/inherited-controls.jsonl"),
    ].into_iter().flat_map(|fixture| fixture.lines().skip(1))
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let controls = controls.as_slice();
    let mut identities = std::collections::BTreeSet::new();
    for control in controls {
        assert!(identities.insert((control["id"].to_string(), control["ansi"].to_string())));
        let case = references
            .iter()
            .find(|case| case["id"] == control["id"] && case["ansi"] == control["ansi"])
            .ok_or("missing inherited reference")?;
        for batch_size in [1, 32] {
            let mut comparison = control["observation"].clone();
            comparison["batch"] = json!(batch_size);
            assert!(slice_matches_issue_contract(case, &comparison, controls));
            for key in comparison.as_object().ok_or("missing observation")?.keys() {
                let mut wrong = comparison.clone();
                wrong[key] = json!("new difference");
                assert!(
                    !slice_matches_issue_contract(case, &wrong, controls),
                    "{key}"
                );
            }
            for (key, value) in [
                ("batch", json!(2)),
                (
                    "ansi",
                    json!(!case["ansi"].as_bool().ok_or("missing ANSI")?),
                ),
                ("panic", Value::Null),
                ("extra", json!(true)),
            ] {
                let mut wrong = comparison.clone();
                wrong[key] = value;
                assert!(
                    !slice_matches_issue_contract(case, &wrong, controls),
                    "{key}"
                );
            }
            for stage in [
                "/public_schema",
                "/before_collect",
                "/physical_schema",
                "/optimized_schema",
                "/optimized_observation/schema",
                "/optimized_observation/raw_schema",
            ] {
                for (field, value) in [
                    ("name", json!("wrong")),
                    ("type", json!("string")),
                    ("nullable", json!("wrong")),
                    ("metadata", json!({"new":"difference"})),
                ] {
                    let pointer = format!("{stage}/fields/0/{field}");
                    let mut wrong = comparison.clone();
                    if let Some(actual) = wrong.pointer_mut(&pointer) {
                        *actual = value;
                        assert!(
                            !slice_matches_issue_contract(case, &wrong, controls),
                            "{pointer}"
                        );
                    }
                }
            }
            for key in [
                "sql",
                "family",
                "consumer",
                "expected",
                "schema_stages",
                "spark_error_phase",
            ] {
                let mut wrong = case.clone();
                wrong[key] = json!("different reference");
                assert!(
                    !slice_matches_issue_contract(&wrong, &comparison, controls),
                    "{key}"
                );
            }
        }
    }
    assert_eq!(identities.len(), 18);
    assert_eq!(
        controls
            .iter()
            .filter(|control| control["issue"] == 881)
            .count(),
        8
    );
    assert_eq!(
        controls
            .iter()
            .filter(|control| control["issue"] == 894)
            .count(),
        10
    );
    Ok(())
}

#[tokio::test]
async fn slice_constant_validation_respects_extension_folding_boundaries() -> TestResult {
    use std::sync::LazyLock;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::DataType;
    use datafusion::logical_expr::{
        ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
    use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, plan_query};

    static INVOCATIONS: AtomicUsize = AtomicUsize::new(0);
    static SIMPLIFICATIONS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct SliceBoundary {
        volatile: bool,
        fail: bool,
    }

    impl ScalarUDFImpl for SliceBoundary {
        fn name(&self) -> &str {
            "slice_boundary"
        }

        fn signature(&self) -> &Signature {
            static OPAQUE: LazyLock<Signature> =
                LazyLock::new(|| Signature::any(1, Volatility::Immutable));
            static VOLATILE: LazyLock<Signature> =
                LazyLock::new(|| Signature::any(1, Volatility::Volatile));
            if self.volatile { &VOLATILE } else { &OPAQUE }
        }

        fn supports_constant_folding(&self) -> bool {
            self.volatile
        }

        fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(args[0].clone())
        }

        fn simplify(&self, args: Vec<Expr>, _: &SimplifyContext)
        -> datafusion::common::Result<ExprSimplifyResult> {
            SIMPLIFICATIONS.fetch_add(1, Ordering::SeqCst);
            Ok(ExprSimplifyResult::Original(args))
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs)
        -> datafusion::common::Result<ColumnarValue> {
            INVOCATIONS.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                datafusion::common::exec_err!("SLICE extension boundary was invoked")
            } else {
                Ok(args.args[0].clone())
            }
        }
    }

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for volatile in [false, true] {
        for fail in [false, true] {
            session.register_udf(ScalarUDF::from(SliceBoundary { volatile, fail }));
            for (sql, reached) in [
                ("SELECT SLICE(slice_boundary(ARRAY(1, 2)), 1, 1) AS r", true),
                ("SELECT SLICE(ARRAY(1, 2), slice_boundary(1), 1) AS r", true),
                ("SELECT SLICE(ARRAY(1, 2), 1, slice_boundary(1)) AS r", true),
                ("SELECT slice_boundary(SLICE(ARRAY(1, 2), 1, 1)) AS r", true),
                ("SELECT CASE WHEN false THEN SLICE(ARRAY(1, 2), slice_boundary(0), 1) ELSE ARRAY(9) END AS r", false),
                ("SELECT CASE WHEN false THEN slice_boundary(1) ELSE 7 END AS r, SLICE(ARRAY(1, 2), 1, 1) AS neighbor", false),
            ] {
                INVOCATIONS.store(0, Ordering::SeqCst);
                SIMPLIFICATIONS.store(0, Ordering::SeqCst);
                let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
                let planning_calls = SIMPLIFICATIONS.load(Ordering::SeqCst);
                assert_eq!(INVOCATIONS.load(Ordering::SeqCst), 0, "planning: {sql}");
                let physical = query.create_physical_plan(&session).await?;
                let physical_calls = SIMPLIFICATIONS.load(Ordering::SeqCst);
                assert_eq!(INVOCATIONS.load(Ordering::SeqCst), 0, "physical: {sql}");
                let result = datafusion::physical_plan::collect(physical, session.task_ctx()).await;
                assert_eq!(result.is_err(), reached && fail, "{sql}: {result:?}");
                assert_eq!(INVOCATIONS.load(Ordering::SeqCst) > 0, reached, "{sql}");
                eprintln!("SLICE_FOLDING_BOUNDARY_OBSERVATION {}", json!({
                    "sql":sql,"volatile":volatile,"fail":fail,
                    "planning_simplifications":planning_calls,
                    "physical_simplifications":physical_calls,
                    "final_simplifications":SIMPLIFICATIONS.load(Ordering::SeqCst),
                    "invocations":INVOCATIONS.load(Ordering::SeqCst),
                    "failed":result.is_err(),
                }));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn spark_configuration_preserves_successful_native_array_slice() -> TestResult {
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for sql in [
        "SELECT ARRAY_SLICE([1, 2, 3, 4], 2, 3) AS r",
        "SELECT ARRAY_SLICE([1, NULL, 3], 1, 2) AS r",
        "SELECT ARRAY_SLICE([1, 2, 3], -2, -1) AS r",
        "SELECT ARRAY_SLICE([], 1, 2) AS r",
        "SELECT ARRAY_SLICE(CAST(NULL AS INT[]), 1, 2) AS r",
        "SELECT ARRAY_SLICE([1, 2], CAST(NULL AS BIGINT), 2) AS r",
    ] {
        let expected = spark_results::capture_native_query_observation(&native, sql).await?;
        let actual = spark_results::capture_native_query_observation(&configured, sql).await?;
        let observation = json!({"sql":sql,"native":expected,"configured":actual});
        eprintln!("SLICE_NATIVE_OBSERVATION {observation}");
        assert!(observation["native"]["error"].is_null(), "{observation}");
        assert_eq!(observation["native"]["phase"], "encode", "{observation}");
        for stage in ["public_schema", "before_collect", "physical_schema"] {
            assert!(observation["native"][stage].is_object(), "{observation}");
        }
        assert!(observation["native"]["rows"].is_array(), "{observation}");
        assert_eq!(
            observation["configured"], observation["native"],
            "{observation}"
        );
    }
    Ok(())
}
