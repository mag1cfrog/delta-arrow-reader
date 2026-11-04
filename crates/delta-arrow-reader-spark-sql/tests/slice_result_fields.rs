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

const SLICE_COMPARISON_FLAGS: &[&str] = &[
    "matches",
    "optimized_matches",
    "public_matches",
    "before_matches",
    "physical_matches",
    "rows_match",
    "ordered_rows_match",
    "error_matches",
];

fn slice_parameters_match_issue_contract(
    case: &Value,
    comparison: &Value,
    controls: &[Value],
) -> bool {
    if comparison["group"] != "slice-parameters-880-current-v1"
        || comparison["id"] != case["id"]
        || comparison["ansi"] != case["ansi"]
        || case["ansi"].as_bool().is_none()
        || !matches!(comparison["batch"].as_u64(), Some(1 | 32))
        || comparison.get("panic").is_some()
    {
        return false;
    }
    if SLICE_COMPARISON_FLAGS
        .iter()
        .all(|flag| comparison[flag] == true)
    {
        return true;
    }
    // shortcut: #894 retains these exact signed-coordinate observations until its fix.
    if !matches!(
        case["id"].as_str(),
        Some(
            "literal_last_max_length"
                | "literal_min_start_zero_length"
                | "literal_negative_start_cross_end"
                | "literal_negative_start_past_end"
                | "provider_valid_boundaries"
        )
    ) {
        return false;
    }
    let Some(control) = controls.iter().find(|control| {
        control["issue"] == 894 && control["id"] == case["id"] && control["ansi"] == case["ansi"]
    }) else {
        return false;
    };
    spark_results::matches_inherited_query_observation(case, comparison, control)
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
            comparison["owned_matches"] = json!(
                SLICE_COMPARISON_FLAGS
                    .iter()
                    .all(|flag| comparison[flag] == true)
            );
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
    assert_eq!(controls.len(), 11);
    assert_eq!(controls[0]["issues"], json!([894]));
    assert_eq!(controls[0]["comparisons"], 20);
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
            if !slice_parameters_match_issue_contract(case, &comparison, &controls[1..]) {
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
    let references = include_str!("fixtures/slice-parameters/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let controls = include_str!("fixtures/slice-parameters/inherited-controls.jsonl")
        .lines()
        .skip(1)
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
            assert!(slice_parameters_match_issue_contract(
                case,
                &comparison,
                controls
            ));
            for key in comparison.as_object().ok_or("missing observation")?.keys() {
                let mut wrong = comparison.clone();
                wrong[key] = json!("new difference");
                assert!(
                    !slice_parameters_match_issue_contract(case, &wrong, controls),
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
                    !slice_parameters_match_issue_contract(case, &wrong, controls),
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
                            !slice_parameters_match_issue_contract(case, &wrong, controls),
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
                    !slice_parameters_match_issue_contract(&wrong, &comparison, controls),
                    "{key}"
                );
            }
        }
    }
    assert_eq!(identities.len(), 10);
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
    use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
    use datafusion::logical_expr::{
        ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };
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

        fn simplify(
            &self,
            args: Vec<Expr>,
            _: &SimplifyContext,
        ) -> datafusion::common::Result<ExprSimplifyResult> {
            SIMPLIFICATIONS.fetch_add(1, Ordering::SeqCst);
            Ok(ExprSimplifyResult::Original(args))
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            INVOCATIONS.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                datafusion::common::exec_err!("SLICE extension boundary was invoked")
            } else {
                Ok(args.args[0].clone())
            }
        }
    }

    fn take_phase_observation<T, E: std::fmt::Display>(result: &Result<T, E>) -> Value {
        json!({
            "simplifications":SIMPLIFICATIONS.swap(0, Ordering::SeqCst),
            "invocations":INVOCATIONS.swap(0, Ordering::SeqCst),
            "error":result.as_ref().err().map(ToString::to_string),
        })
    }

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let mut differences = Vec::new();
    let mut completed = std::collections::BTreeSet::new();
    let mut opaque_simplifications = std::collections::BTreeMap::new();
    for volatile in [false, true] {
        for fail in [false, true] {
            session.register_udf(ScalarUDF::from(SliceBoundary { volatile, fail }));
            for ansi in [true, false] {
                for (id, sql, reached) in [
                    (
                        "array_argument",
                        "SELECT SLICE(slice_boundary(ARRAY(1, 2)), 1, 1) AS r",
                        true,
                    ),
                    (
                        "start_argument",
                        "SELECT SLICE(ARRAY(1, 2), slice_boundary(1), 1) AS r",
                        true,
                    ),
                    (
                        "length_argument",
                        "SELECT SLICE(ARRAY(1, 2), 1, slice_boundary(1)) AS r",
                        true,
                    ),
                    (
                        "parent",
                        "SELECT slice_boundary(SLICE(ARRAY(1, 2), 1, 1)) AS r",
                        true,
                    ),
                    (
                        "discarded_case",
                        "SELECT CASE WHEN false THEN SLICE(ARRAY(1, 2), slice_boundary(0), 1) ELSE ARRAY(9) END AS r",
                        false,
                    ),
                    (
                        "unrelated_neighbor",
                        "SELECT CASE WHEN false THEN slice_boundary(1) ELSE 7 END AS r, SLICE(ARRAY(1, 2), 1, 1) AS neighbor",
                        false,
                    ),
                ] {
                    INVOCATIONS.store(0, Ordering::SeqCst);
                    SIMPLIFICATIONS.store(0, Ordering::SeqCst);
                    let mut phase = "planning";
                    let mut observation = json!({"id":id,"sql":sql,"ansi":ansi,
                        "volatile":volatile,"fail":fail,"planning":null,"physical":null,
                        "collection":null,"public_schema":null,"physical_schema":null,"rows":null});
                    let result = capture_slice_observation(async {
                        let query = plan_query(
                            &session,
                            sql,
                            &SparkSqlOptions {
                                ansi_mode: ansi,
                                ..Default::default()
                            },
                        )
                        .await;
                        observation["planning"] = take_phase_observation(&query);
                        let query = query?;
                        observation["public_schema"] = spark_results::capture_schema(
                            query.logical_plan.schema().fields(),
                            Some(&query.column_names),
                        )?;
                        phase = "physical";
                        let physical = query.create_physical_plan(&session).await;
                        observation["physical"] = take_phase_observation(&physical);
                        let physical = physical?;
                        let schema = physical.schema();
                        observation["physical_schema"] =
                            spark_results::capture_schema(schema.fields(), None)?;
                        phase = "collect";
                        let batches =
                            datafusion::physical_plan::collect(physical, session.task_ctx()).await;
                        observation["collection"] = take_phase_observation(&batches);
                        let batches = batches?;
                        phase = "encode";
                        let mut rows = Vec::new();
                        for batch in batches {
                            spark_results::validate_batch(schema.as_ref(), &batch)?;
                            for row in 0..batch.num_rows() {
                                rows.push(
                                    batch
                                        .columns()
                                        .iter()
                                        .map(|column| {
                                            spark_results::encode_value(column.as_ref(), row)
                                        })
                                        .collect::<TestResult<Vec<_>>>()?,
                                );
                            }
                        }
                        observation["rows"] = json!(rows);
                        Ok(Value::Null)
                    })
                    .await;
                    observation["phase"] = json!(phase);
                    observation["error"] = json!(result.as_ref().err().map(ToString::to_string));
                    if let Ok(value) = &result
                        && let Some(panic) = value.get("panic")
                    {
                        observation["panic"] = panic.clone();
                    }
                    eprintln!("SLICE_FOLDING_BOUNDARY_OBSERVATION {observation}");
                    let expected_rows = match id {
                        "discarded_case" => json!([[["9"]]]),
                        "unrelated_neighbor" => json!([["7", ["1"]]]),
                        _ => json!([[["1"]]]),
                    };
                    let runtime_invocations = observation["collection"]["invocations"].as_u64();
                    let expected_error = reached && fail;
                    let simplifications = ["planning", "physical", "collection"]
                        .map(|phase| observation[phase]["simplifications"].clone());
                    let simplifications_match = if volatile {
                        opaque_simplifications.get(&(id, ansi, fail)) == Some(&simplifications)
                    } else {
                        opaque_simplifications.insert((id, ansi, fail), simplifications);
                        true
                    };
                    let outcome_matches = if expected_error {
                        phase == "collect"
                            && observation["error"].as_str().is_some_and(|error| {
                                error.contains("SLICE extension boundary was invoked")
                            })
                    } else {
                        phase == "encode" && result.is_ok() && observation["rows"] == expected_rows
                    };
                    if !outcome_matches
                        || !simplifications_match
                        || observation.get("panic").is_some()
                        || observation["planning"]["invocations"] != 0
                        || observation["physical"]["invocations"] != 0
                        || runtime_invocations.is_none_or(|calls| (calls > 0) != reached)
                    {
                        differences.push(observation.to_string());
                    }
                    assert!(completed.insert((id, ansi, volatile, fail)));
                }
            }
        }
    }
    assert_eq!(completed.len(), 48);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
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
