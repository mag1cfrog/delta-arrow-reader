//! Nested Spark case conversion runs after foldable expressions are evaluated.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::DataType;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use futures_util::FutureExt;
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, capture_result, register_inputs};

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
async fn case_conversion_prepares_slice_constants_before_nested_rewrite() -> TestResult {
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/case-conversion-slice-manifest.json"))?;
    let mut records = include_str!("fixtures/case-conversion-slice-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 7);
    let header = records.remove(0);
    for (key, value) in manifest.as_object().ok_or("missing manifest")? {
        assert_eq!(&header[key], value, "{key}");
    }
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(
        header["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(header["group"], "slice-894-case-conversion-interaction-v1");
    assert_eq!(header["process_timezone"], "UTC");
    assert_eq!(header["provider_input_roundtrip_checked"], true);
    assert_eq!(
        header["capture_sha256"],
        "984817a165f0a2d5abde13f1220e97727e0059eb64ebc9c592571c23bcba3107"
    );
    assert_eq!(
        header["manifest_sha256"],
        "509184b116cbe8bfbf107de420ea2ca4ca34a774fc7f9262b6b8c241ee13a8ec"
    );
    let identities = records
        .iter()
        .map(|case| json!({"id":case["id"],"ansi":case["ansi"]}))
        .collect::<Vec<_>>();
    let expected = [true, false]
        .into_iter()
        .flat_map(|ansi| {
            [
                "slice_provider_context",
                "array_provider_context",
                "slice_scalar_context",
            ]
            .into_iter()
            .map(move |id| json!({"id":id,"ansi":ansi}))
        })
        .collect::<Vec<_>>();
    assert_eq!(identities, expected);
    assert_eq!(json!(identities), manifest["identities"]);
    for case in &records {
        let definition = manifest["scenarios"]
            .as_array()
            .ok_or("missing scenarios")?
            .iter()
            .find(|definition| definition["id"] == case["id"])
            .ok_or("missing scenario")?;
        for (key, value) in definition.as_object().ok_or("missing definition")? {
            assert_eq!(&case[key], value);
        }
        assert!(case.get("spark_error").is_none());
        assert_eq!(case["timezone"], "UTC");
        assert_eq!(case["expected"]["rows"], json!([["ss"]]));
        assert_eq!(case["spark_collected_rows"], case["expected"]["rows"]);
        assert_eq!(
            case["schema_stages"].as_object().map(|stages| stages.len()),
            Some(7)
        );
    }
    let mut completed = std::collections::BTreeSet::new();
    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = session(batch);
        spark_inputs::register_recorded_inputs(&session, &header, batch)?;
        for case in &records {
            let captured = std::panic::AssertUnwindSafe(
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case),
            )
            .catch_unwind()
            .await;
            let mut observation = match captured {
                Ok(Ok(observation)) => observation,
                Ok(Err(error)) => json!({"observer_error":error.to_string()}),
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
                    json!({"panic":panic})
                }
            };
            observation["group"] = header["group"].clone();
            observation["id"] = case["id"].clone();
            observation["sql"] = case["sql"].clone();
            observation["ansi"] = case["ansi"].clone();
            observation["batch"] = json!(batch);
            let stage_matches = case["schema_stages"]
                .as_object()
                .ok_or("missing stages")?
                .iter()
                .map(|(stage, expected)| {
                    let actual = match stage.as_str() {
                        "public" | "resolved" | "analyzed" => observation.get("public_schema"),
                        "optimized" => observation.get("optimized_schema"),
                        "physical" | "executed_before_collect" => observation.get("before_collect"),
                        "executed" => observation.get("physical_schema"),
                        _ => None,
                    };
                    (stage.clone(), json!(actual == Some(expected)))
                })
                .collect::<serde_json::Map<String, Value>>();
            observation["schema_stages_match"] = json!(
                stage_matches
                    .values()
                    .all(|matched| matched.as_bool() == Some(true))
            );
            observation["schema_stage_matches"] = json!(stage_matches);
            eprintln!("SLICE_CASE_CONVERSION_INTERACTION_OBSERVATION {observation}");
            if [
                "matches",
                "optimized_matches",
                "public_matches",
                "before_matches",
                "physical_matches",
                "rows_match",
                "ordered_rows_match",
                "error_matches",
                "schema_stages_match",
            ]
            .iter()
            .any(|flag| observation[flag] != true)
            {
                differences.push(observation.to_string());
            }
            assert!(completed.insert((
                case["id"].as_str().ok_or("missing ID")?,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
                batch
            )));
        }
    }
    assert_eq!(completed.len(), 12);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn required_cast_error_matches(case: &Value, actual: &Value, controls: &[Value]) -> bool {
    let (spark_phase, native_stage) = match case["id"].as_str() {
        Some("required_invalid_cast" | "nullable_invalid_cast") => ("collect", "execute"),
        Some("literal_invalid_cast" | "empty_invalid_literal") => ("optimized_schema", "physical"),
        _ => return false,
    };
    let expected = &case["expected"];
    expected["status"] == "error"
        && expected["condition"] == "CAST_INVALID_INPUT"
        && expected["sql_state"] == "22018"
        && expected["phase"] == spark_phase
        && actual["error_stage"] == native_stage
        && actual.get("rows").is_none()
        && actual["analyzed_schema"] == expected["analyzed_schema"]
        && controls.iter().any(|control| {
            control["id"] == case["id"]
                && control["sql"] == case["sql"]
                && control["ansi"] == case["ansi"]
                && control["spark_phase"] == spark_phase
                && ["analyzed_schema", "physical_schema", "error_stage", "error"]
                    .iter()
                    .all(|field| actual.get(*field) == control["actual"].get(*field))
        })
}

fn required_cast_error_controls() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/case-conversion-required-native-errors.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 5);
    assert_eq!(records[0]["reference_count"], 4);
    Ok(records[1..].to_vec())
}

#[test]
fn required_cast_errors_reject_changed_stages_causes_fields_and_rows() -> TestResult {
    let controls = required_cast_error_controls()?;
    for case in include_str!("fixtures/case-conversion-evaluation-spark.jsonl")
        .lines()
        .skip(1)
        .map(serde_json::from_str::<Value>)
    {
        let case = case?;
        if case["expected"]["status"] != "error" {
            continue;
        }
        let control = controls
            .iter()
            .find(|c| c["id"] == case["id"])
            .ok_or("missing error control")?;
        let actual = &control["actual"];
        assert!(required_cast_error_matches(&case, actual, &controls));
        for (field, value) in [
            ("error_stage", json!("resolve")),
            (
                "error",
                json!("[CAST_INVALID_INPUT] unrelated conversion cause"),
            ),
            ("analyzed_schema", Value::Null),
            ("physical_schema", json!({"type":"struct","fields":[]})),
            ("rows", json!([])),
        ] {
            let mut changed = actual.clone();
            changed[field] = value;
            assert!(!required_cast_error_matches(&case, &changed, &controls));
        }
        let mut changed = actual.clone();
        changed
            .as_object_mut()
            .ok_or("expected observation object")?
            .remove("physical_schema");
        assert!(!required_cast_error_matches(&case, &changed, &controls));
        let mut changed_case = case.clone();
        changed_case["expected"]["phase"] = json!("analysis");
        assert!(!required_cast_error_matches(
            &changed_case,
            actual,
            &controls
        ));
    }
    Ok(())
}

#[test]
fn genuine_volatile_references_retain_independent_spark_traces() -> TestResult {
    let references = include_str!("fixtures/case-conversion-producer-genuine-volatile-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let traces =
        include_str!("fixtures/case-conversion-producer-genuine-volatile-plan-trace.jsonl")
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(references.len(), 5);
    assert_eq!(traces.len(), 5);
    assert_eq!(traces[0]["spark_version"], "4.2.0");
    for (reference, trace) in references[1..].iter().zip(&traces[1..]) {
        for field in ["id", "sql", "ansi"] {
            assert_eq!(reference[field], trace[field]);
        }
        assert_eq!(
            reference["expected"]["analyzed_schema"],
            trace["analyzed_schema"]
        );
        let projects = trace["optimized_projects"]
            .as_array()
            .ok_or("missing Spark projects")?;
        assert_eq!(projects.len(), 2);
        assert!(
            projects[1]["expressions"]
                .as_array()
                .ok_or("missing producer expressions")?
                .iter()
                .filter_map(Value::as_array)
                .flatten()
                .any(|e| e["deterministic"] == false)
        );
    }
    Ok(())
}

fn retained_case_negative_profiles() -> TestResult<Vec<Value>> {
    let records = include_str!("fixtures/case-conversion-native-dependency-profiles.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len(), 127);
    assert_eq!(records[0]["profile_count"], 126);
    assert_eq!(records[0]["owner_counts"]["#802"], 96);
    assert_eq!(records[0]["owner_counts"]["#838"], 24);
    assert_eq!(
        records[0]["owner_counts"]["unordered-limit-selection-support-boundary"],
        6
    );
    Ok(records[1..].to_vec())
}

fn retained_case_negative_matches(
    profile: &Value,
    fixture: &str,
    case: &Value,
    representation: usize,
    batch_size: usize,
    actual: &Value,
) -> bool {
    // session() fixes two partitions; capture_observation fixes UTC.
    case["expected"]["status"] == "ok"
        && profile["family"] == "792"
        && profile["fixture"] == fixture
        && profile["id"] == case["id"]
        && profile["sql"] == case["sql"]
        && profile["ansi"] == case["ansi"]
        && profile["representation"] == representation
        && profile["batch_size"] == batch_size
        && profile["native_target_partitions"] == 2
        && profile["native_session_timezone"] == "UTC"
        && profile["strict_spark_match"] == false
        && profile["full_compatibility_pass"] == false
        && actual.get("error").is_none()
        && actual.get("error_stage").is_none()
        && ["analyzed_schema", "physical_schema", "rows"]
            .iter()
            .all(|field| {
                profile["actual"]
                    .get(*field)
                    .is_some_and(|recorded| actual.get(*field) == Some(recorded))
            })
}

#[test]
fn case_negative_profiles_reject_other_keys_changed_facets_and_errors() -> TestResult {
    for profile in retained_case_negative_profiles()? {
        let fixture = profile["fixture"].as_str().ok_or("missing fixture")?;
        let case = json!({"id":profile["id"],"sql":profile["sql"],"ansi":profile["ansi"],"expected":{"status":"ok"}});
        let representation = profile["representation"]
            .as_u64()
            .ok_or("missing representation")? as usize;
        let batch = profile["batch_size"].as_u64().ok_or("missing batch")? as usize;
        let actual = &profile["actual"];
        let matches = |case: &Value, actual: &Value| {
            retained_case_negative_matches(&profile, fixture, case, representation, batch, actual)
        };
        assert!(matches(&case, actual));
        assert!(!retained_case_negative_matches(
            &profile,
            "other-fixture",
            &case,
            representation,
            batch,
            actual
        ));
        assert!(!retained_case_negative_matches(
            &profile,
            fixture,
            &case,
            (representation + 1) % 3,
            batch,
            actual
        ));
        assert!(!retained_case_negative_matches(
            &profile,
            fixture,
            &case,
            representation,
            if batch == 1 { 32 } else { 1 },
            actual
        ));
        for field in ["id", "sql", "ansi"] {
            let mut changed = case.clone();
            changed[field] = Value::Null;
            assert!(!matches(&changed, actual));
        }
        for field in ["analyzed_schema", "physical_schema", "rows"] {
            let mut changed = actual.clone();
            changed[field] = Value::Null;
            assert!(!matches(&case, &changed));
            changed
                .as_object_mut()
                .ok_or("expected observation")?
                .remove(field);
            assert!(!matches(&case, &changed));
        }
        for schema in ["analyzed_schema", "physical_schema"] {
            for (field, value) in [
                ("name", json!("changed-name")),
                ("type", json!("changed-type")),
                (
                    "nullable",
                    json!(
                        !actual[schema]["fields"][0]["nullable"]
                            .as_bool()
                            .ok_or("missing nullable")?
                    ),
                ),
                ("metadata", json!({"changed":"metadata"})),
            ] {
                let mut changed = actual.clone();
                changed[schema]["fields"][0][field] = value;
                assert!(!matches(&case, &changed));
            }
        }
        let mut changed = actual.clone();
        changed["rows"][0][0] = json!("changed-value");
        assert!(!matches(&case, &changed));
        for field in ["error", "error_stage"] {
            let mut changed = actual.clone();
            changed[field] = json!("unexpected failure");
            assert!(!matches(&case, &changed));
        }
        let mut error_case = case.clone();
        error_case["expected"]["status"] = json!("error");
        assert!(!matches(&error_case, actual));
    }
    Ok(())
}

#[tokio::test]
async fn case_conversion_results_and_dependency_profiles() -> TestResult {
    let profiles = retained_case_negative_profiles()?;
    let mut strict_successes = 0;
    let mut native_negative_matches = 0;
    let error_controls = required_cast_error_controls()?;
    let mut differences = Vec::new();
    let mut compared = 0;
    let mut errors_compared = 0;
    for (fixture, capture, count) in [
        (
            "case-conversion-spark.jsonl",
            include_str!("fixtures/case-conversion-spark.jsonl"),
            16,
        ),
        (
            "case-conversion-evaluation-spark.jsonl",
            include_str!("fixtures/case-conversion-evaluation-spark.jsonl"),
            84,
        ),
        (
            "case-conversion-order-spark.jsonl",
            include_str!("fixtures/case-conversion-order-spark.jsonl"),
            16,
        ),
        (
            "case-conversion-review-spark.jsonl",
            include_str!("fixtures/case-conversion-review-spark.jsonl"),
            20,
        ),
        (
            "case-conversion-limit-offset-spark.jsonl",
            include_str!("fixtures/case-conversion-limit-offset-spark.jsonl"),
            34,
        ),
        (
            "case-conversion-producer-outer-reference-spark.jsonl",
            include_str!("fixtures/case-conversion-producer-outer-reference-spark.jsonl"),
            8,
        ),
        (
            "case-conversion-producer-whole-project-spark.jsonl",
            include_str!("fixtures/case-conversion-producer-whole-project-spark.jsonl"),
            12,
        ),
        (
            "case-conversion-projected-cast-spark.jsonl",
            include_str!("fixtures/case-conversion-projected-cast-spark.jsonl"),
            16,
        ),
        (
            "case-conversion-producer-genuine-volatile-spark.jsonl",
            include_str!("fixtures/case-conversion-producer-genuine-volatile-spark.jsonl"),
            4,
        ),
        (
            "case-conversion-unaliased-spark.jsonl",
            include_str!("fixtures/case-conversion-unaliased-spark.jsonl"),
            16,
        ),
    ] {
        let records = capture
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(records[0]["spark_version"], "4.2.0");
        assert_eq!(records.len() - 1, count);
        for representation in 0..3 {
            for batch_size in [1, 32] {
                let session = session(batch_size);
                register_inputs(&session, &records[0], representation, batch_size)?;
                for case in &records[1..] {
                    compared += 1;
                    let expected = &case["expected"];
                    let observed = capture_observation(&session, case).await?;
                    let strict_success = expected["status"] == "ok"
                        && observed.get("error").is_none()
                        && observed["analyzed_schema"] == expected["analyzed_schema"]
                        && observed["physical_schema"] == expected["physical_schema"]
                        && observed["rows"] == expected["rows"];
                    let required_error = if expected["status"] == "error" {
                        errors_compared += 1;
                        required_cast_error_matches(case, &observed, &error_controls)
                    } else {
                        false
                    };
                    let negative = if strict_success {
                        None
                    } else {
                        profiles.iter().find(|profile| {
                            retained_case_negative_matches(
                                profile,
                                fixture,
                                case,
                                representation,
                                batch_size,
                                &observed,
                            )
                        })
                    };
                    strict_successes += usize::from(strict_success);
                    native_negative_matches += usize::from(negative.is_some());
                    let matches = strict_success || required_error || negative.is_some();
                    eprintln!(
                        "c06 observation: {}",
                        json!({"family":"792","fixture":fixture,"id":case["id"],"ansi":case["ansi"],"representation":representation,
                            "batch_size":batch_size,"actual":observed,
                            "spark_error_phase":expected.get("phase"),"native_error_stage":observed.get("error_stage"),
                            "native_cast_error_control_match":required_error,
                            "strict_spark_match":strict_success,"full_compatibility_pass":strict_success,
                            "native_negative_match":negative.is_some(),
                            "owner_or_support_boundary":negative.map(|p| &p["owner_or_support_boundary"]),
                            "regression_gate_pass":matches,"complete_spark_diagnostic_parity":"unverified"})
                    );
                    if !matches {
                        differences.push(format!("{} ANSI={} representation={representation} batch={batch_size}: expected {expected}, observed {observed}",case["id"],case["ansi"]));
                    }
                }
            }
        }
    }
    assert_eq!(compared, 226 * 6);
    assert_eq!(errors_compared, 4 * 6);
    assert_eq!(strict_successes, 1206);
    assert_eq!(native_negative_matches, 126);
    eprintln!("C06 compared {compared}, mismatches {}", differences.len());
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn case_conversion_preserves_native_sql() -> TestResult {
    let metadata = serde_json::from_str::<Value>(
        include_str!("fixtures/case-conversion-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing input metadata")?,
    )?;
    let sql = "SELECT lower(upper(s)) AS l,upper(lower(s)) AS u,lower(lower(s)) AS ll,upper(upper(s)) AS uu,upper(s) ILIKE p AS matched FROM c06_case_conversion ORDER BY k";
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let plain = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_batch_size(batch_size)
                    .with_target_partitions(2),
            );
            let session = session(batch_size);
            register_inputs(&plain, &metadata, representation, batch_size)?;
            register_inputs(&session, &metadata, representation, batch_size)?;
            let expected = plain.sql(sql).await?.collect().await?;
            assert_eq!(
                arrow::util::display::array_value_to_string(expected[0].column(0).as_ref(), 0)?,
                "strasse"
            );
            assert_eq!(session.sql(sql).await?.collect().await?, expected);
            let view = session
                .sql("SELECT k,s,p FROM c06_case_conversion")
                .await?
                .into_view();
            session.register_table("case_view", view)?;
            for ansi in [true, false] {
                let actual = capture_result(&session, &json!({"sql":sql,"ansi":ansi})).await?;
                assert_eq!(actual["rows"][0][0], "straße");
                assert_eq!(actual["rows"][4][1], "İ");
                let from_view = capture_result(
                    &session,
                    &json!({"sql":sql.replace("c06_case_conversion", "case_view"),"ansi":ansi}),
                )
                .await?;
                for field in ["analyzed_schema", "physical_schema", "rows"] {
                    assert_eq!(from_view[field], actual[field]);
                }
                assert_eq!(session.sql(sql).await?.collect().await?, expected);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn case_conversion_preserves_native_values_boundaries() -> TestResult {
    let records = include_str!("fixtures/case-conversion-limit-offset-spark.jsonl")
        .lines()
        .skip(1)
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let plain = SessionContext::new_with_config(
        SessionConfig::new()
            .with_batch_size(32)
            .with_target_partitions(2),
    );
    let session = session(32);
    let mut compared = 0;
    for case in records.iter().filter(|case| {
        case["ansi"] == true
            && case["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("exact_"))
    }) {
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        let expected = plain.sql(sql).await?.collect().await?;
        assert_eq!(session.sql(sql).await?.collect().await?, expected);
        for ansi in [true, false] {
            capture_result(&session, &json!({"sql":sql,"ansi":ansi})).await?;
            assert_eq!(session.sql(sql).await?.collect().await?, expected);
        }
        compared += 1;
    }
    assert_eq!(compared, 3);
    Ok(())
}

#[tokio::test]
async fn explicit_string_casts_keep_utf8_arrow_results() -> TestResult {
    let metadata = serde_json::from_str::<Value>(
        include_str!("fixtures/case-conversion-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing input metadata")?,
    )?;
    for representation in 0..3 {
        let session = session(32);
        register_inputs(&session, &metadata, representation, 32)?;
        for ansi in [true, false] {
            let query = plan_query(
                &session,
                "SELECT lower(CAST(upper(s) AS STRING)) AS c,lower(TRY_CAST(upper(s) AS STRING)) AS t FROM c06_case_conversion ORDER BY k",
                &SparkSqlOptions {
                    ansi_mode: ansi,
                    ..Default::default()
                },
            )
            .await?;
            for field in query.logical_plan.schema().fields() {
                assert_eq!(field.data_type(), &DataType::Utf8);
            }
            let physical = query.create_physical_plan(&session).await?;
            for field in physical.schema().fields() {
                assert_eq!(field.data_type(), &DataType::Utf8);
            }
            for batch in collect(physical, session.task_ctx()).await? {
                for column in batch.columns() {
                    assert_eq!(column.data_type(), &DataType::Utf8);
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn case_conversion_closed_arguments_respect_folding_boundaries() -> TestResult {
    use std::sync::LazyLock;

    use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature};

    static INVOCATIONS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct ClosedCaseInput {
        volatile: bool,
    }

    impl ScalarUDFImpl for ClosedCaseInput {
        fn name(&self) -> &str {
            "closed_case_input"
        }

        fn signature(&self) -> &Signature {
            static IMMUTABLE: LazyLock<Signature> =
                LazyLock::new(|| Signature::exact(vec![DataType::Utf8], Volatility::Immutable));
            static VOLATILE: LazyLock<Signature> =
                LazyLock::new(|| Signature::exact(vec![DataType::Utf8], Volatility::Volatile));
            if self.volatile { &VOLATILE } else { &IMMUTABLE }
        }

        fn supports_constant_folding(&self) -> bool {
            self.volatile
        }

        fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            INVOCATIONS.fetch_add(1, Ordering::SeqCst);
            Ok(args.args[0].clone())
        }
    }

    for volatile in [true, false] {
        let session = session(32);
        session.register_udf(ScalarUDF::from(ClosedCaseInput { volatile }));
        session.register_batch(
            "case_closed_rows",
            arrow::record_batch::RecordBatch::try_from_iter(vec![(
                "k",
                Arc::new(arrow::array::Int32Array::from(vec![0])) as arrow::array::ArrayRef,
            )])?,
        )?;
        for ansi in [true, false] {
            INVOCATIONS.store(0, Ordering::SeqCst);
            let query = plan_query(
                &session,
                "SELECT lower(upper(closed_case_input('ß'))) AS r FROM case_closed_rows",
                &SparkSqlOptions {
                    ansi_mode: ansi,
                    ..Default::default()
                },
            ).await?;
            assert_eq!(INVOCATIONS.load(Ordering::SeqCst), 0);
            let physical = query.create_physical_plan(&session).await?;
            assert_eq!(INVOCATIONS.load(Ordering::SeqCst), 0);
            let batches = collect(physical, session.task_ctx()).await?;
            assert_eq!(INVOCATIONS.load(Ordering::SeqCst), 1);
            let mut values = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    values.push(arrow::util::display::array_value_to_string(batch.column(0).as_ref(), row)?);
                }
            }
            assert_eq!(values, ["ß"]);
        }
    }
    Ok(())
}

#[tokio::test]
async fn nested_case_conversion_evaluates_volatile_input_once() -> TestResult {
    let session = session(32);
    session.register_batch(
        "case_input",
        arrow::record_batch::RecordBatch::try_from_iter(vec![(
            "s",
            Arc::new(arrow::array::StringArray::from(vec!["Straße", "ß"]))
                as arrow::array::ArrayRef,
        )])?,
    )?;
    let evaluated_rows = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&evaluated_rows);
    session.register_udf(create_udf(
        "case_identity",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Volatile,
        Arc::new(move |arguments| {
            let arrays = ColumnarValue::values_to_arrays(arguments)?;
            counter.fetch_add(arrays[0].len(), Ordering::SeqCst);
            Ok(ColumnarValue::Array(Arc::clone(&arrays[0])))
        }),
    ));
    for ansi in [true, false] {
        for (expression, expected) in [
            ("lower(upper(case_identity(s)))", ["straße", "ß"]),
            ("upper(lower(case_identity(s)))", ["STRASSE", "SS"]),
            ("lower(lower(case_identity(s)))", ["straße", "ß"]),
            ("upper(upper(case_identity(s)))", ["STRASSE", "SS"]),
        ] {
            evaluated_rows.store(0, Ordering::SeqCst);
            let query = plan_query(
                &session,
                &format!("SELECT {expression} AS r FROM case_input"),
                &SparkSqlOptions {
                    ansi_mode: ansi,
                    ..Default::default()
                },
            )
            .await?;
            assert_eq!(evaluated_rows.load(Ordering::SeqCst), 0);
            let physical = query.create_physical_plan(&session).await?;
            assert_eq!(evaluated_rows.load(Ordering::SeqCst), 0);
            let batches = collect(physical, session.task_ctx()).await?;
            assert_eq!(evaluated_rows.load(Ordering::SeqCst), 2);
            let mut values = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    values.push(arrow::util::display::array_value_to_string(
                        batch.column(0).as_ref(),
                        row,
                    )?);
                }
            }
            assert_eq!(values, expected);
        }
    }
    Ok(())
}
