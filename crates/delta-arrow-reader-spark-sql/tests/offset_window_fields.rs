//! Offset-window Fields match Spark; separately owned native gaps retain exact profiles.

use std::collections::{BTreeMap, BTreeSet};
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
use spark_results::{
    STAGES, TestResult, capture_executed_query_result, capture_native_query_observation,
    case_identity, compare_exact_spark_query_stages,
    compare_exact_spark_query_stages_with_optimized, native_result_matches_spark_reference,
    validate_spark_reference_result,
};

fn new_test_session(batch_size: usize, enable_spark_sql: bool) -> SessionContext {
    let builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(
            SessionConfig::new()
                .with_batch_size(batch_size)
                .with_target_partitions(2),
        );
    SessionContext::new_with_state(
        (if enable_spark_sql {
            configure_spark_sql(builder)
        } else {
            builder
        })
        .build(),
    )
}

fn load_reference_fixtures(
    source_request_json: &str,
    spark_reference_jsonl: &str,
    retained_profiles_json: &str,
) -> TestResult<(Value, Vec<Value>, Value)> {
    let request: Value = serde_json::from_str(source_request_json)?;
    let mut records = spark_reference_jsonl
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let header = records.remove(0);
    let retained: Value = serde_json::from_str(retained_profiles_json)?;
    assert_eq!(request["issue"], 1044);
    assert_eq!(
        request["expected_record_count"],
        retained["expected_source_records"]
    );
    assert_eq!(request["native_batch_sizes"], json!([1, 32]));
    assert_eq!(request["native_provider_partitions"], 2);
    assert_eq!(request["native_target_partitions"], 2);
    assert_eq!(header["issue"], request["issue"]);
    assert_eq!(header["spark_version"], request["spark_version"]);
    assert_eq!(
        header["spark_build"]["SPARK_REVISION"]["value"],
        request["spark_revision"]
    );
    assert_eq!(
        header["input_request"]["sha256"],
        retained["source_request_sha256"]
    );
    assert_eq!(
        header["schema_stage_protocol"],
        json!(STAGES.map(|(stage, _)| stage))
    );
    assert_eq!(header["spark_only"], true);
    assert_eq!(header["reads_expected_outputs"], false);
    assert_eq!(
        header["corpora"]
            .as_array()
            .ok_or("missing captured corpora")?
            .len(),
        1
    );
    assert_eq!(
        request["corpora"]
            .as_array()
            .ok_or("missing source corpora")?
            .len(),
        1
    );
    let corpus = &request["corpora"][0];
    for key in ["corpus", "source", "metadata"] {
        assert_eq!(
            header["corpora"][0][key], corpus[key],
            "captured {key} differs from source"
        );
    }
    let source_cases = corpus["cases"].as_array().ok_or("missing source cases")?;
    let expected = source_cases
        .iter()
        .map(case_identity)
        .collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(json!(source_cases.len()), request["expected_record_count"]);
    assert_eq!(json!(expected.len()), request["expected_record_count"]);
    assert_eq!(request["sql_source_count"], json!(source_cases.len() / 2));
    let mut captured = BTreeSet::new();
    for case in &records {
        validate_spark_reference_result(case)?;
        assert!(
            captured.insert(case_identity(case)?),
            "duplicate captured identity"
        );
        let source = source_cases
            .iter()
            .find(|source| {
                source["corpus"] == case["corpus"]
                    && source["id"] == case["id"]
                    && source["ansi"] == case["ansi"]
            })
            .ok_or("captured identity absent from source")?;
        assert!(
            source
                .as_object()
                .ok_or("source case is not an object")?
                .iter()
                .all(|(key, value)| case.get(key) == Some(value)),
            "captured source context changed"
        );
    }
    assert_eq!(captured, expected);
    for (corpus, id, _) in &captured {
        assert!(captured.contains(&(corpus.clone(), id.clone(), true)));
        assert!(captured.contains(&(corpus.clone(), id.clone(), false)));
    }
    let controls = retained["controls"]
        .as_array()
        .ok_or("missing retained controls")?;
    let profiles = retained["profiles"]
        .as_object()
        .ok_or("missing native profile bank")?;
    assert_eq!(
        json!(controls.len() * 2),
        retained["expected_retained_native_contexts"]
    );
    assert_eq!(
        json!(records.len() * 2),
        retained["expected_native_contexts"]
    );
    let mut controlled = BTreeSet::new();
    let mut used_profiles = BTreeSet::new();
    for control in controls {
        let key = case_identity(control)?;
        assert!(
            controlled.insert(key.clone()),
            "duplicate retained identity"
        );
        let case = records
            .iter()
            .find(|case| case_identity(case).ok().as_ref() == Some(&key))
            .ok_or("retained identity absent from references")?;
        assert_eq!(control["sql"], case["sql"]);
        assert!(
            control["owner_issue"]
                .as_u64()
                .is_some_and(|owner| owner > 0),
            "missing native owner"
        );
        match control["kind"].as_str() {
            Some("rows") => assert_eq!(case["status"], "ok"),
            Some("diagnostic" | "untyped_null") => assert_eq!(case["status"], "error"),
            _ => return Err("unknown retained kind".into()),
        }
        let profile = control["profile"]
            .as_str()
            .ok_or("missing profile binding")?;
        assert!(
            profiles.get(profile).is_some_and(Value::is_object),
            "missing complete native profile"
        );
        used_profiles.insert(profile.to_owned());
    }
    assert_eq!(
        used_profiles,
        profiles.keys().cloned().collect::<BTreeSet<_>>(),
        "unused native profile"
    );
    Ok((header, records, retained))
}

fn matches_retained_native_profile(
    reference_case: &Value,
    native_observation: &Value,
    retained_control: &Value,
    profile_bank: &Value,
) -> TestResult<bool> {
    let expected_native_observation = profile_bank
        .get(
            retained_control["profile"]
                .as_str()
                .ok_or("missing profile name")?,
        )
        .ok_or("missing native profile")?;
    // Independent Spark Fields remain authoritative even for a retained native value/error gap.
    let reference_fields_match = STAGES.iter().all(|(stage, native_field_key)| {
        reference_case["schema_stages"]
            .get(*stage)
            .is_none_or(|expected_schema| {
                native_observation.get(*native_field_key) == Some(expected_schema)
            })
    });
    Ok(reference_fields_match && native_observation == expected_native_observation)
}

// Offset value regressions compare executed names and types without nullable flags or metadata.
fn executed_field_names_and_types(schema: &Value) -> Option<Vec<(&str, Value)>> {
    schema["fields"]
        .as_array()?
        .iter()
        .map(|field| Some((field["name"].as_str()?, executed_data_type(&field["type"])?)))
        .collect()
}

fn executed_data_type(data_type: &Value) -> Option<Value> {
    match data_type["type"].as_str() {
        Some("struct") => {
            Some(json!({"type":"struct","fields":executed_field_names_and_types(data_type)?}))
        }
        Some("array") => Some(json!({"type":"array",
            "elementType":executed_data_type(&data_type["elementType"])?})),
        Some("map") => Some(json!({"type":"map",
            "keyType":executed_data_type(&data_type["keyType"])?,
            "valueType":executed_data_type(&data_type["valueType"])?})),
        _ => data_type.as_str().map(Value::from),
    }
}

#[tokio::test]
async fn zero_offset_nulls_match_spark() -> TestResult {
    let manifest: Value =
        serde_json::from_str(include_str!("fixtures/zero-offset-nulls/inputs.json"))?;
    assert_eq!(manifest["issue"], 1117);
    assert_eq!(manifest["spark_version"], "4.2.0");
    assert_eq!(manifest["ansi_modes"], json!([false, true]));
    assert_eq!(manifest["native_batch_sizes"], json!([1, 32]));
    let inputs: Value = serde_json::from_str(include_str!(
        "fixtures/offset-window-fields-1044-cases.json"
    ))?;
    let cases = manifest["cases"].as_array().ok_or("missing cases")?;
    let references = include_str!("fixtures/zero-offset-nulls/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(cases.len(), 9);
    assert_eq!(references.len(), cases.len() * 2);
    let mut completed = BTreeSet::new();
    for batch in [1, 32] {
        let session = new_test_session(batch, true);
        spark_inputs::register_recorded_inputs(&session, &inputs["corpora"][0]["metadata"], batch)?;
        for reference in &references {
            let source = cases
                .iter()
                .find(|case| case["id"] == reference["id"])
                .ok_or("unexpected reference ID")?;
            assert_eq!(reference["sql"], source["sql"]);
            assert_eq!(reference["ordered"], true);
            assert_eq!(reference["status"], "ok");
            let id = reference["id"].as_str().ok_or("missing reference ID")?;
            let ansi = reference["ansi"].as_bool().ok_or("missing ANSI mode")?;
            assert!(completed.insert((id, ansi, batch)), "duplicate reference");
            let actual = capture_executed_query_result(&session, reference).await?;
            assert_eq!(
                actual["rows"], reference["rows"],
                "{} ANSI={} batch={batch}",
                reference["id"], reference["ansi"]
            );
            assert_eq!(
                executed_field_names_and_types(&actual["schema"])
                    .ok_or("missing executed fields")?,
                executed_field_names_and_types(&reference["physical_schema"])
                    .ok_or("missing Spark executed fields")?,
                "{} ANSI={} batch={batch}",
                reference["id"],
                reference["ansi"]
            );
            eprintln!(
                "ZERO_OFFSET_NULLS {}",
                json!({"id":reference["id"],"ansi":reference["ansi"],"batch":batch,"actual":actual,"matches":true})
            );
        }
    }
    assert_eq!(completed.len(), cases.len() * 4);
    Ok(())
}

#[tokio::test]
async fn row_dependent_offset_defaults_match_spark() -> TestResult {
    let manifest: Value = serde_json::from_str(include_str!(
        "fixtures/row-dependent-offset-defaults/inputs.json"
    ))?;
    assert_eq!(manifest["issue"], 1118);
    assert_eq!(manifest["spark_version"], "4.2.0");
    assert_eq!(
        manifest["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    assert_eq!(manifest["timezone"], "UTC");
    assert_eq!(manifest["ansi_modes"], json!([false, true]));
    let cases = manifest["cases"].as_array().ok_or("missing cases")?;
    assert_eq!(cases.len(), 19);
    let mut planned = BTreeMap::new();
    for case in cases {
        let id = case["id"].as_str().ok_or("missing case ID")?;
        let sql = case["sql"].as_str().ok_or("missing SQL")?;
        let modes = case
            .get("ansi_modes")
            .unwrap_or(&manifest["ansi_modes"])
            .as_array()
            .ok_or("missing ANSI modes")?;
        assert!(!modes.is_empty(), "missing ANSI modes for {id}");
        for mode in modes {
            let ansi = mode.as_bool().ok_or("expected an ANSI boolean")?;
            assert!(planned.insert((id, ansi), sql).is_none(), "duplicate {id}");
        }
    }
    let references = include_str!("fixtures/row-dependent-offset-defaults/spark-results.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let mut expected_fields = Vec::new();
    for case in &references {
        let id = case["id"].as_str().ok_or("missing reference ID")?;
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        let sql = planned
            .remove(&(id, ansi))
            .ok_or_else(|| format!("unexpected Spark reference {id} ANSI={ansi}"))?;
        assert_eq!(case["sql"], sql, "{id} ANSI={ansi}");
        assert_eq!(case["status"], "ok", "{id} ANSI={ansi}");
        assert!(case["rows"].is_array(), "missing Spark rows for {id}");
        expected_fields.push(
            executed_field_names_and_types(&case["physical_schema"])
                .ok_or("missing Spark executed fields")?,
        );
    }
    assert_eq!(references.len(), 34);
    assert!(planned.is_empty(), "missing Spark references: {planned:?}");

    let mut differences = Vec::new();
    for batch in [1, 32] {
        let session = new_test_session(batch, true);
        spark_inputs::register_recorded_inputs(
            &session,
            &json!({"inputs":manifest["registered_inputs"]}),
            batch,
        )?;
        for (case, expected_fields) in references.iter().zip(&expected_fields) {
            let query = json!({"sql":case["sql"],"ansi":case["ansi"],"ordered":true,
                "expected":{"rows":case["rows"]}});
            let actual = match AssertUnwindSafe(compare_exact_spark_query_stages(&session, &query))
                .catch_unwind()
                .await
            {
                Ok(Ok(mut observation)) => {
                    observation["status"] = json!(if observation["error"].is_null() {
                        "ok"
                    } else {
                        "error"
                    });
                    observation
                }
                Ok(Err(error)) => json!({"status":"error","error":error.to_string()}),
                Err(panic) => {
                    let message = panic
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| panic.downcast_ref::<&str>().copied());
                    json!({"status":"panic","panic":message})
                }
            };
            let fields_match = executed_field_names_and_types(&actual["physical_schema"])
                .is_some_and(|fields| &fields == expected_fields);
            let rows_match = actual["rows_raw"] == case["rows"];
            let matches = actual["status"] == "ok" && fields_match && rows_match;
            let observation = json!({"id":case["id"],"sql":case["sql"],"ansi":case["ansi"],
                "batch":batch,"matches":matches,"fields_match":fields_match,"rows_match":rows_match,
                "spark":case,"actual":actual});
            eprintln!("ROW_DEPENDENT_OFFSET_DEFAULTS {observation}");
            if !matches {
                differences.push(format!(
                    "{} ANSI={} batch={batch}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert!(
        differences.is_empty(),
        "{} of {} offset default contexts differ:\n{}",
        differences.len(),
        references.len() * 2,
        differences.join("\n")
    );
    Ok(())
}

#[tokio::test]
async fn offset_window_fields_match_spark_at_each_stage() -> TestResult {
    for (
        source_request_json,
        spark_reference_jsonl,
        retained_profiles_json,
        expected_context_count,
        expected_strict_count,
        expected_retained_count,
    ) in [
        (
            include_str!("fixtures/offset-window-fields-1044-cases.json"),
            include_str!("fixtures/offset-window-fields-1044-spark.jsonl"),
            include_str!("fixtures/offset-window-fields-1044-retained.json"),
            188,
            144,
            44,
        ),
        (
            include_str!("fixtures/offset-window-default-coercion-1044-cases.json"),
            include_str!("fixtures/offset-window-default-coercion-1044-spark.jsonl"),
            include_str!("fixtures/offset-window-default-coercion-1044-retained.json"),
            184,
            164,
            20,
        ),
    ] {
        let (header, cases, retained) = load_reference_fixtures(
            source_request_json,
            spark_reference_jsonl,
            retained_profiles_json,
        )?;
        let controls = retained["controls"]
            .as_array()
            .ok_or("missing retained controls")?;
        let mut expected = BTreeSet::new();
        for case in &cases {
            let key = case_identity(case)?;
            for batch in [1, 32] {
                expected.insert((key.clone(), batch));
            }
        }
        assert_eq!(expected.len(), expected_context_count);
        let mut completed = BTreeSet::new();
        let mut failures = Vec::new();
        let mut raw_strict_count = 0;
        let mut retained_count = 0;
        for batch in [1, 32] {
            let session = new_test_session(batch, true);
            spark_inputs::register_recorded_inputs(
                &session,
                &header["corpora"][0]["metadata"],
                batch,
            )?;
            for case in &cases {
                let key = case_identity(case)?;
                let actual =
                    compare_exact_spark_query_stages_with_optimized(&session, case).await?;
                let strict = native_result_matches_spark_reference(&actual, case);
                let control = controls
                    .iter()
                    .find(|control| case_identity(control).ok().as_ref() == Some(&key));
                let retained_match = match control {
                    Some(control) if !strict => matches_retained_native_profile(
                        case,
                        &actual,
                        control,
                        &retained["profiles"],
                    )?,
                    _ => false,
                };
                let matches = if control.is_some() {
                    retained_match
                } else {
                    strict
                };
                raw_strict_count += usize::from(strict);
                retained_count += usize::from(retained_match);
                assert!(completed.insert((key, batch)), "duplicate native context");
                let observation = json!({"reference":case,"batch":batch,"actual":actual,
                    "raw_strict_matches":strict,"retained_exact_match":retained_match,
                    "owner_issue":control.map(|control| &control["owner_issue"])});
                eprintln!("OFFSET_WINDOW_FIELDS {observation}");
                if !matches {
                    failures.push(observation);
                }
            }
        }
        eprintln!(
            "OFFSET_WINDOW_FIELDS_SUMMARY {}",
            json!({"corpus":header["corpora"][0]["corpus"],"contexts":completed.len(),
            "raw_strict_matches":raw_strict_count,"retained_exact_matches":retained_count})
        );
        assert_eq!(completed, expected);
        assert!(
            failures.is_empty(),
            "unmatched observations or stale retained profiles: {failures:#?}"
        );
        assert_eq!(raw_strict_count, expected_strict_count);
        assert_eq!(
            retained["expected_raw_strict_contexts"],
            json!(raw_strict_count)
        );
        assert_eq!(retained_count, expected_retained_count);
    }
    Ok(())
}

#[test]
fn retained_profiles_cannot_override_spark_field_expectations() -> TestResult {
    for (source_request, spark_reference, retained_profiles, control_kind) in [
        (
            include_str!("fixtures/offset-window-fields-1044-cases.json"),
            include_str!("fixtures/offset-window-fields-1044-spark.jsonl"),
            include_str!("fixtures/offset-window-fields-1044-retained.json"),
            "rows",
        ),
        (
            include_str!("fixtures/offset-window-default-coercion-1044-cases.json"),
            include_str!("fixtures/offset-window-default-coercion-1044-spark.jsonl"),
            include_str!("fixtures/offset-window-default-coercion-1044-retained.json"),
            "diagnostic",
        ),
    ] {
        let (_, cases, mut retained) =
            load_reference_fixtures(source_request, spark_reference, retained_profiles)?;
        if control_kind == "rows" {
            // Successful rows no longer need a waiver, but must still protect the Field guard.
            let case = cases
                .iter()
                .find(|case| case["id"] == "lag_offset_scalar_signed_ignore")
                .ok_or("missing successful row reference")?;
            let mut observation = json!({"error":null,"native_phase":"encode",
                "rows_raw":case["rows_raw"],"optimized_error":null});
            for (stage, native_field_key) in STAGES {
                observation[native_field_key] = case["schema_stages"][stage].clone();
            }
            retained["profiles"]["successful_rows"] = observation;
            retained["controls"]
                .as_array_mut()
                .ok_or("missing controls")?
                .push(
                    json!({"corpus":case["corpus"],"id":case["id"],"ansi":case["ansi"],
                    "kind":"rows","profile":"successful_rows"}),
                );
        }
        let control = retained["controls"]
            .as_array()
            .ok_or("missing controls")?
            .iter()
            .find(|control| control["kind"] == control_kind)
            .ok_or("missing retained control")?;
        let key = case_identity(control)?;
        let case = cases
            .iter()
            .find(|case| case_identity(case).ok().as_ref() == Some(&key))
            .ok_or("missing retained reference")?;
        let name = control["profile"].as_str().ok_or("missing profile")?;
        let actual = &retained["profiles"][name];
        assert!(matches_retained_native_profile(
            case,
            actual,
            control,
            &retained["profiles"]
        )?);
        for (stage, native_field_key) in STAGES {
            let Some(schema) = case["schema_stages"].get(stage) else {
                continue;
            };
            let field = 1;
            let nullable = schema["fields"][field]["nullable"]
                .as_bool()
                .ok_or("missing result Field nullability")?;
            for (key, value) in [
                ("nullable", json!(!nullable)),
                ("name", json!("changed_name")),
                ("type", json!("string")),
                ("metadata", json!({"unexpected":"metadata"})),
            ] {
                let mut changed = actual.clone();
                changed[native_field_key]["fields"][field][key] = value;
                let mut changed_profiles = retained["profiles"].clone();
                changed_profiles[name] = changed.clone();
                assert!(
                    !matches_retained_native_profile(case, &changed, control, &changed_profiles)?,
                    "a copied native profile replaced the independent {stage} {key} contract"
                );
            }
        }
        for (key, value) in [
            ("error", json!("a different cause")),
            ("native_phase", json!("collect")),
            ("rows_raw", json!([])),
            ("optimized_error", json!("different optimized cause")),
        ] {
            let mut changed = actual.clone();
            changed[key] = value;
            assert!(!matches_retained_native_profile(
                case,
                &changed,
                control,
                &retained["profiles"]
            )?);
        }
    }
    Ok(())
}

#[tokio::test]
async fn spark_offset_planning_keeps_native_fields_and_required_defaults_unchanged() -> TestResult {
    let (header, cases, _) = load_reference_fixtures(
        include_str!("fixtures/offset-window-fields-1044-cases.json"),
        include_str!("fixtures/offset-window-fields-1044-spark.jsonl"),
        include_str!("fixtures/offset-window-fields-1044-retained.json"),
    )?;
    let (default_coercion_header, default_coercion_cases, _) = load_reference_fixtures(
        include_str!("fixtures/offset-window-default-coercion-1044-cases.json"),
        include_str!("fixtures/offset-window-default-coercion-1044-spark.jsonl"),
        include_str!("fixtures/offset-window-default-coercion-1044-retained.json"),
    )?;
    let metadata = &header["corpora"][0]["metadata"];
    assert_eq!(&default_coercion_header["corpora"][0]["metadata"], metadata);
    let mut sqls = Vec::new();
    for table in ["offset_scalar", "empty_offset_scalar", "null_offset_scalar"] {
        sqls.push(format!("SELECT id,r,v,LAG(r,2,CAST(-99 AS INT)) OVER (ORDER BY id) AS required_lag,LEAD(r,2,CAST(-99 AS INT)) OVER (ORDER BY id) AS required_lead,LAG(r) OVER (ORDER BY id) AS omitted_default,LEAD(v,0,CAST(NULL AS INT)) OVER (ORDER BY id) AS nullable_payload FROM {table} ORDER BY id"));
        sqls.push(format!("SELECT id,LAG(v,0,-99) IGNORE NULLS OVER (ORDER BY id) AS previous,LEAD(v,0,-99) IGNORE NULLS OVER (ORDER BY id) AS following FROM {table} ORDER BY id"));
    }
    for table in ["offset_nested", "empty_offset_nested", "null_offset_nested"] {
        sqls.push(format!("SELECT id,r,v,LAG(r) OVER (ORDER BY id) AS required_source,LEAD(v) OVER (ORDER BY id) AS nullable_source FROM {table} ORDER BY id"));
    }
    for batch in [1, 32] {
        let native = new_test_session(batch, false);
        let configured = new_test_session(batch, true);
        for session in [&native, &configured] {
            spark_inputs::register_recorded_inputs(session, metadata, batch)?;
        }
        let mut expected = Vec::new();
        for sql in &sqls {
            let result = capture_native_query_observation(&native, sql).await?;
            assert_eq!(
                result["error"],
                Value::Null,
                "native control failed: {sql}: {result}"
            );
            let required = result["public_schema"]["fields"]
                .as_array()
                .ok_or("missing native Fields")?
                .iter()
                .filter(|field| {
                    matches!(
                        field["name"].as_str(),
                        Some("required_lag" | "required_lead")
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                required.len(),
                if sql.contains("AS required_lag") {
                    2
                } else {
                    0
                }
            );
            for field in required {
                assert_eq!(
                    field["nullable"], true,
                    "native DataFusion declaration changed: {sql}"
                );
            }
            expected.push(result);
        }
        for after_spark in [false, true] {
            if after_spark {
                for (reference_cases, warmup_id) in [
                    (&cases, "lead_offset_scalar_defaults"),
                    (
                        &default_coercion_cases,
                        "lead_offset_scalar_valid_string_default",
                    ),
                ] {
                    for ansi in [true, false] {
                        let case = reference_cases
                            .iter()
                            .find(|case| case["id"] == warmup_id && case["ansi"] == ansi)
                            .ok_or("missing successful Spark required-default control")?;
                        let actual =
                            compare_exact_spark_query_stages_with_optimized(&configured, case)
                                .await?;
                        assert!(
                            native_result_matches_spark_reference(&actual, case),
                            "Spark warmup differed: {actual}"
                        );
                    }
                }
            }
            for (sql, expected) in sqls.iter().zip(&expected) {
                let actual = capture_native_query_observation(&configured, sql).await?;
                assert_eq!(
                    &actual, expected,
                    "batch={batch} after_spark={after_spark} {sql}"
                );
            }
        }
        for function in ["LAG", "LEAD"] {
            let sql = format!("SELECT {function}(r,2,d) OVER (ORDER BY id) FROM offset_scalar");
            let expected = capture_native_query_observation(&native, &sql).await?;
            assert_eq!(
                expected["phase"], "create_physical_plan",
                "{sql}: {expected}"
            );
            assert!(expected["error"].is_string(), "{sql}: {expected}");
            let actual = capture_native_query_observation(&configured, &sql).await?;
            assert_eq!(actual, expected, "batch={batch} {sql}");
        }
    }
    Ok(())
}
