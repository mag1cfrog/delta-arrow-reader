//! Independent Spark controls for make_ym_interval's BIGINT argument boundaries.

use std::collections::BTreeSet;

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const MANIFEST: &str =
    include_str!("fixtures/bigint-int-narrowing/make-ym-interval-arguments-manifest.json");
const INPUTS: &str = include_str!("fixtures/bigint-int-narrowing/manifest.json");
const REFERENCE: &str =
    include_str!("fixtures/bigint-int-narrowing/make-ym-interval-arguments-spark.jsonl");
const BASELINE: &str =
    include_str!("fixtures/bigint-int-narrowing/make-ym-interval-arguments-native-baseline.jsonl");
const NULL_ADMISSION: &str =
    include_str!("fixtures/bigint-int-narrowing/make-ym-null-admission-spark.jsonl");

fn records(text: &str) -> TestResult<Vec<Value>> {
    Ok(text
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn references() -> TestResult<(Value, Vec<Value>)> {
    let manifest: Value = serde_json::from_str(MANIFEST)?;
    let inputs: Value = serde_json::from_str(INPUTS)?;
    let records = records(REFERENCE)?;
    assert_eq!(records.len(), 33);
    let header = &records[0];
    assert_eq!(header["spark_version"], "4.2.0");
    assert_eq!(
        header["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    for key in [
        "scope",
        "spark_version",
        "spark_revision",
        "timezone",
        "observations",
    ] {
        assert_eq!(header[key], manifest[key], "{key}");
    }
    for key in ["inputs", "native"] {
        assert_eq!(header[key], inputs[key], "{key}");
    }
    let registered = inputs["inputs"]
        .as_array()
        .ok_or("missing inputs")?
        .iter()
        .map(|table| json!({"name":table["name"],"schema":table["schema"],"rows":table["rows"]}))
        .collect::<Vec<_>>();
    assert_eq!(header["registered_inputs"], json!(registered));
    let definitions = manifest["cases"].as_array().ok_or("missing cases")?;
    assert_eq!(definitions.len(), 16);
    let mut expected_ids = BTreeSet::new();
    for source in definitions {
        assert_eq!(source["ansi_modes"], json!([true, false]));
        for ansi in source["ansi_modes"].as_array().ok_or("missing modes")? {
            assert!(expected_ids.insert((source["id"].to_string(), ansi.to_string())));
        }
    }
    let mut actual_ids = BTreeSet::new();
    for case in &records[1..] {
        assert!(actual_ids.insert((case["id"].to_string(), case["ansi"].to_string())));
        let source = definitions
            .iter()
            .find(|source| source["id"] == case["id"])
            .ok_or("unexpected captured ID")?;
        for key in ["sql", "purpose", "domain", "ordered", "input_tables"] {
            assert_eq!(case[key], source[key], "{key}: {case}");
        }
        for (key, value) in manifest["settings"].as_object().ok_or("missing settings")? {
            assert_eq!(&case["settings"][key], value, "{key}: {case}");
        }
        assert_eq!(
            case["settings"]["spark.sql.ansi.enabled"],
            case["ansi"].to_string()
        );
        if case["expected"]["condition"].is_null() || case["spark_error_phase"] == "collect" {
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"][stage].is_object(), "{stage}: {case}");
            }
        }
        if case["expected"]["condition"].is_null() {
            assert!(case["expected"]["rows"].is_array());
            assert_eq!(
                case["expected"]["schema"],
                case["schema_stages"]["analyzed"]
            );
        } else {
            assert!(case["spark_error_details"]["parameters"].is_object());
            assert!(case["spark_error_details"]["message"].is_string());
            assert_eq!(
                case["spark_error_details"]["condition"],
                case["expected"]["condition"]
            );
        }
    }
    assert_eq!(actual_ids, expected_ids);
    Ok((inputs, records[1..].to_vec()))
}

fn native_controls(cases: &[Value]) -> TestResult<Vec<Value>> {
    let records = records(BASELINE)?;
    assert_eq!(records.len(), 33);
    for (key, expected) in [
        ("source_head", "670770531310a6f008215c0c70daf1e889416088"),
        ("source_tree", "430af79230ba3cb5658311824f191a204df301a6"),
        ("runtime_base", "b0fdd7c4d3ce0b1f0e02fa58a143fc0b68bb10a9"),
        (
            "source_log_sha256",
            "d9bcfc1b1849ec8df4937fb3fc7cb6a9017cb9eeeba65b7807b3435ec5fba9e1",
        ),
        (
            "spark_reference_sha256",
            "2b7fb41644c440fa98ed80c777835b9c8130869fcb0227d1c3f559f25293419a",
        ),
    ] {
        assert_eq!(records[0][key], expected, "{key}");
    }
    let mut ids = BTreeSet::new();
    for control in &records[1..] {
        assert!(ids.insert((control["id"].to_string(), control["ansi"].to_string())));
        assert_eq!(control["batch"], 1);
        assert_eq!(control["id"], control["reference"]["id"]);
        assert_eq!(control["ansi"], control["reference"]["ansi"]);
        assert!(control["capture_error"].is_null());
        assert!(control["observation"].is_object());
        assert!(cases.iter().any(|case| case == &control["reference"]));
    }
    Ok(records[1..].to_vec())
}

fn null_admission_references() -> TestResult<Vec<Value>> {
    let records = records(NULL_ADMISSION)?;
    assert_eq!(records.len(), 5);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["spark_build"]["SPARK_REVISION"]["value"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    let definitions = records[0]["cases"]
        .as_array()
        .ok_or("missing admission cases")?;
    assert_eq!(definitions.len(), 2);
    let mut ids = BTreeSet::new();
    for case in &records[1..] {
        assert!(ids.insert((case["id"].to_string(), case["ansi"].to_string())));
        assert!(case["ansi"].is_boolean());
        assert!(
            definitions
                .iter()
                .any(|source| source["id"] == case["id"] && source["sql"] == case["sql"])
        );
        assert_eq!(case["spark_error_phase"], "sql");
        assert_eq!(case["schema_stages"], json!({}));
        for (key, value) in records[0]["settings"]
            .as_object()
            .ok_or("missing settings")?
        {
            assert_eq!(&case["settings"][key], value);
        }
        assert_eq!(
            case["settings"]["spark.sql.ansi.enabled"],
            case["ansi"].to_string()
        );
    }
    Ok(records[1..].to_vec())
}

fn exact_error_message_matches(case: &Value, observation: &Value) -> bool {
    if case["expected"]["condition"].is_null() {
        observation["error"].is_null()
    } else {
        observation["error"] == case["spark_error_details"]["message"]
    }
}

fn full_spark_matches(case: &Value, observation: &Value) -> bool {
    observation["matches"] == true
        && observation["optimized_matches"] == true
        && exact_error_message_matches(case, observation)
}

/// MAKE_YM_INTERVAL's overflow error in the recorded native control, before #1159.
const RECORDED_OVERFLOW_ERROR: &str = "Arrow error: Compute error: \
    [INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION] Integer overflow while operating with \
    intervals.";

/// MAKE_YM_INTERVAL's admission message in the recorded native control, before #1165.
const RECORDED_ADMISSION_MESSAGE: &str =
    "The arguments of Spark `make_ym_interval` must be integer or string or null";

/// MAKE_YM_INTERVAL's admission message since #1165, which admits every number and names
/// Spark's condition.
const ADMISSION_MESSAGE: &str = "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] The arguments of \
    Spark `make_ym_interval` must be numbers, strings or NULL";

/// The recorded native control with MAKE_YM_INTERVAL's messages as they are now. Since #1159 the
/// overflow is Spark's whole message, without the query context on its later lines, and since
/// #1165 the admission message is `ADMISSION_MESSAGE`. Each is replaced only where the control
/// holds the recorded message.
fn control_with_current_messages(case: &Value, control: &Value) -> Value {
    let mut observation = control["observation"].clone();
    if case["expected"]["condition"] == "INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION"
        && observation["error"] == RECORDED_OVERFLOW_ERROR
        && let Some(message) = case["spark_error_details"]["message"]
            .as_str()
            .and_then(|message| message.lines().next())
    {
        observation["error"] = json!(format!("Arrow error: Compute error: {message}"));
    }
    if case["expected"]["condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" {
        for key in ["error", "optimized_error"] {
            if let Some(error) = observation[key].as_str()
                && error.matches(RECORDED_ADMISSION_MESSAGE).count() == 1
            {
                observation[key] =
                    json!(error.replace(RECORDED_ADMISSION_MESSAGE, ADMISSION_MESSAGE));
            }
        }
        // Derive the flags from the new error as the shared observer does: it names Spark's
        // condition, while resolving the query like Spark's analysis.
        let error_matches = observation["native_phase"] == "resolve"
            && observation["error"]
                .as_str()
                .is_some_and(|error| error.contains("[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]"));
        let matches = error_matches
            && [
                "public_matches",
                "before_matches",
                "physical_matches",
                "rows_match",
                "ordered_rows_match",
            ]
            .iter()
            .all(|key| observation[*key] == true);
        let optimized_matches = error_matches
            && case["schema_stages"].get("optimized").is_none()
            && observation["optimized_error"] == observation["error"];
        observation["error_matches"] = json!(error_matches);
        observation["matches"] = json!(matches);
        observation["optimized_matches"] = json!(optimized_matches);
    }
    observation
}

fn conversion_contract_matches(case: &Value, observation: &Value, control: &Value) -> bool {
    if control["reference"] != *case {
        return false;
    }
    if case["id"] == "ym_arguments_null_month_with_year_overflow" {
        return full_spark_matches(case, observation);
    }
    let low_bits = matches!(
        case["id"].as_str(),
        Some(
            "ym_arguments_required_year_low_bits"
                | "ym_arguments_required_month_low_bits"
                | "ym_arguments_required_month_sliced_low_bits"
                | "ym_arguments_nullable_year_low_bits"
        )
    );
    if !low_bits {
        return full_spark_matches(case, observation)
            || observation == &control_with_current_messages(case, control);
    }
    let stages = [
        ("analyzed", "public_schema"),
        ("optimized", "optimized_schema"),
        ("executed_before_collect", "before_collect"),
        ("executed", "physical_schema"),
    ];
    let spark_fields = stages.iter().all(|(recorded, actual)| {
        observation[*actual].is_object() && observation[*actual] == case["schema_stages"][*recorded]
    });
    // #982 owns only these exact inherited constructor field profiles.
    let inherited_fields = case["id"] != "ym_arguments_nullable_year_low_bits"
        && stages.iter().all(|(_, actual)| {
            observation[*actual].is_object()
                && observation[*actual] == control["observation"][*actual]
        });
    if !(spark_fields || inherited_fields)
        || !observation["optimized_error"].is_null()
        || observation["optimized_observation"]["names_match"] != true
    {
        return false;
    }
    if case["ansi"] == false {
        return observation["error"].is_null()
            && observation["native_phase"] == "encode"
            && observation["rows_match"] == true
            && observation["ordered_rows_match"] == true
            && observation["rows_raw"] == case["expected"]["rows"];
    }
    let parameters = &case["spark_error_details"]["parameters"];
    let Some(value) = parameters["value"]
        .as_str()
        .and_then(|value| value.strip_suffix('L'))
        .and_then(|value| value.parse::<i64>().ok())
    else {
        return false;
    };
    let cause = format!("Arrow error: Cast error: Can't cast value {value} to type Int32");
    case["ansi"] == true
        && case["expected"]["condition"] == "CAST_OVERFLOW"
        && parameters["sourceType"] == "\"BIGINT\""
        && parameters["targetType"] == "\"INT\""
        && case["spark_error_phase"] == "collect"
        && observation["native_phase"] == "collect"
        && observation["error"] == format!("[CAST_OVERFLOW]\ncaused by\n{cause}")
}

fn admission_contract_matches(case: &Value, observation: &Value) -> bool {
    let Some(error) = observation["error"].as_str() else {
        return false;
    };
    let parameters = &case["spark_error_details"]["parameters"];
    let cause_matches = match case["expected"]["condition"].as_str() {
        Some("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE") => {
            parameters["paramIndex"] == "second"
                && parameters["inputType"] == "\"ARRAY<INT>\""
                && parameters["requiredType"] == "\"INT\""
                && error.contains(ADMISSION_MESSAGE)
        }
        Some("WRONG_NUM_ARGS.WITHOUT_SUGGESTION") => {
            parameters["functionName"] == "`make_ym_interval`"
                && parameters["expectedNum"] == "[0, 1, 2]"
                && parameters["actualNum"] == "3"
                && error
                    .contains("Spark `make_ym_interval` function requires 0 to 2 arguments, got 3")
        }
        _ => false,
    };
    cause_matches
        && case["spark_error_phase"] == "sql"
        && observation["native_phase"] == "resolve"
        && [
            "public_schema",
            "optimized_schema",
            "optimized_observation",
            "before_collect",
            "physical_schema",
            "rows",
            "rows_raw",
        ]
        .iter()
        .all(|key| observation[*key].is_null())
        && observation["optimized_error"] == observation["error"]
        && !error.contains("Can't cast value")
}

#[test]
fn argument_boundary_gate_rejects_changed_causes_fields_and_null_priority() -> TestResult {
    let (_, cases) = references()?;
    let controls = native_controls(&cases)?;
    let checked = controls
        .iter()
        .find(|control| {
            control["id"] == "ym_arguments_required_year_low_bits" && control["ansi"] == true
        })
        .ok_or("missing checked control")?;
    let case = &checked["reference"];
    let mut repaired = checked["observation"].clone();
    repaired["error"] = json!(format!(
        "[CAST_OVERFLOW]\ncaused by\n{}",
        checked["observation"]["error"]
            .as_str()
            .ok_or("missing original Arrow cause")?
    ));
    assert!(conversion_contract_matches(case, &repaired, checked));
    assert!(!conversion_contract_matches(
        case,
        &checked["observation"],
        checked
    ));
    for error in [
        "[CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: Can't cast value 4294967296 to type Int32",
        "[CAST_OVERFLOW]\ncaused by\nArrow error: Cast error: Can't cast value 4294967297 to type Int64",
    ] {
        let mut changed = repaired.clone();
        changed["error"] = json!(error);
        assert!(!conversion_contract_matches(case, &changed, checked));
    }
    for key in [
        "public_schema",
        "optimized_schema",
        "before_collect",
        "physical_schema",
    ] {
        let mut changed = repaired.clone();
        changed[key]["fields"][0]["metadata"] = json!({"unexpected":"metadata"});
        assert!(!conversion_contract_matches(case, &changed, checked));
    }
    let mut changed = repaired.clone();
    changed["native_phase"] = json!("create_physical_plan");
    assert!(!conversion_contract_matches(case, &changed, checked));
    let mut changed_case = case.clone();
    changed_case["sql"] = json!("SELECT 1");
    assert!(!conversion_contract_matches(
        &changed_case,
        &repaired,
        checked
    ));
    for control in &controls {
        if control["id"] == "ym_arguments_null_month_with_year_overflow" {
            assert!(!conversion_contract_matches(
                &control["reference"],
                &control["observation"],
                control
            ));
        }
    }
    let inherited = controls
        .iter()
        .find(|control| control["id"] == "ym_arguments_incompatible_type_with_overflow")
        .ok_or("missing admission control")?;
    let current = control_with_current_messages(&inherited["reference"], inherited);
    assert_ne!(current, inherited["observation"]);
    assert!(conversion_contract_matches(
        &inherited["reference"],
        &current,
        inherited
    ));
    assert!(!conversion_contract_matches(
        &inherited["reference"],
        &inherited["observation"],
        inherited
    ));
    let mut changed = current.clone();
    changed["error"] = json!(
        current["error"]
            .as_str()
            .ok_or("missing original type error")?
            .replace("Int64, List", "Int32, List")
    );
    changed["optimized_error"] = changed["error"].clone();
    assert!(!conversion_contract_matches(
        &inherited["reference"],
        &changed,
        inherited
    ));
    for case in null_admission_references()? {
        let original = controls
            .iter()
            .find(|control| {
                control["reference"]["expected"]["condition"] == case["expected"]["condition"]
            })
            .ok_or("missing original admission")?;
        let current = control_with_current_messages(&original["reference"], original);
        assert!(admission_contract_matches(&case, &current));
        let mut overflow = current.clone();
        overflow["error"] =
            json!("Arrow error: Cast error: Can't cast value 4294967296 to type Int32");
        overflow["optimized_error"] = overflow["error"].clone();
        assert!(!admission_contract_matches(&case, &overflow));
        let mut late = current.clone();
        late["native_phase"] = json!("collect");
        assert!(!admission_contract_matches(&case, &late));
    }
    Ok(())
}

#[tokio::test]
async fn bigint_int_argument_boundaries_match_conversion_contract() -> TestResult {
    let (manifest, cases) = references()?;
    let controls = native_controls(&cases)?;
    let admission = null_admission_references()?;
    let mut count = 0;
    let mut admission_count = 0;
    let mut failures = Vec::new();
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
        spark_inputs::register_recorded_inputs_with_slices(&session, &manifest, batch_size)?;
        for case in cases.iter().chain(admission.iter()) {
            let is_admission = case["domain"] == "null_admission";
            count += usize::from(!is_admission);
            admission_count += usize::from(is_admission);
            let observation =
                spark_results::compare_exact_spark_query_stages_with_optimized(&session, case)
                    .await;
            let exact_error_message_matches = observation
                .as_ref()
                .is_ok_and(|observation| exact_error_message_matches(case, observation));
            let strict_matches = observation
                .as_ref()
                .is_ok_and(|observation| full_spark_matches(case, observation));
            let required_contract_matches = observation.as_ref().is_ok_and(|observation| {
                if is_admission {
                    admission_contract_matches(case, observation)
                } else {
                    controls
                        .iter()
                        .find(|control| control["reference"] == *case)
                        .is_some_and(|control| {
                            conversion_contract_matches(case, observation, control)
                        })
                }
            });
            let prefix = if is_admission {
                "BIGINT_INT_NULL_ADMISSION"
            } else {
                "BIGINT_INT_ARGUMENT_BOUNDARY"
            };
            eprintln!(
                "{prefix} {}",
                json!({
                    "id":case["id"],"ansi":case["ansi"],"batch":batch_size,
                    "reference":case,"observation":observation.as_ref().ok(),
                    "capture_error":observation.as_ref().err().map(ToString::to_string),
                    "exact_error_message_matches":exact_error_message_matches,
                    "strict_matches":strict_matches,
                    "required_contract_matches":required_contract_matches,
                })
            );
            if !required_contract_matches {
                failures.push(format!(
                    "{} ansi={} batch={batch_size}",
                    case["id"], case["ansi"]
                ));
            }
        }
    }
    assert_eq!(count, 64);
    assert_eq!(admission_count, 8);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
