//! Provisional native Arrow STRING capability profiles for the measured #780 cases.

use std::collections::{BTreeMap, HashSet};

use arrow::array::{Array, BinaryArray};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::aggregate_statistics::PreserveAggregateStatisticsSchema;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_plan::expression_errors::SparkSqlExpressionSemantics;
use sail_plan::like_errors::set_spark_like_query_clock;
use serde_json::{Value, json};

use crate::contracts::{complete_match, completed_stages_are_retained};
use crate::string_binary::{TestResult, capture_observation, register_inputs};

fn lines(text: &str) -> TestResult<Vec<Value>> {
    Ok(text
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn fixtures() -> TestResult<BTreeMap<&'static str, Vec<Value>>> {
    [
        (
            "binary-casts-spark.jsonl",
            include_str!("../fixtures/binary-casts-spark.jsonl"),
        ),
        (
            "string-binary-contracts-spark.jsonl",
            include_str!("../fixtures/string-binary-contracts-spark.jsonl"),
        ),
        (
            "binary-concat-null-spark.jsonl",
            include_str!("../fixtures/binary-concat-null-spark.jsonl"),
        ),
        (
            "binary-string-capability/boundary-spark.jsonl",
            include_str!("../fixtures/binary-string-capability/boundary-spark.jsonl"),
        ),
        (
            "binary-string-capability/controls-spark.jsonl",
            include_str!("../fixtures/binary-string-capability/controls-spark.jsonl"),
        ),
    ]
    .into_iter()
    .map(|(name, text)| Ok((name, lines(text)?)))
    .collect()
}

fn references() -> TestResult<Vec<Value>> {
    Ok(serde_json::from_str(include_str!(
        "../fixtures/binary-string-capability/references.json"
    ))?)
}

fn pins() -> TestResult<Vec<Value>> {
    lines(include_str!(
        "../fixtures/binary-string-capability/native-profiles.jsonl"
    ))
}

// This list admits only measured evaluated conversions. In particular, the
// NULL-first, NULL-separator, guarded and unused-cast controls cannot use it.
fn capability_limit(id: &str) -> Option<&'static str> {
    match id {
        "c06_780_control_concat_null_last" => Some("explicit_cast_before_trailing_null"),
        "c06_780_control_try_cast" => Some("try_cast_malformed_rows"),
        "concat_binary_string"
        | "concat_string_binary"
        | "cast_binary_string_nul_ff_a"
        | "cast_binary_string_continuation"
        | "cast_binary_string_truncated"
        | "cast_binary_string_bad_second_byte"
        | "cast_binary_string_surrogate"
        | "cast_binary_string_too_large"
        | "cast_binary_string_c06_strings_required"
        | "cast_binary_string_c06_strings_nullable"
        | "invalid_utf8_binary_roundtrip_literal"
        | "invalid_utf8_binary_roundtrip_column"
        | "unaliased_binary_names"
        | "c06_remaining_ws_invalid_binary_value"
        | "c06_remaining_ws_invalid_binary_byte_roundtrip"
        | "c06_780_cast_display"
        | "c06_780_cast_roundtrip"
        | "c06_780_concat_left_roundtrip"
        | "c06_780_concat_right_roundtrip"
        | "c06_780_ws_value_roundtrip"
        | "c06_780_ws_separator_roundtrip"
        | "c06_780_ws_null_value_roundtrip"
        | "c06_780_alias_roundtrip"
        | "c06_780_alias_display_and_roundtrip"
        | "c06_780_substring_roundtrip"
        | "c06_780_length"
        | "c06_780_octet_length"
        | "c06_780_surrogate_display_and_roundtrip"
        | "c06_780_control_overlong_literal"
        | "c06_780_control_overlong_column" => Some("evaluated_malformed_string"),
        _ => None,
    }
}

// Arrow Debug prints HashMap metadata in arbitrary order. Canonicalize only
// those complete string maps, preserving names, types, nullability and values.
fn canonical_arrow(text: &str) -> TestResult<String> {
    let mut rest = text;
    let mut result = String::new();
    while let Some(offset) = rest.find("metadata: {") {
        let start = offset + "metadata: ".len();
        result.push_str(&rest[..start]);
        let mut stream = serde_json::Deserializer::from_str(&rest[start..]).into_iter::<Value>();
        let metadata: BTreeMap<String, String> =
            serde_json::from_value(stream.next().ok_or("missing metadata map")??)?;
        result.push_str(&serde_json::to_string(&metadata)?);
        rest = &rest[start + stream.byte_offset()..];
    }
    result.push_str(rest);
    Ok(result)
}

fn canonical(value: &Value) -> TestResult<Value> {
    let mut result = value.clone();
    for path in [
        "/actual/analyzed_arrow_schema",
        "/actual/physical_arrow_schema",
        "/diagnostic/analyzed_arrow_schema",
        "/diagnostic/optimized_arrow_schema",
        "/diagnostic/physical_arrow_schema",
    ] {
        if let Some(value) = result.pointer_mut(path)
            && let Some(text) = value.as_str()
        {
            *value = json!(canonical_arrow(text)?);
        }
    }
    Ok(result)
}

fn retained(pin: &Value) -> Value {
    json!({"actual":pin["actual"],"diagnostic":pin["diagnostic"],
        "primary_stage_availability":pin["primary_stage_availability"],"capture_failure":pin["capture_failure"]})
}

fn strict_match(reference: &Value, case: &Value, observation: &Value) -> bool {
    complete_match(&observation["actual"], &case["expected"])
        && observation["diagnostic"]["status"] == "ok"
        && observation["diagnostic"]["string_binary_byte_rows"]
            == reference["spark_string_binary_byte_rows"]
}

fn native_bytes_match_rows(observed: &Value) -> bool {
    let Some(rows) = observed["actual"]["rows"].as_array() else {
        return observed["diagnostic"]
            .get("string_binary_byte_rows")
            .is_none();
    };
    let Some(bytes) = observed["diagnostic"]["string_binary_byte_rows"].as_array() else {
        return false;
    };
    let Some(fields) = observed["actual"]["physical_schema"]["fields"].as_array() else {
        return false;
    };
    rows.len() == bytes.len()
        && rows.iter().zip(bytes).all(|(row, bytes)| {
            let (Some(row), Some(bytes)) = (row.as_array(), bytes.as_array()) else {
                return false;
            };
            row.len() == fields.len()
                && bytes.len() == fields.len()
                && row
                    .iter()
                    .zip(bytes)
                    .zip(fields)
                    .all(|((value, bytes), field)| {
                        if value.is_null() {
                            return bytes.is_null();
                        }
                        match field["type"].as_str() {
                            Some("string") => {
                                let Some(hex) = bytes.as_str() else {
                                    return false;
                                };
                                if !hex.len().is_multiple_of(2) || !hex.is_ascii() {
                                    return false;
                                }
                                let decoded = (0..hex.len())
                                    .step_by(2)
                                    .map(|index| u8::from_str_radix(&hex[index..index + 2], 16))
                                    .collect::<Result<Vec<_>, _>>();
                                decoded.is_ok_and(|decoded| {
                                    std::str::from_utf8(&decoded).ok() == value.as_str()
                                })
                            }
                            Some("binary") => value == bytes,
                            _ => bytes.is_null(),
                        }
                    })
        })
}

fn matches_pin(
    reference: &Value,
    case: &Value,
    key: &Value,
    pin: &Value,
    observed: &Value,
) -> TestResult<bool> {
    let Some(id) = case["id"].as_str() else {
        return Ok(false);
    };
    let strict = strict_match(reference, case, observed);
    Ok(reference["id"] == case["id"]
        && reference["ansi"] == case["ansi"]
        && reference["family"] == key[0]
        && key[1] == case["id"]
        && key[2] == case["ansi"]
        && case["ansi"].is_boolean()
        && case["sql"].is_string()
        && pin["key"] == *key
        && pin["sql"] == case["sql"]
        && pin["spark_string_binary_byte_rows"] == reference["spark_string_binary_byte_rows"]
        && observed.get("capture_failure") == Some(&Value::Null)
        && completed_stages_are_retained(&observed["actual"])
        && native_bytes_match_rows(observed)
        && canonical(observed)? == canonical(&retained(pin))?
        && pin["strict_spark_match"] == json!(strict)
        && strict == capability_limit(id).is_none())
}

fn error_chain(error: &(dyn std::error::Error + 'static)) -> Value {
    let mut values = Vec::new();
    let mut current = Some(error);
    while let Some(error) = current {
        values.push(json!({"display":error.to_string(),"debug":format!("{error:?}")}));
        current = error.source();
    }
    json!(values)
}

fn fail(result: &mut Value, stage: &str, error: &(dyn std::error::Error + 'static)) {
    result["status"] = json!("error");
    result["error_stage"] = json!(stage);
    result["error_display"] = json!(format!("{stage}: {error}"));
    result["typed_error_chain"] = error_chain(error);
}

// Retain the checked R3 observer's independent repeat. OPT is diagnostic and
// cannot replace primary AN/PH fields or turn a missing/error result into rows.
async fn stage_and_byte_repeat(session: &SessionContext, case: &Value) -> TestResult<Value> {
    let mut result = json!({"status":"ok","query_column_names":null,
        "analyzed_arrow_schema":null,"optimized_arrow_schema":null,"physical_arrow_schema":null,
        "optimized_stage_source":"independent DataFrame::into_optimized_plan repeat with public API state flags",
        "physical_stage_source":"SparkSqlQuery::create_physical_plan on the same resolved query",
        "primary_observation_replaced":false});
    let query = match plan_query(
        session,
        case["sql"].as_str().ok_or("missing SQL")?,
        &SparkSqlOptions {
            ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
            session_timezone: "UTC".into(),
            ..Default::default()
        },
    )
    .await
    {
        Ok(query) => query,
        Err(error) => {
            fail(&mut result, "resolve", &error);
            return Ok(result);
        }
    };
    result["query_column_names"] = json!(query.column_names);
    result["analyzed_arrow_schema"] = json!(format!("{:?}", query.logical_plan.schema()));
    let mut state = session.state();
    set_spark_like_query_clock(&mut state);
    let extensions = &mut state.config_mut().options_mut().extensions;
    extensions.insert(SparkSqlExpressionSemantics::default());
    extensions.insert(PreserveAggregateStatisticsSchema::default());
    match DataFrame::new(state, query.logical_plan.clone()).into_optimized_plan() {
        Ok(optimized) => {
            result["optimized_arrow_schema"] = json!(format!("{:?}", optimized.schema()))
        }
        Err(error) => {
            result["optimized_repeat_error"] =
                json!({"display":error.to_string(),"typed_error_chain":error_chain(&error)})
        }
    }
    let physical = match query.create_physical_plan(session).await {
        Ok(physical) => physical,
        Err(error) => {
            fail(&mut result, "physical", &error);
            return Ok(result);
        }
    };
    let schema = physical.schema();
    result["physical_arrow_schema"] = json!(format!("{schema:?}"));
    let batches = match collect(physical, session.task_ctx()).await {
        Ok(batches) => batches,
        Err(error) => {
            fail(&mut result, "execute", &error);
            return Ok(result);
        }
    };
    let mut byte_rows = Vec::new();
    for batch in batches {
        assert_eq!(batch.schema(), schema);
        // Checked conversion reads existing STRING bytes without constructing
        // an unchecked STRING or substituting displayed replacement characters.
        let bytes = batch
            .columns()
            .iter()
            .map(|column| {
                if matches!(
                    column.data_type(),
                    DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::BinaryView
                        | DataType::FixedSizeBinary(_)
                ) {
                    cast(column, &DataType::Binary).map(Some)
                } else {
                    Ok(None)
                }
            })
            .collect::<datafusion::arrow::error::Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for column in &bytes {
                values.push(match column {
                    Some(column) if !column.is_null(row) => {
                        let column = column
                            .as_any()
                            .downcast_ref::<BinaryArray>()
                            .ok_or("expected Binary byte receipt")?;
                        json!(
                            column
                                .value(row)
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>()
                        )
                    }
                    _ => Value::Null,
                });
            }
            byte_rows.push(values);
        }
    }
    result["string_binary_byte_rows"] = json!(byte_rows);
    Ok(result)
}

pub(crate) async fn check_capability_profiles() -> TestResult {
    let fixtures = fixtures()?;
    let references = references()?;
    let pins = pins()?;
    assert_eq!(references.len(), 130);
    assert_eq!(pins.len(), 780);
    let mut native = BTreeMap::new();
    for pin in pins {
        assert!(
            native.insert(pin["key"].to_string(), pin).is_none(),
            "duplicate native key"
        );
    }
    let mut keys = HashSet::new();
    let mut reference_keys = HashSet::new();
    let mut counts = BTreeMap::new();
    for family in ["frozen-strings", "frozen-ws", "boundary", "controls"] {
        for representation in 0..3 {
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
                let mut registered = false;
                for reference in references
                    .iter()
                    .filter(|reference| reference["family"] == family)
                {
                    let records = fixtures
                        .get(reference["fixture"].as_str().ok_or("missing fixture")?)
                        .ok_or("unknown fixture")?;
                    let line = reference["line"].as_u64().ok_or("missing line")? as usize;
                    let case = records
                        .get(line.checked_sub(1).ok_or("zero fixture line")?)
                        .ok_or("missing reference case")?;
                    if !registered {
                        register_inputs(&session, &records[0], representation, batch_size)?;
                        registered = true;
                    }
                    reference_keys.insert(json!([family, case["id"], case["ansi"]]).to_string());
                    let key = json!([family, case["id"], case["ansi"], representation, batch_size]);
                    assert!(keys.insert(key.to_string()), "duplicate context {key}");
                    let pin = native
                        .get(&key.to_string())
                        .ok_or("missing native profile")?;
                    let actual = capture_observation(&session, case).await?;
                    let diagnostic = stage_and_byte_repeat(&session, case).await?;
                    let observed = json!({"primary_stage_availability":{
                        "analyzed":if actual["analyzed_schema"].is_null(){"unavailable"}else{"available"},
                        "physical":if actual["physical_schema"].is_null(){"unavailable"}else{"available"},
                        "rows":if actual.get("rows").is_some(){"available"}else{"unavailable"},
                        "unavailability_context":actual["error_stage"]},
                        "actual":actual,"diagnostic":diagnostic,"capture_failure":null});
                    let strict = strict_match(reference, case, &observed);
                    let profile = capability_limit(case["id"].as_str().ok_or("missing ID")?)
                        .unwrap_or("supported_input");
                    *counts.entry(profile).or_insert(0usize) += 1;
                    eprintln!(
                        "c06 binary capability observation: {}",
                        json!({
                        "key":key,"sql":case["sql"],"profile":profile,"policy_status":"provisional",
                        "strict_spark_match":strict,"full_spark_compatibility_pass":strict,
                        "spark_string_binary_byte_rows":reference["spark_string_binary_byte_rows"],"observed":observed})
                    );
                    assert!(
                        matches_pin(reference, case, &key, pin, &observed)?,
                        "changed #780 context {key}: expected {pin}, observed {observed}"
                    );
                }
            }
        }
    }
    assert_eq!(reference_keys.len(), 130);
    assert_eq!(keys.len(), native.len());
    assert_eq!(
        counts,
        BTreeMap::from([
            ("supported_input", 396),
            ("evaluated_malformed_string", 360),
            ("explicit_cast_before_trailing_null", 12),
            ("try_cast_malformed_rows", 12),
        ])
    );
    eprintln!(
        "C06 binary capability: {}",
        json!({"observations":keys.len(),"counts":counts,
        "strict_spark_matches":396,"strict_spark_differences":384,"full_spark_compatibility_pass":false,
        "policy_status":"provisional; final-current-source replay and independent review required"})
    );
    Ok(())
}

pub(crate) fn check_profile_mutations() -> TestResult {
    let fixtures = fixtures()?;
    let references = references()?;
    let pins = pins()?;
    for id in [
        "cast_binary_string_nul_ff_a",
        "c06_780_control_try_cast",
        "c06_780_control_guarded_column_cast",
    ] {
        let reference = references
            .iter()
            .find(|reference| reference["id"] == id && reference["ansi"] == true)
            .ok_or("missing mutation reference")?;
        let records = &fixtures[reference["fixture"].as_str().ok_or("missing fixture")?];
        let case = &records[reference["line"].as_u64().ok_or("missing line")? as usize - 1];
        let pin = pins
            .iter()
            .find(|pin| pin["key"] == json!([reference["family"], id, true, 0, 1]))
            .ok_or("missing mutation profile")?;
        let observed = retained(pin);
        assert!(matches_pin(reference, case, &pin["key"], pin, &observed)?);
        for path in [
            "/actual/analyzed_schema/fields/0/name",
            "/actual/analyzed_schema/fields/0/type",
            "/actual/analyzed_schema/fields/0/nullable",
            "/actual/analyzed_schema/fields/0/metadata",
            "/actual/analyzed_arrow_schema",
            "/actual/physical_schema",
            "/primary_stage_availability/analyzed",
            "/diagnostic/optimized_arrow_schema",
            "/diagnostic/primary_observation_replaced",
            "/capture_failure",
        ] {
            let mut changed = observed.clone();
            *changed
                .pointer_mut(path)
                .ok_or("missing mutation property")? = json!("changed");
            assert!(
                !matches_pin(reference, case, &pin["key"], pin, &changed)?,
                "accepted {id} {path}"
            );
        }
        for property in [
            "actual",
            "diagnostic",
            "primary_stage_availability",
            "capture_failure",
        ] {
            let mut changed = observed.clone();
            changed
                .as_object_mut()
                .ok_or("missing object")?
                .remove(property);
            assert!(
                !matches_pin(reference, case, &pin["key"], pin, &changed)?,
                "accepted missing {property}"
            );
        }
        assert!(!matches_pin(
            reference,
            case,
            &pin["key"],
            pin,
            &Value::Null
        )?);
        for property in ["id", "sql", "ansi"] {
            let mut changed = case.clone();
            changed[property] = json!("changed");
            assert!(!matches_pin(
                reference,
                &changed,
                &pin["key"],
                pin,
                &observed
            )?);
        }
        let mut changed = reference.clone();
        changed["spark_string_binary_byte_rows"] = json!([["ff"]]);
        assert!(!matches_pin(&changed, case, &pin["key"], pin, &observed)?);
        if observed["actual"].get("error").is_some() {
            for (key, value) in [
                ("error_stage", json!("execute")),
                ("error", json!("physical: CAST_INVALID_INPUT")),
                (
                    "error",
                    json!(
                        observed["actual"]["error"]
                            .as_str()
                            .ok_or("missing error")?
                            .replace("index 1", "index 0")
                    ),
                ),
            ] {
                let mut changed = observed.clone();
                changed["actual"][key] = value;
                assert!(!matches_pin(reference, case, &pin["key"], pin, &changed)?);
            }
        } else {
            for path in [
                "/actual/rows/0/1",
                "/diagnostic/string_binary_byte_rows/0/1",
            ] {
                let mut changed = observed.clone();
                *changed.pointer_mut(path).ok_or("missing row cell")? = json!("ff");
                assert!(!matches_pin(reference, case, &pin["key"], pin, &changed)?);
            }
            let mut changed = observed.clone();
            changed["actual"]["rows"] = json!([]);
            assert!(!matches_pin(reference, case, &pin["key"], pin, &changed)?);
            if id == "c06_780_control_try_cast" {
                let mut changed = observed.clone();
                changed["actual"]["rows"][3][1] = json!("replacement");
                assert!(!matches_pin(reference, case, &pin["key"], pin, &changed)?);
            }
            if id == "c06_780_control_guarded_column_cast" {
                let mut changed = observed.clone();
                changed["actual"]
                    .as_object_mut()
                    .ok_or("missing actual")?
                    .remove("rows");
                changed["actual"]["error_stage"] = json!("execute");
                changed["actual"]["error"] = json!("execute: checked invalid UTF-8");
                changed["diagnostic"] = json!({"status":"error"});
                let mut forged = pin.clone();
                forged["actual"] = changed["actual"].clone();
                forged["diagnostic"] = changed["diagnostic"].clone();
                forged["strict_spark_match"] = json!(false);
                assert!(
                    !matches_pin(reference, case, &pin["key"], &forged, &changed)?,
                    "positive control admitted a limit profile"
                );
            }
        }
    }
    for id in [
        "c06_780_control_guarded_column_cast",
        "c06_780_control_unused_column_cast",
        "c06_780_control_concat_null_first",
        "c06_780_ws_null_separator",
        "c06_780_empty_required",
        "c06_780_empty_nullable",
        "c06_780_control_unused_literal_cast",
        "c06_780_control_guarded_literal_cast",
    ] {
        assert!(
            capability_limit(id).is_none(),
            "supported control entered a limit profile"
        );
    }
    assert_eq!(
        canonical_arrow("Schema { metadata: {\"b\": \"2\", \"a\": \"1\"} }")?,
        canonical_arrow("Schema { metadata: {\"a\": \"1\", \"b\": \"2\"} }")?
    );
    Ok(())
}
