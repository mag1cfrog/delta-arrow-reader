//! Complete generated window labels remain usable through outer SQL consumers.

use std::collections::BTreeSet;
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Int32Type};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, create_udf,
};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_inputs::register_recorded_inputs;
use spark_results::{
    TestResult, compare_exact_spark_query_stages_with_optimized, matches_spark_int32_cast_input,
};

fn window_name_references() -> TestResult<Vec<Value>> {
    let staged = include_str!("fixtures/window-name-stages-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(staged.len() - 1, 354);
    assert!(staged[0]["spark_build"].as_str().is_some_and(|build| {
        build.contains("revision=32f7299601108917fb01920a54e084595b7b3bf8")
    }));
    let mut records = Vec::new();
    for (corpus, source) in [
        (
            "original",
            include_str!("fixtures/window-names-spark.jsonl"),
        ),
        (
            "alias_free_control",
            include_str!("fixtures/window-names-pivot-controls-spark.jsonl"),
        ),
    ] {
        let mut captured = source
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        if records.is_empty() {
            records.push(captured.remove(0));
        } else {
            assert_eq!(records[0], captured.remove(0));
        }
        for mut case in captured {
            case["corpus"] = json!(corpus);
            records.push(case);
        }
    }
    assert_eq!(records.len() - 1, 294);
    assert_eq!(staged[0]["inputs"], records[0]["inputs"]);
    for original in &records[1..] {
        let recaptured = staged[1..]
            .iter()
            .find(|case| {
                case["corpus"] == original["corpus"]
                    && case["id"] == original["id"]
                    && case["ansi"] == original["ansi"]
            })
            .ok_or("recapture omitted an original reference")?;
        assert_eq!(recaptured["sql"], original["sql"]);
        assert_eq!(recaptured["expected"], original["expected"]);
        assert_eq!(recaptured["source_expected"], original["expected"]);
        if original["corpus"] == "alias_free_control" {
            let id = format!(
                "{}_parenthesized",
                original["id"].as_str().ok_or("missing ID")?
            );
            assert!(staged[1..].iter().any(|case| {
                case["corpus"] == "supplement"
                    && case["id"] == id
                    && case["ansi"] == original["ansi"]
            }));
        }
    }
    for table in ["inputs", "empty_inputs", "null_inputs"] {
        for name in [
            "literal_arguments",
            "coerced_default",
            "aggregate_named",
            "zero_frame",
            "folded_ignore_nulls",
            "composed",
            "outer_order",
        ] {
            for ansi in [true, false] {
                assert!(staged[1..].iter().any(|case| {
                    case["corpus"] == "supplement"
                        && case["id"] == format!("{table}_{name}")
                        && case["ansi"] == ansi
                }));
            }
        }
    }
    let mut errors = 0;
    let mut ordered = 0;
    for case in &staged[1..] {
        if case["expected"].get("rows").is_some() {
            for stage in [
                "analyzed",
                "optimized",
                "executed_before_collect",
                "executed",
            ] {
                assert!(case["schema_stages"].get(stage).is_some());
            }
        } else {
            errors += 1;
            assert_eq!(
                case["expected"]["condition"],
                "INVALID_PARAMETER_VALUE.BOOLEAN"
            );
            assert_eq!(case["spark_error_phase"], "sql");
            assert_eq!(case["schema_stages"], json!({}));
            assert_eq!(case["spark_error_details"]["sql_state"], "22023");
            assert_eq!(
                case["spark_error_details"]["message_parameters"],
                json!({
                    "functionName":"`nth_value`", "parameter":"`ignoreNulls`",
                    "invalidValue":"\"CAST(true AS BOOLEAN)\"",
                })
            );
        }
        if case["ordered"] == true {
            ordered += 1;
            assert_eq!(case["expected"]["rows"], case["rows_raw"]);
            assert!(
                case["sql"]
                    .as_str()
                    .is_some_and(|sql| sql.ends_with(" ORDER BY id"))
            );
        }
    }
    assert_eq!(errors, 6);
    assert_eq!(ordered, 6);
    Ok(staged)
}

fn matches_window_boolean_parameter_error(observation: &Value) -> bool {
    observation["native_phase"] == "resolve"
        && observation["error"].as_str().is_some_and(|error| {
            [
                "[INVALID_PARAMETER_VALUE.BOOLEAN]",
                "`ignoreNulls` in `nth_value`",
                "expects a boolean literal, but got \"CAST(true AS BOOLEAN)\"",
            ]
            .into_iter()
            .all(|cause| error.contains(cause))
        })
}

/// Compare all schema stages, allowing only (field index, prior native nullable flag).
fn window_schemas_match_reference(
    case: &Value,
    observation: &Value,
    retained_nullability: Option<(usize, bool)>,
) -> bool {
    observation["optimized_observation"]["names_match"] == true
        && [
            ("analyzed", "public_schema"),
            ("optimized", "optimized_schema"),
            ("executed_before_collect", "before_collect"),
            ("executed", "physical_schema"),
        ]
        .into_iter()
        .all(|(stage, key)| {
            let Some(expected) = case["schema_stages"].get(stage) else {
                return false;
            };
            let Some(mut actual) = observation.get(key).cloned() else {
                return false;
            };
            if let Some((field, nullable)) = retained_nullability {
                if actual["fields"][field]["nullable"] != nullable
                    || expected["fields"][field]["nullable"] != false
                {
                    return false;
                }
                actual["fields"][field]["nullable"] = expected["fields"][field]["nullable"].clone();
            }
            actual == *expected
        })
}

/// Require exact errors and phase, with all schema and row observations present as JSON null.
fn matches_unmaterialized_window_error(observation: &Value, error: &str, phase: &str) -> bool {
    observation["native_phase"] == phase
        && observation["error"] == error
        && observation["optimized_error"] == error
        && [
            "public_schema",
            "before_collect",
            "physical_schema",
            "rows",
            "rows_raw",
            "optimized_schema",
            "optimized_observation",
        ]
        .into_iter()
        .all(|key| observation.get(key).is_some_and(Value::is_null))
}

/// Match an exact input and its frozen native facet; reachable labels still use Spark references.
fn matched_window_gap_owner(
    case: &Value,
    observation: &Value,
    controls: &[Value],
) -> Option<String> {
    let control = controls.iter().find(|control| {
        ["corpus", "id", "ansi", "sql"]
            .into_iter()
            .all(|key| control[key] == case[key])
            && control["reference_condition"] == case["expected"]["condition"]
    })?;
    let owner = control["owner"].as_str()?;
    let error = control["error"].as_str().unwrap_or("");
    let phase = control["phase"].as_str().unwrap_or("");
    let success = observation["native_phase"] == "encode"
        && observation.get("error").is_some_and(Value::is_null)
        && observation
            .get("optimized_error")
            .is_some_and(Value::is_null)
        && observation["rows"].is_array()
        && observation["rows_raw"].is_array()
        && observation["ordered_rows_match"] == true;
    let matches = match (control["kind"].as_str()?, owner) {
        ("boolean_error", "#1045") => {
            case["expected"]["condition"] == "INVALID_PARAMETER_VALUE.BOOLEAN"
                && phase == "resolve"
                && matches_unmaterialized_window_error(observation, error, phase)
        }
        ("lag_nullability", "#1044") => {
            let field = usize::try_from(control["nullable_field"].as_u64()?).ok()?;
            let nullable = control["nullable"].as_bool()?;
            success
                && window_schemas_match_reference(case, observation, Some((field, nullable)))
                && observation["rows"] == case["expected"]["rows"]
        }
        ("offset_null_rows", "#1048") => {
            case["expected"]["condition"] == "INTERNAL_ERROR"
                && case["spark_error_phase"] == "collect"
                && success
                && window_schemas_match_reference(case, observation, None)
                && observation["rows"] == control["rows"]
        }
        ("offset_frame_error", "#1048") => {
            matches_unmaterialized_window_error(observation, error, phase)
        }
        ("offset_cast_phase", "#1048") => {
            let fields = observation["public_schema"]["fields"].as_array()?;
            let native = control["native_public_fields"].as_array()?;
            let names = control["name_control"]["names"].as_array()?;
            case["expected"]["condition"] == "CAST_INVALID_INPUT"
                && case["spark_error_phase"] == "sql"
                && phase == "create_physical_plan"
                && observation["native_phase"] == phase
                && observation["error"] == error
                && observation["optimized_error"] == error
                && observation["public_schema"]["type"] == "struct"
                && fields.len() == native.len()
                && fields.len() == names.len()
                && fields
                    .iter()
                    .zip(native)
                    .zip(names)
                    .all(|((field, expected), name)| {
                        let mut nonname = field.clone();
                        if let Some(field) = nonname.as_object_mut() {
                            field.remove("name");
                        }
                        field["name"] == *name && nonname == *expected
                    })
                && [
                    "before_collect",
                    "physical_schema",
                    "rows",
                    "rows_raw",
                    "optimized_schema",
                    "optimized_observation",
                ]
                .into_iter()
                .all(|key| observation.get(key).is_some_and(Value::is_null))
        }
        _ => false,
    };
    matches.then(|| owner.to_string())
}

fn uses_unsupported_lead_null_rows_policy(case: &Value) -> bool {
    case["corpus"] == "additional_boundary"
        && case["id"] == "explicit_lead_invalid_cast"
        && case["ansi"] == false
}

fn matches_unsupported_lead_null_rows_bound(case: &Value, observation: &Value) -> bool {
    uses_unsupported_lead_null_rows_policy(case)
        && case["sql"]
            == "SELECT LEAD(v,CAST('bad' AS INT)) OVER (ORDER BY id ROWS BETWEEN CAST('bad' AS INT) FOLLOWING AND CAST('bad' AS INT) FOLLOWING) FROM inputs"
        && case["expected"]["condition"] == "INTERNAL_ERROR"
        && case["spark_error_phase"] == "collect"
        && case["spark_error_details"]["sql_state"] == "XX000"
        && case["spark_error_details"]["message_parameters"]
            == json!({
                "message":"Unsupported factory: (FRAME_LESS_OFFSET,RowFrame,null,null,List())"
            })
        && observation.get("observer_error").is_none()
        && observation.get("native_panic").is_none()
        && matches_unmaterialized_window_error(
            observation,
            "External error: not supported: NULL ROWS frame lower bound",
            "resolve",
        )
}

fn matches_window_int32_negation_overflow(case: &Value, observation: &Value) -> bool {
    let error = "External error: error in DataFusion: Execution error: Int32 overflow on negative(-2147483648)";
    case["ansi"] == true
        && case["expected"]["condition"] == "ARITHMETIC_OVERFLOW"
        && case["spark_error_phase"] == "sql"
        && case["spark_error_details"]["sql_state"] == "22003"
        && matches!(
            case["sql"].as_str(),
            Some(
                "SELECT LAG(v,-2147483648) OVER (ORDER BY id) FROM inputs"
                    | "SELECT LAG(v,CAST(-2147483648 AS INT)) OVER (ORDER BY id) FROM inputs"
                    | "SELECT id,LAG(v,CAST(-2147483648 AS INT),-99) OVER (ORDER BY id) FROM inputs ORDER BY id"
            )
        )
        && matches_unmaterialized_window_error(observation, error, "resolve")
}

#[test]
fn window_gap_checks_reject_changes_outside_owned_facets() -> TestResult {
    let retained: Value =
        serde_json::from_str(include_str!("fixtures/window-name-retained-facets.json"))?;
    let controls = retained["controls"]
        .as_array()
        .ok_or("missing retained facets")?;
    let mut references = window_name_references()?;
    references.extend(
        include_str!("fixtures/window-name-boundaries-spark.jsonl")
            .lines()
            .skip(1)
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?,
    );
    for control in controls {
        let case = references
            .iter()
            .find(|case| {
                ["corpus", "id", "ansi", "sql"]
                    .into_iter()
                    .all(|key| case[key] == control[key])
            })
            .ok_or("missing retained reference")?;
        let mut observation = json!({
            "public_schema":case["schema_stages"]["analyzed"],
            "optimized_schema":case["schema_stages"]["optimized"],
            "before_collect":case["schema_stages"]["executed_before_collect"],
            "physical_schema":case["schema_stages"]["executed"],
            "optimized_observation":{"names_match":true},"optimized_error":null,
            "rows":case["expected"]["rows"],"rows_raw":case["expected"]["rows"],
            "native_phase":"encode","error":null,"ordered_rows_match":true,
        });
        match control["kind"].as_str().ok_or("missing retained kind")? {
            "boolean_error" | "offset_frame_error" | "offset_cast_phase" => {
                for key in [
                    "public_schema",
                    "optimized_schema",
                    "before_collect",
                    "physical_schema",
                    "rows",
                    "rows_raw",
                    "optimized_observation",
                ] {
                    observation[key] = Value::Null;
                }
                observation["error"] = control["error"].clone();
                observation["optimized_error"] = control["error"].clone();
                observation["native_phase"] = control["phase"].clone();
                if control["kind"] == "offset_cast_phase" {
                    let fields = control["native_public_fields"]
                        .as_array()
                        .ok_or("missing native fields")?
                        .iter()
                        .zip(
                            control["name_control"]["names"]
                                .as_array()
                                .ok_or("missing name control")?,
                        )
                        .map(|(field, name)| {
                            let mut field = field.clone();
                            field["name"] = name.clone();
                            field
                        })
                        .collect::<Vec<_>>();
                    observation["public_schema"] = json!({"type":"struct","fields":fields});
                }
            }
            "offset_null_rows" => {
                observation["rows"] = control["rows"].clone();
                observation["rows_raw"] = control["rows"].clone();
            }
            "lag_nullability" => {
                let field = usize::try_from(
                    control["nullable_field"]
                        .as_u64()
                        .ok_or("missing nullable field")?,
                )?;
                for key in [
                    "public_schema",
                    "optimized_schema",
                    "before_collect",
                    "physical_schema",
                ] {
                    observation[key]["fields"][field]["nullable"] = control["nullable"].clone();
                }
            }
            _ => return Err("unexpected retained kind".into()),
        }
        assert_eq!(
            matched_window_gap_owner(case, &observation, controls).as_deref(),
            control["owner"].as_str()
        );
        for (key, value) in [
            ("error", json!("different cause")),
            ("native_phase", json!("different phase")),
            ("rows", json!([["different value"]])),
        ] {
            let mut changed = observation.clone();
            changed[key] = value;
            assert!(
                matched_window_gap_owner(case, &changed, controls).is_none(),
                "{} {key}",
                control["id"]
            );
        }
        for schema in [
            "public_schema",
            "optimized_schema",
            "before_collect",
            "physical_schema",
        ] {
            let mut changed = observation.clone();
            if changed[schema].is_null() {
                changed[schema] = json!({"unexpected":"schema"});
            } else {
                changed[schema]["fields"][0]["metadata"] = json!({"unexpected":"metadata"});
            }
            assert!(
                matched_window_gap_owner(case, &changed, controls).is_none(),
                "{} {schema}",
                control["id"]
            );
        }
        if !observation["public_schema"].is_null() {
            for (key, value) in [
                ("name", json!("wrong label")),
                ("type", json!("wrong type")),
                (
                    "nullable",
                    json!(
                        !observation["public_schema"]["fields"][0]["nullable"]
                            .as_bool()
                            .ok_or("missing nullable")?
                    ),
                ),
            ] {
                let mut changed = observation.clone();
                changed["public_schema"]["fields"][0][key] = value;
                assert!(
                    matched_window_gap_owner(case, &changed, controls).is_none(),
                    "{} {key}",
                    control["id"]
                );
            }
        }
        let mut changed_case = case.clone();
        changed_case["sql"] = json!("SELECT 1");
        assert!(matched_window_gap_owner(&changed_case, &observation, controls).is_none());
    }
    Ok(())
}

#[test]
fn window_negation_overflow_requires_exact_input_operation_type_and_phase() -> TestResult {
    let cases = include_str!("fixtures/window-name-boundaries-spark.jsonl")
        .lines()
        .skip(1)
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let error = "External error: error in DataFusion: Execution error: Int32 overflow on negative(-2147483648)";
    let observation = json!({"native_phase":"resolve","error":error,"optimized_error":error,
        "public_schema":null,"optimized_schema":null,"before_collect":null,"physical_schema":null,
        "rows":null,"rows_raw":null,"optimized_observation":null});
    let minimum_cases = cases
        .iter()
        .filter(|case| {
            case["ansi"] == true && case["expected"]["condition"] == "ARITHMETIC_OVERFLOW"
        })
        .collect::<Vec<_>>();
    assert_eq!(minimum_cases.len(), 3);
    for case in minimum_cases {
        assert!(matches_window_int32_negation_overflow(case, &observation));
        for wrong_error in [
            error.replace("Int32", "Int64"),
            error.replace("negative", "add"),
            error.replace("-2147483648", "-2147483647"),
            format!("{error}\nother cause"),
        ] {
            let mut changed = observation.clone();
            changed["error"] = json!(wrong_error);
            changed["optimized_error"] = changed["error"].clone();
            assert!(!matches_window_int32_negation_overflow(case, &changed));
        }
        let mut changed = observation.clone();
        changed["native_phase"] = json!("collect");
        assert!(!matches_window_int32_negation_overflow(case, &changed));
        let mut changed = observation.clone();
        changed["public_schema"] = json!({"type":"struct","fields":[]});
        assert!(!matches_window_int32_negation_overflow(case, &changed));
        for (key, value) in [
            ("ansi", json!(false)),
            (
                "sql",
                json!(
                    case["sql"]
                        .as_str()
                        .ok_or("missing SQL")?
                        .replace("-2147483648", "-2147483647")
                ),
            ),
        ] {
            let mut changed = case.clone();
            changed[key] = value;
            assert!(!matches_window_int32_negation_overflow(
                &changed,
                &observation
            ));
        }
        let mut changed = case.clone();
        changed["expected"]["condition"] = json!("CAST_INVALID_INPUT");
        assert!(!matches_window_int32_negation_overflow(
            &changed,
            &observation
        ));
        let mut changed = case.clone();
        changed["spark_error_phase"] = json!("collect");
        assert!(!matches_window_int32_negation_overflow(
            &changed,
            &observation
        ));
    }
    Ok(())
}

#[test]
fn explicit_lead_null_rows_requires_the_intentional_error_and_exact_observations() -> TestResult {
    let cases = include_str!("fixtures/window-name-boundaries-spark.jsonl")
        .lines()
        .skip(1)
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let selected = cases
        .iter()
        .filter(|case| uses_unsupported_lead_null_rows_policy(case))
        .collect::<Vec<_>>();
    assert_eq!(selected.len(), 1);
    let case = selected[0];
    let error = "External error: not supported: NULL ROWS frame lower bound";
    let observation = json!({"native_phase":"resolve","error":error,"optimized_error":error,
        "public_schema":null,"optimized_schema":null,"before_collect":null,"physical_schema":null,
        "rows":null,"rows_raw":null,"optimized_observation":null});
    assert!(matches_unsupported_lead_null_rows_bound(case, &observation));
    for wrong_error in [
        "External error: invalid argument: invalid window boundary offset".to_owned(),
        "External error: invalid argument: [INTERNAL_ERROR] another cause".to_owned(),
        error.replace("lower", "upper"),
        error.replace("ROWS", "RANGE"),
        format!("{error} unrelated cause"),
    ] {
        let mut changed = observation.clone();
        changed["error"] = json!(wrong_error);
        changed["optimized_error"] = changed["error"].clone();
        assert!(!matches_unsupported_lead_null_rows_bound(case, &changed));
    }
    for (field, value) in [
        ("native_phase", json!("collect")),
        ("optimized_error", json!("another cause")),
        ("rows", json!([])),
        ("rows_raw", json!([])),
        ("observer_error", json!("capture failed")),
        ("native_panic", json!("capture failed")),
    ] {
        let mut changed = observation.clone();
        changed[field] = value;
        assert!(!matches_unsupported_lead_null_rows_bound(case, &changed));
    }
    for field in [
        "public_schema",
        "optimized_schema",
        "before_collect",
        "physical_schema",
        "rows",
        "rows_raw",
        "optimized_observation",
    ] {
        let mut changed = observation.clone();
        changed
            .as_object_mut()
            .ok_or("missing observation")?
            .remove(field);
        assert!(!matches_unsupported_lead_null_rows_bound(case, &changed));
        let mut changed = observation.clone();
        changed[field] = json!({"type":"struct","fields":[]});
        assert!(!matches_unsupported_lead_null_rows_bound(case, &changed));
    }
    for (field, value) in [
        ("ansi", json!(true)),
        ("id", json!("another_case")),
        ("corpus", json!("another_corpus")),
        (
            "sql",
            json!(
                case["sql"]
                    .as_str()
                    .ok_or("missing SQL")?
                    .replace(" AS INT", " AS BIGINT")
            ),
        ),
        ("expected", json!({"condition":"CAST_INVALID_INPUT"})),
        ("spark_error_phase", json!("sql")),
    ] {
        let mut changed = case.clone();
        changed[field] = value;
        assert!(!matches_unsupported_lead_null_rows_bound(
            &changed,
            &observation
        ));
    }
    Ok(())
}

#[test]
fn window_parameter_errors_require_the_specific_boolean_cause_during_resolution() {
    let error = "[INVALID_PARAMETER_VALUE.BOOLEAN] The value of parameter(s) `ignoreNulls` in `nth_value` is invalid: expects a boolean literal, but got \"CAST(true AS BOOLEAN)\".";
    assert!(matches_window_boolean_parameter_error(&json!({
        "native_phase":"resolve", "error":error,
    })));
    for observation in [
        json!({"native_phase":"collect", "error":error}),
        json!({"native_phase":"resolve", "error":"unsupported window function"}),
        json!({"native_phase":"resolve", "error":error.replace("nth_value", "lead")}),
        json!({"native_phase":"resolve", "error":error.replace("ignoreNulls", "offset")}),
        json!({"native_phase":"resolve", "error":error.replace("CAST(true AS BOOLEAN)", "NULL")}),
        json!({"native_phase":"resolve", "error":error.replace("INVALID_PARAMETER_VALUE.BOOLEAN", "DATATYPE_MISMATCH")}),
        json!({"native_phase":"encode", "error":null}),
    ] {
        assert!(!matches_window_boolean_parameter_error(&observation));
    }
}

#[tokio::test]
async fn generated_window_names_check_spark_and_retained_facets() -> TestResult {
    let records = window_name_references()?;
    let retained: Value =
        serde_json::from_str(include_str!("fixtures/window-name-retained-facets.json"))?;
    let controls = retained["controls"]
        .as_array()
        .ok_or("missing retained facets")?;
    assert!(controls.iter().all(|control| control["owner"] != "#747"));
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let mut identities = BTreeSet::new();
    for case in &records[1..] {
        assert!(identities.insert((
            case["corpus"].as_str().ok_or("missing corpus")?,
            case["id"].as_str().ok_or("missing ID")?,
            case["ansi"].as_bool().ok_or("missing ANSI mode")?,
        )));
    }
    let mut differences = Vec::new();
    let report_path = std::env::var_os("DAR_WINDOW_NAMES_REPORT");
    let mut observations = Vec::new();
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
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &records[1..] {
            let actual = compare_exact_spark_query_stages_with_optimized(&session, case).await;
            let cause_matches = case["expected"].get("condition").is_none()
                || actual
                    .as_ref()
                    .is_ok_and(matches_window_boolean_parameter_error);
            let matches = actual.as_ref().is_ok_and(|result| {
                result["matches"] == true && result["optimized_matches"] == true
            }) && cause_matches;
            let gap_owner = if matches {
                None
            } else {
                actual
                    .as_ref()
                    .ok()
                    .and_then(|result| matched_window_gap_owner(case, result, controls))
            };
            let accepted = matches || gap_owner.is_some();
            let disposition = if matches {
                "spark_match"
            } else if gap_owner.is_some() {
                "retained_prior_native_facets"
            } else {
                "unexpected_difference"
            };
            if report_path.is_some() {
                observations.push(json!({
                    "corpus":case["corpus"], "id":case["id"],
                    "ansi":case["ansi"], "batch":batch_size, "reference":case,
                    "actual":actual.as_ref().map_or_else(
                        |error| json!({"error":error.to_string()}), |result| result.clone()),
                    "cause_matches":cause_matches, "matches":matches,
                    "accepted":accepted,"disposition":disposition,"owned_gap":gap_owner,
                }));
            }
            if !accepted {
                differences.push(format!(
                    "{}:{} ANSI={} batch={batch_size} cause_matches={cause_matches}: {actual:?}",
                    case["corpus"], case["id"], case["ansi"]
                ));
            }
        }
    }
    if let Some(path) = report_path {
        std::fs::write(path, serde_json::to_vec_pretty(&observations)?)?;
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

#[tokio::test]
async fn generated_interval_window_names_match_original_spark_stages() -> TestResult {
    let records = include_str!("fixtures/interval-qualifiers/interval-qualifiers-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let cases = records[1..]
        .iter()
        .filter(|case| {
            case["context"] == "window"
                && matches!(
                    case["id"].as_str(),
                    Some(
                        "range_day"
                            | "range_hour"
                            | "range_day_hour"
                            | "range_day_second"
                            | "range_year"
                            | "range_month"
                    )
                )
        })
        .collect::<Vec<_>>();
    assert_eq!(cases.len(), 12);
    let mut observations = Vec::new();
    for batch_size in [1, 32] {
        let session = SessionContext::new_with_state(
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
        );
        register_recorded_inputs(&session, &records[0], batch_size)?;
        for case in &cases {
            let actual = compare_exact_spark_query_stages_with_optimized(&session, case).await;
            let matches = actual.as_ref().is_ok_and(|result| {
                result["matches"] == true && result["optimized_matches"] == true
            });
            observations.push(json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "reference":case, "matches":matches,
                "actual":actual.unwrap_or_else(|error| json!({"error":error.to_string()})),
            }));
        }
    }
    if let Some(path) = std::env::var_os("DAR_WINDOW_NAMES_REPORT") {
        std::fs::write(
            std::path::PathBuf::from(path).with_extension("interval.json"),
            serde_json::to_vec_pretty(&observations)?,
        )?;
    }
    let differences = observations
        .iter()
        .filter(|observation| observation["matches"] != true)
        .collect::<Vec<_>>();
    assert!(differences.is_empty(), "{differences:#?}");
    Ok(())
}

#[tokio::test]
async fn generated_window_names_cover_source_boundaries() -> TestResult {
    let retained: Value =
        serde_json::from_str(include_str!("fixtures/window-name-retained-facets.json"))?;
    let controls = retained["controls"]
        .as_array()
        .ok_or("missing retained facets")?;
    let records = include_str!("fixtures/window-name-boundaries-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 40);
    assert_eq!(
        records[0]["spark_revision"],
        "32f7299601108917fb01920a54e084595b7b3bf8"
    );
    // Python UDF admission is research evidence; Rust UDF capability tests are separate below.
    let (python_udf, cases): (Vec<_>, Vec<_>) = records[1..].iter().partition(|case| {
        matches!(
            case["id"].as_str(),
            Some(
                "immutable_offset_727_implicit"
                    | "immutable_offset_727_explicit"
                    | "volatile_offset_727_implicit"
                    | "volatile_offset_727_explicit"
            )
        )
    });
    assert_eq!(python_udf.len(), 8);
    // Typed Spark metadata cannot be registered through Arrow's string-valued metadata API.
    let (typed_metadata, mut cases): (Vec<_>, Vec<_>) = cases
        .into_iter()
        .partition(|case| case["id"] == "aliases_metadata");
    assert_eq!(typed_metadata.len(), 2);
    assert_eq!(cases.len(), 30);
    let mut input_metadata = records[0].clone();
    input_metadata["inputs"]
        .as_array_mut()
        .ok_or("missing inputs")?
        .retain(|input| input["name"] != "metadata_inputs");
    let string_metadata = include_str!("fixtures/window-name-string-metadata-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(string_metadata.len() - 1, 2);
    assert_eq!(
        string_metadata[0]["spark_revision"],
        records[0]["spark_revision"]
    );
    assert!(
        string_metadata[1..]
            .iter()
            .all(|case| case["id"] == "aliases_string_metadata")
    );
    assert_eq!(
        string_metadata[1..]
            .iter()
            .filter_map(|case| case["ansi"].as_bool())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([false, true])
    );
    let string_inputs = string_metadata[0]["inputs"]
        .as_array()
        .ok_or("missing string-metadata inputs")?
        .iter()
        .filter(|input| input["name"] == "metadata_string_inputs")
        .collect::<Vec<_>>();
    assert_eq!(string_inputs.len(), 1);
    input_metadata["inputs"]
        .as_array_mut()
        .ok_or("missing inputs")?
        .push((*string_inputs[0]).clone());
    cases.extend(&string_metadata[1..]);
    assert_eq!(cases.len(), 32);
    let mut observations = Vec::new();
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
        register_recorded_inputs(&session, &input_metadata, batch_size)?;
        for case in &cases {
            let actual = compare_exact_spark_query_stages_with_optimized(&session, case).await;
            let matches = actual.as_ref().is_ok_and(|result| {
                result["matches"] == true
                    && result["optimized_matches"] == true
                    && matches_spark_int32_cast_input(case, result)
            });
            let negation_overflow_matches = actual
                .as_ref()
                .is_ok_and(|result| matches_window_int32_negation_overflow(case, result));
            let unsupported_null_rows = uses_unsupported_lead_null_rows_policy(case);
            let unsupported_null_rows_matches = actual
                .as_ref()
                .is_ok_and(|result| matches_unsupported_lead_null_rows_bound(case, result));
            let gap_owner = if matches || negation_overflow_matches || unsupported_null_rows {
                None
            } else {
                actual
                    .as_ref()
                    .ok()
                    .and_then(|result| matched_window_gap_owner(case, result, controls))
            };
            let accepted = if unsupported_null_rows {
                unsupported_null_rows_matches
            } else {
                matches || negation_overflow_matches || gap_owner.is_some()
            };
            let disposition = if unsupported_null_rows_matches {
                "intentional_unsupported_null_rows_bound"
            } else if unsupported_null_rows {
                "unexpected_difference"
            } else if matches {
                "spark_match"
            } else if negation_overflow_matches {
                "equivalent_int32_negation_overflow"
            } else if gap_owner.is_some() {
                "retained_prior_native_facets"
            } else {
                "unexpected_difference"
            };
            observations.push(json!({
                "id":case["id"], "ansi":case["ansi"], "batch":batch_size,
                "reference":case, "matches":matches,
                "actual":actual.unwrap_or_else(|error| json!({"error":error.to_string()})),
                "accepted":accepted,"disposition":disposition,"owned_gap":gap_owner,
                "negation_overflow_matches":negation_overflow_matches,
                "intentional_unsupported_null_rows_matches":unsupported_null_rows_matches,
            }));
        }
    }
    if let Some(path) = std::env::var_os("DAR_WINDOW_NAMES_REPORT") {
        std::fs::write(
            std::path::PathBuf::from(path).with_extension("boundaries.json"),
            serde_json::to_vec_pretty(&observations)?,
        )?;
    }
    let differences = observations
        .iter()
        .filter(|observation| observation["accepted"] != true)
        .collect::<Vec<_>>();
    assert!(differences.is_empty(), "{differences:#?}");
    Ok(())
}

#[tokio::test]
async fn window_names_do_not_evaluate_value_or_default_arguments() -> TestResult {
    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "observed_value",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |arguments| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(arguments[0].clone())
        }),
    ));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "observed_default",
        vec![],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Int32(Some(-99)),
            ))
        }),
    ));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "observed_key",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(move |arguments| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(arguments[0].clone())
        }),
    ));
    let observed = calls.clone();
    session.register_udf(create_udf(
        "observed_filter",
        vec![DataType::Boolean],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(move |arguments| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(arguments[0].clone())
        }),
    ));
    for ansi_mode in [true, false] {
        let query = plan_query(&session,
            "SELECT SUM(observed_value(v)) OVER (ORDER BY id),LAG(observed_value(v),1,observed_default()) OVER (ORDER BY id) FROM VALUES(1,2),(2,3) inputs(id,v)",
            &SparkSqlOptions { ansi_mode, ..SparkSqlOptions::default() },
        ).await?;
        assert!(query.column_names[0].contains("observed_value(v)"));
        assert!(query.column_names[1].contains("observed_default()"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let query = plan_query(&session,
            "SELECT SUM(v) FILTER (WHERE observed_filter(p)) OVER (PARTITION BY observed_key(g) ORDER BY observed_key(id)) FROM VALUES(1,1,2,true),(2,1,3,false) inputs(id,g,v,p)",
            &SparkSqlOptions { ansi_mode, ..SparkSqlOptions::default() },
        ).await?;
        for display in [
            "observed_filter(p)",
            "PARTITION BY observed_key(g)",
            "ORDER BY observed_key(id) ASC NULLS FIRST",
        ] {
            assert!(
                query.column_names[0].contains(display),
                "{}",
                query.column_names[0]
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn offset_window_names_respect_folding_permission_and_evaluate_once() -> TestResult {
    static CALLS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct ObservedOffset {
        name: &'static str,
        allow_folding: bool,
    }

    impl ScalarUDFImpl for ObservedOffset {
        fn name(&self) -> &str {
            self.name
        }

        fn signature(&self) -> &Signature {
            static SIGNATURE: LazyLock<Signature> =
                LazyLock::new(|| Signature::exact(vec![], Volatility::Immutable));
            &SIGNATURE
        }

        fn supports_constant_folding(&self) -> bool {
            self.allow_folding
        }

        fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(
            &self,
            _: ScalarFunctionArgs,
        ) -> datafusion::common::Result<ColumnarValue> {
            CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Int32(Some(1)),
            ))
        }
    }

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    session.register_udf(
        ScalarUDF::from(ObservedOffset {
            name: "abs",
            allow_folding: false,
        })
        .with_aliases(["unfolded_offset"]),
    );
    session.register_udf(ScalarUDF::from(ObservedOffset {
        name: "folded_offset",
        allow_folding: true,
    }));
    session.register_udf(create_udf(
        "volatile_offset",
        vec![],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(|_| {
            CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(
                datafusion::common::ScalarValue::Int32(Some(1)),
            ))
        }),
    ));
    session.register_udf(create_udf(
        "throwing_offset",
        vec![],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|_| {
            CALLS.fetch_add(1, Ordering::SeqCst);
            datafusion::common::exec_err!("observed offset must not run before admission")
        }),
    ));
    let mut failures = Vec::new();
    for ansi_mode in [true, false] {
        let options = SparkSqlOptions {
            ansi_mode,
            ..SparkSqlOptions::default()
        };
        for kind in ["arity", "filter"] {
            let mut control_error = None;
            for (id, offset) in [
                ("one", "1"),
                ("zero", "0"),
                ("folded", "folded_offset()"),
                ("unfolded", "unfolded_offset()"),
                ("volatile", "volatile_offset()"),
                ("throwing", "throwing_offset()"),
                ("divide", "CAST(1/0 AS INT)"),
                (
                    "higher_order",
                    "AGGREGATE(ARRAY(1),0,(acc,x)->folded_offset())",
                ),
            ] {
                let call = if kind == "arity" {
                    format!("LAG(v,{offset},0,0)")
                } else {
                    format!("LAG(v,{offset},0) FILTER (WHERE true)")
                };
                let sql = format!(
                    "SELECT {call} OVER (ORDER BY id) FROM VALUES(1,10),(2,20) inputs(id,v)"
                );
                let before = CALLS.load(Ordering::SeqCst);
                let result = plan_query(&session, &sql, &options).await;
                let error = result.as_ref().err().map(ToString::to_string);
                let callback_count = CALLS.load(Ordering::SeqCst) - before;
                let observation = json!({"id":format!("{kind}_{id}"),"ansi":ansi_mode,"sql":sql,
                    "callback_count":callback_count,"phase":if result.is_err(){"resolve"}else{"resolved"},
                    "error":error,"expected_cause_kind":kind});
                eprintln!("WINDOW_OFFSET_ADMISSION {observation}");
                if id == "one" {
                    let cause_matches = error.as_ref().is_some_and(|error| {
                        if kind == "arity" {
                            error.contains("lag(Int32, Int32, Int32, Int32)")
                        } else {
                            error
                                .contains("FILTER is only supported for aggregate window functions")
                        }
                    });
                    if !cause_matches {
                        failures.push(format!("invalid admission control: {observation}"));
                    }
                    control_error = error.clone();
                }
                if error.is_none() || error != control_error || callback_count != 0 {
                    failures.push(observation.to_string());
                }
            }
        }
        for function in ["LAG", "LEAD"] {
            for offset in [
                "unfolded_offset()",
                "1 + unfolded_offset()",
                "volatile_offset()",
                "AGGREGATE(ARRAY(1),0,(acc,x)->folded_offset())",
                "1 + AGGREGATE(ARRAY(1),0,(acc,x)->folded_offset())",
            ] {
                for explicit in [false, true] {
                    let frame = if !explicit {
                        ""
                    } else if function == "LAG" {
                        " ROWS BETWEEN -1 FOLLOWING AND -1 FOLLOWING"
                    } else {
                        " ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING"
                    };
                    let sql = format!(
                        "SELECT {function}(v,{offset}) OVER (ORDER BY id{frame}) FROM VALUES(1,10),(2,20) inputs(id,v)"
                    );
                    let before = CALLS.load(Ordering::SeqCst);
                    let result = plan_query(&session, &sql, &options).await;
                    let error = result.as_ref().err().map(ToString::to_string);
                    let callback_count = CALLS.load(Ordering::SeqCst) - before;
                    let observation = json!({"id":format!("blocked_{function}_{offset}_{explicit}"),
                        "ansi":ansi_mode,"sql":sql,"callback_count":callback_count,
                        "phase":if result.is_err(){"resolve"}else{"resolved"},"error":error});
                    eprintln!("WINDOW_OFFSET_ADMISSION {observation}");
                    if !error
                        .as_ref()
                        .is_some_and(|error| error.contains("[WINDOW_OFFSET_FRAME_MISMATCH]"))
                        || callback_count != 0
                    {
                        failures.push(observation.to_string());
                    }
                }
            }
        }
        for offset in ["folded_offset()", "abs(-1)"] {
            for frame in ["", " ROWS BETWEEN -1 FOLLOWING AND -1 FOLLOWING"] {
                let sql = format!(
                    "SELECT LAG(v,{offset},-99) OVER (ORDER BY id{frame}) FROM VALUES(1,10),(2,20) inputs(id,v)"
                );
                let before = CALLS.load(Ordering::SeqCst);
                let mut phase = "resolve";
                let mut plan_callback_count = None;
                let result: TestResult<_> = async {
                    let query = plan_query(&session, &sql, &options).await?;
                    plan_callback_count = Some(CALLS.load(Ordering::SeqCst) - before);
                    phase = "create_physical_plan";
                    let physical = query.create_physical_plan(&session).await?;
                    phase = "collect";
                    let mut values = collect(physical, session.task_ctx())
                        .await?
                        .into_iter()
                        .flat_map(|batch| {
                            batch
                                .column(0)
                                .as_primitive::<Int32Type>()
                                .values()
                                .to_vec()
                        })
                        .collect::<Vec<_>>();
                    values.sort_unstable();
                    Ok((query.column_names, values))
                }
                .await;
                let callback_count = CALLS.load(Ordering::SeqCst) - before;
                let observation = json!({"id":format!("allowed_{offset}_{frame}"),"ansi":ansi_mode,"sql":sql,
                    "callback_count":callback_count,"plan_callback_count":plan_callback_count,
                    "phase":phase,"error":result.as_ref().err().map(ToString::to_string),
                    "result":result.as_ref().ok()});
                eprintln!("WINDOW_OFFSET_ADMISSION {observation}");
                let expected_calls = usize::from(offset == "folded_offset()");
                if !result
                    .as_ref()
                    .is_ok_and(|(names, values)| names[0].contains(offset) && values == &[-99, 10])
                    || callback_count != expected_calls
                    || plan_callback_count != Some(expected_calls)
                {
                    failures.push(observation.to_string());
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[tokio::test]
async fn generated_window_names_leave_native_sql_unchanged() -> TestResult {
    let metadata: Value = serde_json::from_str(
        include_str!("fixtures/window-names-spark.jsonl")
            .lines()
            .next()
            .ok_or("missing metadata")?,
    )?;
    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    for session in [&native, &configured] {
        register_recorded_inputs(session, &metadata, 2)?;
    }
    let sqls = [
        "SELECT id,ROW_NUMBER() OVER (ORDER BY id),LAG(v) OVER (ORDER BY id),SUM(v) OVER (PARTITION BY g) FROM inputs ORDER BY id",
        "SELECT SUM(v),CAST(v AS BIGINT),ABS(v) FROM inputs GROUP BY v ORDER BY v",
        "SELECT SUM(v) AS total,CAST(v AS BIGINT) AS converted,ABS(v) AS magnitude FROM inputs GROUP BY v ORDER BY v",
        "SELECT id,SUM(v) OVER (ORDER BY id) FROM empty_inputs ORDER BY id",
    ];
    let mut observations = Vec::new();
    for after_spark_query in [false, true] {
        if after_spark_query {
            let query = plan_query(
                &configured,
                "SELECT ROW_NUMBER() OVER (ORDER BY id),SUM(v) OVER (PARTITION BY g) FROM inputs",
                &SparkSqlOptions::default(),
            )
            .await?;
            let physical = query.create_physical_plan(&configured).await?;
            collect(physical, configured.task_ctx()).await?;
        }
        for sql in sqls {
            let mut results = Vec::new();
            for (name, session) in [("native", &native), ("configured", &configured)] {
                let mut phase = "sql";
                let result: TestResult<_> = async {
                    let frame = session.sql(sql).await?;
                    let public_schema = frame.schema().clone();
                    phase = "create_physical_plan";
                    let physical = frame.create_physical_plan().await?;
                    let before_collect = physical.schema();
                    phase = "collect";
                    let batches = collect(physical.clone(), session.task_ctx()).await?;
                    let physical_schema = physical.schema();
                    phase = "concat_batches";
                    let rows = arrow::compute::concat_batches(&physical_schema, &batches)?;
                    Ok((public_schema, before_collect, physical_schema, rows))
                }
                .await;
                results.push((name, phase, result.map_err(|error| error.to_string())));
            }
            observations.push((after_spark_query, sql, results));
        }
    }
    eprintln!("native isolation observations: {observations:#?}");
    for (index, observation) in observations.iter().enumerate() {
        let results = &observation.2;
        assert_eq!(results[0].1, results[1].1, "{observation:?}");
        assert_eq!(results[0].2, results[1].2, "{observation:?}");
        let actual = &results[0].2;
        if index % sqls.len() == 1 {
            assert!(actual.as_ref().is_err_and(|error| error.contains(
                "Projections require unique expression names but the expression \"CAST(inputs.v AS Int64)\" at position 1 and \"inputs.v\" at position 3 have the same name."
            )), "original duplicate-name query: {observation:?}");
        } else {
            let (public, before, after, rows) = actual
                .as_ref()
                .map_err(|error| format!("{observation:?}: {error}"))?;
            assert_eq!(public.fields(), before.fields(), "{observation:?}");
            assert_eq!(before, after, "{observation:?}");
            assert_eq!(&rows.schema(), after, "{observation:?}");
        }
        if index >= sqls.len() {
            assert_eq!(
                observations[index - sqls.len()].2,
                *results,
                "{observation:?}"
            );
        }
    }
    Ok(())
}
