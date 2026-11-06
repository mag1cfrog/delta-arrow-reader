//! Complete substring descendant comparisons and historical alias receipts.

use std::collections::{BTreeMap, HashSet};

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[allow(
    dead_code,
    reason = "This suite uses partial observations rather than capture_result."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, register_inputs};

#[allow(
    dead_code,
    reason = "This suite reuses the shared stage and exact-error checks."
)]
#[path = "support/string_binary_contracts.rs"]
mod contracts;

const FIXTURES: [(&str, &str, &str, usize); 2] = [
    (
        "descendants",
        "substring-descendant-spark.jsonl",
        include_str!("fixtures/substring-descendant-spark.jsonl"),
        98,
    ),
    (
        "parent_null",
        "substring-parent-null-spark.jsonl",
        include_str!("fixtures/substring-parent-null-spark.jsonl"),
        12,
    ),
];
const DIVIDE_ERROR: &str =
    "physical: preserve_spark_expression_errors\ncaused by\nArrow error: Divide by zero error";
const ARRAY_CONCAT_SQL: &str = "SELECT array_concat(CAST(NULL AS ARRAY<INT>),array(CAST(1/0 AS INT))) AS r FROM substring_descendant_input";
const UNRESOLVED_ARRAY_CONCAT: &str = "[UNRESOLVED_ROUTINE] Cannot resolve routine `array_concat` on search path [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. SQLSTATE: 42883";

fn records(source: &str) -> TestResult<Vec<Value>> {
    Ok(source
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?)
}

fn row_counts(rows: &Value) -> TestResult<BTreeMap<String, usize>> {
    let mut counts = BTreeMap::new();
    for row in rows.as_array().ok_or("missing rows")? {
        *counts.entry(serde_json::to_string(row)?).or_default() += 1;
    }
    Ok(counts)
}

fn has_arrow_receipt(actual: &Value, key: &str) -> bool {
    actual[key]
        .as_str()
        .is_some_and(|schema| !schema.is_empty())
}

// The cause is the same, while these two public stage names remain different.
fn matches_preexecution_divide(case: &Value, actual: &Value) -> bool {
    let expected = &case["expected"];
    case["ansi"] == true
        && expected["status"] == "error"
        && expected["condition"] == "DIVIDE_BY_ZERO"
        && expected["phase"] == "optimized_schema"
        && expected["parameters"] == json!({"config":"\"spark.sql.ansi.enabled\""})
        && expected["sql_state"] == "22012"
        && expected.get("physical_schema").is_none()
        && actual["analyzed_schema"] == expected["analyzed_schema"]
        && has_arrow_receipt(actual, "analyzed_arrow_schema")
        && actual.get("physical_schema") == Some(&Value::Null)
        && actual.get("physical_arrow_schema") == Some(&Value::Null)
        && contracts::error_is(actual, "physical", DIVIDE_ERROR)
}

fn matches_collection_error(case: &Value, actual: &Value) -> bool {
    let expected = &case["expected"];
    if case["ansi"] != true
        || !case["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("substring_cse_") && id.ends_with("_pair"))
        || expected["status"] != "error"
        || expected["phase"] != "collect"
        || expected.get("physical_schema").is_some()
        || !expected["optimized_schema"].is_object()
        || !has_arrow_receipt(actual, "physical_arrow_schema")
        || actual["physical_schema"] != expected["optimized_schema"]
    {
        return false;
    }
    match expected["condition"].as_str() {
        Some("CAST_INVALID_INPUT") => {
            expected["sql_state"] == "22018"
                && expected["parameters"]
                    == json!({"expression":"'bad'","sourceType":"\"STRING\"",
                    "targetType":"\"INT\"","ansiConfig":"\"spark.sql.ansi.enabled\""})
                && contracts::error_is(
                    actual,
                    "execute",
                    "execute: [CAST_INVALID_INPUT]\ncaused by\nArrow error: Cast error: Cannot cast string 'bad' to value of Int32 type",
                )
        }
        Some("DIVIDE_BY_ZERO") => {
            expected["sql_state"] == "22012"
                && expected["parameters"] == json!({"config":"\"spark.sql.ansi.enabled\""})
                && contracts::error_is(
                    actual,
                    "execute",
                    "execute: Arrow error: Divide by zero error",
                )
        }
        _ => false,
    }
}

fn matches_meaningful_contract(case: &Value, actual: &Value) -> TestResult<bool> {
    if case["expected"]["condition"] == "UNRESOLVED_ROUTINE" {
        return Ok(matches_array_concat_resolution(case, actual));
    }
    if !contracts::completed_stages_are_retained(actual)
        || !has_arrow_receipt(actual, "analyzed_arrow_schema")
        || ["analyzed_schema", "physical_schema"]
            .into_iter()
            .any(|key| {
                case["expected"]
                    .get(key)
                    .is_some_and(|schema| actual.get(key) != Some(schema))
            })
    {
        return Ok(false);
    }
    let expected = &case["expected"];
    Ok(match expected["status"].as_str() {
        Some("ok") => {
            actual.get("error").is_none()
                && actual.get("error_stage").is_none()
                && actual["analyzed_schema"] == expected["analyzed_schema"]
                && actual["physical_schema"] == expected["physical_schema"]
                && has_arrow_receipt(actual, "physical_arrow_schema")
                && row_counts(&actual["rows"])? == row_counts(&expected["rows"])?
        }
        Some("error") => {
            matches_preexecution_divide(case, actual) || matches_collection_error(case, actual)
        }
        _ => false,
    })
}

fn matches_array_concat_resolution(case: &Value, actual: &Value) -> bool {
    let parameters = json!({
        "routineName":"`array_concat`",
        "searchPath":"[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"
    });
    let message = format!("{UNRESOLVED_ARRAY_CONCAT}; line 1 pos 7");
    case["expected"]
        == json!({
            "status":"error","phase":"sql","condition":"UNRESOLVED_ROUTINE",
            "parameters":parameters,"sql_state":"42883","message":message,
            "python_class":"pyspark.errors.exceptions.captured.AnalysisException",
            "java_causes":[{"class":"org.apache.spark.sql.AnalysisException",
                "message":message,"condition":"UNRESOLVED_ROUTINE","parameters":parameters}]
        })
        && *actual
            == json!({
                "error":format!("resolve: External error: analysis error: {UNRESOLVED_ARRAY_CONCAT}"),
                "error_stage":"resolve","analyzed_schema":null,"physical_schema":null,
                "analyzed_arrow_schema":null,"physical_arrow_schema":null
            })
}

fn context(matrix: &str, case: &Value, representation: usize, batch_size: usize) -> Value {
    json!({"matrix":matrix,"id":case["id"],"ansi":case["ansi"],"sql":case["sql"],
        "representation":representation,"batch_size":batch_size,"input_partitions":2,
        "target_partitions":2,"timezone":"UTC"})
}

fn is_array_concat_reference(case: &Value) -> bool {
    case["id"] == "substring_descendant_array_boundary"
        && case["sql"] == ARRAY_CONCAT_SQL
        && case["expected"]["status"] == "error"
        && case["expected"]["phase"] == "sql"
        && case["expected"]["condition"] == "UNRESOLVED_ROUTINE"
        && case["expected"]["parameters"]["routineName"] == "`array_concat`"
}

fn public_native_observation(actual: &Value) -> TestResult<Value> {
    let mut public = actual
        .as_object()
        .ok_or("missing native observation")?
        .clone();
    // Keep raw storage receipts in fixtures and logs. Internal #n aliases are
    // not the public output names compared by this native regression profile.
    public.remove("analyzed_arrow_schema");
    public.remove("physical_arrow_schema");
    if let Some(rows) = actual.get("rows") {
        public.insert("rows".to_owned(), json!(row_counts(rows)?));
    }
    Ok(Value::Object(public))
}

fn matches_retained_alias(
    context: &Value,
    case: &Value,
    actual: &Value,
    profiles: &[Value],
) -> TestResult<bool> {
    if !is_array_concat_reference(case)
        || matches_meaningful_contract(case, actual)?
        || !contracts::completed_stages_are_retained(actual)
        || !has_arrow_receipt(actual, "analyzed_arrow_schema")
        || !has_arrow_receipt(actual, "physical_arrow_schema")
    {
        return Ok(false);
    }
    let mut matches = profiles
        .iter()
        .filter(|profile| &profile["context"] == context);
    let Some(profile) = matches.next() else {
        return Ok(false);
    };
    let captured = &profile["captured_observation"];
    if matches.next().is_some()
        || profile["strict_meaningful_match"] != false
        || profile["full_compatibility_pass"] != false
        || captured["capture_failure"] != false
        || captured["expected"] != case["expected"]
        || ["id", "ansi", "sql", "representation", "batch_size"]
            .into_iter()
            .any(|key| captured[key] != context[key])
    {
        return Ok(false);
    }
    Ok(public_native_observation(actual)? == public_native_observation(&captured["actual"])?)
}

fn session(batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(2)
                        .with_default_catalog_and_schema("spark_catalog", "default"),
                ),
        )
        .build(),
    )
}

async fn capture_case(
    session: &SessionContext,
    case: &Value,
    registration_error: Option<&str>,
) -> (Value, bool) {
    if let Some(error) = registration_error {
        return (
            json!({"capture_error":error,"capture_stage":"input_registration"}),
            true,
        );
    }
    let task_session = session.clone();
    let task_case = case.clone();
    match tokio::spawn(async move { capture_observation(&task_session, &task_case).await }).await {
        Ok(Ok(actual)) => (actual, false),
        Ok(Err(error)) => (json!({"capture_error":error.to_string()}), true),
        Err(error) => (json!({"capture_task_error":error.to_string()}), true),
    }
}

#[tokio::test]
async fn substring_descendants_retain_all_660_comparisons() -> TestResult {
    let mut reference_keys = HashSet::new();
    let mut sql_statements = HashSet::new();
    let mut expected_contexts = HashSet::new();
    let mut expected_unresolved_contexts = HashSet::new();
    let mut seen_contexts = HashSet::new();
    let mut unresolved_contexts = HashSet::new();
    let mut failures = Vec::new();
    let mut references = 0;
    let mut divide_references = 0;
    let mut observations = 0;
    let mut capture_failures = 0;
    let mut meaningful_matches = 0;
    let mut preexecution_divides = 0;
    let mut unresolved_routines = 0;
    let mut constant_parent_null_matches = 0;
    let mut local_parent_null_matches = 0;
    for (matrix, name, source, count) in FIXTURES {
        let records = records(source)?;
        if records[0]["spark_version"] != "4.2.0" || records.len() != count + 1 {
            failures.push(json!({"fixture":name,"failure":"changed reference header/count"}));
        }
        for case in &records[1..] {
            references += 1;
            let identity = (
                case["id"].as_str().ok_or("missing ID")?.to_owned(),
                case["ansi"].as_bool().ok_or("missing ANSI mode")?,
            );
            if !reference_keys.insert(identity) {
                failures.push(json!({"fixture":name,"failure":"duplicate reference","case":case}));
            }
            sql_statements.insert(case["sql"].as_str().ok_or("missing SQL")?.to_owned());
            divide_references += usize::from(case["expected"]["condition"] == "DIVIDE_BY_ZERO");
            for representation in 0..3 {
                for batch_size in [1, 32] {
                    let key = context(matrix, case, representation, batch_size).to_string();
                    expected_contexts.insert(key.clone());
                    if is_array_concat_reference(case) {
                        expected_unresolved_contexts.insert(key);
                    }
                }
            }
        }
        eprintln!(
            "c06 substring permanent descendant header: {}",
            json!({
            "matrix":matrix,"fixture":name,"spark_capture_header":records[0],
            "native_target_partitions":2,"native_timezone":"UTC",
            "native_crate_cwd":env!("CARGO_MANIFEST_DIR")})
        );
        for representation in 0..3 {
            for batch_size in [1, 32] {
                let session = session(batch_size);
                let registration_error =
                    register_inputs(&session, &records[0], representation, batch_size)
                        .err()
                        .map(|error| error.to_string());
                for case in &records[1..] {
                    let context = context(matrix, case, representation, batch_size);
                    let key = context.to_string();
                    if !seen_contexts.insert(key.clone()) {
                        failures
                            .push(json!({"context":context,"failure":"duplicate native context"}));
                    }
                    let (actual, capture_failure) =
                        capture_case(&session, case, registration_error.as_deref()).await;
                    let comparison: TestResult<(bool, bool, bool)> = (|| {
                        Ok((
                            matches_meaningful_contract(case, &actual)?,
                            matches_preexecution_divide(case, &actual),
                            matches_array_concat_resolution(case, &actual),
                        ))
                    })();
                    let (meaningful, divide, unresolved) = match comparison {
                        Ok(value) => value,
                        Err(error) => {
                            failures.push(
                                json!({"context":context,"comparison_failure":error.to_string()}),
                            );
                            (false, false, false)
                        }
                    };
                    observations += 1;
                    capture_failures += usize::from(capture_failure);
                    meaningful_matches += usize::from(meaningful);
                    preexecution_divides += usize::from(divide);
                    unresolved_routines += usize::from(unresolved);
                    if unresolved {
                        unresolved_contexts.insert(key);
                    }
                    if meaningful && matrix == "parent_null" {
                        if case["id"]
                            .as_str()
                            .is_some_and(|id| id.starts_with("substring_descendant_constant_"))
                        {
                            constant_parent_null_matches += 1;
                        } else {
                            local_parent_null_matches += 1;
                        }
                    }
                    let observation = json!({"context":context,"expected":case["expected"],"actual":actual,
                        "capture_failure":capture_failure,"strict_meaningful_match":meaningful,
                        "preexecution_divide_match":divide,
                        "spark_error_phase":case["expected"].get("phase"),
                        "native_error_stage":actual.get("error_stage"),
                        "public_phase_names_equal":if divide { Some(false) } else { None },
                        "unresolved_routine_match":unresolved,
                        "fields_match_when_reference_available":{
                            "analyzed_schema":case["expected"].get("analyzed_schema").map(|schema| actual.get("analyzed_schema") == Some(schema)),
                            "physical_schema":case["expected"].get("physical_schema").map(|schema| actual.get("physical_schema") == Some(schema))},
                        "ordered_rows_equal_when_available":case["expected"].get("rows").zip(actual.get("rows")).map(|(expected,actual)| expected == actual),
                        "regression_invariant_pass":!capture_failure && meaningful});
                    eprintln!("c06 substring permanent descendant observation: {observation}");
                    if capture_failure || !meaningful {
                        failures.push(observation);
                    }
                }
            }
        }
    }
    let complete = failures.is_empty()
        && references == 110
        && reference_keys.len() == 110
        && sql_statements.len() == 55
        && divide_references == 33
        && observations == 660
        && seen_contexts.len() == 660
        && seen_contexts == expected_contexts
        && capture_failures == 0
        && meaningful_matches == 660
        && preexecution_divides == 198
        && unresolved_routines == 12
        && constant_parent_null_matches == 36
        && local_parent_null_matches == 36
        && unresolved_contexts == expected_unresolved_contexts;
    eprintln!(
        "c06 substring permanent descendant summary: {}",
        json!({
        "references":references,"distinct_sql_statements":sql_statements.len(),
        "observations":observations,"unique_contexts":seen_contexts.len(),"capture_failures":capture_failures,
        "meaningful_matches":meaningful_matches,"named_preexecution_divide_mappings":preexecution_divides,
        "unresolved_routine_matches":unresolved_routines,
        "constant_parent_null_matches":constant_parent_null_matches,"local_parent_null_matches":local_parent_null_matches,
        "full_compatibility_pass":observations == 660 && meaningful_matches == 660 && preexecution_divides == 0,
        "regression_invariant_pass":complete,"unexpected":failures.len()})
    );
    // Disposition and completeness are checked after retaining the entire matrix.
    assert_eq!(references, 110);
    assert_eq!(reference_keys.len(), 110);
    assert!(
        reference_keys
            .iter()
            .all(|(id, ansi)| reference_keys.contains(&(id.clone(), !*ansi)))
    );
    assert_eq!(sql_statements.len(), 55);
    assert_eq!(divide_references, 33);
    assert_eq!(observations, 660);
    assert_eq!(seen_contexts.len(), 660);
    assert_eq!(seen_contexts, expected_contexts);
    assert_eq!(capture_failures, 0);
    assert_eq!(meaningful_matches, 660);
    assert_eq!(preexecution_divides, 198);
    assert_eq!(unresolved_routines, 12);
    assert_eq!(constant_parent_null_matches, 36);
    assert_eq!(local_parent_null_matches, 36);
    assert_eq!(unresolved_contexts, expected_unresolved_contexts);
    assert!(failures.is_empty(), "{failures:#?}");
    Ok(())
}

#[test]
fn descendant_comparisons_reject_changed_fields_rows_causes_and_stages() -> TestResult {
    let controls = records(include_str!(
        "fixtures/substring-descendant-comparison-controls.jsonl"
    ))?;
    assert_eq!(controls[0]["control_count"], 3);
    assert_eq!(controls.len(), 4);
    let references = FIXTURES
        .into_iter()
        .map(|(_, _, source, _)| records(source))
        .collect::<TestResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    for control in &controls[1..] {
        // Every starting schema and outcome is a real native capture, not a Spark overlay.
        let captured = &control["captured_observation"];
        let case = references
            .iter()
            .find(|case| case["id"] == captured["id"] && case["ansi"] == captured["ansi"])
            .ok_or("missing frozen mutation-control reference")?;
        assert_eq!(case["sql"], captured["sql"]);
        assert_eq!(case["expected"], captured["expected"]);
        let actual = &captured["actual"];
        assert!(matches_meaningful_contract(case, actual)?);
        for stage in ["analyzed_schema", "physical_schema"] {
            if let Some(fields) = actual[stage]["fields"].as_array() {
                for (index, field) in fields.iter().enumerate() {
                    for (property, value) in [
                        ("name", json!("wrong_name")),
                        ("type", json!("wrong_type")),
                        (
                            "nullable",
                            json!(!field["nullable"].as_bool().ok_or("missing nullable")?),
                        ),
                        ("metadata", json!({"wrong":"metadata"})),
                    ] {
                        let mut changed = actual.clone();
                        changed[stage]["fields"][index][property] = value;
                        assert!(
                            !matches_meaningful_contract(case, &changed)?,
                            "{stage}/{property}"
                        );
                    }
                }
            }
        }
        for key in ["analyzed_arrow_schema", "physical_arrow_schema"] {
            if actual[key].is_string() {
                let mut changed = actual.clone();
                changed[key] = json!("");
                assert!(!matches_meaningful_contract(case, &changed)?);
            }
        }
        if let Some(rows) = actual["rows"].as_array() {
            assert!(!rows.is_empty());
            let mut missing = actual.clone();
            missing["rows"]
                .as_array_mut()
                .ok_or("missing rows")?
                .remove(0);
            assert!(!matches_meaningful_contract(case, &missing)?);
            let mut duplicate = actual.clone();
            duplicate["rows"]
                .as_array_mut()
                .ok_or("missing rows")?
                .push(rows[0].clone());
            assert!(!matches_meaningful_contract(case, &duplicate)?);
            let mut changed = actual.clone();
            changed["rows"][0][0] = json!("changed_value");
            assert!(!matches_meaningful_contract(case, &changed)?);
            let mut reordered = actual.clone();
            reordered["rows"]
                .as_array_mut()
                .ok_or("missing rows")?
                .reverse();
            assert!(matches_meaningful_contract(case, &reordered)?);
        } else {
            assert!(matches_preexecution_divide(case, actual));
            for (key, value) in [
                ("error", json!("physical: unrelated cause")),
                ("error_stage", json!("execute")),
                ("physical_schema", json!({})),
                ("physical_arrow_schema", json!("fabricated later stage")),
                ("rows", json!([])),
            ] {
                let mut changed = actual.clone();
                changed[key] = value;
                assert!(!matches_meaningful_contract(case, &changed)?, "{key}");
            }
            for (key, value) in [
                ("condition", json!("UNRELATED")),
                ("phase", json!("collect")),
                ("sql_state", json!("00000")),
                ("parameters", json!({"config":"wrong"})),
            ] {
                let mut changed = case.clone();
                changed["expected"][key] = value;
                assert!(!matches_meaningful_contract(&changed, actual)?, "{key}");
            }
        }
    }
    Ok(())
}

#[test]
fn historical_alias_profiles_reject_changed_public_results_and_contexts() -> TestResult {
    let profile_records = records(include_str!(
        "fixtures/substring-descendant-native-negative-profiles.jsonl"
    ))?;
    let references = records(FIXTURES[0].2)?;
    let cases = references[1..]
        .iter()
        .filter(|case| is_array_concat_reference(case))
        .collect::<Vec<_>>();
    assert_eq!(references[0]["spark_version"], "4.2.0");
    assert_eq!(references[0]["timezone"], "UTC");
    assert_eq!(cases.len(), 2);
    assert!(cases.iter().any(|case| case["ansi"] == false));
    assert!(cases.iter().any(|case| case["ansi"] == true));
    let mut expected_contexts = HashSet::new();
    for case in &cases {
        for representation in 0..3 {
            for batch_size in [1, 32] {
                assert!(
                    expected_contexts.insert(
                        context(FIXTURES[0].0, case, representation, batch_size).to_string()
                    )
                );
            }
        }
    }
    let profiles = &profile_records[1..];
    let profile_contexts = profiles
        .iter()
        .map(|profile| profile["context"].to_string())
        .collect::<HashSet<_>>();
    assert_eq!(profile_records[0]["profile_count"], 12);
    assert_eq!(
        profile_records[0]["spark_reference_sha256"],
        "5929b4eaba7f058a23160ef41ac6f216177af27b8f6fa987a5ece87195fbd4b3"
    );
    assert_eq!(profiles.len(), 12);
    assert_eq!(profile_contexts.len(), 12);
    assert_eq!(profile_contexts, expected_contexts);
    for profile in profiles {
        let context = &profile["context"];
        let case = cases
            .iter()
            .copied()
            .find(|case| case["id"] == context["id"] && case["ansi"] == context["ansi"])
            .ok_or("missing authoritative historical alias reference")?;
        let captured = &profile["captured_observation"];
        assert_eq!(captured["expected"], case["expected"]);
        let actual = &captured["actual"];
        assert!(!matches_meaningful_contract(case, actual)?);
        assert!(matches_retained_alias(context, case, actual, profiles)?);
        for stage in ["analyzed_schema", "physical_schema"] {
            for (property, value) in [
                ("name", json!("wrong_name")),
                ("type", json!("wrong_type")),
                ("nullable", json!(false)),
                ("metadata", json!({"wrong":"metadata"})),
            ] {
                let mut changed = actual.clone();
                changed[stage]["fields"][0][property] = value;
                assert!(!matches_retained_alias(context, case, &changed, profiles)?);
            }
            for (property, value) in [
                ("elementType", json!("long")),
                ("containsNull", json!(false)),
            ] {
                let mut changed = actual.clone();
                changed[stage]["fields"][0]["type"][property] = value;
                assert!(!matches_retained_alias(context, case, &changed, profiles)?);
            }
        }
        for (key, value) in [
            ("error", json!("physical: unrelated cause")),
            ("error_stage", json!("physical")),
            ("rows", json!([])),
            ("rows", json!([[null], [null]])),
            ("rows", json!([[null], [null], [null], [null]])),
        ] {
            let mut changed = actual.clone();
            changed[key] = value;
            assert!(!matches_retained_alias(context, case, &changed, profiles)?);
        }
        for key in ["analyzed_arrow_schema", "physical_arrow_schema"] {
            let mut changed = actual.clone();
            changed[key] = json!(format!("{} [internal #999]", actual[key]));
            assert!(matches_retained_alias(context, case, &changed, profiles)?);
            changed[key] = json!("");
            assert!(!matches_retained_alias(context, case, &changed, profiles)?);
        }
        for key in [
            "matrix",
            "id",
            "ansi",
            "sql",
            "representation",
            "batch_size",
            "input_partitions",
            "target_partitions",
            "timezone",
        ] {
            let mut changed = context.clone();
            changed[key] = json!("wrong_context");
            assert!(!matches_retained_alias(&changed, case, actual, profiles)?);
        }
        let mut changed_case = case.clone();
        changed_case["expected"]["condition"] = json!("DIVIDE_BY_ZERO");
        assert!(!matches_retained_alias(
            context,
            &changed_case,
            actual,
            profiles
        )?);
        assert!(!matches_retained_alias(context, case, actual, &[])?);
        let mut changed_profile = profile.clone();
        changed_profile["context"]["target_partitions"] = json!(99);
        assert!(!matches_retained_alias(
            context,
            case,
            actual,
            &[changed_profile]
        )?);
        let mut changed_profile = profile.clone();
        changed_profile["captured_observation"]["expected"]["message"] = json!("wrong message");
        assert!(!matches_retained_alias(
            context,
            case,
            actual,
            &[changed_profile]
        )?);
        let mut duplicates = profiles.to_vec();
        duplicates.push(profile.clone());
        assert!(!matches_retained_alias(context, case, actual, &duplicates)?);
        let mut changed_profiles = profiles.to_vec();
        for changed in &mut changed_profiles {
            changed["strict_meaningful_match"] = json!(true);
        }
        assert!(!matches_retained_alias(
            context,
            case,
            actual,
            &changed_profiles
        )?);
        let mut changed_profiles = profiles.to_vec();
        for changed in &mut changed_profiles {
            changed["captured_observation"]["expected"]["condition"] = json!("UNRELATED");
        }
        assert!(!matches_retained_alias(
            context,
            case,
            actual,
            &changed_profiles
        )?);
    }
    Ok(())
}

#[tokio::test]
async fn unresolved_routine_comparisons_reject_changed_causes_and_available_stages() -> TestResult {
    let references = records(FIXTURES[0].2)?;
    let case = references[1..]
        .iter()
        .find(|case| is_array_concat_reference(case))
        .ok_or("missing ARRAY_CONCAT reference")?;
    let session = session(1);
    register_inputs(&session, &references[0], 0, 1)?;
    let actual = capture_observation(&session, case).await?;
    assert!(matches_meaningful_contract(case, &actual)?, "{actual}");
    for (key, value) in [
        ("error", json!("resolve: unrelated routine")),
        ("error_stage", json!("physical")),
        ("analyzed_schema", json!({})),
        ("analyzed_arrow_schema", json!("fabricated analyzed schema")),
        ("optimized_schema", json!({})),
        ("physical_schema", json!({})),
        ("physical_arrow_schema", json!("fabricated physical schema")),
        ("rows", json!([])),
    ] {
        let mut changed = actual.clone();
        changed[key] = value;
        assert!(!matches_meaningful_contract(case, &changed)?, "{key}");
    }
    for key in actual
        .as_object()
        .ok_or("missing native resolution object")?
        .keys()
    {
        let mut changed = actual.clone();
        changed
            .as_object_mut()
            .ok_or("missing changed observation")?
            .remove(key);
        assert!(!matches_meaningful_contract(case, &changed)?, "{key}");
    }
    for (key, value) in [
        ("condition", json!("UNRELATED")),
        ("phase", json!("optimized_schema")),
        ("sql_state", json!("00000")),
        ("message", json!("unrelated resolution cause")),
        ("python_class", json!("unrelated exception class")),
        ("java_causes", json!([])),
        ("parameters", json!({"routineName":"`other`"})),
        ("analyzed_schema", Value::Null),
        ("optimized_schema", Value::Null),
        ("physical_schema", Value::Null),
        ("rows", json!([])),
    ] {
        let mut changed = case.clone();
        changed["expected"][key] = value;
        assert!(!matches_meaningful_contract(&changed, &actual)?, "{key}");
    }
    for parameter in ["routineName", "searchPath"] {
        let mut changed = case.clone();
        changed["expected"]["parameters"][parameter] = json!("wrong parameter");
        assert!(
            !matches_meaningful_contract(&changed, &actual)?,
            "{parameter}"
        );
    }
    for key in ["class", "message", "condition", "parameters"] {
        let mut changed = case.clone();
        changed["expected"]["java_causes"][0][key] = json!("wrong cause");
        assert!(!matches_meaningful_contract(&changed, &actual)?, "{key}");
    }
    Ok(())
}

const CSE_FIXTURE: &str = include_str!("fixtures/substring-cse-spark.jsonl");
const NULL_ORDER_FIXTURE: &str = include_str!("fixtures/substring-null-order-spark.jsonl");

fn positive_header_matches(metadata: &Value, cse: bool) -> bool {
    metadata["spark_version"] == "4.2.0"
        && metadata["timezone"] == "UTC"
        && (!cse
            || (metadata["probe_runtime_sql_settings"]["spark.sql.subexpressionElimination.enabled"]
                == "true"
                && metadata["spark_context_settings"]["spark.sql.subexpressionElimination.enabled"]
                    == "true"))
}

fn matches_positive_observation(
    context: &Value,
    case: &Value,
    observation: &Value,
) -> TestResult<bool> {
    let namespace = match context["matrix"].as_str() {
        Some("cse") => "substring_cse_",
        Some("null_order") => "substring_null_order_",
        _ => return Ok(false),
    };
    if !case["id"]
        .as_str()
        .is_some_and(|id| id.starts_with(namespace))
        || context["input_partitions"] != 2
        || context["target_partitions"] != 2
        || context["timezone"] != "UTC"
        || !matches!(context["representation"].as_u64(), Some(0..=2))
        || !matches!(context["batch_size"].as_u64(), Some(1 | 32))
        || observation["context"] != *context
        || observation["capture_failure"] != false
        || observation["expected"] != case["expected"]
        || observation["actual"].get("status").is_some()
        || ["id", "ansi", "sql"]
            .into_iter()
            .any(|key| context[key] != case[key])
        || ["id", "ansi", "sql", "representation", "batch_size"]
            .into_iter()
            .any(|key| observation[key] != context[key])
    {
        return Ok(false);
    }
    matches_meaningful_contract(case, &observation["actual"])
}

async fn run_positive_matrix(cse: bool) -> TestResult {
    let (matrix, name, source, reference_count, sql_count, successes, preexecution, collection) =
        if cse {
            (
                "cse",
                "substring-cse-spark.jsonl",
                CSE_FIXTURE,
                16,
                8,
                72,
                0,
                24,
            )
        } else {
            (
                "null_order",
                "substring-null-order-spark.jsonl",
                NULL_ORDER_FIXTURE,
                64,
                32,
                372,
                12,
                0,
            )
        };
    let records = records(source)?;
    let metadata = &records[0];
    let mut failures = Vec::new();
    let mut references = HashSet::new();
    let mut sql_statements = HashSet::new();
    let mut expected_contexts = HashSet::new();
    let mut observed_contexts = HashSet::new();
    for case in &records[1..] {
        let id = case["id"]
            .as_str()
            .ok_or("missing positive-matrix ID")?
            .to_owned();
        let ansi = case["ansi"]
            .as_bool()
            .ok_or("missing positive-matrix ANSI mode")?;
        if !references.insert((id, ansi)) {
            failures.push(json!({"failure":"duplicate positive reference","case":case}));
        }
        sql_statements.insert(
            case["sql"]
                .as_str()
                .ok_or("missing positive-matrix SQL")?
                .to_owned(),
        );
        for representation in 0..3 {
            for batch_size in [1, 32] {
                expected_contexts
                    .insert(context(matrix, case, representation, batch_size).to_string());
            }
        }
    }
    let manifest_path = std::env::var("C06_SUBSTRING_CSE_ORACLE_SOURCE_MANIFEST")
        .or_else(|_| std::env::var("C06_ARGS_SOURCE_MANIFEST"))
        .ok();
    let source_manifest = manifest_path
        .as_ref()
        .map(std::fs::read_to_string)
        .transpose()?
        .map(|source| serde_json::from_str::<Value>(&source))
        .transpose()?;
    eprintln!(
        "c06 substring positive matrix header: {}",
        json!({
        "matrix":matrix,"fixture":name,"spark_capture_header":metadata,
        "spark_actual_cse_sql_receipt":metadata["probe_runtime_sql_settings"]["spark.sql.subexpressionElimination.enabled"],
        "spark_actual_cse_context_receipt":metadata["spark_context_settings"]["spark.sql.subexpressionElimination.enabled"],
        "native_source_manifest_path":manifest_path,"native_source_manifest":source_manifest,
        "native_crate_cwd":env!("CARGO_MANIFEST_DIR"),"native_crate_version":env!("CARGO_PKG_VERSION"),
        "native_capture_helper":"tests/support/string_binary.rs"})
    );
    let mut observations = 0;
    let mut capture_failures = 0;
    let mut matched = 0;
    let mut success_matches = 0;
    let mut preexecution_matches = 0;
    let mut collection_matches = 0;
    let mut cast_collection_matches = 0;
    let mut divide_collection_matches = 0;
    let mut default_cse_sessions = 0;
    for representation in 0..3 {
        for batch_size in [1, 32] {
            let session = session(batch_size);
            let state = session.state();
            let logical_rules = state
                .optimizers()
                .iter()
                .map(|rule| rule.name().to_owned())
                .collect::<Vec<_>>();
            let physical_rules = state
                .physical_optimizers()
                .iter()
                .map(|rule| rule.name().to_owned())
                .collect::<Vec<_>>();
            let default_cse = logical_rules
                .iter()
                .any(|name| name == "common_sub_expression_eliminate");
            default_cse_sessions += usize::from(default_cse);
            eprintln!(
                "c06 substring positive matrix session: {}",
                json!({
                "matrix":matrix,"representation":representation,"batch_size":batch_size,
                "native_default_logical_optimizer_rules":logical_rules,
                "native_default_physical_optimizer_rules":physical_rules,
                "native_optimizer_options":format!("{:?}", state.config_options().optimizer),
                "native_logical_cse_rule_present":default_cse})
            );
            let registration_error =
                register_inputs(&session, metadata, representation, batch_size)
                    .err()
                    .map(|error| error.to_string());
            for case in &records[1..] {
                let context = context(matrix, case, representation, batch_size);
                if !observed_contexts.insert(context.to_string()) {
                    failures.push(
                        json!({"context":context,"failure":"duplicate positive native context"}),
                    );
                }
                let (actual, capture_failure) =
                    capture_case(&session, case, registration_error.as_deref()).await;
                let mut observation = json!({"context":context,"id":case["id"],"ansi":case["ansi"],"sql":case["sql"],
                    "representation":representation,"batch_size":batch_size,"expected":case["expected"],
                    "actual":actual,"capture_failure":capture_failure,"native_logical_cse_rule_present":default_cse});
                let comparison = matches_positive_observation(&context, case, &observation);
                let matches = match comparison {
                    Ok(matches) => matches,
                    Err(error) => {
                        observation["comparison_failure"] = json!(error.to_string());
                        false
                    }
                };
                observations += 1;
                capture_failures += usize::from(capture_failure);
                matched += usize::from(matches);
                success_matches += usize::from(matches && case["expected"]["status"] == "ok");
                preexecution_matches +=
                    usize::from(matches && matches_preexecution_divide(case, &actual));
                collection_matches +=
                    usize::from(matches && matches_collection_error(case, &actual));
                cast_collection_matches += usize::from(
                    matches
                        && matches_collection_error(case, &actual)
                        && case["expected"]["condition"] == "CAST_INVALID_INPUT",
                );
                divide_collection_matches += usize::from(
                    matches
                        && matches_collection_error(case, &actual)
                        && case["expected"]["condition"] == "DIVIDE_BY_ZERO",
                );
                observation["strict_meaningful_match"] = json!(matches);
                observation["spark_error_phase"] = json!(case["expected"].get("phase"));
                observation["native_error_stage"] = json!(actual.get("error_stage"));
                observation["public_phase_names_equal"] = if case["expected"]["status"] == "error" {
                    json!(false)
                } else {
                    Value::Null
                };
                observation["fields_match_when_reference_available"] = json!({
                    "analyzed_schema":case["expected"].get("analyzed_schema").map(|schema| actual.get("analyzed_schema") == Some(schema)),
                    "physical_schema":case["expected"].get("physical_schema").map(|schema| actual.get("physical_schema") == Some(schema)),
                    "native_physical_to_spark_optimized_schema":if case["expected"]["phase"] == "collect" {
                        Some(actual["physical_schema"] == case["expected"]["optimized_schema"]) } else { None }});
                observation["ordered_rows_equal_when_available"] = json!(
                    case["expected"]
                        .get("rows")
                        .zip(actual.get("rows"))
                        .map(|(expected, actual)| expected == actual)
                );
                eprintln!("c06 substring positive matrix observation: {observation}");
                if !matches || capture_failure || !default_cse {
                    failures.push(observation);
                }
            }
        }
    }
    let complete = positive_header_matches(metadata, cse)
        && records.len() == reference_count + 1
        && references.len() == reference_count
        && sql_statements.len() == sql_count
        && observations == reference_count * 6
        && observed_contexts == expected_contexts
        && observed_contexts.len() == observations
        && capture_failures == 0
        && matched == observations
        && success_matches == successes
        && preexecution_matches == preexecution
        && collection_matches == collection
        && cast_collection_matches == (if cse { 12 } else { 0 })
        && divide_collection_matches == (if cse { 12 } else { 0 })
        && default_cse_sessions == 6
        && failures.is_empty();
    eprintln!(
        "c06 substring positive matrix summary: {}",
        json!({
        "matrix":matrix,"references":references.len(),"distinct_sql_statements":sql_statements.len(),
        "observations":observations,"unique_contexts":observed_contexts.len(),"capture_failures":capture_failures,
        "meaningful_matches":matched,"success_matches":success_matches,"named_preexecution_divides":preexecution_matches,
        "exact_collection_errors":collection_matches,"exact_cast_collection_errors":cast_collection_matches,
        "exact_divide_collection_errors":divide_collection_matches,"default_cse_sessions":default_cse_sessions,
        "negative_profiles_used":0,"all_positive_contracts_pass":complete,
        "public_phase_name_equivalence_claimed":false,"unexpected":failures.len()})
    );
    // All contexts and their complete receipts are retained before these checks.
    assert!(positive_header_matches(metadata, cse));
    assert_eq!(records.len(), reference_count + 1);
    assert_eq!(references.len(), reference_count);
    assert!(
        references
            .iter()
            .all(|(id, ansi)| references.contains(&(id.clone(), !*ansi)))
    );
    assert_eq!(sql_statements.len(), sql_count);
    assert_eq!(observations, reference_count * 6);
    assert_eq!(observed_contexts.len(), reference_count * 6);
    assert_eq!(observed_contexts, expected_contexts);
    assert_eq!(capture_failures, 0);
    assert_eq!(matched, reference_count * 6);
    assert_eq!(success_matches, successes);
    assert_eq!(preexecution_matches, preexecution);
    assert_eq!(collection_matches, collection);
    assert_eq!(cast_collection_matches, if cse { 12 } else { 0 });
    assert_eq!(divide_collection_matches, if cse { 12 } else { 0 });
    assert_eq!(default_cse_sessions, 6);
    assert!(failures.is_empty(), "{failures:#?}");
    Ok(())
}

#[tokio::test]
async fn substring_cse_requires_all_96_spark_comparisons() -> TestResult {
    run_positive_matrix(true).await
}

#[tokio::test]
async fn substring_null_order_requires_all_384_spark_comparisons() -> TestResult {
    run_positive_matrix(false).await
}

#[test]
fn positive_matrices_reject_changed_causes_fields_rows_status_and_contexts() -> TestResult {
    for (cse, fixture, control_source, count) in [
        (
            true,
            CSE_FIXTURE,
            include_str!("fixtures/substring-cse-comparison-controls.jsonl"),
            4,
        ),
        (
            false,
            NULL_ORDER_FIXTURE,
            include_str!("fixtures/substring-null-order-comparison-controls.jsonl"),
            3,
        ),
    ] {
        let references = records(fixture)?;
        let controls = records(control_source)?;
        assert_eq!(controls.len(), count + 1);
        assert_eq!(controls[0]["control_count"], count);
        assert!(positive_header_matches(&references[0], cse));
        if cse {
            for key in ["probe_runtime_sql_settings", "spark_context_settings"] {
                let mut changed = references[0].clone();
                changed[key]["spark.sql.subexpressionElimination.enabled"] = json!("false");
                assert!(!positive_header_matches(&changed, true));
            }
        }
        for control in &controls[1..] {
            // The CSE controls retain distinct base/R4 provenance. A selected
            // matching receipt is mutation input, never an all-96 engine result.
            let context = &control["context"];
            let captured = &control["captured_observation"];
            let case = references[1..]
                .iter()
                .find(|case| case["id"] == captured["id"] && case["ansi"] == captured["ansi"])
                .ok_or("missing positive mutation reference")?;
            assert_eq!(case["expected"], captured["expected"]);
            let mut observation = captured.clone();
            observation["context"] = context.clone();
            let actual = &captured["actual"];
            assert!(matches_positive_observation(context, case, &observation)?);
            for stage in ["analyzed_schema", "physical_schema"] {
                if let Some(fields) = actual[stage]["fields"].as_array() {
                    for (index, field) in fields.iter().enumerate() {
                        for (property, value) in [
                            ("name", json!("wrong_name")),
                            ("type", json!("wrong_type")),
                            (
                                "nullable",
                                json!(!field["nullable"].as_bool().ok_or("missing nullability")?),
                            ),
                            ("metadata", json!({"wrong":"metadata"})),
                        ] {
                            let mut changed = observation.clone();
                            changed["actual"][stage]["fields"][index][property] = value;
                            assert!(
                                !matches_positive_observation(context, case, &changed)?,
                                "{stage}/{index}/{property}"
                            );
                        }
                    }
                }
            }
            for key in ["analyzed_arrow_schema", "physical_arrow_schema"] {
                if actual[key].is_string() {
                    let mut changed = observation.clone();
                    changed["actual"][key] = json!("");
                    assert!(!matches_positive_observation(context, case, &changed)?);
                }
            }
            for key in [
                "matrix",
                "id",
                "ansi",
                "sql",
                "representation",
                "batch_size",
                "input_partitions",
                "target_partitions",
                "timezone",
            ] {
                let mut changed = observation.clone();
                changed["context"][key] = json!("wrong_context");
                assert!(!matches_positive_observation(context, case, &changed)?);
            }
            for key in ["id", "ansi", "sql", "representation", "batch_size"] {
                let mut changed = observation.clone();
                changed[key] = json!("wrong_observer_context");
                assert!(!matches_positive_observation(context, case, &changed)?);
            }
            let mut other_matrix = context.clone();
            other_matrix["matrix"] = json!(if cse { "null_order" } else { "cse" });
            let mut changed = observation.clone();
            changed["context"] = other_matrix.clone();
            assert!(!matches_positive_observation(
                &other_matrix,
                case,
                &changed
            )?);
            assert!(!matches_retained_alias(context, case, actual, &[])?);
            let mut changed = observation.clone();
            changed["capture_failure"] = json!(true);
            assert!(!matches_positive_observation(context, case, &changed)?);
            let mut changed = observation.clone();
            changed["actual"]["status"] = json!("wrong_status");
            assert!(!matches_positive_observation(context, case, &changed)?);
            let mut changed_case = case.clone();
            changed_case["expected"]["status"] = json!(if case["expected"]["status"] == "ok" {
                "error"
            } else {
                "ok"
            });
            assert!(!matches_meaningful_contract(&changed_case, actual)?);
            if let Some(rows) = actual["rows"].as_array() {
                assert!(!rows.is_empty());
                for duplicate in [false, true] {
                    let mut changed = observation.clone();
                    let changed_rows = changed["actual"]["rows"]
                        .as_array_mut()
                        .ok_or("missing rows")?;
                    if duplicate {
                        changed_rows.push(rows[0].clone());
                    } else {
                        changed_rows.remove(0);
                    }
                    assert!(!matches_positive_observation(context, case, &changed)?);
                }
                let mut changed = observation.clone();
                changed["actual"]["rows"][0][0] = json!("wrong_value");
                assert!(!matches_positive_observation(context, case, &changed)?);
                let mut changed = observation.clone();
                changed["actual"]["rows"]
                    .as_array_mut()
                    .ok_or("missing rows")?
                    .reverse();
                assert!(matches_positive_observation(context, case, &changed)?);
            } else {
                for (key, value) in [
                    ("error", json!("execute: unrelated cause")),
                    ("error_stage", json!("resolve")),
                    ("rows", json!([])),
                ] {
                    let mut changed = observation.clone();
                    changed["actual"][key] = value;
                    assert!(
                        !matches_positive_observation(context, case, &changed)?,
                        "{key}"
                    );
                }
                for (key, value) in [
                    ("condition", json!("UNRELATED")),
                    ("phase", json!("wrong_stage")),
                    ("sql_state", json!("00000")),
                ] {
                    let mut changed = case.clone();
                    changed["expected"][key] = value;
                    assert!(!matches_meaningful_contract(&changed, actual)?, "{key}");
                }
                for parameter in case["expected"]["parameters"]
                    .as_object()
                    .ok_or("missing error parameters")?
                    .keys()
                {
                    let mut changed = case.clone();
                    changed["expected"]["parameters"][parameter] = json!("wrong_parameter");
                    assert!(
                        !matches_meaningful_contract(&changed, actual)?,
                        "{parameter}"
                    );
                }
                let mut changed = case.clone();
                changed["ansi"] = json!(false);
                assert!(!matches_meaningful_contract(&changed, actual)?);
                if case["expected"]["condition"] == "CAST_INVALID_INPUT" {
                    let cause = actual["error"].as_str().ok_or("missing CAST error")?;
                    for wrong in [
                        cause.replace("'bad'", "'other'"),
                        cause.replace("Int32", "Int64"),
                        cause.replace("[CAST_INVALID_INPUT]", "[UNRELATED]"),
                    ] {
                        let mut changed = observation.clone();
                        changed["actual"]["error"] = json!(wrong);
                        assert!(!matches_positive_observation(context, case, &changed)?);
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn descendant_matrices_keep_all_190_references_and_1140_contexts() -> TestResult {
    let mut sources = FIXTURES
        .into_iter()
        .map(|(matrix, _, source, count)| (matrix, source, count))
        .collect::<Vec<_>>();
    sources.extend([
        ("cse", CSE_FIXTURE, 16),
        ("null_order", NULL_ORDER_FIXTURE, 64),
    ]);
    let mut references = HashSet::new();
    let mut sql_statements = HashSet::new();
    let mut contexts = HashSet::new();
    for (matrix, source, count) in sources {
        let records = records(source)?;
        assert_eq!(records.len(), count + 1);
        for case in &records[1..] {
            let id = case["id"].as_str().ok_or("missing matrix ID")?.to_owned();
            let ansi = case["ansi"].as_bool().ok_or("missing matrix ANSI mode")?;
            assert!(references.insert((matrix, id, ansi)));
            sql_statements.insert(case["sql"].as_str().ok_or("missing matrix SQL")?.to_owned());
            for representation in 0..3 {
                for batch_size in [1, 32] {
                    assert!(
                        contexts
                            .insert(context(matrix, case, representation, batch_size).to_string())
                    );
                }
            }
        }
    }
    assert_eq!(references.len(), 190);
    assert!(
        references
            .iter()
            .all(|(matrix, id, ansi)| references.contains(&(*matrix, id.clone(), !*ansi)))
    );
    assert_eq!(sql_statements.len(), 95);
    assert_eq!(contexts.len(), 1140);
    Ok(())
}
