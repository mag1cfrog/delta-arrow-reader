//! Complete index references. Native stage contracts and diagnostic identifiers are separate facets.
use std::collections::{BTreeMap, BTreeSet, HashSet};

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::configure_spark_sql;
use serde_json::{Value, json};

#[expect(
    dead_code,
    reason = "This target uses full observations rather than capture_result."
)]
#[path = "support/string_binary.rs"]
mod string_binary;
use string_binary::{TestResult, capture_observation, register_inputs};

#[path = "support/regexp_extract_index_native.rs"]
mod index_native;

fn error_cause(actual: &Value, expected: &Value) -> bool {
    let Some(error) = actual["error"].as_str() else {
        return false;
    };
    let parameters = &expected["parameters"];
    match expected["condition"].as_str() {
        Some("CAST_INVALID_INPUT") => parameters["sourceType"] == "\"STRING\""
            && parameters["targetType"] == "\"INT\""
            && parameters["expression"].as_str().is_some_and(|value| {
                error.ends_with(&format!("Arrow error: Cast error: Cannot cast string {value} to value of Int32 type"))
            }),
        Some("INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION") =>
            parameters["sqlExpr"] == "\"CAST(CAST(bad AS INT) AS STRING)\""
                && error.split_once(": ").is_some_and(|(_, cause)| cause == concat!(
                    "External error: error in DataFusion: ",
                    "[INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION]\ncaused by\n",
                    "[CAST_INVALID_INPUT]\ncaused by\n",
                    "Arrow error: Cast error: Cannot cast string 'bad' to value of Int32 type"
                )),
        Some("CAST_OVERFLOW") if parameters["targetType"] == "\"INT\"" => {
            let Some(value) = parameters["value"].as_str() else { return false; };
            match parameters["sourceType"].as_str() {
                Some("\"BIGINT\"") => error.ends_with(&format!(
                    "Arrow error: Cast error: Can't cast value {} to type Int32", value.trim_end_matches('L'))),
                Some("\"FLOAT\"" | "\"DOUBLE\"") => {
                    let native = if value == "Infinity" { "inf" } else { value };
                    error.ends_with(&format!("Arrow error: Cast error: Can't cast value {native} to type Int32"))
                }
                Some(source) if source.starts_with("\"DECIMAL(") => error.ends_with(&format!(
                    "Arrow error: Cast error: value of {} is out of range Int32", value.trim_end_matches("BD"))),
                _ => false,
            }
        }
        Some("INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX") => parameters["functionName"] == "`regexp_extract`"
            && match (parameters["groupCount"].as_str(), parameters["groupIndex"].as_str()) {
                (Some(count), Some(index)) => error.ends_with(&format!(
                    "Spark `regexp_extract` function: Expects group index between 0 and {count}, but got {index}")),
                _ => false,
            },
        _ => false,
    }
}

fn availability(actual: &Value, key: &str) -> &'static str {
    match actual.get(key) {
        None => "absent",
        Some(Value::Null) => "null",
        Some(_) => "available",
    }
}

fn reviewed_error_boundary(actual: &Value, expected: &Value, contract: Option<&Value>) -> bool {
    let Some(contract) = contract.filter(|contract| contract["reviewed"] == true) else {
        return false;
    };
    let Some(stage) = contract["native_stage"]
        .as_str()
        .filter(|stage| matches!(*stage, "resolve" | "physical" | "execute"))
    else {
        return false;
    };
    contract["spark_phase"] == expected["phase"]
        && contract["condition"] == expected["condition"]
        && actual["error_stage"] == stage
        && actual["error"]
            .as_str()
            .is_some_and(|error| error.starts_with(&format!("{stage}: ")))
        && [
            "analyzed_schema",
            "analyzed_arrow_schema",
            "physical_schema",
            "physical_arrow_schema",
            "rows",
        ]
        .into_iter()
        .all(|key| contract["availability"][key] == availability(actual, key))
        && contract["evidence"].is_object()
        && contract["rationale"].is_string()
}

type ContextKey = (String, String, String, bool, usize, usize);

fn context_key(value: &Value) -> TestResult<ContextKey> {
    Ok((
        value["fixture"]
            .as_str()
            .ok_or("missing fixture")?
            .to_owned(),
        value["id"].as_str().ok_or("missing ID")?.to_owned(),
        value["sql"].as_str().ok_or("missing SQL")?.to_owned(),
        value["ansi"].as_bool().ok_or("missing ANSI")?,
        value["representation"]
            .as_u64()
            .ok_or("missing representation")? as usize,
        value["batch_size"].as_u64().ok_or("missing batch size")? as usize,
    ))
}

fn by_context(records: impl IntoIterator<Item = Value>) -> TestResult<BTreeMap<ContextKey, Value>> {
    let mut result = BTreeMap::new();
    for record in records {
        if result.insert(context_key(&record)?, record).is_some() {
            return Err("duplicate profile context".into());
        }
    }
    Ok(result)
}

fn reference_cases() -> TestResult<BTreeMap<(String, String, bool), Value>> {
    let mut result = BTreeMap::new();
    for (fixture, source) in [
        (
            "regexp-extract-index-original-spark.jsonl",
            include_str!("fixtures/regexp-extract-index-original-spark.jsonl"),
        ),
        (
            "regexp-extract-index-expanded-spark.jsonl",
            include_str!("fixtures/regexp-extract-index-expanded-spark.jsonl"),
        ),
        (
            "regexp-extract-index-cast-controls-spark.jsonl",
            include_str!("fixtures/regexp-extract-index-cast-controls-spark.jsonl"),
        ),
        (
            "regexp-extract-index-cse-spark.jsonl",
            include_str!("fixtures/regexp-extract-index-cse-spark.jsonl"),
        ),
        (
            "regexp-extract-index-local-frontier-spark.jsonl",
            include_str!("fixtures/regexp-extract-index-local-frontier-spark.jsonl"),
        ),
        (
            "regexp-extract-index-foldable-null-spark.jsonl",
            include_str!("fixtures/regexp-extract-index-foldable-null-spark.jsonl"),
        ),
    ] {
        for line in source.lines().skip(1) {
            let case: Value = serde_json::from_str(line)?;
            let key = (
                fixture.to_owned(),
                case["id"]
                    .as_str()
                    .ok_or("missing reference ID")?
                    .to_owned(),
                case["ansi"].as_bool().ok_or("missing reference ANSI")?,
            );
            assert!(result.insert(key, case).is_none(), "duplicate reference");
        }
    }
    assert_eq!(result.len(), 168);
    Ok(result)
}

fn reference_comparison(actual: &Value, expected: &Value, contract: Option<&Value>) -> Value {
    let error = expected["status"] == "error";
    let mut facets = json!({
        "status":actual["error"].is_string() == error
            && actual.get("capture_error").is_none() && actual.get("capture_task_error").is_none(),
        "rows":if error {actual.get("rows").is_none()} else {actual.get("rows") == expected.get("rows")},
    });
    for key in ["analyzed_schema", "physical_schema"] {
        facets[key] = expected
            .get(key)
            .map_or(json!("reference_unavailable"), |field| {
                json!(actual.get(key) == Some(field))
            });
    }
    if error {
        facets["meaningful_cause"] = json!(error_cause(actual, expected));
        facets["native_boundary"] = json!(reviewed_error_boundary(actual, expected, contract));
        let stage = match expected["phase"].as_str() {
            Some("sql") => "resolve",
            Some("optimized_schema") => "physical",
            Some("collect") => "execute",
            _ => "unknown Spark phase",
        };
        facets["phase"] = json!(actual["error_stage"] == stage);
        facets["diagnostic_identifier"] =
            json!(expected["condition"].as_str().is_some_and(|condition| {
                actual["error"]
                    .as_str()
                    .is_some_and(|message| message.contains(&format!("[{condition}]")))
            }));
    }
    let analyzed =
        actual["analyzed_schema"].is_object() && actual["analyzed_arrow_schema"].is_string();
    let physical =
        actual["physical_schema"].is_object() && actual["physical_arrow_schema"].is_string();
    let null_fields = |keys: &[&str]| {
        keys.iter()
            .all(|key| actual.get(*key).is_some_and(Value::is_null))
    };
    let completed = match actual["error_stage"].as_str() {
        Some("resolve") => {
            null_fields(&[
                "analyzed_schema",
                "analyzed_arrow_schema",
                "physical_schema",
                "physical_arrow_schema",
            ]) && actual.get("rows").is_none()
        }
        Some("physical") => {
            analyzed
                && null_fields(&["physical_schema", "physical_arrow_schema"])
                && actual.get("rows").is_none()
        }
        Some("execute") => analyzed && physical && actual.get("rows").is_none(),
        None => analyzed && physical && actual["rows"].is_array(),
        Some(_) => false,
    };
    let keys: BTreeSet<_> = [
        "analyzed_schema",
        "analyzed_arrow_schema",
        "physical_schema",
        "physical_arrow_schema",
    ]
    .into_iter()
    .chain(if actual.get("error").is_some() {
        vec!["error", "error_stage"]
    } else {
        vec!["rows"]
    })
    .collect();
    let shape = actual
        .as_object()
        .is_some_and(|object| object.keys().map(String::as_str).collect::<BTreeSet<_>>() == keys);
    let evidence = json!({"completed_stages":completed,"primary_shape":shape});
    let strict = facets
        .as_object()
        .is_some_and(|facets| facets.values().all(|value| value != false))
        && completed
        && shape;
    let mut available = json!({});
    for key in [
        "analyzed_schema",
        "analyzed_arrow_schema",
        "physical_schema",
        "physical_arrow_schema",
        "rows",
        "optimized_schema",
    ] {
        available[key] =
            json!({"spark_reference":availability(expected,key),"actual":availability(actual,key)});
    }
    json!({"facets":facets,"evidence":evidence,"availability":available,"strict_compatibility":strict,"full_compatibility":false})
}

fn false_facets(comparison: &Value) -> TestResult<Vec<String>> {
    let mut result = Vec::new();
    for group in ["facets", "evidence"] {
        let facets = comparison[group]
            .as_object()
            .ok_or("missing or malformed comparison group")?;
        result.extend(
            facets
                .iter()
                .filter(|(_, value)| value.as_bool() == Some(false))
                .map(|(key, _)| {
                    if group == "facets" {
                        key.clone()
                    } else {
                        format!("evidence.{key}")
                    }
                }),
        );
    }
    result.sort();
    Ok(result)
}

fn retained_matches(profile: &Value, context: &Value, actual: &Value, comparison: &Value) -> bool {
    profile["reviewed"] == true
        && profile["strict_compatibility"] == false
        && profile["full_compatibility"] == false
        && profile["accepted_as_compatibility"] == false
        && comparison["strict_compatibility"] == false
        && comparison["full_compatibility"] == false
        && context_key(profile).ok() == context_key(context).ok()
        && context_key(context).is_ok()
        && profile["actual"] == *actual
        && profile["comparison"] == *comparison
        && false_facets(comparison).is_ok_and(|facets| profile["false_facets"] == json!(facets))
}

fn load_contracts() -> TestResult<BTreeMap<ContextKey, Value>> {
    let source: Value = serde_json::from_str(include_str!(
        "fixtures/regexp-extract-index-current-stages.json"
    ))?;
    let contracts = by_context(
        source["contracts"]
            .as_array()
            .ok_or("missing native contracts")?
            .iter()
            .cloned(),
    )?;
    let mut required = BTreeSet::new();
    for ((fixture, id, ansi), case) in reference_cases()? {
        if case["expected"]["status"] != "error" {
            continue;
        }
        for representation in 0..3 {
            for batch_size in [1, 32] {
                required.insert((
                    fixture.clone(),
                    id.clone(),
                    case["sql"].as_str().ok_or("missing SQL")?.to_owned(),
                    ansi,
                    representation,
                    batch_size,
                ));
            }
        }
    }
    assert_eq!(required.len(), 342);
    assert_eq!(contracts.keys().cloned().collect::<BTreeSet<_>>(), required);
    Ok(contracts)
}

fn load_negatives() -> TestResult<BTreeMap<ContextKey, Value>> {
    let mut lines = include_str!("fixtures/regexp-extract-index-current-negative.jsonl").lines();
    let header: Value = serde_json::from_str(lines.next().ok_or("missing negative header")?)?;
    assert_eq!(
        header["kind"],
        "complete-native-negative-observations-not-spark-oracle"
    );
    assert_eq!(header["acceptance_waivers"], false);
    let profiles = by_context(
        lines
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?,
    )?;
    assert_eq!(
        profiles.len() as u64,
        header["observations"]
            .as_u64()
            .ok_or("missing negative count")?
    );
    let references = reference_cases()?;
    let contracts = load_contracts()?;
    let mut owner_counts = BTreeMap::<&str, usize>::new();
    for (key, profile) in &profiles {
        let reference = references
            .get(&(key.0.clone(), key.1.clone(), key.3))
            .ok_or("unknown negative reference")?;
        assert_eq!(profile["sql"], reference["sql"]);
        let comparison = reference_comparison(
            &profile["actual"],
            &reference["expected"],
            contracts.get(key),
        );
        assert!(retained_matches(
            profile,
            profile,
            &profile["actual"],
            &comparison
        ));
        let facets = false_facets(&comparison)?;
        assert!(
            facets
                .iter()
                .all(|facet| ["diagnostic_identifier", "phase"].contains(&facet.as_str()))
        );
        let owners = profile["facet_owners"]
            .as_object()
            .ok_or("missing facet owners")?;
        assert_eq!(
            owners.keys().cloned().collect::<BTreeSet<_>>(),
            facets.into_iter().collect::<BTreeSet<_>>()
        );
        for owner in owners.values() {
            let owner = owner.as_str().ok_or("malformed facet owner")?;
            assert!(["#922", "#788"].contains(&owner));
            *owner_counts.entry(owner).or_default() += 1;
        }
    }
    assert_eq!(json!(owner_counts), header["owner_facets"]);
    Ok(profiles)
}

#[test]
fn extraction_index_cause_keeps_the_exact_value_type_and_function() {
    let expected = json!({"condition":"CAST_OVERFLOW","parameters":{
        "sourceType":"\"BIGINT\"","targetType":"\"INT\"","value":"4294967296L"}});
    let actual = json!({"error":"physical: Arrow error: Cast error: Can't cast value 4294967296 to type Int32"});
    assert!(error_cause(&actual, &expected));
    for message in [
        "Arrow error: Cast error: Can't cast value 4294967297 to type Int32",
        "Arrow error: Cast error: Can't cast value 4294967296 to type Int64",
        "arbitrary overflow",
    ] {
        assert!(!error_cause(&json!({"error":message}), &expected));
    }
    let group = json!({"condition":"INVALID_PARAMETER_VALUE.REGEX_GROUP_INDEX","parameters":{
        "functionName":"`regexp_extract`","groupCount":"1","groupIndex":"-2147483648"}});
    assert!(error_cause(
        &json!({"error":"execute: Execution error: Spark `regexp_extract` function: Expects group index between 0 and 1, but got -2147483648"}),
        &group
    ));
    assert!(!error_cause(
        &json!({"error":"execute: Execution error: Spark `regexp_extract_all` function: Expects group index between 0 and 1, but got -2147483648"}),
        &group
    ));
    assert!(!reviewed_error_boundary(&actual, &expected, None));
}

#[test]
fn index_profiles_reject_changed_data_diagnostics_and_inventories() -> TestResult {
    let profiles = load_negatives()?;
    let contracts = load_contracts()?;
    let references = reference_cases()?;
    let overflow = profiles
        .values()
        .find(|profile| profile["id"] == "index_limit_zero_local" && profile["ansi"] == true)
        .ok_or("missing measured overflow")?;
    let historical = by_context(
        include_str!("fixtures/regexp-extract-index-native-negative.jsonl")
            .lines()
            .skip(1)
            .map(serde_json::from_str)
            .collect::<Result<Vec<Value>, _>>()?,
    )?;
    let inline = historical
        .values()
        .find(|profile| profile["id"] == "bad_values_extract_empty")
        .ok_or("missing measured inline-table error")?;
    let group = profiles
        .values()
        .find(|profile| profile["id"] == "index_long_negative_group")
        .ok_or("missing measured group error")?;
    for group in ["facets", "evidence"] {
        for malformed in [Value::Null, json!([])] {
            let mut comparison = overflow["comparison"].clone();
            comparison[group] = malformed;
            assert!(false_facets(&comparison).is_err());
            let mut profile = overflow.clone();
            profile["comparison"] = comparison.clone();
            assert!(!retained_matches(
                &profile,
                overflow,
                &overflow["actual"],
                &comparison
            ));
        }
        let mut comparison = overflow["comparison"].clone();
        comparison
            .as_object_mut()
            .ok_or("missing measured comparison")?
            .remove(group);
        assert!(false_facets(&comparison).is_err());
    }
    let compare = |profile: &Value, actual: &Value| -> TestResult<Value> {
        let key = context_key(profile)?;
        let reference = references
            .get(&(key.0.clone(), key.1.clone(), key.3))
            .ok_or("missing frozen reference")?;
        Ok(reference_comparison(
            actual,
            &reference["expected"],
            contracts.get(&key),
        ))
    };
    assert_eq!(
        compare(inline, &inline["actual"])?["facets"]["meaningful_cause"],
        false
    );
    assert_eq!(
        compare(inline, &inline["actual"])?["strict_compatibility"],
        false
    );
    let corrected_inline = json!({
        "error":"resolve: External error: error in DataFusion: [INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION]\ncaused by\n[CAST_INVALID_INPUT]\ncaused by\nArrow error: Cast error: Cannot cast string 'bad' to value of Int32 type",
        "error_stage":"resolve",
        "analyzed_schema":null,"analyzed_arrow_schema":null,
        "physical_schema":null,"physical_arrow_schema":null
    });
    assert_eq!(
        compare(inline, &corrected_inline)?["strict_compatibility"],
        true
    );
    for (before, after) in [
        (
            "[INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION]\ncaused by\n",
            "",
        ),
        (
            "INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION",
            "INVALID_INLINE_TABLE.WRONG_CONDITION",
        ),
        ("[CAST_INVALID_INPUT]\ncaused by\n", ""),
        (
            "External error: error in DataFusion: ",
            "arbitrary wrapper\ncaused by\nExternal error: error in DataFusion: ",
        ),
        ("'bad'", "'other'"),
        ("Int32", "Int64"),
    ] {
        let mut actual = corrected_inline.clone();
        actual["error"] = json!(actual["error"].as_str().unwrap().replace(before, after));
        let comparison = compare(inline, &actual)?;
        assert_eq!(comparison["facets"]["meaningful_cause"], false);
        assert_eq!(comparison["strict_compatibility"], false);
    }
    for (field, replacement) in [
        ("error_stage", json!("physical")),
        ("analyzed_schema", json!({"type":"struct","fields":[]})),
        ("physical_schema", json!({"type":"struct","fields":[]})),
        ("rows", json!([])),
    ] {
        let mut actual = corrected_inline.clone();
        actual[field] = replacement;
        assert_eq!(compare(inline, &actual)?["strict_compatibility"], false);
    }
    for field in [
        "error_stage",
        "analyzed_schema",
        "analyzed_arrow_schema",
        "physical_schema",
        "physical_arrow_schema",
    ] {
        let mut actual = corrected_inline.clone();
        actual.as_object_mut().unwrap().remove(field);
        assert_eq!(compare(inline, &actual)?["strict_compatibility"], false);
    }
    for (profile, before, after) in [
        (overflow, "4294967296", "4294967297"),
        (overflow, "Int32", "Int64"),
        (group, "regexp_extract", "regexp_extract_all"),
    ] {
        let mut actual = profile["actual"].clone();
        actual["error"] = json!(actual["error"].as_str().unwrap().replace(before, after));
        assert!(!retained_matches(
            profile,
            profile,
            &actual,
            &compare(profile, &actual)?
        ));
    }
    for key in ["error_stage", "analyzed_arrow_schema", "physical_schema"] {
        let mut actual = overflow["actual"].clone();
        actual[key] = json!("changed observed value");
        assert!(!retained_matches(
            overflow,
            overflow,
            &actual,
            &compare(overflow, &actual)?
        ));
    }
    let mut missing = overflow["actual"].clone();
    missing.as_object_mut().unwrap().remove("physical_schema");
    assert!(!retained_matches(
        overflow,
        overflow,
        &missing,
        &compare(overflow, &missing)?
    ));
    let mut arbitrary = overflow["actual"].clone();
    arbitrary["error"] = json!("physical: arbitrary overflow");
    assert!(!retained_matches(
        overflow,
        overflow,
        &arbitrary,
        &compare(overflow, &arbitrary)?
    ));
    let mut identified = overflow["actual"].clone();
    identified["error"] = json!(format!(
        "physical: [CAST_OVERFLOW] {}",
        identified["error"].as_str().unwrap()
    ));
    assert_eq!(
        compare(overflow, &overflow["actual"])?["facets"]["diagnostic_identifier"],
        false
    );
    assert_eq!(
        compare(overflow, &identified)?["facets"]["diagnostic_identifier"],
        true
    );
    assert!(!retained_matches(
        overflow,
        overflow,
        &identified,
        &compare(overflow, &identified)?
    ));
    for (key, replacement) in [
        ("fixture", json!("unknown.jsonl")),
        ("id", json!("unknown")),
        ("sql", json!("SELECT 1")),
        ("ansi", json!(false)),
        ("representation", json!(9)),
        ("batch_size", json!(9)),
    ] {
        let mut context = overflow.clone();
        context[key] = replacement;
        assert!(!retained_matches(
            overflow,
            &context,
            &overflow["actual"],
            &compare(overflow, &overflow["actual"])?
        ));
    }
    assert!(by_context([overflow.clone(), overflow.clone()]).is_err());

    let inputs: Value = serde_json::from_str(include_str!(
        "fixtures/regexp-extract-index-selection-inputs.json"
    ))?;
    let native: Value = serde_json::from_str(include_str!(
        "fixtures/regexp-extract-index-selection-native.json"
    ))?;
    let cases = inputs["cases"].as_array().ok_or("missing native inputs")?;
    let controls = native["controls"]
        .as_array()
        .ok_or("missing native profiles")?;
    let keys = &["kind", "folding", "surface", "ansi"];
    let exact = index_native::native_controls(cases, controls, keys)?;
    let control = controls
        .iter()
        .find(|control| {
            !control["actual"]["invocations"]
                .as_array()
                .unwrap()
                .is_empty()
        })
        .ok_or("missing measured callback")?;
    let case = cases
        .iter()
        .find(|case| {
            case["sql"] == control["sql"]
                && case["ansi"] == control["ansi"]
                && case["folding"] == control["folding"]
                && case["surface"] == control["surface"]
        })
        .ok_or("missing callback input")?;
    assert!(index_native::native_matches(
        control,
        case,
        &control["actual"]
    ));
    assert_eq!(exact.len(), 32);
    for field in ["name", "type", "nullable", "metadata"] {
        let mut actual = control["actual"].clone();
        actual["physical_fields"][0][field] = match field {
            "nullable" => json!(!actual["physical_fields"][0][field].as_bool().unwrap()),
            "metadata" => json!({"unexpected":"metadata"}),
            _ => json!("changed"),
        };
        assert!(!index_native::native_matches(control, case, &actual));
    }
    for key in [
        "rows",
        "invocations",
        "physical_arrow_schema",
        "after_physical",
    ] {
        let mut actual = control["actual"].clone();
        actual[key] = json!("changed observed value");
        assert!(!index_native::native_matches(control, case, &actual));
    }
    let mut changed = controls.clone();
    changed[0]["sql"] = json!("SELECT 1");
    assert!(index_native::native_controls(cases, &changed, keys).is_err());
    assert!(index_native::native_controls(cases, &controls[1..], keys).is_err());
    let mut extra = controls.clone();
    extra.push(controls[0].clone());
    assert!(index_native::native_controls(cases, &extra, keys).is_err());
    extra.last_mut().unwrap()["kind"] = json!("unknown");
    assert!(index_native::native_controls(cases, &extra, keys).is_err());
    Ok(())
}

#[tokio::test]
async fn regexp_extract_index_retains_all_complete_references() -> TestResult {
    capture_references(
        &[
            (
                "original_bigint",
                "regexp-extract-index-original-spark.jsonl",
                include_str!("fixtures/regexp-extract-index-original-spark.jsonl"),
                10,
            ),
            (
                "expanded",
                "regexp-extract-index-expanded-spark.jsonl",
                include_str!("fixtures/regexp-extract-index-expanded-spark.jsonl"),
                106,
            ),
            (
                "direct_cast",
                "regexp-extract-index-cast-controls-spark.jsonl",
                include_str!("fixtures/regexp-extract-index-cast-controls-spark.jsonl"),
                10,
            ),
        ],
        126,
        756,
    )
    .await
}

#[tokio::test]
async fn regexp_extract_index_retains_distinct_shared_cast_references() -> TestResult {
    capture_references(
        &[(
            "shared_cast",
            "regexp-extract-index-cse-spark.jsonl",
            include_str!("fixtures/regexp-extract-index-cse-spark.jsonl"),
            8,
        )],
        8,
        48,
    )
    .await
}

#[tokio::test]
async fn regexp_extract_index_retains_local_input_and_null_boundaries() -> TestResult {
    capture_references(
        &[(
            "local_frontier",
            "regexp-extract-index-local-frontier-spark.jsonl",
            include_str!("fixtures/regexp-extract-index-local-frontier-spark.jsonl"),
            18,
        )],
        18,
        108,
    )
    .await
}

#[tokio::test]
async fn regexp_extract_index_retains_bare_and_local_known_null_references() -> TestResult {
    capture_references(
        &[(
            "foldable_null",
            "regexp-extract-index-foldable-null-spark.jsonl",
            include_str!("fixtures/regexp-extract-index-foldable-null-spark.jsonl"),
            16,
        )],
        16,
        96,
    )
    .await
}

#[tokio::test]
async fn regexp_extract_index_checks_aliased_local_indices_before_limit_zero() -> TestResult {
    let records = include_str!("fixtures/regexp-extract-index-expanded-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let (header, cases) = records.split_first().ok_or("empty reference fixture")?;
    assert_eq!(header["spark_version"], "4.2.0");
    let selected = [
        "index_limit_zero_local",
        "index_empty_local",
        "index_int_zero",
        "index_int_one",
        "index_long_null_source",
        "index_long_null_pattern",
    ];
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(32)
                        .with_target_partitions(2),
                ),
        )
        .build(),
    );
    register_inputs(&session, header, 0, 32)?;
    let mut checked = HashSet::new();
    for case in cases {
        let id = case["id"].as_str().ok_or("missing reference ID")?;
        if !selected.contains(&id) {
            continue;
        }
        let ansi = case["ansi"].as_bool().ok_or("missing ANSI mode")?;
        assert!(checked.insert((id, ansi)), "duplicate reference");
        let expected = &case["expected"];
        let actual = capture_observation(&session, case).await?;
        assert_eq!(
            actual["analyzed_schema"], expected["analyzed_schema"],
            "{id} ANSI={ansi}"
        );
        assert!(
            actual["analyzed_arrow_schema"]
                .as_str()
                .is_some_and(|raw| !raw.is_empty())
        );
        let identifier_match = if id == "index_limit_zero_local" && ansi {
            assert_eq!(expected["status"], "error");
            assert_eq!(expected["phase"], "optimized_schema");
            assert_eq!(expected["condition"], "CAST_OVERFLOW");
            assert_eq!(
                expected["parameters"],
                json!({
                    "value":"4294967296L","sourceType":"\"BIGINT\"","targetType":"\"INT\"",
                    "ansiConfig":"\"spark.sql.ansi.enabled\""
                })
            );
            // This measured native boundary is independent of the stage profiles.
            assert_eq!(actual["error_stage"], "physical");
            assert_eq!(
                actual["error"],
                "physical: preserve_spark_expression_errors\ncaused by\nArrow error: Cast error: Can't cast value 4294967296 to type Int32"
            );
            assert!(error_cause(&actual, expected));
            assert_eq!(availability(&actual, "physical_schema"), "null");
            assert_eq!(availability(&actual, "physical_arrow_schema"), "null");
            assert_eq!(availability(&actual, "rows"), "absent");
            // The missing Spark identifier remains a strict diagnostic difference under #922.
            actual["error"]
                .as_str()
                .is_some_and(|error| error.contains("[CAST_OVERFLOW]"))
        } else {
            assert_eq!(expected["status"], "ok");
            assert!(actual.get("error").is_none() && actual.get("error_stage").is_none());
            assert_eq!(
                actual["physical_schema"], expected["physical_schema"],
                "{id} ANSI={ansi}"
            );
            assert!(
                actual["physical_arrow_schema"]
                    .as_str()
                    .is_some_and(|raw| !raw.is_empty())
            );
            assert!(actual.get("rows").is_some());
            assert_eq!(actual.get("rows"), expected.get("rows"), "{id} ANSI={ansi}");
            let comparison = reference_comparison(&actual, expected, None);
            assert_eq!(comparison["strict_compatibility"], true);
            assert_eq!(comparison["full_compatibility"], false);
            true
        };
        eprintln!(
            "c06 alias boundary observation: {}",
            json!({
                "id":id,"ansi":ansi,"sql":case["sql"],"actual":actual,
                "diagnostic_identifier_match":identifier_match,"strict_reference_match":identifier_match,
                "full_compatibility_pass":false
            })
        );
    }
    assert_eq!(checked.len(), selected.len() * 2);
    Ok(())
}

async fn capture_references(
    fixtures: &[(&str, &str, &str, usize)],
    expected_references: usize,
    expected_observations: usize,
) -> TestResult {
    let contracts = load_contracts()?;
    let mut profiles = load_negatives()?;
    profiles.retain(|key, _| fixtures.iter().any(|(_, fixture, _, _)| key.0 == *fixture));
    let mut references = 0;
    let mut observations = 0;
    let mut capture_failures = 0;
    let mut strict_matches = 0;
    let mut differences = Vec::new();
    for &(family, fixture, source, expected_count) in fixtures {
        let records = source
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        let (header, cases) = records.split_first().ok_or("empty reference fixture")?;
        assert_eq!(header["spark_version"], "4.2.0");
        assert_eq!(cases.len(), expected_count);
        let mut identities = HashSet::new();
        for case in cases {
            assert!(
                identities.insert((case["id"].as_str(), case["ansi"].as_bool())),
                "duplicate reference"
            );
        }
        references += cases.len();
        eprintln!(
            "c06 index regression header: {}",
            json!({"family":family,"fixture":fixture,
            "spark_capture_header":header,"native_stage_contracts":contracts.len(),"native_representations":["Utf8","LargeUtf8","Utf8View"],
            "native_batch_sizes":[1,32],"native_target_partitions":2,"native_session_timezone":"UTC"})
        );
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
                register_inputs(&session, header, representation, batch_size)?;
                for case in cases {
                    let session = session.clone();
                    let input = json!({"sql":case["sql"],"ansi":case["ansi"]});
                    let actual =
                        match tokio::spawn(
                            async move { capture_observation(&session, &input).await },
                        )
                        .await
                        {
                            Ok(Ok(actual)) => actual,
                            Ok(Err(error)) => {
                                capture_failures += 1;
                                json!({"capture_error":error.to_string()})
                            }
                            Err(error) => {
                                capture_failures += 1;
                                json!({"capture_task_error":error.to_string()})
                            }
                        };
                    let expected = &case["expected"];
                    let context = json!({"fixture":fixture,"id":case["id"],"sql":case["sql"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size});
                    let key = context_key(&context)?;
                    let contract = contracts.get(&key);
                    let comparison = reference_comparison(&actual, expected, contract);
                    let strict = comparison["strict_compatibility"] == true;
                    let profile = if strict { None } else { profiles.remove(&key) };
                    let preserved = profile.as_ref().is_some_and(|profile| {
                        retained_matches(profile, &context, &actual, &comparison)
                    });
                    observations += 1;
                    strict_matches += usize::from(strict);
                    eprintln!(
                        "c06 index regression observation: {}",
                        json!({"family":family,"fixture":fixture,
                        "id":case["id"],"ansi":case["ansi"],"representation":representation,"batch_size":batch_size,"sql":case["sql"],
                        "expected":expected,"actual":actual,"comparison":comparison,
                        "strict_reference_match":strict,"known_negative_preserved":preserved,
                        "false_facets":false_facets(&comparison)?,"facet_owners":profile.as_ref().map(|profile| &profile["facet_owners"]),
                        "full_compatibility_pass":false})
                    );
                    if !strict && !preserved {
                        differences.push(format!("{fixture}: {} ANSI={} representation={representation} batch={batch_size}: {actual}", case["id"],case["ansi"]));
                    }
                }
            }
        }
    }
    assert_eq!(references, expected_references);
    assert_eq!(observations, expected_observations);
    eprintln!(
        "c06 index regression summary: {}",
        json!({"references":references,"observations":observations,
        "capture_failures":capture_failures,"strict_matches":strict_matches,"strict_differences":observations-strict_matches,"unexpected_differences":differences.len(),
        "full_compatibility_pass":false,"complete_raw_diagnostic_parity":"unverified"})
    );
    assert_eq!(capture_failures, 0);
    assert!(
        profiles.is_empty(),
        "unused negative profiles: {:?}",
        profiles.keys()
    );
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
