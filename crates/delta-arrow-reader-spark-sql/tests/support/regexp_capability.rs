//! Exact recorded limits for the two Spark regex adapters.

use std::collections::HashSet;
use std::sync::OnceLock;

use serde_json::{Value, json};

#[allow(
    dead_code,
    reason = "Regex tests reuse complete-result and completed-stage checks only."
)]
#[path = "string_binary_contracts.rs"]
mod contracts;
pub(super) use contracts::{complete_match, completed_stages_are_retained};

const SPARK_FIELDS: &[&str] = &[
    "status",
    "analyzed_schema",
    "physical_schema",
    "rows",
    "phase",
    "condition",
    "parameters",
    "sql_state",
    "java_causes",
];
const NATIVE_FIELDS: &[&str] = &[
    "analyzed_schema",
    "physical_schema",
    "rows",
    "error",
    "error_stage",
];

fn selected_fields(value: &Value, fields: &[&str]) -> Value {
    Value::Object(
        fields
            .iter()
            .filter_map(|key| value.get(*key).map(|value| ((*key).into(), value.clone())))
            .collect(),
    )
}

fn validate_profiles(profiles: Vec<Value>) -> Result<Vec<Value>, String> {
    let keys = profiles
        .iter()
        .map(|profile| profile["key"].to_string())
        .collect::<HashSet<_>>();
    if profiles.len() != 516 || keys.len() != profiles.len() {
        return Err("recorded regex profile inventory requires 516 unique context keys".into());
    }
    Ok(profiles)
}

fn profiles() -> Result<&'static [Value], &'static str> {
    static PROFILES: OnceLock<Result<Vec<Value>, String>> = OnceLock::new();
    PROFILES
        .get_or_init(|| {
            include_str!("../fixtures/regex-native-limit-profiles.jsonl")
                .split('\n')
                .skip(1)
                .filter(|line| !line.is_empty())
                .map(serde_json::from_str)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("recorded regex profile JSON: {error}"))
                .and_then(validate_profiles)
        })
        .as_deref()
        .map_err(String::as_str)
}

fn profile_matches(
    profile: &Value,
    fixture: &str,
    case: &Value,
    representation: usize,
    batch_size: usize,
    actual: Option<&Value>,
) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    completed_stages_are_retained(actual)
        && !complete_match(actual, &case["expected"])
        && actual.get("capture_failure").is_none()
        && actual.get("capture_failure_kind").is_none()
        && actual.get("capture_error").is_none()
        && actual
            .get("native_optimized_schema")
            .is_none_or(Value::is_null)
        && actual
            .get("native_optimized_arrow_schema")
            .is_none_or(Value::is_null)
        && profile["key"]
            == json!({"fixture":fixture,"id":case["id"],"ansi":case["ansi"],
                "representation":representation,"batch_size":batch_size})
        && profile["sql"] == case["sql"]
        && profile["spark_expected"] == selected_fields(&case["expected"], SPARK_FIELDS)
        && selected_fields(&profile["native_actual"], NATIVE_FIELDS)
            == selected_fields(actual, NATIVE_FIELDS)
        && profile["matches_reference"] == false
        && profile["full_compatibility_pass"] == false
}

pub(super) fn recorded_limit(
    fixture: &str,
    case: &Value,
    representation: usize,
    batch_size: usize,
    actual: &Value,
) -> Result<Option<&'static str>, &'static str> {
    let key = json!({"fixture":fixture,"id":case["id"],"ansi":case["ansi"],
        "representation":representation,"batch_size":batch_size});
    Ok(profiles()?
        .iter()
        .find(|profile| {
            profile["key"] == key
                && profile_matches(
                    profile,
                    fixture,
                    case,
                    representation,
                    batch_size,
                    Some(actual),
                )
        })
        .and_then(|profile| profile.get("category"))
        .and_then(Value::as_str))
}

// A recorded diagnostic must not fall through to a looser legacy error matcher.
pub(super) fn has_recorded_case(fixture: &str, case: &Value) -> Result<bool, &'static str> {
    Ok(profiles()?.iter().any(|profile| {
        profile["key"]["fixture"] == fixture && profile["key"]["id"] == case["id"]
    }))
}

#[test]
fn recorded_regex_limits_require_complete_identity_and_outcome() -> crate::string_binary::TestResult {
    use std::collections::BTreeMap;

    let mut counts = BTreeMap::new();
    let profiles = profiles()?;
    assert!(validate_profiles(Vec::new()).is_err());
    let mut truncated = profiles.to_vec();
    truncated.pop();
    assert!(validate_profiles(truncated).is_err());
    let mut duplicated = profiles.to_vec();
    duplicated[1] = duplicated[0].clone();
    assert!(validate_profiles(duplicated).is_err());
    for profile in profiles {
        *counts.entry(profile["category"].as_str().ok_or("missing profile category")?).or_insert(0) += 1;
    }
    assert_eq!(
        counts,
        BTreeMap::from([
            ("accepted_pattern_line_terminator", 204),
            ("java_only_lookbehind_syntax", 96),
            ("malformed_pattern_diagnostic", 36),
            ("native_unicode_word_classification", 96),
            ("utf16_optional_advancement", 48),
            ("utf16_split_region", 36),
        ])
    );
    for category in counts.keys() {
        let profile = profiles
            .iter()
            .find(|profile| profile["category"] == *category)
            .ok_or("missing profile category example")?;
        let fixture = profile["key"]["fixture"].as_str().ok_or("missing profile fixture")?;
        let representation = profile["key"]["representation"].as_u64().ok_or("missing profile representation")? as usize;
        let batch_size = profile["key"]["batch_size"].as_u64().ok_or("missing profile batch size")? as usize;
        let case = json!({"id":profile["key"]["id"],"ansi":profile["key"]["ansi"],
            "sql":profile["sql"],"expected":profile["spark_expected"]});
        let actual = &profile["native_actual"];
        let matches = |case: &Value, actual: Option<&Value>| {
            profile_matches(profile, fixture, case, representation, batch_size, actual)
        };
        assert!(matches(&case, Some(actual)));
        assert!(!matches(&case, None));
        assert!(!profile_matches(profile, "wrong.jsonl", &case, representation, batch_size, Some(actual)));
        assert!(!profile_matches(profile, fixture, &case, 3, batch_size, Some(actual)));
        assert!(!profile_matches(profile, fixture, &case, representation, 7, Some(actual)));
        for (key, value) in [("id", json!("wrong")), ("sql", json!("SELECT 1")), ("ansi", json!(!case["ansi"].as_bool().ok_or("missing case ANSI mode")?))] {
            let mut changed = case.clone();
            changed[key] = value;
            assert!(!matches(&changed, Some(actual)));
        }
        for stage in ["analyzed_schema", "physical_schema"] {
            for (field, value) in [("name", json!("wrong")), ("type", json!("binary")), ("nullable", json!(!actual[stage]["fields"][0]["nullable"].as_bool().ok_or("missing field nullability")?)), ("metadata", json!({"wrong":"metadata"}))] {
                let mut changed = actual.clone();
                changed[stage]["fields"][0][field] = value;
                assert!(!matches(&case, Some(&changed)));
            }
            let mut changed = actual.clone();
            changed.as_object_mut().ok_or("native actual is not an object")?.remove(stage);
            assert!(!matches(&case, Some(&changed)));
        }
        for key in ["analyzed_arrow_schema", "physical_arrow_schema"] {
            let mut changed = actual.clone();
            changed.as_object_mut().ok_or("native actual is not an object")?.remove(key);
            assert!(!matches(&case, Some(&changed)));
        }
        let mut changed_case = case.clone();
        changed_case["expected"]["analyzed_schema"]["fields"][0]["metadata"] =
            json!({"wrong":"metadata"});
        assert!(!matches(&changed_case, Some(actual)));
        let mut changed = actual.clone();
        changed["capture_failure"] = json!(true);
        assert!(!matches(&case, Some(&changed)));
        changed = actual.clone();
        changed["native_optimized_schema"] = actual["analyzed_schema"].clone();
        assert!(!matches(&case, Some(&changed)));
        if actual.get("error").is_some() {
            for (key, value) in [("error_stage", json!("resolve")), ("error", json!("execute: unrelated cause")), ("rows", json!([]))] {
                let mut changed = actual.clone();
                changed[key] = value;
                assert!(!matches(&case, Some(&changed)));
            }
            let error = actual["error"].as_str().ok_or("native error is not a string")?;
            let wrong_cause = if let Some((prefix, _)) = error.rsplit_once('\n') {
                format!("{prefix}\nerror: wrong cause")
            } else {
                error.replace("UTF-16 surrogate pair", "wrong boundary")
            };
            let mut changed = actual.clone();
            changed["error"] = json!(wrong_cause);
            assert!(!matches(&case, Some(&changed)));
            changed["error"] = json!(error.replace("regexp_", "wrong_"));
            assert!(!matches(&case, Some(&changed)));
            let wrong_pattern = error
                .replace("(?<=a)(b)", "(?<=x)(y)")
                .replace("\n    [\n", "\n    (\n");
            if wrong_pattern != error {
                changed["error"] = json!(wrong_pattern);
                assert!(!matches(&case, Some(&changed)));
            }
        } else {
            for rows in [json!([]), json!([["wrong"]]), json!([["extra"],["extra"]])] {
                let mut changed = actual.clone();
                changed["rows"] = rows;
                assert!(!matches(&case, Some(&changed)));
            }
            let mut changed = actual.clone();
            changed["rows"][0][0] = json!("wrong value");
            assert!(!matches(&case, Some(&changed)));
            let mut changed = actual.clone();
            changed["error"] = json!("execute: new error");
            changed["error_stage"] = json!("execute");
            assert!(!matches(&case, Some(&changed)));
            let mut positive = actual.clone();
            positive["rows"] = case["expected"]["rows"].clone();
            assert!(complete_match(&positive, &case["expected"]));
            assert!(!matches(&case, Some(&positive)));
        }
    }
    let profile = profiles
        .iter()
        .find(|profile| profile["native_actual"]["rows"].as_array().is_some_and(|rows| rows.len() > 1))
        .ok_or("missing multi-row profile")?;
    let fixture = profile["key"]["fixture"].as_str().ok_or("missing profile fixture")?;
    let representation = profile["key"]["representation"].as_u64().ok_or("missing profile representation")? as usize;
    let batch_size = profile["key"]["batch_size"].as_u64().ok_or("missing profile batch size")? as usize;
    let case = json!({"id":profile["key"]["id"],"ansi":profile["key"]["ansi"],
        "sql":profile["sql"],"expected":profile["spark_expected"]});
    let mut reversed = profile["native_actual"].clone();
    reversed["rows"].as_array_mut().ok_or("native rows are not an array")?.reverse();
    assert!(!profile_matches(profile, fixture, &case, representation, batch_size, Some(&reversed)));
    let mut duplicate = profile["native_actual"].clone();
    let row = duplicate["rows"][0].clone();
    duplicate["rows"].as_array_mut().ok_or("native rows are not an array")?.push(row);
    assert!(!profile_matches(profile, fixture, &case, representation, batch_size, Some(&duplicate)));
    Ok(())
}
