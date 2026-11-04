//! Exact recorded limits for the two Spark regex adapters.

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

fn profiles() -> &'static [Value] {
    static PROFILES: OnceLock<Vec<Value>> = OnceLock::new();
    PROFILES.get_or_init(|| {
        include_str!("../fixtures/regex-native-limit-profiles.jsonl")
            .split('\n')
            .skip(1)
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).expect("recorded regex profile JSON"))
            .collect()
    })
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
) -> Option<&'static str> {
    let key = json!({"fixture":fixture,"id":case["id"],"ansi":case["ansi"],
        "representation":representation,"batch_size":batch_size});
    profiles()
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
        })?
        .get("category")?
        .as_str()
}

// A recorded diagnostic must not fall through to a looser legacy error matcher.
pub(super) fn has_recorded_case(fixture: &str, case: &Value) -> bool {
    profiles().iter().any(|profile| {
        profile["key"]["fixture"] == fixture && profile["key"]["id"] == case["id"]
    })
}

#[test]
fn recorded_regex_limits_require_complete_identity_and_outcome() {
    use std::collections::{BTreeMap, HashSet};

    let mut keys = HashSet::new();
    let mut counts = BTreeMap::new();
    for profile in profiles() {
        assert!(keys.insert(profile["key"].to_string()));
        *counts.entry(profile["category"].as_str().unwrap()).or_insert(0) += 1;
    }
    assert_eq!(keys.len(), 516);
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
        let profile = profiles()
            .iter()
            .find(|profile| profile["category"] == *category)
            .unwrap();
        let fixture = profile["key"]["fixture"].as_str().unwrap();
        let representation = profile["key"]["representation"].as_u64().unwrap() as usize;
        let batch_size = profile["key"]["batch_size"].as_u64().unwrap() as usize;
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
        for (key, value) in [("id", json!("wrong")), ("sql", json!("SELECT 1")), ("ansi", json!(!case["ansi"].as_bool().unwrap()))] {
            let mut changed = case.clone();
            changed[key] = value;
            assert!(!matches(&changed, Some(actual)));
        }
        for stage in ["analyzed_schema", "physical_schema"] {
            for (field, value) in [("name", json!("wrong")), ("type", json!("binary")), ("nullable", json!(!actual[stage]["fields"][0]["nullable"].as_bool().unwrap())), ("metadata", json!({"wrong":"metadata"}))] {
                let mut changed = actual.clone();
                changed[stage]["fields"][0][field] = value;
                assert!(!matches(&case, Some(&changed)));
            }
            let mut changed = actual.clone();
            changed.as_object_mut().unwrap().remove(stage);
            assert!(!matches(&case, Some(&changed)));
        }
        for key in ["analyzed_arrow_schema", "physical_arrow_schema"] {
            let mut changed = actual.clone();
            changed.as_object_mut().unwrap().remove(key);
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
            let error = actual["error"].as_str().unwrap();
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
    let profile = profiles()
        .iter()
        .find(|profile| profile["native_actual"]["rows"].as_array().is_some_and(|rows| rows.len() > 1))
        .unwrap();
    let case = json!({"id":profile["key"]["id"],"ansi":profile["key"]["ansi"],
        "sql":profile["sql"],"expected":profile["spark_expected"]});
    let mut reversed = profile["native_actual"].clone();
    reversed["rows"].as_array_mut().unwrap().reverse();
    assert!(!profile_matches(profile, profile["key"]["fixture"].as_str().unwrap(), &case,
        profile["key"]["representation"].as_u64().unwrap() as usize,
        profile["key"]["batch_size"].as_u64().unwrap() as usize, Some(&reversed)));
    let mut duplicate = profile["native_actual"].clone();
    let row = duplicate["rows"][0].clone();
    duplicate["rows"].as_array_mut().unwrap().push(row);
    assert!(!profile_matches(profile, profile["key"]["fixture"].as_str().unwrap(), &case,
        profile["key"]["representation"].as_u64().unwrap() as usize,
        profile["key"]["batch_size"].as_u64().unwrap() as usize, Some(&duplicate)));
}
