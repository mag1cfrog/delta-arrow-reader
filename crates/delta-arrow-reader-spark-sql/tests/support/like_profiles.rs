//! Complete retained native observations remain separate from Spark compatibility.

use serde_json::{Value, json};
use std::collections::BTreeMap;

use super::{TestResult, nullif_error_matches, required_error_matches};

pub(super) type ProfileKey = (String, bool, usize, usize);

pub(super) fn key(
    case: &Value,
    representation: usize,
    batch_size: usize,
) -> TestResult<ProfileKey> {
    Ok((
        case["id"].as_str().ok_or("missing case ID")?.to_owned(),
        case["ansi"].as_bool().ok_or("missing ANSI flag")?,
        representation,
        batch_size,
    ))
}

pub(super) fn load() -> TestResult<BTreeMap<ProfileKey, Value>> {
    let mut lines = include_str!("../fixtures/like-retained-native-observations.jsonl").lines();
    let header: Value = serde_json::from_str(lines.next().ok_or("missing native profile header")?)?;
    assert_eq!(
        header["kind"],
        "complete-native-negative-observations-not-spark-oracle"
    );
    assert_eq!(header["observations"], 180);
    assert_eq!(header["acceptance_waivers"], false);
    let mut profiles = BTreeMap::new();
    let mut owners = BTreeMap::<String, usize>::new();
    for line in lines {
        let profile: Value = serde_json::from_str(line)?;
        assert_eq!(profile["strict_compatibility"], false);
        assert_eq!(profile["full_compatibility"], false);
        assert_eq!(profile["accepted_as_compatibility"], false);
        let representation = profile["representation"]
            .as_u64()
            .ok_or("missing representation")? as usize;
        let batch_size = profile["batch_size"].as_u64().ok_or("missing batch size")? as usize;
        let owner = profile["owner"].as_str().ok_or("missing owner")?.to_owned();
        *owners.entry(owner).or_default() += 1;
        assert!(
            profiles
                .insert(key(&profile, representation, batch_size)?, profile)
                .is_none()
        );
    }
    assert_eq!(profiles.len(), 180);
    assert_eq!(
        owners,
        BTreeMap::from([
            ("#791".into(), 12),
            ("#875".into(), 96),
            ("#915".into(), 72)
        ])
    );
    Ok(profiles)
}

pub(super) fn compare_spark(case: &Value, actual: &Value) -> Value {
    let expected = &case["expected"];
    let expected_error = expected["status"] == "error";
    let mut facets = json!({
        "status":actual["error"].is_null() != expected_error,
        "analyzed_schema":actual["analyzed_schema"] == expected["analyzed_schema"],
        "physical_schema":if expected["physical_schema"].is_null() {
            json!("unavailable-spark-reference")
        } else {
            json!(actual["physical_schema"] == expected["physical_schema"])
        },
    });
    if expected_error {
        let error = actual["error"].as_str().unwrap_or_default();
        facets["cause_and_stage"] = json!(if expected["condition"] == "CAST_INVALID_INPUT" {
            nullif_error_matches(case, error)
        } else {
            required_error_matches(case, error)
        });
        let condition = expected["condition"]
            .as_str()
            .unwrap_or("missing condition");
        facets["spark_error_class"] = json!(error.contains(&format!("[{condition}]")));
        if condition == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION" {
            let parameters = &expected["parameters"];
            let function = parameters["functionName"]
                .as_str()
                .unwrap_or("missing function");
            let count = parameters["expectedNum"]
                .as_str()
                .unwrap_or("missing expected count");
            let actual_count = parameters["actualNum"]
                .as_str()
                .unwrap_or("missing actual count");
            facets["spark_error_parameters"] = json!(error.contains(&format!(
                "The {function} requires {count} parameters but the actual number is {actual_count}."
            )));
        }
    } else {
        assert_eq!(expected["status"], "ok");
        facets["rows"] = json!(actual["rows"] == expected["rows"]);
    }
    // Unavailable Spark physical fields are explicitly labeled above. They are
    // never replaced with optimized fields or with the native observation.
    let strict = facets
        .as_object()
        .is_some_and(|facets| facets.values().all(|value| value.as_bool() != Some(false)));
    let mut evidence = json!({
        "analyzed_raw":actual["analyzed_schema"].is_null() || actual["analyzed_arrow_schema"].is_string(),
        "physical_raw":actual["physical_schema"].is_null() || actual["physical_arrow_schema"].is_string(),
    });
    if expected_error {
        evidence["physical_stage"] = json!(if actual["error_stage"] == "execute" {
            actual["physical_schema"].is_object() && actual["physical_arrow_schema"].is_string()
        } else {
            actual["physical_schema"].is_null() && actual["physical_arrow_schema"].is_null()
        });
    }
    let full = strict
        && evidence
            .as_object()
            .is_some_and(|evidence| evidence.values().all(|value| value.as_bool() == Some(true)));
    let mut availability = json!({});
    for stage in ["analyzed", "physical"] {
        let field = format!("{stage}_schema");
        availability[stage] = json!({
            "spark_reference":if expected[field.as_str()].is_null() {"unavailable"} else {"available"},
            "actual":if actual[field.as_str()].is_null() {"unavailable"} else {"available"},
        });
    }
    json!({"facets":facets,"evidence":evidence,"availability":availability,
        "strict_compatibility":strict,"full_compatibility":full})
}

pub(super) fn retained_matches(profile: &Value, actual: &Value, comparison: &Value) -> bool {
    profile["strict_compatibility"] == false
        && profile["full_compatibility"] == false
        && profile["accepted_as_compatibility"] == false
        && comparison["strict_compatibility"] == false
        && comparison["full_compatibility"] == false
        && profile["comparison"] == *comparison
        && profile["actual"] == *actual
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(profile: &Value) -> TestResult<Value> {
        let capture = match profile["fixture"].as_str() {
            Some("like-escapes-spark.jsonl") => {
                include_str!("../fixtures/like-escapes-spark.jsonl")
            }
            Some("like-unevaluable-sibling-spark.jsonl") => {
                include_str!("../fixtures/like-unevaluable-sibling-spark.jsonl")
            }
            _ => return Err("unexpected negative-profile fixture".into()),
        };
        capture
            .lines()
            .skip(1)
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .find(|case| case["id"] == profile["id"] && case["ansi"] == profile["ansi"])
            .ok_or_else(|| "missing original Spark reference".into())
    }

    #[test]
    fn all_complete_native_profiles_remain_incompatible_with_spark() -> TestResult {
        for profile in load()?.values() {
            let comparison = compare_spark(&reference(profile)?, &profile["actual"]);
            assert_eq!(comparison["strict_compatibility"], false);
            assert_eq!(comparison["full_compatibility"], false);
            assert!(retained_matches(profile, &profile["actual"], &comparison));
        }
        Ok(())
    }

    #[test]
    fn complete_native_profiles_reject_field_value_error_and_availability_drift() -> TestResult {
        let profiles = load()?;
        let named = profiles
            .values()
            .find(|profile| profile["owner"] == "#791")
            .ok_or("missing name profile")?;
        let arity = profiles
            .values()
            .find(|profile| profile["owner"] == "#915")
            .ok_or("missing arity profile")?;
        let rand = profiles
            .values()
            .find(|profile| {
                profile["owner"] == "#875" && profile["actual"]["physical_schema"].is_object()
            })
            .ok_or("missing RAND physical profile")?;
        let mut mutations = Vec::new();
        for field in ["name", "type", "nullable", "metadata"] {
            let mut actual = named["actual"].clone();
            actual["analyzed_schema"]["fields"][0][field] = match field {
                "nullable" => json!(true),
                "metadata" => json!({"unexpected":"metadata"}),
                _ => json!("unexpected"),
            };
            mutations.push((named, actual));
        }
        let mut actual = named["actual"].clone();
        actual["rows"][0][0] = json!("false");
        mutations.push((named, actual));
        let mut actual = rand["actual"].clone();
        actual["physical_schema"]["fields"][1]["nullable"] = json!(false);
        mutations.push((rand, actual));
        let mut actual = rand["actual"].clone();
        actual["physical_arrow_schema"] = json!("changed actual raw fields");
        mutations.push((rand, actual));
        let mut actual = arity["actual"].clone();
        actual["error"] = json!(format!(
            "{} changed cause",
            actual["error"].as_str().unwrap()
        ));
        mutations.push((arity, actual));
        let mut actual = arity["actual"].clone();
        actual["error_stage"] = json!("physical");
        mutations.push((arity, actual));
        let mut actual = arity["actual"].clone();
        actual["analyzed_schema"] = json!({"type":"struct","fields":[]});
        mutations.push((arity, actual));
        let mut actual = rand["actual"].clone();
        actual["physical_schema"] = Value::Null;
        mutations.push((rand, actual));
        let mut actual = arity["actual"].clone();
        actual
            .as_object_mut()
            .unwrap()
            .remove("physical_arrow_schema");
        mutations.push((arity, actual));
        for (profile, actual) in mutations {
            let comparison = compare_spark(&reference(profile)?, &actual);
            assert!(!retained_matches(profile, &actual, &comparison));
        }
        Ok(())
    }
}
