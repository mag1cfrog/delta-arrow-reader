//! Exact native-only callback profiles shared by the index control targets.
use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::TestResult;

pub fn control_key(value: &Value, keys: &[&str]) -> TestResult<String> {
    let identity = keys
        .iter()
        .map(|key| value.get(*key).ok_or("missing native control identity"))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!(identity).to_string())
}

pub fn native_controls(
    cases: &[Value],
    controls: &[Value],
    keys: &[&str],
) -> TestResult<BTreeMap<String, Value>> {
    let mut inputs = BTreeMap::new();
    let mut profiles = BTreeMap::new();
    for case in cases {
        if inputs
            .insert(control_key(case, keys)?, &case["sql"])
            .is_some()
        {
            return Err("duplicate native input identity".into());
        }
    }
    for control in controls {
        let key = control_key(control, keys)?;
        if control["reviewed"] != true
            || inputs.get(&key).copied() != Some(&control["sql"])
            || !control["sql"].is_string()
            || profiles.insert(key, control.clone()).is_some()
        {
            return Err("unknown, changed or duplicate native control".into());
        }
    }
    if inputs.keys().ne(profiles.keys()) {
        return Err("missing native control".into());
    }
    Ok(profiles)
}

pub fn native_matches(control: &Value, case: &Value, actual: &Value) -> bool {
    control["reviewed"] == true && control["sql"] == case["sql"] && control["actual"] == *actual
}
