//! Execution contracts shared by the controller and native readers.

use serde_json::{Value, json};

use super::Result;

pub fn is_hash(value: &Value) -> bool {
    value.as_str().is_some_and(|s| {
        s.len() == 64
            && s.bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    })
}

pub fn workload(value: &Value) -> Result<Value> {
    let mut contract = value
        .as_object()
        .ok_or("invalid workload contract")?
        .clone();
    for key in [
        "protocol_sha256",
        "base_protocol_sha256",
        "sampling_sha256",
        "source_sha256",
        "definition_sha256",
        "definition_controller_sha256",
    ] {
        contract.remove(key);
    }
    if let Some(translations) = contract
        .get_mut("translations")
        .and_then(Value::as_object_mut)
    {
        for translation in translations.values_mut() {
            translation
                .as_object_mut()
                .ok_or("invalid translation")?
                .remove("adapter_sha256");
        }
    }
    Ok(Value::Object(contract))
}

pub fn build(value: &Value) -> Result<Value> {
    if value["reader_id"].as_str().is_none_or(str::is_empty)
        || !is_hash(&value["executable_sha256"])
        || !is_hash(&value["lockfile_sha256"])
    {
        return Err("invalid reader artifact identity".into());
    }
    let mut result = json!({"reader_id":value["reader_id"],
        "executable_sha256":value["executable_sha256"], "lockfile_sha256":value["lockfile_sha256"]});
    if let Some(runtime) = value.get("runtime") {
        result["runtime"] = runtime.clone();
    }
    if let Some(bundled) = value.get("bundled_sha256") {
        result["bundled_sha256"] = Value::Object(
            bundled
                .as_object()
                .ok_or("invalid bundled artifacts")?
                .iter()
                .filter(|(name, _)| {
                    !matches!(
                        name.as_str(),
                        "protocol.md"
                            | "large-workloads.md"
                            | "sampling.md"
                            | "production-workloads.md"
                            | "spark-matrix.md"
                            | "oracle.py"
                    )
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
    }
    Ok(result)
}

pub fn comparison(value: &Value) -> Result<Value> {
    let revision = value["comparison_revision"]
        .as_u64()
        .filter(|v| (2..=6).contains(v))
        .ok_or("unknown comparison revision")?;
    let mut keys = vec!["comparison_revision", "protocol_sha256"];
    if revision >= 3 {
        if !is_hash(&value["workload_manifest_sha256"]) {
            return Err("missing or stale large-workload identity".into());
        }
        keys.extend(["base_protocol_sha256", "workload_manifest_sha256"]);
        if let Some(contract) = value.get("workload_contract") {
            if !contract.is_object()
                || contract["comparison_revision"] != revision
                || contract["sampling_stage"] != value["sampling_stage"]
            {
                return Err("invalid workload contract".into());
            }
            keys.push("workload_contract");
        }
    } else if !value["workload_manifest_sha256"].is_null()
        || value.get("workload_contract").is_some()
    {
        return Err("revision 2 cannot carry large-workload identities".into());
    }
    if revision >= 4 {
        keys.push("sampling_sha256");
    }
    if revision >= 5 {
        if !matches!(value["sampling_stage"].as_str(), Some("pilot" | "formal")) {
            return Err("missing or invalid production sampling stage".into());
        }
        keys.push("sampling_stage");
    } else if !value["sampling_stage"].is_null() {
        return Err("historical revisions cannot carry a production sampling stage".into());
    }
    Ok(Value::Object(
        keys.into_iter()
            .map(|k| (k.into(), value[k].clone()))
            .collect(),
    ))
}

pub fn same_comparison(left: &Value, right: &Value) -> bool {
    left["comparison_revision"] == right["comparison_revision"]
        && left["sampling_stage"] == right["sampling_stage"]
        && match (
            left.get("workload_contract"),
            right.get("workload_contract"),
        ) {
            (Some(left), Some(right)) => left == right,
            _ => left["workload_manifest_sha256"] == right["workload_manifest_sha256"],
        }
}

pub fn identity_matches(expected: &Value, actual: &Value) -> bool {
    let Some(expected) = expected.as_object() else {
        return false;
    };
    expected.iter().all(|(key, value)| {
        let replacement = match key.as_str() {
            "protocol_sha256" | "base_protocol_sha256" | "sampling_sha256" => return true,
            "reader_build_sha256" => Some("reader_artifact"),
            "workload_manifest_sha256" => Some("workload_contract"),
            "reader_artifact" | "workload_contract" if actual.get(key).is_none() => {
                let legacy = if key == "reader_artifact" {
                    "reader_build_sha256"
                } else {
                    "workload_manifest_sha256"
                };
                return expected
                    .get(legacy)
                    .is_some_and(|v| actual.get(legacy) == Some(v));
            }
            _ => None,
        };
        // Legacy certificates can only be reused with their original immutable inputs.
        if replacement.is_some_and(|k| expected.contains_key(k) && actual.get(k).is_some()) {
            return true;
        }
        actual.get(key) == Some(value)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_edits_preserve_contracts_but_execution_changes_do_not() -> Result<()> {
        let definition = json!({"format":"selective-read-production-workload-v1", "comparison_revision":6,
            "sampling_stage":"pilot", "cases":[{"canonical_sql":"SELECT id FROM bench", "projection":["id"],
            "writer":{"data_page_rows":2048}, "fixture_manifest_sha256":"fixture"}],
            "source_sha256":{"planner":"before"}, "definition_controller_sha256":"before",
            "translations":{"polars":{"adapter_sha256":"before", "expressions":{"case":"native expression"}}}});
        let expected = workload(&definition)?;
        let mut changed = definition.clone();
        changed["source_sha256"] = json!({"planner":"after"});
        changed["definition_controller_sha256"] = json!("after");
        changed["translations"]["polars"]["adapter_sha256"] = json!("after");
        assert_eq!(workload(&changed)?, expected);
        for (pointer, value) in [
            ("/cases/0/canonical_sql", json!("SELECT other FROM bench")),
            ("/cases/0/projection", json!(["other"])),
            ("/cases/0/writer/data_page_rows", json!(20000)),
            ("/cases/0/fixture_manifest_sha256", json!("other fixture")),
            (
                "/translations/polars/expressions/case",
                json!("other expression"),
            ),
        ] {
            let mut changed = definition.clone();
            *changed.pointer_mut(pointer).unwrap() = value;
            assert_ne!(workload(&changed)?, expected, "{pointer}");
        }
        let artifact = json!({"reader_id":"test", "executable_sha256":"1".repeat(64), "lockfile_sha256":"2".repeat(64),
            "harness_git_commit":"before", "runtime":{"engine":"1"},
            "bundled_sha256":{"run.py":"runtime", "protocol.md":"docs", "oracle.py":"checker"}});
        let mut changed = artifact.clone();
        changed["harness_git_commit"] = json!("after");
        changed["bundled_sha256"]["protocol.md"] = json!("new docs");
        changed["bundled_sha256"]["oracle.py"] = json!("new checker");
        assert_eq!(build(&artifact)?, build(&changed)?);
        for (pointer, value) in [
            ("/executable_sha256", json!("3".repeat(64))),
            ("/lockfile_sha256", json!("3".repeat(64))),
            ("/runtime/engine", json!("2")),
            ("/bundled_sha256/run.py", json!("different runtime")),
        ] {
            let mut changed = artifact.clone();
            *changed.pointer_mut(pointer).unwrap() = value;
            assert_ne!(build(&artifact)?, build(&changed)?, "{pointer}");
        }
        Ok(())
    }

    #[test]
    fn legacy_proofs_need_exact_artifacts_and_new_proofs_compare_contracts() {
        let legacy = json!({"reader_build_sha256":"old", "workload_manifest_sha256":"old", "canonical_sql_sha256":"query"});
        let mut current = legacy.clone();
        current["reader_artifact"] = json!({"executable_sha256":"binary"});
        current["workload_contract"] = json!({"cases":["query"]});
        assert!(identity_matches(&current, &legacy));
        let mut updated = current.clone();
        updated["reader_build_sha256"] = json!("new audit");
        updated["workload_manifest_sha256"] = json!("new audit");
        assert!(identity_matches(&updated, &current));
        assert!(!identity_matches(&updated, &legacy));
        assert!(!identity_matches(
            &json!({"reader_artifact":{}}),
            &json!({})
        ));
        updated["workload_contract"]["cases"] = json!(["different query"]);
        assert!(!identity_matches(&updated, &current));
    }
}
