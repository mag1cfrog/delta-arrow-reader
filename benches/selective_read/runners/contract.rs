//! Execution contracts shared by the controller and native readers.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::PathBuf;

use super::Result;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub table_uri: String,
    pub snapshot_version: u64,
    pub case_id: String,
    pub(super) canonical_sql: String,
    pub(super) comparison_revision: u64,
    pub(super) protocol_sha256: String,
    pub(super) base_protocol_sha256: Option<String>,
    pub(super) workload_manifest_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) workload_contract: Option<Value>,
    pub(super) sampling_sha256: Option<String>,
    pub(super) sampling_stage: Option<String>,
    pub(super) fixture_manifest_sha256: String,
    pub(super) profile: String,
    pub(super) execution_mode: String,
    pub(super) concurrent_queries: Option<usize>,
    pub(super) purpose: String,
    #[serde(default)]
    pub(super) validation_diagnostics: bool,
    pub(super) resource_budget: Value,
    pub(super) correctness_file: Option<PathBuf>,
    pub(super) campaign_id: Option<String>,
    pub(super) run_id: String,
    pub(super) repetition: Option<u64>,
    pub(super) order: Option<u64>,
}

impl Request {
    pub fn reuse(&self) -> bool {
        self.execution_mode == "reuse"
    }
    pub fn query_count(&self) -> usize {
        if let Some(count) = self.concurrent_queries {
            count
        } else if !self.reuse() {
            1
        } else if self.comparison_revision >= 4 {
            2
        } else {
            10
        }
    }
    pub fn timed(&self) -> bool {
        self.purpose == "timing"
    }

    pub fn validate(&self) -> Result<()> {
        let uri = url::Url::parse(&self.table_uri)?;
        if !matches!(uri.scheme(), "file" | "s3")
            || !uri.username().is_empty()
            || uri.password().is_some()
            || uri.query().is_some()
            || uri.fragment().is_some()
            || (uri.scheme() == "file"
                && (!matches!(uri.host_str(), None | Some("localhost"))
                    || !self
                        .table_uri
                        .strip_prefix("file:")
                        .is_some_and(|p| p.starts_with('/'))))
            || (uri.scheme() == "s3" && uri.host_str().is_none_or(str::is_empty))
        {
            return Err(
                "expected a file/s3 table URL without embedded credentials, query or fragment"
                    .into(),
            );
        }
        if !matches!(self.execution_mode.as_str(), "open" | "reuse")
            || !matches!(
                self.purpose.as_str(),
                "timing" | "validation" | "diagnostic" | "io"
            )
            || self.concurrent_queries.is_some_and(|count| {
                !matches!(count, 2 | 4) || !self.reuse() || self.comparison_revision != 6
            })
            || !self.valid_comparison()
            || !is_hash(&json!(self.fixture_manifest_sha256))
            || self.case_id.trim().is_empty()
            || self.run_id.trim().is_empty()
            || self.profile.trim().is_empty()
            || self.canonical_sql.trim().is_empty()
            || self.resource_budget != budget()
            || self.validation_diagnostics
                && (self.comparison_revision != 6 || self.purpose != "validation")
        {
            return Err("invalid request or settings differ from the frozen protocol".into());
        }
        Ok(())
    }

    fn valid_comparison(&self) -> bool {
        serde_json::to_value(self).is_ok_and(|value| comparison(&value).is_ok())
    }
}

pub fn budget() -> Value {
    json!({"worker_threads": 8, "max_blocking_threads": 64, "target_partitions": 8,
        "batch_rows": 8192, "datafusion_pool_bytes": 4_u64 * 1024 * 1024 * 1024,
        "process_memory_bytes": 8_u64 * 1024 * 1024 * 1024, "logical_cpus": 8})
}

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
                            | "native.py"
                            | "selective-read-controller"
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

pub fn identity(
    request: &Value,
    reader: &str,
    build_record: &Value,
    hashes: Value,
) -> Result<Value> {
    let mut identity = comparison(request)?;
    let hashes = hashes.as_object().ok_or("invalid reader identity hashes")?;
    if hashes.len() != 4
        || [
            "reader_build_sha256",
            "reader_config_sha256",
            "canonical_sql_sha256",
        ]
        .iter()
        .any(|key| hashes.get(*key).is_none_or(|value| !is_hash(value)))
        || hashes
            .get("native_expression_sha256")
            .is_none_or(|v| !v.is_null() && !is_hash(v))
        || build_record["reader_id"] != reader
    {
        return Err("invalid reader artifact identity".into());
    }
    identity.as_object_mut().unwrap().extend(hashes.clone());
    identity["reader_id"] = json!(reader);
    identity["reader_artifact"] = build(build_record)?;
    for field in ["fixture_manifest_sha256", "case_id", "snapshot_version"] {
        identity[field] = request
            .get(field)
            .ok_or("missing reader identity field")?
            .clone();
    }
    Ok(identity)
}

pub fn correctness(proof: &Value, identity: &Value, query_count: usize) -> Result<Vec<u64>> {
    let checks = proof["checks"]
        .as_array()
        .ok_or("missing correctness checks")?;
    if proof["status"] != "passed" || checks.len() != query_count || query_count == 0 {
        return Err("failed or incomplete correctness certificate".into());
    }
    checks
        .iter()
        .map(|check| {
            if check["status"] != "passed" {
                return Err("failed oracle check".into());
            }
            if !identity_matches(identity, check) {
                return Err("stale correctness identity".into());
            }
            check["output_rows"]
                .as_u64()
                .ok_or_else(|| "missing validated row count".into())
        })
        .collect()
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Operation {
    Comparison {
        value: Value,
    },
    SameComparison {
        left: Value,
        right: Value,
    },
    Identity {
        value: Value,
        reader: String,
        build: Value,
        hashes: Value,
    },
    Validate {
        value: Value,
    },
    Correctness {
        proof: Value,
        identity: Value,
        query_count: usize,
    },
}

pub fn run(operation: Operation) -> Result<Value> {
    match operation {
        Operation::Comparison { value } => comparison(&value),
        Operation::SameComparison { left, right } => Ok(json!(same_comparison(&left, &right))),
        Operation::Identity {
            value,
            reader,
            build,
            hashes,
        } => identity(&value, &reader, &build, hashes),
        Operation::Validate { value } => {
            if ["campaign_id", "repetition", "order", "correctness_file"]
                .iter()
                .any(|key| value.get(*key).is_none())
            {
                return Err("missing Python adapter request context".into());
            }
            let request: Request = serde_json::from_value(value)?;
            request.validate()?;
            if request.concurrent_queries.is_some() {
                return Err("Python adapters do not support concurrent queries".into());
            }
            Ok(Value::Null)
        }
        Operation::Correctness {
            proof,
            identity,
            query_count,
        } => Ok(json!(correctness(&proof, &identity, query_count)?)),
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
    fn shared_request_and_proof_cases() -> Result<()> {
        let cases: Value = serde_json::from_str(include_str!("contract-cases.json"))?;
        for case in cases["requests"].as_array().unwrap() {
            let mut value = cases["request"].clone();
            value
                .as_object_mut()
                .unwrap()
                .extend(case["change"].as_object().unwrap().clone());
            if let Some(remove) = case["remove"].as_array() {
                for key in remove {
                    value.as_object_mut().unwrap().remove(key.as_str().unwrap());
                }
            }
            let result = serde_json::from_value::<Request>(value.clone())
                .map_err(Into::into)
                .and_then(|request| request.validate());
            assert_eq!(
                result.is_ok(),
                case["valid"],
                "{}: {result:?}",
                case["name"]
            );
            assert_eq!(
                run(Operation::Validate { value }).is_ok(),
                case["python_valid"],
                "{}",
                case["name"]
            );
        }
        for case in cases["proofs"].as_array().unwrap() {
            let mut check = cases["identity"].clone();
            check["status"] = json!("passed");
            check["output_rows"] = json!(3);
            check
                .as_object_mut()
                .unwrap()
                .extend(case["change"].as_object().unwrap().clone());
            if let Some(remove) = case["remove"].as_array() {
                for key in remove {
                    check.as_object_mut().unwrap().remove(key.as_str().unwrap());
                }
            }
            let proof = json!({"status":case.get("status").unwrap_or(&json!("passed")),
                "checks":vec![check; case["checks"].as_u64().unwrap_or(2) as usize]});
            let result = correctness(&proof, &cases["identity"], 2);
            assert_eq!(
                result.is_ok(),
                case["valid"],
                "{}: {result:?}",
                case["name"]
            );
            if let Ok(rows) = result {
                assert_eq!(rows, vec![3, 3]);
            }
        }
        Ok(())
    }

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
            "bundled_sha256":{"run.py":"runtime", "protocol.md":"docs", "oracle.py":"checker",
                "native.py":"bridge", "selective-read-controller":"validator"}});
        let mut changed = artifact.clone();
        changed["harness_git_commit"] = json!("after");
        changed["bundled_sha256"]["protocol.md"] = json!("new docs");
        changed["bundled_sha256"]["oracle.py"] = json!("new checker");
        changed["bundled_sha256"]["selective-read-controller"] = json!("rebuilt validator");
        changed["bundled_sha256"]["native.py"] = json!("updated bridge");
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
