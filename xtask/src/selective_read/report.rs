use super::{
    array, budget,
    campaign::{
        evidence::{Mode, comparison},
        roster,
    },
    field, object, require,
    storage::{read_json, save},
    text,
};
use crate::{Result, selective_read_build::digest};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

const MODES: [&str; 2] = ["open", "reuse"];

pub(super) mod batch;
pub(super) mod controller;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    definition: PathBuf,
    frozen: Value,
    identity: Value,
    campaigns: Vec<(PathBuf, Value)>,
    output: PathBuf,
    protocols: PathBuf,
}

fn conditions(config: &Value, protocols: &Path) -> Result<Value> {
    let builds = object(field(config, "reader_builds")?)?
        .iter()
        .map(|(reader, build)| Ok((reader.clone(), super::contract::build(build)?)))
        .collect::<Result<Map<_, _>>>()?;
    let server = &config["server"];
    let mut model = BTreeSet::new();
    for line in text(&server["cpu_info"])?.lines() {
        let (key, value) = line.split_once(':').unwrap_or((line, ""));
        if matches!(
            key.trim(),
            "vendor_id" | "cpu family" | "model" | "model name" | "stepping" | "microcode"
        ) {
            model.insert((key.trim(), value.trim()));
        }
    }
    let network = &server["network"];
    let mut transport = Value::Null;
    if network.is_object() && !object(network)?.is_empty() {
        transport = json!({"limits":{"MemoryMax":field(&network["limits"],"MemoryMax")?,"MemorySwapMax":field(&network["limits"],"MemorySwapMax")?}});
        for key in [
            "profile",
            "cpu",
            "binary_sha256",
            "byte_boundary",
            "chunk_bytes",
            "jitter",
            "go_version",
        ] {
            transport[key] = field(network, key)?.clone();
        }
    }
    Ok(json!({"reader_builds":builds, "resource_budget":budget(),
        "gate_diagnostics":Mode::GateDiagnostics.enabled(config,protocols)?, "server_build_sha256":field(server,"build_sha256")?,
        "cpus":field(server,"cpus")?, "topology":field(server,"topology")?, "cpu_model":model, "kernel":field(server,"kernel")?,
        "cache_policy":field(config,"cache_policy")?, "transport":transport}))
}

pub(super) fn run(request: Request) -> Result<Value> {
    let Request {
        definition,
        frozen,
        identity,
        campaigns,
        output,
        protocols,
    } = request;
    let identity = comparison(&identity, &protocols)?;
    let readers = roster(&identity)?;
    let definitions = array(&frozen["cases"])?;
    let cases: Map<_, _> = definitions
        .iter()
        .map(|row| Ok((text(&row["case_id"])?.to_owned(), row.clone())))
        .collect::<Result<_>>()?;
    let order: Vec<_> = definitions
        .iter()
        .map(|row| text(&row["case_id"]))
        .collect::<Result<_>>()?;
    require(
        matches!(identity["comparison_revision"].as_u64(), Some(5 | 6))
            && identity["sampling_stage"] == "formal"
            && frozen["scope"] == "formal"
            && cases.len() == 8
            && cases.len() == definitions.len()
            && object(&frozen["inventory"])?.keys().eq(cases.keys()),
        "use the complete eight-case formal definition",
    )?;
    require(!output.exists(), "report output already exists")?;
    let mut entries = BTreeMap::new();
    let mut sources = Vec::new();
    let mut shared = Value::Null;
    let mut builds = Map::new();
    let mut build_hashes = Map::new();
    for (directory, audited) in campaigns {
        let config = read_json(&directory.join("campaign.json"))?;
        let combined = Mode::CombinedDiagnostics.enabled(&config, &protocols)?;
        let warmup = Mode::GateWarmup.enabled(&config, &protocols)?;
        let gate_diagnostics = Mode::GateDiagnostics.enabled(&config, &protocols)?;
        let declared = comparison(&config, &protocols)?;
        require(
            object(&identity)?
                .iter()
                .filter(|(k, _)| {
                    !matches!(
                        k.as_str(),
                        "workload_manifest_sha256"
                            | "workload_contract"
                            | "protocol_sha256"
                            | "base_protocol_sha256"
                            | "sampling_sha256"
                    )
                })
                .all(|(k, v)| declared.get(k) == Some(v)),
            "campaign is not from the same formal protocol",
        )?;
        let observed = conditions(&config, &protocols)?;
        require(
            shared.is_null() || shared == observed,
            "incompatible reader builds, resources, cache or transport",
        )?;
        shared = observed;
        let observations: Vec<Value> = fs::read_to_string(directory.join("observations.jsonl"))?
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<_, _>>()?;
        for row in &observations {
            let record = &row["observation"];
            if record["status"] == "success" {
                let (limits, watch) = (&record["external_resource_limits"], &record["supervision"]);
                require(
                    row["request"]["resource_budget"] == budget()
                        && watch["query_deadline_seconds"].as_f64() == Some(1800.0)
                        && watch["cleanup_deadline_seconds"].as_f64() == Some(60.0)
                        && limits["cpu_affinity"] == shared["cpus"]["reader"]
                        && limits["process_memory_bytes"] == budget()["process_memory_bytes"]
                        && limits["swap_bytes"] == 0,
                    "formal resource or watchdog limits changed",
                )?;
            }
        }
        for row in array(&audited["rows"])? {
            let job = &row["job"];
            let reader = text(&row["reader_id"])?;
            let (case, mode) = (text(&job["case_id"])?, text(&job["execution_mode"])?);
            require(
                cases.contains_key(case) && cases.get(case) == row.get("case"),
                "campaign case differs from the frozen definition",
            )?;
            require(
                readers.contains(&reader) && MODES.contains(&mode),
                "unknown reader or query profile",
            )?;
            let key = (case.to_owned(), mode.to_owned(), reader.to_owned());
            require(
                !entries.contains_key(&key),
                "duplicate case/profile coverage; do not pool retries",
            )?;
            let gate = &row["gate"];
            if gate["identity"].is_object() && !object(&gate["identity"])?.is_empty() {
                let build = field(&gate["identity"], "reader_artifact")?;
                require(
                    build.is_object(),
                    "missing reader artifact contract; rerun validation",
                )?;
                require(
                    builds.get(reader).is_none_or(|previous| previous == build),
                    "native reader build changed",
                )?;
                builds.insert(reader.to_owned(), build.clone());
                build_hashes.insert(
                    reader.to_owned(),
                    gate["identity"]["reader_build_sha256"].clone(),
                );
            }
            let measurement = &row["measurements"];
            let eligible = measurement["eligible"]
                .as_bool()
                .ok_or("invalid eligibility flag")?;
            if eligible {
                require(
                    measurement["scheduled_samples"] == 5
                        && object(&measurement["metrics"])?
                            .values()
                            .all(|metric| metric["samples"] == 5),
                    "formal distributions require five independent samples",
                )?;
            }
            let mut entry = row.clone();
            entry
                .as_object_mut()
                .ok_or("invalid audited row")?
                .remove("case");
            entry["status"] = if gate["status"] == "success" && !eligible {
                json!("incomplete")
            } else {
                field(gate, "status")?.clone()
            };
            entry["campaign_id"] = field(&audited, "campaign_id")?.clone();
            entry["campaign_status"] = field(&audited, "status")?.clone();
            entry["warmup_source"] = json!(if warmup {
                "exact-validation-gate"
            } else {
                "standalone"
            });
            entry["plans"] = json!(observations.iter().filter(|observed| observed["job_id"] == job["id"] && observed["reader_id"] == reader
                && observed["observation"].get("plan_artifacts").is_some()).map(|observed| json!({"run_id":observed["run_id"],"artifacts":observed["observation"]["plan_artifacts"]})).collect::<Vec<_>>());
            entries.insert(key, entry);
        }
        sources.push(json!({"path":fs::canonicalize(&directory)?, "campaign_id":audited["campaign_id"],
            "upload_verification":config["upload_verification"], "in_process_upload":config.get("in_process_upload").unwrap_or(&json!(false)),
            "combined_diagnostics":combined, "gate_warmup":warmup, "gate_diagnostics":gate_diagnostics,
            "status":audited["status"], "workload_manifest_sha256":declared["workload_manifest_sha256"],
            "observations_sha256":field(&audited,"observations_sha256")?, "summary_sha256":field(&audited,"summary_sha256")?,
            "config_sha256":digest(&directory.join("campaign.json"))?, "frozen_sha256":digest(&directory.join("frozen.json"))?}));
    }
    let mut rows = Vec::new();
    for case in &order {
        for mode in MODES {
            for reader in readers {
                rows.push(entries.remove(&(case.to_string(),mode.into(),reader.into())).unwrap_or_else(|| json!({
                    "job":{"id":if mode=="open" { case.to_string() } else { format!("reuse.{case}") }, "case_id":case,"execution_mode":mode},
                    "reader_id":reader,"status":"not_run","gate":null,"measurements":null,"oracle":null,"campaign_id":null,"campaign_status":null})));
            }
        }
    }
    let complete: Vec<_> = order
        .iter()
        .filter(|case| {
            rows.iter()
                .filter(|row| row["job"]["case_id"] == **case)
                .all(|row| {
                    matches!(row["status"].as_str(), Some("success" | "unsupported"))
                        && row["campaign_status"] == "complete"
                })
        })
        .copied()
        .collect();
    let complete_coverage = complete.len() == cases.len();
    let mut result = identity;
    result.as_object_mut().ok_or("invalid report identity")?.extend(object(&json!({"format":"selective-read-production-report-v1",
        "status":if complete_coverage { "complete" } else { "incomplete" }, "formal_coverage_complete":complete_coverage,
        "complete_cases":complete,"publication_ready":false,"readers":readers,"conditions":shared,
        "reader_build_sha256":build_hashes,"reader_artifacts":builds,
        "definition":fs::canonicalize(&definition)?,"cases":cases,"campaigns":sources,"rows":rows}))?.clone());
    fs::create_dir(&output)?;
    save(&output.join("production-report.json"), &result)?;
    crate::artifacts::write_new_file(
        &output.join("README.md"),
        markdown(&result, &order)?.as_bytes(),
    )?;
    Ok(result)
}

fn seconds(row: &Value, key: &str) -> Result<String> {
    let metric = &row["measurements"]["metrics"][key];
    if metric.is_null() {
        return Ok("-".into());
    }
    let time = |key: &str| -> Result<f64> {
        Ok(metric[key].as_f64().ok_or("invalid timing quartile")? / 1e9)
    };
    Ok(format!(
        "{:.3} [{:.3}, {:.3}]",
        time("median")?,
        time("q1")?,
        time("q3")?
    ))
}

fn markdown(result: &Value, order: &[&str]) -> Result<String> {
    let mut lines = vec!["# Staged Q2/Q4 benchmark report".to_owned(), "".into(),
        format!("Completed formal cases: {}/8. Publication ready: false.", array(&result["complete_cases"])?.len()), "".into(),
        "Times show median [Q1, Q3] in seconds from five independent invocations.".into(),
        "The bracketed range covers the middle 50% of timings. Reuse retains one native source for two queries;".into(),
        "initialization plus query 1 and query 2 are reported separately. Missing results have no timing value.".into(), "".into(),
        "| Case | Files | Stored columns | Output columns | Parquet GiB |".into(), "| --- | ---: | ---: | ---: | ---: |".into()];
    for case in order {
        let row = &result["cases"][case];
        let geometry = &row["geometry"];
        lines.push(format!(
            "| {case} | {} | {} | {} | {:.2} |",
            geometry["files"],
            geometry["stored_columns"],
            row["projection_columns"],
            geometry["physical_bytes"]
                .as_f64()
                .ok_or("invalid Parquet bytes")?
                / 1024f64.powi(3)
        ));
    }
    lines.extend(["", "DV snapshots share their base snapshot's Parquet data. Table bytes describe the physical layout, not traffic.", "",
        "| Case | Reader | Open status | Open seconds | Reuse status | Init + query 1 seconds | Query 2 seconds |",
        "| --- | --- | --- | ---: | --- | ---: | ---: |"].map(str::to_owned));
    for case in order {
        for reader in array(&result["readers"])? {
            let reader = text(reader)?;
            let entry = |mode| -> Result<&Value> {
                array(&result["rows"])?
                    .iter()
                    .find(|row| {
                        row["job"]["case_id"] == *case
                            && row["job"]["execution_mode"] == mode
                            && row["reader_id"] == reader
                    })
                    .ok_or_else(|| "missing report row".into())
            };
            let (opened, reused) = (entry("open")?, entry("reuse")?);
            lines.push(format!(
                "| {case} | {reader} | {} | {} | {} | {} | {} |",
                text(&opened["status"])?,
                seconds(opened, "open_query_ns")?,
                text(&reused["status"])?,
                seconds(reused, "initialization_plus_query1_ns")?,
                seconds(reused, "query_1.completion_ns")?
            ));
        }
    }
    lines.extend(["", "The JSON report retains IQRs, sample statuses, eligible ratios, exact-gate evidence, file/group/page geometry",
        "and I/O diagnostics outside timing. An incomplete campaign remains visible and cannot complete a case.", "",
        "Each campaign declares whether plans share its untimed I/O invocation. DuckDB EXPLAIN remains separate;",
        "the JSON retains diagnostic modes and plan hashes, including I/O collected in exact gates when declared.",
        "Diagnostic sessions are excluded from timing distributions.", "",
        "Warmup may come from the exact validation gate or a separate invocation, as declared per campaign.",
        "Compare readers within the same case and declared warming method; samples are not pooled across methods.", "",
        "Matching cases and query profiles are taken from one audited campaign each. Samples are never pooled across retries.", ""].map(str::to_owned));
    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests;
