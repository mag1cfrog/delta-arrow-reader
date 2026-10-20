use std::{
    collections::BTreeMap,
    fs::{self, File},
    os::unix::process::{CommandExt, ExitStatusExt},
    path::PathBuf,
    process::Command,
};

use rustix::process::{Resource, getrlimit, setrlimit};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    campaign::evidence::comparison,
    require,
    storage::{read_json, save, timestamp_ns},
    supervise::{self, progress},
};
use crate::Result;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    pub(super) binary: PathBuf,
    pub(super) payload: Value,
    pub(super) output: PathBuf,
    pub(super) reference: Option<PathBuf>,
    pub(super) env: Option<BTreeMap<String, String>>,
    pub(super) command_prefix: Vec<String>,
    pub(super) supervised: bool,
    pub(super) protocols: PathBuf,
}

fn export_limit(request: &Request) -> Result<Option<u64>> {
    let payload = &request.payload;
    if payload["purpose"] != "validation"
        || !matches!(payload["comparison_revision"].as_u64(), Some(3..=6))
    {
        return Ok(None);
    }
    let reference = request
        .reference
        .as_ref()
        .ok_or("validation requires a reference")?;
    let metadata = read_json(&reference.join("reference.json"))?;
    require(
        comparison(&metadata, &request.protocols)? == comparison(payload, &request.protocols)?,
        "export/reference workload mismatch",
    )?;
    // Half covers session exports; the other half covers the reference and sort spill.
    let limit = metadata["oracle_limits"]["disk_bytes"]
        .as_u64()
        .and_then(|bytes| bytes.checked_sub(8 * 1024 * 1024))
        .ok_or("insufficient validation export allowance")?
        / (2 * progress::count(payload)? as u64);
    require(limit > 0, "insufficient validation export allowance")?;
    Ok(Some(
        getrlimit(Resource::Fsize)
            .current
            .map_or(limit, |previous| previous.min(limit)),
    ))
}

pub(super) fn apply_export_limit(command: &mut Command, limit: Option<u64>) {
    if let Some(limit) = limit {
        let mut resource = getrlimit(Resource::Fsize);
        resource.current = Some(
            resource
                .current
                .map_or(limit, |previous| previous.min(limit)),
        );
        // SAFETY: setrlimit only changes the child resource limit before exec.
        unsafe {
            command.pre_exec(move || Ok(setrlimit(Resource::Fsize, resource)?));
        }
    }
}

pub(super) fn run(request: &Request) -> Result<Value> {
    fs::create_dir(&request.output)?;
    let output = fs::canonicalize(&request.output)?;
    let payload = &request.payload;
    save(&output.join("request.json"), payload)?;
    for name in ["stdout.jsonl", "stderr.log"] {
        File::create_new(output.join(name))?;
    }
    let export_bytes = export_limit(request)?;
    let mut command = request.command_prefix.clone();
    command.extend(
        [
            fs::canonicalize(&request.binary)?,
            output.join("request.json"),
            output.join("reader"),
        ]
        .iter()
        .map(|path| path.to_string_lossy().into_owned()),
    );
    let started = timestamp_ns()?;
    let (returncode, supervision) = if request.supervised {
        let result = supervise::launch(supervise::Launch {
            command,
            payload: payload.clone(),
            output: output.clone(),
            stdout: output.join("stdout.jsonl"),
            stderr: output.join("stderr.log"),
            env: request.env.clone(),
            query_seconds: 1800.0,
            cleanup_seconds: 60.0,
            export_limit: export_bytes,
        })?;
        save(&output.join("process.json"), &result)?;
        (result["returncode"].as_i64(), Some(result))
    } else {
        let mut child = Command::new(&command[0]);
        child
            .args(&command[1..])
            .stdout(
                File::options()
                    .append(true)
                    .open(output.join("stdout.jsonl"))?,
            )
            .stderr(
                File::options()
                    .append(true)
                    .open(output.join("stderr.log"))?,
            );
        if let Some(env) = &request.env {
            child.env_clear().envs(env);
        }
        apply_export_limit(&mut child, export_bytes);
        let status = child.status()?;
        (
            Some(
                status
                    .code()
                    .unwrap_or_else(|| -status.signal().unwrap_or(0)) as i64,
            ),
            None,
        )
    };
    let exited = timestamp_ns()?;
    let path = output.join("reader/record.json");
    let mut record = if path.exists() {
        let read = read_json(&path).and_then(|record| {
            require(
                record.is_object() && record["status"].is_string(),
                "missing observation status",
            )?;
            Ok(record)
        });
        match read {
            Ok(record) => record,
            Err(error) => {
                json!({"status":"operational_failure","failure_reason":format!("malformed reader record: {error}")})
            }
        }
    } else {
        let error = fs::read_to_string(output.join("stderr.log"))?;
        let rejected = serde_json::from_str::<Value>(&error)
            .is_ok_and(|error| error["status"] == "invalid_input");
        json!({"status":if rejected {"invalid_input"} else {"operational_failure"},
            "failure_reason":if error.is_empty() {"reader exited without an observation record".into()} else {error}})
    };
    if returncode.is_some_and(|code| code != 0) && record["status"] == "success" {
        record["status"] = json!("operational_failure");
        record["failure_reason"] = json!(format!(
            "reader exited with status {} after writing its record",
            returncode.unwrap()
        ));
    }
    if let Some(bytes) = export_bytes {
        record["validation_export_limit_bytes_per_file"] = json!(bytes);
    }
    if let Some(supervision) = supervision {
        if supervision["status"] != "success" {
            record["status"] = supervision["status"].clone();
            record["failure_reason"] = supervision["failure_reason"].clone();
        }
        if record["status"] == "success"
            && (supervision["last_phase"] != "cleanup"
                || super::array(&supervision["completed_queries"])?.len()
                    != progress::count(payload)?)
        {
            record["status"] = json!("operational_failure");
            record["failure_reason"] = json!("reader did not complete the watchdog lifecycle");
        }
        if record["cleanup_ns"]
            .as_u64()
            .is_some_and(|ns| ns > 60_000_000_000)
        {
            record["status"] = json!("timeout");
            record["failure_reason"] = json!("reported cleanup exceeded its deadline");
        }
        let record_map = record.as_object_mut().unwrap();
        record_map
            .entry("queries")
            .or_insert_with(|| supervision["completed_queries"].clone());
        let external = record_map
            .entry("external_metrics")
            .or_insert_with(|| json!({}));
        require(external.is_object(), "invalid external metrics")?;
        for key in [
            "process_user_cpu_ns",
            "process_system_cpu_ns",
            "process_cpu_ns",
            "peak_rss_bytes",
            "process_elapsed_ns",
        ] {
            external[key] = supervision[key].clone();
        }
        record["supervision"] = supervision;
    }
    if matches!(payload["purpose"].as_str(), Some("diagnostic" | "io"))
        || payload["validation_diagnostics"] == true
    {
        record["diagnostic_process"] = json!({"started_ns":started,"exited_ns":exited});
    }
    Ok(record)
}

#[cfg(test)]
mod tests;
