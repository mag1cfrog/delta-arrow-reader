use super::*;
use crate::{
    command::wait_with_timeout, selective_read::experiment::Process, selective_read_build::digest,
};
use serde::Deserialize;
use std::{
    os::unix::process::CommandExt,
    process::{Command, Stdio},
    time::Duration,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::selective_read) struct Delta {
    binary: PathBuf,
    fixtures: PathBuf,
    output: PathBuf,
    python: PathBuf,
    harness: PathBuf,
}

struct Capabilities {
    binary: PathBuf,
    corpus: PathBuf,
    check: Checker,
}

impl Capabilities {
    fn start(args: &Delta) -> Result<Self> {
        let harness = fs::canonicalize(&args.harness)?;
        let output = fs::canonicalize(&args.output)?;
        Ok(Self {
            binary: fs::canonicalize(&args.binary)?,
            corpus: fs::canonicalize(harness.join(
                "../../crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus",
            ))?,
            check: Checker {
                fixtures: fs::canonicalize(&args.fixtures)?,
                output: output.clone(),
                protocols: harness.join("../../docs/content/benchmarks"),
                worker: Worker::start(&args.python, &harness, &output)?,
            },
        })
    }

    fn probe(&self, payload: &Value, name: &str) -> Result<Value> {
        probe(&self.binary, payload, &self.check.output.join(name), None)
    }

    fn exported(
        &mut self,
        record: &Value,
        name: &str,
        deleted: bool,
        ids: Option<&[u64]>,
    ) -> Result<Value> {
        self.check
            .worker
            .call(&json!({"action":"capability-export","record":record,
            "output":self.check.output.join(name),"deleted":deleted,"ids":ids}))
    }

    fn delta(&mut self) -> Result<Vec<Value>> {
        let manifest = read_json(&self.corpus.join("manifest.json"))?;
        let fixture = array(&manifest["fixtures"])?
            .iter()
            .find(|f| f["name"] == "deletion_vectors")
            .ok_or("missing external DV corpus")?;
        let original = self.corpus.join("deletion_vectors");
        for (name, expected) in super::super::object(&fixture["files"])? {
            let path = original.join(name);
            require(
                expected["bytes"] == fs::metadata(&path)?.len()
                    && expected["sha256"] == digest(&path)?,
                "external DV corpus changed",
            )?;
        }
        let (no_dv, unknown, missing) = stage(&original.join("table"), &self.check.output)?;
        let mut base =
            self.check
                .request("li.clustered.eq2-in20", "open", "validation", "probe")?;
        // Corpus checks cannot authorize a public-case timing certificate.
        base["fixture_manifest_sha256"] = json!(digest(&self.corpus.join("manifest.json"))?);
        base["case_id"] = json!("probe.spark-dv");
        let mut observations = Vec::new();
        for mode in ["open", "reuse"] {
            for (feature, table, version) in [
                ("no-dv", no_dv.clone(), 0),
                ("feature-only", original.join("table"), 0),
                ("real-dv", original.join("table"), 1),
            ] {
                for (predicate, ids) in [
                    ("", None),
                    (" WHERE id IN (2, 5, 12)", Some(&[2, 5, 12][..])),
                    (" WHERE id IN (1, 2, 5, 6, 12)", Some(&[1, 2, 5, 6, 12][..])),
                ] {
                    if feature == "no-dv" && ids.is_some() {
                        continue;
                    }
                    let name = format!("{mode}-{feature}-{}", observations.len());
                    let mut payload = base.clone();
                    payload["table_uri"] = json!(uri(&table)?);
                    payload["snapshot_version"] = json!(version);
                    payload["canonical_sql"] =
                        json!(format!("SELECT id, value, label FROM bench{predicate}"));
                    payload["execution_mode"] = json!(mode);
                    payload["run_id"] = json!(name);
                    let record = self.probe(&payload, &name)?;
                    success(&record)?;
                    let rows = self.exported(&record, &name, feature == "real-dv", ids)?;
                    require(
                        array(&record["queries"])?.len() == if mode == "reuse" { 10 } else { 1 },
                        "incomplete capability query sequence",
                    )?;
                    observations.push(json!({"path":name,"status":"passed","mode":mode,"feature":feature,
                        "version":version,"predicate":predicate,"output_rows":rows,"reason":record["failure_reason"]}));
                }
            }
        }
        for mode in ["open", "reuse"] {
            for (feature, table, version, status) in [
                (
                    "wrong-version",
                    original.join("table"),
                    999,
                    "operational_failure",
                ),
                ("unsupported", unknown.clone(), 0, "unsupported"),
                ("missing-dv", missing.clone(), 1, "operational_failure"),
            ] {
                let name = format!("{mode}-{feature}");
                let mut payload = base.clone();
                payload["table_uri"] = json!(uri(&table)?);
                payload["snapshot_version"] = json!(version);
                payload["canonical_sql"] = json!("SELECT * FROM bench");
                payload["execution_mode"] = json!(mode);
                payload["run_id"] = json!(name);
                let record = self.probe(&payload, &name)?;
                require(
                    record["status"] == status && !text(&record["failure_reason"])?.is_empty(),
                    "incorrect Delta failure classification",
                )?;
                if mode == "reuse" && feature == "wrong-version" {
                    require(
                        record["phase"] == "snapshot_open" && array(&record["queries"])?.is_empty(),
                        "missing snapshot reached query execution",
                    )?;
                }
                observations
                    .push(json!({"path":name,"status":status,"reason":record["failure_reason"]}));
            }
        }
        Ok(observations)
    }
}

fn uri(path: &Path) -> Result<String> {
    Ok(url::Url::from_file_path(fs::canonicalize(path)?)
        .map_err(|_| "invalid local table URI")?
        .into())
}

fn probe(
    binary: &Path,
    payload: &Value,
    output: &Path,
    env: Option<&BTreeMap<String, String>>,
) -> Result<Value> {
    fs::create_dir(output)?;
    let output = fs::canonicalize(output)?;
    save(&output.join("request.json"), payload)?;
    let mut command = Command::new(fs::canonicalize(binary)?);
    command
        .arg(output.join("request.json"))
        .arg(output.join("reader"))
        .stdin(Stdio::null())
        .stdout(fs::File::create_new(output.join("stdout.jsonl"))?)
        .stderr(fs::File::create_new(output.join("stderr.log"))?)
        .process_group(0);
    if let Some(env) = env {
        command.envs(env);
    }
    let mut process = Process(command.spawn()?);
    let status = wait_with_timeout(&mut process.0, Duration::from_secs(600))?;
    let path = output.join("reader/record.json");
    let record = if path.exists() {
        read_json(&path)?
    } else {
        json!({"status":"invalid_input","failure_reason":fs::read_to_string(output.join("stderr.log"))?})
    };
    require(
        status.success() == (record["status"] == "success"),
        "reader exit and capability status disagree",
    )?;
    Ok(record)
}

fn rewrite_log(table: &Path, change: impl Fn(&mut Value)) -> Result<()> {
    let path = table.join("_delta_log/00000000000000000000.json");
    let mut rewritten = String::new();
    for line in fs::read_to_string(&path)?.lines() {
        let mut action: Value = serde_json::from_str(line)?;
        change(&mut action);
        rewritten.push_str(&serde_json::to_string(&action)?);
        rewritten.push('\n');
    }
    fs::write(path, rewritten)?;
    Ok(())
}

fn stage(original: &Path, output: &Path) -> Result<(PathBuf, PathBuf, PathBuf)> {
    let no_dv = output.join("no-dv");
    copy_directory(original, &no_dv)?;
    fs::remove_file(no_dv.join("_delta_log/00000000000000000001.json"))?;
    rewrite_log(&no_dv, |action| {
        if let Some(protocol) = action.get_mut("protocol") {
            *protocol = json!({"minReaderVersion":1,"minWriterVersion":2});
        }
        if let Some(config) = action
            .pointer_mut("/metaData/configuration")
            .and_then(Value::as_object_mut)
        {
            config.remove("delta.enableDeletionVectors");
        }
    })?;
    let unknown = output.join("unsupported-feature");
    copy_directory(original, &unknown)?;
    rewrite_log(&unknown, |action| {
        if let Some(protocol) = action.get_mut("protocol") {
            for key in ["readerFeatures", "writerFeatures"] {
                if let Some(features) = protocol[key].as_array_mut() {
                    features.push(json!("unknownBenchmarkProbe"));
                }
            }
        }
    })?;
    let missing = output.join("missing-dv");
    copy_directory(original, &missing)?;
    let sidecar = fs::read_dir(&missing)?
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .find(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("deletion_vector_") && n.ends_with(".bin"))
        })
        .ok_or("missing corpus DV sidecar")?;
    fs::remove_file(sidecar.path())?;
    Ok((no_dv, unknown, missing))
}

pub(in crate::selective_read) fn delta(args: Delta) -> Result<Value> {
    Ok(json!(Capabilities::start(&args)?.delta()?))
}

#[cfg(test)]
mod tests;
