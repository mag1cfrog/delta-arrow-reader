use super::*;
use crate::{
    command::wait_with_timeout, selective_read::experiment::Process, selective_read_build::digest,
};
use rustix::process::{Resource, getrlimit};
use std::{
    os::unix::process::CommandExt,
    process::{Command, Stdio},
    time::Duration,
};

mod duckdb;
mod polars;
mod spark;

#[derive(clap::Args)]
pub(crate) struct Options {
    #[arg(long)]
    binary: PathBuf,
    #[arg(long)]
    fixtures: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    python: PathBuf,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

struct Capabilities {
    binary: PathBuf,
    corpus: PathBuf,
    file_limit: Option<u64>,
    check: Checker,
}

impl Capabilities {
    fn start(args: &Options) -> Result<Self> {
        let harness = fs::canonicalize(&args.harness)?;
        let output = fs::canonicalize(&args.output)?;
        Ok(Self {
            binary: fs::canonicalize(&args.binary)?,
            file_limit: (read_json(&args.binary.with_file_name("build.json"))?["reader_id"]
                == "spark")
                .then(|| {
                    getrlimit(Resource::Fsize)
                        .current
                        .unwrap_or(u64::MAX)
                        .min(256 * 1024 * 1024)
                }),
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
        probe(
            &self.binary,
            payload,
            &self.check.output.join(name),
            None,
            self.file_limit,
        )
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
    file_limit: Option<u64>,
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
    invoke::apply_export_limit(&mut command, file_limit);
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

pub(in crate::selective_read) fn run(args: Options) -> Result<()> {
    let build = read_json(&args.binary.with_file_name("build.json"))?;
    let reader = text(&build["reader_id"])?;
    require(
        matches!(reader, "duckdb" | "polars" | "spark"),
        "expected a DuckDB, Polars or Spark capability build",
    )?;
    fs::create_dir(&args.output)?;
    if reader != "spark" {
        super::local(super::Args {
            binaries: vec![args.binary.clone()],
            fixtures: args.fixtures.clone(),
            output: args.output.join("public-cases"),
            python: args.python.clone(),
            harness: args.harness.clone(),
        })?;
    }
    let mut checks = Capabilities::start(&args)?;
    let mut observations = checks.delta()?;
    match reader {
        "duckdb" => checks.duckdb(&args.python, &mut observations)?,
        "polars" => checks.polars(&args.harness, &mut observations)?,
        _ => checks.spark(&args.harness, &mut observations)?,
    }
    let mut summary = json!({"status":"passed"});
    if reader == "spark" {
        summary["invocations"] = json!(observations.len());
        summary["pilot_only"] = json!(true);
    } else {
        let public =
            read_json(&args.output.join("public-cases/checks.json"))?["invocations"].clone();
        summary["public_contract_invocations"] = public.clone();
        if reader == "duckdb" {
            summary["capability_invocations"] = json!(observations.len());
            summary["total_invocations"] = json!(count(&public)? + observations.len() as u64);
        } else {
            summary["capability_checks"] = json!(observations.len());
        }
    }
    let mut result = summary.clone();
    result.as_object_mut().unwrap().extend(super::super::object(&json!({
        "reader":reader,"build_sha256":digest(&args.binary.with_file_name("build.json"))?,
        "corpus_manifest_sha256":digest(&checks.corpus.join("manifest.json"))?,
        "controller_sha256":digest(&std::env::current_exe()?)?,
        "scope":"bounded fixtures; rerun against exact campaign fixtures","observations":observations}))?.clone());
    if reader == "spark" {
        result["publication_ready"] = json!(false);
        result["scope"] =
            json!("bounded exact-value capability checks; not a performance campaign");
        result["max_file_size_bytes"] = json!(checks.file_limit);
    }
    save(&args.output.join("capabilities.json"), &result)?;
    println!("{summary}");
    Ok(())
}

#[cfg(test)]
mod tests;
