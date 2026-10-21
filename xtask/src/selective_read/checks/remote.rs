use super::*;
use crate::selective_read::{experiment::median, object, observe, storage};
use crate::selective_read_build::digest;

#[derive(clap::Args)]
pub(crate) struct StorageArgs {
    #[command(flatten)]
    common: super::Args,
    #[arg(long)]
    state: PathBuf,
}

struct Remote {
    check: Checker,
    state: PathBuf,
}

impl Remote {
    fn invoke(
        &mut self,
        binary: &Path,
        payload: &Value,
        name: &str,
        reference: Option<&Path>,
        traced: Option<bool>,
    ) -> Result<Value> {
        let _lock = storage::exclusive(&self.state)?;
        let output = self.check.output.join(name);
        let record = observe::run(serde_json::from_value(
            json!({"action":"invoke","directory":self.state,
            "binary":binary,"payload":payload,"output":output,"reference":reference,"traced":traced,"protocols":self.check.protocols}),
        )?)?;
        let record = self.check.validate(record, payload, &output, reference)?;
        save(&output.join("storage-observation.json"), &record)?;
        Ok(record)
    }
}

fn remote_io(result: &Value, expected: &Value, empty: bool) -> Result<()> {
    success(result)?;
    require(
        result["storage_capture"]["status"] == "passed"
            && result["external_resource_limits"]["process_memory_bytes"] == 8 * 1024u64.pow(3)
            && result["external_resource_limits"]["swap_bytes"] == 0,
        "missing remote capture or process limits",
    )?;
    require(
        array(&result["queries"])?
            .iter()
            .all(|q| q["output_rows"] == *expected && q["physical_plan"].is_null())
            && result["provider_evidence"].is_null()
            && count(&result["diagnostic_session_ns"])? > 0,
        "invalid remote output or mixed diagnostic evidence",
    )?;
    require(
        count(&result["storage_io"]["by_class"]["delta_log"]["requests"])? > 0,
        "no remote Delta log requests",
    )?;
    if !empty {
        require(
            !array(&result["storage_io"]["parquet_get_objects"])?.is_empty(),
            "no remote Parquet GET",
        )?;
    }
    require(
        count(&result["storage_capture"]["started_ns"])?
            <= count(&result["diagnostic_process"]["started_ns"])?
            && count(&result["storage_capture"]["drained_ns"])?
                >= count(&result["diagnostic_process"]["exited_ns"])?,
        "storage capture missed part of the reader lifecycle",
    )
}

fn overhead(reader: &str, off: &[u64], on: &[u64]) -> Result<Value> {
    require(
        off.len() == 2 && on.len() == 2,
        "incomplete ABBA observer calibration",
    )?;
    let a = median(off.iter().map(|n| *n as f64).collect())?;
    let b = median(on.iter().map(|n| *n as f64).collect())?;
    require(a > 0.0 && b > 0.0, "invalid observer calibration clocks")?;
    Ok(
        json!({"reader":reader,"order":[false,true,false,true,true,false],"warmups":2,"untraced_session_ns":off,
        "traced_session_ns":on,"median_difference_ns":b-a,"median_ratio":b/a,
        "interpretation":"bounded diagnostic overhead check, not a performance estimate"}),
    )
}

fn secrets(root: &Path, secret: &[u8]) -> Result<()> {
    require(!secret.is_empty(), "empty storage credential")?;
    for path in crate::repository::files(root)? {
        if path
            .extension()
            .is_some_and(|ext| ["json", "jsonl", "log"].iter().any(|value| ext == *value))
        {
            let bytes = fs::read(&path)?;
            let authorization = b"AWS4-HMAC-SHA256 Credential=";
            require(
                !bytes.windows(secret.len()).any(|part| part == secret)
                    && !bytes
                        .windows(authorization.len())
                        .any(|part| part == authorization),
                &format!("storage credentials leaked into {}", path.display()),
            )?;
        }
    }
    Ok(())
}

pub(crate) fn run(args: StorageArgs) -> Result<()> {
    let state = fs::canonicalize(&args.state)?;
    let mut remote = Remote {
        check: Checker::start(&args.common)?,
        state,
    };
    storage::verify_server(&remote.state)?;
    let known = observe::run(observe::Request::CheckRequests {
        directory: remote.state.clone(),
        output: remote.check.output.join("known"),
    })?;
    let receipt = storage::upload::run(
        &serde_json::from_value(
            json!({"directory":remote.state,"fixtures":remote.check.fixtures,
        "output":remote.check.output.join("upload.json"),"table_ids":null,"checksum":false}),
        )?,
        storage::upload::hashes,
    )?;
    let table_root = text(&receipt["table_root"])?;
    let mut failed =
        remote
            .check
            .request("li.clustered.eq2-in20", "open", "io", "failed-reader")?;
    failed["table_uri"] = json!(format!("{table_root}/li.clustered"));
    let failed = remote.invoke(
        Path::new("/bin/false"),
        &failed,
        "failed-reader",
        None,
        None,
    )?;
    require(
        failed["status"] == "operational_failure"
            && failed["storage_capture"]["status"] == "passed"
            && failed["storage_io"]["requests"] == 0,
        "failed reader lost its zero-request capture",
    )?;
    let cases = [
        "li.clustered.eq2-in20",
        "li.shuffled.eq2-in20",
        "li.clustered.empty",
        "li.clustered.date7-limit",
        "wide.clustered.eq2-in20",
    ];
    let references = remote.check.references(&cases)?;
    let corpus_root = fs::canonicalize(args.common.harness.join(
        "../../crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus",
    ))?;
    let corpus = read_json(&corpus_root.join("manifest.json"))?;
    let dv = array(&corpus["fixtures"])?
        .iter()
        .find(|f| f["name"] == "deletion_vectors")
        .ok_or("missing Spark DV corpus")?;
    let corpus_hash = digest(&corpus_root.join("manifest.json"))?;
    let dv_root = format!("_checks/{corpus_hash}");
    {
        let _lock = storage::exclusive(&remote.state)?;
        for (name, expected) in object(&dv["files"])? {
            let source = corpus_root.join("deletion_vectors").join(name);
            require(
                expected["bytes"] == fs::metadata(&source)?.len()
                    && expected["sha256"] == digest(&source)?,
                "changed DV corpus file",
            )?;
            if name.starts_with("table/") {
                storage::put_verified(
                    &remote.state,
                    &format!("{dv_root}/{name}"),
                    &source,
                    text(&expected["sha256"])?,
                    false,
                )?;
            }
        }
    }
    let mut records = Vec::new();
    let mut overheads = Vec::new();
    for binary in &args.common.binaries {
        let build = read_json(&binary.with_file_name("build.json"))?;
        let reader = text(&build["reader_id"])?;
        for case in cases {
            let uri = format!(
                "{table_root}/{}",
                case.rsplit_once('.').ok_or("invalid case ID")?.0
            );
            for mode in if case == cases[0] {
                vec!["open", "reuse"]
            } else {
                vec!["open"]
            } {
                let name = format!("{reader}-{case}-{mode}");
                let mut payload = remote.check.request(case, mode, "validation", &name)?;
                payload["table_uri"] = json!(uri);
                let validation = format!("{name}-validation");
                let (reference, expected) = &references[case];
                let validated =
                    remote.invoke(binary, &payload, &validation, Some(reference), None)?;
                success(&validated)?;
                require(
                    validated["correctness"]["status"] == "passed"
                        && validated["storage_capture"]["status"] == "disabled",
                    "remote correctness failed or validation was traced",
                )?;
                payload["purpose"] = json!("io");
                let observed =
                    remote.invoke(binary, &payload, &format!("{name}-io"), None, None)?;
                remote_io(&observed, &expected["output_rows"], case.ends_with("empty"))?;
                records.push(json!({"reader":reader,"case":case,"mode":mode,"status":"passed","io":observed["storage_io"]}));
                if case == cases[0] {
                    payload["purpose"] = json!("timing");
                    payload["run_id"] = json!(format!("{name}-timing"));
                    payload["correctness_file"] = json!(
                        remote
                            .check
                            .output
                            .join(&validation)
                            .join("correctness.json")
                    );
                    let measured =
                        remote.invoke(binary, &payload, &format!("{name}-timing"), None, None)?;
                    success(&measured)?;
                    require(
                        measured["storage_capture"]["status"] == "disabled"
                            && measured["external_metrics"]["response_bytes"].is_null(),
                        "remote timing was traced",
                    )?;
                }
            }
        }
        let name = format!("{reader}-dv");
        let mut payload = remote.check.request(cases[0], "open", "io", &name)?;
        payload["table_uri"] = json!(format!("s3://{}/{dv_root}/table", storage::BUCKET));
        payload["snapshot_version"] = json!(1);
        payload["case_id"] = json!("probe.spark-dv");
        payload["canonical_sql"] = json!("SELECT id, value, label FROM bench");
        payload["fixture_manifest_sha256"] = json!(corpus_hash);
        let result = remote.invoke(binary, &payload, &name, None, None)?;
        success(&result)?;
        require(
            result["storage_capture"]["status"] == "passed"
                && result["queries"][0]["output_rows"] == 9
                && count(&result["storage_io"]["by_class"]["deletion_vector"]["response_bytes"])?
                    > 0,
            "remote DV sidecar traffic or row count missing",
        )?;
        records.push(json!({"reader":reader,"case":"probe.spark-dv","status":result["status"],"io":result["storage_io"]}));
        let mut samples = [Vec::new(), Vec::new()];
        for (index, traced) in [false, true, false, true, true, false]
            .into_iter()
            .enumerate()
        {
            let name = format!("{reader}-overhead-{index}");
            let mut payload = remote.check.request(cases[0], "open", "io", &name)?;
            payload["table_uri"] = json!(format!("{table_root}/li.clustered"));
            let result = remote.invoke(binary, &payload, &name, None, Some(traced))?;
            success(&result)?;
            if index >= 2 {
                samples[usize::from(traced)].push(count(&result["diagnostic_session_ns"])?);
            }
        }
        let overhead = overhead(reader, &samples[0], &samples[1])?;
        println!(
            "{}",
            json!({"reader":reader,"status":"passed","median_observer_ratio":overhead["median_ratio"]})
        );
        overheads.push(overhead);
    }
    let credentials = storage::credentials(&remote.state)?;
    secrets(
        &remote.check.output,
        text(&credentials["secret_key"])?.as_bytes(),
    )?;
    let builds = args
        .common
        .binaries
        .iter()
        .map(|binary| {
            Ok((
                binary.to_string_lossy().into_owned(),
                json!(digest(&binary.with_file_name("build.json"))?),
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    save(
        &remote.check.output.join("checks.json"),
        &json!({"status":"passed",
        "known":known,"readers":records,"observer_overhead":overheads,
        "server_sha256":digest(&remote.state.join("server.json"))?,"fixture_manifest_sha256":digest(&remote.check.fixtures.join("manifest.json"))?,
        "builds":builds}),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
