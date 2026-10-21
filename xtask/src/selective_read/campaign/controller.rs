use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use serde_json::{Map, Value, json};

use super::{array, evidence, object, require, roster, schedule, summarize, text};
use crate::{
    Result,
    repository::files,
    selective_read::{
        network, observe, request, storage,
        validation::{Affinity, Worker},
        workload,
    },
    selective_read_build::digest,
};
use storage::{read_json, save, timestamp_ns};

#[derive(clap::Args)]
pub(crate) struct Args {
    #[arg(long)]
    state: PathBuf,
    #[arg(long)]
    fixtures: PathBuf,
    #[arg(long)]
    upload: PathBuf,
    #[arg(long)]
    output: PathBuf,
    /// Pinned Python environment containing the independent PyArrow oracle.
    #[arg(long)]
    python: PathBuf,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
    #[arg(long)]
    binary: Vec<PathBuf>,
    #[arg(long)]
    reference: Vec<PathBuf>,
    #[arg(long)]
    case: Vec<String>,
    /// Upload these native table IDs to a new receipt before running.
    #[arg(long)]
    upload_table: Vec<String>,
    #[arg(long, conflicts_with = "workload")]
    matrix: Option<PathBuf>,
    #[arg(long)]
    workload: Option<PathBuf>,
    #[arg(long)]
    combined_diagnostics: bool,
    #[arg(long)]
    gate_warmup: bool,
    #[arg(long)]
    gate_diagnostics: bool,
    #[arg(long, conflicts_with = "no_sessions")]
    session: Vec<String>,
    #[arg(long)]
    no_sessions: bool,
}

struct Selection {
    cases: BTreeMap<String, Value>,
    references: BTreeMap<String, PathBuf>,
    jobs: Vec<Value>,
}

fn select(
    args: &Args,
    prepared: &Value,
    workload: &Value,
    builds: &Value,
    manifest: &str,
) -> Result<Selection> {
    let mut cases = BTreeMap::new();
    let mut references = BTreeMap::new();
    let mut sessions = json!({"reuse.li":"li.clustered.eq2-in20",
        "reuse.wide":"wide.clustered.eq2-in20", "reuse.files4096":"files4096.eq2-in20"});
    if !prepared.is_null() {
        require(
            args.case.is_empty()
                && args.reference.is_empty()
                && args.session.is_empty()
                && !args.no_sessions,
            "--matrix supplies all cases and references; do not mix case/session overrides",
        )?;
        for (reader, build) in object(builds)? {
            if let Some(lock) = prepared["translation_locks"].get(reader) {
                require(
                    build["lockfile_sha256"] == *lock,
                    "native translation lock changed",
                )?;
            }
        }
        for row in array(&prepared["cases"])? {
            let case = text(&row["case_id"])?;
            cases.insert(case.to_owned(), row.clone());
            if row["status"] == "prepared" {
                references.insert(case.to_owned(), PathBuf::from(text(&row["reference"])?));
            }
        }
    }
    if !workload.is_null() {
        for row in array(&workload["cases"])? {
            let case = text(&row["case_id"])?;
            if if args.case.is_empty() {
                row["fixture_manifest_sha256"] == manifest
            } else {
                args.case.iter().any(|c| c == case)
            } {
                let mut row = row.clone();
                row["status"] = json!("prepared");
                row["native_expressions"] = json!(
                    object(&workload["translations"])?
                        .iter()
                        .map(|(reader, translation)| (
                            reader.clone(),
                            translation["expressions"][case]["native_expression"].clone()
                        ))
                        .collect::<Map<_, _>>()
                );
                cases.insert(case.to_owned(), row);
            }
        }
        require(
            !cases.is_empty()
                && (args.case.is_empty()
                    || args.case.iter().collect::<BTreeSet<_>>() == cases.keys().collect()),
            "unknown/empty workload case selection",
        )?;
        sessions = json!(
            object(&workload["sessions"])?
                .iter()
                .filter(|(_, case)| case.as_str().is_some_and(|case| cases.contains_key(case)))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Map<_, _>>()
        );
        require(
            args.session.iter().all(|s| sessions.get(s).is_some()),
            "reuse session is outside the selected workload cases",
        )?;
        for (reader, translation) in object(&workload["translations"])? {
            if let Some(build) = builds.get(reader) {
                require(
                    build["lockfile_sha256"] == translation["lock_sha256"],
                    "native translation lock changed",
                )?;
            }
        }
    }
    for reference in &args.reference {
        let metadata = read_json(&reference.join("reference.json"))?;
        require(
            references
                .insert(
                    text(&metadata["case_id"])?.to_owned(),
                    fs::canonicalize(reference)?,
                )
                .is_none(),
            "duplicate reference case",
        )?;
    }
    let open: BTreeSet<_> = if !cases.is_empty() {
        cases.keys().cloned().collect()
    } else if !args.case.is_empty() {
        args.case.iter().cloned().collect()
    } else {
        ["li.clustered.eq2-in20", "li.shuffled.eq2-in20"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    };
    let mut jobs: Vec<_> = open
        .into_iter()
        .map(|case| json!({"id":case, "case_id":case, "execution_mode":"open"}))
        .collect();
    if prepared.is_null() && !args.no_sessions {
        require(
            args.session.iter().all(|s| sessions.get(s).is_some()),
            "unknown reuse session",
        )?;
        let selected: BTreeSet<_> = if args.session.is_empty() {
            object(&sessions)?.keys().cloned().collect()
        } else {
            args.session.iter().cloned().collect()
        };
        jobs.extend(selected.into_iter().map(
            |session| json!({"id":session, "case_id":sessions[&session], "execution_mode":"reuse"}),
        ));
    }
    if args.gate_warmup || args.gate_diagnostics {
        require(
            cases.len() == 1
                && jobs
                    .iter()
                    .map(|j| j["execution_mode"].as_str().unwrap())
                    .collect::<BTreeSet<_>>()
                    == BTreeSet::from(["open", "reuse"]),
            "gate warmup requires one snapshot with both open and reuse profiles",
        )?;
    }
    Ok(Selection {
        cases,
        references,
        jobs,
    })
}

fn modes(args: &Args, comparison: &Value, protocols: &Path) -> Result<Value> {
    let mut config = comparison.clone();
    for (key, enabled, hash, file, mode) in [
        (
            "combined_diagnostics",
            args.combined_diagnostics || args.gate_diagnostics,
            "diagnostics_amendment_sha256",
            "combined-diagnostics",
            evidence::Mode::CombinedDiagnostics,
        ),
        (
            "gate_warmup",
            args.gate_warmup || args.gate_diagnostics,
            "warmup_amendment_sha256",
            "gate-warmup",
            evidence::Mode::GateWarmup,
        ),
        (
            "gate_diagnostics",
            args.gate_diagnostics,
            "gate_diagnostics_amendment_sha256",
            "gate-diagnostics",
            evidence::Mode::GateDiagnostics,
        ),
    ] {
        config[key] = json!(enabled);
        if enabled {
            config[hash] = json!(digest(
                &protocols.join(format!("selective-read-{file}.md"))
            )?);
        }
        mode.enabled(&config, protocols)?;
    }
    Ok(config)
}

fn source_hashes(args: &Args, config: &Value, protocols: &Path) -> Result<(Value, Value)> {
    let root = args
        .harness
        .parent()
        .and_then(Path::parent)
        .ok_or("invalid harness directory")?;
    let mut sources: Vec<_> = [
        "oracle_io.py",
        "runners/run.py",
        "runners/python_common.py",
        "runners/native.py",
    ]
    .into_iter()
    .map(|p| args.harness.join(p))
    .collect();
    sources.extend([
        std::env::current_exe()?,
        root.join("Cargo.lock"),
        root.join("xtask/Cargo.toml"),
        protocols.join("selective-read-protocol.md"),
    ]);
    sources.extend(
        files(&root.join("xtask/src"))?
            .into_iter()
            .filter(|p| p.extension().is_some_and(|e| e == "rs")),
    );
    let mut inputs = vec![args.fixtures.join("manifest.json"), args.upload.clone()];
    if !config["server"]["network"].is_null() {
        inputs.push(args.state.join("network.json"));
    }
    for (enabled, name) in [
        (
            config["combined_diagnostics"] == true,
            "combined-diagnostics",
        ),
        (config["gate_warmup"] == true, "gate-warmup"),
        (config["gate_diagnostics"] == true, "gate-diagnostics"),
    ] {
        if enabled {
            sources.push(protocols.join(format!("selective-read-{name}.md")));
        }
    }
    if let Some(matrix) = &args.matrix {
        sources.push(args.harness.join("matrix.py"));
        inputs.extend([args.harness.join("query-matrix.json"), matrix.clone()]);
    }
    if let Some(workload) = &args.workload {
        inputs.push(workload.clone());
        sources.extend([protocols.join("selective-read-large-workloads.md")]);
        if config["comparison_revision"].as_u64().unwrap_or(0) >= 4 {
            sources.push(protocols.join("selective-read-sampling.md"));
        }
        if matches!(config["comparison_revision"].as_u64(), Some(5 | 6)) {
            sources.extend([protocols.join(if config["comparison_revision"] == 6 {
                "selective-read-spark-matrix.md"
            } else {
                "selective-read-production-workloads.md"
            })]);
        }
    }
    Ok((hashes(&sources)?, hashes(&inputs)?))
}

fn hashes(paths: &[PathBuf]) -> Result<Value> {
    Ok(json!(
        paths
            .iter()
            .map(|p| Ok((p.to_string_lossy().into_owned(), json!(digest(p)?))))
            .collect::<Result<Map<_, _>>>()?
    ))
}

fn remote_request(
    mut payload: Value,
    fixtures: &Path,
    receipt: &Value,
    reference: &Path,
    comparison: &Value,
    protocols: &Path,
) -> Result<Value> {
    let location = url::Url::parse(text(&payload["table_uri"])?)?
        .to_file_path()
        .map_err(|()| "expected a local fixture URI")?;
    let location = location
        .strip_prefix(fixtures)?
        .to_str()
        .ok_or("invalid fixture path")?;
    require(
        array(&receipt["objects"])?.iter().any(|item| {
            item["path"]
                .as_str()
                .is_some_and(|p| p.starts_with(&format!("{location}/")))
        }),
        "table is absent from verified upload",
    )?;
    payload["table_uri"] = json!(format!("{}/{location}", text(&receipt["table_root"])?));
    let metadata = read_json(&reference.join("reference.json"))?;
    require(
        metadata["status"] == "complete"
            && [
                "fixture_manifest_sha256",
                "canonical_sql",
                "snapshot_version",
            ]
            .iter()
            .all(|key| metadata[*key] == payload[*key]),
        "reference differs from request",
    )?;
    require(
        evidence::same_comparison(&evidence::comparison(&metadata, protocols)?, comparison),
        "reference comparison/workload identity differs",
    )?;
    Ok(payload)
}

fn purpose(slot: &Value, combined: bool) -> Result<&'static str> {
    Ok(match text(&slot["stage"])? {
        "gate" => "validation",
        "warmup" | "timing" => "timing",
        "plan" => "diagnostic",
        "diagnostic" if combined && slot["reader_id"] != "duckdb" => "diagnostic",
        _ => "io",
    })
}

struct Campaign {
    args: Args,
    worker: Worker,
    selection: Selection,
    protocols: PathBuf,
    comparison: Value,
    config: Value,
    binaries: BTreeMap<String, PathBuf>,
    templates: BTreeMap<String, Value>,
    rows: Vec<Value>,
    journal: File,
    abort: Option<String>,
}

impl Campaign {
    fn invoke(&mut self, slot: &Value, job: &Value, gate: Option<&Value>) -> Result<Value> {
        let job_id = text(&job["id"])?;
        let run_id = text(&slot["run_id"])?;
        let reader = text(&slot["reader_id"])?;
        let stage = text(&slot["stage"])?;
        let purpose = purpose(slot, self.config["combined_diagnostics"] == true)?;
        let mut payload = self
            .templates
            .get(job_id)
            .ok_or("missing job template")?
            .clone();
        for (key, value) in [
            ("campaign_id", self.config["campaign_id"].clone()),
            ("run_id", json!(run_id)),
            ("purpose", json!(purpose)),
            ("repetition", slot["repetition"].clone()),
            ("order", slot["order"].clone()),
            (
                "correctness_file",
                gate.map(|g| g["correctness_file"].clone())
                    .unwrap_or(Value::Null),
            ),
        ] {
            payload[key] = value;
        }
        if self.config["gate_diagnostics"] == true && stage == "gate" {
            payload["validation_diagnostics"] = json!(true);
        }
        let destination = self.args.output.join(run_id);
        let record = if let Some(reason) = &self.abort {
            json!({"status":"not_run", "failure_reason":reason})
        } else {
            let invocation = (|| -> Result<Value> {
                let reference = self
                    .selection
                    .references
                    .get(text(&job["case_id"])?)
                    .ok_or("missing reference")?;
                let mut record = observe::run(serde_json::from_value(
                    json!({"action":"invoke", "directory":self.args.state,
                    "binary":self.binaries.get(reader).ok_or("missing reader binary")?, "payload":payload,
                    "output":destination, "traced":slot["traced"], "reference":reference, "protocols":self.protocols}),
                )?)?;
                if record["status"] == "success" && purpose == "validation" {
                    record = self.worker.check(
                        record,
                        &payload,
                        &destination,
                        &self.args.fixtures,
                        reference,
                    )?;
                }
                save(&destination.join("storage-observation.json"), &record)?;
                if record["cleanup"]["status"] != "passed" {
                    self.abort = Some(format!(
                        "previous invocation did not prove process/server cleanup: {run_id}"
                    ));
                }
                let validation = (|| -> Result<()> {
                    if self.config["combined_diagnostics"] == true
                        && record["status"] == "success"
                        && (purpose == "diagnostic"
                            || self.config["gate_diagnostics"] == true
                                && stage == "gate"
                                && reader != "duckdb")
                    {
                        record["plan_artifacts"] = evidence::plans(&record, &destination)?;
                    }
                    evidence::validate(&record, &payload, reader, gate, &self.protocols)?;
                    if let Some(case) = self.selection.cases.get(text(&job["case_id"])?)
                        && record["status"] == "success"
                        && case["status"] == "prepared"
                    {
                        require(
                            record["identity"]["native_expression"]
                                == case["native_expressions"][reader],
                            "native expression differs from checked-in matrix",
                        )?;
                    }
                    Ok(())
                })();
                if let Err(error) = validation {
                    record["status"] = json!("operational_failure");
                    record["failure_reason"] = json!(format!("invalid observation: {error}"));
                }
                Ok(record)
            })();
            match invocation {
                Ok(record) => record,
                Err(error) => {
                    self.abort = Some(format!(
                        "invocation stopped without proven cleanup: {run_id}"
                    ));
                    json!({"status":"operational_failure", "failure_reason":error.to_string()})
                }
            }
        };
        let mut row = slot.clone();
        object(&record)?;
        row.as_object_mut().ok_or("invalid slot")?.extend(object(&json!({"status":record["status"], "request":payload,
            "session_id":if job["execution_mode"] == "reuse" { job["id"].clone() } else { Value::Null },
            "gate_run_id":gate.map(|g| &g["run_id"]), "artifacts":destination, "observation":record}))?.clone());
        writeln!(self.journal, "{row}")?;
        self.journal.flush()?;
        println!(
            "{}",
            json!({"run_id":run_id, "job_id":job_id, "reader_id":reader, "stage":stage, "status":record["status"]})
        );
        self.rows.push(row);
        Ok(record)
    }

    fn gates(&mut self, receipt: &Value) -> Result<Value> {
        let mut inventory = Map::new();
        for job in self.selection.jobs.clone() {
            let case = text(&job["case_id"])?;
            let id = text(&job["id"])?;
            let prepared = (|| -> Result<Value> {
                if self.args.matrix.is_some() {
                    let row = self
                        .selection
                        .cases
                        .get(case)
                        .ok_or("missing matrix case")?;
                    require(
                        row["status"] == "prepared",
                        row["failure_reason"]
                            .as_str()
                            .unwrap_or("missing matrix reference"),
                    )?;
                }
                let payload = request::run(request::Request {
                    fixtures: self.args.fixtures.clone(),
                    case_id: case.into(),
                    execution_mode: text(&job["execution_mode"])?.into(),
                    workload: self.args.workload.clone(),
                    harness: self.args.harness.clone(),
                })?;
                let reference = self
                    .selection
                    .references
                    .get(case)
                    .ok_or("missing reference")?;
                remote_request(
                    payload,
                    &self.args.fixtures,
                    receipt,
                    reference,
                    &self.comparison,
                    &self.protocols,
                )
            })();
            let missing = match prepared {
                Ok(payload) => {
                    self.templates.insert(id.to_owned(), payload);
                    None
                }
                Err(error) => Some(format!("fixture/query/reference not prepared: {error}")),
            };
            let mut entries = Map::new();
            for (order, reader) in roster(&self.comparison)?.into_iter().enumerate() {
                let mut entry = json!({"runnable":false, "status":"preparation_failed", "failure_reason":missing,
                    "case_id":case, "execution_mode":job["execution_mode"]});
                for key in ["combined_diagnostics", "gate_warmup", "gate_diagnostics"] {
                    if self.config[key] == true {
                        entry[key] = json!(true);
                    }
                }
                if missing.is_none() {
                    if !self.binaries.contains_key(reader) {
                        entry["status"] = json!("operational_failure");
                        entry["failure_reason"] = json!("reader build not supplied");
                    } else {
                        let run_id = format!(
                            "{}-gate-{:05}",
                            text(&self.config["campaign_id"])?,
                            self.rows.len()
                        );
                        let mut slot = json!({"run_id":run_id, "job_id":id, "reader_id":reader, "stage":"gate",
                            "repetition":0, "order":order, "traced":self.config["gate_diagnostics"]});
                        if self.args.workload.is_some() {
                            slot.as_object_mut()
                                .unwrap()
                                .extend(object(&self.comparison)?.clone());
                        }
                        let record = self.invoke(&slot, &job, None)?;
                        let reference = &self.selection.references[case];
                        entry.as_object_mut().unwrap().extend(object(&json!({"status":record["status"],
                            "runnable":record["status"] == "success", "run_id":run_id, "failure_reason":record["failure_reason"],
                            "identity":record["identity"], "correctness_file":self.args.output.join(&run_id).join("correctness.json"),
                            "reference":reference, "reference_sha256":digest(&reference.join("reference.json"))?}))?.clone());
                    }
                }
                entries.insert(reader.to_owned(), entry);
            }
            inventory.insert(id.to_owned(), json!(entries));
        }
        Ok(json!(inventory))
    }

    fn execute(&mut self, receipt: &Value) -> Result<Value> {
        let _lock = storage::exclusive(&self.args.state)?;
        let inventory = self.gates(receipt)?;
        save(&self.args.output.join("inventory.json"), &inventory)?;
        let ordered: Vec<_> = self
            .selection
            .jobs
            .iter()
            .map(|job| {
                let id = job["id"].as_str().unwrap();
                (id.to_owned(), inventory[id].clone())
            })
            .collect();
        let historical = json!({});
        let slots = schedule(
            &ordered,
            text(&self.config["campaign_id"])?,
            if self.args.workload.is_some() {
                &self.comparison
            } else {
                &historical
            },
            Some(self.config["combined_diagnostics"] == true),
            Some(self.config["gate_warmup"] == true),
            Some(self.config["gate_diagnostics"] == true),
        )?;
        save(&self.args.output.join("schedule.json"), &slots)?;
        let frozen: Map<_, _> = ["campaign.json", "inventory.json", "schedule.json"]
            .into_iter()
            .map(|name| {
                Ok((
                    name.to_owned(),
                    json!(digest(&self.args.output.join(name))?),
                ))
            })
            .collect::<Result<_>>()?;
        save(&self.args.output.join("frozen.json"), &json!(frozen))?;
        for slot in array(&slots)? {
            let job = self
                .selection
                .jobs
                .iter()
                .find(|job| job["id"] == slot["job_id"])
                .ok_or("unknown scheduled job")?
                .clone();
            let gate = &inventory[text(&slot["job_id"])?][text(&slot["reader_id"])?];
            self.invoke(slot, &job, Some(gate))?;
        }
        let mut intact = true;
        for (path, hash) in object(&self.config["input_sha256"])? {
            intact &= digest(Path::new(path)).is_ok_and(|actual| *hash == actual);
        }
        for (name, hash) in &frozen {
            intact &= digest(&self.args.output.join(name)).is_ok_and(|actual| *hash == actual);
        }
        let jobs = summarize(
            &inventory,
            array(&slots)?,
            &self.rows,
            self.config["timer_resolution"]["ratio_floor_ns"]
                .as_f64()
                .ok_or("invalid timer resolution")?,
            intact,
        )?;
        let failures =
            object(&inventory)?.values().any(|entries| {
                entries.as_object().unwrap().values().any(|entry| {
                    !matches!(entry["status"].as_str(), Some("success" | "unsupported"))
                })
            });
        let failed_slots = self
            .rows
            .iter()
            .any(|row| row["stage"] != "gate" && row["status"] != "success");
        let status = if intact && !failures && !failed_slots && self.abort.is_none() {
            "complete"
        } else {
            "incomplete"
        };
        let mut summary = self.comparison.clone();
        summary.as_object_mut().unwrap().extend(object(&json!({"status":status, "integrity_passed":intact,
            "abort_reason":self.abort, "campaign_id":self.config["campaign_id"], "finished_ns":timestamp_ns()?,
            "timer_resolution":self.config["timer_resolution"], "scheduled_slots":array(&slots)?.len(),
            "recorded_slots":self.rows.iter().filter(|row| row["stage"] != "gate").count(), "jobs":jobs}))?.clone());
        save(&self.args.output.join("summary.json"), &summary)?;
        Ok(json!({"status":status, "summary":self.args.output.join("summary.json")}))
    }
}

pub(in crate::selective_read) fn run(mut args: Args) -> Result<()> {
    fs::create_dir(&args.output)?;
    args.output = fs::canonicalize(&args.output)?;
    args.fixtures = fs::canonicalize(&args.fixtures)?;
    args.state = fs::canonicalize(&args.state)?;
    args.harness = fs::canonicalize(&args.harness)?;
    for path in [&mut args.matrix, &mut args.workload].into_iter().flatten() {
        *path = fs::canonicalize(&*path)?;
    }
    let protocols = args
        .harness
        .parent()
        .and_then(Path::parent)
        .ok_or("invalid harness directory")?
        .join("docs/content/benchmarks");
    let mut worker = Worker::start(&args.python, &args.harness, &args.output)?;
    let preparation = storage::upload::prepare(
        &args.state,
        &args.fixtures,
        &args.upload,
        &args.upload_table,
        |paths| {
            let values = worker.call(&json!({"action":"hashes", "paths":paths}))?;
            array(&values)?
                .iter()
                .map(|value| Ok(text(value)?.to_owned()))
                .collect()
        },
    )?;
    storage::upload::verified(&args.state, &args.fixtures, &preparation)?;
    args.upload = fs::canonicalize(&args.upload)?;
    let receipt = &preparation["receipt"];
    let mut binaries = BTreeMap::new();
    let mut builds = Map::new();
    for binary in &args.binary {
        let binary = fs::canonicalize(binary)?;
        let build = read_json(&binary.parent().ok_or("invalid binary")?.join("build.json"))?;
        let reader = text(&build["reader_id"])?.to_owned();
        require(
            [
                "delta-arrow-reader",
                "delta-rs",
                "duckdb",
                "polars",
                "daft",
                "spark",
            ]
            .contains(&reader.as_str())
                && !binaries.contains_key(&reader),
            "unknown or duplicate reader",
        )?;
        require(
            build["executable_sha256"] == digest(&binary)?,
            "reader executable changed",
        )?;
        binaries.insert(reader.clone(), binary);
        builds.insert(reader, build);
    }
    let prepared = if let Some(path) = &args.matrix {
        let value = workload::matrix::load(path, &args.fixtures, &args.harness)?;
        for row in array(&value["cases"])? {
            if row["status"] == "prepared" {
                worker.call(
                    &json!({"action":"prepare-reference","fixtures":args.fixtures,
                    "case_id":row["case_id"],"reference":row["reference"],"reuse":true}),
                )?;
            }
        }
        value
    } else {
        Value::Null
    };
    let (frozen, comparison) = if let Some(path) = &args.workload {
        workload::load(path, &args.harness)?
    } else {
        (
            Value::Null,
            json!({"comparison_revision":2,"protocol_sha256":digest(&protocols.join("selective-read-protocol.md"))?}),
        )
    };
    let comparison = evidence::comparison(&comparison, &protocols)?;
    let mut config = modes(&args, &comparison, &protocols)?;
    require(
        binaries
            .keys()
            .all(|r| roster(&comparison).is_ok_and(|rs| rs.contains(&r.as_str()))),
        "binary is outside the comparison reader roster",
    )?;
    let selection = select(
        &args,
        &prepared,
        &frozen,
        &json!(builds),
        text(&receipt["fixture_manifest_sha256"])?,
    )?;
    let mut server = storage::state(&args.state)?;
    if let Some(network) = network::config(&args.state)? {
        server["network"] = network;
    }
    let _affinity = Affinity::pin(&server, &worker)?;
    config.as_object_mut().unwrap().extend(object(&json!({
        "campaign_id":args.output.file_name().ok_or("missing campaign ID")?.to_str().ok_or("invalid campaign ID")?,
        "fixtures":args.fixtures, "upload":args.upload, "upload_sha256":digest(&args.upload)?,
        "server":server, "reader_builds":builds, "jobs":selection.jobs,
        "upload_verification":receipt["verification"], "in_process_upload":!args.upload_table.is_empty(),
        "matrix":args.matrix.as_ref().map(|p| Ok::<_,Box<dyn std::error::Error>>(json!({"path":p, "sha256":digest(p)?}))).transpose()?,
        "workload":args.workload, "timer_resolution":evidence::timer_resolution()?, "started_ns":timestamp_ns()?,
        "cache_policy":"fresh clients, reused MinIO/OS caches, no flushes; complete warmup/run history in observations.jsonl",
        "scope":"requested cases/sessions only; not a complete publication campaign"
    }))?.clone());
    let (sources, inputs) = source_hashes(&args, &config, &protocols)?;
    config["source_sha256"] = sources;
    config["input_sha256"] = inputs;
    save(&args.output.join("campaign.json"), &config)?;
    let journal = File::create_new(args.output.join("observations.jsonl"))?;
    let mut campaign = Campaign {
        args,
        worker,
        selection,
        protocols,
        comparison,
        config,
        binaries,
        templates: BTreeMap::new(),
        rows: Vec::new(),
        journal,
        abort: None,
    };
    let result = campaign.execute(receipt)?;
    println!("{result}");
    require(
        result["status"] == "complete",
        "campaign incomplete; inspect summary.json",
    )
}

#[cfg(test)]
mod tests;
