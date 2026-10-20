use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use super::{
    array, invoke, require,
    storage::{read_json, save},
    text,
    validation::Worker,
};
use crate::{Result, process::copy_directory};
use serde_json::{Value, json};

pub(super) mod capabilities;
pub(super) mod remote;

#[derive(clap::Args)]
pub(crate) struct Args {
    #[arg(long = "binary", required = true)]
    binaries: Vec<PathBuf>,
    #[arg(long)]
    fixtures: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    python: PathBuf,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

struct Checker {
    fixtures: PathBuf,
    output: PathBuf,
    protocols: PathBuf,
    harness: PathBuf,
    worker: Worker,
}

impl Checker {
    fn start(args: &Args) -> Result<Self> {
        let fixtures = fs::canonicalize(&args.fixtures)?;
        let harness = fs::canonicalize(&args.harness)?;
        fs::create_dir(&args.output)?;
        let output = fs::canonicalize(&args.output)?;
        let worker = Worker::start(&args.python, &harness, &output)?;
        Ok(Self {
            fixtures,
            output,
            protocols: harness.join("../../docs/content/benchmarks"),
            worker,
            harness,
        })
    }

    fn references(&mut self, cases: &[&str]) -> Result<BTreeMap<String, (PathBuf, Value)>> {
        let mut references = BTreeMap::new();
        for case in cases {
            let path = self.output.join(format!("reference-{case}"));
            let metadata = self.worker.call(
                &json!({"action":"prepare-reference","fixtures":self.fixtures,
                "case_id":case,"reference":path,"reuse":false}),
            )?;
            references.insert((*case).into(), (path, metadata));
        }
        Ok(references)
    }

    fn request(&mut self, case: &str, mode: &str, purpose: &str, name: &str) -> Result<Value> {
        let mut payload = super::request::run(super::request::Request {
            fixtures: self.fixtures.clone(),
            case_id: case.into(),
            execution_mode: mode.into(),
            workload: None,
            harness: self.harness.clone(),
        })?;
        payload["purpose"] = json!(purpose);
        payload["run_id"] = json!(name);
        Ok(payload)
    }

    fn invoke(
        &mut self,
        binary: &Path,
        payload: &Value,
        name: &str,
        reference: Option<&Path>,
    ) -> Result<Value> {
        let output = self.output.join(name);
        let record = invoke::run(&invoke::Request {
            binary: binary.into(),
            payload: payload.clone(),
            output: output.clone(),
            reference: reference.map(Path::to_owned),
            env: None,
            command_prefix: vec![],
            supervised: false,
            protocols: self.protocols.clone(),
        })?;
        let record = self.validate(record, payload, &output, reference)?;
        save(&output.join("observation.json"), &record)?;
        Ok(record)
    }

    fn validate(
        &mut self,
        mut record: Value,
        payload: &Value,
        output: &Path,
        reference: Option<&Path>,
    ) -> Result<Value> {
        if record["status"] == "success" && payload["purpose"] == "validation" {
            record = self.worker.call(&json!({"action":"check","record":record,"output":output,
                "fixtures":self.fixtures,"reference":reference.ok_or("missing independent reference")?,"payload":payload}))?;
        }
        Ok(record)
    }
}

fn success(record: &Value) -> Result<()> {
    require(
        record["status"] == "success",
        &format!("reader check failed: {}", record["failure_reason"]),
    )
}

fn count(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| "expected a nonnegative reader counter".into())
}

fn timing(record: &Value, rows: &Value, reuse: bool) -> Result<()> {
    success(record)?;
    require(
        record["correctness"]["status"] == "passed",
        "timing lost its correctness gate",
    )?;
    let queries = array(&record["queries"])?;
    require(
        queries.len() == if reuse { 10 } else { 1 },
        "wrong timing query count",
    )?;
    count(&record["cleanup_ns"])?;
    let mut total = 0u64;
    for query in queries {
        let duration = count(&query["completion_ns"])?;
        require(
            query["output_rows"] == *rows && duration > 0,
            "wrong timed output or completion clock",
        )?;
        if count(rows)? > 0 {
            let first = count(&query["first_batch_ns"])?;
            require(first > 0 && first <= duration, "invalid first batch clock")?;
        } else {
            require(
                query["first_batch_ns"].is_null()
                    && query["first_batch_unavailable_reason"] == "empty result",
                "empty result has a first batch clock",
            )?;
        }
        total = total.checked_add(duration).ok_or("reader clock overflow")?;
    }
    if reuse {
        let initialization = count(&record["initialization_ns"])?;
        total = total
            .checked_add(initialization)
            .ok_or("reader clock overflow")?;
        require(
            record["initialization_plus_all_queries_ns"] == total
                && count(&record["session_elapsed_ns"])? >= total
                && record["initialization_plus_query1_ns"]
                    == initialization + count(&queries[0]["completion_ns"])?,
            "reuse clocks disagree",
        )?;
    } else {
        require(
            record["open_query_ns"] == queries[0]["completion_ns"],
            "open query clocks disagree",
        )?;
    }
    Ok(())
}

fn io(record: &Value, reuse: bool) -> Result<()> {
    success(record)?;
    require(
        count(&record["diagnostic_session_ns"])? > 0
            && record["provider_evidence"].is_null()
            && record["open_query_ns"].is_null(),
        "I/O observation includes timing or provider evidence",
    )?;
    require(
        array(&record["queries"])?
            .iter()
            .all(|q| q["physical_plan"].is_null() && q["completion_ns"].is_null()),
        "I/O query includes timing or plans",
    )?;
    let mut expected = vec!["snapshot_open"];
    for _ in 0..if reuse { 10 } else { 1 } {
        expected.extend(["query_start", "stream_complete"]);
    }
    expected.push("cleanup_complete");
    let events = array(&record["diagnostic_events"])?;
    require(
        events
            .iter()
            .map(|e| text(&e["event"]))
            .collect::<Result<Vec<_>>>()?
            == expected,
        "wrong I/O lifecycle event sequence",
    )?;
    let times = events
        .iter()
        .map(|e| count(&e["time_ns"]))
        .collect::<Result<Vec<_>>>()?;
    require(
        times.windows(2).all(|pair| pair[0] <= pair[1])
            && count(&record["diagnostic_process"]["exited_ns"])? >= *times.last().unwrap(),
        "I/O lifecycle clocks disagree",
    )
}

pub(super) fn local(args: Args) -> Result<()> {
    let cases = [
        "li.clustered.eq2-in20",
        "li.shuffled.eq2-in20",
        "li.clustered.empty",
        "li.clustered.date7-limit",
        "li.clustered.q6-scan",
        "wide.clustered.eq2-in20",
    ];
    let mut check = Checker::start(&args)?;
    let references = check.references(&cases)?;
    let mut invocations = 0;
    for binary in &args.binaries {
        let build = read_json(&binary.with_file_name("build.json"))?;
        let reader = text(&build["reader_id"])?;
        for case in cases {
            let (reference, expected) = &references[case];
            for mode in if case.ends_with("eq2-in20") {
                vec!["open", "reuse"]
            } else {
                vec!["open"]
            } {
                let name = format!("{reader}-{case}-{mode}");
                let validation = format!("{name}-validation");
                let payload = check.request(case, mode, "validation", &name)?;
                let result = check.invoke(binary, &payload, &validation, Some(reference))?;
                success(&result)?;
                require(
                    result["correctness"]["status"] == "passed"
                        && array(&result["queries"])?
                            .iter()
                            .all(|q| q["completion_ns"].is_null()),
                    "validation failed or included timing clocks",
                )?;
                if reader == "delta-rs" {
                    let config = &result["provider_evidence"]["scan_config"];
                    require(
                        config["enable_parquet_pushdown"] == true
                            && config["schema_force_view_types"] == true,
                        "wrong delta-rs scan settings",
                    )?;
                }
                let proof = check.output.join(&validation).join("correctness.json");
                let mut payload = check.request(case, mode, "timing", &name)?;
                payload["correctness_file"] = json!(proof);
                let measured_name = format!("{name}-timing");
                let measured = check.invoke(binary, &payload, &measured_name, None)?;
                timing(&measured, &expected["output_rows"], mode == "reuse")?;
                require(
                    !fs::read_dir(check.output.join(&measured_name).join("reader"))?.any(|e| {
                        e.is_ok_and(|e| e.path().extension().is_some_and(|ext| ext == "arrow"))
                    }),
                    "timed reader exported Arrow results",
                )?;
                invocations += 2;
            }
        }
        let case = cases[0];
        let reference = &references[case].0;
        let name = format!("{reader}-diagnostic");
        let payload = check.request(case, "open", "diagnostic", &name)?;
        let diagnostic = check.invoke(binary, &payload, &name, None)?;
        success(&diagnostic)?;
        require(
            diagnostic["open_query_ns"].is_null()
                && fs::metadata(
                    check
                        .output
                        .join(&name)
                        .join("reader")
                        .join(text(&diagnostic["queries"][0]["physical_plan"])?),
                )?
                .len()
                    > 0,
            "missing diagnostic plan or mixed timing clock",
        )?;
        invocations += 1;
        for mode in ["open", "reuse"] {
            let name = format!("{reader}-io-{mode}");
            let payload = check.request(case, mode, "io", &name)?;
            io(
                &check.invoke(binary, &payload, &name, None)?,
                mode == "reuse",
            )?;
            invocations += 1;
        }
        for (name, field, value) in [
            ("wrong-snapshot", "snapshot_version", json!(999)),
            (
                "bad-query",
                "canonical_sql",
                json!("SELECT missing_column FROM bench"),
            ),
        ] {
            let mut broken = payload.clone();
            broken[field] = value;
            let failed = check.invoke(binary, &broken, &format!("{reader}-{name}"), None)?;
            require(
                failed["status"] == "operational_failure"
                    && !text(&failed["failure_reason"])?.is_empty(),
                "broken query did not fail",
            )?;
            invocations += 1;
        }
        let table = check.output.join(format!("{reader}-wrong-schema"));
        require(!table.exists(), "schema test destination already exists")?;
        copy_directory(&check.fixtures.join("li.clustered"), &table)?;
        let log = table.join("_delta_log/00000000000000000000.json");
        let mut actions = fs::read_to_string(&log)?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for action in &mut actions {
            if let Some(metadata) = action.get_mut("metaData") {
                let mut schema: Value = serde_json::from_str(text(&metadata["schemaString"])?)?;
                let fields = schema["fields"]
                    .as_array_mut()
                    .ok_or("missing schema fields")?;
                let line = fields
                    .iter_mut()
                    .find(|f| f["name"] == "l_linenumber")
                    .ok_or("missing line number column")?;
                line["type"] = json!("long");
                metadata["schemaString"] = json!(serde_json::to_string(&schema)?);
            }
        }
        fs::write(
            &log,
            actions
                .iter()
                .map(|action| format!("{action}\n"))
                .collect::<String>(),
        )?;
        let name = format!("{reader}-wrong-schema");
        let mut broken = check.request(case, "open", "validation", &name)?;
        broken["table_uri"] = json!(
            url::Url::from_directory_path(&table)
                .map_err(|_| "invalid table path")?
                .as_str()
                .trim_end_matches('/')
        );
        let failed = check.invoke(
            binary,
            &broken,
            &format!("{reader}-schema-result"),
            Some(reference),
        )?;
        require(
            ["validation_failed", "operational_failure"].contains(&text(&failed["status"])?)
                && !text(&failed["failure_reason"])?.is_empty(),
            "wrong schema did not fail",
        )?;
        invocations += 1;
        let proof = check
            .output
            .join(format!("{reader}-{case}-open-validation/correctness.json"));
        let mut base = check.request(case, "open", "timing", &format!("{reader}-stale"))?;
        base["correctness_file"] = json!(proof);
        let shuffled = url::Url::from_directory_path(check.fixtures.join("li.shuffled"))
            .map_err(|_| "invalid fixture path")?;
        for (field, value) in [
            ("snapshot_version", json!(1)),
            ("canonical_sql", json!("SELECT 1")),
            ("table_uri", json!(shuffled.as_str().trim_end_matches('/'))),
        ] {
            let mut broken = base.clone();
            broken[field] = value;
            let failed = check.invoke(binary, &broken, &format!("{reader}-stale-{field}"), None)?;
            require(
                failed["status"] == "validation_failed"
                    && array(&failed["queries"])?.is_empty()
                    && failed["open_query_ns"].is_null(),
                "stale correctness identity reached query execution",
            )?;
            invocations += 1;
        }
        let mut wrong_count = read_json(&proof)?;
        wrong_count["checks"][0]["output_rows"] = json!(
            count(&wrong_count["checks"][0]["output_rows"])?
                .checked_add(1)
                .ok_or("row count overflow")?
        );
        let count_proof = check
            .output
            .join(format!("{reader}-wrong-count-proof.json"));
        save(&count_proof, &wrong_count)?;
        base["correctness_file"] = json!(count_proof);
        let failed = check.invoke(binary, &base, &format!("{reader}-wrong-timing-count"), None)?;
        require(
            failed["status"] == "validation_failed" && !array(&failed["queries"])?.is_empty(),
            "wrong timed output count was accepted",
        )?;
        invocations += 1;
    }
    let summary = json!({"status":"passed","readers":args.binaries.len(),"public_cases":cases.len(),"invocations":invocations});
    save(&check.output.join("checks.json"), &summary)?;
    println!("{summary}");
    Ok(())
}

#[cfg(test)]
mod tests;
