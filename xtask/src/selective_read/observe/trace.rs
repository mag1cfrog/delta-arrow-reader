use super::{array, integer, metrics, reconcile, require, sanitize, storage, text, unquote};
use crate::{Result, selective_read::supervise::monotonic_ns, selective_read_build::digest};
use rustix::thread::{CpuSet, sched_setaffinity};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::Child,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Default)]
struct State {
    marks: BTreeSet<String>,
    counts: BTreeMap<String, u64>,
    size: u64,
    error: Option<String>,
    running: bool,
}

#[derive(Default)]
struct Shared {
    data: Mutex<State>,
    condition: Condvar,
}

struct Trace {
    directory: PathBuf,
    control: String,
    shared: Arc<Shared>,
    child: Child,
    thread: Option<JoinHandle<()>>,
    before: Value,
    started_ns: u64,
}

impl Trace {
    fn new(
        directory: &Path,
        output: &Path,
        scope: String,
        run_id: String,
        case_id: String,
    ) -> Result<Self> {
        let control = format!(
            "_observer/{}-{}/",
            std::process::id(),
            storage::timestamp_ns()?
        );
        let shared = Arc::new(Shared::default());
        shared
            .data
            .lock()
            .map_err(|_| "trace state poisoned")?
            .running = true;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output.join("requests.jsonl"))?;
        let mut child = storage::curl(
            directory,
            "/minio/admin/v3/trace?s3=true",
            &["--no-buffer", "--fail"],
        )?;
        let stdout = child.stdout.take().ok_or("missing trace stdout")?;
        let reader_state = Arc::clone(&shared);
        let prefix = control.clone();
        let thread = thread::spawn(move || {
            let read = (|| -> Result<()> {
                for line in BufReader::new(stdout).lines() {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let raw: Value = serde_json::from_str(&line)?;
                    let path = unquote(text(&raw["http"]["request"]["path"])?);
                    let key = path
                        .strip_prefix(&format!("/{}/", storage::BUCKET))
                        .unwrap_or(&path);
                    let mut data = reader_state
                        .data
                        .lock()
                        .map_err(|_| "trace state poisoned")?;
                    if key.starts_with(&prefix) {
                        data.marks.insert(key.to_owned());
                    } else {
                        serde_json::to_writer(
                            &mut file,
                            &sanitize(&raw, &scope, &run_id, &case_id)?,
                        )?;
                        file.write_all(b"\n")?;
                    }
                    let api = text(&raw["funcname"])?;
                    let count = data
                        .counts
                        .entry(api.strip_prefix("s3.").unwrap_or(api).to_owned())
                        .or_default();
                    *count = count
                        .checked_add(1)
                        .ok_or("trace request counter overflow")?;
                    data.size = data
                        .size
                        .checked_add(integer(&raw["http"]["stats"]["outputbytes"])?)
                        .ok_or("trace byte counter overflow")?;
                    reader_state.condition.notify_all();
                }
                require(
                    !reader_state
                        .data
                        .lock()
                        .map_err(|_| "trace state poisoned")?
                        .running,
                    "native trace disconnected",
                )?;
                file.flush()?;
                Ok(())
            })();
            if let Err(error) = read
                && let Ok(mut data) = reader_state.data.lock()
            {
                data.error = Some(error.to_string());
                reader_state.condition.notify_all();
            }
        });
        Ok(Self {
            directory: directory.to_owned(),
            control,
            shared,
            child,
            thread: Some(thread),
            before: Value::Null,
            started_ns: 0,
        })
    }

    fn marker(&self, name: &str) -> Result<String> {
        let key = format!("{}{name}", self.control);
        let child = storage::curl(
            &self.directory,
            &format!("/{}/{key}", storage::BUCKET),
            &["--head", "--max-time", "10", "--output", "/dev/null"],
        )?;
        require(
            child.wait_with_output()?.status.success(),
            "native trace marker failed",
        )?;
        Ok(key)
    }

    fn begin(&mut self) -> Result<()> {
        let deadline = monotonic_ns()? + 20_000_000_000;
        loop {
            let key = self.marker(&format!("ready-{}", storage::timestamp_ns()?))?;
            let data = self
                .shared
                .data
                .lock()
                .map_err(|_| "trace state poisoned")?;
            let (data, _) = self
                .shared
                .condition
                .wait_timeout_while(data, Duration::from_millis(100), |data| {
                    !data.marks.contains(&key) && data.error.is_none()
                })
                .map_err(|_| "trace state poisoned")?;
            if let Some(error) = &data.error {
                return Err(error.clone().into());
            }
            if data.marks.contains(&key) {
                break;
            }
            require(monotonic_ns()? < deadline, "native trace did not subscribe")?;
        }
        self.before = metrics(&self.directory, 10.0)?;
        require(
            self.before["active"] == 0,
            "S3 requests were active before capture",
        )?;
        let mut data = self
            .shared
            .data
            .lock()
            .map_err(|_| "trace state poisoned")?;
        data.counts.clear();
        data.size = 0;
        self.started_ns = storage::timestamp_ns()?;
        Ok(())
    }

    fn finish(&self, deadline: u64) -> Result<Value> {
        let timeout = || -> Result<f64> {
            let remaining = deadline.saturating_sub(monotonic_ns()?);
            require(remaining > 0, "pending S3 requests did not finish")?;
            Ok((remaining as f64 / 1_000_000_000.0).clamp(0.001, 10.0))
        };
        while metrics(&self.directory, timeout()?)?["active"] != 0 {
            thread::sleep(Duration::from_millis(20));
        }
        let key = self.marker("finished")?;
        loop {
            let after = metrics(&self.directory, timeout()?)?;
            let data = self
                .shared
                .data
                .lock()
                .map_err(|_| "trace state poisoned")?;
            if let Some(error) = &data.error {
                return Err(error.clone().into());
            }
            if let Ok(mut expected) = reconcile(&self.before, &after, &data.counts, data.size)
                && data.marks.contains(&key)
            {
                let count = integer(&expected["requests"]["HeadObject"])?;
                expected["requests"]["HeadObject"] =
                    json!(count.checked_sub(1).ok_or("missing end marker")?);
                return Ok(
                    json!({"status":"passed", "boundary":"MinIO HTTP ResponseWriter accepted body bytes",
                    "before":self.before, "after":after, "reader_counters":expected, "started_ns":self.started_ns,
                    "drained_ns":storage::timestamp_ns()?, "control_requests":1}),
                );
            }
            require(
                monotonic_ns()? < deadline,
                "incomplete native trace: request/byte reconciliation failed",
            )?;
            drop(
                self.shared
                    .condition
                    .wait_timeout(data, Duration::from_millis(20))
                    .map_err(|_| "trace state poisoned")?,
            );
        }
    }

    fn close(&mut self) -> Result<()> {
        if self.thread.is_none() {
            return Ok(());
        }
        self.shared
            .data
            .lock()
            .map_err(|_| "trace state poisoned")?
            .running = false;
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        self.child.wait()?;
        self.thread
            .take()
            .unwrap()
            .join()
            .map_err(|_| "native trace thread panicked")?;
        Ok(())
    }
}

impl Drop for Trace {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub(in crate::selective_read) fn capture(
    directory: &Path,
    request: &Path,
    output: &Path,
) -> Result<()> {
    let config = storage::state(directory)?;
    let mut cpus = CpuSet::new();
    for cpu in array(&config["cpus"]["observer"])? {
        let index: usize = integer(cpu)?.try_into()?;
        require(index < CpuSet::MAX_CPU, "invalid observer CPU")?;
        cpus.set(index);
    }
    sched_setaffinity(None, &cpus)?;
    let payload = storage::read_json(request)?;
    let table = url::Url::parse(text(&payload["table_uri"])?)?;
    require(
        table.scheme() == "s3"
            && table.host_str() == Some(storage::BUCKET)
            && table.username().is_empty()
            && table.password().is_none()
            && table.port().is_none(),
        "invalid storage table URI",
    )?;
    let scope = format!("{}/", unquote(table.path()).trim_matches('/'));
    require(scope != "/", "empty observer object scope")?;
    let controller_hash = digest(&std::env::current_exe()?)?;
    let mut trace = Trace::new(
        directory,
        output,
        scope,
        text(&payload["run_id"])?.into(),
        text(&payload["case_id"])?.into(),
    )?;
    let result = (|| -> Result<Value> {
        trace.begin()?;
        println!("{{\"ready\": true}}");
        std::io::stdout().flush()?;
        let mut line = String::new();
        std::io::stdin().read_line(&mut line)?;
        let control: Value = serde_json::from_str(&line)?;
        let deadline = Duration::try_from_secs_f64(
            control["deadline"]
                .as_f64()
                .ok_or("invalid trace deadline")?,
        )?;
        trace.finish(deadline.as_nanos().try_into()?)
    })();
    let result = match trace.close() {
        Ok(()) => result,
        Err(error) => Err(format!("trace shutdown failed: {error}").into()),
    };
    let mut summary = match result {
        Ok(summary) => summary,
        Err(error) => json!({"status":"operational_failure", "failure_reason":error.to_string()}),
    };
    summary["observer_affinity"] = json!(storage::affinity(None)?);
    summary["observer_executable_sha256"] = json!(controller_hash);
    storage::save(&output.join("capture.json"), &summary)?;
    require(
        summary["status"] == "passed",
        summary["failure_reason"]
            .as_str()
            .unwrap_or("native trace failed"),
    )
}
