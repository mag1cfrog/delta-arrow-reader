use super::{array, object, require, storage, text};
use crate::{
    Result,
    command::{command_output, run_command},
    selective_read_build::digest,
};
use clap::Args;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use storage::{affinity, endpoint, http, properties, read_json, save, service, timestamp_ns};

pub(super) const BOUNDARY: &str = "proxy client-facing HTTP ResponseWriter accepted body bytes";
mod check;
pub(super) use check::calibrate;

#[derive(Args)]
pub(crate) struct Start {
    #[arg(long)]
    state: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 19002)]
    port: u16,
    #[arg(long, default_value_t = 200)]
    latency_ms: u64,
    #[arg(long, default_value_t = 20)]
    jitter_ms: u64,
    #[arg(long, default_value_t = 150)]
    mbps: u64,
    #[arg(long, default_value = "0")]
    seed: String,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case", deny_unknown_fields)]
pub(super) enum Request {
    Config { directory: PathBuf },
}

pub(super) fn run(request: Request) -> Result<Value> {
    match request {
        Request::Config { directory } => Ok(config(&directory)?.unwrap_or(Value::Null)),
    }
}

pub(super) fn capture(directory: &Path, output: &Path) -> Result<Value> {
    let value = control(directory, false, false, 10.0)?;
    if value.is_null() {
        Ok(value)
    } else {
        finish(output, value)
    }
}

pub(super) fn config(directory: &Path) -> Result<Option<Value>> {
    let path = directory.join("network.json");
    if !path.exists() {
        return Ok(None);
    }
    let value = read_json(&path)?;
    endpoint(&value)?;
    require(
        value["upstream"] == storage::state(directory)?["endpoint"],
        "proxy points to another server",
    )?;
    Ok(Some(value))
}

pub(super) fn control(directory: &Path, reset: bool, traced: bool, timeout: f64) -> Result<Value> {
    let Some(value) = config(directory)? else {
        return Ok(Value::Null);
    };
    let url = format!(
        "{}/_benchmark/network{}",
        endpoint(&value)?,
        if traced { "?trace=true" } else { "" }
    );
    let result: Value =
        serde_json::from_slice(&http(&url, if reset { "POST" } else { "GET" }, timeout)?)?;
    require(
        result["profile"] == value["profile"],
        "proxy network profile changed",
    )?;
    Ok(result)
}

pub(super) fn verify(directory: &Path) -> Result<()> {
    let Some(value) = config(directory)? else {
        return Ok(());
    };
    let output = Path::new(text(&value["output"])?);
    require(
        read_json(&output.join("network.json"))? == value,
        "proxy record changed",
    )?;
    require(
        value["binary_sha256"] == digest(&output.join("proxy"))?,
        "proxy binary changed",
    )?;
    let actual = properties(text(&value["unit"])?)?;
    require(
        actual == value["limits"] && actual["ActiveState"] == "active",
        "proxy changed or stopped",
    )?;
    let pid = rustix::process::Pid::from_raw(text(&actual["MainPID"])?.parse()?)
        .ok_or("invalid proxy PID")?;
    require(
        json!(affinity(Some(pid))?) == json!([value["cpu"]]),
        "proxy affinity changed",
    )?;
    storage::verify_memory(text(&actual["ControlGroup"])?, 512 * 1024u64.pow(2))?;
    control(directory, false, false, 10.0)?;
    Ok(())
}

fn select_cpu(server: &Value, available: &Value) -> Result<u64> {
    let occupied: BTreeSet<_> = object(&server["cpus"])?
        .values()
        .map(array)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .map(|cpu| cpu.as_u64().ok_or_else(|| "invalid occupied CPU".into()))
        .collect::<Result<_>>()?;
    let topology = array(&server["topology"])?;
    for row in array(available)? {
        let shared = topology
            .iter()
            .filter(|other| other["core"] == row["core"] && other["socket"] == row["socket"])
            .any(|other| {
                other["cpu"]
                    .as_u64()
                    .is_some_and(|cpu| occupied.contains(&cpu))
            });
        if !shared {
            return row["cpu"]
                .as_u64()
                .ok_or_else(|| "invalid proxy CPU".into());
        }
    }
    Err("proxy needs one physical core separate from reader, MinIO and observer".into())
}

pub(super) fn start(args: Start) -> Result<()> {
    let _lock = storage::exclusive(&args.state)?;
    storage::verify_server(&args.state)?;
    require(
        config(&args.state)?.is_none(),
        "stop the attached proxy before starting another",
    )?;
    require(
        args.port >= 1024
            && args.jitter_ms <= args.latency_ms
            && args.latency_ms <= 60000
            && args.mbps <= 100000
            && args.seed.len() <= 128,
        "invalid network profile",
    )?;
    let server = storage::state(&args.state)?;
    let server_port = url::Url::parse(endpoint(&server)?)?
        .port()
        .ok_or("missing server port")?;
    require(
        args.port != server_port && u32::from(args.port) != u32::from(server_port) + 1,
        "proxy port overlaps MinIO",
    )?;
    let cpu = select_cpu(&server, &storage::topology()?)?;
    fs::create_dir(&args.output)?;
    let output = fs::canonicalize(&args.output)?;
    let root = PathBuf::from(
        command_output(Command::new("git").args(["rev-parse", "--show-toplevel"]))?.trim(),
    );
    let source = root.join("benches/selective_read/network_proxy.go");
    fs::copy(&source, output.join("network_proxy.go"))?;
    let version = service::go_version()?;
    let build = vec![
        "go".to_owned(),
        "build".into(),
        "-trimpath".into(),
        "-o".into(),
        output
            .join("proxy")
            .to_str()
            .ok_or("non-UTF8 proxy path")?
            .into(),
        output
            .join("network_proxy.go")
            .to_str()
            .ok_or("non-UTF8 proxy source")?
            .into(),
    ];
    run_command(service::go().args(&build[1..]))?;
    let unit = format!("selective-read-network-{}", service::random_hex(6)?);
    let profile = json!({"latency_ms":args.latency_ms, "jitter_ms":args.jitter_ms, "mbps":args.mbps, "seed":args.seed});
    let command = vec![
        "systemd-run".to_owned(),
        "--user".into(),
        "--quiet".into(),
        format!("--unit={unit}"),
        "--service-type=exec".into(),
        "-p".into(),
        "MemoryMax=512M".into(),
        "-p".into(),
        "MemorySwapMax=0".into(),
        "-p".into(),
        format!("CPUAffinity={cpu}"),
        "-p".into(),
        "UnsetEnvironment=GOGC GOMEMLIMIT GODEBUG".into(),
        "--setenv=GOMAXPROCS=1".into(),
        output
            .join("proxy")
            .to_str()
            .ok_or("non-UTF8 proxy path")?
            .into(),
        "--listen".into(),
        format!("127.0.0.1:{}", args.port),
        "--upstream".into(),
        endpoint(&server)?.into(),
        "--latency-ms".into(),
        args.latency_ms.to_string(),
        "--jitter-ms".into(),
        args.jitter_ms.to_string(),
        "--mbps".into(),
        args.mbps.to_string(),
        "--seed".into(),
        args.seed,
    ];
    let environment: BTreeMap<_, _> = service::GO_ENV[..6].iter().copied().collect();
    let mut value = json!({"format":"selective-read-network-v1", "output":output, "unit":unit, "cpu":cpu,
        "endpoint":format!("http://127.0.0.1:{}", args.port), "upstream":server["endpoint"], "profile":profile,
        "jitter":"uniform integer microseconds; SHA256(seed, method, request URI, Range); identical requests repeat",
        "byte_boundary":BOUNDARY, "chunk_bytes":65536, "go_version":version, "build_command":build,
        "build_environment":environment, "binary_sha256":digest(&output.join("proxy"))?,
         "command":command, "started_ns":timestamp_ns()?});
    let mut published = false;
    let started = (|| -> Result<()> {
        run_command(Command::new(&command[0]).args(&command[1..]))?;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(bytes) = http(
                &format!("{}/_benchmark/network", endpoint(&value)?),
                "GET",
                1.0,
            ) {
                let ready: Value = serde_json::from_slice(&bytes)?;
                require(ready["profile"] == profile, "proxy network profile changed")?;
                break;
            }
            require(Instant::now() < deadline, "proxy did not become ready")?;
            thread::sleep(Duration::from_millis(50));
        }
        value["limits"] = properties(&unit)?;
        save(&output.join("network.json"), &value)?;
        save(&args.state.join("network.json"), &value)?;
        published = true;
        verify(&args.state)
    })();
    if started.is_err() {
        let _ = Command::new("systemctl")
            .args(["--user", "stop", &unit])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if published {
            fs::remove_file(args.state.join("network.json"))?;
        }
    }
    started
}

fn stopped(directory: &Path, value: &Value, actual: Value) -> Result<()> {
    require(
        actual["ActiveState"] != "active" && actual["MainPID"] == "0",
        "proxy did not stop",
    )?;
    let receipt = Path::new(text(&value["output"])?).join("stopped.json");
    if !receipt.exists() {
        save(
            &receipt,
            &json!({"stopped_ns":timestamp_ns()?, "properties":actual}),
        )?;
    }
    fs::remove_file(directory.join("network.json"))?;
    Ok(())
}

pub(super) fn stop(directory: &Path) -> Result<()> {
    if let Some(value) = config(directory)? {
        let unit = text(&value["unit"])?;
        require(
            unit.starts_with("selective-read-network-"),
            "unexpected proxy unit",
        )?;
        // A vanished transient unit can return nonzero; its process still must be gone.
        let _ = Command::new("systemctl")
            .args(["--user", "stop", unit])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        stopped(directory, &value, properties(unit)?)?;
    }
    Ok(())
}

fn finish(output: &Path, mut value: Value) -> Result<Value> {
    require(
        value["active"] == 0 && value["trace_truncated"] == false,
        "proxy is active or trace is incomplete",
    )?;
    let records = value
        .as_object_mut()
        .ok_or("invalid proxy capture")?
        .remove("records")
        .unwrap_or(json!([]));
    value["boundary"] = json!(BOUNDARY);
    value["by_class"] = json!({});
    if value["trace_enabled"]
        .as_bool()
        .ok_or("invalid trace flag")?
    {
        let records = array(&records)?;
        let bytes = records
            .iter()
            .try_fold(0u64, |sum, row| {
                sum.checked_add(row["response_bytes"].as_u64()?)
            })
            .ok_or("invalid proxy byte counters")?;
        require(
            value["requests"] == records.len() as u64 && value["response_bytes"] == bytes,
            "proxy trace counters disagree",
        )?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output.join("network-requests.jsonl"))?;
        for record in records {
            serde_json::to_writer(&mut file, record)?;
            file.write_all(b"\n")?;
            let class = text(&record["object_class"])?;
            if value["by_class"][class].is_null() {
                value["by_class"][class] = json!({"requests":0,"response_bytes":0});
            }
            let group = &mut value["by_class"][class];
            group["requests"] = json!(group["requests"].as_u64().unwrap() + 1);
            group["response_bytes"] = json!(
                group["response_bytes"].as_u64().unwrap()
                    + record["response_bytes"].as_u64().unwrap()
            );
        }
    }
    save(&output.join("network-capture.json"), &value)?;
    Ok(value)
}

#[cfg(test)]
mod tests;
