use std::{
    fs::{self, File},
    io::{self, Read, Write},
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

use rustix::{
    io::ioctl_fionbio,
    process::{Pid, Signal, kill_process_group},
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{integer, metrics, require, storage, summarize, text};
use crate::{
    Result,
    command::wait_with_timeout,
    selective_read::{invoke, network, supervise::monotonic_ns},
    selective_read_build::digest,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::selective_read) struct Request {
    directory: PathBuf,
    binary: PathBuf,
    payload: Value,
    output: PathBuf,
    reference: Option<PathBuf>,
    protocols: PathBuf,
    traced: Option<bool>,
}

pub(super) struct Observer {
    child: Child,
    finished: bool,
}

impl Observer {
    pub(super) fn start(directory: &Path, output: &Path) -> Result<Self> {
        let child = Command::new(std::env::current_exe()?)
            .args(["selective-read", "capture", "--state"])
            .arg(fs::canonicalize(directory)?)
            .arg("--request")
            .arg(fs::canonicalize(output.join("request.json"))?)
            .arg("--output")
            .arg(fs::canonicalize(output)?)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(File::create_new(output.join("observer.stderr.log"))?)
            .process_group(0)
            .spawn()?;
        let mut observer = Self {
            child,
            finished: false,
        };
        let stdout = observer
            .child
            .stdout
            .as_mut()
            .ok_or("missing observer stdout")?;
        ioctl_fionbio(&*stdout, true)?;
        let deadline = monotonic_ns()? + 25_000_000_000;
        let mut ready = Vec::new();
        loop {
            let mut buffer = [0; 64];
            match stdout.read(&mut buffer) {
                Ok(0) => {
                    return Err("observer exited before readiness; inspect its artifacts".into());
                }
                Ok(count) => {
                    ready.extend_from_slice(&buffer[..count]);
                    require(ready.len() <= 64, "invalid observer readiness")?;
                    if ready.contains(&b'\n') {
                        break;
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                    ) => {}
                Err(error) => return Err(error.into()),
            }
            require(
                monotonic_ns()? < deadline,
                "observer failed to become ready; inspect its artifacts",
            )?;
            thread::sleep(Duration::from_millis(10));
        }
        require(
            serde_json::from_slice::<Value>(&ready)? == json!({"ready":true}),
            "invalid observer readiness",
        )?;
        Ok(observer)
    }

    pub(super) fn finish(&mut self, deadline: u64) -> Result<()> {
        let control = json!({"deadline":deadline as f64 / 1e9});
        let mut input = self.child.stdin.take().ok_or("missing observer stdin")?;
        writeln!(input, "{control}")?;
        drop(input);
        let timeout =
            Duration::from_nanos(deadline.saturating_sub(monotonic_ns()?).max(1_000_000_000))
                + Duration::from_secs(15);
        let status = wait_with_timeout(&mut self.child, timeout)?;
        require(status.success(), "storage observer exited unsuccessfully")?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        if !self.finished {
            let _ = kill_process_group(Pid::from_child(&self.child), Signal::KILL);
            let _ = self.child.wait();
        }
    }
}

fn kill_scope(unit: &str) {
    let _ = Command::new("systemctl")
        .args(["--user", "kill", "--signal=KILL", unit])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn drain(directory: &Path, unit: &str, deadline: u64) -> Result<()> {
    loop {
        if monotonic_ns()? >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "reader scope or server requests exceeded the cleanup deadline",
            )
            .into());
        }
        let properties = storage::properties(unit)?;
        let group = text(&properties["ControlGroup"])?;
        let populated = if group.is_empty() {
            false
        } else {
            match fs::read_to_string(
                Path::new("/sys/fs/cgroup")
                    .join(group.trim_start_matches('/'))
                    .join("cgroup.events"),
            ) {
                Ok(events) => events.lines().any(|line| line == "populated 1"),
                Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                Err(error) => return Err(error.into()),
            }
        };
        let timeout = || -> Result<f64> {
            Ok((deadline.saturating_sub(monotonic_ns()?) as f64 / 1e9).clamp(0.001, 10.0))
        };
        let proxy = network::control(directory, false, false, timeout()?)?;
        if !populated
            && (proxy.is_null() || proxy["active"] == 0)
            && metrics(directory, timeout()?)?["active"] == 0
            && monotonic_ns()? < deadline
        {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
}

pub(in crate::selective_read) fn run(request: Request) -> Result<Value> {
    let Request {
        directory,
        binary,
        payload,
        output,
        reference,
        protocols,
        traced,
    } = request;
    let traced = traced.unwrap_or(payload["purpose"] == "io");
    require(
        !traced
            || matches!(payload["purpose"].as_str(), Some("io" | "diagnostic"))
            || payload["purpose"] == "validation"
                && payload["validation_diagnostics"] == true
                && payload["comparison_revision"] == 6,
        "detailed tracing requires an untimed diagnostic or declared exact gate",
    )?;
    let table = url::Url::parse(text(&payload["table_uri"])?)?;
    require(
        table.scheme() == "s3"
            && table.host_str() == Some(storage::BUCKET)
            && table.username().is_empty()
            && table.password().is_none()
            && table.port().is_none()
            && !table.path().trim_matches('/').is_empty(),
        "invalid storage table URI",
    )?;
    let config = storage::state(&directory)?;
    storage::verify_server(&directory)?;
    require(
        metrics(&directory, 10.0)?["active"] == 0,
        "pending server requests before reader launch",
    )?;
    require(!output.exists(), "invocation output already exists")?;
    network::control(&directory, true, traced, 10.0)?;
    let trace_output = output.with_file_name(format!(
        "{}-io",
        output
            .file_name()
            .ok_or("invalid invocation output")?
            .to_str()
            .ok_or("non-UTF8 invocation output")?
    ));
    fs::create_dir(&trace_output)?;
    storage::save(&trace_output.join("request.json"), &payload)?;
    let unit = format!(
        "selective-read-{}-{}.scope",
        std::process::id(),
        storage::timestamp_ns()?
    );
    let mut deadline = monotonic_ns()? + 60_000_000_000;
    let mut observer = if traced {
        Some(Observer::start(&directory, &trace_output)?)
    } else {
        None
    };
    let observed = (|| -> Result<Value> {
        let mut record = invoke::run(&invoke::Request {
            binary,
            payload: payload.clone(),
            output: output.clone(),
            reference,
            protocols,
            supervised: true,
            env: Some(serde_json::from_value(storage::reader_environment(
                &directory,
            )?)?),
            command_prefix: serde_json::from_value(storage::reader_prefix(
                &directory,
                Some(&unit),
            )?)?,
        })?;
        storage::save(&output.join("observation.json"), &record)?;
        deadline = record["supervision"]["cleanup_started_monotonic_ns"]
            .as_u64()
            .unwrap_or(monotonic_ns()?)
            + 60_000_000_000;
        if record["supervision"].get("returncode") == Some(&Value::Null) {
            deadline = monotonic_ns()?;
        }
        if output.join("limits.json").exists() {
            record["external_resource_limits"] = storage::read_json(&output.join("limits.json"))?;
        }
        if record["status"] != "success" {
            kill_scope(&unit);
        }
        match drain(&directory, &unit, deadline) {
            Ok(()) => {
                record["cleanup"] = json!({"status":"passed","scope_unit":unit,"drained_monotonic_ns":monotonic_ns()?})
            }
            Err(error) => {
                kill_scope(&unit);
                record["cleanup"] = json!({"status":"operational_failure","scope_unit":unit,"failure_reason":error.to_string()});
                record["status"] = json!(if error
                    .downcast_ref::<io::Error>()
                    .is_some_and(|e| e.kind() == io::ErrorKind::TimedOut)
                {
                    "timeout"
                } else {
                    "operational_failure"
                });
                record["failure_reason"] = json!(error.to_string());
            }
        }
        Ok(record)
    })();
    let mut record = match observed {
        Ok(record) => record,
        Err(error) => {
            kill_scope(&unit);
            fs::create_dir_all(&output)?;
            json!({"status":"operational_failure","failure_reason":error.to_string(),"cleanup":{"status":"unverified","scope_unit":unit}})
        }
    };
    let observer_passed = observer
        .as_mut()
        .is_none_or(|observer| observer.finish(deadline).is_ok());
    drop(observer);
    record["storage_environment"] = json!({
        "server_record":fs::canonicalize(directory.join("server.json"))?,"server_sha256":digest(&directory.join("server.json"))?,
        "trace_enabled":traced,"controller_sha256":digest(&std::env::current_exe()?)?,
        "reader_affinity":config["cpus"]["reader"],"server_affinity":config["cpus"]["server"],"observer_affinity":config["cpus"]["observer"]
    });
    if let Some(network) = network::config(&directory)? {
        record["storage_environment"]["network"] = network;
    }
    record.as_object_mut().unwrap().entry("external_resource_limits").or_insert_with(|| json!({"status":"unverified","reason":"reader launcher did not record effective limits"}));
    if traced {
        let proof = trace_output.join("capture.json");
        let proof = if proof.exists() {
            storage::read_json(&proof)?
        } else {
            json!({"status":"operational_failure","failure_reason":"observer exited without a capture record"})
        };
        record["storage_capture"] = proof.clone();
        if !observer_passed || proof["status"] != "passed" {
            record["status"] = json!("operational_failure");
            record["failure_reason"] = json!("storage capture failed reconciliation");
        } else {
            let io = summarize(&trace_output, &record)?;
            let requests = super::object(&proof["reader_counters"]["requests"])?
                .values()
                .map(integer)
                .collect::<Result<Vec<_>>>()?
                .iter()
                .sum::<u64>();
            require(
                io["response_bytes"] == proof["reader_counters"]["response_bytes"]
                    && io["requests"] == requests,
                "capture summary differs from reconciled counters",
            )?;
            let external = record
                .as_object_mut()
                .unwrap()
                .entry("external_metrics")
                .or_insert_with(|| json!({}));
            for (key, value) in [
                ("requests", io["requests"].clone()),
                ("response_bytes", io["response_bytes"].clone()),
                (
                    "touched_parquet_objects",
                    json!(super::array(&io["touched_parquet_objects"])?.len()),
                ),
                ("reason", Value::Null),
            ] {
                external[key] = value;
            }
            record["storage_io"] = io;
        }
    } else {
        record["storage_capture"] =
            json!({"status":"disabled","reason":"no detailed tracing in this invocation"});
        record
            .as_object_mut()
            .unwrap()
            .entry("external_metrics")
            .or_insert_with(|| json!({}))["reason"] =
            json!("request/byte metrics require a separate I/O diagnostic");
    }
    match network::capture(&directory, &trace_output) {
        Ok(transport) if !transport.is_null() => {
            if traced {
                let external = record
                    .as_object_mut()
                    .unwrap()
                    .entry("external_metrics")
                    .or_insert_with(|| json!({}));
                external["requests"] = transport["requests"].clone();
                external["response_bytes"] = transport["response_bytes"].clone();
                external["response_bytes_boundary"] = json!(network::BOUNDARY);
            }
            record["network_io"] = transport;
        }
        Ok(_) => {}
        Err(error) => {
            record["status"] = json!("operational_failure");
            record["failure_reason"] = json!(format!("proxy capture failed: {error}"));
        }
    }
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_tracing_or_storage_addresses_fail_before_launch() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        for (purpose, uri, traced) in [
            ("timing", "s3://selective-read/table", true),
            ("validation", "s3://selective-read/table", true),
            ("io", "s3://another-bucket/table", true),
            ("io", "s3://user:secret@selective-read/table", true),
            ("io", "file:///table", false),
            ("io", "s3://selective-read/", false),
        ] {
            let output = temporary.path().join("result");
            let failure = run(Request {
                directory: temporary.path().join("absent-state"),
                binary: "/bin/true".into(),
                payload: json!({"purpose":purpose,"table_uri":uri,"comparison_revision":6}),
                output: output.clone(),
                reference: None,
                protocols: temporary.path().to_owned(),
                traced: Some(traced),
            })
            .unwrap_err()
            .to_string();
            assert!(
                failure.contains("detailed tracing") || failure.contains("invalid storage table"),
                "{failure}"
            );
            assert!(!output.exists());
        }
        assert_eq!(
            drain(temporary.path(), "unused", 0)
                .unwrap_err()
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::TimedOut
        );
        Ok(())
    }

    #[test]
    fn abandoned_observer_reaps_its_process_and_kills_trace_children() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let pidfile = temporary.path().join("grandchild");
        let child = Command::new("bash")
            .args([
                "-c",
                "sleep 30 & printf '%s' \"$!\" > \"$1\"; wait",
                "observer",
            ])
            .arg(&pidfile)
            .process_group(0)
            .spawn()?;
        let pid = child.id();
        let observer = Observer {
            child,
            finished: false,
        };
        let deadline = monotonic_ns()? + 2_000_000_000;
        while !pidfile.exists() && monotonic_ns()? < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        let grandchild = fs::read_to_string(&pidfile)?;
        drop(observer);
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
        if let Ok(stat) = fs::read_to_string(format!("/proc/{grandchild}/stat")) {
            assert!(stat.split(") ").nth(1).unwrap().starts_with('Z'), "{stat}");
        }
        Ok(())
    }
}
