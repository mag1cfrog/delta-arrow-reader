#![cfg(target_os = "linux")]

use serde_json::{Value, json};
use std::{
    error::Error,
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Stdio},
};

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

fn supervise(root: &Path, script: &str, payload: Value) -> Result<Value> {
    let reader = root.join("reader");
    fs::write(&reader, format!("#!/bin/bash\nset -eu\n{script}\n"))?;
    fs::set_permissions(&reader, fs::Permissions::from_mode(0o755))?;
    for name in ["stdout", "stderr"] {
        fs::write(root.join(name), "")?;
    }
    let input = json!({"command": [reader], "payload": payload, "output": root,
        "stdout": root.join("stdout"), "stderr": root.join("stderr"),
        "query_seconds": 0.2, "cleanup_seconds": 0.2});
    let mut child = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .args(["selective-read", "supervise"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(serde_json::to_string(&input)?.as_bytes())?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

const OPEN: &str = r#"printf '%s\n' '{"phase":"open","query_index":null,"query":null,"initialization_ns":null}' >&"$SELECTIVE_READ_CONTROL_FD""#;
const END: &str = r#"printf '%s\n' '{"phase":"query_end","query_index":0,"initialization_ns":null,"query":{"query_index":0,"output_rows":3,"output_batches":1,"completion_ns":100,"first_batch_ns":20}}' >&"$SELECTIVE_READ_CONTROL_FD""#;

#[test]
fn watchdog_records_success_failures_and_resource_usage() -> Result {
    for (script, status, phase, count) in [
        (format!("{OPEN}\n{END}"), "success", "cleanup", 1),
        (format!("{OPEN}\nsleep 2"), "timeout", "open", 0),
        (format!("{OPEN}\n{END}\nsleep 2"), "timeout", "cleanup", 1),
        (
            "printf '{' >&\"$SELECTIVE_READ_CONTROL_FD\"".into(),
            "operational_failure",
            "setup",
            0,
        ),
        (
            "printf '{}\\n' >&\"$SELECTIVE_READ_CONTROL_FD\"; sleep 2".into(),
            "operational_failure",
            "setup",
            0,
        ),
        (
            "printf '%5000s' ' ' >&\"$SELECTIVE_READ_CONTROL_FD\"; sleep 2".into(),
            "operational_failure",
            "setup",
            0,
        ),
    ] {
        let temporary = tempfile::tempdir()?;
        let result = supervise(
            temporary.path(),
            &script,
            json!({"execution_mode": "open", "purpose": "timing"}),
        )?;
        assert_eq!(result["status"], status, "{result}");
        assert_eq!(result["last_phase"], phase, "{result}");
        assert_eq!(result["completed_queries"].as_array().unwrap().len(), count);
        assert!(result["returncode"].is_i64());
        assert!(result["process_cpu_ns"].as_u64().unwrap() > 0);
        assert!(result["peak_rss_bytes"].as_u64().unwrap() > 0);
        assert_eq!(
            result["supervisor_executable_sha256"]
                .as_str()
                .unwrap()
                .len(),
            64
        );
        assert!(temporary.path().join("progress.jsonl").is_file());
    }
    Ok(())
}

#[test]
fn killed_process_groups_do_not_leave_a_running_grandchild() -> Result {
    let temporary = tempfile::tempdir()?;
    let pidfile = temporary.path().join("grandchild");
    let script = format!(
        "{OPEN}\nsleep 20 &\nprintf '%s' \"$!\" > '{}'\nwait",
        pidfile.display()
    );
    let result = supervise(
        temporary.path(),
        &script,
        json!({"execution_mode": "open", "purpose": "timing"}),
    )?;
    assert_eq!(result["status"], "timeout");
    assert_eq!(result["returncode"], -9);
    let pid = fs::read_to_string(pidfile)?;
    if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
        assert!(stat.split(") ").nth(1).unwrap().starts_with('Z'), "{stat}");
    }
    Ok(())
}
