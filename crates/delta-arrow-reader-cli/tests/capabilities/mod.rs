//! Offline discovery and its stdout completion and failure behavior.

use super::*;
use crate::diagnostics::assert_single_terminal_record;

#[test]
fn discovery_is_deterministic_without_credentials_or_a_search_path()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::Builder::new().prefix("secret-home-").tempdir()?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
    command
        .env_clear()
        .env("PATH", "")
        .current_dir(directory.path())
        .arg("capabilities");
    let baseline = run_with_timeout(&mut command)?;
    assert!(baseline.status.success(), "{baseline:?}");
    assert!(baseline.stderr.is_empty());
    assert!(baseline.stdout.len() <= 1024 * 1024);
    let inventory: Value = serde_json::from_slice(&baseline.stdout)?;
    assert_eq!(inventory["kind"], "capabilities");
    assert_eq!(inventory["format_version"], 1);
    assert_eq!(inventory["cli_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(inventory["limits"]["deadline_ms"], 5000);
    assert_eq!(
        inventory["stdout_formats"]["scan"]["format"],
        "arrow_ipc_stream"
    );

    command
        .env("AWS_ACCESS_KEY_ID", "secret-access-key")
        .env("AWS_SECRET_ACCESS_KEY", "secret-access-secret")
        .env(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "secret-missing-credentials.json",
        )
        .env("DATABRICKS_CONFIG_FILE", "secret-missing-profile")
        .env("RUST_LOG", "trace");
    let output = run_with_timeout(&mut command)?;
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(output.stdout, baseline.stdout);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("secret"));

    let output = run_with_timeout(
        command
            .arg("--diagnostics=json")
            .env("RUST_LOG", "secret=invalid"),
    )?;
    assert!(output.status.success());
    assert_eq!(output.stdout, baseline.stdout);
    let completion = assert_single_terminal_record(&output.stderr, "completion");
    assert_eq!(completion["command"], "capabilities");
    assert_eq!(completion["bindings"], serde_json::json!([]));
    assert_eq!(completion["output_bytes"], output.stdout.len().to_string());
    assert_eq!(fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}

#[test]
fn a_closed_discovery_pipe_reports_failure() -> Result<(), Box<dyn std::error::Error>> {
    let (reader, writer) = io::pipe()?;
    drop(reader);
    let output = run_with_stdout(
        Command::new(env!("CARGO_BIN_EXE_dar")).args(["capabilities", "--diagnostics=json"]),
        Stdio::from(writer),
    )?;
    assert_eq!(output.status.code(), Some(3));
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["code"], "output_write");
    assert_eq!(record["source_kind"], "broken_pipe");
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn blocked_discovery_output_respects_the_deadline() -> Result<(), Box<dyn std::error::Error>> {
    use std::{io::Write, os::fd::AsRawFd};

    let (reader, mut writer) = io::pipe()?;
    // Fill the pipe before starting the child, independently of the inventory's size.
    let capacity = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETPIPE_SZ) };
    assert!(capacity > 0);
    writer.write_all(&vec![0; capacity as usize])?;
    let output = run_with_stdout(
        Command::new(env!("CARGO_BIN_EXE_dar")).args(["capabilities", "--diagnostics=json"]),
        Stdio::from(writer),
    )?;
    drop(reader);
    assert_eq!(output.status.code(), Some(1));
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["reason"], "discovery_deadline_exceeded");
    assert_eq!(record["source_kind"], "timeout");
    Ok(())
}
