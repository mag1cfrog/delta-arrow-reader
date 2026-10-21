//! Offline discovery and its stdout completion and failure behavior.

use super::*;
use crate::diagnostics::assert_single_terminal_record;

pub(super) fn assert_output_matches_schema(name: &str, output: &Value) {
    use std::sync::LazyLock;

    static INSPECTION: LazyLock<jsonschema::Validator> =
        LazyLock::new(|| output_schema_validator("inspection"));
    static DIAGNOSTIC: LazyLock<jsonschema::Validator> =
        LazyLock::new(|| output_schema_validator("diagnostic"));
    static COMPLETION: LazyLock<jsonschema::Validator> =
        LazyLock::new(|| output_schema_validator("completion"));
    let validator = match name {
        "inspection" => &*INSPECTION,
        "diagnostic" => &*DIAGNOSTIC,
        "completion" => &*COMPLETION,
        _ => panic!("unknown output schema: {name}"),
    };
    validator.validate(output).unwrap();
}

fn output_schema_validator(name: &str) -> jsonschema::Validator {
    let output = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .env_clear()
            .env("PATH", "")
            .args(["capabilities", "--schema", name]),
    )
    .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty());
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["kind"], "output_schema");
    jsonschema::validator_for(&document["schema"]).unwrap()
}

#[test]
fn named_schemas_are_offline_deterministic_and_use_the_same_diagnostics()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for (name, kind) in [
        ("storage-options", "input_schema"),
        ("execution-options", "input_schema"),
        ("predicate", "input_schema"),
        ("inspection", "output_schema"),
        ("diagnostic", "output_schema"),
        ("completion", "output_schema"),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command
            .env_clear()
            .env("PATH", "")
            .current_dir(directory.path())
            .args(["capabilities", "--schema", name]);
        let baseline = run_with_timeout(&mut command)?;
        assert!(baseline.status.success(), "{baseline:?}");
        assert!(baseline.stderr.is_empty());
        assert!(baseline.stdout.len() <= 1024 * 1024);
        let document: Value = serde_json::from_slice(&baseline.stdout)?;
        assert_eq!(document["kind"], kind);
        assert_eq!(document["format_version"], 1);
        assert_eq!(document["cli_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(document["name"], name);
        jsonschema::meta::validate(&document["schema"])
            .map_err(|error| io::Error::other(error.to_string()))?;

        command
            .env("AWS_SECRET_ACCESS_KEY", "secret-access-secret")
            .env("DATABRICKS_CONFIG_FILE", "secret-missing-profile")
            .env(
                "GOOGLE_APPLICATION_CREDENTIALS",
                "secret-missing-credentials.json",
            )
            .env("RUST_LOG", "secret=invalid")
            .arg("--diagnostics=json");
        let output = run_with_timeout(&mut command)?;
        assert!(output.status.success());
        assert_eq!(baseline.stdout, output.stdout);
        let completion = assert_single_terminal_record(&output.stderr, "completion");
        assert_eq!(completion["command"], "capabilities");
        assert_eq!(completion["bindings"], serde_json::json!([]));
        assert_eq!(completion["output_bytes"], output.stdout.len().to_string());
        assert!(!String::from_utf8_lossy(&output.stdout).contains("secret"));
    }
    assert_eq!(fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}

#[test]
fn unknown_schema_names_are_argument_errors_without_reading_or_echoing_them()
-> Result<(), Box<dyn std::error::Error>> {
    let output = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .env_clear()
            .env("PATH", "")
            .args([
                "--diagnostics=json",
                "capabilities",
                "--schema",
                "secret-missing-input.json",
            ]),
    )?;
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["code"], "invalid_cli_argument");
    assert_eq!(record["argument"], "--schema");
    assert_eq!(record["argument_kind"], "InvalidValue");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    Ok(())
}

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
    let text = String::from_utf8_lossy(&output.stdout);
    for private_value in [
        "secret-access-key",
        "secret-access-secret",
        "secret-missing-credentials.json",
        "secret-missing-profile",
        directory.path().to_str().unwrap(),
    ] {
        assert!(!text.contains(private_value));
    }

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
