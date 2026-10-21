//! Offline discovery and its stdout completion and failure behavior.

use super::*;
use crate::diagnostics::assert_single_terminal_record;

#[cfg(target_os = "linux")]
mod blocked_output;

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
fn unknown_discovery_names_are_argument_errors_without_reading_or_echoing_them()
-> Result<(), Box<dyn std::error::Error>> {
    for (selector, kind) in [
        ("--schema", "InvalidValue"),
        ("--command", "ValueValidation"),
    ] {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .env_clear()
                .env("PATH", "")
                .args([
                    "--diagnostics=json",
                    "capabilities",
                    selector,
                    "secret-missing-input.json",
                ]),
        )?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "invalid_cli_argument");
        assert_eq!(record["argument"], selector);
        assert_eq!(record["argument_kind"], kind);
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    Ok(())
}

#[test]
fn discovery_is_deterministic_without_credentials_or_a_search_path()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::Builder::new().prefix("secret-home-").tempdir()?;
    for (selectors, kind) in [
        (vec![], "capability_index"),
        (vec!["--all"], "capabilities"),
        (vec!["--command", "inspect"], "command_capabilities"),
        (vec!["--command", "scan"], "command_capabilities"),
        (vec!["--command", "query"], "command_capabilities"),
        (vec!["--command", "capabilities"], "command_capabilities"),
        (vec!["--command", "help"], "command_capabilities"),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command
            .env_clear()
            .env("PATH", "")
            .current_dir(directory.path())
            .arg("capabilities")
            .args(&selectors);
        let baseline = run_with_timeout(&mut command)?;
        assert!(baseline.status.success(), "{baseline:?}");
        assert!(baseline.stderr.is_empty());
        assert!(baseline.stdout.len() <= 1024 * 1024);
        let inventory: Value = serde_json::from_slice(&baseline.stdout)?;
        assert_eq!(inventory["kind"], kind);
        if selectors.is_empty() {
            assert!(baseline.stdout.len() <= 2048);
            assert_eq!(inventory["limits"]["max_output_bytes"], 2048);
        }
        assert_eq!(inventory["format_version"], 1);
        assert_eq!(inventory["cli_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(inventory["limits"]["deadline_ms"], 5000);

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
    }
    assert_eq!(fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}

#[test]
fn index_routes_to_complete_command_details_and_named_schemas()
-> Result<(), Box<dyn std::error::Error>> {
    let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).arg("capabilities"))?;
    assert!(output.status.success());
    assert!(output.stdout.len() <= 2048);
    let index: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(index["kind"], "capability_index");
    assert!(index.get("authorization").is_none());
    assert!(index.get("read_support").is_none());
    let run_advertised = |invocation: &Value| -> Result<Value, Box<dyn std::error::Error>> {
        let args: Vec<_> = invocation
            .as_array()
            .unwrap()
            .iter()
            .map(|arg| arg.as_str().unwrap())
            .collect();
        assert_eq!(args[0], "dar");
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .env_clear()
                .args(&args[1..]),
        )?;
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
        Ok(serde_json::from_slice(&output.stdout)?)
    };
    let full = run_advertised(&index["all_command"])?;
    assert_eq!(full["kind"], "capabilities");
    let commands = index["commands"].as_array().unwrap();
    assert_eq!(
        commands.len(),
        full["command"]["subcommands"].as_array().unwrap().len()
    );
    for entry in commands {
        assert!(entry.get("arguments").is_none());
        let name = entry["name"].as_str().unwrap();
        let details = run_advertised(&entry["details"])?;
        assert_eq!(details["kind"], "command_capabilities");
        let complete_command = full["command"]["subcommands"]
            .as_array()
            .unwrap()
            .iter()
            .find(|command| command["name"] == name)
            .unwrap();
        assert_eq!(&details["command"], complete_command);
        assert_eq!(details["stdout_formats"].as_object().unwrap().len(), 1);
        assert_eq!(
            details["stdout_formats"][name],
            full["stdout_formats"][name]
        );
        assert_eq!(details["read_support"][name], full["read_support"][name]);
        assert!(
            details["read_support"]
                .as_object()
                .unwrap()
                .keys()
                .all(|key| key == name)
        );
        let expected_sources = match name {
            "inspect" | "scan" => vec!["direct_storage", "local", "unity_catalog"],
            "query" => vec!["direct_storage", "local"],
            _ => vec![],
        };
        assert_eq!(
            details["authorization"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            expected_sources
        );
        for source in expected_sources {
            assert_eq!(
                details["authorization"][source],
                full["authorization"][source]
            );
        }
        let input_names: Vec<_> = details["input_schemas"]
            .as_array()
            .unwrap()
            .iter()
            .map(|schema| schema["name"].as_str().unwrap())
            .collect();
        let expected_inputs = match name {
            "scan" => vec!["storage-options", "predicate", "execution-options"],
            "inspect" | "query" => vec!["storage-options"],
            _ => vec![],
        };
        assert_eq!(input_names, expected_inputs);
        let output_names: Vec<_> = details["output_schemas"]
            .as_array()
            .unwrap()
            .iter()
            .map(|schema| schema["name"].as_str().unwrap())
            .collect();
        let expected_outputs = match name {
            "inspect" => vec!["inspection", "diagnostic", "completion"],
            "help" => vec!["diagnostic"],
            _ => vec!["diagnostic", "completion"],
        };
        assert_eq!(output_names, expected_outputs);
    }
    for name in index["schemas"].as_array().unwrap() {
        let mut invocation = index["schema_command"].clone();
        *invocation.as_array_mut().unwrap().last_mut().unwrap() = name.clone();
        let schema = run_advertised(&invocation)?;
        assert_eq!(&schema["name"], name);
    }
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
