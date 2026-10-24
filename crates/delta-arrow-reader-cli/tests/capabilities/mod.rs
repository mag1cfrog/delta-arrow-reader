//! Offline discovery and its stdout completion and failure behavior.

use super::*;
use crate::diagnostics::assert_single_terminal_record;

#[cfg(target_os = "linux")]
mod blocked_output;

pub(super) fn assert_output_matches_schema(name: &str, output: &Value) {
    use std::sync::LazyLock;

    static INSPECTION_VALIDATOR: LazyLock<jsonschema::Validator> =
        LazyLock::new(|| load_cli_output_schema_validator("inspection"));
    static INSPECTION_SUMMARY_VALIDATOR: LazyLock<jsonschema::Validator> =
        LazyLock::new(|| load_cli_output_schema_validator("inspection-summary"));
    static DIAGNOSTIC_VALIDATOR: LazyLock<jsonschema::Validator> =
        LazyLock::new(|| load_cli_output_schema_validator("diagnostic"));
    static COMPLETION_VALIDATOR: LazyLock<jsonschema::Validator> =
        LazyLock::new(|| load_cli_output_schema_validator("completion"));
    static ROWS_VALIDATOR: LazyLock<jsonschema::Validator> =
        LazyLock::new(|| load_cli_output_schema_validator("rows"));
    static ARTIFACT_VALIDATOR: LazyLock<jsonschema::Validator> =
        LazyLock::new(|| load_cli_output_schema_validator("result-artifact"));
    static MANIFEST_VALIDATOR: LazyLock<jsonschema::Validator> =
        LazyLock::new(|| load_cli_output_schema_validator("result-manifest"));
    let validator = match name {
        "inspection" => &*INSPECTION_VALIDATOR,
        "inspection-summary" => &*INSPECTION_SUMMARY_VALIDATOR,
        "diagnostic" => &*DIAGNOSTIC_VALIDATOR,
        "completion" => &*COMPLETION_VALIDATOR,
        "rows" => &*ROWS_VALIDATOR,
        "result-artifact" => &*ARTIFACT_VALIDATOR,
        "result-manifest" => &*MANIFEST_VALIDATOR,
        _ => panic!("unknown output schema: {name}"),
    };
    validator.validate(output).unwrap();
}

fn load_cli_output_schema_validator(name: &str) -> jsonschema::Validator {
    let output = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .env_clear()
            .env("PATH", "")
            .args(["capabilities", "--schema", name, "--full"]),
    )
    .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stderr.is_empty());
    let document: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(document["kind"], "output_schema");
    jsonschema::validator_for(&document["schema"]).unwrap()
}

#[test]
fn discovered_completion_versions_match_each_command_and_query_format()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let sql = directory.path().join("count.sql");
    fs::write(&sql, "SELECT count(*) AS n FROM t")?;
    let table = Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table",
    );
    let table = table.to_str().unwrap();
    let sql = sql.to_str().unwrap();
    let inventory = run_with_timeout(
        Command::new(env!("CARGO_BIN_EXE_dar"))
            .env_clear()
            .args(["capabilities", "--all"]),
    )?;
    assert!(inventory.status.success(), "{inventory:?}");
    let inventory: Value = serde_json::from_slice(&inventory.stdout)?;
    for (name, arguments, format, expected_version) in [
        ("capabilities", vec![], None, 1),
        ("help", vec![], None, 1),
        ("inspect", vec![table, "--view", "summary"], None, 1),
        ("scan", vec![table, "--limit", "1"], None, 1),
        (
            "preview",
            vec![table, "--column", "id", "--limit", "1"],
            None,
            2,
        ),
        ("query", vec![table, "--sql-file", sql], None, 1),
        (
            "query",
            vec![table, "--sql-file", sql, "--format", "arrow"],
            Some("arrow"),
            1,
        ),
        (
            "query",
            vec![table, "--sql-file", sql, "--format", "json"],
            Some("json"),
            2,
        ),
    ] {
        let metadata =
            run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).env_clear().args([
                "capabilities",
                "--command",
                name,
                "--full",
            ]))?;
        assert!(metadata.status.success(), "{metadata:?}");
        let metadata: Value = serde_json::from_slice(&metadata.stdout)?;
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .env_clear()
                .args(["--diagnostics=json", name])
                .args(arguments),
        )?;
        assert!(output.status.success(), "{output:?}");
        let receipt = assert_single_terminal_record(&output.stderr, "completion");
        assert_output_matches_schema("completion", &receipt);
        let guide = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).args([
            "capabilities",
            "--command",
            name,
        ]))?;
        assert!(guide.status.success());
        let guide: Value = serde_json::from_slice(&guide.stdout)?;
        assert!(
            guide["output_schemas"]
                .as_array()
                .unwrap()
                .contains(&receipt["kind"])
        );
        assert!(
            metadata["diagnostics"]["json"]["schemas"]
                .as_array()
                .unwrap()
                .contains(&receipt["kind"])
        );
        assert!(
            metadata["output_schemas"]
                .as_array()
                .unwrap()
                .iter()
                .any(|schema| schema["name"] == receipt["kind"])
        );
        assert_eq!(receipt["format_version"], expected_version);
        if format.is_none() {
            assert_eq!(
                metadata["diagnostics"]["json"]["format_versions"]["completion"],
                receipt["format_version"],
                "{name} default completion"
            );
        }
        for document in [&inventory, &metadata] {
            let stdout = &document["stdout_formats"][name];
            let selected_format = format.or_else(|| stdout["default"].as_str());
            let advertised = selected_format
                .map(|format| &stdout["choices"][format]["completion_format_version"])
                .or_else(|| stdout.get("completion_format_version"))
                .unwrap_or(&document["diagnostics"]["json"]["format_versions"]["completion"]);
            assert_eq!(advertised, &receipt["format_version"], "{name} {format:?}");
        }
        assert_eq!(
            metadata["diagnostics"]["json"]["completion_format_versions"],
            match name {
                "preview" => serde_json::json!([2]),
                "query" => serde_json::json!([1, 2, 3]),
                "scan" => serde_json::json!([1, 3]),
                _ => serde_json::json!([1]),
            }
        );
    }
    Ok(())
}

#[test]
fn named_schemas_are_offline_deterministic_and_use_the_same_diagnostics()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for (name, kind) in [
        ("storage-options", "input_schema"),
        ("execution-options", "input_schema"),
        ("predicate", "input_schema"),
        ("tables", "input_schema"),
        ("inspection", "output_schema"),
        ("inspection-summary", "output_schema"),
        ("diagnostic", "output_schema"),
        ("completion", "output_schema"),
        ("rows", "output_schema"),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command
            .env_clear()
            .env("PATH", "")
            .current_dir(directory.path())
            .args(["capabilities", "--schema", name, "--full"]);
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
        ("--authorization", "InvalidValue"),
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
    for schema in ["predicate", "completion", "storage-options"] {
        let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).args([
            "capabilities",
            "--schema",
            schema,
            "--detail",
            "secret-missing-input.json",
            "--diagnostics=json",
        ]))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["argument"], "--detail");
        assert_eq!(record["recovery"], "fix_input");
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    for (service, detail) in [
        ("s3", "secret-missing-input.json"),
        ("unity-catalog", "env-file"),
        ("unity-catalog", "env-file-grammar"),
    ] {
        let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).env_clear().args([
            "capabilities",
            "--authorization",
            service,
            "--detail",
            detail,
            "--diagnostics=json",
        ]))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["argument"], "--detail");
        assert_eq!(record["recovery"], "fix_input");
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
        (vec!["--command", "inspect"], "command_guide"),
        (vec!["--command", "scan"], "command_guide"),
        (vec!["--command", "query"], "command_guide"),
        (vec!["--command", "capabilities"], "command_guide"),
        (vec!["--command", "help"], "command_guide"),
        (vec!["--command", "query", "--full"], "command_capabilities"),
        (
            vec!["--command", "query", "--detail", "runtime"],
            "command_detail",
        ),
        (
            vec!["--command", "query", "--detail", "resources"],
            "command_detail",
        ),
        (
            vec!["--command", "query", "--detail", "artifacts"],
            "command_detail",
        ),
        (
            vec!["--command", "scan", "--detail", "artifacts"],
            "command_detail",
        ),
        (vec!["--schema", "predicate"], "schema_guide"),
        (vec!["--schema", "tables"], "schema_guide"),
        (
            vec!["--schema", "tables", "--detail", "tables[].authorization"],
            "schema_detail",
        ),
        (
            vec!["--schema", "predicate", "--detail", "timestamp_us"],
            "schema_detail",
        ),
        (vec!["--authorization", "s3"], "authorization_guide"),
        (
            vec!["--authorization", "s3", "--detail", "env-file"],
            "authorization_detail",
        ),
        (
            vec!["--authorization", "s3", "--detail", "env-file-grammar"],
            "authorization_detail",
        ),
        (
            vec!["--authorization", "azure-blob", "--detail", "env-file"],
            "authorization_detail",
        ),
        (
            vec!["--authorization", "gcs", "--detail", "env-file"],
            "authorization_detail",
        ),
        (
            vec!["--authorization", "s3", "--full"],
            "authorization_capabilities",
        ),
        (
            vec!["--authorization", "unity-catalog", "--full"],
            "authorization_capabilities",
        ),
        (vec!["--authorization", "azure-blob"], "authorization_guide"),
        (vec!["--authorization", "gcs"], "authorization_guide"),
        (
            vec!["--authorization", "unity-catalog"],
            "authorization_guide",
        ),
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
        let limit = match kind {
            "capability_index"
            | "command_detail"
            | "schema_guide"
            | "schema_detail"
            | "authorization_guide"
            | "authorization_detail" => 2048,
            "command_guide" => 4096,
            _ => 1024 * 1024,
        };
        assert!(baseline.stdout.len() <= limit);
        assert_eq!(inventory["format_version"], 1);
        if limit == 1024 * 1024 {
            assert_eq!(inventory["cli_version"], env!("CARGO_PKG_VERSION"));
            assert_eq!(inventory["limits"]["deadline_ms"], 5000);
        } else {
            assert!(inventory.get("limits").is_none());
        }

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
fn discovery_routes_from_guides_to_selected_details_and_full_definitions()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for executable_name in ["dar", "delta-arrow-reader"] {
        let executable = directory
            .path()
            .join(format!("{executable_name}{}", std::env::consts::EXE_SUFFIX));
        #[cfg(unix)]
        std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_dar"), &executable)?;
        #[cfg(not(unix))]
        fs::copy(env!("CARGO_BIN_EXE_dar"), &executable)?;
        let output = run_with_timeout(Command::new(&executable).arg("capabilities"))?;
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
            assert_eq!(args[0], executable_name);
            let output = run_with_timeout(Command::new(&executable).env_clear().args(&args[1..]))?;
            assert!(output.status.success(), "{output:?}");
            assert!(output.stderr.is_empty());
            let document: Value = serde_json::from_slice(&output.stdout)?;
            let limit = match document["kind"].as_str().unwrap() {
                "command_guide" => 4096,
                "schema_guide"
                | "schema_detail"
                | "authorization_guide"
                | "authorization_detail" => 2048,
                _ => 1024 * 1024,
            };
            assert!(
                output.stdout.len() <= limit,
                "{args:?}: {} bytes",
                output.stdout.len()
            );
            Ok(document)
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
            let guide = run_advertised(&entry["details"])?;
            assert_eq!(guide["kind"], "command_guide");
            assert_eq!(guide["command"], name);
            assert!(guide.get("help").is_none());
            assert!(guide.get("argument_conflicts").is_none());
            assert!(guide.get("read_support").is_none());
            if matches!(name, "preview" | "query") {
                assert_eq!(
                    guide["output_schemas"],
                    if name == "query" {
                        serde_json::json!([
                            "diagnostic",
                            "completion",
                            "rows",
                            "result-artifact",
                            "result-manifest"
                        ])
                    } else {
                        serde_json::json!(["diagnostic", "completion", "rows"])
                    }
                );
                assert!(guide.get("join").is_none());
                let mut invocation = guide["schema_command"].clone();
                *invocation.as_array_mut().unwrap().last_mut().unwrap() =
                    serde_json::json!("completion");
                let completion = run_advertised(&invocation)?;
                assert_eq!(
                    completion["example"]["format_version"],
                    guide["completion_format_version"]
                );
                for omitted in ["batches", "terminal", "scope", "stop_reason", "has_more"] {
                    assert!(completion["example"].get(omitted).is_none(), "{omitted}");
                }
            }
            let help: Vec<_> = guide["help_command"]
                .as_array()
                .unwrap()
                .iter()
                .map(|arg| arg.as_str().unwrap())
                .collect();
            let output = run_with_timeout(Command::new(&executable).args(&help[1..]))?;
            assert!(output.status.success(), "{output:?}");
            assert!(String::from_utf8_lossy(&output.stdout).contains("Usage:"));
            assert!(guide["authorization"].get("direct_storage").is_none());
            assert!(guide["authorization"].get("unity_catalog").is_none());
            for guide_name in guide["authorization"]["guides"]
                .as_array()
                .into_iter()
                .flatten()
            {
                let mut invocation = guide["authorization"]["details_command"].clone();
                *invocation.as_array_mut().unwrap().last_mut().unwrap() = guide_name.clone();
                let authorization = run_advertised(&invocation)?;
                assert_eq!(authorization["kind"], "authorization_guide");
                assert_eq!(&authorization["name"], guide_name);
                assert!(
                    authorization["authorization"]
                        .get("credential_option_aliases")
                        .is_none()
                );
                let expanded = run_advertised(&authorization["full_command"])?;
                assert_eq!(expanded["kind"], "authorization_capabilities");
                assert_eq!(&expanded["name"], guide_name);
                if guide_name != "unity-catalog" {
                    let variables = run_advertised(
                        &authorization["authorization"]["env_file_details_command"],
                    )?;
                    assert_eq!(variables["kind"], "authorization_detail");
                    assert_eq!(&variables["name"], guide_name);
                    assert!(variables.get("syntax").is_none());
                    let grammar = run_advertised(&variables["grammar_command"])?;
                    assert_eq!(grammar["detail"], "env-file-grammar");
                    assert!(grammar.get("variables").is_none());
                    assert_eq!(run_advertised(&grammar["variables_command"])?, variables);
                }
                assert!(
                    authorization["authorization"]["commands"]
                        .as_array()
                        .unwrap()
                        .contains(&guide["command"])
                );
            }
            let details = run_advertised(&guide["full_command"])?;
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
                "inspect" | "scan" | "preview" | "query" => {
                    vec!["direct_storage", "local", "unity_catalog"]
                }
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
                "scan" | "preview" => vec!["storage-options", "predicate", "execution-options"],
                "inspect" => vec!["storage-options"],
                "query" => vec!["storage-options", "tables"],
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
                "inspect" => vec![
                    "inspection",
                    "inspection-summary",
                    "diagnostic",
                    "completion",
                ],
                "query" => vec![
                    "diagnostic",
                    "completion",
                    "rows",
                    "result-artifact",
                    "result-manifest",
                ],
                "preview" => vec!["diagnostic", "completion", "rows"],
                "scan" => vec![
                    "diagnostic",
                    "completion",
                    "result-artifact",
                    "result-manifest",
                ],
                _ => vec!["diagnostic", "completion"],
            };
            assert_eq!(output_names, expected_outputs);
        }
        for name in index["schemas"].as_array().unwrap() {
            let mut invocation = index["schema_command"].clone();
            *invocation.as_array_mut().unwrap().last_mut().unwrap() = name.clone();
            let guide = run_advertised(&invocation)?;
            assert_eq!(guide["kind"], "schema_guide");
            assert_eq!(&guide["name"], name);
            assert!(guide.get("schema").is_none());
            for guide_name in guide["authorization_guides"]
                .as_array()
                .into_iter()
                .flatten()
            {
                let mut invocation = guide["authorization_command"].clone();
                *invocation.as_array_mut().unwrap().last_mut().unwrap() = guide_name.clone();
                let authorization = run_advertised(&invocation)?;
                assert_eq!(authorization["kind"], "authorization_guide");
                assert_eq!(&authorization["name"], guide_name);
                assert!(
                    authorization["authorization"]
                        .get("credential_option_aliases")
                        .is_none()
                );
                let expanded = run_advertised(&authorization["full_command"])?;
                assert_eq!(expanded["kind"], "authorization_capabilities");
                assert_eq!(&expanded["name"], guide_name);
            }
            let full = run_advertised(&guide["full_command"])?;
            assert_eq!(&full["name"], name);
            if matches!(name.as_str(), Some("result-artifact" | "result-manifest")) {
                assert_eq!(
                    guide["fields"],
                    serde_json::json!(
                        full["schema"]["properties"]
                            .as_object()
                            .unwrap()
                            .keys()
                            .collect::<Vec<_>>()
                    )
                );
                for field in guide["fields"].as_array().unwrap() {
                    let mut invocation = guide["detail_command"].clone();
                    *invocation.as_array_mut().unwrap().last_mut().unwrap() = field.clone();
                    let detail = run_advertised(&invocation)?;
                    assert_eq!(detail["detail"], *field);
                    assert!(detail.get("definition").is_some() || detail.get("fields").is_some());
                }
                continue;
            }
            jsonschema::validate(&full["schema"], &guide["example"])
                .map_err(|error| io::Error::other(error.to_string()))?;
            let mut detail_names = std::collections::BTreeSet::new();
            for field in [
                "other_fields",
                "version_1_fields",
                "operators",
                "scalar_types",
            ] {
                for name in guide[field].as_array().into_iter().flatten() {
                    assert!(detail_names.insert(name.as_str().unwrap().to_owned()));
                }
            }
            if name == "completion" {
                assert_eq!(guide["example"]["format_version"], 2);
                assert!(guide.get("other_fields").is_none());
                let legacy_fields = guide["version_1_fields"].as_array().unwrap();
                assert_eq!(legacy_fields.len(), 6);
                for field in [
                    "terminal",
                    "batches",
                    "scope",
                    "completion",
                    "stop_reason",
                    "has_more",
                ] {
                    assert!(legacy_fields.iter().any(|name| name == field));
                    assert!(guide["example"].get(field).is_none());
                }
            }
            if let Some(properties) = full["schema"]["properties"].as_object() {
                assert!(guide.get("fields").is_none());
                for name in guide["example"].as_object().unwrap().keys() {
                    assert!(detail_names.insert(name.clone()), "repeated field: {name}");
                }
                let root_fields = detail_names
                    .iter()
                    .filter(|name| !name.starts_with("tables[]."))
                    .cloned()
                    .collect::<std::collections::BTreeSet<_>>();
                assert_eq!(root_fields, properties.keys().cloned().collect());
                if let Some(other_fields) = guide.get("other_fields") {
                    assert!(!other_fields.as_array().unwrap().is_empty());
                }
            }
            if name == "tables" {
                assert!(
                    guide["detail_syntax"]
                        .as_str()
                        .unwrap()
                        .contains("tables[].FIELD")
                );
                for field in ["alias", "location", "table_version"] {
                    assert!(guide["example"]["tables"][0].get(field).is_some());
                    detail_names.insert(format!("tables[].{field}"));
                }
                for field in ["source", "storage_options_file", "databricks_profile"] {
                    detail_names.insert(format!("tables[].authorization.{field}"));
                }
            }
            for name in detail_names {
                let mut invocation = guide["detail_command"].clone();
                *invocation.as_array_mut().unwrap().last_mut().unwrap() = name.clone().into();
                let detail = run_advertised(&invocation)?;
                assert_eq!(detail["kind"], "schema_detail");
                assert_eq!(detail["detail"], name);
                assert!(detail.get("schema").is_none());
                assert!(detail["definition"].is_object());
                if name == "tables[].authorization" {
                    for guide_name in detail["authorization_guides"].as_array().unwrap() {
                        let mut invocation = detail["authorization_command"].clone();
                        *invocation.as_array_mut().unwrap().last_mut().unwrap() =
                            guide_name.clone();
                        let authorization = run_advertised(&invocation)?;
                        assert_eq!(authorization["kind"], "authorization_guide");
                        assert_eq!(&authorization["name"], guide_name);
                        let expanded = run_advertised(&authorization["full_command"])?;
                        assert_eq!(expanded["kind"], "authorization_capabilities");
                        assert_eq!(&expanded["name"], guide_name);
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn discovered_guides_support_pinned_reads_and_predicate_repair()
-> Result<(), Box<dyn std::error::Error>> {
    use arrow::{
        array::{BooleanArray, Int32Array},
        compute::filter_record_batch,
    };
    use serde_json::json;

    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned",
    );
    let table = fixture.join("table");
    let reader = FileReader::try_new(File::open(fixture.join("expected.arrow"))?, None)?;
    let expected = concat_batches(&reader.schema(), &reader.collect::<Result<Vec<_>, _>>()?)?;
    let ids = expected
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let selected = BooleanArray::from_iter(ids.iter().map(|id| id.is_some_and(|id| id >= 100)));
    let selected_rows = filter_record_batch(&expected, &selected)?;
    assert!(selected_rows.num_rows() > 0 && selected_rows.num_rows() < expected.num_rows());

    let directory = tempfile::Builder::new()
        .prefix("discovered read ")
        .tempdir()?;
    for name in ["dar", "delta-arrow-reader"] {
        let executable = directory
            .path()
            .join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
        #[cfg(unix)]
        std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_dar"), &executable)?;
        #[cfg(not(unix))]
        fs::copy(env!("CARGO_BIN_EXE_dar"), &executable)?;
        let discover = |invocation: &Value| -> Result<Value, Box<dyn std::error::Error>> {
            let args: Vec<_> = invocation
                .as_array()
                .unwrap()
                .iter()
                .map(|arg| arg.as_str().unwrap())
                .collect();
            assert_eq!(args[0], name);
            assert!(
                !args
                    .iter()
                    .any(|arg| matches!(*arg, "--full" | "--all" | "--authorization"))
            );
            let output = run_with_timeout(Command::new(&executable).env_clear().args(&args[1..]))?;
            assert!(output.status.success(), "{output:?}");
            assert!(output.stderr.is_empty());
            Ok(serde_json::from_slice(&output.stdout)?)
        };
        let index = discover(&json!([name, "capabilities"]))?;
        let command_guide = |command: &str| {
            let entry = index["commands"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["name"] == command)
                .unwrap();
            discover(&entry["details"])
        };
        let inspect_guide = command_guide("inspect")?;
        assert_eq!(inspect_guide["stdout"], "json");
        let inspect_args: Vec<_> = inspect_guide["example"]
            .as_array()
            .unwrap()
            .iter()
            .skip(1)
            .map(|arg| {
                if arg == "./table" {
                    table.to_str().unwrap()
                } else {
                    arg.as_str().unwrap()
                }
            })
            .collect();
        let inspection =
            run_with_timeout(Command::new(&executable).env_clear().args(inspect_args))?;
        assert!(inspection.status.success(), "{inspection:?}");
        let snapshot: Value = serde_json::from_slice(&inspection.stdout)?;
        let version = snapshot["table_version"].as_str().unwrap();

        let scan_guide = command_guide("scan")?;
        let sample_args: Vec<_> = scan_guide["example"]
            .as_array()
            .unwrap()
            .iter()
            .skip(1)
            .map(|arg| {
                if arg == "./table" {
                    table.to_str().unwrap()
                } else {
                    arg.as_str().unwrap()
                }
            })
            .collect();
        let sample = run_with_timeout(Command::new(&executable).env_clear().args(sample_args))?;
        assert!(sample.status.success(), "{sample:?}");
        let reader = StreamReader::try_new(Cursor::new(&sample.stdout), None)?;
        let rows = concat_batches(&reader.schema(), &reader.collect::<Result<Vec<_>, _>>()?)?;
        assert_eq!(rows.num_rows(), 1);
        assert_eq!(rows.schema(), expected.schema());
        let sample_completion: Value = serde_json::from_slice(&sample.stderr)?;
        assert_eq!(sample_completion["scope"], "limited_scan");
        assert_eq!(sample_completion["has_more"], Value::Null);
        let mut invocation = scan_guide["schema_command"].clone();
        *invocation.as_array_mut().unwrap().last_mut().unwrap() = json!("completion");
        let completion_guide = discover(&invocation)?;
        let mut invocation = completion_guide["detail_command"].clone();
        *invocation.as_array_mut().unwrap().last_mut().unwrap() = json!("has_more");
        let has_more = discover(&invocation)?;
        assert!(jsonschema::is_valid(
            &has_more["definition"],
            &sample_completion["has_more"]
        ));
        let conditions = json!({"allOf": has_more["conditions"]});
        assert!(jsonschema::is_valid(&conditions, &sample_completion));
        let mut missing = sample_completion.clone();
        missing.as_object_mut().unwrap().remove("has_more");
        assert!(!jsonschema::is_valid(&conditions, &missing));
        assert!(
            has_more["definition"]["description"]
                .as_str()
                .unwrap()
                .contains("row limit")
        );
        let input = scan_guide["input_schemas"]
            .as_array()
            .unwrap()
            .iter()
            .find(|input| input["name"] == "predicate")
            .unwrap();
        let mut invocation = scan_guide["schema_command"].clone();
        *invocation.as_array_mut().unwrap().last_mut().unwrap() = input["name"].clone();
        let predicate_guide = discover(&invocation)?;
        let mut predicate = predicate_guide["example"].clone();
        predicate["op"] = json!("ge");
        predicate["column"] = json!("id");
        predicate["value"]["value"] = json!(100);
        let predicate_file = directory.path().join("row filter.json");
        fs::write(&predicate_file, serde_json::to_vec(&predicate)?)?;
        let mut scan = Command::new(&executable);
        scan.env_clear()
            .args(["--diagnostics=json", "scan", "--table-version", version])
            .arg(input["argument"].as_str().unwrap())
            .arg(&predicate_file)
            .arg(&table);
        let rejected = run_with_timeout(&mut scan)?;
        assert_eq!(rejected.status.code(), Some(2));
        assert!(rejected.stdout.is_empty());
        let diagnostic: Value = serde_json::from_slice(&rejected.stderr)?;
        assert_eq!(diagnostic["kind"], "diagnostic");
        assert_eq!(diagnostic["recovery"], "fix_input");
        assert_eq!(diagnostic["argument"], input["argument"]);
        assert_eq!(diagnostic["field_path"], "/value/value");

        let mut invocation = predicate_guide["detail_command"].clone();
        *invocation.as_array_mut().unwrap().last_mut().unwrap() =
            predicate["value"]["type"].clone();
        let scalar = discover(&invocation)?;
        assert_eq!(
            scalar["definition"]["properties"]["value"]["type"],
            "string"
        );
        *predicate
            .pointer_mut(diagnostic["field_path"].as_str().unwrap())
            .unwrap() = json!("100");
        fs::write(&predicate_file, serde_json::to_vec(&predicate)?)?;
        let output = run_with_timeout(&mut scan)?;
        assert!(output.status.success(), "{output:?}");
        let reader = StreamReader::try_new(Cursor::new(&output.stdout), None)?;
        let rows = concat_batches(&reader.schema(), &reader.collect::<Result<Vec<_>, _>>()?)?;
        assert_eq!(sort_by_id(&rows)?, sort_by_id(&selected_rows)?);
        let completion: Value = serde_json::from_slice(&output.stderr)?;
        assert_eq!(completion["kind"], "completion");
        assert_eq!(completion["scope"], "scan");
        assert_eq!(completion["has_more"], false);
        assert!(jsonschema::is_valid(
            &has_more["definition"],
            &completion["has_more"]
        ));
        assert_eq!(completion["bindings"][0]["table_version"], version);
        assert_eq!(completion["rows"], rows.num_rows().to_string());
        assert_eq!(completion["output_bytes"], output.stdout.len().to_string());

        let query_guide = command_guide("query")?;
        let sql = &query_guide["sql"];
        let alias = sql["table"].as_str().unwrap();
        let sql_file = directory.path().join("count rows.sql");
        fs::write(
            &sql_file,
            format!("{} WHERE id >= 100", sql["example"].as_str().unwrap()),
        )?;
        assert_eq!(alias, "t");
        let query_args: Vec<_> = query_guide["example"]
            .as_array()
            .unwrap()
            .iter()
            .skip(1)
            .map(|arg| match arg.as_str().unwrap() {
                "./table" => table.to_str().unwrap(),
                "query.sql" => sql_file.to_str().unwrap(),
                argument => argument,
            })
            .collect();
        let output = run_with_timeout(
            Command::new(&executable)
                .env_clear()
                .args(query_args)
                .args(["--table-version", version]),
        )?;
        assert!(output.status.success(), "{output:?}");
        let result: Value = serde_json::from_slice(&output.stdout)?;
        assert_output_matches_schema("rows", &result);
        assert_eq!(
            result["rows"],
            json!([[selected_rows.num_rows().to_string()]])
        );
        assert_eq!(result["schema"]["fields"].as_array().unwrap().len(), 1);
        assert_eq!(result["bindings"][0]["table_version"], version);
        assert_eq!(result["scope"], "query");
        assert_eq!(result["completion"], "complete");
        let completion = assert_single_terminal_record(&output.stderr, "completion");
        assert_eq!(
            completion["format_version"],
            query_guide["completion_format_version"]
        );
        assert_eq!(completion["command"], "query");
        assert_eq!(completion["bindings"][0]["table_version"], version);
        assert_eq!(completion["rows"], "1");
        assert_eq!(completion["output_bytes"], output.stdout.len().to_string());
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
