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
fn named_schemas_are_offline_deterministic_and_use_the_same_diagnostics()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for (name, kind) in [
        ("storage-options", "input_schema"),
        ("execution-options", "input_schema"),
        ("predicate", "input_schema"),
        ("tables", "input_schema"),
        ("inspection", "output_schema"),
        ("diagnostic", "output_schema"),
        ("completion", "output_schema"),
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
    for schema in ["predicate", "completion", "storage-options", "tables"] {
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
        (vec!["--schema", "predicate"], "schema_guide"),
        (
            vec!["--schema", "predicate", "--detail", "timestamp_us"],
            "schema_detail",
        ),
        (vec!["--authorization", "s3"], "authorization_guide"),
        (vec!["--authorization", "azure"], "authorization_guide"),
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
fn discovery_routes_from_guides_to_selected_details_and_full_definitions()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for name in ["dar", "delta-arrow-reader"] {
        let executable = directory
            .path()
            .join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
        #[cfg(unix)]
        std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_dar"), &executable)?;
        #[cfg(not(unix))]
        fs::copy(env!("CARGO_BIN_EXE_dar"), &executable)?;
        assert_discovery_routes(&executable, name)?;
    }
    Ok(())
}

fn assert_discovery_routes(
    executable: &Path,
    executable_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let output = run_with_timeout(Command::new(executable).arg("capabilities"))?;
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
        let output = run_with_timeout(Command::new(executable).env_clear().args(&args[1..]))?;
        assert!(output.status.success(), "{args:?}: {output:?}");
        assert!(output.stderr.is_empty());
        let document: Value = serde_json::from_slice(&output.stdout)?;
        let limit = match document["kind"].as_str().unwrap() {
            "command_guide" => 4096,
            "schema_guide" | "schema_detail" | "authorization_guide" => 2048,
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
        assert!(guide["help"].as_str().unwrap().contains("Usage:"));
        assert!(guide["authorization"].get("direct_storage").is_none());
        assert!(guide["authorization"].get("unity_catalog").is_none());
        for provider in guide["authorization"]["providers"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let mut invocation = guide["authorization"]["details_command"].clone();
            *invocation.as_array_mut().unwrap().last_mut().unwrap() = provider.clone();
            let authorization = run_advertised(&invocation)?;
            assert_eq!(authorization["kind"], "authorization_guide");
            assert_eq!(&authorization["provider"], provider);
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
            "inspect" => vec!["inspection", "diagnostic", "completion"],
            "help" => vec!["diagnostic"],
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
        let full = run_advertised(&guide["full_command"])?;
        assert_eq!(&full["name"], name);
        jsonschema::validate(&full["schema"], &guide["example"])
            .map_err(|error| io::Error::other(error.to_string()))?;
        for field in ["fields", "operators", "scalar_types"] {
            for name in guide[field].as_array().into_iter().flatten() {
                let mut invocation = guide["detail_command"].clone();
                *invocation.as_array_mut().unwrap().last_mut().unwrap() = name.clone();
                let detail = run_advertised(&invocation)?;
                assert_eq!(detail["kind"], "schema_detail");
                assert_eq!(&detail["detail"], name);
                assert!(detail.get("schema").is_none());
                assert!(detail["definition"].is_object());
            }
        }
    }
    Ok(())
}

#[test]
fn discovered_guides_support_pinned_reads_and_predicate_repair()
-> Result<(), Box<dyn std::error::Error>> {
    use arrow::{
        array::{BooleanArray, Int32Array, Int64Array},
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
        assert_eq!(inspect_guide["stdout_formats"]["inspect"]["format"], "json");
        let inspection = run_with_timeout(
            Command::new(&executable)
                .env_clear()
                .args(["--diagnostics=json", "inspect"])
                .arg(&table),
        )?;
        assert!(inspection.status.success(), "{inspection:?}");
        let snapshot: Value = serde_json::from_slice(&inspection.stdout)?;
        let version = snapshot["table_version"].as_str().unwrap();

        let scan_guide = command_guide("scan")?;
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
        assert_eq!(completion["bindings"][0]["table_version"], version);
        assert_eq!(completion["rows"], rows.num_rows().to_string());
        assert_eq!(completion["output_bytes"], output.stdout.len().to_string());

        let query_guide = command_guide("query")?;
        let support = &query_guide["read_support"]["query"];
        let alias = support["positional_alias"].as_str().unwrap();
        let sql_file = directory.path().join("count rows.sql");
        fs::write(
            &sql_file,
            format!("SELECT count(*) AS n FROM {alias} WHERE id >= 100"),
        )?;
        let output = run_with_timeout(
            Command::new(&executable)
                .env_clear()
                .args(["--diagnostics=json", "query", "--table-version", version])
                .arg(support["sql_input"]["argument"].as_str().unwrap())
                .arg(&sql_file)
                .arg(&table),
        )?;
        assert!(output.status.success(), "{output:?}");
        let reader = StreamReader::try_new(Cursor::new(&output.stdout), None)?;
        let rows = concat_batches(&reader.schema(), &reader.collect::<Result<Vec<_>, _>>()?)?;
        assert_eq!(rows.num_rows(), 1);
        let count = rows
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count as usize, selected_rows.num_rows());
        let completion: Value = serde_json::from_slice(&output.stderr)?;
        assert_eq!(completion["kind"], "completion");
        assert_eq!(completion["scope"], "query");
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
