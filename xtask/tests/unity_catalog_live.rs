//! Offline checks for the opt-in UC harness, including its report redaction boundary.

#![allow(clippy::panic, clippy::unwrap_used)]

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{Arc, OnceLock},
};

use arrow_array::{Int32Array, RecordBatch};
use arrow_ipc::writer::{FileWriter, StreamWriter};
use arrow_schema::{DataType, Field, Schema};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[path = "unity_catalog_live/local_cli.rs"]
mod local_cli;
#[path = "unity_catalog_live/sql.rs"]
mod sql;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";

/// Synthetic reference data and fake executables, isolated from installed cloud tools.
struct OfflineFixture {
    directory: tempfile::TempDir,
    config: Value,
}

impl OfflineFixture {
    fn new() -> TestResult<Self> {
        let directory = tempfile::Builder::new()
            .prefix("secret uc harness-")
            .tempdir()?;
        let root = directory.path();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("secret_value", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )?;
        write_arrow_file(&root.join("table.arrow"), &batch)?;
        let projected = batch.project(&[1])?;
        write_arrow_file(&root.join("scan.arrow"), &projected.slice(0, 2))?;

        let inspection = json!({"format_version": 1, "table_version": "7", "schema": schema});
        fs::write(root.join("inspect.stdout"), format!("{inspection}\n"))?;
        let mut stream = StreamWriter::try_new(Vec::new(), &projected.schema())?;
        // Scan order and batch boundaries deliberately differ from the independent reference.
        stream.write(&projected.slice(1, 1))?;
        stream.write(&projected.slice(0, 1))?;
        stream.finish()?;
        fs::write(root.join("scan.stdout"), stream.into_inner()?)?;
        for operation in ["inspect", "scan"] {
            let mut completion = json!({
                "format_version": 1, "kind": "completion", "terminal": true,
                "command": operation, "completion": "complete", "scope": operation,
                "stop_reason": "end_of_input", "has_more": false,
                "bindings": [{"alias": "t", "table_version": "7"}],
                "output_bytes": fs::metadata(root.join(format!("{operation}.stdout")))?.len().to_string(),
            });
            if operation == "scan" {
                completion["scope"] = json!("limited_scan");
                completion["rows"] = json!("2");
                completion["batches"] = json!("2");
            }
            fs::write(
                root.join(format!("{operation}.stderr")),
                format!("{completion}\n"),
            )?;
        }
        fs::write(
            root.join("predicate.json"),
            r#"{"op":"lt","column":"id","value":{"type":"int32","value":"3"}}"#,
        )?;
        fs::write(root.join("storage.json"), r#"{"region":"us-west-2"}"#)?;
        sql::prepare_fixture(root)?;
        fs::write(root.join("databricks.version"), "Databricks CLI v0.300.0\n")?;
        for name in ["databricks", "dar"] {
            write_mock_cli(&executable_path(root, name))?;
        }
        let config = json!({
            "databricks_profile": "secret profile $(do-not-execute)",
            "table": "uc://secret.catalog.table", "table_version": 7,
            "expected_table": "table.arrow",
            "scan": {"columns": ["secret_value"], "predicate_file": "predicate.json", "expected_result": "scan.arrow"},
            "sql": {
                "aggregate": {"sql_file": "aggregate.sql", "expected_result": "aggregate.arrow"},
                "join": {"sql_file": "join.sql", "expected_result": "join.arrow"},
                "local_table": "local-table", "local_table_version": 3,
            },
            "storage_options_file": "storage.json",
            "context": {"credential_form": "s3_session", "auth_mode": "user_oauth", "table_kind": "external",
                "protocol": {"min_reader_version": 1, "min_writer_version": 2, "reader_features": []}},
        });
        let fixture = Self { directory, config };
        fixture.save_config()?;
        Ok(fixture)
    }

    fn save_config(&self) -> TestResult {
        fs::write(
            self.directory.path().join("fixture.json"),
            serde_json::to_vec(&self.config)?,
        )?;
        Ok(())
    }

    fn command(&self) -> Command {
        let mut command = isolated_xtask(self.directory.path());
        command
            .env("UC_FIXTURE_DIR", self.directory.path())
            // Relative fixture paths must resolve against the configuration, not this directory.
            .current_dir(std::env::temp_dir())
            .args(["cli", "test-unity-catalog", "--fixture"])
            .arg(self.directory.path().join("fixture.json"))
            .arg("--binary")
            .arg(executable_path(self.directory.path(), "dar"))
            .args(["--dar-revision", REVISION]);
        command
    }

    fn change_json(&self, name: &str, change: impl FnOnce(&mut Value)) -> TestResult {
        let path = self.directory.path().join(name);
        let mut value: Value = serde_json::from_slice(&fs::read(&path)?)?;
        change(&mut value);
        fs::write(path, format!("{value}\n"))?;
        Ok(())
    }
}

fn executable_path(directory: &Path, name: &str) -> PathBuf {
    directory.join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
}

fn write_mock_cli(path: &Path) -> TestResult {
    static BINARY: OnceLock<Vec<u8>> = OnceLock::new();
    let bytes = BINARY.get_or_init(|| {
        let directory = tempfile::tempdir().unwrap();
        let compiler = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        // A native mock preserves argument boundaries and binary IPC output on every platform.
        let output = Command::new(compiler)
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/unity_catalog_live/mock_cli.rs"))
            .args(["--edition=2024", "--deny=warnings", "-o"])
            .arg(executable_path(directory.path(), "mock_cli"))
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        fs::read(executable_path(directory.path(), "mock_cli")).unwrap()
    });
    fs::write(path, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
    }
    Ok(())
}

fn isolated_xtask(directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command.env_clear().env("PATH", directory);
    // Windows needs its system directory and a writable temporary directory after env_clear.
    if let Some(system_root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", system_root);
    }
    for variable in ["TMPDIR", "TMP", "TEMP"] {
        command.env(variable, directory);
    }
    command
}

fn write_arrow_file(path: &Path, batch: &RecordBatch) -> TestResult {
    let mut writer = FileWriter::try_new(fs::File::create(path)?, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(())
}

fn report(output: &Output) -> TestResult<Value> {
    for bytes in [&output.stdout, &output.stderr] {
        let text = String::from_utf8_lossy(bytes);
        assert!(!text.contains("secret"), "{text}");
        assert!(!text.contains("do-not-execute"), "{text}");
    }
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[test]
fn unconfigured_harness_skips_without_any_installed_cli() -> TestResult {
    let directory = tempfile::tempdir()?;
    let output = isolated_xtask(directory.path())
        .args(["cli", "test-unity-catalog"])
        .output()?;
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let result = report(&output)?;
    assert_eq!(result["status"], "skipped");
    assert!(
        result["checks"]
            .as_object()
            .unwrap()
            .values()
            .all(|status| status == "not_run")
    );
    assert_eq!(fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}

#[test]
fn configured_harness_pins_commands_and_reports_only_declared_public_context() -> TestResult {
    let fixture = OfflineFixture::new()?;
    let output = fixture.command().output()?;
    assert!(output.status.success(), "{output:?}");
    let result = report(&output)?;
    assert_eq!(result["status"], "passed");
    assert_eq!(result["scope"], "inspect_scan_and_sql");
    assert_eq!(
        result["checks"],
        json!({"inspect": "passed", "scan": "passed", "sql_aggregate": "passed", "sql_join": "passed"})
    );
    assert_eq!(result["dar_revision"], REVISION);
    assert_eq!(
        result["dar_sha256"],
        format!(
            "{:x}",
            Sha256::digest(fs::read(executable_path(fixture.directory.path(), "dar"))?)
        )
    );
    assert_eq!(result["databricks_cli_version"], "0.300.0");
    assert_eq!(result["cloud"], "aws");
    assert_eq!(result["table_version"], "7");
    assert_eq!(result["local_table_version"], "3");
    assert_eq!(
        result["declared_fixture_context"],
        fixture.config["context"]
    );
    assert!(result.get("step").is_none());
    for operation in ["inspect", "scan"] {
        let args = fs::read_to_string(fixture.directory.path().join(format!("{operation}.args")))?;
        let args: Vec<_> = args.lines().collect();
        assert_eq!(args[0], operation);
        assert_eq!(args[1], "uc://secret.catalog.table");
        assert!(args.contains(&"--diagnostics=json"));
        assert!(args.contains(&"--databricks-profile=secret profile $(do-not-execute)"));
        assert!(args.windows(2).any(|pair| pair == ["--table-version", "7"]));
        assert!(
            args.windows(2)
                .any(|pair| pair[0] == "--storage-options-file"
                    && Path::new(pair[1]).ends_with("storage.json"))
        );
        if operation == "scan" {
            assert!(args.windows(2).any(|pair| pair == ["--limit", "3"]));
            assert!(
                args.windows(2)
                    .any(|pair| pair == ["--column", "secret_value"])
            );
            assert!(args.windows(2).any(|pair| pair[0] == "--predicate-file"
                && Path::new(pair[1]).ends_with("predicate.json")));
        }
    }
    assert_eq!(
        fs::read_to_string(fixture.directory.path().join("databricks.args"))?,
        "--version\n"
    );
    Ok(())
}

#[test]
fn relative_binary_executes_the_hashed_file_instead_of_a_path_shadow() -> TestResult {
    let fixture = OfflineFixture::new()?;
    let shadow = tempfile::tempdir()?;
    fs::copy(
        executable_path(fixture.directory.path(), "databricks"),
        executable_path(shadow.path(), "databricks"),
    )?;
    fs::write(executable_path(shadow.path(), "dar"), b"not an executable")?;
    let output = isolated_xtask(shadow.path())
        .env("UC_FIXTURE_DIR", fixture.directory.path())
        .current_dir(fixture.directory.path())
        .args([
            "cli",
            "test-unity-catalog",
            "--fixture",
            "fixture.json",
            "--binary",
        ])
        .arg(executable_path(Path::new(""), "dar"))
        .args(["--dar-revision", REVISION])
        .output()?;
    assert!(output.status.success(), "{output:?}");
    let result = report(&output)?;
    assert_eq!(
        result["dar_sha256"],
        format!(
            "{:x}",
            Sha256::digest(fs::read(executable_path(fixture.directory.path(), "dar"))?)
        )
    );
    Ok(())
}

#[test]
fn invalid_private_inputs_fail_before_either_cli_runs() -> TestResult {
    for (pointer, value) in [
        ("/databricks_profile", json!("")),
        ("/table", json!("s3://secret/table")),
        ("/expected_table", json!("secret-missing.arrow")),
        ("/scan/columns", json!(["id", "id"])),
        ("/scan/predicate_file", json!("secret-missing.json")),
        ("/context/credential_form", json!("secret-token")),
        ("/context/auth_mode", json!("secret-token")),
        (
            "/context/protocol/reader_features",
            json!(["secret-feature"]),
        ),
    ] {
        let mut fixture = OfflineFixture::new()?;
        *fixture.config.pointer_mut(pointer).unwrap() = value;
        fixture.save_config()?;
        let output = fixture.command().output()?;
        assert!(!output.status.success(), "{pointer}");
        let result = report(&output)?;
        assert_eq!(result["step"], "configuration");
        for name in ["databricks.args", "inspect.args", "scan.args"] {
            assert!(!fixture.directory.path().join(name).exists(), "{pointer}");
        }
    }
    let fixture = OfflineFixture::new()?;
    fs::write(
        fixture.directory.path().join("fixture.json"),
        b"{\"secret-token\":",
    )?;
    let output = fixture.command().output()?;
    assert!(!output.status.success());
    assert_eq!(report(&output)?["step"], "configuration");
    Ok(())
}

#[test]
fn malformed_version_output_is_redacted_before_any_table_read() -> TestResult {
    let fixture = OfflineFixture::new()?;
    fs::write(
        fixture.directory.path().join("databricks.version"),
        "Databricks CLI v0.300.0 secret-token\n",
    )?;
    let output = fixture.command().output()?;
    assert!(!output.status.success());
    assert_eq!(report(&output)?["step"], "databricks_version");
    assert!(!fixture.directory.path().join("inspect.args").exists());
    Ok(())
}

#[test]
fn fixture_preflight_rejects_unknown_fields_large_files_and_invalid_options() -> TestResult {
    for failure in ["unknown_field", "too_large", "directory", "predicate"] {
        let mut fixture = OfflineFixture::new()?;
        match failure {
            "unknown_field" => {
                fixture.config["secret-field"] = json!("secret-token");
                fixture.save_config()?;
            }
            "too_large" => {
                fs::OpenOptions::new()
                    .append(true)
                    .open(fixture.directory.path().join("fixture.json"))?
                    .write_all(&vec![b' '; 16 * 1024 * 1024])?;
            }
            "directory" => {
                fixture.config["expected_table"] = json!(".");
                fixture.save_config()?;
            }
            "predicate" => fs::write(
                fixture.directory.path().join("predicate.json"),
                b"secret-token",
            )?,
            _ => unreachable!(),
        }
        let output = fixture.command().output()?;
        assert!(!output.status.success(), "{failure}");
        assert_eq!(report(&output)?["step"], "configuration");
        assert!(!fixture.directory.path().join("databricks.args").exists());
    }
    Ok(())
}

#[test]
fn inspection_rejects_wrong_snapshot_schema_and_completion() -> TestResult {
    for (file, pointer, value) in [
        ("inspect.stdout", "/table_version", json!("8")),
        (
            "inspect.stdout",
            "/schema/fields/0/name",
            json!("secret-wrong-column"),
        ),
        ("inspect.stdout", "/schema/fields/0/nullable", json!(false)),
        ("inspect.stderr", "/bindings/0/table_version", json!("8")),
        ("inspect.stderr", "/output_bytes", json!("1")),
        ("inspect.stderr", "/terminal", json!(false)),
    ] {
        let fixture = OfflineFixture::new()?;
        fixture.change_json(file, |value_json| {
            *value_json.pointer_mut(pointer).unwrap() = value
        })?;
        if file == "inspect.stdout" {
            let length = fs::metadata(fixture.directory.path().join(file))?.len();
            fixture.change_json("inspect.stderr", |value| {
                value["output_bytes"] = json!(length.to_string())
            })?;
        }
        let output = fixture.command().output()?;
        assert!(!output.status.success(), "{file} {pointer}");
        let result = report(&output)?;
        assert_eq!(result["step"], "inspect");
        assert_eq!(result["checks"]["inspect"], "failed");
        assert_eq!(result["checks"]["scan"], "not_run");
        assert!(!fixture.directory.path().join("scan.args").exists());
    }
    Ok(())
}

#[test]
fn scan_rejects_wrong_values_truncated_ipc_and_inconsistent_completion() -> TestResult {
    for failure in ["values", "extra_rows", "ipc", "completion", "diagnostic"] {
        let fixture = OfflineFixture::new()?;
        match failure {
            "values" => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "secret_value",
                    DataType::Int32,
                    true,
                )]));
                let batch =
                    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![10, 10]))])?;
                write_arrow_file(&fixture.directory.path().join("scan.arrow"), &batch)?;
            }
            "extra_rows" => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "secret_value",
                    DataType::Int32,
                    true,
                )]));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
                )?;
                let mut writer = StreamWriter::try_new(Vec::new(), &schema)?;
                writer.write(&batch)?;
                writer.finish()?;
                let bytes = writer.into_inner()?;
                fixture.change_json("scan.stderr", |value| {
                    value["rows"] = json!("3");
                    value["batches"] = json!("1");
                    value["output_bytes"] = json!(bytes.len().to_string());
                    value["stop_reason"] = json!("row_limit");
                    value["has_more"] = Value::Null;
                })?;
                fs::write(fixture.directory.path().join("scan.stdout"), bytes)?;
            }
            "ipc" => {
                let path = fixture.directory.path().join("scan.stdout");
                let length = fs::metadata(&path)?.len();
                fs::OpenOptions::new()
                    .write(true)
                    .open(path)?
                    .set_len(length - 8)?;
                fixture.change_json("scan.stderr", |value| {
                    value["output_bytes"] = json!((length - 8).to_string())
                })?;
            }
            "completion" => {
                fixture.change_json("scan.stderr", |value| value["rows"] = json!("1"))?
            }
            "diagnostic" => fs::write(
                fixture.directory.path().join("scan.stderr"),
                "secret-token\n",
            )?,
            _ => unreachable!(),
        }
        let output = fixture.command().output()?;
        assert!(!output.status.success(), "{failure}");
        let result = report(&output)?;
        assert_eq!(result["step"], "scan");
        assert_eq!(result["checks"]["inspect"], "passed");
        assert_eq!(result["checks"]["scan"], "failed");
    }
    let fixture = OfflineFixture::new()?;
    let output = fixture.command().env("UC_FIXTURE_EXIT", "1").output()?;
    assert!(!output.status.success());
    assert_eq!(report(&output)?["step"], "inspect");
    Ok(())
}
