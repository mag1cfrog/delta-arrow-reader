//! Opt-in direct-read checks using an existing Databricks CLI profile.

mod fixture;
mod inspection;
mod sql;

use std::{
    fs,
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

use clap::Args;
use serde_json::{Value, json};
use snafu::{ResultExt, ensure};

use crate::{
    artifacts::file_sha256,
    command::output_with_timeout,
    error::{FileSnafu, InvalidInputSnafu, JsonSnafu, Result},
    scan::ipc::{IpcTable, check_completed_scan},
};
use fixture::LiveFixtureConfig;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(120);

/// Explicit opt-in and binary identity for a single prepared fixture.
#[derive(Args)]
pub(crate) struct TestUnityCatalogArgs {
    /// Private JSON configuration; omit to skip without running either CLI.
    #[arg(long, requires_all = ["binary", "dar_revision"])]
    fixture: Option<PathBuf>,
    /// Extracted dar or installed delta-arrow-reader executable.
    #[arg(long, requires = "fixture")]
    binary: Option<PathBuf>,
    /// Full Git commit used to build the executable, from its build provenance.
    #[arg(long, requires = "fixture")]
    dar_revision: Option<String>,
}

impl TestUnityCatalogArgs {
    pub(crate) fn run(self) -> Result<()> {
        let mut report = json!({
            "format_version": 1, "scope": "inspect_scan_and_sql", "status": "skipped",
            "checks": {"inspect": "not_run", "scan": "not_run", "sql_aggregate": "not_run", "sql_join": "not_run"},
        });
        let Some(path) = &self.fixture else {
            println!("{report}");
            return Ok(());
        };
        report["step"] = json!("configuration");
        let result = self.run_fixture_checks(path, &mut report);
        report["status"] = json!(if result.is_ok() { "passed" } else { "failed" });
        if result.is_ok()
            && let Some(fields) = report.as_object_mut()
        {
            fields.remove("step");
        }
        println!("{report}");
        // File paths, parser errors, schemas, and subprocess diagnostics can contain private data.
        result.map_err(|_| {
            InvalidInputSnafu {
                message: "Unity Catalog validation failed; see the report's step field",
            }
            .build()
        })
    }

    fn run_fixture_checks(&self, path: &std::path::Path, report: &mut Value) -> Result<()> {
        let config = LiveFixtureConfig::load(path)?;
        let revision = self.dar_revision.as_deref().unwrap_or_default();
        ensure!(
            revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit()),
            InvalidInputSnafu {
                message: "DAR revision must be a full Git commit"
            }
        );
        let binary = self.binary.as_ref().ok_or_else(|| {
            InvalidInputSnafu {
                message: "DAR executable is required",
            }
            .build()
        })?;
        let binary = fs::canonicalize(binary).context(FileSnafu { path: binary })?;
        report["dar_revision"] = json!(revision.to_ascii_lowercase());
        ensure!(
            fs::metadata(&binary)
                .context(FileSnafu { path: &binary })?
                .is_file(),
            InvalidInputSnafu {
                message: "DAR executable must be a regular file"
            }
        );
        report["dar_sha256"] = json!(file_sha256(&binary)?);
        report["cloud"] = json!(config.context.credential_form.cloud());
        report["declared_fixture_context"] = json!(config.context);
        report["table_version"] = json!(config.table_version.to_string());
        report["local_table_version"] = json!(config.sql.local_table_version.to_string());

        let (expected_table, expected_scan) = load_expected_outputs(&config)?;
        let inputs = capture_options(&config)?;
        let sql_checks = sql::prepare_checks(&config, inputs.path())?;
        report["step"] = json!("databricks_version");
        report["databricks_cli_version"] = json!(databricks_cli_version()?);
        let table_command = |operation: &str| {
            let mut command = Command::new(&binary);
            command
                .args([
                    operation,
                    &config.table,
                    "--diagnostics=json",
                    "--table-version",
                    &config.table_version.to_string(),
                ])
                .arg(format!(
                    "--databricks-profile={}",
                    config.databricks_profile
                ))
                .env_remove("RUST_LOG")
                .stdin(Stdio::null());
            if config.storage_options_file.is_some() {
                command
                    .arg("--storage-options-file")
                    .arg(inputs.path().join("storage.json"));
            }
            command
        };
        report["step"] = json!("inspect");
        report["checks"]["inspect"] = json!("failed");
        let inspected = output_with_timeout(&mut table_command("inspect"), COMMAND_TIMEOUT)?;
        inspection::check_inspection(&inspected, config.table_version, &expected_table)?;
        report["checks"]["inspect"] = json!("passed");
        report["step"] = json!("scan");
        report["checks"]["scan"] = json!("failed");
        let mut scan = table_command("scan");
        // One extra row detects an overly broad predicate without materializing the whole table.
        let row_limit = expected_scan.row_count() + 1;
        scan.arg("--predicate-file")
            .arg(inputs.path().join("predicate.json"))
            .args(["--limit", &row_limit.to_string()]);
        for column in &config.scan.columns {
            scan.arg("--column").arg(column);
        }
        let output = output_with_timeout(&mut scan, COMMAND_TIMEOUT)?;
        check_completed_scan(&output, config.table_version, Some(row_limit))?
            .check_matches(&expected_scan, None)?;
        report["checks"]["scan"] = json!("passed");
        for check in sql_checks {
            report["step"] = json!(check.report_key);
            report["checks"][check.report_key] = json!("failed");
            check.run(&binary)?;
            report["checks"][check.report_key] = json!("passed");
        }
        Ok(())
    }
}

fn databricks_cli_version() -> Result<String> {
    let output = output_with_timeout(
        Command::new("databricks")
            .arg("--version")
            .stdin(Stdio::null()),
        Duration::from_secs(30),
    )?;
    let version = std::str::from_utf8(&output.stdout)
        .unwrap_or_default()
        .trim();
    let version = version.strip_prefix("Databricks CLI v").unwrap_or_default();
    ensure!(
        output.status.success()
            && output.stderr.is_empty()
            && version.len() <= 32
            && version.split('.').count() == 3
            && version
                .split('.')
                .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())),
        InvalidInputSnafu {
            message: "Could not identify the Databricks CLI release"
        }
    );
    Ok(version.to_owned())
}

fn load_expected_outputs(config: &LiveFixtureConfig) -> Result<(IpcTable, IpcTable)> {
    fixture::check_input_size(&config.expected_table)?;
    fixture::check_input_size(&config.scan.expected_result)?;
    let expected_table = IpcTable::read_file(&config.expected_table)?;
    let expected_scan = IpcTable::read_file(&config.scan.expected_result)?;
    ensure!(
        (1..=1000).contains(&expected_table.row_count())
            && expected_scan.row_count() > 0
            && expected_scan.row_count() < expected_table.row_count()
            && config.scan.columns.len() < expected_table.schema.fields().len()
            && config
                .scan
                .columns
                .iter()
                .map(String::as_str)
                .eq(expected_scan
                    .schema
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())),
        InvalidInputSnafu {
            message: "Use a small nonempty fixture with a filtered projection and matching reference schema"
        }
    );
    let indices = config
        .scan
        .columns
        .iter()
        .map(|name| expected_table.schema.index_of(name))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| {
            InvalidInputSnafu {
                message: "Projected column is absent from the fixture",
            }
            .build()
        })?;
    expected_table
        .project(&indices, &config.expected_table)?
        .check_schema_matches(&expected_scan.schema)?;

    Ok((expected_table, expected_scan))
}

fn capture_options(config: &LiveFixtureConfig) -> Result<tempfile::TempDir> {
    // Capture both option files once so every check uses the same fixture inputs.
    let inputs = tempfile::tempdir().context(FileSnafu {
        path: std::env::temp_dir(),
    })?;
    for (source, name) in [
        (Some(&config.scan.predicate_file), "predicate.json"),
        (config.storage_options_file.as_ref(), "storage.json"),
    ] {
        if let Some(source) = source {
            fixture::check_input_size(source)?;
            let bytes = fs::read(source).context(FileSnafu { path: source })?;
            let value: Value = serde_json::from_slice(&bytes).context(JsonSnafu {
                subject: "fixture options",
            })?;
            ensure!(
                value.is_object(),
                InvalidInputSnafu {
                    message: "Fixture options must be JSON objects"
                }
            );
            let destination = inputs.path().join(name);
            fs::write(&destination, bytes).context(FileSnafu { path: &destination })?;
        }
    }
    Ok(inputs)
}
