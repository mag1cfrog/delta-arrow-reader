//! Local SQL execution over pinned UC and local Delta snapshots.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use serde_json::json;
use snafu::{ResultExt, ensure};

use crate::{
    command::output_with_timeout,
    error::{FileSnafu, InvalidInputSnafu, Result},
    scan::ipc::{IpcTable, check_completed_query},
};

use super::{
    COMMAND_TIMEOUT,
    fixture::{LiveFixtureConfig, check_input_size},
};

/// Captured query inputs and the independent output to check after execution.
pub(super) struct PreparedSqlCheck {
    pub report_key: &'static str,
    sql_file: PathBuf,
    tables_file: PathBuf,
    expected_bindings: Vec<(&'static str, u64)>,
    expected_result: IpcTable,
}

impl PreparedSqlCheck {
    pub(super) fn run(&self, binary: &Path) -> Result<()> {
        let output = output_with_timeout(
            Command::new(binary)
                .args(["query", "--diagnostics=json", "--tables-file"])
                .arg(&self.tables_file)
                .arg("--sql-file")
                .arg(&self.sql_file)
                .args([
                    "--max-rows",
                    &self.expected_result.row_count().to_string(),
                    "--max-output-bytes",
                    "16777216",
                ])
                .env_remove("RUST_LOG")
                .stdin(Stdio::null()),
            COMMAND_TIMEOUT,
        )?;
        check_completed_query(&output, &self.expected_bindings)?
            .check_matches(&self.expected_result, None)
    }
}

pub(super) fn prepare_checks(
    config: &LiveFixtureConfig,
    directory: &Path,
) -> Result<Vec<PreparedSqlCheck>> {
    ensure!(
        config.sql.local_table.is_dir() && config.sql.local_table.join("_delta_log").is_dir(),
        InvalidInputSnafu {
            message: "SQL join requires a local Delta table directory"
        }
    );
    let mut uc_binding = json!({
        "alias": "uc_rows", "location": config.table,
        "table_version": config.table_version.to_string(),
        "authorization": {"source": "databricks", "databricks_profile": config.databricks_profile},
    });
    if config.storage_options_file.is_some() {
        uc_binding["authorization"]["storage_options_file"] = json!(directory.join("storage.json"));
    }
    let mut checks = Vec::new();
    for (report_key, expectation, includes_local_table) in [
        ("sql_aggregate", &config.sql.aggregate, false),
        ("sql_join", &config.sql.join, true),
    ] {
        check_input_size(&expectation.sql_file)?;
        let sql = fs::read_to_string(&expectation.sql_file).context(FileSnafu {
            path: &expectation.sql_file,
        })?;
        ensure!(
            !sql.trim().is_empty() && sql.len() <= 64 * 1024,
            InvalidInputSnafu {
                message: "SQL fixture must contain a query of at most 64 KiB"
            }
        );
        check_input_size(&expectation.expected_result)?;
        let expected_result = IpcTable::read_file(&expectation.expected_result)?;
        ensure!(
            (1..=1000).contains(&expected_result.row_count())
                && !expected_result.schema.fields().is_empty(),
            InvalidInputSnafu {
                message: "SQL reference must contain columns and 1-1000 result rows"
            }
        );
        let mut tables = vec![uc_binding.clone()];
        let mut expected_bindings = vec![("uc_rows", config.table_version)];
        if includes_local_table {
            tables.push(json!({
                "alias": "local_rows", "location": config.sql.local_table,
                "table_version": config.sql.local_table_version.to_string(),
            }));
            expected_bindings.push(("local_rows", config.sql.local_table_version));
        }
        let sql_file = directory.join(format!("{report_key}.sql"));
        let tables_file = directory.join(format!("{report_key}.tables.json"));
        fs::write(&sql_file, sql).context(FileSnafu { path: &sql_file })?;
        fs::write(
            &tables_file,
            json!({"format_version": 1, "tables": tables}).to_string(),
        )
        .context(FileSnafu { path: &tables_file })?;
        checks.push(PreparedSqlCheck {
            report_key,
            sql_file,
            tables_file,
            expected_bindings,
            expected_result,
        });
    }
    Ok(checks)
}
