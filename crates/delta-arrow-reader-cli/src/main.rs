//! CLI commands for inspecting Delta snapshots and exporting their rows.
//!
//! `inspect` returns a snapshot's version, read checks, or column types as JSON.
//! `scan` and `query` return rows as Arrow IPC streams; `query` also supports typed
//! JSON, and `preview` returns a bounded JSON sample. Results use stdout;
//! diagnostics and completion records use stderr.
//! `capabilities` describes the installed commands without opening a table.
//!
//! For a Delta table at `./table` with an `id` column:
//!
//! ```sh
//! dar inspect ./table --view summary --column id
//! dar scan ./table --column id --limit 10 > rows.arrow
//! ```
//!
//! The first command reports the version and `id` type. The second saves an Arrow IPC
//! stream containing the `id` column from at most ten rows.

#![cfg_attr(test, allow(clippy::panic))]

mod capabilities;
mod cli;
mod diagnostics;
mod input;
mod output;
mod output_capacity;
mod query;
mod read;
mod runtime;
mod table_access;

use std::{
    io::{self, Write},
    process::ExitCode,
};

use clap::{Parser, error::ErrorKind};
use cli::{Cli, Command};
use diagnostics::{
    ArtifactIoSnafu, ConfigurationSnafu, DiagnosticFormat, Error, InputCorrection, InputIoSnafu,
    InputValidationSnafu, OutputSnafu, QuerySnafu, ReaderSnafu, RuntimeSnafu,
    StorageAuthorizationSnafu, TerminalOutputSnafu, UnityCatalogSnafu,
};
use output::OutputStats;
use serde_json::{Value, json};
use snafu::ResultExt;

fn build_completion_record(
    command: &'static str,
    table_version: Option<u64>,
    output_stats: OutputStats,
    row_limit: Option<usize>,
) -> Value {
    let limit_reached = row_limit.is_some_and(|limit| output_stats.rows_written >= limit as u64);
    let mut record = json!({
        "format_version": 1,
        "kind": "completion",
        "terminal": true,
        "command": command,
        "completion": "complete",
        "scope": if row_limit.is_some() { "limited_scan" } else { command },
        "stop_reason": if limit_reached { "row_limit" } else { "end_of_input" },
        "has_more": if limit_reached { None } else { Some(false) },
        "bindings": table_version.map(|version| json!({"alias": "t", "table_version": version.to_string()})).into_iter().collect::<Vec<_>>(),
        "output_bytes": output_stats.bytes_written.to_string(),
    });
    if matches!(command, "scan" | "query") {
        record["rows"] = output_stats.rows_written.to_string().into();
        record["batches"] = output_stats.batches_written.to_string().into();
    }
    record
}

fn build_json_read_completion(
    command: &'static str,
    bindings: Vec<Value>,
    output_stats: OutputStats,
) -> Value {
    json!({
        "format_version": 2,
        "kind": "completion",
        "command": command,
        "bindings": bindings,
        "rows": output_stats.rows_written.to_string(),
        "output_bytes": output_stats.bytes_written.to_string(),
    })
}

fn write_stdout(output: &[u8]) -> Result<(), Error> {
    let mut stdout = io::stdout().lock();
    stdout
        .write_all(output)
        .and_then(|()| stdout.flush())
        .context(OutputSnafu)
}

fn write_help_or_version(error: clap::Error) -> Result<Value, Error> {
    let output = error.to_string();
    write_stdout(output.as_bytes())?;
    Ok(build_completion_record(
        if error.kind() == ErrorKind::DisplayHelp {
            "help"
        } else {
            "version"
        },
        None,
        OutputStats {
            bytes_written: output.len() as u64,
            ..Default::default()
        },
        None,
    ))
}

fn main() -> ExitCode {
    let diagnostic_format = DiagnosticFormat::from_cli_args();
    runtime::install_panic_hook(diagnostic_format);
    #[cfg(target_os = "linux")]
    if let Err(error) = runtime::install_container_signal_handlers().context(RuntimeSnafu {
        reason: "signal_handler_install_failed",
    }) {
        return diagnostic_format.report_failure_with_timeout(error);
    }
    match Cli::try_parse() {
        Ok(Cli {
            command: Command::Capabilities(options),
            ..
        }) => capabilities::run_capabilities(options, diagnostic_format),
        Ok(cli) => runtime::run_table_command(cli.command, diagnostic_format),
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            capabilities::run_help_or_version(error, diagnostic_format)
        }
        Err(error) => {
            // Clap's diagnostics may contain argument values; report only safe fields.
            diagnostic_format.report_failure_with_timeout(Error::Argument {
                kind: error.kind(),
                correction: InputCorrection::from_clap(&error),
            })
        }
    }
}
