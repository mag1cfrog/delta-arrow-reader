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
mod query;
mod read;
mod runtime;
mod table_access;

use std::process::ExitCode;

use clap::{Parser, error::ErrorKind};
use cli::{Cli, Command};
#[cfg(target_os = "linux")]
use snafu::ResultExt;

fn main() -> ExitCode {
    let diagnostic_format = diagnostics::DiagnosticFormat::from_cli_args();
    runtime::install_panic_hook(diagnostic_format);
    #[cfg(target_os = "linux")]
    if let Err(error) =
        runtime::install_container_signal_handlers().context(diagnostics::RuntimeSnafu {
            reason: "signal_handler_install_failed",
        })
    {
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
            diagnostic_format.report_failure_with_timeout(diagnostics::Error::Argument {
                kind: error.kind(),
                correction: diagnostics::InputCorrection::from_clap(&error),
            })
        }
    }
}
