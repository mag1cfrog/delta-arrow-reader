//! Command declarations and command-line value parsing.

#[cfg(test)]
mod tests;

use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    str::FromStr,
};

use clap::{Parser, Subcommand, error::ErrorKind};
use snafu::ensure;

use crate::{
    capabilities,
    diagnostics::{self, ArgumentSnafu, Error},
    query,
    read::{RowReadArgs, inspect, preview},
    table_access::TableArgs,
};

/// Read Delta Lake snapshots as JSON or Arrow IPC streams.
#[derive(Parser)]
#[command(
    name = command_name(), bin_name = command_name(), version,
    after_help = "When version or column types are unknown, use inspect TABLE --view summary [--column NAME]; pin reads with --table-version N.\nGuides: capabilities --command NAME; capabilities --schema NAME [--detail FIELD_OR_TYPE].\nInput formats: predicate, storage-options, execution-options, tables. Output formats: rows, inspection-summary, inspection, diagnostic, completion."
)]
pub(crate) struct Cli {
    #[command(flatten)]
    diagnostics: diagnostics::DiagnosticOptions,
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
    /// List installed commands, with details and JSON schemas available on request.
    ///
    /// Offline; no table, input file, or authorization is loaded.
    Capabilities(capabilities::CapabilitiesArgs),
    /// Inspect a snapshot's version, read checks, or selected column types as JSON.
    ///
    /// Only snapshot metadata is loaded; data files are not read.
    Inspect {
        #[command(flatten)]
        table: TableArgs,
        /// Existing Databricks CLI profile; required for uc://catalog.schema.table targets.
        #[arg(long, value_name = "PROFILE")]
        databricks_profile: Option<String>,
        #[command(flatten)]
        inspection: inspect::InspectionArgs,
        /// Whole-command deadline in milliseconds. Summary defaults to 30000; full has no default.
        #[arg(long, value_name = "N", default_value_if("view", "summary", "30000"), value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
        deadline_ms: Option<NonZeroUsize>,
    },
    /// Read a small typed JSON sample with row, byte, and time limits.
    Preview(preview::PreviewArgs),
    /// Run one read-only SQL query over explicitly selected Delta snapshots.
    Query(query::QueryArgs),
    /// Plan a read-only SQL query without executing result rows.
    Explain(query::explain::ExplainArgs),
    /// Export a Delta snapshot's rows as Arrow IPC.
    ///
    /// Use --output PATH to save a complete file, or redirect stdout.
    Scan {
        #[command(flatten)]
        read: RowReadArgs,
        /// Maximum output rows as unsigned decimal digits; zero is valid.
        #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
        limit: Option<usize>,
        /// Save one complete result without replacing an existing file.
        /// Stdout returns a small JSON receipt. Windows staging inherits the directory ACL.
        #[arg(long, value_name = "PATH")]
        output: Option<PathBuf>,
        /// Also save PATH.manifest.json with the sources and options needed for replay.
        ///
        /// Replay details: capabilities --schema result-manifest --detail replay.
        #[arg(long, requires = "output")]
        manifest: bool,
        /// File result row ceiling; defaults to 1000000. Exceeding it fails, unlike --limit.
        #[arg(long, requires = "output", default_value_if("output", clap::builder::ArgPredicate::IsPresent, "1000000"), value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
        max_rows: Option<usize>,
        /// File result byte ceiling, including framing; defaults to 1 GiB.
        #[arg(long, requires = "output", default_value_if("output", clap::builder::ArgPredicate::IsPresent, "1073741824"), value_name = "BYTES", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
        max_output_bytes: Option<NonZeroUsize>,
        /// Whole-command deadline in milliseconds; file output defaults to 300000, stdout has no default.
        #[arg(long, default_value_if("output", clap::builder::ArgPredicate::IsPresent, "300000"), value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
        deadline_ms: Option<NonZeroUsize>,
    },
}

pub(crate) fn command_name() -> &'static str {
    let executable = std::env::args_os().next().unwrap_or_default();
    match Path::new(&executable).file_name() {
        Some(name) if name == "delta-arrow-reader" || name == "delta-arrow-reader.exe" => {
            "delta-arrow-reader"
        }
        _ => "dar",
    }
}

pub(crate) fn parse_unsigned_decimal<T: FromStr>(value: &str) -> Result<T, Error> {
    ensure!(
        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
        ArgumentSnafu {
            kind: ErrorKind::ValueValidation,
            correction: None,
        }
    );
    value.parse().map_err(|_| {
        ArgumentSnafu {
            kind: ErrorKind::ValueValidation,
            correction: None,
        }
        .build()
    })
}
