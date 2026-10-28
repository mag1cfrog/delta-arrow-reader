//! Single-table inspection, scans, and bounded previews.

pub(crate) mod inspect;
pub(crate) mod preview;
mod rows;

use std::{
    io::{self, IsTerminal},
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, Ordering},
};

use clap::Args;
use futures_util::future::AbortHandle;
use serde_json::Value;
use snafu::{ResultExt, ensure};

use crate::{
    cli::{Command, parse_unsigned_decimal},
    diagnostics::{Error, TerminalOutputSnafu, UnityCatalogSnafu},
    output::artifact::PendingArtifactPublication,
    table_access::{TableArgs, unity_catalog::UnityCatalogTableTarget},
};

/// Snapshot, projection, predicate, and execution choices shared by scan and preview.
#[derive(Args, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct RowReadArgs {
    #[command(flatten)]
    #[serde(flatten)]
    pub(crate) table: TableArgs,
    /// Existing Databricks CLI profile; required for uc://catalog.schema.table targets.
    #[arg(long, value_name = "PROFILE")]
    pub(crate) databricks_profile: Option<String>,
    /// Logical column name; repeat to select columns in argument order. Default: all columns.
    #[arg(long = "column", value_name = "NAME")]
    #[serde(rename = "column")]
    pub(crate) columns: Vec<String>,
    /// Read zero columns while preserving row counts.
    #[arg(long, conflicts_with = "columns")]
    pub(crate) no_columns: bool,
    /// Positive execution partition target; default: native automatic selection.
    #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) target_partitions: Option<NonZeroUsize>,
    /// Local typed JSON predicate; at most 1 MiB. '-' is a filename, not stdin.
    /// Match literal types to inspected column types; integer values are strings.
    /// Format: capabilities --schema predicate.
    #[arg(long, value_name = "PATH")]
    pub(crate) predicate_file: Option<String>,
    /// Local JSON execution options; at most 1 MiB. '-' is a filename, not stdin.
    #[arg(long, value_name = "PATH")]
    pub(crate) execution_options_file: Option<String>,
}

pub(crate) async fn execute(
    mut command: Command,
    report_uc_cancellation: &AtomicBool,
    pending_artifact: &mut Option<PendingArtifactPublication>,
    cancellation: &AbortHandle,
) -> Result<Value, Error> {
    if let Command::Scan {
        read,
        output: Some(_),
        ..
    } = &mut command
    {
        read.table
            .options
            .select_file_output_credentials(&read.table.table)?;
    }
    if matches!(&command, Command::Scan { output: None, .. }) {
        ensure!(!io::stdout().is_terminal(), TerminalOutputSnafu);
    }
    if let Command::Inspect { inspection, .. } = &command {
        inspection.validate_view_options()?;
    }
    let uc_target = match &command {
        Command::Inspect {
            table,
            databricks_profile,
            ..
        } => UnityCatalogTableTarget::parse(&table.table, databricks_profile.as_deref())
            .context(UnityCatalogSnafu)?,
        Command::Scan { read, .. } | Command::Preview(preview::PreviewArgs { read, .. }) => {
            UnityCatalogTableTarget::parse(&read.table.table, read.databricks_profile.as_deref())
                .context(UnityCatalogSnafu)?
        }
        _ => unreachable!("single-table read command"),
    };
    report_uc_cancellation.store(
        uc_target.is_some() && !matches!(command, Command::Preview(_)),
        Ordering::Relaxed,
    );
    match &command {
        Command::Inspect {
            table, inspection, ..
        } => inspection.execute(table, uc_target).await,
        Command::Scan { .. } | Command::Preview(_) => {
            rows::execute(&command, uc_target, pending_artifact, cancellation).await
        }
        _ => unreachable!("single-table read command"),
    }
}
