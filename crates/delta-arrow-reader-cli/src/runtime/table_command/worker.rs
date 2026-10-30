//! Execute a table command, clean temporary resources, then publish any completed result.

use std::{
    io::{self, IsTerminal},
    sync::atomic::{AtomicBool, Ordering},
};

use futures_util::future::{AbortHandle, AbortRegistration, Abortable};
use serde_json::Value;
use snafu::{ResultExt, ensure};

use super::CLEANUP_GRACE;
use crate::{
    cli::Command,
    diagnostics::{self, DiagnosticFormat, Error, RuntimeSnafu, TerminalOutputSnafu},
    output, read,
};

pub(super) fn run(
    command: Command,
    diagnostic_format: DiagnosticFormat,
    report_uc_cancellation: &AtomicBool,
    publication_started: &AtomicBool,
    cancellation: &AbortHandle,
    registration: AbortRegistration,
) -> Result<Value, Error> {
    let mut spill_directory = None;
    let mut pending_artifact = None;
    let result = (|| {
        diagnostics::init_tracing(diagnostic_format);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context(RuntimeSnafu {
                reason: "runtime_build_failed",
            })?;
        let result = runtime.block_on(async {
            let result = Abortable::new(
                execute_table_command(
                    command,
                    report_uc_cancellation,
                    &mut spill_directory,
                    &mut pending_artifact,
                    cancellation,
                ),
                registration,
            )
            .await;
            // A read/output error can finish the command while a renewal is still pending.
            let _ = tokio::time::timeout(
                CLEANUP_GRACE,
                crate::table_access::unity_catalog::cancel_pending_requests(),
            )
            .await;
            result
        });
        runtime.shutdown_background();
        result.map_err(|_| Error::Cancelled { status: 1 })?
    })();
    let result = if let Some(directory) = spill_directory {
        match crate::query::resources::remove_spill_files(directory) {
            Ok(()) => result,
            Err(cleanup) => Err(match result {
                Err(source) => Error::WithCleanupFailure {
                    source: Box::new(source),
                    cleanup: Box::new(cleanup),
                },
                Ok(completion) => match completion["bindings"].as_array().cloned() {
                    Some(bindings) => Error::WithResolvedBindings {
                        source: Box::new(cleanup),
                        bindings,
                    },
                    None => cleanup,
                },
            }),
        }
    } else {
        result
    };
    result.and_then(|completion| {
        if let Some(artifact) = pending_artifact {
            (|| {
                publication_started.store(true, Ordering::Release);
                let receipt = artifact.publish(cancellation)?;
                crate::output::write_stdout(&receipt).map_err(|error| match error {
                    Error::Output { source } => Error::ArtifactIo {
                        source,
                        reason: "artifact_receipt_write_failed",
                    },
                    error => error,
                })?;
                Ok(serde_json::json!({
                    "format_version": 3, "kind": "completion", "command": completion["command"],
                    "output_bytes": receipt.len().to_string(),
                }))
            })()
            .map_err(|source| Error::WithResolvedBindings {
                source: Box::new(source),
                bindings: completion["bindings"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default(),
            })
        } else {
            Ok(completion)
        }
    })
}

async fn execute_table_command(
    command: Command,
    report_uc_cancellation: &AtomicBool,
    spill_directory: &mut Option<tempfile::TempDir>,
    pending_artifact: &mut Option<output::artifact::PendingArtifactPublication>,
    cancellation: &AbortHandle,
) -> Result<Value, Error> {
    match command {
        Command::Query(mut args) => {
            if args.output.is_some()
                && let Some(location) = &args.input.table
            {
                args.input
                    .table_options
                    .select_file_output_credentials(location)?;
            }
            if args.output.is_none() && args.output_format == output::RowOutputFormat::Arrow {
                ensure!(!io::stdout().is_terminal(), TerminalOutputSnafu);
            }
            let (uses_unity_catalog, execution) =
                args.prepare_execution(spill_directory, pending_artifact, cancellation)?;
            report_uc_cancellation.store(uses_unity_catalog, Ordering::Relaxed);
            execution.await
        }
        Command::Explain(args) => {
            let (uses_unity_catalog, planning) = args.prepare_planning()?;
            report_uc_cancellation.store(uses_unity_catalog, Ordering::Relaxed);
            planning.await
        }
        command @ (Command::Inspect { .. } | Command::Scan { .. } | Command::Preview(_)) => {
            read::execute(
                command,
                report_uc_cancellation,
                pending_artifact,
                cancellation,
            )
            .await
        }
        Command::Capabilities(_) => {
            unreachable!("main handles discovery and its terminal reporting separately")
        }
    }
}
