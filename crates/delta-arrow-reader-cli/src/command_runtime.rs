//! Table commands remain interruptible while inputs, readers, or output are blocked.
//!
//! The command runs on a separate thread so synchronous credential-file reads cannot
//! stop the deadline or signal listener. Completion is published only after that
//! worker finishes; cancellation first gives owned credential children time to exit.

use std::{
    io,
    process::ExitCode,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::future::{AbortHandle, Abortable};
use snafu::ResultExt;

use crate::{
    Command, Error, OutputSnafu, RuntimeSnafu, diagnostics, diagnostics::DiagnosticFormat,
    execute_table_command, signals, stdout_monitor,
};

pub(crate) const CLEANUP_GRACE: Duration = Duration::from_secs(1);

pub(crate) fn run_table_command(command: Command, diagnostic_format: DiagnosticFormat) -> ExitCode {
    let started = Instant::now();
    let manifest_requested = match &command {
        Command::Query(args) => args.manifest,
        Command::Scan { manifest, .. } => *manifest,
        _ => false,
    };
    let deadline_ms = match &command {
        Command::Inspect { deadline_ms, .. } | Command::Scan { deadline_ms, .. } => *deadline_ms,
        Command::Preview(args) => Some(args.deadline_ms),
        Command::Query(args) => Some(args.deadline_ms),
        Command::Explain(args) => Some(args.deadline_ms),
        Command::Capabilities(_) => unreachable!("discovery has its own fixed deadline"),
    };
    let (spawn_failure, lost_result) = if matches!(command, Command::Preview(_)) {
        ("preview_thread_spawn_failed", "preview_thread_result_lost")
    } else {
        ("command_thread_spawn_failed", "command_thread_result_lost")
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context(RuntimeSnafu {
            reason: "runtime_build_failed",
        }) {
        Ok(runtime) => runtime,
        Err(error) => return diagnostic_format.report_failure_with_timeout(error),
    };
    let result = runtime.block_on(async {
        let (abort, registration) = AbortHandle::new_pair();
        let worker_cancellation = abort.clone();
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        let report_uc_cancellation = Arc::new(AtomicBool::new(false));
        let worker_report_uc_cancellation = Arc::clone(&report_uc_cancellation);
        let publication_started = Arc::new(AtomicBool::new(false));
        let worker_publication_started = Arc::clone(&publication_started);
        let cancellation = signals::wait_for_cancellation();
        let deadline = async {
            match deadline_ms {
                Some(milliseconds) => tokio::time::sleep(
                    Duration::from_millis(milliseconds.get() as u64).saturating_sub(started.elapsed())
                ).await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(cancellation, deadline);
        let cancelled = |status| {
            if report_uc_cancellation.load(Ordering::Relaxed) {
                Error::UnityCatalog { source: crate::table_access::unity_catalog::Error::Cancelled { status } }
            } else {
                Error::Cancelled { status }
            }
        };
        let mut worker_finished = false;
        let result = tokio::select! {
            biased;
            status = &mut cancellation => Err(cancelled(status?)),
            () = &mut deadline => Err(Error::DeadlineExceeded),
            result = async {
                // This branch starts only after the signal listener has been polled.
                std::thread::Builder::new().name("dar-command".into()).spawn(move || {
                    let mut spill_directory = None;
                    let mut pending_artifact = None;
                    let result = (|| {
                        diagnostics::init_tracing(diagnostic_format);
                        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()
                            .context(RuntimeSnafu { reason: "runtime_build_failed" })?;
                        let result = runtime.block_on(async {
                            let result = Abortable::new(
                                execute_table_command(command, &worker_report_uc_cancellation, &mut spill_directory, &mut pending_artifact, &worker_cancellation), registration,
                            ).await;
                            // A read/output error can finish the command while a renewal is still pending.
                            let _ = tokio::time::timeout(CLEANUP_GRACE, crate::table_access::unity_catalog::cancel_pending_requests()).await;
                            result
                        });
                        runtime.shutdown_background();
                        result.map_err(|_| Error::Cancelled { status: 1 })?
                    })();
                    let result = if let Some(directory) = spill_directory {
                        match crate::query::resources::remove_spill_files(directory) {
                            Ok(()) => result,
                            Err(source) => {
                                let bindings = match result {
                                    Ok(completion) => completion["bindings"].as_array().cloned(),
                                    Err(Error::WithResolvedBindings { bindings, .. }) => Some(bindings),
                                    _ => None,
                                };
                                Err(match bindings {
                                    Some(bindings) => Error::WithResolvedBindings { source: Box::new(source), bindings },
                                    None => source,
                                })
                            }
                        }
                    } else { result };
                    let result = result.and_then(|completion| {
                        if let Some(artifact) = pending_artifact {
                            (|| {
                                worker_publication_started.store(true, Ordering::Release);
                                let receipt = artifact.publish(&worker_cancellation)?;
                                crate::write_stdout(&receipt).map_err(|error| match error {
                                    Error::Output { source } => Error::ArtifactIo { source, reason: "artifact_receipt_write_failed" },
                                    error => error,
                                })?;
                                Ok(serde_json::json!({
                                    "format_version": 3, "kind": "completion", "command": completion["command"],
                                    "output_bytes": receipt.len().to_string(),
                                }))
                            })().map_err(|source| Error::WithResolvedBindings {
                                source: Box::new(source), bindings: completion["bindings"].as_array().cloned().unwrap_or_default(),
                            })
                        } else { Ok(completion) }
                    });
                    let _ = sender.send(result);
                }).context(RuntimeSnafu { reason: spawn_failure })?;
                let result = (&mut receiver).await;
                worker_finished = true;
                result.map_err(io::Error::other).context(RuntimeSnafu { reason: lost_result })?
            } => result,
            source = stdout_monitor::wait_for_disconnect() => Err(Error::Output { source }),
        };
        let outcome = match result {
            Ok(completion) => tokio::select! {
                biased;
                status = &mut cancellation => Err(cancelled(status?)),
                () = &mut deadline => Err(Error::DeadlineExceeded),
                result = diagnostic_format.write_terminal_record_when_writable(&completion) => result.context(OutputSnafu),
            },
            Err(error) => Err(error),
        };
        if !worker_finished {
            // Stop output first; credential runners own their children through cleanup.
            abort.abort();
            let cleanup = async {
                crate::table_access::unity_catalog::cancel_pending_requests().await;
                let _ = receiver.await;
            };
            let _ = tokio::time::timeout(CLEANUP_GRACE, cleanup).await;
        }
        outcome.map_err(|error| {
            if publication_started.load(Ordering::Acquire)
                && matches!(error, Error::Cancelled { .. } | Error::DeadlineExceeded | Error::Output { .. }
                    | Error::UnityCatalog { source: crate::table_access::unity_catalog::Error::Cancelled { .. } })
            {
                Error::ArtifactPublicationInterrupted { source: Box::new(error), manifest_requested }
            } else { error }
        })
    });
    runtime.shutdown_background();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => diagnostic_format.report_failure_with_timeout(error),
    }
}
