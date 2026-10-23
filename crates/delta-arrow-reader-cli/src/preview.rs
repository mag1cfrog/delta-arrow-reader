//! A small row sample for choosing a projection or checking a predicate.
//!
//! For example, `dar preview ./table --column id --limit 3` returns up to three
//! typed rows. Reaching a limit never claims that the snapshot was exhausted.

use std::{io, num::NonZeroUsize, process::ExitCode, time::Duration};

use clap::Args;
use futures_util::future::{AbortHandle, Abortable};
use snafu::ResultExt;

use crate::{
    Command, Error, OutputSnafu, RowReadArgs, RuntimeSnafu, diagnostics,
    diagnostics::DiagnosticFormat, execute_table_command, parse_unsigned_decimal, signals,
    stdout_monitor,
};

const CLEANUP_GRACE: Duration = Duration::from_secs(1);

#[derive(Args)]
pub(crate) struct PreviewArgs {
    #[command(flatten)]
    pub(crate) read: RowReadArgs,
    /// Maximum sample rows; zero returns only the selected schema.
    #[arg(long, default_value = "20", value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
    pub(crate) limit: usize,
    /// Whole JSON response budget, including schema and newline. Narrow the projection first.
    #[arg(long, default_value = "8192", value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) max_output_bytes: NonZeroUsize,
    /// Whole-command deadline in milliseconds, plus bounded cleanup and failure reporting.
    #[arg(long, default_value = "30000", value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    deadline_ms: NonZeroUsize,
}

pub(crate) fn run_preview(args: PreviewArgs, diagnostic_format: DiagnosticFormat) -> ExitCode {
    let started = std::time::Instant::now();
    let time_budget = Duration::from_millis(args.deadline_ms.get() as u64);
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
        let (sender, mut receiver) = tokio::sync::oneshot::channel();
        // Credential providers can synchronously read files, including blocked FIFOs.
        // Keep the deadline and cancellation listeners off their executor.
        std::thread::Builder::new()
            .name("dar-preview".into())
            .spawn(move || {
                let result = (|| {
                    diagnostics::init_tracing(diagnostic_format);
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .context(RuntimeSnafu {
                            reason: "runtime_build_failed",
                        })?;
                    let result = runtime.block_on(Abortable::new(
                        execute_table_command(Command::Preview(args)),
                        registration,
                    ));
                    // Drop initial and renewal authorization tasks before acknowledging cancellation.
                    runtime.shutdown_background();
                    result.map_err(|_| Error::Cancelled { status: 1 })?
                })();
                let _ = sender.send(result);
            })
            .context(RuntimeSnafu {
                reason: "preview_thread_spawn_failed",
            })?;
        let mut worker_finished = false;
        let outcome = tokio::select! {
            biased;
            status = signals::wait_for_cancellation() => Err(Error::Cancelled { status: status? }),
            () = tokio::time::sleep(time_budget.saturating_sub(started.elapsed())) => Err(Error::DeadlineExceeded),
            source = stdout_monitor::wait_for_disconnect() => Err(Error::Output { source }),
            result = async {
                let result = (&mut receiver).await;
                worker_finished = true;
                let completion = result.map_err(io::Error::other).context(RuntimeSnafu {
                    reason: "preview_thread_result_lost",
                })??;
                diagnostic_format.write_terminal_record_when_writable(completion)
                    .await.context(OutputSnafu)
            } => result,
        };
        if !worker_finished {
            // Let child runners reach kill().await before dropping their futures.
            if !crate::unity_catalog::cancel_pending_requests() {
                abort.abort();
            }
            let _ = tokio::time::timeout(CLEANUP_GRACE, receiver).await;
            abort.abort();
        }
        outcome
    });
    runtime.shutdown_background();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => diagnostic_format.report_failure_with_timeout(error),
    }
}
