//! Deadlines and cancellation for capabilities, help, and version output.

use std::{io, process::ExitCode, time::Duration};

use serde_json::Value;
use snafu::ResultExt;

use super::signals;
use crate::diagnostics::{self, DiagnosticFormat, Error, OutputSnafu, RuntimeSnafu};

pub(crate) const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn run_discovery(
    diagnostic_format: DiagnosticFormat,
    write_response: impl FnOnce() -> Result<Value, Error> + Send + 'static,
) -> ExitCode {
    let deadline = std::time::Instant::now() + DISCOVERY_TIMEOUT;
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
        tokio::select! {
            biased;
            status = signals::wait_for_cancellation() => Err(Error::Cancelled { status: status? }),
            () = tokio::time::sleep_until(deadline.into()) => Err(Error::Runtime {
                source: io::ErrorKind::TimedOut.into(),
                reason: "discovery_deadline_exceeded",
            }),
            result = async {
                let completion = tokio::task::spawn_blocking(move || {
                    diagnostics::init_tracing(diagnostic_format);
                    write_response()
                }).await.map_err(io::Error::other).context(RuntimeSnafu {
                    reason: "discovery_output_task_failed",
                })??;
                diagnostic_format.write_terminal_record_when_writable(&completion).await.context(OutputSnafu)
            } => result,
        }
    });
    let status = match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => diagnostic_format.report_failure_with_timeout(error),
    };
    // stdout or text logging may still be held by a blocked worker.
    runtime.shutdown_background();
    status
}
