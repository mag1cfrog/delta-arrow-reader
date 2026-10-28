//! Error and completion reporting on the CLI's stderr stream.
//!
//! The CLI reports command outcomes separately from the data it writes to stdout.
//! This module provides the text and JSON diagnostic formats and the correction
//! details attached to recognized input errors. Error categories and command
//! outcomes originate in the CLI and the reader.
//!
//! For example, an invalid row limit can be reported as JSON:
//!
//! ```sh
//! dar scan ./table --limit invalid --diagnostics=json
//! ```
//!
//! This exits with status 2 before opening the table and leaves stdout empty.
//! The stderr record includes `code: "invalid_cli_argument"`,
//! `argument: "--limit"`, and `recovery: "fix_input"`.

use std::{
    io::{self, Write},
    process::ExitCode,
    sync::{Arc, Mutex, TryLockError, mpsc},
    time::Duration,
};

use clap::{Args, FromArgMatches, ValueEnum};
use serde_json::Value;

mod completion;
mod correction;
mod error;
mod fallback;
mod logging;

pub(crate) use completion::{build_completion_record, build_json_read_completion};
pub(crate) use correction::{InputCorrection, argument_error_hint};
pub(crate) use error::{
    ArgumentSnafu, ArtifactIoSnafu, ConfigurationSnafu, Error, InputIoSnafu, InputValidationSnafu,
    OutputSnafu, QuerySnafu, ReaderSnafu, RuntimeSnafu, StorageAuthorizationSnafu,
    TerminalOutputSnafu, UnityCatalogSnafu,
};

#[cfg(test)]
pub(crate) use logging::capture_tracing_output;
pub(crate) use logging::init_tracing;

pub(crate) const FAILURE_DIAGNOSTIC_TIMEOUT: Duration = Duration::from_secs(1);
pub(crate) const DIAGNOSTIC_FORMAT_VERSION: u8 = 2;

/// Safe error fields shared by text tracing and JSON diagnostics.
pub(crate) struct ErrorClassification {
    pub(crate) phase: &'static str,
    pub(crate) code: &'static str,
    pub(crate) reason: &'static str,
    pub(crate) source_kind: Option<&'static str>,
}

#[derive(Args)]
pub(crate) struct DiagnosticOptions {
    /// JSON errors and completion on stderr, including argument errors.
    /// Verification: capabilities --schema completion.
    #[arg(
        long = "diagnostics",
        value_name = "FORMAT",
        value_enum,
        global = true,
        require_equals = true
    )]
    diagnostic_format: Option<DiagnosticFormat>,
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum DiagnosticFormat {
    #[value(skip)]
    Text,
    Json,
}

// Serialize terminal writes so simultaneous worker panics cannot interleave records.
static TERMINAL_RECORD_STARTED: Mutex<bool> = Mutex::new(false);

impl DiagnosticFormat {
    pub(crate) fn from_cli_args() -> Self {
        let mut parser = DiagnosticOptions::augment_args(
            clap::Command::new("diagnostics")
                .no_binary_name(true)
                .disable_help_flag(true),
        );
        // Required '=' makes each selection self-contained, even after invalid arguments.
        for arg in std::env::args_os().skip(1).take_while(|arg| arg != "--") {
            if let Ok(matches) = parser.try_get_matches_from_mut([arg])
                && let Ok(options) = DiagnosticOptions::from_arg_matches(&matches)
                && let Some(diagnostic_format) = options.diagnostic_format
            {
                return diagnostic_format;
            }
        }
        Self::Text
    }

    pub(crate) fn report_failure(self, error: &Error) {
        match self {
            Self::Text => error.emit_tracing_diagnostic(),
            Self::Json => self.write_terminal_record(error.to_json_diagnostic()),
        }
    }

    pub(crate) fn report_failure_with_timeout(self, error: Error) -> ExitCode {
        let status = error.exit_code();
        let error = Arc::new(error);
        let worker_error = Arc::clone(&error);
        let (finished, receiver) = mpsc::channel();
        // This path must work even when constructing a Tokio runtime failed.
        if std::thread::Builder::new()
            .name("dar-diagnostic".into())
            .spawn(move || {
                init_tracing(self);
                self.report_failure(&worker_error);
                let _ = finished.send(());
            })
            .is_ok()
        {
            let _ = receiver.recv_timeout(FAILURE_DIAGNOSTIC_TIMEOUT);
        } else {
            self.report_failure_without_thread(&error);
        }
        status
    }

    /// Publishes a terminal record in small writes, remaining cancellable between them.
    pub(crate) async fn write_terminal_record_when_writable(
        self,
        record: &Value,
    ) -> io::Result<()> {
        if let Self::Json = self {
            let line = format!("{record}\n");
            let mut bytes_written = 0;
            loop {
                let started = match TERMINAL_RECORD_STARTED.try_lock() {
                    Ok(guard) => Some(guard),
                    Err(TryLockError::Poisoned(error)) => Some(error.into_inner()),
                    Err(TryLockError::WouldBlock) => None,
                };
                let mut wrote_chunk = false;
                if let Some(mut started) = started {
                    if *started && bytes_written == 0 {
                        return Ok(());
                    }
                    // POSIX guarantees at least 512 bytes for an atomic pipe write.
                    let end = (bytes_written + 512).min(line.len());
                    let chunk = &line.as_bytes()[bytes_written..end];
                    if crate::output::capacity::can_write(&io::stderr(), chunk.len())? {
                        // A partial record must not be followed by a second terminal record.
                        *started = true;
                        io::stderr().lock().write_all(chunk)?;
                        bytes_written = end;
                        if bytes_written == line.len() {
                            return Ok(());
                        }
                        wrote_chunk = true;
                    }
                }
                if wrote_chunk {
                    tokio::task::yield_now().await;
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn write_terminal_record(self, record: Value) {
        if let Self::Json = self {
            let mut written = TERMINAL_RECORD_STARTED
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !*written {
                let mut line = record.to_string();
                line.push('\n');
                let _ = io::stderr().lock().write_all(line.as_bytes());
                *written = true;
            }
        }
    }
}

#[cfg(test)]
mod tests;
