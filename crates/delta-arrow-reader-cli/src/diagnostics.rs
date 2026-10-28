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
use tracing::Subscriber;
use tracing_subscriber::{
    filter::{LevelFilter, Targets, filter_fn},
    fmt::MakeWriter,
    prelude::*,
};

mod completion;
mod correction;
mod error;
mod fallback;

pub(crate) use completion::{build_completion_record, build_json_read_completion};
pub(crate) use correction::{InputCorrection, argument_error_hint};
pub(crate) use error::{
    ArgumentSnafu, ArtifactIoSnafu, ConfigurationSnafu, Error, InputIoSnafu, InputValidationSnafu,
    OutputSnafu, QuerySnafu, ReaderSnafu, RuntimeSnafu, StorageAuthorizationSnafu,
    TerminalOutputSnafu, UnityCatalogSnafu,
};

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

pub(crate) fn init_tracing(diagnostic_format: DiagnosticFormat) {
    if let DiagnosticFormat::Json = diagnostic_format {
        // Machine mode emits terminal records directly, never formatted trace fields.
        return;
    }
    let (targets, invalid) = log_targets();
    // Failure reporting can reach this after command setup already installed tracing.
    if build_subscriber(targets, io::stderr).try_init().is_ok() && invalid {
        tracing::warn!(target: "delta_arrow_reader_cli",
            event = "cli.log_filter.invalid",
            reason = "invalid_rust_log",
            "Invalid RUST_LOG; using WARN"
        );
    }
}

fn log_targets() -> (Targets, bool) {
    match std::env::var("RUST_LOG") {
        Ok(value) if !value.trim().is_empty() => match value.parse::<Targets>() {
            Ok(targets) => (targets, false),
            Err(_) => (Targets::new(), true),
        },
        Err(std::env::VarError::NotUnicode(_)) => (Targets::new(), true),
        _ => (Targets::new(), false),
    }
}

fn build_subscriber<W>(targets: Targets, writer: W) -> impl Subscriber + Send + Sync
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let targets = if targets.default_level().is_some() {
        targets
    } else {
        targets.with_default(LevelFilter::WARN)
    };
    tracing_subscriber::registry()
        .with(targets)
        // Dependency logs have not been audited for credentials or data.
        .with(filter_fn(|metadata| {
            let target = metadata.target();
            target == "delta_arrow_reader"
                || target.starts_with("delta_arrow_reader::")
                || target == "delta_arrow_reader_cli"
                || target.starts_with("delta_arrow_reader_cli::")
        }))
        .with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_ansi(false)
                // Retrying a failed diagnostic through stderr could block the fallback writer.
                .log_internal_errors(false)
                .with_writer(writer),
        )
}

#[cfg(test)]
pub(crate) fn capture_tracing_output(filter: &str, emit: impl FnOnce()) -> String {
    use std::io::{Read, Seek};

    let mut file = tempfile::tempfile().unwrap();
    tracing::subscriber::with_default(
        build_subscriber(
            if filter.is_empty() {
                Targets::new()
            } else {
                filter.parse().unwrap()
            },
            file.try_clone().unwrap(),
        ),
        emit,
    );
    file.rewind().unwrap();
    let mut text = String::new();
    file.read_to_string(&mut text).unwrap();
    text
}

#[cfg(test)]
mod tests {
    use std::{
        fmt, io,
        process::{Command, Stdio},
        sync::Barrier,
        time::{Duration, Instant},
    };

    use arrow_schema::ArrowError;
    use serde_json::Value;

    use super::{DiagnosticFormat, Error, capture_tracing_output};

    struct PanicOnFormatError;

    impl fmt::Debug for PanicOnFormatError {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("secret source Debug was called")
        }
    }

    impl fmt::Display for PanicOnFormatError {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("secret source Display was called")
        }
    }

    impl std::error::Error for PanicOnFormatError {}

    // Run through the production panic hook without adding a CLI fault-injection flag.
    #[test]
    fn run_diagnostic_subprocess_scenario() {
        let Ok(scenario) = std::env::var("DAR_DIAGNOSTIC_TEST_SCENARIO") else {
            return;
        };
        let diagnostic_format = DiagnosticFormat::Json;
        super::init_tracing(diagnostic_format);
        crate::runtime::install_panic_hook(diagnostic_format);
        tracing::error!(target: "delta_arrow_reader", "secret core trace");
        tracing::error!(target: "reqwest", "secret dependency trace");
        tracing::error!(target: "delta_arrow_reader_cli", "secret CLI trace");
        let error = match scenario.as_str() {
            "main_panic" => panic!("secret panic payload"),
            "locked_terminal_panic" => {
                let _guard = super::TERMINAL_RECORD_STARTED.lock().unwrap();
                panic!("secret panic while a terminal writer owns the lock");
            }
            "worker_panics" => {
                let barrier = Barrier::new(3);
                std::thread::scope(|scope| {
                    for _ in 0..2 {
                        scope.spawn(|| {
                            barrier.wait();
                            std::panic::panic_any(PanicOnFormatError);
                        });
                    }
                    barrier.wait();
                });
                unreachable!()
            }
            "source" => Error::Runtime {
                source: io::Error::other("fake child stdout=secret-token\nstderr=secret-password"),
                reason: "runtime_build_failed",
            },
            "unprintable" => Error::ArrowIpc {
                source: ArrowError::ExternalError(Box::new(PanicOnFormatError)),
            },
            "encoding" => Error::ArrowIpc {
                source: ArrowError::IpcError("secret schema metadata".into()),
            },
            _ => unreachable!(),
        };
        diagnostic_format.report_failure(&error);
        diagnostic_format.report_failure(&error);
        diagnostic_format.write_terminal_record(serde_json::json!({"kind": "completion"}));
        std::process::exit(1);
    }

    #[test]
    fn panics_and_source_errors_keep_safe_bounded_reporting() {
        for (scenario, code, reason) in [
            ("main_panic", "runtime_initialization", "process_panicked"),
            (
                "locked_terminal_panic",
                "runtime_initialization",
                "process_panicked",
            ),
            (
                "worker_panics",
                "runtime_initialization",
                "process_panicked",
            ),
            ("source", "runtime_initialization", "runtime_build_failed"),
            ("unprintable", "arrow_ipc", "arrow_ipc_encode_failed"),
            ("encoding", "arrow_ipc", "arrow_ipc_encode_failed"),
        ] {
            for filter in ["off", "trace", "secret-invalid-filter=not-a-level"] {
                let mut child = Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "diagnostics::tests::run_diagnostic_subprocess_scenario",
                        "--nocapture",
                    ])
                    .env("DAR_DIAGNOSTIC_TEST_SCENARIO", scenario)
                    .env("RUST_LOG", filter)
                    .env("RUST_BACKTRACE", "full")
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                let deadline = Instant::now() + Duration::from_secs(3);
                while child.try_wait().unwrap().is_none() {
                    if Instant::now() >= deadline {
                        child.kill().unwrap();
                        child.wait().unwrap();
                        panic!("{scenario} did not exit");
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                let output = child.wait_with_output().unwrap();
                assert_eq!(output.status.code(), Some(1), "{output:?}");
                let text = String::from_utf8(output.stderr).unwrap();
                if scenario == "locked_terminal_panic" {
                    assert!(text.is_empty(), "{text}");
                    continue;
                }
                assert_eq!(text.lines().count(), 1, "{text}");
                assert!(text.ends_with('\n') && text.len() <= 8192, "{text}");
                assert!(!text.contains("secret") && !text.contains('\x1b'), "{text}");
                let record: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(record["format_version"], super::DIAGNOSTIC_FORMAT_VERSION);
                assert!(record.get("message").is_none());
                assert_eq!(record["kind"], "diagnostic");
                assert_eq!(record["terminal"], true);
                assert_eq!(record["phase"], "execution");
                assert_eq!(record["code"], code);
                assert_eq!(record["reason"], reason);
                assert_eq!(record["recovery"], "unknown");
                assert_eq!(record["retryable"], Value::Null);
            }
        }
    }

    fn emit_levels() {
        tracing::error!(target: "delta_arrow_reader", "error marker");
        tracing::warn!(target: "delta_arrow_reader", "warn marker");
        tracing::info!(target: "delta_arrow_reader", "info marker");
        tracing::debug!(target: "delta_arrow_reader::profile", "debug marker");
        tracing::trace!(target: "delta_arrow_reader::profile", "trace marker");
        tracing::error!(target: "delta_arrow_reader_cli", "CLI marker");
    }

    #[test]
    fn native_filter_preserves_levels_and_target_overrides() {
        for (filter, expected) in [
            ("", vec!["ERROR", "WARN", "ERROR"]),
            ("info", vec!["ERROR", "WARN", "INFO", "ERROR"]),
            ("debug", vec!["ERROR", "WARN", "INFO", "DEBUG", "ERROR"]),
            (
                "trace",
                vec!["ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ERROR"],
            ),
            ("off", vec![]),
            ("delta_arrow_reader=off", vec![]),
            (
                "delta_arrow_reader=off,delta_arrow_reader_cli=error",
                vec!["ERROR"],
            ),
            (
                "off,delta_arrow_reader::profile=trace",
                vec!["DEBUG", "TRACE"],
            ),
        ] {
            let text = capture_tracing_output(filter, emit_levels);
            assert_eq!(
                text.lines()
                    .map(|line| line.split_whitespace().next().unwrap())
                    .collect::<Vec<_>>(),
                expected,
                "{filter}: {text}"
            );
            assert!(!text.contains('\x1b'));
        }
    }

    #[test]
    fn dependency_targets_stay_disabled_even_with_explicit_overrides() {
        for filter in [
            "trace",
            "reqwest=trace,object_store=trace,delta_arrow_reader_extra=trace",
            "off,reqwest=error",
        ] {
            let text = capture_tracing_output(filter, || {
                tracing::error!(target: "reqwest", token = "secret-token", "secret HTTP error");
                tracing::warn!(target: "object_store", "secret storage error");
                tracing::trace!(target: "reqwest", "secret HTTP body");
                tracing::error!(target: "delta_arrow_reader_extra", "secret prefix collision");
                tracing::error!(target: "delta_arrow_reader_cli_extra", "secret prefix collision");
                tracing::error!(target: "delta_kernel", "secret log path");
            });
            assert!(text.is_empty(), "{filter}: {text}");
        }
    }
}
