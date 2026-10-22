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
    sync::{Mutex, TryLockError, mpsc},
    time::Duration,
};

use clap::{
    Args, CommandFactory, FromArgMatches, ValueEnum,
    error::{ContextKind, ContextValue, ErrorKind},
};
use serde_json::Value;
use tracing::Subscriber;
use tracing_subscriber::{
    filter::{LevelFilter, Targets, filter_fn},
    fmt::MakeWriter,
    prelude::*,
};

mod stderr_capacity;

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
static TERMINAL_RECORD_WRITTEN: Mutex<bool> = Mutex::new(false);

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

    pub(crate) fn report_failure(self, error: &crate::Error) {
        match self {
            Self::Text => error.log_failure(),
            Self::Json => self.write_terminal_record(error.to_json_diagnostic()),
        }
    }

    pub(crate) fn report_failure_with_timeout(self, error: crate::Error) -> ExitCode {
        let status = error.exit_code();
        let (finished, receiver) = mpsc::channel();
        // This path must work even when constructing a Tokio runtime failed.
        if std::thread::Builder::new()
            .name("dar-diagnostic".into())
            .spawn(move || {
                init_tracing(self);
                self.report_failure(&error);
                let _ = finished.send(());
            })
            .is_ok()
        {
            let _ = receiver.recv_timeout(FAILURE_DIAGNOSTIC_TIMEOUT);
        }
        status
    }

    /// Waits for space before publishing a terminal record of at most 512 bytes.
    pub(crate) async fn write_terminal_record_when_writable(self, record: Value) -> io::Result<()> {
        if let Self::Json = self {
            let line = format!("{record}\n");
            // POSIX guarantees at least 512 bytes for an atomic pipe write.
            // Keep terminal records within the atomic write bound.
            if line.len() > 512 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "terminal record too large",
                ));
            }
            loop {
                let written = match TERMINAL_RECORD_WRITTEN.try_lock() {
                    Ok(guard) => Some(guard),
                    Err(TryLockError::Poisoned(error)) => Some(error.into_inner()),
                    Err(TryLockError::WouldBlock) => None,
                };
                if let Some(mut written) = written {
                    if *written {
                        return Ok(());
                    }
                    if stderr_capacity::can_write(line.len())? {
                        *written = true;
                        return io::stderr().lock().write_all(line.as_bytes());
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Ok(())
    }

    pub(crate) fn write_terminal_record(self, record: Value) {
        if let Self::Json = self {
            let mut written = TERMINAL_RECORD_WRITTEN
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

#[derive(Clone)]
pub(crate) struct InputCorrection {
    pub(crate) argument: String,
    pub(crate) field_path: Option<String>,
    pub(crate) expected: String,
    pub(crate) supplied: Option<Box<str>>,
    pub(crate) hint: &'static str,
}

impl InputCorrection {
    pub(crate) fn from_clap(error: &clap::Error) -> Option<Self> {
        if matches!(
            error.kind(),
            ErrorKind::UnknownArgument | ErrorKind::InvalidSubcommand | ErrorKind::InvalidUtf8
        ) {
            return None;
        }
        let mut command = crate::Cli::command();
        command.build();
        match error.get(ContextKind::InvalidArg)? {
            ContextValue::String(argument) => {
                Self::from_declaration(&command, argument, error.kind())
            }
            ContextValue::Strings(arguments) => arguments
                .iter()
                .find_map(|argument| Self::from_declaration(&command, argument, error.kind())),
            _ => None,
        }
    }

    fn from_declaration(command: &clap::Command, reported: &str, kind: ErrorKind) -> Option<Self> {
        // Only return declaration text, never values copied from the failing invocation.
        for argument in command.get_arguments() {
            if argument.to_string() == reported {
                let expected = match argument.get_id().as_str() {
                    "limit" => format!("unsigned decimal integer from 0 to {}", usize::MAX),
                    "target_partitions" | "max_output_bytes" | "deadline_ms" => {
                        format!("unsigned decimal integer from 1 to {}", usize::MAX)
                    }
                    "table_version" => format!("unsigned decimal integer from 0 to {}", u64::MAX),
                    "diagnostic_format" => "json (use --diagnostics=json)".to_owned(),
                    _ => argument.get_help()?.to_string(),
                };
                return Some(Self {
                    argument: argument
                        .get_long()
                        .map_or_else(|| argument.get_id().to_string(), |name| format!("--{name}")),
                    field_path: None,
                    expected,
                    supplied: None,
                    hint: argument_error_hint(kind),
                });
            }
        }
        command
            .get_subcommands()
            .find_map(|command| Self::from_declaration(command, reported, kind))
    }

    pub(crate) fn from_reader(error: &delta_arrow_reader::DeltaReaderError) -> Option<Self> {
        if let delta_arrow_reader::DeltaReaderError::PredicateTypeMismatch {
            expected,
            supplied,
            path,
            ..
        } = error
        {
            return Some(crate::input::predicate_type_mismatch_correction(
                expected, supplied, path,
            ));
        }
        let (argument, expected, hint) = match (error.code(), error.reason()) {
            ("invalid_projection", "column_not_found") => (
                "--column",
                "a logical column name in the selected snapshot",
                "Use inspect TABLE --view summary --column-prefix PREFIX with the same authorization and --table-version N to find column names.",
            ),
            ("invalid_projection", "duplicate_column") => (
                "--column",
                "each logical column selected at most once",
                "Remove repeated --column values.",
            ),
            ("unsupported_predicate", "column_not_found" | "ambiguous_column") => (
                "--predicate-file",
                "an unambiguous top-level logical column in the selected snapshot",
                "Use inspect TABLE --view summary --column-prefix PREFIX with the same authorization and --table-version N; choose an unambiguous name for the predicate's column field.",
            ),
            ("unsupported_predicate", "invalid_column_reference") => (
                "--predicate-file",
                "a nonempty top-level logical column name without dots",
                "Use inspect TABLE --view summary --column-prefix PREFIX with the same authorization and --table-version N. Predicates accept top-level names without dots.",
            ),
            ("unsupported_predicate", "invalid_decimal") => (
                "--predicate-file",
                "a decimal128 value within its declared precision and valid scale",
                "Use inspect TABLE --view summary --column NAME with the same authorization and --table-version N for precision/scale; ensure the unscaled value fits.",
            ),
            _ => return None,
        };
        Some(Self {
            argument: argument.to_owned(),
            field_path: None,
            expected: expected.to_owned(),
            supplied: None,
            hint,
        })
    }
}

pub(crate) fn argument_error_hint(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::MissingRequiredArgument => "Supply the required argument.",
        ErrorKind::ArgumentConflict => "Remove the conflicting or repeated argument.",
        ErrorKind::NoEquals => "Attach the value to the option with '='.",
        ErrorKind::InvalidUtf8 => "Use UTF-8 text for command-line arguments.",
        ErrorKind::UnknownArgument | ErrorKind::InvalidSubcommand => {
            "Use --help to choose a supported command or option."
        }
        ErrorKind::ValueValidation
        | ErrorKind::InvalidValue
        | ErrorKind::TooFewValues
        | ErrorKind::TooManyValues
        | ErrorKind::WrongNumberOfValues => {
            "Supply a value matching the argument's expected format."
        }
        _ => "Use --help to check the command's arguments.",
    }
}

pub(crate) fn init_tracing(diagnostic_format: DiagnosticFormat) {
    if let DiagnosticFormat::Json = diagnostic_format {
        // Machine mode emits terminal records directly, never formatted trace fields.
        return;
    }
    let (targets, invalid) = match std::env::var("RUST_LOG") {
        Ok(value) if !value.trim().is_empty() => match value.parse::<Targets>() {
            Ok(targets) => (targets, false),
            Err(_) => (Targets::new(), true),
        },
        Err(std::env::VarError::NotUnicode(_)) => (Targets::new(), true),
        _ => (Targets::new(), false),
    };
    // Failure reporting can reach this after command setup already installed tracing.
    if subscriber(targets, io::stderr).try_init().is_ok() && invalid {
        tracing::warn!(target: "delta_arrow_reader_cli",
            event = "cli.log_filter.invalid",
            reason = "invalid_rust_log",
            "Invalid RUST_LOG; using WARN"
        );
    }
}

fn subscriber<W>(targets: Targets, writer: W) -> impl Subscriber + Send + Sync
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
                .with_writer(writer),
        )
}

#[cfg(test)]
pub(crate) fn capture(filter: &str, emit: impl FnOnce()) -> String {
    use std::io::{Read, Seek};

    let mut file = tempfile::tempfile().unwrap();
    tracing::subscriber::with_default(
        subscriber(
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

    use super::{DiagnosticFormat, capture};
    use crate::Error;

    struct PanicOnFormatError;

    #[tokio::test]
    async fn oversized_completion_is_rejected_before_writing() {
        let error = DiagnosticFormat::Json
            .write_terminal_record_when_writable(serde_json::json!({"padding": "x".repeat(512)}))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

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
    fn diagnostic_subprocess_fixture() {
        let Ok(scenario) = std::env::var("DAR_DIAGNOSTIC_TEST_SCENARIO") else {
            return;
        };
        let diagnostic_format = DiagnosticFormat::Json;
        super::init_tracing(diagnostic_format);
        crate::install_panic_hook(diagnostic_format);
        tracing::error!(target: "delta_arrow_reader", "secret core trace");
        tracing::error!(target: "reqwest", "secret dependency trace");
        tracing::error!(target: "delta_arrow_reader_cli", "secret CLI trace");
        let error = match scenario.as_str() {
            "main_panic" => panic!("secret panic payload"),
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
    fn panics_and_source_errors_emit_one_safe_terminal_record() {
        for (scenario, code, reason) in [
            ("main_panic", "runtime_initialization", "process_panicked"),
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
                        "diagnostics::tests::diagnostic_subprocess_fixture",
                        "--nocapture",
                    ])
                    .env("DAR_DIAGNOSTIC_TEST_SCENARIO", scenario)
                    .env("RUST_LOG", filter)
                    .env("RUST_BACKTRACE", "full")
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .spawn()
                    .unwrap();
                let deadline = Instant::now() + Duration::from_secs(15);
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
            let text = capture(filter, emit_levels);
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
            let text = capture(filter, || {
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
