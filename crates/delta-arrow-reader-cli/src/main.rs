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

mod azure_sas;
mod capabilities;
mod command_runtime;
mod diagnostics;
mod direct_credentials;
mod input;
mod inspection;
mod output;
mod output_capacity;
mod preview;
mod query;
mod signals;
mod stdout_monitor;
mod table_input;
mod unity_catalog;

use std::{
    fmt,
    io::{self, IsTerminal, Write},
    num::NonZeroUsize,
    path::Path,
    process::ExitCode,
    str::FromStr,
    sync::atomic::{AtomicBool, Ordering},
};

use arrow_schema::{ArrowError, Schema};
use clap::{Args, Parser, Subcommand, error::ErrorKind};
use delta_arrow_reader::{DeltaReaderError, error_source_kind};
use diagnostics::{DiagnosticFormat, ErrorClassification, InputCorrection};
use futures_util::{FutureExt, TryStreamExt, future::LocalBoxFuture};
use input::JsonInputKind;
use serde_json::{Value, json};
use snafu::{ResultExt, Snafu, ensure};
use table_input::TableArgs;

/// Read Delta Lake snapshots as JSON or Arrow IPC streams.
#[derive(Parser)]
#[command(
    name = command_name(), bin_name = command_name(), version,
    after_help = "When version or column types are unknown, use inspect TABLE --view summary [--column NAME]; pin reads with --table-version N.\nGuides: capabilities --command NAME; capabilities --schema NAME [--detail FIELD_OR_TYPE].\nInput formats: predicate, storage-options, execution-options, tables. Output formats: rows, inspection-summary, inspection, diagnostic, completion."
)]
struct Cli {
    #[command(flatten)]
    diagnostics: diagnostics::DiagnosticOptions,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
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
        inspection: inspection::InspectionArgs,
        /// Whole-command deadline in milliseconds. Summary defaults to 30000; full has no default.
        #[arg(long, value_name = "N", default_value_if("view", "summary", "30000"), value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
        deadline_ms: Option<NonZeroUsize>,
    },
    /// Read a small typed JSON sample with row, byte, and time limits.
    Preview(preview::PreviewArgs),
    /// Run one read-only SQL query over explicitly selected Delta snapshots.
    Query(query::QueryArgs),
    /// Stream a Delta snapshot's rows as Arrow IPC to stdout.
    ///
    /// Redirect stdout to a file or pipe; terminal output is rejected.
    Scan {
        #[command(flatten)]
        read: RowReadArgs,
        /// Maximum output rows as unsigned decimal digits; zero is valid.
        #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
        limit: Option<usize>,
        /// Whole-command deadline in milliseconds; no default for legacy stdout scans.
        #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
        deadline_ms: Option<NonZeroUsize>,
    },
}

/// Snapshot, projection, predicate, and execution choices shared by scan and preview.
#[derive(Args)]
struct RowReadArgs {
    #[command(flatten)]
    table: TableArgs,
    /// Existing Databricks CLI profile; required for uc://catalog.schema.table targets.
    #[arg(long, value_name = "PROFILE")]
    databricks_profile: Option<String>,
    /// Logical column name; repeat to select columns in argument order. Default: all columns.
    #[arg(long = "column", value_name = "NAME")]
    columns: Vec<String>,
    /// Read zero columns while preserving row counts.
    #[arg(long, conflicts_with = "columns")]
    no_columns: bool,
    /// Positive execution partition target; default: native automatic selection.
    #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    target_partitions: Option<NonZeroUsize>,
    /// Local typed JSON predicate; at most 1 MiB. '-' is a filename, not stdin.
    /// Match literal types to inspected column types; integer values are strings.
    /// Format: capabilities --schema predicate.
    #[arg(long, value_name = "PATH")]
    predicate_file: Option<String>,
    /// Local JSON execution options; at most 1 MiB. '-' is a filename, not stdin.
    #[arg(long, value_name = "PATH")]
    execution_options_file: Option<String>,
}

#[derive(Snafu)]
enum Error {
    #[snafu(display("Invalid command-line arguments (kind={kind:?})."))]
    Argument {
        kind: ErrorKind,
        correction: Option<InputCorrection>,
    },
    #[snafu(display("Arrow IPC output requires redirected stdout."))]
    TerminalOutput,
    #[snafu(display(
        "Invalid JSON input (kind={:?}, line={}, column={}).",
        source.classify(), source.line(), source.column()
    ))]
    InputJson {
        source: serde_json::Error,
        input_kind: JsonInputKind,
        correction: Option<InputCorrection>,
    },
    #[snafu(display("Invalid JSON input (reason={reason})."))]
    InputValidation {
        reason: &'static str,
        correction: InputCorrection,
    },
    #[snafu(display("Invalid env file (reason={reason})."))]
    EnvFileInput {
        reason: &'static str,
        line: Option<usize>,
        expected: &'static str,
    },
    #[snafu(display("Could not read the input file."))]
    InputIo {
        source: io::Error,
        argument: &'static str,
    },
    #[snafu(display("Invalid storage authorization (reason={reason})."))]
    StorageAuthorization { reason: &'static str },
    #[snafu(display("{source}"))]
    CredentialFileValidation {
        source: Box<Error>,
        correction: InputCorrection,
    },
    #[snafu(display("{source}"))]
    ManifestAuthorizationInput {
        source: Box<Error>,
        field_path: String,
    },
    #[snafu(display("The reader runtime failed."))]
    Runtime {
        source: io::Error,
        reason: &'static str,
    },
    #[snafu(display("Could not write or flush stdout."))]
    Output { source: io::Error },
    #[snafu(display("Could not encode the Arrow IPC stream."))]
    ArrowIpc { source: ArrowError },
    #[snafu(display("Could not encode the JSON result."))]
    JsonOutput { source: serde_json::Error },
    #[snafu(display("The result contains a type unsupported by typed JSON."))]
    UnsupportedJsonType { command: &'static str },
    #[snafu(display("{source}"))]
    Reader { source: DeltaReaderError },
    #[snafu(display("{source}"))]
    Configuration { source: DeltaReaderError },
    #[snafu(display("{source}"))]
    UnityCatalog { source: unity_catalog::Error },
    #[snafu(display("{source}"))]
    Query { source: query::QueryError },
    #[snafu(display("{source}"))]
    QueryWithResolvedBindings {
        source: Box<Error>,
        bindings: Vec<Value>,
    },
    #[snafu(display("Output exceeded its resource ceiling (reason={reason})."))]
    OutputLimit { reason: &'static str },
    #[snafu(display("Command exceeded its time budget."))]
    DeadlineExceeded,
    #[snafu(display("Command was cancelled."))]
    Cancelled { status: u8 },
}

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        match source {
            ArrowError::IoError(_, source)
                if source
                    .get_ref()
                    .is_some_and(|error| error.is::<output::arrow::OutputBytesExceeded>()) =>
            {
                Self::OutputLimit {
                    reason: "output_byte_limit_exceeded",
                }
            }
            ArrowError::IoError(_, source) => Self::Output { source },
            source => Self::ArrowIpc { source },
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl Error {
    fn classification(&self) -> ErrorClassification {
        match self {
            Self::ManifestAuthorizationInput { source, .. }
            | Self::CredentialFileValidation { source, .. }
            | Self::QueryWithResolvedBindings { source, .. } => source.classification(),
            Self::Reader { source } | Self::Configuration { source } => ErrorClassification {
                phase: source.phase().as_str(),
                code: source.code(),
                reason: source.reason(),
                source_kind: source.source_kind(),
            },
            Self::Argument { .. } => ErrorClassification {
                phase: "configuration",
                code: "invalid_cli_argument",
                reason: "argument_parse_failed",
                source_kind: None,
            },
            Self::TerminalOutput => ErrorClassification {
                phase: "configuration",
                code: "invalid_cli_argument",
                reason: "stdout_is_terminal",
                source_kind: None,
            },
            Self::InputJson { .. } => ErrorClassification {
                phase: "configuration",
                code: "invalid_input_json",
                reason: "input_json_decode_failed",
                source_kind: None,
            },
            Self::InputValidation { reason, .. } => ErrorClassification {
                phase: "configuration",
                code: "invalid_input_json",
                reason,
                source_kind: None,
            },
            Self::EnvFileInput { reason, .. } => ErrorClassification {
                phase: "configuration",
                code: "invalid_input_env",
                reason,
                source_kind: None,
            },
            Self::InputIo { source, .. } => ErrorClassification {
                phase: "configuration",
                code: "input_file_io",
                reason: "input_file_read_failed",
                source_kind: Some(error_source_kind(source)),
            },
            Self::StorageAuthorization { reason } => ErrorClassification {
                phase: "configuration",
                code: "invalid_storage_authorization",
                reason,
                source_kind: None,
            },
            Self::Runtime { source, reason } => ErrorClassification {
                phase: "execution",
                code: "runtime_initialization",
                reason,
                source_kind: Some(error_source_kind(source)),
            },
            Self::Output { source } => ErrorClassification {
                phase: "execution",
                code: "output_write",
                reason: "stdout_write_failed",
                source_kind: Some(error_source_kind(source)),
            },
            Self::ArrowIpc { source } => ErrorClassification {
                phase: "execution",
                code: "arrow_ipc",
                reason: "arrow_ipc_encode_failed",
                source_kind: Some(error_source_kind(source)),
            },
            Self::JsonOutput { .. } => ErrorClassification {
                phase: "execution",
                code: "json_output",
                reason: "json_encode_failed",
                source_kind: None,
            },
            Self::UnsupportedJsonType { .. } => ErrorClassification {
                phase: "execution",
                code: "unsupported_json_type",
                reason: "unsupported_json_type",
                source_kind: Some("unsupported"),
            },
            Self::UnityCatalog { source } => ErrorClassification {
                phase: source.phase(),
                code: source.code(),
                reason: source.code(),
                source_kind: None,
            },
            Self::Query { source } => source.classification(),
            Self::OutputLimit { reason } => ErrorClassification {
                phase: "execution",
                code: "output_limit",
                reason,
                source_kind: Some("resource_exhausted"),
            },
            Self::DeadlineExceeded => ErrorClassification {
                phase: "execution",
                code: "deadline_exceeded",
                reason: "command_deadline_exceeded",
                source_kind: Some("resource_exhausted"),
            },
            Self::Cancelled { .. } => ErrorClassification {
                phase: "execution",
                code: "command_cancelled",
                reason: "command_cancelled",
                source_kind: None,
            },
        }
    }

    fn input_correction(&self) -> Option<InputCorrection> {
        let (input_kind, hint) = match self {
            Self::QueryWithResolvedBindings { source, .. } => return source.input_correction(),
            Self::Argument { correction, .. } => return correction.clone(),
            Self::InputValidation { correction, .. }
            | Self::CredentialFileValidation { correction, .. } => return Some(correction.clone()),
            Self::ManifestAuthorizationInput { source, field_path } => {
                let mut correction = source.input_correction()?;
                correction.field_path = Some(field_path.clone());
                correction.argument = "--tables-file".into();
                if matches!(source.as_ref(), Self::InputIo { source, .. } if source.kind() == io::ErrorKind::NotFound)
                {
                    correction.hint = "Provide an existing input file; relative references use the manifest directory.";
                }
                return Some(correction);
            }
            Self::EnvFileInput { expected, .. } => {
                return Some(InputCorrection {
                    argument: "--env-file".into(),
                    field_path: None,
                    expected: (*expected).into(),
                    supplied: None,
                    hint: "Use one NAME=VALUE record per line; quotes must enclose the whole value.",
                });
            }
            Self::InputJson {
                correction: Some(correction),
                ..
            } => return Some(correction.clone()),
            Self::InputJson {
                source, input_kind, ..
            } => (
                *input_kind,
                match source.classify() {
                    serde_json::error::Category::Syntax => {
                        "Fix JSON syntax at the reported line and column."
                    }
                    serde_json::error::Category::Eof => {
                        "Complete the JSON document at the reported line and column."
                    }
                    serde_json::error::Category::Data => input_kind.schema_hint(),
                    serde_json::error::Category::Io => "Check that the input file is readable.",
                },
            ),
            Self::InputIo { source, argument } => {
                let hint = match source.kind() {
                    io::ErrorKind::NotFound => {
                        "Provide an existing input file; relative paths use the working directory."
                    }
                    io::ErrorKind::PermissionDenied => "Request read access to the input file.",
                    _ => "Check that the input path names a readable file.",
                };
                return Some(InputCorrection {
                    argument: (*argument).to_owned(),
                    field_path: None,
                    expected: "a readable local file".to_owned(),
                    supplied: None,
                    hint,
                });
            }
            Self::Configuration { source } => {
                return input::execution_options_correction(source.reason());
            }
            Self::Reader { source } => return InputCorrection::from_reader(source),
            Self::OutputLimit {
                reason: "inspection_output_limit_exceeded",
            } => {
                return Some(InputCorrection {
                    argument: "--max-output-bytes".to_owned(),
                    expected: "a budget large enough for the complete selected inspection"
                        .to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Narrow the selection or increase --max-output-bytes; keep the same authorization and --table-version N.",
                });
            }
            Self::OutputLimit {
                reason: "json_output_limit_exceeded",
            } => {
                return Some(InputCorrection {
                    argument: "--max-output-bytes".to_owned(),
                    expected: "enough bytes for the complete JSON result".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Explicitly increase --max-output-bytes for the complete JSON, or use --format arrow with redirected stdout and an explicit byte budget. Keep the same SQL, authorization, table bindings, and snapshot versions.",
                });
            }
            Self::OutputLimit {
                reason: "output_byte_limit_exceeded",
            } => {
                return Some(InputCorrection {
                    argument: "--max-output-bytes".to_owned(),
                    expected: "enough bytes for the complete Arrow stream".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Increase --max-output-bytes explicitly; keep the same SQL, authorization, table bindings, and snapshot versions.",
                });
            }
            Self::OutputLimit {
                reason: "preview_schema_limit_exceeded",
            } => {
                return Some(InputCorrection {
                    argument: "--column".to_owned(),
                    expected: "a selected schema within the preview byte budget".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Select fewer --column values, or explicitly increase --max-output-bytes for the required schema; preserve authorization and --table-version N.",
                });
            }
            Self::OutputLimit {
                reason: "output_row_limit_exceeded",
            } => {
                return Some(InputCorrection {
                    argument: "--max-rows".to_owned(),
                    expected: "enough rows for the complete SQL result".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Explicitly increase --max-rows; keep the same SQL, authorization, table bindings, and snapshot versions.",
                });
            }
            Self::OutputLimit {
                reason: "preview_row_too_large",
            } => {
                return Some(InputCorrection {
                    argument: "--max-output-bytes".to_owned(),
                    expected: "enough bytes for the selected schema and first row".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Explicitly increase --max-output-bytes; keep the same projection, predicate, row limit, authorization, --table-version N, and --deadline-ms.",
                });
            }
            Self::UnsupportedJsonType { command: "preview" } => {
                return Some(InputCorrection {
                    argument: "--column".to_owned(),
                    expected: "selected values that fit typed JSON".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Use scan with redirected stdout and the same projection, predicate, row limit, authorization, and --table-version N.",
                });
            }
            Self::DeadlineExceeded => {
                return Some(InputCorrection {
                    argument: "--deadline-ms".to_owned(),
                    expected: "enough milliseconds to finish the command".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Discard stdout; check source responsiveness, then retry the same pinned request with an explicit --deadline-ms. Keep authorization and all other limits.",
                });
            }
            Self::UnsupportedJsonType { .. } => {
                return Some(InputCorrection {
                    argument: "--format".to_owned(),
                    expected: "arrow for this result type".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Use --format arrow with redirected stdout and an explicit byte budget; keep the same SQL, authorization, table bindings, and snapshot versions.",
                });
            }
            _ => return None,
        };
        Some(input_kind.correction(hint))
    }

    fn emit_tracing_diagnostic(&self) {
        let mut source_error = self;
        let mut manifest_field_path = None;
        loop {
            match source_error {
                Self::ManifestAuthorizationInput { source, field_path } => {
                    source_error = source;
                    manifest_field_path = Some(field_path.as_str());
                }
                Self::QueryWithResolvedBindings { source, .. }
                | Self::CredentialFileValidation { source, .. } => source_error = source,
                _ => break,
            }
        }
        if let Self::Cancelled { status } = source_error {
            tracing::info!(target: "delta_arrow_reader_cli",
                event = "cli.cancelled", operation = "cli_command", outcome = "cancelled",
                error_phase = "execution", error_code = "command_cancelled", exit_status = status,
                "CLI command cancelled"
            );
            return;
        }
        if let Self::UnityCatalog {
            source: unity_catalog::Error::Cancelled { status },
        } = source_error
        {
            tracing::info!(target: "delta_arrow_reader_cli",
                event = "cli.cancelled", operation = "cli_command", outcome = "cancelled",
                error_phase = "execution", error_code = "uc_cancelled", exit_status = status,
                "Unity Catalog command cancelled"
            );
            return;
        }
        let correction = self.input_correction();
        let argument = manifest_field_path
            .map(|_| "--tables-file")
            .or_else(|| correction.as_ref().map(|value| value.argument.as_str()));
        let expected = correction.as_ref().map(|value| value.expected.as_str());
        let supplied = correction
            .as_ref()
            .and_then(|value| value.supplied.as_deref());
        let field_path = manifest_field_path.or_else(|| {
            correction
                .as_ref()
                .and_then(|value| value.field_path.as_deref())
        });
        let hint = correction
            .as_ref()
            .map(|value| value.hint)
            .or_else(|| match source_error {
                Self::Argument { kind, .. } => Some(diagnostics::argument_error_hint(*kind)),
                _ => None,
            });
        let ErrorClassification {
            phase,
            code,
            reason,
            source_kind,
        } = self.classification();
        // Core owns the terminal failure; CLI guidance supplements that event.
        if matches!(source_error, Self::Reader { .. }) {
            if correction.is_some() {
                tracing::error!(target: "delta_arrow_reader_cli",
                    event = "cli.input_correction", error_phase = phase, error_code = code,
                    error_reason = reason, argument, expected, supplied, hint, field_path,
                    "Correct the command input"
                );
            }
            return;
        }
        let argument_kind = match source_error {
            Self::Argument { kind, .. } => Some(tracing::field::debug(kind)),
            _ => None,
        };
        let (json_kind, json_line, json_column) = match source_error {
            Self::InputJson { source, .. } => (
                Some(tracing::field::debug(source.classify())),
                Some(source.line()),
                Some(source.column()),
            ),
            _ => (None, None, None),
        };
        let (io_kind, os_error) = match source_error {
            Self::InputIo { source, .. }
            | Self::Runtime { source, .. }
            | Self::Output { source } => (
                Some(tracing::field::debug(source.kind())),
                source.raw_os_error(),
            ),
            _ => (None, None),
        };
        let env_file_line = match source_error {
            Self::EnvFileInput { line, .. } => *line,
            _ => None,
        };
        let message: &dyn fmt::Display = match source_error {
            Self::UnityCatalog { source } => source,
            Self::Query { source } => source,
            Self::StorageAuthorization { .. } => source_error,
            _ => &"CLI command failed",
        };
        tracing::error!(target: "delta_arrow_reader_cli",
            event = "cli.failed", operation = "cli_command", outcome = "failed",
            error_phase = phase, error_code = code, error_reason = reason,
            error_source_kind = source_kind, argument_kind, argument, expected, supplied, hint, field_path,
            json_kind, json_line, json_column, env_file_line, io_kind, os_error,
            "{message}"
        );
    }

    fn to_json_diagnostic(&self) -> Value {
        let ErrorClassification {
            phase,
            code,
            reason,
            source_kind,
        } = self.classification();
        let (recovery, retryable) = match source_kind {
            Some("resource_exhausted")
                if matches!(
                    self,
                    Self::OutputLimit {
                        reason: "preview_schema_limit_exceeded"
                    }
                ) =>
            {
                ("change_request", Some(false))
            }
            Some("resource_exhausted") => ("adjust_budget", Some(false)),
            Some("unauthenticated") => ("authorize", Some(false)),
            Some("permission_denied") => ("request_access", Some(false)),
            Some("timeout" | "connection_reset" | "connection_aborted") => ("retry", Some(true)),
            Some("unsupported") => ("change_request", Some(false)),
            _ if phase == "configuration"
                || matches!(code, "invalid_projection" | "unsupported_predicate") =>
            {
                ("fix_input", Some(false))
            }
            _ if code == "unsupported_protocol" => ("change_request", Some(false)),
            _ => ("unknown", None),
        };
        let mut record = json!({
            "format_version": diagnostics::DIAGNOSTIC_FORMAT_VERSION,
            "kind": "diagnostic",
            "terminal": true,
            "phase": phase,
            "code": code,
            "reason": reason,
            "source_kind": source_kind,
            "retryable": retryable,
            "recovery": recovery,
        });
        match self {
            Self::CredentialFileValidation { source, .. } => record = source.to_json_diagnostic(),
            Self::QueryWithResolvedBindings { source, bindings } => {
                record = source.to_json_diagnostic();
                record["bindings"] = bindings.clone().into();
            }
            Self::ManifestAuthorizationInput { source, field_path } => {
                record = source.to_json_diagnostic();
                record["argument"] = "--tables-file".into();
                record["field_path"] = field_path.clone().into();
            }
            Self::UnityCatalog {
                source: source @ unity_catalog::Error::DatabricksCliNotFound,
            } => {
                record["hint"] = source.to_string().into();
            }
            Self::Cancelled { .. }
            | Self::UnityCatalog {
                source: unity_catalog::Error::Cancelled { .. },
            } => {
                record["hint"] = "Discard stdout. Retry only if intended, preserving the request, pinned snapshots, authorization and limits.".into();
            }
            Self::Argument { kind, .. } => {
                record["argument_kind"] = format!("{kind:?}").into();
                record["hint"] = diagnostics::argument_error_hint(*kind).into();
            }
            Self::InputJson { source, .. } => {
                record["json_kind"] = format!("{:?}", source.classify()).into();
                record["line"] = source.line().into();
                record["column"] = source.column().into();
            }
            Self::EnvFileInput {
                line: Some(line), ..
            } => {
                record["line"] = (*line).into();
            }
            Self::InputIo { source, .. }
            | Self::Runtime { source, .. }
            | Self::Output { source } => {
                record["io_kind"] = format!("{:?}", source.kind()).into();
                record["os_error"] = json!(source.raw_os_error());
                if matches!(self, Self::Output { .. }) {
                    record["hint"] = "Discard stdout. Restore the output consumer before retrying the same request and limits.".into();
                } else if reason == "discovery_deadline_exceeded" {
                    record["hint"] = "Discard stdout. Drain stdout and stderr before retrying the same discovery request.".into();
                }
            }
            _ => {}
        }
        if let Some(correction) = self.input_correction() {
            record["argument"] = correction.argument.into();
            record["expected"] = correction.expected.into();
            record["hint"] = correction.hint.into();
            if let Some(supplied) = correction.supplied {
                record["supplied"] = supplied.into_string().into();
            }
            if let Some(path) = correction.field_path {
                record["field_path"] = path.into();
            }
        }
        record
    }

    fn exit_code(&self) -> ExitCode {
        ExitCode::from(match self {
            Self::ManifestAuthorizationInput { source, .. }
            | Self::CredentialFileValidation { source, .. }
            | Self::QueryWithResolvedBindings { source, .. } => return source.exit_code(),
            Self::Argument { .. }
            | Self::TerminalOutput
            | Self::InputJson { .. }
            | Self::InputValidation { .. }
            | Self::EnvFileInput { .. }
            | Self::InputIo { .. }
            | Self::StorageAuthorization { .. }
            | Self::Configuration { .. } => 2,
            Self::Runtime { .. }
            | Self::Reader { .. }
            | Self::ArrowIpc { .. }
            | Self::JsonOutput { .. }
            | Self::UnsupportedJsonType { .. } => 1,
            Self::Output { .. } => 3,
            Self::UnityCatalog { source } => source.exit_status(),
            Self::Query { source } => source.exit_status(),
            Self::OutputLimit { .. } | Self::DeadlineExceeded => 1,
            Self::Cancelled { status } => *status,
        })
    }
}

fn command_name() -> &'static str {
    let executable = std::env::args_os().next().unwrap_or_default();
    match Path::new(&executable).file_name() {
        Some(name) if name == "delta-arrow-reader" || name == "delta-arrow-reader.exe" => {
            "delta-arrow-reader"
        }
        _ => "dar",
    }
}

fn parse_unsigned_decimal<T: FromStr>(value: &str) -> Result<T, Error> {
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

fn encode_inspection_json(version: u64, schema: &Schema) -> Vec<u8> {
    // Arrow Schema's Serde representation contains only JSON-compatible types.
    // Keep upstream serialization, including metadata and nested field order.
    let mut output = json!({
        "format_version": 1,
        "table_version": version.to_string(),
        "schema": schema,
    })
    .to_string()
    .into_bytes();
    output.push(b'\n');
    output
}

#[derive(Default)]
struct StdoutStats {
    rows_written: u64,
    batches_written: u64,
    bytes_written: u64,
}

fn build_completion_record(
    command: &'static str,
    table_version: Option<u64>,
    stdout_stats: StdoutStats,
    row_limit: Option<usize>,
) -> Value {
    let limit_reached = row_limit.is_some_and(|limit| stdout_stats.rows_written >= limit as u64);
    let mut record = json!({
        "format_version": 1,
        "kind": "completion",
        "terminal": true,
        "command": command,
        "completion": "complete",
        "scope": if row_limit.is_some() { "limited_scan" } else { command },
        "stop_reason": if limit_reached { "row_limit" } else { "end_of_input" },
        "has_more": if limit_reached { None } else { Some(false) },
        "bindings": table_version.map(|version| json!({"alias": "t", "table_version": version.to_string()})).into_iter().collect::<Vec<_>>(),
        "output_bytes": stdout_stats.bytes_written.to_string(),
    });
    if matches!(command, "scan" | "query") {
        record["rows"] = stdout_stats.rows_written.to_string().into();
        record["batches"] = stdout_stats.batches_written.to_string().into();
    }
    record
}

fn build_json_read_completion(
    command: &'static str,
    bindings: Vec<Value>,
    stdout_stats: StdoutStats,
) -> Value {
    json!({
        "format_version": 2,
        "kind": "completion",
        "command": command,
        "bindings": bindings,
        "rows": stdout_stats.rows_written.to_string(),
        "output_bytes": stdout_stats.bytes_written.to_string(),
    })
}

async fn execute_table_command(
    command: Command,
    report_uc_cancellation: &AtomicBool,
) -> Result<Value, Error> {
    let arrow_stdout = match &command {
        Command::Scan { .. } => true,
        Command::Query(args) => args.output_format == query::QueryOutputFormat::Arrow,
        _ => false,
    };
    if arrow_stdout {
        ensure!(!io::stdout().is_terminal(), TerminalOutputSnafu);
    }
    if let Command::Inspect { inspection, .. } = &command {
        inspection.validate_view_options()?;
    }
    let uc_target = match &command {
        Command::Capabilities(_) => {
            unreachable!("main handles discovery and its terminal reporting separately")
        }
        Command::Inspect {
            table,
            databricks_profile,
            ..
        } => unity_catalog::UnityCatalogTableTarget::parse(
            &table.table,
            databricks_profile.as_deref(),
        )
        .context(UnityCatalogSnafu)?,
        Command::Scan { read, .. } | Command::Preview(preview::PreviewArgs { read, .. }) => {
            unity_catalog::UnityCatalogTableTarget::parse(
                &read.table.table,
                read.databricks_profile.as_deref(),
            )
            .context(UnityCatalogSnafu)?
        }
        Command::Query(_) => None,
    };
    report_uc_cancellation.store(
        uc_target.is_some() && !matches!(command, Command::Preview(_)),
        Ordering::Relaxed,
    );
    let row_read = match &command {
        Command::Scan { read, .. } | Command::Preview(preview::PreviewArgs { read, .. }) => {
            Some(read)
        }
        _ => None,
    };
    let predicate = match row_read.and_then(|read| read.predicate_file.as_deref()) {
        Some(path) => Some(
            input::read_json_file::<input::PredicateInput>(path, JsonInputKind::Predicate)?
                .into_predicate()?,
        ),
        _ => None,
    };
    let execution_options = match row_read.and_then(|read| read.execution_options_file.as_deref()) {
        Some(path) => {
            let options = input::read_json_file::<input::JsonObject<input::ExecutionOptionsInput>>(
                path,
                JsonInputKind::ExecutionOptions,
            )?
            .0;
            Some(
                options
                    .into_execution_options()
                    .context(ConfigurationSnafu)?,
            )
        }
        _ => None,
    };
    let command_output: LocalBoxFuture<'_, Result<Value, Error>> = match &command {
        Command::Capabilities(_) => unreachable!("capabilities returned before table loading"),
        Command::Query(args) => {
            let (query_uses_unity_catalog, execution) = args.prepare_execution()?;
            report_uc_cancellation.store(query_uses_unity_catalog, Ordering::Relaxed);
            execution
        }
        Command::Inspect {
            table, inspection, ..
        } => async move {
            let (table, _authorized_access) = table.load_table(uc_target).await?;
            let version = table.version();
            let output = inspection.encode_json(&table)?;
            let stdout_stats = StdoutStats {
                bytes_written: output.len() as u64,
                ..Default::default()
            };
            write_stdout(&output)?;
            Ok(build_completion_record(
                "inspect",
                Some(version),
                stdout_stats,
                None,
            ))
        }
        .boxed_local(),
        Command::Scan { read, .. } | Command::Preview(preview::PreviewArgs { read, .. }) => async {
            let (table, authorized_access) = read.table.load_table(uc_target).await?;
            let version = table.version();
            let limit = match &command {
                Command::Scan { limit, .. } => *limit,
                Command::Preview(args) => Some(args.limit),
                _ => unreachable!("row reader command"),
            };
            let mut builder = table.scan();
            if read.no_columns || !read.columns.is_empty() {
                builder = builder.with_projection(read.columns.iter().cloned());
            }
            if let Some(limit) = limit {
                builder = builder.with_limit(limit);
            }
            if let Some(predicate) = predicate {
                builder = builder.with_predicate(predicate);
            }
            if let Some(execution_options) = execution_options {
                builder = builder.with_execution_options(execution_options);
            }
            if let Some(target_partitions) = read.target_partitions {
                builder = builder
                    .with_target_partitions(target_partitions.get())
                    .context(ConfigurationSnafu)?;
            }
            let scan_result = async {
                let scan = builder.build().await.context(ReaderSnafu)?;
                let schema = scan.schema();
                let stream = scan
                    .into_stream()
                    .map_err(|source| Error::Reader { source });
                if let Command::Preview(args) = &command {
                    let stats = output::json::write_preview_to_stdout(
                        schema,
                        stream,
                        version,
                        output::OutputLimits {
                            max_rows: args.limit as u64,
                            max_bytes: args.max_output_bytes.get() as u64,
                        },
                    )
                    .await?;
                    Ok(build_json_read_completion(
                        "preview",
                        vec![json!({"alias": "t", "table_version": version.to_string()})],
                        stats,
                    ))
                } else {
                    let stats =
                        output::arrow::write_arrow_stream_to_stdout(schema, stream, None).await?;
                    Ok(build_completion_record("scan", Some(version), stats, limit))
                }
            }
            .await;
            match (scan_result, &authorized_access) {
                (Err(error @ Error::Reader { .. }), Some(access)) => {
                    return Err(access.map_read_error(error).await);
                }
                (result, _) => result,
            }
        }
        .boxed_local(),
    };
    command_output.await
}

fn write_stdout(output: &[u8]) -> Result<(), Error> {
    let mut stdout = io::stdout().lock();
    stdout
        .write_all(output)
        .and_then(|()| stdout.flush())
        .context(OutputSnafu)
}

fn write_help_or_version(error: clap::Error) -> Result<Value, Error> {
    let output = error.to_string();
    write_stdout(output.as_bytes())?;
    Ok(build_completion_record(
        if error.kind() == ErrorKind::DisplayHelp {
            "help"
        } else {
            "version"
        },
        None,
        StdoutStats {
            bytes_written: output.len() as u64,
            ..Default::default()
        },
        None,
    ))
}

fn install_panic_hook(diagnostic_format: DiagnosticFormat) {
    // Kernel workers can panic without waking their caller. Bound the report and
    // exit without unwinding into a blocking join; never format the payload.
    std::panic::set_hook(Box::new(move |_| {
        diagnostic_format.report_failure_with_timeout(Error::Runtime {
            source: io::Error::other("process panicked"),
            reason: "process_panicked",
        });
        std::process::exit(1);
    }));
}

fn main() -> ExitCode {
    let diagnostic_format = DiagnosticFormat::from_cli_args();
    install_panic_hook(diagnostic_format);
    #[cfg(target_os = "linux")]
    if let Err(error) = signals::install_container_signal_handlers().context(RuntimeSnafu {
        reason: "signal_handler_install_failed",
    }) {
        return diagnostic_format.report_failure_with_timeout(error);
    }
    match Cli::try_parse() {
        Ok(Cli {
            command: Command::Capabilities(options),
            ..
        }) => capabilities::run_capabilities(options, diagnostic_format),
        Ok(cli) => command_runtime::run_table_command(cli.command, diagnostic_format),
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
            diagnostic_format.report_failure_with_timeout(Error::Argument {
                kind: error.kind(),
                correction: InputCorrection::from_clap(&error),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::{DataType, Field, TimeUnit};
    use serde_json::Value;

    use super::*;

    #[test]
    fn deadline_defaults_and_explicit_values_preserve_legacy_commands() {
        for (args, expected) in [
            (vec!["inspect", "table"], None),
            (vec!["inspect", "table", "--view", "full"], None),
            (vec!["inspect", "table", "--view", "summary"], Some(30000)),
            (vec!["scan", "table"], None),
            (vec!["preview", "table"], Some(30000)),
            (
                vec!["query", "table", "--sql-file", "query.sql"],
                Some(300000),
            ),
        ] {
            for explicit in [None, Some("1234"), Some("0"), Some("-1")] {
                let mut argv = vec!["dar"];
                argv.extend(args.iter().copied());
                if let Some(value) = explicit {
                    argv.extend(["--deadline-ms", value]);
                }
                let parsed = Cli::try_parse_from(argv);
                if matches!(explicit, Some("0" | "-1")) {
                    assert!(parsed.is_err());
                    continue;
                }
                let deadline = match parsed.unwrap().command {
                    Command::Inspect { deadline_ms, .. } | Command::Scan { deadline_ms, .. } => {
                        deadline_ms
                    }
                    Command::Preview(args) => Some(args.deadline_ms),
                    Command::Query(args) => Some(args.deadline_ms),
                    Command::Capabilities(_) => unreachable!(),
                };
                assert_eq!(
                    deadline.map(NonZeroUsize::get),
                    if explicit.is_some() {
                        Some(1234)
                    } else {
                        expected
                    }
                );
            }
        }
    }

    #[test]
    fn uc_cancellation_is_informational_and_keeps_signal_exit_status() {
        for status in [130, 143] {
            let error = Error::UnityCatalog {
                source: unity_catalog::Error::Cancelled { status },
            };
            assert_eq!(error.exit_code(), ExitCode::from(status));
            assert!(
                diagnostics::capture_tracing_output("warn", || error.emit_tracing_diagnostic())
                    .is_empty()
            );
            let text =
                diagnostics::capture_tracing_output("info", || error.emit_tracing_diagnostic());
            assert_eq!(text.lines().count(), 1, "{text}");
            for field in [
                "INFO delta_arrow_reader_cli",
                "event=\"cli.cancelled\"",
                "outcome=\"cancelled\"",
                "error_code=\"uc_cancelled\"",
            ] {
                assert!(text.contains(field), "{text}");
            }
            assert!(!text.contains("ERROR"), "{text}");
        }
    }

    #[test]
    fn inspection_format_preserves_nested_schema_and_all_metadata() {
        let schema = Schema::new_with_metadata(
            vec![
                Field::new(
                    "profile",
                    DataType::Struct(
                        vec![
                            Field::new("amount", DataType::Decimal128(20, 4), false).with_metadata(
                                HashMap::from([("comment".into(), "secret user metadata".into())]),
                            ),
                        ]
                        .into(),
                    ),
                    true,
                ),
                Field::new(
                    "times",
                    DataType::List(Arc::new(Field::new(
                        "element",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                        false,
                    ))),
                    false,
                ),
            ],
            HashMap::from([("owner".into(), "table author".into())]),
        );
        let encoded = encode_inspection_json(u64::MAX, &schema);
        assert_eq!(encoded.last(), Some(&b'\n'));
        assert_eq!(encoded.iter().filter(|&&byte| byte == b'\n').count(), 1);
        let actual: Value = serde_json::from_slice(&encoded).unwrap();
        let schema_document = capabilities::DiscoverySchema::Inspection.schema_document();
        jsonschema::validate(&schema_document["schema"], &actual).unwrap();
        // This literal freezes the upstream Arrow encoding within format version 1.
        assert_eq!(
            actual,
            json!({
                "format_version": 1, "table_version": "18446744073709551615",
                "schema": {"fields": [
                    {"name": "profile", "nullable": true, "dict_id": 0, "dict_is_ordered": false, "metadata": {},
                     "data_type": {"Struct": [
                         {"name": "amount", "data_type": {"Decimal128": [20, 4]}, "nullable": false,
                          "dict_id": 0, "dict_is_ordered": false, "metadata": {"comment": "secret user metadata"}}
                     ]}},
                    {"name": "times", "nullable": false, "dict_id": 0, "dict_is_ordered": false, "metadata": {},
                     "data_type": {"List": {
                         "name": "element", "data_type": {"Timestamp": ["Microsecond", "UTC"]}, "nullable": false,
                         "dict_id": 0, "dict_is_ordered": false, "metadata": {}
                     }}}
                ], "metadata": {"owner": "table author"}}
            })
        );
        let decoded: Schema = serde_json::from_value(actual["schema"].clone()).unwrap();
        assert_eq!(decoded, schema);
    }

    #[test]
    fn table_version_accepts_full_unsigned_range() {
        for text in ["0", "000", "18446744073709551615"] {
            let cli =
                Cli::try_parse_from(["dar", "inspect", "--table-version", text, "table"]).unwrap();
            assert!(
                matches!(cli.command, Command::Inspect { table: TableArgs {options: table_input::TableOptions { table_version: Some(version), .. }, ..}, .. } if version == text.parse::<u64>().unwrap())
            );
        }
    }

    #[test]
    fn scan_arguments_preserve_column_names_and_full_limit_range() {
        for limit in ["0".to_owned(), "0001".to_owned(), usize::MAX.to_string()] {
            let cli = Cli::try_parse_from([
                "dar", "scan", "--column", "a,b", "--column", "a.b", "--column", "a,b", "--column",
                "", "--limit", &limit, "table",
            ])
            .unwrap();
            let Command::Scan {
                read:
                    RowReadArgs {
                        columns,
                        no_columns,
                        ..
                    },
                limit: parsed_limit,
                ..
            } = cli.command
            else {
                panic!("expected scan command");
            };
            assert_eq!(columns, ["a,b", "a.b", "a,b", ""]);
            assert!(!no_columns);
            assert_eq!(parsed_limit, Some(limit.parse::<usize>().unwrap()));
        }
    }

    #[test]
    fn partition_targets_accept_positive_usize_values_and_preserve_omission() {
        for text in [
            None,
            Some("1".into()),
            Some("0002".into()),
            Some(usize::MAX.to_string()),
        ] {
            let mut args = vec!["dar", "scan", "table"];
            if let Some(text) = &text {
                args.extend(["--target-partitions", text]);
            }
            let Command::Scan {
                read: RowReadArgs {
                    target_partitions, ..
                },
                ..
            } = Cli::try_parse_from(args).unwrap().command
            else {
                panic!("expected scan command");
            };
            assert_eq!(
                target_partitions.map(NonZeroUsize::get),
                text.map(|text| text.parse::<usize>().unwrap())
            );
        }
        let overflow = (usize::MAX as u128 + 1).to_string();
        for text in ["0", "000", &overflow] {
            assert!(
                Cli::try_parse_from(["dar", "scan", "--target-partitions", text, "table"]).is_err()
            );
        }
    }

    #[test]
    fn ipc_errors_distinguish_output_failures_and_redact_sources() {
        use std::error::Error as _;

        let output = Error::from(ArrowError::from(io::Error::other("secret output path")));
        assert_eq!(output.exit_code(), ExitCode::from(3));
        assert!(
            diagnostics::capture_tracing_output("warn", || output.emit_tracing_diagnostic())
                .contains("error_code=\"output_write\"")
        );
        assert!(output.source().unwrap().is::<io::Error>());
        let encoding = Error::from(ArrowError::IpcError("secret schema".into()));
        assert_eq!(encoding.exit_code(), ExitCode::from(1));
        assert!(
            diagnostics::capture_tracing_output("warn", || encoding.emit_tracing_diagnostic())
                .contains("error_code=\"arrow_ipc\"")
        );
        assert!(encoding.source().unwrap().is::<ArrowError>());
        for error in [output, encoding] {
            let text =
                diagnostics::capture_tracing_output("warn", || error.emit_tracing_diagnostic());
            assert!(text.contains("error_phase=\"execution\""));
            assert!(!text.contains("secret"));
            assert_eq!(format!("{error:?}"), error.to_string());
        }
    }

    #[test]
    fn manifest_authorization_preserves_file_access_recovery() {
        let error = Error::ManifestAuthorizationInput {
            source: Box::new(Error::InputIo {
                source: io::Error::new(io::ErrorKind::PermissionDenied, "secret path"),
                argument: "--env-file",
            }),
            field_path: "/tables/1/authorization/env_file".into(),
        };
        assert_eq!(error.exit_code(), ExitCode::from(2));
        let record = error.to_json_diagnostic();
        assert_eq!(record["code"], "input_file_io");
        assert_eq!(record["source_kind"], "permission_denied");
        assert_eq!(record["recovery"], "request_access");
        assert_eq!(record["io_kind"], "PermissionDenied");
        assert_eq!(record["argument"], "--tables-file");
        assert_eq!(record["field_path"], "/tables/1/authorization/env_file");
        assert_eq!(record["hint"], "Request read access to the input file.");
        let text = diagnostics::capture_tracing_output("warn", || error.emit_tracing_diagnostic());
        assert_eq!(text.lines().count(), 1, "{text}");
        assert!(text.contains("io_kind=PermissionDenied"), "{text}");
        assert!(
            text.contains("Request read access to the input file."),
            "{text}"
        );
        for rendered in [record.to_string(), text, format!("{error:?}")] {
            assert!(!rendered.contains("secret"), "{rendered}");
        }
    }

    #[test]
    fn cli_failures_preserve_safe_causes_without_formatting_sources() {
        let failures = [
            (
                Error::InputIo {
                    source: io::Error::new(io::ErrorKind::NotFound, "secret path"),
                    argument: "--storage-options-file",
                },
                "not_found",
                "io_kind=NotFound",
            ),
            (
                Error::InputIo {
                    source: io::Error::new(io::ErrorKind::PermissionDenied, "secret path"),
                    argument: "--predicate-file",
                },
                "permission_denied",
                "io_kind=PermissionDenied",
            ),
            (
                Error::Output {
                    source: io::Error::new(io::ErrorKind::BrokenPipe, "secret pipe"),
                },
                "broken_pipe",
                "io_kind=BrokenPipe",
            ),
            (
                Error::Output {
                    source: io::Error::new(io::ErrorKind::WriteZero, "secret output"),
                },
                "unknown",
                "io_kind=WriteZero",
            ),
            (
                Error::Runtime {
                    source: io::Error::from_raw_os_error(11),
                    reason: "output_thread_spawn_failed",
                },
                "unknown",
                "os_error=11",
            ),
            (
                Error::ArrowIpc {
                    source: ArrowError::IpcError("secret metadata".into()),
                },
                "arrow_ipc",
                "error_code=\"arrow_ipc\"",
            ),
            (
                Error::ArrowIpc {
                    source: ArrowError::SchemaError("secret schema".into()),
                },
                "schema",
                "error_code=\"arrow_ipc\"",
            ),
        ];
        for (error, source_kind, detail) in failures {
            let record = error.to_json_diagnostic();
            assert_eq!(record["source_kind"], source_kind);
            assert!(!record.to_string().contains("secret"));
            if let Error::InputIo { argument, source } = &error {
                assert_eq!(record["argument"], *argument);
                assert!(record.get("field_path").is_none());
                if source.kind() == io::ErrorKind::PermissionDenied {
                    assert_eq!(record["recovery"], "request_access");
                    assert_eq!(record["hint"], "Request read access to the input file.");
                }
            }
            for filter in ["warn", "trace"] {
                let text =
                    diagnostics::capture_tracing_output(filter, || error.emit_tracing_diagnostic());
                assert_eq!(text.lines().count(), 1, "{text}");
                assert!(
                    text.contains(&format!("error_source_kind=\"{source_kind}\"")),
                    "{text}"
                );
                assert!(text.contains(detail), "{text}");
                assert!(!text.contains("secret"), "{text}");
                assert!(!format!("{error:?}").contains("secret"));
            }
        }
    }

    #[test]
    fn failed_query_bindings_fit_the_terminal_record_limit() {
        let source = Error::OutputLimit {
            reason: "json_output_limit_exceeded",
        };
        let expected = source.to_json_diagnostic();
        let bindings: Vec<_> = (0..query::tables_manifest::MAX_TABLES)
            .map(|index| {
                json!({
                    "alias": format!("table_{index:057}"),
                    "table_version": u64::MAX.to_string(),
                })
            })
            .collect();
        let error = Error::QueryWithResolvedBindings {
            source: Box::new(source),
            bindings: bindings.clone(),
        };
        assert_eq!(error.exit_code(), ExitCode::from(1));
        let mut record = error.to_json_diagnostic();
        assert!(record.to_string().len() < 8192);
        assert_eq!(
            record.as_object_mut().unwrap().remove("bindings"),
            Some(json!(bindings))
        );
        assert_eq!(record, expected);
    }

    #[test]
    fn recovery_uses_typed_causes_without_interpreting_messages() {
        for (kind, recovery, retryable) in [
            (io::ErrorKind::TimedOut, "retry", Some(true)),
            (io::ErrorKind::ConnectionReset, "retry", Some(true)),
            (io::ErrorKind::ConnectionAborted, "retry", Some(true)),
            (
                io::ErrorKind::PermissionDenied,
                "request_access",
                Some(false),
            ),
            (io::ErrorKind::Other, "unknown", None),
        ] {
            let record = Error::Runtime {
                source: io::Error::new(kind, "secret: unauthorized, timeout, permission denied"),
                reason: "runtime_build_failed",
            }
            .to_json_diagnostic();
            assert_eq!(record["recovery"], recovery);
            assert_eq!(record["retryable"], json!(retryable));
        }
        let record = Error::ArrowIpc {
            source: ArrowError::NotYetImplemented("secret unsupported schema".into()),
        }
        .to_json_diagnostic();
        assert_eq!(record["recovery"], "change_request");
        assert_eq!(record["retryable"], false);

        let record = Error::UnsupportedJsonType { command: "preview" }.to_json_diagnostic();
        assert_eq!(record["recovery"], "change_request");
        assert_eq!(record["argument"], "--column");
        assert!(record["hint"].as_str().unwrap().contains("Use scan"));

        for (source, source_kind, recovery) in [
            (
                object_store::Error::Unauthenticated {
                    path: "secret-path".into(),
                    source: Box::new(io::Error::other("secret-token: timeout, permission denied")),
                },
                "unauthenticated",
                "authorize",
            ),
            (
                object_store::Error::PermissionDenied {
                    path: "secret-path".into(),
                    source: Box::new(io::Error::other("secret-token: unauthorized, timeout")),
                },
                "permission_denied",
                "request_access",
            ),
        ] {
            let error = Error::ArrowIpc {
                source: ArrowError::ExternalError(Box::new(source)),
            };
            let record = error.to_json_diagnostic();
            assert_eq!(record["source_kind"], source_kind);
            assert_eq!(record["recovery"], recovery);
            assert_eq!(record["retryable"], false);
            assert!(!record.to_string().contains("secret"));
            assert!(
                !diagnostics::capture_tracing_output("trace", || error.emit_tracing_diagnostic())
                    .contains("secret")
            );
        }
    }

    #[test]
    fn storage_options_preserve_strings() {
        let options: input::StorageOptionsInput = serde_json::from_str(
            r#"{"AWS_ACCESS_KEY_ID":" secret, value ","MixedCase":"unchanged"}"#,
        )
        .unwrap();
        assert_eq!(options.0["AWS_ACCESS_KEY_ID"], " secret, value ");
        assert_eq!(options.0["MixedCase"], "unchanged");
        assert_eq!(options.0.len(), 2);
    }

    #[test]
    fn json_input_keeps_the_source_but_formats_only_safe_context() {
        use std::error::Error as _;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("secret-input.json");
        std::fs::write(&path, r#"{"secret-field":0}"#).unwrap();
        let error = input::read_json_file::<input::JsonObject<input::ExecutionOptionsInput>>(
            path.to_str().unwrap(),
            JsonInputKind::ExecutionOptions,
        )
        .err()
        .unwrap();
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<serde_json::Error>()
            .unwrap();
        assert_eq!(source.classify(), serde_json::error::Category::Data);
        assert!(source.to_string().contains("secret-field"));
        assert!(error.to_string().contains("kind=Data"));
        assert!(
            !diagnostics::capture_tracing_output("trace", || error.emit_tracing_diagnostic())
                .contains("secret")
        );
        assert!(!format!("{error:?}").contains("secret"));

        std::fs::write(&path, b" \n !secret").unwrap();
        let error = input::read_json_file::<input::StorageOptionsInput>(
            path.to_str().unwrap(),
            JsonInputKind::StorageOptions,
        )
        .err()
        .unwrap();
        assert_eq!(
            error.to_string(),
            "Invalid JSON input (kind=Syntax, line=2, column=2)."
        );
        assert_eq!(error.exit_code(), ExitCode::from(2));
    }

    #[test]
    fn snafu_retains_io_sources_without_exposing_them_in_diagnostics() {
        use std::error::Error as _;

        let error = Err::<(), _>(io::Error::other("secret input path"))
            .context(InputIoSnafu {
                argument: "--storage-options-file",
            })
            .unwrap_err();
        assert!(error.source().unwrap().is::<io::Error>());
        let text = diagnostics::capture_tracing_output("warn", || error.emit_tracing_diagnostic());
        assert!(text.contains("error_code=\"input_file_io\""));
        assert!(!text.contains("secret"));
        assert_eq!(format!("{error:?}"), error.to_string());
    }
}
