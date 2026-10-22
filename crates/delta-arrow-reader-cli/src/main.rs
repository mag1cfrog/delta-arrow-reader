//! CLI commands for inspecting Delta snapshots and exporting their rows.
//!
//! `inspect` returns a snapshot's version, read checks, or column types as JSON.
//! `scan` and `query` return rows as Arrow IPC streams; `query` also supports typed
//! JSON. Results use stdout; diagnostics and completion records use stderr.
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
mod diagnostics;
mod direct_credentials;
mod input;
mod inspection;
mod output;
mod query;
mod signals;
mod stdout_monitor;
mod unity_catalog;

use std::{
    fmt,
    io::{self, IsTerminal, Write},
    num::NonZeroUsize,
    path::Path,
    process::ExitCode,
    str::FromStr,
};

use arrow_schema::{ArrowError, Schema};
use clap::{Args, Parser, Subcommand, error::ErrorKind};
use delta_arrow_reader::{
    DeltaReaderError, DeltaSnapshotSelection, DeltaTableBuilder, WarmupMode, error_source_kind,
};
use diagnostics::{DiagnosticFormat, ErrorClassification, InputCorrection};
use futures_util::{FutureExt, TryStreamExt, future::LocalBoxFuture};
use input::JsonInputKind;
use serde_json::{Value, json};
use snafu::{ResultExt, Snafu, ensure};

/// Read Delta Lake snapshots as JSON or Arrow IPC streams.
#[derive(Parser)]
#[command(
    name = command_name(), bin_name = command_name(), version,
    after_help = "When version or column types are unknown, use inspect TABLE --view summary [--column NAME]; pin reads with --table-version N.\nGuides: capabilities --command NAME; capabilities --schema NAME [--detail FIELD_OR_TYPE].\nInput formats: predicate, storage-options, execution-options. Output formats: rows, inspection-summary, inspection, diagnostic, completion."
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
    },
    /// Run one read-only SQL query over a Delta snapshot, registered as t.
    ///
    /// Arrow IPC requires redirected stdout. Use --format json for a small typed result.
    Query(query::QueryArgs),
    /// Stream a Delta snapshot's rows as Arrow IPC to stdout.
    ///
    /// Redirect stdout to a file or pipe; terminal output is rejected.
    Scan {
        #[command(flatten)]
        table: TableArgs,
        /// Existing Databricks CLI profile; required for uc://catalog.schema.table targets.
        #[arg(long, value_name = "PROFILE")]
        databricks_profile: Option<String>,
        /// Logical column name; repeat to select columns in argument order.
        /// Default: all columns.
        #[arg(long = "column", value_name = "NAME")]
        columns: Vec<String>,
        /// Read zero columns while preserving row counts.
        #[arg(long, conflicts_with = "columns")]
        no_columns: bool,
        /// Maximum output rows as unsigned decimal digits; zero is valid.
        #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
        limit: Option<usize>,
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
    },
}

/// Table location, snapshot selection, and storage configuration shared by all commands.
#[derive(Args)]
struct TableArgs {
    /// UTF-8 local path or storage URL.
    /// Relative paths use the working directory.
    /// Use '--' before a table path beginning with '-'.
    table: String,
    /// Unsigned decimal snapshot version; default: latest.
    #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<u64>)]
    table_version: Option<u64>,
    /// Local JSON object of string keys and string values.
    /// At most 1 MiB; '-' is a filename, not stdin.
    #[arg(long, value_name = "PATH")]
    storage_options_file: Option<String>,
    /// Explicit S3, Azure Blob, or GCS credential source. Without it, existing storage behavior applies.
    #[arg(long, value_enum)]
    auth_source: Option<direct_credentials::CredentialSource>,
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
    #[snafu(display("Could not read the input file."))]
    InputIo {
        source: io::Error,
        argument: &'static str,
    },
    #[snafu(display("Invalid storage authorization (reason={reason})."))]
    StorageAuthorization { reason: &'static str },
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
    #[snafu(display("The result contains a type unsupported by typed JSON. Use --format arrow."))]
    UnsupportedJsonType,
    #[snafu(display("{source}"))]
    Reader { source: DeltaReaderError },
    #[snafu(display("{source}"))]
    Configuration { source: DeltaReaderError },
    #[snafu(display("{source}"))]
    UnityCatalog { source: unity_catalog::Error },
    #[snafu(display("{source}"))]
    Query { source: query::QueryError },
    #[snafu(display("Output exceeded its resource ceiling (reason={reason})."))]
    OutputLimit { reason: &'static str },
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
            Self::UnsupportedJsonType => ErrorClassification {
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
            Self::Argument { correction, .. } => return correction.clone(),
            Self::InputValidation { correction, .. } => return Some(correction.clone()),
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
                    argument: "--format".to_owned(),
                    expected: "arrow for this result size".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Use --format arrow with redirected stdout; keep the same SQL, authorization, and --table-version N.",
                });
            }
            Self::UnsupportedJsonType => {
                return Some(InputCorrection {
                    argument: "--format".to_owned(),
                    expected: "arrow for this result type".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Use --format arrow with redirected stdout; keep the same SQL, authorization, and --table-version N.",
                });
            }
            _ => return None,
        };
        Some(input_kind.correction(hint))
    }

    fn log_failure(&self) {
        if let Self::Cancelled { status } = self {
            tracing::info!(target: "delta_arrow_reader_cli",
                event = "cli.cancelled", operation = "cli_command", outcome = "cancelled",
                error_phase = "execution", error_code = "command_cancelled", exit_status = status,
                "CLI command cancelled"
            );
            return;
        }
        if let Self::UnityCatalog {
            source: unity_catalog::Error::Cancelled { status },
        } = self
        {
            tracing::info!(target: "delta_arrow_reader_cli",
                event = "cli.cancelled", operation = "cli_command", outcome = "cancelled",
                error_phase = "execution", error_code = "uc_cancelled", exit_status = status,
                "Unity Catalog command cancelled"
            );
            return;
        }
        let correction = self.input_correction();
        let argument = correction.as_ref().map(|value| value.argument.as_str());
        let expected = correction.as_ref().map(|value| value.expected.as_str());
        let supplied = correction
            .as_ref()
            .and_then(|value| value.supplied.as_deref());
        let field_path = correction
            .as_ref()
            .and_then(|value| value.field_path.as_deref());
        let hint = match self {
            Self::Argument { kind, .. } => Some(diagnostics::argument_error_hint(*kind)),
            _ => correction.as_ref().map(|value| value.hint),
        };
        let ErrorClassification {
            phase,
            code,
            reason,
            source_kind,
        } = self.classification();
        // Core owns the terminal failure; CLI guidance supplements that event.
        if matches!(self, Self::Reader { .. }) {
            if correction.is_some() {
                tracing::error!(target: "delta_arrow_reader_cli",
                    event = "cli.input_correction", error_phase = phase, error_code = code,
                    error_reason = reason, argument, expected, supplied, hint, field_path,
                    "Correct the command input"
                );
            }
            return;
        }
        let argument_kind = match self {
            Self::Argument { kind, .. } => Some(tracing::field::debug(kind)),
            _ => None,
        };
        let (json_kind, json_line, json_column) = match self {
            Self::InputJson { source, .. } => (
                Some(tracing::field::debug(source.classify())),
                Some(source.line()),
                Some(source.column()),
            ),
            _ => (None, None, None),
        };
        let (io_kind, os_error) = match self {
            Self::InputIo { source, .. }
            | Self::Runtime { source, .. }
            | Self::Output { source } => (
                Some(tracing::field::debug(source.kind())),
                source.raw_os_error(),
            ),
            _ => (None, None),
        };
        let message: &dyn fmt::Display = match self {
            Self::UnityCatalog { source } => source,
            Self::Query { source } => source,
            Self::StorageAuthorization { .. } => self,
            _ => &"CLI command failed",
        };
        tracing::error!(target: "delta_arrow_reader_cli",
            event = "cli.failed", operation = "cli_command", outcome = "failed",
            error_phase = phase, error_code = code, error_reason = reason,
            error_source_kind = source_kind, argument_kind, argument, expected, supplied, hint, field_path,
            json_kind, json_line, json_column, io_kind, os_error,
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
                        reason: "json_output_limit_exceeded"
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
            Self::UnityCatalog {
                source: source @ unity_catalog::Error::DatabricksCliNotFound,
            } => {
                record["hint"] = source.to_string().into();
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
            Self::InputIo { source, .. }
            | Self::Runtime { source, .. }
            | Self::Output { source } => {
                record["io_kind"] = format!("{:?}", source.kind()).into();
                record["os_error"] = json!(source.raw_os_error());
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
            Self::Argument { .. }
            | Self::TerminalOutput
            | Self::InputJson { .. }
            | Self::InputValidation { .. }
            | Self::InputIo { .. }
            | Self::StorageAuthorization { .. }
            | Self::Configuration { .. } => 2,
            Self::Runtime { .. }
            | Self::Reader { .. }
            | Self::ArrowIpc { .. }
            | Self::JsonOutput { .. }
            | Self::UnsupportedJsonType => 1,
            Self::Output { .. } => 3,
            Self::UnityCatalog { source } => source.exit_status(),
            Self::Query { source } => source.exit_status(),
            Self::OutputLimit { .. } => 1,
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

fn run_table_command(command: Command) -> Result<Value, Error> {
    let arrow_stdout = match &command {
        Command::Scan { .. } => true,
        Command::Query(args) => args.output_format == query::QueryOutputFormat::Arrow,
        _ => false,
    };
    if arrow_stdout {
        ensure!(!io::stdout().is_terminal(), TerminalOutputSnafu);
    }
    let (table_args, databricks_profile) = match &command {
        Command::Capabilities(_) => {
            unreachable!("main handles discovery and its terminal reporting separately")
        }
        Command::Inspect {
            table,
            databricks_profile,
            ..
        }
        | Command::Scan {
            table,
            databricks_profile,
            ..
        } => (table, databricks_profile.as_deref()),
        Command::Query(args) => {
            if args
                .table
                .table
                .get(..5)
                .is_some_and(|scheme| scheme.eq_ignore_ascii_case("uc://"))
            {
                return Err(query::QueryError::UnsupportedUnityCatalogQuery).context(QuerySnafu);
            }
            (&args.table, None)
        }
    };
    if let Command::Inspect { inspection, .. } = &command {
        inspection.validate()?;
    }
    let uc_target =
        unity_catalog::UnityCatalogTableTarget::parse(&table_args.table, databricks_profile)
            .context(UnityCatalogSnafu)?;
    ensure!(
        uc_target.is_none() || table_args.auth_source.is_none(),
        StorageAuthorizationSnafu {
            reason: "auth_source_conflicts_with_unity_catalog"
        }
    );
    ensure!(
        !matches!(
            table_args.auth_source,
            Some(direct_credentials::CredentialSource::Json)
        ) || table_args.storage_options_file.is_some(),
        StorageAuthorizationSnafu {
            reason: "auth_source_requires_storage_options_file"
        }
    );
    let storage_options = match &table_args.storage_options_file {
        Some(path) => {
            input::read_json_file::<input::StorageOptionsInput>(
                path,
                JsonInputKind::StorageOptions,
            )?
            .0
        }
        None => Default::default(),
    };
    let predicate = match &command {
        Command::Scan {
            predicate_file: Some(path),
            ..
        } => Some(
            input::read_json_file::<input::PredicateInput>(path, JsonInputKind::Predicate)?
                .into_predicate()?,
        ),
        _ => None,
    };
    let execution_options = match &command {
        Command::Scan {
            execution_options_file: Some(path),
            ..
        } => {
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
    let load_table = async {
        let snapshot_selection = table_args.table_version.map_or(
            DeltaSnapshotSelection::Latest,
            DeltaSnapshotSelection::Version,
        );
        let mut authorized_access = None;
        let table = match uc_target {
            Some(target) => {
                let access = target
                    .request_read_access(storage_options)
                    .await
                    .context(UnityCatalogSnafu)?;
                let table = access.load_table(snapshot_selection).await?;
                authorized_access = Some(access);
                table
            }
            None => {
                // Each command plans its own reads after the snapshot is loaded.
                let builder = DeltaTableBuilder::new(&table_args.table)
                    .with_warmup(WarmupMode::None)
                    .with_snapshot_selection(snapshot_selection);
                let builder = match table_args.auth_source {
                    Some(source) => source.configure_table_credentials(
                        builder,
                        &table_args.table,
                        storage_options,
                    )?,
                    None => builder.with_storage_options(storage_options),
                };
                builder.load_table().await.context(ReaderSnafu)?
            }
        };
        Ok::<_, Error>((table, authorized_access))
    };
    let work: LocalBoxFuture<'_, Result<Value, Error>> = match &command {
        Command::Capabilities(_) => unreachable!("capabilities returned before table loading"),
        Command::Query(args) => {
            let sql_query = query::ReadOnlySqlQuery::read_file(&args.sql_file)?;
            async move {
                let (table, _) = load_table.await?;
                let version = table.version();
                let stream = sql_query
                    .execute_stream(table, args.memory_limit_bytes.get())
                    .await?;
                let schema = stream.schema();
                let stream = stream.map_err(|source| Error::Query {
                    source: query::QueryError::Execution { source },
                });
                let limits = output::OutputLimits {
                    max_rows: args.max_rows as u64,
                    max_bytes: args.max_output_bytes.get() as u64,
                };
                let stdout_stats = match args.output_format {
                    query::QueryOutputFormat::Arrow => {
                        output::arrow::write_arrow_stream_to_stdout(schema, stream, Some(limits))
                            .await?
                    }
                    query::QueryOutputFormat::Json => {
                        output::json::write_query_to_stdout(schema, stream, version, limits).await?
                    }
                };
                let mut completion =
                    build_completion_record("query", Some(version), stdout_stats, None);
                if args.output_format == query::QueryOutputFormat::Json {
                    completion["format_version"] = json!(2);
                    if let Some(fields) = completion.as_object_mut() {
                        for name in [
                            "batches",
                            "stop_reason",
                            "has_more",
                            "scope",
                            "completion",
                            "terminal",
                        ] {
                            fields.remove(name);
                        }
                    }
                }
                Ok(completion)
            }
            .boxed_local()
        }
        Command::Inspect { inspection, .. } => async move {
            let (table, _authorized_access) = load_table.await?;
            let version = table.version();
            let output = inspection.encode(&table)?;
            let stdout_stats = StdoutStats {
                bytes_written: output.len() as u64,
                ..Default::default()
            };
            if uc_target.is_some() {
                // Keep the UC cancellation handler responsive when stdout is blocked.
                tokio::task::spawn_blocking(move || write_stdout(&output))
                    .await
                    .map_err(io::Error::other)
                    .context(RuntimeSnafu {
                        reason: "inspection_output_task_failed",
                    })?
            } else {
                write_stdout(&output)
            }?;
            Ok(build_completion_record(
                "inspect",
                Some(version),
                stdout_stats,
                None,
            ))
        }
        .boxed_local(),
        Command::Scan {
            columns,
            no_columns,
            limit,
            target_partitions,
            ..
        } => async move {
            let (table, authorized_access) = load_table.await?;
            let version = table.version();
            let mut builder = table.scan();
            if *no_columns || !columns.is_empty() {
                builder = builder.with_projection(columns.iter().cloned());
            }
            if let Some(limit) = limit {
                builder = builder.with_limit(*limit);
            }
            if let Some(predicate) = predicate {
                builder = builder.with_predicate(predicate);
            }
            if let Some(execution_options) = execution_options {
                builder = builder.with_execution_options(execution_options);
            }
            if let Some(target_partitions) = target_partitions {
                builder = builder
                    .with_target_partitions(target_partitions.get())
                    .context(ConfigurationSnafu)?;
            }
            let scan_result = async {
                let scan = builder.build().await.context(ReaderSnafu)?;
                output::arrow::write_arrow_stream_to_stdout(
                    scan.schema(),
                    scan.into_stream()
                        .map_err(|source| Error::Reader { source }),
                    None,
                )
                .await
            }
            .await;
            let stdout_stats = match (scan_result, &authorized_access) {
                (Err(Error::Reader { source }), Some(access)) => {
                    return Err(access.map_reader_error(source).await);
                }
                (result, _) => result?,
            };
            Ok(build_completion_record(
                "scan",
                Some(version),
                stdout_stats,
                *limit,
            ))
        }
        .boxed_local(),
    };
    // Drive async work on the calling thread. A dedicated Tokio worker can
    // occupy the last OS thread and leave blocking loads queued forever.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context(RuntimeSnafu {
            reason: "runtime_build_failed",
        })?;
    let result = if uc_target.is_some() || matches!(command, Command::Query(_)) {
        runtime.block_on(async {
            tokio::select! {
                // Install listeners before polling work; cancellation drops any pending child.
                biased;
                exit_status = signals::wait_for_cancellation() => {
                    let status = exit_status?;
                    if uc_target.is_some() {
                        Err(Error::UnityCatalog { source: unity_catalog::Error::Cancelled { status } })
                    } else {
                        Err(Error::Cancelled { status })
                    }
                },
                source = stdout_monitor::wait_for_disconnect(), if matches!(command, Command::Scan { .. } | Command::Query(_)) => {
                    Err(Error::Output { source })
                },
                result = work => result,
            }
        })
    } else {
        runtime.block_on(work)
    };
    // Blocking core work must not delay process shutdown after this command finishes.
    runtime.shutdown_background();
    result
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
    // Kernel workers can panic without waking their caller. Report synchronously
    // and exit without unwinding into a blocking join; never format the payload.
    std::panic::set_hook(Box::new(move |_| {
        diagnostic_format.report_failure(&Error::Runtime {
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
    let result = match Cli::try_parse() {
        Ok(Cli {
            command: Command::Capabilities(options),
            ..
        }) => {
            return capabilities::run_capabilities(options, diagnostic_format);
        }
        Ok(cli) => {
            diagnostics::init_tracing(diagnostic_format);
            run_table_command(cli.command)
        }
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            diagnostics::init_tracing(diagnostic_format);
            write_help_or_version(error)
        }
        Err(error) => {
            // Clap's diagnostics may contain argument values; report only safe fields.
            Err(Error::Argument {
                kind: error.kind(),
                correction: InputCorrection::from_clap(&error),
            })
        }
    };
    match result {
        Ok(completion_record) => {
            diagnostic_format.write_terminal_record(completion_record);
            ExitCode::SUCCESS
        }
        Err(error) => diagnostic_format.report_failure_with_timeout(error),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::{DataType, Field, TimeUnit};
    use serde_json::Value;

    use super::*;

    #[test]
    fn uc_cancellation_is_informational_and_keeps_signal_exit_status() {
        for status in [130, 143] {
            let error = Error::UnityCatalog {
                source: unity_catalog::Error::Cancelled { status },
            };
            assert_eq!(error.exit_code(), ExitCode::from(status));
            assert!(diagnostics::capture("warn", || error.log_failure()).is_empty());
            let text = diagnostics::capture("info", || error.log_failure());
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
                matches!(cli.command, Command::Inspect { table: TableArgs {table_version: Some(version), ..}, .. } if version == text.parse::<u64>().unwrap())
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
                columns,
                no_columns,
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
                target_partitions, ..
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
            diagnostics::capture("warn", || output.log_failure())
                .contains("error_code=\"output_write\"")
        );
        assert!(output.source().unwrap().is::<io::Error>());
        let encoding = Error::from(ArrowError::IpcError("secret schema".into()));
        assert_eq!(encoding.exit_code(), ExitCode::from(1));
        assert!(
            diagnostics::capture("warn", || encoding.log_failure())
                .contains("error_code=\"arrow_ipc\"")
        );
        assert!(encoding.source().unwrap().is::<ArrowError>());
        for error in [output, encoding] {
            let text = diagnostics::capture("warn", || error.log_failure());
            assert!(text.contains("error_phase=\"execution\""));
            assert!(!text.contains("secret"));
            assert_eq!(format!("{error:?}"), error.to_string());
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
                let text = diagnostics::capture(filter, || error.log_failure());
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
            assert!(!diagnostics::capture("trace", || error.log_failure()).contains("secret"));
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
        assert!(!diagnostics::capture("trace", || error.log_failure()).contains("secret"));
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
        let text = diagnostics::capture("warn", || error.log_failure());
        assert!(text.contains("error_code=\"input_file_io\""));
        assert!(!text.contains("secret"));
        assert_eq!(format!("{error:?}"), error.to_string());
    }
}
