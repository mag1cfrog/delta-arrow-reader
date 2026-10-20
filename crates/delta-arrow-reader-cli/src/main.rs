//! Read Delta snapshots as JSON metadata or Arrow IPC streams.

#![cfg_attr(test, allow(clippy::panic))]

mod diagnostics;
mod execution_options;
mod input;
mod predicate;
mod signals;
mod unity_catalog;

use std::{
    fmt,
    io::{self, IsTerminal, Write},
    num::NonZeroUsize,
    path::Path,
    process::ExitCode,
    str::FromStr,
};

use arrow::ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, Schema};
use clap::{Args, Parser, Subcommand, error::ErrorKind};
use delta_arrow_reader::{
    DeltaReaderError, DeltaScan, DeltaSnapshotSelection, DeltaTableBuilder, WarmupMode,
    error_source_kind,
};
use serde_json::json;
use snafu::{ResultExt, Snafu, ensure};

/// Read Delta Lake snapshots as JSON metadata or Arrow IPC streams.
#[derive(Parser)]
#[command(name = command_name(), bin_name = command_name(), version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print one Delta snapshot's version and full Arrow schema as JSON.
    ///
    /// Only snapshot metadata is loaded; data files are not read.
    Inspect(TableArgs),
    /// Stream a Delta snapshot's rows as Arrow IPC to stdout.
    ///
    /// Redirect stdout to a file or pipe; terminal output is rejected.
    Scan {
        #[command(flatten)]
        table: TableArgs,
        /// Logical column name; repeat to select columns in argument order.
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
        #[arg(long, value_name = "PATH")]
        predicate_file: Option<String>,
        /// Local JSON execution options; at most 1 MiB. '-' is a filename, not stdin.
        #[arg(long, value_name = "PATH")]
        execution_options_file: Option<String>,
    },
}

#[derive(Args)]
struct TableArgs {
    /// UTF-8 local path, storage URL, or uc://catalog.schema.table for inspection.
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
    /// Existing Databricks CLI profile; required for uc:// inspection.
    #[arg(long, value_name = "PROFILE")]
    databricks_profile: Option<String>,
}

#[derive(Snafu)]
enum Error {
    #[snafu(display("Invalid command-line arguments (kind={kind:?})."))]
    Argument { kind: ErrorKind },
    #[snafu(display("Arrow IPC output requires redirected stdout."))]
    TerminalOutput,
    #[snafu(display(
        "Invalid JSON input (kind={:?}, line={}, column={}).",
        source.classify(), source.line(), source.column()
    ))]
    InputJson { source: serde_json::Error },
    #[snafu(display("Invalid JSON input (reason={reason})."))]
    InputValidation { reason: &'static str },
    #[snafu(display("Could not read the input file."))]
    InputIo { source: io::Error },
    #[snafu(display("The reader runtime failed."))]
    Runtime {
        source: io::Error,
        reason: &'static str,
    },
    #[snafu(display("Could not write or flush stdout."))]
    Output { source: io::Error },
    #[snafu(display("Could not encode the Arrow IPC stream."))]
    ArrowIpc { source: ArrowError },
    #[snafu(display("{source}"))]
    Reader { source: DeltaReaderError },
    #[snafu(display("{source}"))]
    Configuration { source: DeltaReaderError },
    #[snafu(display("{source}"))]
    UnityCatalog { source: unity_catalog::Error },
}

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        match source {
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
    fn log_failure(&self) {
        if let Self::UnityCatalog {
            source: unity_catalog::Error::Cancelled { status },
        } = self
        {
            tracing::info!(target: "delta_arrow_reader_cli",
                event = "cli.cancelled", operation = "cli_command", outcome = "cancelled",
                error_phase = "execution", error_code = "uc_cancelled", exit_status = status,
                "Unity Catalog inspection cancelled"
            );
            return;
        }
        let (phase, code, reason, source_kind) = match self {
            // These API boundaries already emit their terminal core event.
            Self::Reader { .. } => return,
            Self::Configuration { source } => (
                source.phase().as_str(),
                source.code(),
                source.reason(),
                source.source_kind(),
            ),
            Self::Argument { .. } => (
                "configuration",
                "invalid_cli_argument",
                "argument_parse_failed",
                None,
            ),
            Self::TerminalOutput => (
                "configuration",
                "invalid_cli_argument",
                "stdout_is_terminal",
                None,
            ),
            Self::InputJson { .. } => (
                "configuration",
                "invalid_input_json",
                "input_json_decode_failed",
                None,
            ),
            Self::InputValidation { reason } => {
                ("configuration", "invalid_input_json", *reason, None)
            }
            Self::InputIo { source } => (
                "configuration",
                "input_file_io",
                "input_file_read_failed",
                Some(error_source_kind(source)),
            ),
            Self::Runtime { source, reason } => (
                "execution",
                "runtime_initialization",
                *reason,
                Some(error_source_kind(source)),
            ),
            Self::Output { source } => (
                "execution",
                "output_write",
                "stdout_write_failed",
                Some(error_source_kind(source)),
            ),
            Self::ArrowIpc { source } => (
                "execution",
                "arrow_ipc",
                "arrow_ipc_encode_failed",
                Some(error_source_kind(source)),
            ),
            Self::UnityCatalog { source } => (source.phase(), source.code(), source.code(), None),
        };
        let argument_kind = match self {
            Self::Argument { kind } => Some(tracing::field::debug(kind)),
            _ => None,
        };
        let (json_kind, json_line, json_column) = match self {
            Self::InputJson { source } => (
                Some(tracing::field::debug(source.classify())),
                Some(source.line()),
                Some(source.column()),
            ),
            _ => (None, None, None),
        };
        let (io_kind, os_error) = match self {
            Self::InputIo { source } | Self::Runtime { source, .. } | Self::Output { source } => (
                Some(tracing::field::debug(source.kind())),
                source.raw_os_error(),
            ),
            _ => (None, None),
        };
        let message: &dyn fmt::Display = match self {
            Self::UnityCatalog { source } => source,
            _ => &"CLI command failed",
        };
        tracing::error!(target: "delta_arrow_reader_cli",
            event = "cli.failed", operation = "cli_command", outcome = "failed",
            error_phase = phase, error_code = code, error_reason = reason,
            error_source_kind = source_kind, argument_kind, json_kind, json_line, json_column, io_kind, os_error,
            "{message}"
        );
    }

    fn exit_code(&self) -> ExitCode {
        ExitCode::from(match self {
            Self::Argument { .. }
            | Self::TerminalOutput
            | Self::InputJson { .. }
            | Self::InputValidation { .. }
            | Self::InputIo { .. }
            | Self::Configuration { .. } => 2,
            Self::Runtime { .. } | Self::Reader { .. } | Self::ArrowIpc { .. } => 1,
            Self::Output { .. } => 3,
            Self::UnityCatalog { source } => source.exit_status(),
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
            kind: ErrorKind::ValueValidation
        }
    );
    value.parse().map_err(|_| {
        ArgumentSnafu {
            kind: ErrorKind::ValueValidation,
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

fn run() -> Result<(), Error> {
    #[cfg(target_os = "linux")]
    signals::install_container_signal_handlers().context(RuntimeSnafu {
        reason: "signal_handler_install_failed",
    })?;

    let command = match Cli::try_parse() {
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            return write_stdout(error.to_string().as_bytes());
        }
        // Clap's other diagnostics may contain argument values. Emit our redacted error.
        Err(error) => return ArgumentSnafu { kind: error.kind() }.fail(),
        Ok(cli) => cli.command,
    };

    if matches!(command, Command::Scan { .. }) {
        ensure!(!io::stdout().is_terminal(), TerminalOutputSnafu);
    }
    let table_args = match &command {
        Command::Inspect(table) | Command::Scan { table, .. } => table,
    };
    let uc_target = unity_catalog::UnityCatalogTableTarget::parse(
        &table_args.table,
        table_args.databricks_profile.as_deref(),
        matches!(command, Command::Scan { .. }),
    )
    .context(UnityCatalogSnafu)?;
    let storage_options = match &table_args.storage_options_file {
        Some(path) => input::read_json_file::<input::StorageOptionsInput>(path)?.0,
        None => Default::default(),
    };
    let predicate = match &command {
        Command::Scan {
            predicate_file: Some(path),
            ..
        } => Some(input::read_json_file::<predicate::PredicateInput>(path)?.into_predicate()?),
        _ => None,
    };
    let execution_options = match &command {
        Command::Scan {
            execution_options_file: Some(path),
            ..
        } => {
            let options = input::read_json_file::<
                input::JsonObject<execution_options::ExecutionOptionsInput>,
            >(path)?
            .0;
            Some(
                options
                    .into_execution_options()
                    .context(ConfigurationSnafu)?,
            )
        }
        _ => None,
    };
    // Drive async work on the calling thread. A dedicated Tokio worker can
    // occupy the last OS thread and leave blocking loads queued forever.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context(RuntimeSnafu {
            reason: "runtime_build_failed",
        })?;
    let work = async {
        let snapshot_selection = table_args.table_version.map_or(
            DeltaSnapshotSelection::Latest,
            DeltaSnapshotSelection::Version,
        );
        let table = match uc_target {
            Some(target) => {
                target
                    .request_read_access(storage_options)
                    .await
                    .context(UnityCatalogSnafu)?
                    .load_table_for_inspection(snapshot_selection)
                    .await?
            }
            None => {
                // Inspect needs only metadata. Scan planning happens explicitly below.
                DeltaTableBuilder::new(&table_args.table)
                    .with_warmup(WarmupMode::None)
                    .with_snapshot_selection(snapshot_selection)
                    .with_storage_options(storage_options)
                    .load_table()
                    .await
                    .context(ReaderSnafu)?
            }
        };
        match &command {
            Command::Inspect(_) => {
                let output = encode_inspection_json(table.version(), table.schema().as_ref());
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
                }
            }
            Command::Scan {
                columns,
                no_columns,
                limit,
                target_partitions,
                ..
            } => {
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
                write_arrow_stream(builder.build().await.context(ReaderSnafu)?).await
            }
        }
    };
    let result = if uc_target.is_some() {
        runtime.block_on(async {
            tokio::select! {
                // Install listeners before polling work; cancellation drops any pending child.
                biased;
                exit_status = signals::wait_for_cancellation() => Err(Error::UnityCatalog {
                    source: unity_catalog::Error::Cancelled { status: exit_status? },
                }),
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

async fn write_arrow_stream(scan: DeltaScan) -> Result<(), Error> {
    let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
    let runtime = tokio::runtime::Handle::current();
    // A blocked stdout must leave the runtime and its blocking pool free to read files.
    std::thread::Builder::new()
        .name("dar-stdout".into())
        .spawn(move || {
            let result = runtime.block_on(async {
                let mut writer =
                    StreamWriter::try_new(io::stdout().lock(), scan.schema().as_ref())?;
                // Flush before polling so output failures stop further reads.
                writer.flush()?;
                let mut stream = scan.into_stream();
                while let Some(batch) = stream.next_batch().await.context(ReaderSnafu)? {
                    writer.write(&batch)?;
                    writer.flush()?;
                }
                // finish writes the end marker and flushes; never call it after an error.
                writer.finish()?;
                Ok(())
            });
            let _ = result_sender.send(result);
        })
        .context(RuntimeSnafu {
            reason: "output_thread_spawn_failed",
        })?;
    result_receiver
        .await
        .map_err(io::Error::other)
        .context(RuntimeSnafu {
            reason: "output_thread_result_lost",
        })?
}

fn main() -> ExitCode {
    diagnostics::init();
    // Kernel workers can panic without waking their caller. Report synchronously
    // and exit without unwinding into a blocking join; never format the payload.
    std::panic::set_hook(Box::new(|_| {
        tracing::error!(target: "delta_arrow_reader_cli",
            event = "cli.failed", operation = "cli_command", outcome = "failed",
            error_phase = "execution", error_code = "runtime_initialization",
            error_reason = "process_panicked", "CLI process panicked"
        );
        std::process::exit(1);
    }));
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error.log_failure();
            error.exit_code()
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
                matches!(cli.command, Command::Inspect(TableArgs {table_version: Some(version), ..}) if version == text.parse::<u64>().unwrap())
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
                },
                "not_found",
                "io_kind=NotFound",
            ),
            (
                Error::InputIo {
                    source: io::Error::new(io::ErrorKind::PermissionDenied, "secret path"),
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
        let error = input::read_json_file::<
            input::JsonObject<execution_options::ExecutionOptionsInput>,
        >(path.to_str().unwrap())
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
        let error = input::read_json_file::<input::StorageOptionsInput>(path.to_str().unwrap())
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
            .context(InputIoSnafu)
            .unwrap_err();
        assert!(error.source().unwrap().is::<io::Error>());
        let text = diagnostics::capture("warn", || error.log_failure());
        assert!(text.contains("error_code=\"input_file_io\""));
        assert!(!text.contains("secret"));
        assert_eq!(format!("{error:?}"), error.to_string());
    }
}
