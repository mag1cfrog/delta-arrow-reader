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

mod capabilities;
mod command_runtime;
mod diagnostics;
mod input;
mod inspection;
mod output;
mod output_capacity;
mod preview;
mod query;
mod signals;
mod stdout_monitor;
mod table_access;

use std::{
    io::{self, IsTerminal, Write},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::ExitCode,
    str::FromStr,
    sync::atomic::{AtomicBool, Ordering},
};

use arrow_schema::Schema;
use clap::{Args, Parser, Subcommand, error::ErrorKind};
use diagnostics::{
    ArgumentSnafu, ArtifactIoSnafu, ConfigurationSnafu, DiagnosticFormat, Error, InputCorrection,
    InputIoSnafu, InputValidationSnafu, OutputSnafu, QuerySnafu, ReaderSnafu, RuntimeSnafu,
    StorageAuthorizationSnafu, TerminalOutputSnafu, UnityCatalogSnafu,
};
use futures_util::{FutureExt, TryStreamExt, future::LocalBoxFuture};
use input::JsonInputKind;
use output::OutputStats;
use serde_json::{Value, json};
use snafu::{ResultExt, ensure};
use table_access::{TableArgs, unity_catalog};

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
    /// Plan a read-only SQL query without executing result rows.
    Explain(query::explain::ExplainArgs),
    /// Export a Delta snapshot's rows as Arrow IPC.
    ///
    /// Use --output PATH to save a complete file, or redirect stdout.
    Scan {
        #[command(flatten)]
        read: RowReadArgs,
        /// Maximum output rows as unsigned decimal digits; zero is valid.
        #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
        limit: Option<usize>,
        /// Save one complete result without replacing an existing file.
        /// Stdout returns a small JSON receipt. Windows staging inherits the directory ACL.
        #[arg(long, value_name = "PATH")]
        output: Option<PathBuf>,
        /// Also save PATH.manifest.json with the sources and options needed for replay.
        ///
        /// Replay details: capabilities --schema result-manifest --detail replay.
        #[arg(long, requires = "output")]
        manifest: bool,
        /// File result row ceiling; defaults to 1000000. Exceeding it fails, unlike --limit.
        #[arg(long, requires = "output", default_value_if("output", clap::builder::ArgPredicate::IsPresent, "1000000"), value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
        max_rows: Option<usize>,
        /// File result byte ceiling, including framing; defaults to 1 GiB.
        #[arg(long, requires = "output", default_value_if("output", clap::builder::ArgPredicate::IsPresent, "1073741824"), value_name = "BYTES", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
        max_output_bytes: Option<NonZeroUsize>,
        /// Whole-command deadline in milliseconds; file output defaults to 300000, stdout has no default.
        #[arg(long, default_value_if("output", clap::builder::ArgPredicate::IsPresent, "300000"), value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
        deadline_ms: Option<NonZeroUsize>,
    },
}

/// Snapshot, projection, predicate, and execution choices shared by scan and preview.
#[derive(Args, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct RowReadArgs {
    #[command(flatten)]
    #[serde(flatten)]
    table: TableArgs,
    /// Existing Databricks CLI profile; required for uc://catalog.schema.table targets.
    #[arg(long, value_name = "PROFILE")]
    databricks_profile: Option<String>,
    /// Logical column name; repeat to select columns in argument order. Default: all columns.
    #[arg(long = "column", value_name = "NAME")]
    #[serde(rename = "column")]
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

fn build_completion_record(
    command: &'static str,
    table_version: Option<u64>,
    output_stats: OutputStats,
    row_limit: Option<usize>,
) -> Value {
    let limit_reached = row_limit.is_some_and(|limit| output_stats.rows_written >= limit as u64);
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
        "output_bytes": output_stats.bytes_written.to_string(),
    });
    if matches!(command, "scan" | "query") {
        record["rows"] = output_stats.rows_written.to_string().into();
        record["batches"] = output_stats.batches_written.to_string().into();
    }
    record
}

fn build_json_read_completion(
    command: &'static str,
    bindings: Vec<Value>,
    output_stats: OutputStats,
) -> Value {
    json!({
        "format_version": 2,
        "kind": "completion",
        "command": command,
        "bindings": bindings,
        "rows": output_stats.rows_written.to_string(),
        "output_bytes": output_stats.bytes_written.to_string(),
    })
}

async fn execute_table_command(
    mut command: Command,
    report_uc_cancellation: &AtomicBool,
    spill_directory: &mut Option<tempfile::TempDir>,
    pending_artifact: &mut Option<output::artifact::PendingArtifactPublication>,
    cancellation: &futures_util::future::AbortHandle,
) -> Result<Value, Error> {
    match &mut command {
        Command::Scan {
            read,
            output: Some(_),
            ..
        } => {
            read.table
                .options
                .select_file_output_credentials(&read.table.table)?;
        }
        Command::Query(args) if args.output.is_some() => {
            if let Some(location) = &args.input.table {
                args.input
                    .table_options
                    .select_file_output_credentials(location)?;
            }
        }
        _ => {}
    }
    let arrow_stdout = match &command {
        Command::Scan { output, .. } => output.is_none(),
        Command::Query(args) => {
            args.output.is_none() && args.output_format == output::RowOutputFormat::Arrow
        }
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
        Command::Query(_) | Command::Explain(_) => None,
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
    let mut artifact = match &command {
        Command::Scan {
            read,
            output: Some(path),
            manifest,
            limit,
            deadline_ms,
            max_rows,
            max_output_bytes,
        } => {
            let options = if *manifest {
                let mut options =
                    serde_json::to_value(read).map_err(|source| Error::JsonOutput { source })?;
                options["limit"] = json!(limit);
                options["deadline-ms"] = json!(deadline_ms);
                options["max-rows"] = json!(max_rows);
                options["max-output-bytes"] = json!(max_output_bytes);
                Some(options)
            } else {
                None
            };
            Some(output::artifact::StagedResult::new(
                path,
                output::RowOutputFormat::Arrow,
                options,
                cancellation.clone(),
            )?)
        }
        _ => None,
    };
    let predicate = match row_read.and_then(|read| read.predicate_file.as_deref()) {
        Some(path) => {
            let (value, bytes) = input::read_json_file_with_bytes::<input::PredicateInput>(
                path,
                JsonInputKind::Predicate,
            )?;
            if let Some(artifact) = &mut artifact {
                artifact.record_replay_input("--predicate-file", path, &bytes);
            }
            Some(value.into_predicate()?)
        }
        _ => None,
    };
    let execution_options = match row_read.and_then(|read| read.execution_options_file.as_deref()) {
        Some(path) => {
            let (input::JsonObject(options), bytes) = input::read_json_file_with_bytes::<
                input::JsonObject<input::ExecutionOptionsInput>,
            >(
                path, JsonInputKind::ExecutionOptions
            )?;
            if let Some(artifact) = &mut artifact {
                artifact.record_replay_input("--execution-options-file", path, &bytes);
            }
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
        Command::Explain(args) => {
            let (uses_unity_catalog, planning) = args.prepare_planning()?;
            report_uc_cancellation.store(uses_unity_catalog, Ordering::Relaxed);
            planning
        }
        Command::Query(args) => {
            let (query_uses_unity_catalog, execution) =
                args.prepare_execution(spill_directory, pending_artifact, cancellation)?;
            report_uc_cancellation.store(query_uses_unity_catalog, Ordering::Relaxed);
            execution
        }
        Command::Inspect {
            table, inspection, ..
        } => async move {
            let (table, _authorized_access) = table.load_table(uc_target).await?;
            let version = table.version();
            let output = inspection.encode_json(&table)?;
            let output_stats = OutputStats {
                bytes_written: output.len() as u64,
                ..Default::default()
            };
            write_stdout(&output)?;
            Ok(build_completion_record(
                "inspect",
                Some(version),
                output_stats,
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
                } else if let Some(staged) = artifact.take() {
                    let (staged, stats) = staged
                        .write_rows(
                            schema,
                            stream,
                            Vec::new(),
                            match &command {
                                Command::Scan {
                                    max_rows: Some(rows),
                                    max_output_bytes: Some(bytes),
                                    ..
                                } => Some(output::OutputLimits {
                                    max_rows: *rows as u64,
                                    max_bytes: bytes.get() as u64,
                                }),
                                _ => None,
                            },
                        )
                        .await?;
                    let completion = build_completion_record("scan", Some(version), stats, limit);
                    *pending_artifact = Some(staged.prepare_publication(&completion)?);
                    Ok(completion)
                } else {
                    let stats =
                        output::arrow::write_arrow_stream_to_stdout(schema, stream, None).await?;
                    Ok(build_completion_record("scan", Some(version), stats, limit))
                }
            }
            .await;
            let result = match (scan_result, &authorized_access) {
                (Err(error @ Error::Reader { .. }), Some(access)) => {
                    Err(access.map_read_error(error).await)
                }
                (result, _) => result,
            };
            if matches!(
                &command,
                Command::Scan {
                    output: Some(_),
                    ..
                }
            ) {
                result.map_err(|source| Error::WithResolvedBindings {
                    source: Box::new(source),
                    bindings: vec![json!({"alias": "t", "table_version": version.to_string()})],
                })
            } else {
                result
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
        OutputStats {
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
            (vec!["scan", "table", "--output", "rows"], Some(300000)),
            (vec!["preview", "table"], Some(30000)),
            (
                vec!["explain", "table", "--sql-file", "query.sql"],
                Some(30000),
            ),
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
                    Command::Explain(args) => Some(args.deadline_ms),
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
    fn file_and_stdout_defaults_match_their_distinct_output_budgets() {
        for format in ["arrow", "json"] {
            for output in [false, true] {
                let mut argv = vec![
                    "dar",
                    "query",
                    "table",
                    "--sql-file",
                    "query.sql",
                    "--format",
                    format,
                ];
                if output {
                    argv.extend(["--output", "rows"]);
                }
                let Command::Query(args) = Cli::try_parse_from(&argv).unwrap().command else {
                    unreachable!()
                };
                let expected = match (format, output) {
                    ("json", false) => 8192,
                    ("json", true) => 16777216,
                    _ => 1073741824,
                };
                assert_eq!(args.output_byte_limit(), expected);
                argv.extend(["--max-output-bytes", "123"]);
                let Command::Query(args) = Cli::try_parse_from(&argv).unwrap().command else {
                    unreachable!()
                };
                assert_eq!(args.output_byte_limit(), 123);
            }
        }
        let Command::Scan {
            max_rows,
            max_output_bytes,
            ..
        } = Cli::try_parse_from(["dar", "scan", "table", "--output", "rows"])
            .unwrap()
            .command
        else {
            unreachable!()
        };
        assert_eq!(max_rows, Some(1000000));
        assert_eq!(max_output_bytes.map(NonZeroUsize::get), Some(1073741824));
        for flag in ["--max-rows", "--max-output-bytes"] {
            assert!(Cli::try_parse_from(["dar", "scan", "table", flag, "1"]).is_err());
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
                matches!(cli.command, Command::Inspect { table: TableArgs {options: table_access::TableOptions { table_version: Some(version), .. }, ..}, .. } if version == text.parse::<u64>().unwrap())
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
    fn storage_options_preserve_strings() {
        let options: input::StorageOptionsInput = serde_json::from_str(
            r#"{"AWS_ACCESS_KEY_ID":" secret, value ","MixedCase":"unchanged"}"#,
        )
        .unwrap();
        assert_eq!(options.0["AWS_ACCESS_KEY_ID"], " secret, value ");
        assert_eq!(options.0["MixedCase"], "unchanged");
        assert_eq!(options.0.len(), 2);
    }
}
