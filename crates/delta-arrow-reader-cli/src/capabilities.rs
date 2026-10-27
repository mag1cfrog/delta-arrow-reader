//! Discover commands, then look up the inputs needed for a particular read.
//!
//! `dar capabilities` lists the installed commands and how to request details.
//! For example, `--command scan` gives usage and limits, `--schema predicate`
//! gives a filter example, and `--schema predicate --detail int64` describes
//! one literal type. Add `--full` for complete command or authorization metadata,
//! or a validation schema. All views work without opening a table or resolving
//! authorization.

mod env_file_guides;
mod output_schemas;
mod read_support;
mod schemas;

pub(crate) use schemas::DiscoverySchema;

use std::{io, process::ExitCode, time::Duration};

use clap::{Arg, ArgAction, ArgGroup, Args, Command, CommandFactory, ValueEnum, error::ErrorKind};
use serde_json::{Value, json};
use snafu::ResultExt;

use crate::{
    Cli, Error, OutputSnafu, OutputStats, RuntimeSnafu, build_completion_record,
    diagnostics::{self, DiagnosticFormat, FAILURE_DIAGNOSTIC_TIMEOUT},
    input::JsonInputKind,
    signals, write_stdout,
};

const MAX_INDEX_BYTES: usize = 2 * 1024;
const MAX_COMMAND_GUIDE_BYTES: usize = 4 * 1024;
const MAX_COMMAND_DETAIL_BYTES: usize = 2 * 1024;
const MAX_SCHEMA_GUIDE_BYTES: usize = 2 * 1024;
const MAX_AUTHORIZATION_GUIDE_BYTES: usize = 2 * 1024;
const MAX_FULL_RESPONSE_BYTES: usize = 1024 * 1024;
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Args)]
#[command(group(ArgGroup::new("full_definition_target").args(["command", "schema", "authorization"])))]
#[command(group(ArgGroup::new("detail_target").args(["command", "schema", "authorization"])))]
pub(crate) struct CapabilitiesArgs {
    /// Show usage and limits for one command.
    #[arg(long, value_name = "NAME", value_parser = parse_command_name, conflicts_with_all = ["schema", "authorization", "all"])]
    command: Option<String>,
    /// Show an example and available fields or types for one JSON format.
    #[arg(long, value_enum, conflicts_with_all = ["command", "authorization", "all"])]
    schema: Option<DiscoverySchema>,
    /// Select runtime/artifacts (or query resources) from --command, a field/type from --schema, or env-file/env-file-grammar from --authorization.
    #[arg(
        long,
        value_name = "NAME",
        requires = "detail_target",
        conflicts_with_all = ["all", "full"]
    )]
    detail: Option<String>,
    /// Show an authorization guide for a storage or catalog service.
    #[arg(long, value_enum, conflicts_with_all = ["command", "schema", "all"])]
    authorization: Option<read_support::AuthorizationService>,
    /// Export a full JSON Schema for validators, or command/authorization metadata for tooling.
    #[arg(long, requires = "full_definition_target", conflicts_with = "detail")]
    full: bool,
    /// Return the complete inventory of commands, authorization, and limits.
    #[arg(long, conflicts_with_all = ["command", "schema", "authorization", "full"])]
    all: bool,
}

fn parse_command_name(name: &str) -> Result<String, &'static str> {
    let mut cli = Cli::command();
    cli.build();
    cli.get_subcommands()
        .any(|command| command.get_name() == name && !command.is_hide_set())
        .then(|| name.to_owned())
        .ok_or("unknown command; run capabilities to list installed commands")
}

fn build_capability_index() -> Value {
    let mut cli = Cli::command();
    cli.build();
    let executable = cli.get_name();
    let commands: Vec<_> = cli
        .get_subcommands()
        .filter(|command| !command.is_hide_set())
        .map(|command| {
            json!({
                "name": command.get_name(),
                "description": command.get_about().map(ToString::to_string),
                "details": [executable, "capabilities", "--command", command.get_name()],
            })
        })
        .collect();
    json!({
        "format_version": 1,
        "kind": "capability_index",
        "cli_version": env!("CARGO_PKG_VERSION"),
        "commands": commands,
        "schemas": DiscoverySchema::value_variants(),
        "schema_command": [executable, "capabilities", "--schema", "NAME"],
        "all_command": [executable, "capabilities", "--all"],
    })
}

fn discovery_limits(max_output_bytes: usize) -> Value {
    json!({
        "max_output_bytes": max_output_bytes,
        "deadline_ms": DISCOVERY_TIMEOUT.as_millis(),
        "failure_diagnostic_timeout_ms": FAILURE_DIAGNOSTIC_TIMEOUT.as_millis(),
    })
}

fn build_command_guide(name: &str) -> Result<Value, Error> {
    let mut cli = Cli::command();
    cli.build();
    let executable = cli.get_name();
    let command = cli.find_subcommand(name).ok_or(Error::Argument {
        kind: ErrorKind::InvalidValue,
        correction: None,
    })?;
    let mut guide = match name {
        "inspect" => json!({
            "example": [executable, "--diagnostics=json", "inspect", "./table", "--view", "summary"],
            "stdout": "json",
            "notes": [
                "Pin lookups with --table-version N: --column-prefix PREFIX finds names; --column NAME returns only selected types.",
                "--include-metadata requires --column and adds metadata to every selected column. Select only columns whose metadata you need; get other columns' types separately.",
                "Summary checks scan protocol support, not Parquet or deletion-vector access. Names/types lookups omit checks.",
                "Summary defaults to 4096 stdout bytes and 30000 ms; adjust --max-output-bytes or --deadline-ms explicitly. Full inspection has neither default; redirect it and select a deadline.",
            ],
        }),
        "scan" => json!({
            "example": [executable, "--diagnostics=json", "scan", "./table", "--limit", "1", "--deadline-ms", "30000"],
            "stdout": "arrow_ipc_stream",
            "notes": [
                "Use --output PATH to save a complete result with a manifest and return a JSON receipt. Otherwise redirect Arrow stdout to a file or pipe.",
                "--limit selects a sample. Files default to 1000000 rows, 1 GiB and 300000 ms; --max-rows/--max-output-bytes are failure ceilings. Legacy stdout has no byte ceiling or default deadline.",
            ],
        }),
        "preview" => json!({
            "example": [executable, "--diagnostics=json", "preview", "./table", "--column", "id", "--limit", "3"],
            "stdout": "json",
            "completion_format_version": 2,
            "notes": [
                "Choose only needed columns and rows. Find names with inspect --view summary --column-prefix PREFIX; pin --table-version N.",
                "Always a sample: end_of_input/has_more=false proves exhaustion; row_limit/null does not; byte_limit/true means an observed row was omitted.",
                "Schema or first row too large fails with empty stdout. The deadline includes inputs, authorization and output.",
            ],
        }),
        "query" => json!({
            "example": [executable, "--diagnostics=json", "query", "./table", "--sql-file", "query.sql", "--format", "json"],
            "stdout": "json",
            "completion_format_version": 2,
            "resource_defaults_format": "json",
            "resource_details_command": [executable, "capabilities", "--command", "query", "--detail", "resources"],
            "sql": {
                "argument": "--sql-file", "table": "t",
                "example": "SELECT count(*) AS row_count FROM t",
                "max_file_bytes": crate::query::MAX_SQL_BYTES,
            },
            "notes": [
                "One read-only SQL statement in a UTF-8 file. Choose TABLE as t or named tables from --tables-file.",
                "The example returns complete typed JSON. Select only needed columns or aggregate; ceilings fail without JSON output. SQL LIMIT changes the result.",
                "With --output PATH, --max-output-bytes limits the saved file; the stdout receipt is at most 8192 bytes. Defaults: 16 MiB for JSON files, 1 GiB for Arrow. Omit --format for Arrow IPC.",
                "The deadline includes inputs, planning and output. Memory is limited to the DataFusion pool, not process RSS; spill defaults to disabled.",
                read_support::QUERY_SNAPSHOT_SELECTION,
                read_support::QUERY_ROW_ORDERING,
            ],
        }),
        "explain" => json!({
            "example": [executable, "--diagnostics=json", "explain", "./table", "--sql-file", "query.sql"],
            "stdout": "json", "completion_format_version": 3,
            "notes": [
                "Loads snapshot metadata and plans; never executes result rows or reads data files. Default: counts and actual bindings. Pin snapshots and preserve SQL/options for follow-ups. Memory bounds the DataFusion pool, not metadata, planning buffers or process RSS.",
                "Physical plan: combine --node N (operator/children), repeated --field N (exact output types), and --statistics. Fields/statistics default to root node 0; estimates are not observed results.",
                "Add --scan N for source projection and pushdown. Physical nodes and logical scans have separate zero-based preorder IDs; selectors can be combined within one output budget.",
            ],
        }),
        _ => json!({
            "example": [executable, name],
            "stdout": if name == "capabilities" { "json" } else { "text" },
        }),
    };
    let options: serde_json::Map<_, _> = command
        .get_arguments()
        .filter(|argument| {
            matches!(
                argument.get_id().as_str(),
                "table" | "tables_file" | "table_version" | "columns" | "limit"
            )
        })
        .map(|argument| {
            (
                argument_name(argument),
                json!(argument.get_help().map(ToString::to_string)),
            )
        })
        .collect();
    if !options.is_empty() {
        guide["options"] = json!(options);
    }
    if name == "explain" {
        guide["options"] = json!({
            "TABLE": "Snapshot location, registered as t.",
            "--tables-file": "Named bindings instead of TABLE; relative locations use the manifest directory.",
            "--table-version": "Snapshot version for TABLE; default: latest.",
        });
    }
    if matches!(name, "query" | "preview" | "explain") {
        let ceilings: serde_json::Map<_, _> = command
            .get_arguments()
            .filter(|argument| {
                matches!(
                    argument.get_id().as_str(),
                    "max_rows"
                        | "max_output_bytes"
                        | "memory_limit_bytes"
                        | "deadline_ms"
                        | "limit"
                )
            })
            .filter_map(|argument| {
                argument
                    .get_default_values()
                    .first()
                    .map(|value| (argument_name(argument), json!(value.to_string_lossy())))
            })
            .collect();
        guide["resource_defaults"] = json!(ceilings);
        if name != "explain" {
            guide["resource_defaults"]["--max-output-bytes"] =
                json!(crate::output::json::DEFAULT_MAX_BYTES.to_string());
        }
    }
    let input_schemas: Vec<_> = JsonInputKind::value_variants()
        .iter()
        .filter(|kind| {
            command
                .get_arguments()
                .any(|argument| argument_name(argument) == kind.argument())
        })
        .map(|kind| json!({"name": kind, "argument": kind.argument()}))
        .collect();
    if !input_schemas.is_empty() {
        guide["input_schemas"] = json!(input_schemas);
    }
    let mut authorization_guides = Vec::new();
    if command
        .get_arguments()
        .any(|argument| argument.get_id() == "storage_options_file")
    {
        authorization_guides.extend(["s3", "azure-blob", "gcs"]);
    }
    if command
        .get_arguments()
        .any(|argument| argument.get_id() == "databricks_profile")
    {
        authorization_guides.push("unity-catalog");
    }
    if !authorization_guides.is_empty() {
        guide["authorization"] = json!({
            "local": "Local paths need no credentials.",
            "guides": authorization_guides,
            "details_command": [executable, "capabilities", "--authorization", "NAME"],
        });
        let mut conflicts = vec!["--auth-source", "--databricks-profile"];
        if matches!(name, "query" | "explain") {
            conflicts.push("--tables-file");
        }
        if name != "explain" {
            guide["authorization"]["env_file"] = json!({
                "arguments": ["--env-file", "PATH"],
                "conflicts_with": conflicts,
                "usage": "For a direct cloud TABLE, pass a provisioned credential file without reading or expanding its values. Selected-file failures never fall back to another identity.",
            });
        }
    }
    let mut output_schemas = vec![DiscoverySchema::Diagnostic, DiscoverySchema::Completion];
    if name == "inspect" {
        output_schemas.push(DiscoverySchema::InspectionSummary);
    } else if matches!(name, "query" | "preview") {
        output_schemas.push(DiscoverySchema::Rows);
    } else if name == "explain" {
        output_schemas.push(DiscoverySchema::QueryPlan);
    }
    if matches!(name, "scan" | "query") {
        output_schemas.extend([
            DiscoverySchema::ResultArtifact,
            DiscoverySchema::ResultManifest,
        ]);
        guide["artifact_details_command"] = json!([
            executable,
            "capabilities",
            "--command",
            name,
            "--detail",
            "artifacts"
        ]);
    }
    guide["output_schemas"] = json!(output_schemas);
    guide["schema_command"] = json!([executable, "capabilities", "--schema", "NAME"]);
    guide["format_version"] = json!(1);
    guide["kind"] = json!("command_guide");
    guide["command"] = json!(name);
    guide["help_command"] = if name == "help" {
        json!([executable, "--help"])
    } else {
        json!([executable, name, "--help"])
    };
    guide["full_command"] = json!([executable, "capabilities", "--command", name, "--full"]);
    guide["runtime_details_command"] = json!([
        executable,
        "capabilities",
        "--command",
        name,
        "--detail",
        "runtime"
    ]);
    guide["success"] = json!(
        "Require exit 0; with --diagnostics=json, also validate the stderr completion for its command and format_version. Otherwise discard stdout."
    );
    Ok(guide)
}

fn build_command_detail(name: &str, detail: &str) -> Result<Value, Error> {
    if matches!(name, "scan" | "query") && detail == "artifacts" {
        return Ok(json!({
            "format_version": 1, "kind": "command_detail", "command": name, "detail": "artifacts",
            "option": "--output PATH",
            "authorization": "Local: none. UC: Databricks profile. Remote: --auth-source or --env-file; a storage-options file alone selects json. Each remote tables-file binding needs authorization.",
            "publication": "Publishes PATH, then PATH.manifest.json; never overwrites either. Without a matching complete manifest, the result is uncommitted. Failures never stream rows to stdout. Before retrying, check whether a complete pair already exists; otherwise choose unused names.",
            "receipt": "Stdout: result_artifact v1, absolute result/manifest paths, at most 8192 bytes. --diagnostics=json: completion v3 with command/output_bytes (receipt bytes). Result counts are in the manifest.",
            "read_result": "Require manifest completion=complete and result.path=receipt.result. Use manifest result.format to read the saved file; for JSON, use the result's schema.fields to decode needed columns. Keep complete results in files.",
            "replay": "Reuse the original input files with replay.options in replay.working_directory and a new --output. Pin every alias from bindings. With --tables-file, copy it beside the original tables file and set each table_version. Keep authorization/limits. SQL order or volatile values may vary.",
            "storage": "Temporary files: owner-only on Unix; Windows needs a private inherited directory ACL. Crashes/uninterruptible IO may leave temps; no power-loss durability guarantee.",
            "manifest_schema": [crate::command_name(), "capabilities", "--schema", "result-manifest"],
        }));
    }
    if name == "query" && detail == "resources" {
        return Ok(json!({
            "format_version": 1, "kind": "command_detail", "command": "query", "detail": "resources",
            "memory": "--memory-limit-bytes BYTES: 268435456 by default. Bounds DataFusion execution-pool reservations; reader, metadata and output buffers are outside it. Use OS/container limits for process RSS.",
            "spill": "Disabled by default. Enable with --spill-limit-bytes BYTES > 0 and --spill-dir PATH, an existing writable directory. DataFusion checks total spill file sizes after writes and can temporarily overshoot; it is not a filesystem hard cap.",
            "privacy": "Spill contains table data. Query-specific storage is owner-only on Unix; on Windows choose a parent with a private inherited ACL.",
            "cleanup": "Removes query temporary files on success, handled failure and cancellation; retains the parent. Cleanup errors fail the command. SIGKILL, crashes and OS-uninterruptible IO cannot guarantee cleanup.",
            "retry": "Preserve SQL, table bindings and pinned snapshots when changing a budget.",
        }));
    }
    if detail != "runtime" {
        return Err(Error::Argument {
            kind: ErrorKind::InvalidValue,
            correction: Some(crate::InputCorrection {
                argument: "--detail".into(),
                field_path: None,
                expected: "runtime, artifacts for scan/query, or resources for query".into(),
                supplied: None,
                hint: "Use --detail runtime for deadlines and cancellation, or --command query --detail resources for memory and spill.",
            }),
        });
    }
    let is_table_command = matches!(name, "inspect" | "scan" | "preview" | "query" | "explain");
    Ok(json!({
        "format_version": 1, "kind": "command_detail", "command": name, "detail": "runtime",
        "deadline": match name {
            "inspect" => "--deadline-ms N: summary defaults to 30000 ms; full has no default.",
            "scan" => "--deadline-ms N: defaults to 300000 with --output; stdout has no default.",
            "preview" | "explain" => "--deadline-ms N: defaults to 30000 ms.",
            "query" => "--deadline-ms N: defaults to 300000 ms.",
            _ => "Fixed 5000 ms, including output and completion.",
        },
        "scope": if name == "explain" { "Inputs, authorization, metadata IO, planning, stdout and completion; no result execution." } else if is_table_command { "Inputs, authorization, planning, execution, stdout and completion." } else { "Offline generation, stdout and completion; no input or credential files are opened." },
        "cleanup_ms": if is_table_command { crate::command_runtime::CLEANUP_GRACE.as_millis() } else { 0 },
        "failure_reporting_ms": FAILURE_DIAGNOSTIC_TIMEOUT.as_millis(),
        "signals": "Unix SIGINT exits 130; SIGTERM exits 143, including Linux PID 1. Windows Ctrl+C exits 130.",
        "output": "Require exit 0; with --diagnostics=json, also validate the stderr completion for its command and format_version. Otherwise discard stdout, even if it looks complete.",
        "retry": if is_table_command { "Retry only if intended. Preserve the request, pinned snapshots, authorization and other limits; choose any larger deadline explicitly." } else { "Drain stdout and stderr before retrying the same discovery request." },
        "termination_limits": "SIGKILL and OS-uninterruptible IO cannot guarantee cleanup. Closed or blocked stderr can prevent a complete diagnostic.",
        "broken_pipe": if is_table_command { "Exit 3. Unix detects a closed stdout pipe during work; other platforms detect it on the next write, with the selected deadline still applying." } else { "Exit 3 when an output write fails." },
    }))
}

fn build_capability_details(selected_command: Option<&str>) -> Result<Value, Error> {
    let mut cli = Cli::command();
    cli.build();
    let command = match selected_command {
        Some(name) => cli.find_subcommand(name).ok_or(Error::Argument {
            kind: ErrorKind::InvalidValue,
            correction: None,
        })?,
        None => &cli,
    };
    let input_schemas: Vec<_> = JsonInputKind::value_variants()
        .iter()
        .filter(|kind| {
            selected_command.is_none()
                || command
                    .get_arguments()
                    .any(|argument| argument_name(argument) == kind.argument())
        })
        .map(|kind| json!({"name": kind, "argument": kind.argument()}))
        .collect();
    let terminal_schemas = [DiscoverySchema::Diagnostic, DiscoverySchema::Completion];
    let mut output_schemas: Vec<_> = terminal_schemas
        .iter()
        .map(|schema| json!({"name": schema, "stream": "stderr"}))
        .collect();
    if selected_command.is_none() || selected_command == Some("inspect") {
        output_schemas.splice(
            0..0,
            [
                json!({"name": DiscoverySchema::Inspection, "stream": "stdout"}),
                json!({"name": DiscoverySchema::InspectionSummary, "stream": "stdout"}),
            ],
        );
    }
    if selected_command.is_none() || matches!(selected_command, Some("query" | "preview")) {
        output_schemas.push(json!({"name": DiscoverySchema::Rows, "stream": "stdout"}));
    }
    if selected_command.is_none() || selected_command == Some("explain") {
        output_schemas.push(json!({"name": DiscoverySchema::QueryPlan, "stream": "stdout"}));
    }
    if selected_command.is_none() || matches!(selected_command, Some("scan" | "query")) {
        output_schemas.extend([
            json!({"name": DiscoverySchema::ResultArtifact, "stream": "stdout"}),
            json!({"name": DiscoverySchema::ResultManifest, "stream": "file"}),
        ]);
    }
    let mut authorization = read_support::build_authorization_capabilities();
    let mut read_support = read_support::command_read_support();
    let mut stdout_formats = json!({
        "capabilities": {"format": "json", "format_version": 1},
        "inspect": {
            "format": "json", "format_version": 1,
            "schema": DiscoverySchema::Inspection,
            "views": {"full": DiscoverySchema::Inspection, "summary": DiscoverySchema::InspectionSummary},
            "default_view": "full",
        },
        "scan": {"format": "arrow_ipc_stream"},
        "preview": {"format": "json", "schema": DiscoverySchema::Rows, "completion_format_version": 2},
        "explain": {"format": "json", "schema": DiscoverySchema::QueryPlan, "completion_format_version": 3},
        "query": {
            "format": "arrow_ipc_stream",
            "default": "arrow",
            "choices": {
                "arrow": {"format": "arrow_ipc_stream", "completion_format_version": 1},
                "json": {"format": "json", "schema": DiscoverySchema::Rows, "default_max_output_bytes": crate::output::json::DEFAULT_MAX_BYTES, "completion_format_version": 2},
            },
        },
        "help": {"format": "text"},
    });
    for name in ["scan", "query"] {
        stdout_formats[name]["output_file"] = json!({"argument": "--output", "schema": DiscoverySchema::ResultArtifact, "completion_format_version": 3});
    }
    if let Some(name) = selected_command {
        if let Some(sources) = authorization.as_object_mut() {
            sources.retain(|_, source| {
                source["commands"]
                    .as_array()
                    .is_some_and(|commands| commands.iter().any(|command| command == name))
            });
        }
        for descriptions in [&mut read_support, &mut stdout_formats] {
            if let Some(commands) = descriptions.as_object_mut() {
                commands.retain(|command, _| command == name);
            }
        }
    }
    Ok(json!({
        "format_version": 1,
        "kind": if selected_command.is_some() { "command_capabilities" } else { "capabilities" },
        "cli_version": env!("CARGO_PKG_VERSION"),
        "command": describe_command(command),
        "input_schemas": input_schemas,
        "output_schemas": output_schemas,
        "schema_command": [cli.get_name(), "capabilities", "--schema", "NAME"],
        "authorization": authorization,
        "read_support": read_support,
        "stdout_formats": stdout_formats,
        "diagnostics": {
            "stream": "stderr",
            "default_format": "text",
            "json": {
                "option": "--diagnostics=json",
                "format_versions": {
                    "diagnostic": crate::diagnostics::DIAGNOSTIC_FORMAT_VERSION,
                    "completion": match selected_command { Some("preview") => 2, Some("explain") => 3, _ => 1 },
                },
                "completion_format_versions": match selected_command {
                    Some("preview") => vec![2],
                    Some("explain") => vec![3],
                    Some("query") | None => vec![1, 2, 3],
                    Some("scan") => vec![1, 3],
                    _ => vec![1],
                },
                "schemas": terminal_schemas,
            },
        },
        "limits": discovery_limits(MAX_FULL_RESPONSE_BYTES),
    }))
}

fn describe_command(command: &Command) -> Value {
    let arguments: Vec<_> = command
        .get_arguments()
        .filter(|argument| !argument.is_hide_set())
        .map(|argument| describe_argument(command, argument))
        .collect();
    let subcommands: Vec<_> = command
        .get_subcommands()
        .filter(|command| !command.is_hide_set())
        .map(describe_command)
        .collect();
    json!({
        "name": command.get_name(),
        "description": command.get_long_about().or_else(|| command.get_about()).map(ToString::to_string),
        "arguments": arguments,
        "subcommands": subcommands,
    })
}

fn describe_argument(command: &Command, argument: &Arg) -> Value {
    let values = argument.get_num_args().unwrap_or_default();
    let defaults: Vec<_> = argument
        .get_default_values()
        .iter()
        .map(|value| value.to_string_lossy())
        .collect();
    let choices: Vec<_> = argument
        .get_possible_values()
        .into_iter()
        .filter(|value| !value.is_hide_set())
        .map(|value| value.get_name().to_owned())
        .collect();
    let conflicts: Vec<_> = command
        .get_arg_conflicts_with(argument)
        .into_iter()
        .map(argument_name)
        .collect();
    let value_names: Vec<_> = argument
        .get_value_names()
        .unwrap_or_default()
        .iter()
        .map(|value| value.as_str())
        .collect();
    json!({
        "name": argument_name(argument),
        "short": argument.get_short().map(|name| format!("-{name}")),
        "position": argument.get_index(),
        "description": argument.get_long_help().or_else(|| argument.get_help()).map(ToString::to_string),
        "required": argument.is_required_set(),
        "global": argument.is_global_set(),
        "repeatable": matches!(argument.get_action(), ArgAction::Append | ArgAction::Count),
        "require_equals": argument.is_require_equals_set(),
        "value_names": value_names,
        "min_values": values.min_values(),
        "max_values": (values.max_values() != usize::MAX).then_some(values.max_values()),
        "default_values": defaults,
        "possible_values": choices,
        "conflicts_with": conflicts,
    })
}

fn argument_name(argument: &Arg) -> String {
    argument
        .get_long()
        .map_or_else(|| argument.get_id().to_string(), |name| format!("--{name}"))
}

/// Includes terminal reporting in discovery's deadline, even when stderr is blocked.
pub(crate) fn run_capabilities(
    options: CapabilitiesArgs,
    diagnostic_format: DiagnosticFormat,
) -> ExitCode {
    run_discovery(diagnostic_format, move || {
        let stats = write_discovery_response(options)?;
        Ok(build_completion_record("capabilities", None, stats, None))
    })
}

pub(crate) fn run_help_or_version(
    error: clap::Error,
    diagnostic_format: DiagnosticFormat,
) -> ExitCode {
    run_discovery(diagnostic_format, move || {
        crate::write_help_or_version(error)
    })
}

fn run_discovery(
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

fn write_discovery_response(options: CapabilitiesArgs) -> Result<OutputStats, Error> {
    let (document, max_output_bytes) = if let Some(schema) = options.schema {
        if options.full {
            (schema.schema_document(), MAX_FULL_RESPONSE_BYTES)
        } else {
            (
                schema.build_schema_guide(options.detail.as_deref())?,
                MAX_SCHEMA_GUIDE_BYTES,
            )
        }
    } else if let Some(name) = options.command {
        if let Some(detail) = options.detail.as_deref() {
            (
                build_command_detail(&name, detail)?,
                MAX_COMMAND_DETAIL_BYTES,
            )
        } else if options.full {
            (
                build_capability_details(Some(&name))?,
                MAX_FULL_RESPONSE_BYTES,
            )
        } else {
            (build_command_guide(&name)?, MAX_COMMAND_GUIDE_BYTES)
        }
    } else if let Some(service) = options.authorization {
        if options.full {
            (
                service.build_full_authorization_guide(),
                MAX_FULL_RESPONSE_BYTES,
            )
        } else if let Some(detail) = options.detail.as_deref() {
            (
                env_file_guides::build_env_file_guide(service, detail)?,
                MAX_AUTHORIZATION_GUIDE_BYTES,
            )
        } else {
            (
                service.build_authorization_guide(),
                MAX_AUTHORIZATION_GUIDE_BYTES,
            )
        }
    } else if options.all {
        (build_capability_details(None)?, MAX_FULL_RESPONSE_BYTES)
    } else {
        (build_capability_index(), MAX_INDEX_BYTES)
    };
    let output = serialize_discovery_response(&document, max_output_bytes)?;
    write_stdout(&output)?;
    Ok(OutputStats {
        bytes_written: output.len() as u64,
        ..Default::default()
    })
}

fn serialize_discovery_response(
    document: &Value,
    max_output_bytes: usize,
) -> Result<Vec<u8>, Error> {
    let mut output = document.to_string().into_bytes();
    output.push(b'\n');
    if output.len() > max_output_bytes {
        return Err(Error::OutputLimit {
            reason: "discovery_output_too_large",
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use clap::Parser;

    use super::*;

    #[test]
    fn command_examples_parse_and_link_to_complete_help() {
        let mut cli = Cli::command();
        cli.build();
        for command in cli.get_subcommands() {
            let guide = build_command_guide(command.get_name()).unwrap();
            assert!(guide.get("shutdown").is_none());
            let runtime_request: Vec<_> = guide["runtime_details_command"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap())
                .collect();
            Cli::try_parse_from(runtime_request).unwrap();
            let runtime = build_command_detail(command.get_name(), "runtime").unwrap();
            serialize_discovery_response(&runtime, MAX_COMMAND_DETAIL_BYTES).unwrap();
            assert!(runtime.get("options").is_none());
            assert!(runtime.get("schema").is_none());
            assert_eq!(runtime["failure_reporting_ms"], 1000);
            assert_eq!(
                runtime["cleanup_ms"],
                if matches!(
                    command.get_name(),
                    "inspect" | "scan" | "preview" | "query" | "explain"
                ) {
                    1000
                } else {
                    0
                }
            );
            let example: Vec<_> = guide["example"]
                .as_array()
                .unwrap()
                .iter()
                .map(|arg| arg.as_str().unwrap())
                .collect();
            if command.get_name() == "help" {
                assert_eq!(
                    Cli::try_parse_from(example).err().unwrap().kind(),
                    ErrorKind::DisplayHelp
                );
            } else {
                Cli::try_parse_from(example).unwrap();
            }
            let help: Vec<_> = guide["help_command"]
                .as_array()
                .unwrap()
                .iter()
                .map(|arg| arg.as_str().unwrap())
                .collect();
            let error = Cli::try_parse_from(help).err().unwrap();
            assert_eq!(error.kind(), ErrorKind::DisplayHelp);
            for omitted in [
                "help",
                "argument_conflicts",
                "read_support",
                "limits",
                "cli_version",
            ] {
                assert!(guide.get(omitted).is_none(), "{omitted}");
            }
            if let Some(conflicts) = guide["authorization"]["env_file"]["conflicts_with"].as_array()
            {
                let mut args = vec![
                    "dar",
                    command.get_name(),
                    "s3://bucket/table",
                    "--env-file",
                    "credentials.env",
                ];
                if matches!(command.get_name(), "query" | "explain") {
                    args.extend(["--sql-file", "query.sql"]);
                }
                assert!(Cli::try_parse_from(&args).is_ok());
                for conflict in conflicts {
                    let mut conflicting_args = args.clone();
                    let option = conflict.as_str().unwrap();
                    conflicting_args.extend([
                        option,
                        if option == "--auth-source" {
                            "env"
                        } else {
                            "reference"
                        },
                    ]);
                    assert_eq!(
                        Cli::try_parse_from(conflicting_args).err().unwrap().kind(),
                        ErrorKind::ArgumentConflict
                    );
                }
            }
        }
        let guide = build_command_guide("query").unwrap();
        assert_eq!(guide["resource_defaults_format"], "json");
        assert_eq!(guide["completion_format_version"], 2);
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), guide["sql"]["example"].as_str().unwrap()).unwrap();
        crate::query::ReadOnlySqlQuery::read_file(file.path().to_str().unwrap()).unwrap();
        let parsed = Cli::try_parse_from([
            "dar",
            "query",
            "table",
            "--sql-file",
            "query.sql",
            "--format",
            "json",
        ])
        .unwrap();
        let crate::Command::Query(args) = parsed.command else {
            panic!("query expected")
        };
        assert_eq!(
            guide["resource_defaults"]["--max-rows"],
            args.max_rows.to_string()
        );
        assert_eq!(
            guide["resource_defaults"]["--max-output-bytes"],
            args.output_byte_limit().to_string()
        );
        assert_eq!(
            guide["resource_defaults"]["--memory-limit-bytes"],
            args.memory_limit_bytes.to_string()
        );
        let guide = build_command_guide("scan").unwrap();
        assert!(guide["options"].get("--column").is_some());
        assert!(guide["options"].get("--table-version").is_some());
        assert!(guide["options"].get("--limit").is_some());
        assert!(guide["options"].get("--target-partitions").is_none());
    }

    #[test]
    fn discovery_selectors_are_mutually_exclusive_and_describe_their_conflicts() {
        let details = build_capability_details(Some("capabilities")).unwrap();
        let arguments = details["command"]["arguments"].as_array().unwrap();
        let selectors = [
            vec!["--command", "query"],
            vec!["--schema", "predicate"],
            vec!["--authorization", "s3"],
            vec!["--all"],
        ];
        for (index, selector) in selectors.iter().enumerate() {
            let argument = arguments
                .iter()
                .find(|argument| argument["name"] == selector[0])
                .unwrap();
            for (other_index, other) in selectors.iter().enumerate() {
                if index == other_index {
                    continue;
                }
                assert!(
                    argument["conflicts_with"]
                        .as_array()
                        .unwrap()
                        .contains(&json!(other[0]))
                );
                let args: Vec<_> = ["dar", "capabilities"]
                    .iter()
                    .chain(selector)
                    .chain(other)
                    .copied()
                    .collect();
                let error = Cli::try_parse_from(args).err().unwrap();
                assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
            }
        }
        for name in ["unknown", "analyze", "query --help", "", "QUERY"] {
            assert!(Cli::try_parse_from(["dar", "capabilities", "--command", name]).is_err());
            assert!(build_capability_details(Some(name)).is_err());
        }
        for invalid in [
            vec!["--full"],
            vec!["--all", "--full"],
            vec!["--detail", "rows"],
            vec!["--authorization", "s3", "--detail", "env-file", "--full"],
            vec!["--all", "--detail", "rows"],
            vec!["--schema", "predicate", "--detail", "int64", "--full"],
        ] {
            assert!(
                Cli::try_parse_from(
                    ["dar", "capabilities"]
                        .into_iter()
                        .chain(invalid.iter().copied())
                )
                .is_err(),
                "{invalid:?}"
            );
        }
        for valid in [
            vec!["--authorization", "s3", "--full"],
            vec!["--command", "query", "--full"],
            vec!["--command", "query", "--detail", "runtime"],
            vec!["--full", "--schema", "predicate"],
            vec!["--detail", "rows", "--schema", "completion"],
            vec!["--authorization", "s3", "--detail", "env-file"],
            vec!["--detail", "env-file-grammar", "--authorization", "gcs"],
        ] {
            assert!(Cli::try_parse_from(["dar", "capabilities"].into_iter().chain(valid)).is_ok());
        }
        assert!(build_command_detail("scan", "rows").is_err());
        assert!(build_command_detail("scan", "resources").is_err());
        let resources = build_command_detail("query", "resources").unwrap();
        assert!(resources.to_string().len() < MAX_COMMAND_DETAIL_BYTES);
    }

    #[test]
    fn response_limits_include_json_framing_and_the_final_newline() {
        for limit in [
            MAX_INDEX_BYTES,
            MAX_COMMAND_GUIDE_BYTES,
            MAX_FULL_RESPONSE_BYTES,
        ] {
            let exact = json!("a".repeat(limit - 3));
            let encoded = serialize_discovery_response(&exact, limit).unwrap();
            assert_eq!(encoded.len(), limit);
            assert_eq!(encoded.last(), Some(&b'\n'));
            assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), exact);
            let oversized = json!("a".repeat(limit - 2));
            assert!(matches!(
                serialize_discovery_response(&oversized, limit),
                Err(Error::OutputLimit {
                    reason: "discovery_output_too_large"
                })
            ));
        }
        assert!(serialize_discovery_response(&json!("\u{e9}"), 4).is_err());
        assert_eq!(
            serialize_discovery_response(&json!("\u{e9}"), 5)
                .unwrap()
                .len(),
            5
        );
    }

    #[test]
    fn completion_fits_the_minimum_atomic_pipe_write_size() {
        let record = build_completion_record(
            "capabilities",
            None,
            OutputStats {
                bytes_written: u64::MAX,
                ..Default::default()
            },
            None,
        );
        assert!(format!("{record}\n").len() <= 512);
    }

    #[test]
    fn command_formats_cover_the_parser_without_advertising_future_commands() {
        let inventory = build_capability_details(None).unwrap();
        let mut advertised: Vec<_> = inventory["command"]["subcommands"]
            .as_array()
            .unwrap()
            .iter()
            .map(|command| command["name"].as_str().unwrap())
            .collect();
        advertised.sort_unstable();
        assert_eq!(
            advertised,
            [
                "capabilities",
                "explain",
                "help",
                "inspect",
                "preview",
                "query",
                "scan"
            ]
        );
        assert_eq!(
            advertised,
            inventory["stdout_formats"]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
        );
        for name in advertised {
            let args = if name == "help" {
                vec!["dar", "help"]
            } else {
                vec!["dar", name, "--help"]
            };
            let error = Cli::try_parse_from(args).err().unwrap();
            assert_eq!(error.kind(), clap::error::ErrorKind::DisplayHelp);
        }
        for name in ["unknown", "analyze"] {
            let error = Cli::try_parse_from(["dar", name]).err().unwrap();
            assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
        }
    }

    #[test]
    fn advertised_argument_rules_match_accepted_invocations() {
        let inventory = build_capability_details(None).unwrap();
        let commands = inventory["command"]["subcommands"].as_array().unwrap();
        let scan = commands
            .iter()
            .find(|command| command["name"] == "scan")
            .unwrap();
        let arguments = scan["arguments"].as_array().unwrap();
        let argument = |name| {
            arguments
                .iter()
                .find(|argument| argument["name"] == name)
                .unwrap()
        };
        assert_eq!(argument("--column")["repeatable"], true);
        assert_eq!(argument("--column")["min_values"], 1);
        assert_eq!(argument("--column")["max_values"], 1);
        assert_eq!(argument("--no-columns")["max_values"], 0);
        assert_eq!(
            argument("--no-columns")["conflicts_with"],
            json!(["--column"])
        );
        assert_eq!(argument("--diagnostics")["global"], true);
        assert_eq!(argument("--diagnostics")["require_equals"], true);
        assert_eq!(
            argument("--diagnostics")["possible_values"],
            json!(["json"])
        );

        let cli = Cli::try_parse_from([
            "dar",
            "scan",
            "table",
            "--column",
            "id",
            "--column",
            "value",
            "--diagnostics=json",
        ])
        .unwrap();
        let crate::Command::Scan {
            read: crate::RowReadArgs { columns, .. },
            ..
        } = cli.command
        else {
            panic!("expected scan")
        };
        assert_eq!(columns, ["id", "value"]);
        let error = Cli::try_parse_from(["dar", "scan", "table", "--column", "id", "--no-columns"])
            .err()
            .unwrap();
        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
        let error = Cli::try_parse_from(["dar", "scan", "table", "--diagnostics", "json"])
            .err()
            .unwrap();
        assert_eq!(error.kind(), clap::error::ErrorKind::NoEquals);

        let cli =
            Cli::try_parse_from(["dar", "query", "table", "--sql-file", "query.sql"]).unwrap();
        let crate::Command::Query(query) = cli.command else {
            panic!("expected query")
        };
        let advertised = commands
            .iter()
            .find(|command| command["name"] == "query")
            .unwrap();
        let arguments = advertised["arguments"].as_array().unwrap();
        for (name, value) in [
            ("--max-rows", query.max_rows),
            ("--memory-limit-bytes", query.memory_limit_bytes.get()),
            ("--spill-limit-bytes", query.spill_limit_bytes as usize),
        ] {
            let argument = arguments
                .iter()
                .find(|argument| argument["name"] == name)
                .unwrap();
            assert_eq!(argument["default_values"], json!([value.to_string()]));
        }
        assert!(
            arguments
                .iter()
                .any(|argument| argument["name"] == "--databricks-profile")
        );
    }

    #[test]
    fn schema_catalog_matches_clap_choices_and_existing_file_arguments() {
        let inventory = build_capability_details(None).unwrap();
        let commands = inventory["command"]["subcommands"].as_array().unwrap();
        let discovery = commands
            .iter()
            .find(|command| command["name"] == "capabilities")
            .unwrap();
        let schema_argument = discovery["arguments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|argument| argument["name"] == "--schema")
            .unwrap();
        let input_catalog = inventory["input_schemas"].as_array().unwrap();
        let output_catalog = inventory["output_schemas"].as_array().unwrap();
        let names: Vec<_> = input_catalog
            .iter()
            .chain(output_catalog)
            .map(|schema| schema["name"].clone())
            .collect();
        assert_eq!(schema_argument["possible_values"], json!(names));
        assert_eq!(
            names,
            [
                json!("storage-options"),
                json!("predicate"),
                json!("execution-options"),
                json!("tables"),
                json!("inspection"),
                json!("inspection-summary"),
                json!("diagnostic"),
                json!("completion"),
                json!("rows"),
                json!("query-plan"),
                json!("result-artifact"),
                json!("result-manifest"),
            ]
        );
        for schema in input_catalog.iter().chain(output_catalog) {
            let name = schema["name"].as_str().unwrap();
            let cli = Cli::try_parse_from(["dar", "capabilities", "--schema", name]).unwrap();
            let crate::Command::Capabilities(CapabilitiesArgs {
                schema: Some(kind), ..
            }) = cli.command
            else {
                panic!("expected named schema")
            };
            let document = kind.schema_document();
            assert_eq!(document["name"], name);
            if let Some(argument) = schema.get("argument") {
                assert_eq!(&document["argument"], argument);
                assert_eq!(document["kind"], "input_schema");
                assert!(commands.iter().any(|command| {
                    command["arguments"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|candidate| &candidate["name"] == argument)
                }));
            } else {
                assert_eq!(document["kind"], "output_schema");
                assert_eq!(document["stream"], schema["stream"]);
            }
        }
        assert_eq!(
            inventory["stdout_formats"]["inspect"]["schema"],
            "inspection"
        );
        assert_eq!(
            inventory["diagnostics"]["json"]["schemas"],
            json!(["diagnostic", "completion"])
        );
    }
}
