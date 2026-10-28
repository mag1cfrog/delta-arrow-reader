//! Discover commands, then look up the inputs needed for a particular read.
//!
//! `dar capabilities` lists the installed commands and how to request details.
//! For example, `--command scan` gives usage and limits, `--schema predicate`
//! gives a filter example, and `--schema predicate --detail int64` describes
//! one literal type. Add `--full` for complete command or authorization metadata,
//! or a validation schema. All views work without opening a table or resolving
//! authorization.

mod catalog;
mod env_file_guides;
mod output_schemas;
mod read_support;
mod schemas;

pub(crate) use schemas::DiscoverySchema;

use std::process::ExitCode;

use clap::{ArgGroup, Args, CommandFactory, ValueEnum, error::ErrorKind};
use serde_json::{Value, json};

use crate::{
    cli::Cli,
    diagnostics::{DiagnosticFormat, Error, FAILURE_DIAGNOSTIC_TIMEOUT, build_completion_record},
    input::JsonInputKind,
    output::{OutputStats, write_stdout},
    runtime::{DISCOVERY_TIMEOUT, run_discovery},
};

const MAX_INDEX_BYTES: usize = 2 * 1024;
const MAX_COMMAND_GUIDE_BYTES: usize = 4 * 1024;
const MAX_COMMAND_DETAIL_BYTES: usize = 2 * 1024;
const MAX_SCHEMA_GUIDE_BYTES: usize = 2 * 1024;
const MAX_AUTHORIZATION_GUIDE_BYTES: usize = 2 * 1024;
const MAX_FULL_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Args)]
#[command(group(ArgGroup::new("full_definition_target").args(["command", "schema", "authorization"])))]
#[command(group(ArgGroup::new("detail_target").args(["command", "schema", "authorization"])))]
pub(crate) struct CapabilitiesArgs {
    /// Show usage and limits for one command.
    #[arg(long, value_name = "NAME", value_parser = catalog::parse_command_name, conflicts_with_all = ["schema", "authorization", "all"])]
    command: Option<String>,
    /// Show an example and available fields or types for one JSON format.
    #[arg(long, value_enum, conflicts_with_all = ["command", "authorization", "all"])]
    schema: Option<DiscoverySchema>,
    /// Select a command topic, schema field/type, or authorization source/configuration from its guide.
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
                "Use --output PATH to save a complete result and return a JSON receipt. Otherwise redirect Arrow stdout to a file or pipe.",
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
                catalog::argument_name(argument),
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
                argument.get_default_values().first().map(|value| {
                    (
                        catalog::argument_name(argument),
                        json!(value.to_string_lossy()),
                    )
                })
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
                .any(|argument| catalog::argument_name(argument) == kind.argument())
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
            "files": "--env-file PATH selects a credential file; optional --storage-options-file PATH then supplies nonsecret connection settings only. Never expand credential values into arguments.",
            "guides": authorization_guides,
            "details_command": [executable, "capabilities", "--authorization", "NAME"],
        });
    }
    let mut output_schemas = vec![DiscoverySchema::Diagnostic, DiscoverySchema::Completion];
    if name == "inspect" {
        output_schemas.push(DiscoverySchema::InspectionSummary);
    } else if matches!(name, "query" | "preview") {
        output_schemas.push(DiscoverySchema::Rows);
        guide["json_values"] = json!(
            "Decode JSON cells using schema.fields. Decimal strings are unscaled integers: value = integer * 10^(-scale), using scale from data_type. Encodings: capabilities --schema rows."
        );
    } else if name == "explain" {
        output_schemas.push(DiscoverySchema::QueryPlan);
    }
    if matches!(name, "scan" | "query") {
        output_schemas.push(DiscoverySchema::ResultArtifact);
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
            "authorization": if name == "query" {
                "Local: none. UC: Databricks profile. Remote: --auth-source or --env-file; a storage-options file alone selects json. Each remote tables-file binding needs authorization."
            } else {
                "Local: none. UC: Databricks profile. Remote: --auth-source or --env-file; a storage-options file alone selects json."
            },
            "publication": "Publishes one complete file at PATH; never overwrites it. Require exit 0. A failure after publication may leave a complete result; preserve it and choose an unused path if retrying.",
            "receipt": "Default stdout: result_artifact v2 with the absolute result path, at most 8192 bytes. --diagnostics=json: completion v3 with command/output_bytes (receipt bytes).",
            "read_result": if name == "query" {
                "Read the file using --format (default arrow), regardless of its extension. JSON uses schema.fields to decode columns. Read needed values locally and print only task results; keep complete files for reuse."
            } else {
                "Open the file with an Arrow IPC stream reader, regardless of its extension. Read needed values locally and print only task results; keep the complete file for reuse."
            },
            "storage": "Temporary files: owner-only on Unix; Windows needs a private inherited directory ACL. Crashes/uninterruptible IO may leave temps; no power-loss durability guarantee.",
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
            correction: Some(crate::diagnostics::InputCorrection {
                argument: "--detail".into(),
                field_path: None,
                expected: match name {
                    "query" => "runtime, artifacts, or resources",
                    "scan" => "runtime or artifacts",
                    _ => "runtime",
                }
                .into(),
                supplied: None,
                hint: "Use --detail with a listed value for this command.",
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
        "cleanup_ms": if is_table_command { crate::runtime::CLEANUP_GRACE.as_millis() } else { 0 },
        "failure_reporting_ms": FAILURE_DIAGNOSTIC_TIMEOUT.as_millis(),
        "signals": "Unix SIGINT exits 130; SIGTERM exits 143, including Linux PID 1. Windows Ctrl+C exits 130.",
        "output": "Require exit 0; with --diagnostics=json, also validate the stderr completion for its command and format_version. Otherwise discard stdout, even if it looks complete.",
        "retry": if is_table_command { "Retry only if intended. Preserve the request, pinned snapshots, authorization and other limits; choose any larger deadline explicitly." } else { "Drain stdout and stderr before retrying the same discovery request." },
        "termination_limits": "SIGKILL and OS-uninterruptible IO cannot guarantee cleanup. Closed or blocked stderr can prevent a complete diagnostic.",
        "broken_pipe": if is_table_command { "Exit 3. Unix detects a closed stdout pipe during work; other platforms detect it on the next write, with the selected deadline still applying." } else { "Exit 3 when an output write fails." },
    }))
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
    })
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
                catalog::build_capability_details(Some(&name))?,
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
                service.build_authorization_detail(detail)?,
                MAX_AUTHORIZATION_GUIDE_BYTES,
            )
        } else {
            (
                service.build_authorization_guide(),
                MAX_AUTHORIZATION_GUIDE_BYTES,
            )
        }
    } else if options.all {
        (
            catalog::build_capability_details(None)?,
            MAX_FULL_RESPONSE_BYTES,
        )
    } else {
        (catalog::build_capability_index(), MAX_INDEX_BYTES)
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
                Cli::try_parse_from(&example).unwrap();
                if matches!(
                    command.get_name(),
                    "inspect" | "scan" | "preview" | "query" | "explain"
                ) {
                    let files = guide["authorization"]["files"].as_str().unwrap();
                    assert!(files.contains("--env-file PATH"));
                    assert!(files.contains("--storage-options-file PATH"));
                    let mut with_files = example;
                    with_files.extend([
                        "--env-file",
                        "credentials.env",
                        "--storage-options-file",
                        "connection.json",
                    ]);
                    Cli::try_parse_from(with_files).unwrap();
                }
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
            assert!(guide["authorization"].get("env_file").is_none());
        }
        let guide = build_command_guide("query").unwrap();
        assert_eq!(guide["resource_defaults_format"], "json");
        assert_eq!(guide["completion_format_version"], 2);
        for name in ["query", "preview"] {
            let guide = build_command_guide(name).unwrap();
            let decoding = guide["json_values"].as_str().unwrap();
            assert!(decoding.contains("schema.fields"));
            assert!(decoding.contains("integer * 10^(-scale)"));
            assert!(decoding.contains("scale from data_type"));
            assert!(decoding.contains("capabilities --schema rows"));
        }
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), guide["sql"]["example"].as_str().unwrap()).unwrap();
        crate::query::ReadOnlySqlQuery::read_file_with_bytes(file.path().to_str().unwrap())
            .unwrap();
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
        let crate::cli::Command::Query(args) = parsed.command else {
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
        let details = catalog::build_capability_details(Some("capabilities")).unwrap();
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
            assert!(catalog::build_capability_details(Some(name)).is_err());
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
    fn command_details_cover_only_the_selected_operation() {
        for name in ["scan", "query"] {
            let guide = build_command_guide(name).unwrap();
            assert!(
                !guide["output_schemas"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("result-manifest"))
            );
            let detail = build_command_detail(name, "artifacts").unwrap();
            for deferred in ["manifest", "manifest_schema", "replay"] {
                assert!(detail.get(deferred).is_none());
            }
            assert!(
                detail["publication"]
                    .as_str()
                    .unwrap()
                    .contains("never overwrites")
            );
            if name == "scan" {
                let text = detail.to_string();
                for query_only in ["--format", "--tables-file", "schema.fields", "SQL"] {
                    assert!(!text.contains(query_only), "{query_only}");
                }
                assert!(
                    detail["read_result"]
                        .as_str()
                        .unwrap()
                        .contains("Arrow IPC stream reader")
                );
            }
        }
        for (command, expected) in [
            ("inspect", "runtime"),
            ("scan", "runtime or artifacts"),
            ("query", "runtime, artifacts, or resources"),
        ] {
            let Error::Argument {
                correction: Some(correction),
                ..
            } = build_command_detail(command, "unknown").unwrap_err()
            else {
                panic!("expected a detail correction")
            };
            assert_eq!(correction.expected, expected);
            assert!(!correction.hint.contains("--command query"));
        }
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
}
