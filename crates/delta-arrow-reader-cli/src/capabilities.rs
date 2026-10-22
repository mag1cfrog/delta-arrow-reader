//! Discover commands, then look up the inputs needed for a particular read.
//!
//! `dar capabilities` lists the installed commands and how to request details.
//! For example, `--command scan` gives usage and limits, `--schema predicate`
//! gives a filter example, and `--schema predicate --detail int64` describes
//! one literal type. Add `--full` for complete command or authorization metadata,
//! or a validation schema. All views work without opening a table or resolving
//! authorization.

mod output_schemas;
mod read_support;
mod schemas;

pub(crate) use schemas::DiscoverySchema;

use std::{io, process::ExitCode, time::Duration};

use clap::{Arg, ArgAction, ArgGroup, Args, Command, CommandFactory, ValueEnum, error::ErrorKind};
use serde_json::{Value, json};
use snafu::ResultExt;

use crate::{
    Cli, Error, OutputSnafu, RuntimeSnafu, StdoutStats, build_completion_record,
    diagnostics::{self, DiagnosticFormat, FAILURE_DIAGNOSTIC_TIMEOUT},
    input::JsonInputKind,
    signals, write_stdout,
};

const MAX_INDEX_BYTES: usize = 2 * 1024;
const MAX_COMMAND_GUIDE_BYTES: usize = 4 * 1024;
const MAX_SCHEMA_GUIDE_BYTES: usize = 2 * 1024;
const MAX_AUTHORIZATION_GUIDE_BYTES: usize = 2 * 1024;
const MAX_FULL_RESPONSE_BYTES: usize = 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Args)]
#[command(group(ArgGroup::new("full_definition_target").args(["command", "schema", "authorization"])))]
pub(crate) struct CapabilitiesArgs {
    /// Show usage and limits for one command.
    #[arg(long, value_name = "NAME", value_parser = parse_command_name, conflicts_with_all = ["schema", "authorization", "all"])]
    command: Option<String>,
    /// Show an example and available fields or types for one JSON format.
    #[arg(long, value_enum, conflicts_with_all = ["command", "authorization", "all"])]
    schema: Option<DiscoverySchema>,
    /// Describe one field, predicate operator, or scalar type listed by --schema.
    #[arg(
        long,
        value_name = "NAME",
        requires = "schema",
        conflicts_with_all = ["command", "authorization", "all", "full"]
    )]
    detail: Option<String>,
    /// Show an authorization guide for a storage or catalog service.
    #[arg(long, value_enum, conflicts_with_all = ["command", "schema", "all"])]
    authorization: Option<read_support::AuthorizationGuide>,
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
        "deadline_ms": DEADLINE.as_millis(),
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
                "Summary checks scan protocol support, not Parquet or deletion-vector access. Names/types lookups omit checks; metadata is opt-in with --include-metadata.",
                "Summary stdout defaults to 4096 bytes; narrow selection or increase --max-output-bytes if needed. No total deadline. Omitting --view retains the unbounded full schema; redirect it to a file.",
            ],
        }),
        "scan" => json!({
            "example": [executable, "--diagnostics=json", "scan", "./table", "--limit", "1"],
            "stdout": "arrow_ipc_stream",
            "notes": [
                "Redirect stdout to a file or pipe; terminal output is rejected.",
                "--limit selects a sample. No output byte limit or total deadline.",
            ],
        }),
        "query" => json!({
            "example": [executable, "--diagnostics=json", "query", "./table", "--sql-file", "query.sql"],
            "stdout": "arrow_ipc_stream",
            "sql": {
                "argument": "--sql-file", "table": "t",
                "example": "SELECT count(*) AS row_count FROM t",
                "max_file_bytes": crate::query::MAX_SQL_BYTES,
            },
            "notes": [
                "One read-only SQL statement in a UTF-8 file, over one direct table as t. Unity Catalog targets are unsupported.",
                "Redirect stdout to a file or pipe. Resource ceilings fail the query; SQL LIMIT selects a smaller result.",
                "No total deadline. Memory is limited to the DataFusion pool, not process RSS; disk spill is disabled.",
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
                "table" | "table_version" | "columns" | "limit"
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
    if name == "query" {
        let ceilings: serde_json::Map<_, _> = command
            .get_arguments()
            .filter(|argument| {
                matches!(
                    argument.get_id().as_str(),
                    "max_rows" | "max_output_bytes" | "memory_limit_bytes"
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
    }
    let mut output_schemas = vec![DiscoverySchema::Diagnostic];
    if name != "help" {
        output_schemas.push(DiscoverySchema::Completion);
    }
    if name == "inspect" {
        output_schemas.push(DiscoverySchema::InspectionSummary);
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
    Ok(guide)
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
    let mut terminal_schemas = vec![DiscoverySchema::Diagnostic];
    if selected_command != Some("help") {
        terminal_schemas.push(DiscoverySchema::Completion);
    }
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
    let mut authorization = read_support::authorization_sources();
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
        "query": {"format": "arrow_ipc_stream"},
        "help": {"format": "text"},
    });
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
                "format_versions": {"diagnostic": crate::diagnostics::DIAGNOSTIC_FORMAT_VERSION, "completion": 1},
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
    let deadline = std::time::Instant::now() + DEADLINE;
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
                let stats = tokio::task::spawn_blocking(move || {
                    diagnostics::init_tracing(diagnostic_format);
                    write_discovery_response(options)
                }).await.map_err(io::Error::other).context(RuntimeSnafu {
                    reason: "discovery_output_task_failed",
                })??;
                diagnostic_format.write_terminal_record_when_writable(build_completion_record(
                    "capabilities", None, stats, None,
                )).await.context(OutputSnafu)
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

fn write_discovery_response(options: CapabilitiesArgs) -> Result<StdoutStats, Error> {
    let (document, max_output_bytes) = if let Some(schema) = options.schema {
        if options.full {
            (schema.schema_document(), MAX_FULL_RESPONSE_BYTES)
        } else {
            (
                schema.guide_document(options.detail.as_deref())?,
                MAX_SCHEMA_GUIDE_BYTES,
            )
        }
    } else if let Some(name) = options.command {
        if options.full {
            (
                build_capability_details(Some(&name))?,
                MAX_FULL_RESPONSE_BYTES,
            )
        } else {
            (build_command_guide(&name)?, MAX_COMMAND_GUIDE_BYTES)
        }
    } else if let Some(guide) = options.authorization {
        if options.full {
            (guide.full_document(), MAX_FULL_RESPONSE_BYTES)
        } else {
            (guide.guide_document(), MAX_AUTHORIZATION_GUIDE_BYTES)
        }
    } else if options.all {
        (build_capability_details(None)?, MAX_FULL_RESPONSE_BYTES)
    } else {
        (build_capability_index(), MAX_INDEX_BYTES)
    };
    let output = serialize_discovery_response(&document, max_output_bytes)?;
    write_stdout(&output)?;
    Ok(StdoutStats {
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
        }
        let guide = build_command_guide("query").unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), guide["sql"]["example"].as_str().unwrap()).unwrap();
        crate::query::ReadOnlySqlQuery::read_file(file.path().to_str().unwrap()).unwrap();
        let parsed =
            Cli::try_parse_from(["dar", "query", "table", "--sql-file", "query.sql"]).unwrap();
        let crate::Command::Query(args) = parsed.command else {
            panic!("query expected")
        };
        assert_eq!(
            guide["resource_defaults"]["--max-rows"],
            args.max_rows.to_string()
        );
        assert_eq!(
            guide["resource_defaults"]["--max-output-bytes"],
            args.max_output_bytes.to_string()
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
        for name in ["preview", "explain", "query --help", "", "QUERY"] {
            assert!(Cli::try_parse_from(["dar", "capabilities", "--command", name]).is_err());
            assert!(build_capability_details(Some(name)).is_err());
        }
        for invalid in [
            vec!["--full"],
            vec!["--all", "--full"],
            vec!["--detail", "rows"],
            vec!["--command", "scan", "--detail", "rows"],
            vec!["--authorization", "s3", "--detail", "rows"],
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
            vec!["--full", "--schema", "predicate"],
            vec!["--detail", "rows", "--schema", "completion"],
        ] {
            assert!(Cli::try_parse_from(["dar", "capabilities"].into_iter().chain(valid)).is_ok());
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
            StdoutStats {
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
            ["capabilities", "help", "inspect", "query", "scan"]
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
        for name in ["preview", "explain"] {
            let error = Cli::try_parse_from(["dar", name]).err().unwrap();
            assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
        }
        assert!(Cli::try_parse_from(["dar", "capabilities", "--schema", "tables"]).is_err());
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
        let crate::Command::Scan { columns, .. } = cli.command else {
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
            ("--max-output-bytes", query.max_output_bytes.get()),
            ("--memory-limit-bytes", query.memory_limit_bytes.get()),
        ] {
            let argument = arguments
                .iter()
                .find(|argument| argument["name"] == name)
                .unwrap();
            assert_eq!(argument["default_values"], json!([value.to_string()]));
        }
        assert!(
            !arguments
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
                json!("inspection"),
                json!("inspection-summary"),
                json!("diagnostic"),
                json!("completion"),
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
