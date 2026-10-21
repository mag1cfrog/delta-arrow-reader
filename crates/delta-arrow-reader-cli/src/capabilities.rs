//! Commands, input/output schemas, and output formats available in this executable.
//!
//! `dar capabilities` returns this inventory without opening a table or resolving
//! authorization. For example, a caller can find that `scan --column` is repeatable
//! and that `--diagnostics` accepts `json` with an equals sign.
//! `dar capabilities --schema predicate` returns the JSON row-filter schema.
//! The authorization and read-support sections describe which inputs can be
//! combined. For example, a Databricks profile can authorize `scan`, while
//! `query` currently requires a direct table location.

mod output_schemas;
mod read_support;
mod schemas;

pub(crate) use schemas::DiscoverySchema;

use std::{io, process::ExitCode, time::Duration};

use clap::{Arg, ArgAction, Command, CommandFactory, ValueEnum};
use serde_json::{Value, json};
use snafu::ResultExt;

use crate::{
    Cli, Error, OutputSnafu, RuntimeSnafu, StdoutStats, build_completion_record,
    diagnostics::{self, DiagnosticFormat, FAILURE_DIAGNOSTIC_TIMEOUT},
    input::JsonInputKind,
    signals, write_stdout,
};

const MAX_OUTPUT_BYTES: usize = 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(5);

fn build_capabilities() -> Value {
    let mut command = Cli::command();
    command.build();
    let input_schemas: Vec<_> = JsonInputKind::value_variants()
        .iter()
        .map(|kind| json!({"name": kind, "argument": kind.argument()}))
        .collect();
    json!({
        "format_version": 1,
        "kind": "capabilities",
        "cli_version": env!("CARGO_PKG_VERSION"),
        "command": describe_command(&command),
        "input_schemas": input_schemas,
        "output_schemas": [
            {"name": DiscoverySchema::Inspection, "stream": "stdout"},
            {"name": DiscoverySchema::Diagnostic, "stream": "stderr"},
            {"name": DiscoverySchema::Completion, "stream": "stderr"},
        ],
        "authorization": read_support::authorization_sources(),
        "read_support": read_support::command_read_support(),
        "stdout_formats": {
            "capabilities": {"format": "json", "format_version": 1},
            "inspect": {"format": "json", "format_version": 1, "schema": DiscoverySchema::Inspection},
            "scan": {"format": "arrow_ipc_stream"},
            "query": {"format": "arrow_ipc_stream"},
            "help": {"format": "text"},
        },
        "diagnostics": {
            "stream": "stderr",
            "default_format": "text",
            "json": {
                "option": "--diagnostics=json", "format_version": 1,
                "schemas": [DiscoverySchema::Diagnostic, DiscoverySchema::Completion],
            },
        },
        "limits": {
            "max_output_bytes": MAX_OUTPUT_BYTES,
            "deadline_ms": DEADLINE.as_millis(),
            "failure_diagnostic_timeout_ms": FAILURE_DIAGNOSTIC_TIMEOUT.as_millis(),
        },
    })
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
    schema: Option<DiscoverySchema>,
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
                    write_discovery_response(schema)
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

fn write_discovery_response(schema: Option<DiscoverySchema>) -> Result<StdoutStats, Error> {
    let document = schema.map_or_else(build_capabilities, DiscoverySchema::schema_document);
    let mut output = document.to_string().into_bytes();
    output.push(b'\n');
    if output.len() > MAX_OUTPUT_BYTES {
        return Err(Error::OutputLimit {
            reason: "discovery_output_too_large",
        });
    }
    write_stdout(&output)?;
    Ok(StdoutStats {
        bytes_written: output.len() as u64,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use clap::Parser;

    use super::*;

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
        let inventory = build_capabilities();
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
        let inventory = build_capabilities();
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
        let inventory = build_capabilities();
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
                json!("diagnostic"),
                json!("completion"),
            ]
        );
        for schema in input_catalog.iter().chain(output_catalog) {
            let name = schema["name"].as_str().unwrap();
            let cli = Cli::try_parse_from(["dar", "capabilities", "--schema", name]).unwrap();
            let crate::Command::Capabilities { schema: Some(kind) } = cli.command else {
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
