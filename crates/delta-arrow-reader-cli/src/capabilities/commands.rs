//! Command inventory and argument metadata derived from the CLI parser.

mod guides;
mod read_support;

pub(super) use guides::{build_command_detail, build_command_guide};
pub(super) use read_support::command_read_support;

use clap::{Arg, ArgAction, Command, CommandFactory, ValueEnum, error::ErrorKind};
use serde_json::{Value, json};

use super::{DiscoverySchema, MAX_FULL_RESPONSE_BYTES, authorization, discovery_limits};
use crate::{cli::Cli, diagnostics::Error, input::JsonInputKind};

pub(super) fn parse_command_name(name: &str) -> Result<String, &'static str> {
    let mut cli = Cli::command();
    cli.build();
    cli.get_subcommands()
        .any(|command| command.get_name() == name && !command.is_hide_set())
        .then(|| name.to_owned())
        .ok_or("unknown command; run capabilities to list installed commands")
}

pub(super) fn build_capability_index() -> Value {
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

pub(super) fn build_capability_details(selected_command: Option<&str>) -> Result<Value, Error> {
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
    let mut authorization = authorization::build_authorization_capabilities();
    let mut read_support = command_read_support();
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

#[cfg(test)]
mod tests;
