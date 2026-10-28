//! Discover commands, then look up the inputs needed for a particular read.
//!
//! `dar capabilities` lists the installed commands and how to request details.
//! For example, `--command scan` gives usage and limits, `--schema predicate`
//! gives a filter example, and `--schema predicate --detail int64` describes
//! one literal type. Add `--full` for complete command or authorization metadata,
//! or a validation schema. All views work without opening a table or resolving
//! authorization.

mod authorization;
mod commands;
mod schemas;

pub(crate) use schemas::DiscoverySchema;

use std::process::ExitCode;

use clap::{ArgGroup, Args, error::ErrorKind};
use serde_json::{Value, json};

use crate::{
    diagnostics::{DiagnosticFormat, Error, FAILURE_DIAGNOSTIC_TIMEOUT, build_completion_record},
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
    #[arg(long, value_name = "NAME", value_parser = commands::parse_command_name, conflicts_with_all = ["schema", "authorization", "all"])]
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
    authorization: Option<authorization::AuthorizationService>,
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
                commands::build_command_detail(&name, detail)?,
                MAX_COMMAND_DETAIL_BYTES,
            )
        } else if options.full {
            (
                commands::build_capability_details(Some(&name))?,
                MAX_FULL_RESPONSE_BYTES,
            )
        } else {
            (
                commands::build_command_guide(&name)?,
                MAX_COMMAND_GUIDE_BYTES,
            )
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
            commands::build_capability_details(None)?,
            MAX_FULL_RESPONSE_BYTES,
        )
    } else {
        (commands::build_capability_index(), MAX_INDEX_BYTES)
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
mod tests;
