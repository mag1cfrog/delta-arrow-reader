//! Discover commands, then look up the inputs needed for a particular read.
//!
//! `dar capabilities` lists the installed commands and how to request details.
//! For example, `--command scan` gives usage and limits, `--schema predicate`
//! gives a filter example, and `--schema predicate --detail int64` describes
//! one literal type. Add `--full` for complete command or authorization metadata,
//! or a validation schema. All views work without opening a table or resolving
//! authorization.

mod catalog;
mod command_guides;
mod env_file_guides;
mod output_schemas;
mod read_support;
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
                command_guides::build_command_detail(&name, detail)?,
                MAX_COMMAND_DETAIL_BYTES,
            )
        } else if options.full {
            (
                catalog::build_capability_details(Some(&name))?,
                MAX_FULL_RESPONSE_BYTES,
            )
        } else {
            (
                command_guides::build_command_guide(&name)?,
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
    use crate::cli::Cli;

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
        assert!(command_guides::build_command_detail("scan", "rows").is_err());
        assert!(command_guides::build_command_detail("scan", "resources").is_err());
        let resources = command_guides::build_command_detail("query", "resources").unwrap();
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
}
