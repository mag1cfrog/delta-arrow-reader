//! CLI failures, safe diagnostic representations, and exit statuses.

mod recovery;

use std::{fmt, io, process::ExitCode};

use arrow_schema::ArrowError;
use clap::error::ErrorKind;
use delta_arrow_reader::{DeltaReaderError, error_source_kind};
use serde_json::{Value, json};
use snafu::Snafu;

use super::{self as diagnostics, ErrorClassification, InputCorrection};
use crate::{cli::command_name, input::JsonInputKind, output, query, table_access::unity_catalog};

#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum Error {
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
    #[snafu(display("Invalid env file (reason={reason})."))]
    EnvFileInput {
        reason: &'static str,
        line: Option<usize>,
        expected: &'static str,
    },
    #[snafu(display("Could not read the input file."))]
    InputIo {
        source: io::Error,
        argument: &'static str,
    },
    #[snafu(display("Invalid storage authorization (reason={reason})."))]
    StorageAuthorization { reason: &'static str },
    #[snafu(display("{source}"))]
    CredentialFileValidation {
        source: Box<Error>,
        correction: InputCorrection,
    },
    #[snafu(display("{source}"))]
    ManifestAuthorizationInput {
        source: Box<Error>,
        field_path: String,
    },
    #[snafu(display("The reader runtime failed."))]
    Runtime {
        source: io::Error,
        reason: &'static str,
    },
    #[snafu(display("Could not write or flush stdout."))]
    Output { source: io::Error },
    #[snafu(display("Could not save the result artifact (reason={reason})."))]
    ArtifactIo {
        source: io::Error,
        reason: &'static str,
    },
    #[snafu(display("Could not encode the Arrow IPC stream."))]
    ArrowIpc { source: ArrowError },
    #[snafu(display("Could not encode the JSON result."))]
    JsonOutput { source: serde_json::Error },
    #[snafu(display("The result contains a type unsupported by typed JSON."))]
    UnsupportedJsonType { command: &'static str },
    #[snafu(display("{source}"))]
    Reader { source: DeltaReaderError },
    #[snafu(display("{source}"))]
    Configuration { source: DeltaReaderError },
    #[snafu(display("{source}"))]
    UnityCatalog { source: unity_catalog::Error },
    #[snafu(display("{source}"))]
    Query { source: query::QueryError },
    #[snafu(display("{source}"))]
    WithResolvedBindings {
        source: Box<Error>,
        bindings: Vec<Value>,
    },
    #[snafu(display("{source}"))]
    ArtifactPublicationInterrupted {
        source: Box<Error>,
        manifest_requested: bool,
    },
    #[snafu(display("Output exceeded its resource ceiling (reason={reason})."))]
    OutputLimit { reason: &'static str },
    #[snafu(display("Command exceeded its time budget."))]
    DeadlineExceeded,
    #[snafu(display("Command was cancelled."))]
    Cancelled { status: u8 },
}

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        match source {
            ArrowError::IoError(_, source)
                if source
                    .get_ref()
                    .is_some_and(|error| error.is::<output::OutputBytesExceeded>()) =>
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
            Self::ManifestAuthorizationInput { source, .. }
            | Self::CredentialFileValidation { source, .. }
            | Self::ArtifactPublicationInterrupted { source, .. }
            | Self::WithResolvedBindings { source, .. } => source.classification(),
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
            Self::EnvFileInput { reason, .. } => ErrorClassification {
                phase: "configuration",
                code: "invalid_input_env",
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
            Self::ArtifactIo { source, reason } => ErrorClassification {
                phase: "execution",
                code: "artifact_io",
                reason,
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
            Self::UnsupportedJsonType { .. } => ErrorClassification {
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
            Self::DeadlineExceeded => ErrorClassification {
                phase: "execution",
                code: "deadline_exceeded",
                reason: "command_deadline_exceeded",
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

    pub(crate) fn emit_tracing_diagnostic(&self) {
        let mut source_error = self;
        let mut manifest_field_path = None;
        loop {
            match source_error {
                Self::ManifestAuthorizationInput { source, field_path } => {
                    source_error = source;
                    manifest_field_path = Some(field_path.as_str());
                }
                Self::WithResolvedBindings { source, .. }
                | Self::ArtifactPublicationInterrupted { source, .. }
                | Self::CredentialFileValidation { source, .. } => source_error = source,
                _ => break,
            }
        }
        if let Self::Cancelled { status } = source_error {
            tracing::info!(target: "delta_arrow_reader_cli",
                event = "cli.cancelled", operation = "cli_command", outcome = "cancelled",
                error_phase = "execution", error_code = "command_cancelled", exit_status = status,
                hint = self.input_correction().map(|correction| correction.hint),
                "CLI command cancelled"
            );
            return;
        }
        if let Self::UnityCatalog {
            source: unity_catalog::Error::Cancelled { status },
        } = source_error
        {
            tracing::info!(target: "delta_arrow_reader_cli",
                event = "cli.cancelled", operation = "cli_command", outcome = "cancelled",
                error_phase = "execution", error_code = "uc_cancelled", exit_status = status,
                hint = self.input_correction().map(|correction| correction.hint),
                "Unity Catalog command cancelled"
            );
            return;
        }
        let correction = self.input_correction();
        let argument = manifest_field_path
            .map(|_| "--tables-file")
            .or_else(|| correction.as_ref().map(|value| value.argument.as_str()));
        let expected = correction.as_ref().map(|value| value.expected.as_str());
        let supplied = correction
            .as_ref()
            .and_then(|value| value.supplied.as_deref());
        let field_path = manifest_field_path.or_else(|| {
            correction
                .as_ref()
                .and_then(|value| value.field_path.as_deref())
        });
        let hint = correction
            .as_ref()
            .map(|value| value.hint)
            .or_else(|| match source_error {
                Self::Argument { kind, .. } => Some(diagnostics::argument_error_hint(*kind)),
                _ => None,
            });
        let ErrorClassification {
            phase,
            code,
            reason,
            source_kind,
        } = self.classification();
        // Core owns the terminal failure; CLI guidance supplements that event.
        if matches!(source_error, Self::Reader { .. }) {
            if correction.is_some() {
                tracing::error!(target: "delta_arrow_reader_cli",
                    event = "cli.input_correction", error_phase = phase, error_code = code,
                    error_reason = reason, argument, expected, supplied, hint, field_path,
                    "Correct the command input"
                );
            }
            return;
        }
        let argument_kind = match source_error {
            Self::Argument { kind, .. } => Some(tracing::field::debug(kind)),
            _ => None,
        };
        let (json_kind, json_line, json_column) = match source_error {
            Self::InputJson { source, .. } => (
                Some(tracing::field::debug(source.classify())),
                Some(source.line()),
                Some(source.column()),
            ),
            _ => (None, None, None),
        };
        let (io_kind, os_error) = match source_error {
            Self::InputIo { source, .. }
            | Self::Runtime { source, .. }
            | Self::Output { source }
            | Self::ArtifactIo { source, .. } => (
                Some(tracing::field::debug(source.kind())),
                source.raw_os_error(),
            ),
            _ => (None, None),
        };
        let env_file_line = match source_error {
            Self::EnvFileInput { line, .. } => *line,
            _ => None,
        };
        let message: &dyn fmt::Display = match source_error {
            Self::UnityCatalog { source } => source,
            Self::Query { source } => source,
            Self::StorageAuthorization { .. } => source_error,
            _ => &"CLI command failed",
        };
        tracing::error!(target: "delta_arrow_reader_cli",
            event = "cli.failed", operation = "cli_command", outcome = "failed",
            error_phase = phase, error_code = code, error_reason = reason,
            error_source_kind = source_kind, argument_kind, argument, expected, supplied, hint, field_path,
            json_kind, json_line, json_column, env_file_line, io_kind, os_error,
            "{message}"
        );
    }

    pub(crate) fn to_json_diagnostic(&self) -> Value {
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
                        reason: "preview_schema_limit_exceeded"
                            | "artifact_receipt_too_large"
                            | "artifact_manifest_too_large"
                    }
                ) =>
            {
                ("change_request", Some(false))
            }
            Some("resource_exhausted") => ("adjust_budget", Some(false)),
            Some("storage_full" | "storage_quota_exceeded") => ("fix_input", Some(false)),
            Some("unauthenticated") => ("authorize", Some(false)),
            Some("permission_denied") => ("request_access", Some(false)),
            Some("timeout" | "connection_reset" | "connection_aborted") => ("retry", Some(true)),
            Some("unsupported") => ("change_request", Some(false)),
            _ if phase == "configuration"
                || matches!(code, "invalid_projection" | "unsupported_predicate")
                || (code == "sql_planning"
                    && matches!(reason, "column_not_found" | "ambiguous_column")) =>
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
            Self::CredentialFileValidation { source, .. }
            | Self::ArtifactPublicationInterrupted { source, .. } => {
                record = source.to_json_diagnostic()
            }
            Self::Query { .. } if source_kind == Some("resource_exhausted") => {
                record["resource_details_command"] = json!([
                    command_name(),
                    "capabilities",
                    "--command",
                    "query",
                    "--detail",
                    "resources"
                ]);
            }
            Self::WithResolvedBindings { source, bindings } => {
                record = source.to_json_diagnostic();
                record["bindings"] = bindings.clone().into();
            }
            Self::ManifestAuthorizationInput { source, field_path } => {
                record = source.to_json_diagnostic();
                record["argument"] = "--tables-file".into();
                record["field_path"] = field_path.clone().into();
            }
            Self::UnityCatalog {
                source: source @ unity_catalog::Error::DatabricksCliNotFound,
            } => {
                record["hint"] = source.to_string().into();
            }
            Self::Cancelled { .. }
            | Self::UnityCatalog {
                source: unity_catalog::Error::Cancelled { .. },
            } => {
                record["hint"] = "Discard stdout. Retry only if intended, preserving the request, pinned snapshots, authorization and limits.".into();
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
            Self::EnvFileInput {
                line: Some(line), ..
            } => {
                record["line"] = (*line).into();
            }
            Self::InputIo { source, .. }
            | Self::Runtime { source, .. }
            | Self::Output { source }
            | Self::ArtifactIo { source, .. } => {
                record["io_kind"] = format!("{:?}", source.kind()).into();
                record["os_error"] = json!(source.raw_os_error());
                if matches!(self, Self::Output { .. }) {
                    record["hint"] = "Discard stdout. Restore the output consumer before retrying the same request and limits.".into();
                } else if reason == "discovery_deadline_exceeded" {
                    record["hint"] = "Discard stdout. Drain stdout and stderr before retrying the same discovery request.".into();
                }
            }
            _ => {}
        }
        if let Some(correction) = self.input_correction() {
            record["argument"] = correction.argument.into();
            // Budget names carry the units, and their hints already state the required change.
            if !matches!(reason, "memory_limit_exceeded" | "spill_limit_exceeded") {
                record["expected"] = correction.expected.into();
            }
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

    pub(crate) fn exit_code(&self) -> ExitCode {
        ExitCode::from(match self {
            Self::ManifestAuthorizationInput { source, .. }
            | Self::CredentialFileValidation { source, .. }
            | Self::ArtifactPublicationInterrupted { source, .. }
            | Self::WithResolvedBindings { source, .. } => return source.exit_code(),
            Self::Argument { .. }
            | Self::TerminalOutput
            | Self::InputJson { .. }
            | Self::InputValidation { .. }
            | Self::EnvFileInput { .. }
            | Self::InputIo { .. }
            | Self::StorageAuthorization { .. }
            | Self::Configuration { .. } => 2,
            Self::Runtime { .. }
            | Self::Reader { .. }
            | Self::ArrowIpc { .. }
            | Self::JsonOutput { .. }
            | Self::UnsupportedJsonType { .. } => 1,
            Self::Output { .. } | Self::ArtifactIo { .. } => 3,
            Self::UnityCatalog { source } => source.exit_status(),
            Self::Query { source } => source.exit_status(),
            Self::OutputLimit { .. } | Self::DeadlineExceeded => 1,
            Self::Cancelled { status } => *status,
        })
    }
}

#[cfg(test)]
mod tests;
