//! CLI failures, safe diagnostic representations, and exit statuses.

mod recovery;
mod render;

use std::{fmt, io, process::ExitCode};

use arrow_schema::ArrowError;
use clap::error::ErrorKind;
use delta_arrow_reader::{DeltaReaderError, error_source_kind};
use serde_json::Value;
use snafu::Snafu;

use super::{ErrorClassification, InputCorrection};
use crate::{input::JsonInputKind, output, query, table_access::unity_catalog};

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
