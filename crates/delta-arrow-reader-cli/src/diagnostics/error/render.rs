//! Safe text and JSON representations of CLI failures.

use std::fmt;

use serde_json::{Value, json};

use super::Error;
use crate::{
    cli::command_name,
    diagnostics::{self, ErrorClassification},
    table_access::unity_catalog,
};

impl Error {
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
                Self::UnityCatalog { source } => source
                    .command_error_details()
                    .and_then(|details| details.hint()),
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
            Self::Query { source } => {
                if let Some(cause) = source.reader_cause() {
                    record["cause"] = json!({
                        "phase": cause.phase().as_str(),
                        "code": cause.code(),
                        "reason": cause.reason(),
                        "source_kind": cause.source_kind(),
                    });
                }
                if source_kind == Some("resource_exhausted") {
                    record["resource_details_command"] = json!([
                        command_name(),
                        "capabilities",
                        "--command",
                        "query",
                        "--detail",
                        "resources"
                    ]);
                }
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
            Self::UnityCatalog { source } => {
                if let Some(details) = source.command_error_details() {
                    if let Some(message) = &details.message {
                        record["upstream_message"] = message.clone().into();
                    }
                    if details.message_truncated {
                        record["upstream_message_truncated"] = true.into();
                    }
                    if let Some(hint) = details.hint() {
                        record["hint"] = hint.into();
                    }
                }
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
}
