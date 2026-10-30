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
                Self::WithCleanupFailure { source, cleanup } => {
                    source.emit_tracing_diagnostic();
                    cleanup.emit_tracing_diagnostic();
                    return;
                }
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
            Self::WithCleanupFailure { source, cleanup } => {
                record = source.to_json_diagnostic();
                let mut cleanup = cleanup.to_json_diagnostic();
                if let Some(fields) = cleanup.as_object_mut() {
                    for field in ["format_version", "kind", "terminal"] {
                        fields.remove(field);
                    }
                }
                record["cleanup_error"] = cleanup;
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
                record["hint"] = "Discard stdout. Retry only if intended, preserving the request, snapshot selection, authorization and limits.".into();
            }
            Self::UnityCatalog { source } => {
                if let Some(details) = source.command_error_details() {
                    if let Some(operation) = details.operation {
                        record["command_operation"] = operation.into();
                    }
                    if let Some(kind) = details.io_kind {
                        record["io_kind"] = format!("{kind:?}").into();
                        record["os_error"] = json!(details.os_error);
                        if kind == std::io::ErrorKind::PermissionDenied {
                            record["recovery"] = "fix_input".into();
                        }
                    }
                    if let Some(code) = details.exit_code {
                        record["command_exit_code"] = code.into();
                    }
                    if let Some(signal) = details.signal {
                        record["command_signal"] = signal.into();
                    }
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
        limit_diagnostic_record(&mut record);
        record
    }
}

fn limit_diagnostic_record(record: &mut Value) {
    // Keep every binding's version and replay restriction, including 64 maximum-length aliases.
    let max_record_bytes: usize = if record["bindings"].as_array().is_some_and(|bindings| {
        bindings
            .iter()
            .any(|binding| binding["time_travel"] == false)
    }) {
        10 * 1024
    } else {
        8 * 1024
    };
    // Prefer the primary failure's details over optional cleanup guidance.
    for field in ["expected", "hint"] {
        if record.to_string().len() < max_record_bytes {
            return;
        }
        if let Some(cleanup) = record
            .get_mut("cleanup_error")
            .and_then(Value::as_object_mut)
        {
            cleanup.remove(field);
        }
    }
    let Some(message) = record.get("upstream_message").and_then(Value::as_str) else {
        return;
    };
    if record.to_string().len() < max_record_bytes {
        return;
    }
    let message = message.to_owned();
    record["upstream_message"] = "".into();
    record["upstream_message_truncated"] = true.into();
    // Reserve the newline and all other fields before measuring the escaped excerpt.
    let available = max_record_bytes.saturating_sub(record.to_string().len() + 1);
    let (mut start, mut end) = (0, message.len());
    while start < end {
        let midpoint = (start + end).div_ceil(2);
        let prefix = &message[..message.floor_char_boundary(midpoint)];
        if json!(prefix).to_string().len() - 2 <= available {
            start = midpoint;
        } else {
            end = midpoint - 1;
        }
    }
    record["upstream_message"] = message[..message.floor_char_boundary(start)].into();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_diagnostics_fit_with_maximum_snapshot_bindings() {
        for (latest_only_count, limit) in [(0, 8192), (1, 10240), (64, 10240)] {
            for kind in [
                std::io::ErrorKind::StorageFull,
                std::io::ErrorKind::QuotaExceeded,
            ] {
                let bindings: Vec<_> = (0..crate::input::MAX_TABLES)
                    .map(|index| {
                        let mut binding = json!({
                            "alias": format!("table_{index:057}"),
                            "table_version": (u64::MAX - 1).to_string(),
                        });
                        if index < latest_only_count {
                            binding["time_travel"] = json!(false);
                        }
                        binding
                    })
                    .collect();
                let source = Error::Query {
                    source: crate::query::QueryError::Execution {
                        source: datafusion::common::DataFusionError::ResourcesExhausted(
                            "Failed to allocate additional memory".into(),
                        ),
                    },
                };
                let expected = source.to_json_diagnostic();
                let cleanup = Error::Query {
                    source: crate::query::QueryError::SpillCleanup {
                        source: kind.into(),
                    },
                };
                let expected_cleanup = cleanup.to_json_diagnostic();
                let error = Error::WithCleanupFailure {
                    source: Box::new(Error::WithResolvedBindings {
                        source: Box::new(source),
                        bindings: bindings.clone(),
                    }),
                    cleanup: Box::new(cleanup),
                };
                let mut record = error.to_json_diagnostic();
                assert!(record.to_string().len() < limit, "{record}");
                assert_eq!(record["bindings"], json!(bindings));
                for field in [
                    "phase",
                    "code",
                    "reason",
                    "source_kind",
                    "retryable",
                    "recovery",
                    "argument",
                ] {
                    assert_eq!(record["cleanup_error"][field], expected_cleanup[field]);
                }
                if latest_only_count > 0 {
                    assert_eq!(record["cleanup_error"]["hint"], expected_cleanup["hint"]);
                    assert_eq!(
                        record["cleanup_error"]["expected"],
                        expected_cleanup["expected"]
                    );
                }
                let fields = record.as_object_mut().unwrap();
                fields.remove("bindings");
                fields.remove("cleanup_error");
                assert_eq!(record, expected);
            }
        }
    }

    #[test]
    fn uc_excerpts_fit_with_all_snapshot_bindings_without_discarding_recovery() {
        for (latest_only_count, limit, with_cleanup) in [
            (0, 8192, false),
            (1, 10240, false),
            (64, 10240, false),
            (0, 8192, true),
            (1, 10240, true),
            (64, 10240, true),
        ] {
            for version in [0, u64::MAX] {
                let bindings: Vec<_> = (0..crate::input::MAX_TABLES)
                    .map(|index| {
                        let mut binding = json!({
                            "alias": format!("table_{index:057}"),
                            "table_version": version.to_string(),
                        });
                        if index < latest_only_count {
                            binding["time_travel"] = json!(false);
                        }
                        binding
                    })
                    .collect();
                for message in ["x".repeat(1024), "\"".repeat(1024), "界".repeat(341)] {
                    let mut source = unity_catalog::Error::ReadCredentialsRequestFailed {
                        details: Default::default(),
                    };
                    if let unity_catalog::Error::ReadCredentialsRequestFailed { details } =
                        &mut source
                    {
                        details.message = Some(message.clone());
                        details.source_kind = Some("permission_denied");
                    }
                    let error = Error::WithResolvedBindings {
                        source: Box::new(Error::UnityCatalog { source }),
                        bindings: bindings.clone(),
                    };
                    let error = if with_cleanup {
                        Error::WithCleanupFailure {
                            source: Box::new(error),
                            cleanup: Box::new(Error::Query {
                                source: crate::query::QueryError::SpillCleanup {
                                    source: std::io::ErrorKind::StorageFull.into(),
                                },
                            }),
                        }
                    } else {
                        error
                    };
                    let mut record = error.to_json_diagnostic();
                    assert!(record.to_string().len() < limit);
                    assert_eq!(record["bindings"], json!(bindings));
                    assert_eq!(record["reason"], "uc_permission_denied");
                    assert_eq!(record["recovery"], "request_access");
                    assert_eq!(record["retryable"], false);
                    assert!(record["hint"].is_string());
                    if with_cleanup {
                        assert_eq!(record["cleanup_error"]["code"], "spill_cleanup");
                        assert_eq!(record["cleanup_error"]["source_kind"], "storage_full");
                        assert_eq!(record["cleanup_error"]["recovery"], "fix_input");
                    }
                    let kept = record["upstream_message"].as_str().unwrap();
                    assert!(!kept.is_empty() && message.starts_with(kept));
                    if let Some(next) = message[kept.len()..].chars().next() {
                        assert_eq!(record["upstream_message_truncated"], true);
                        let longer = format!("{kept}{next}");
                        record["upstream_message"] = longer.into();
                        assert!(
                            record.to_string().len() >= limit,
                            "discarded an excerpt character that would fit"
                        );
                    } else {
                        assert!(record.get("upstream_message_truncated").is_none());
                    }
                }
            }
        }
    }
}
