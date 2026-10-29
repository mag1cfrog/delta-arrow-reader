//! Input corrections and retry guidance for typed CLI failures.

use std::io;

use super::Error;
use crate::{diagnostics::InputCorrection, input, query};

impl Error {
    pub(crate) fn input_correction(&self) -> Option<InputCorrection> {
        let (input_kind, hint) = match self {
            Self::ArtifactPublicationInterrupted {
                source,
                manifest_requested,
            } => {
                let mut correction = source.input_correction().unwrap_or(InputCorrection {
                    argument: "--output".into(),
                    expected: "an unused output path".into(),
                    field_path: None,
                    supplied: None,
                    hint: "",
                });
                correction.hint = if *manifest_requested {
                    "Check PATH and PATH.manifest.json before retrying; a matching manifest with completion=complete may already exist. Otherwise choose unused names and preserve the pinned request and authorization; adjust only the reported budget."
                } else {
                    "A complete result may already exist at PATH. Preserve it; retry with an unused output path if completion is uncertain. Keep the pinned request and authorization; adjust only the reported budget."
                };
                return Some(correction);
            }
            Self::WithResolvedBindings { source, .. } => return source.input_correction(),
            Self::Argument { correction, .. } => return correction.clone(),
            Self::InputValidation { correction, .. }
            | Self::CredentialFileValidation { correction, .. } => return Some(correction.clone()),
            Self::ManifestAuthorizationInput { source, field_path } => {
                let mut correction = source.input_correction()?;
                correction.field_path = Some(field_path.clone());
                correction.argument = "--tables-file".into();
                if matches!(source.as_ref(), Self::InputIo { source, .. } if source.kind() == io::ErrorKind::NotFound)
                {
                    correction.hint = "Provide an existing input file; relative references use the manifest directory.";
                }
                return Some(correction);
            }
            Self::EnvFileInput { expected, .. } => {
                return Some(InputCorrection {
                    argument: "--env-file".into(),
                    field_path: None,
                    expected: (*expected).into(),
                    supplied: None,
                    hint: "Use one NAME=VALUE record per line; quotes must enclose the whole value.",
                });
            }
            Self::InputJson {
                correction: Some(correction),
                ..
            } => return Some(correction.clone()),
            Self::InputJson {
                source, input_kind, ..
            } => (
                *input_kind,
                match source.classify() {
                    serde_json::error::Category::Syntax => {
                        "Fix JSON syntax at the reported line and column."
                    }
                    serde_json::error::Category::Eof => {
                        "Complete the JSON document at the reported line and column."
                    }
                    serde_json::error::Category::Data => input_kind.schema_hint(),
                    serde_json::error::Category::Io => "Check that the input file is readable.",
                },
            ),
            Self::StorageAuthorization {
                reason: "file_output_requires_auth_source",
            } => {
                return Some(InputCorrection {
                    argument: "--auth-source".into(),
                    expected: "an explicit credential source".into(),
                    field_path: None,
                    supplied: None,
                    hint: "Select --auth-source, --env-file, or a credential JSON --storage-options-file. For --tables-file, set authorization for every remote binding.",
                });
            }
            Self::ArtifactIo {
                reason: "artifact_manifest_destination_exists" | "artifact_manifest_publish_failed",
                ..
            } => {
                return Some(InputCorrection {
                    argument: "--output".into(),
                    expected: "unused result and manifest names in a writable directory".into(),
                    field_path: None,
                    supplied: None,
                    hint: "Check PATH and PATH.manifest.json for a matching result.path and completion=complete. Otherwise retry with unused names, preserving inputs, snapshots and limits.",
                });
            }
            Self::ArtifactIo { .. } => {
                return Some(InputCorrection {
                    argument: "--output".into(),
                    expected: "an unused output path in a writable directory".into(),
                    field_path: None,
                    supplied: None,
                    hint: "Check the output directory and preserve any existing result. If completion is uncertain, retry with an unused path; keep inputs, snapshots and limits.",
                });
            }
            Self::OutputLimit {
                reason: "artifact_receipt_too_large" | "artifact_manifest_too_large",
            } => {
                return Some(InputCorrection {
                    argument: "--output".into(),
                    expected: "shorter local paths".into(),
                    field_path: None,
                    supplied: None,
                    hint: "Use shorter output or input references. Preserve the request, snapshots and limits; no artifacts were published.",
                });
            }
            Self::InputIo { source, argument } => {
                let hint = match source.kind() {
                    io::ErrorKind::NotFound => {
                        "Provide an existing input file; relative paths use the working directory."
                    }
                    io::ErrorKind::PermissionDenied => "Request read access to the input file.",
                    _ => "Check that the input path names a readable file.",
                };
                return Some(InputCorrection {
                    argument: (*argument).to_owned(),
                    field_path: None,
                    expected: "a readable local file".to_owned(),
                    supplied: None,
                    hint,
                });
            }
            Self::Configuration { source } => {
                return input::execution_options_correction(source.reason());
            }
            Self::Query {
                source: query::QueryError::SpillDirectory { .. },
            } => {
                return Some(InputCorrection {
                    argument: "--spill-dir".into(),
                    expected: "an existing writable local directory".into(),
                    field_path: None,
                    supplied: None,
                    hint: "Choose a writable directory for temporary table data; on Windows its inherited ACL must be private.",
                });
            }
            Self::Query { source } => {
                let classification = source.classification();
                let (argument, expected, hint) = match classification.reason {
                    "column_not_found" => (
                        "--sql-file",
                        "an existing SQL column reference",
                        "Check column names and aliases; inspect the selected table only if its columns are unknown.",
                    ),
                    "ambiguous_column" => (
                        "--sql-file",
                        "an unambiguous SQL column reference",
                        "Qualify the column reference with its table or subquery alias.",
                    ),
                    "spill_disabled" => (
                        "--spill-limit-bytes",
                        "a positive byte budget together with --spill-dir PATH",
                        "Discard stdout. Enable spill in an existing private writable directory; preserve SQL, snapshot bindings and other limits.",
                    ),
                    "memory_limit_exceeded" => (
                        "--memory-limit-bytes",
                        "a larger DataFusion pool budget in bytes",
                        "Discard stdout. Increase only --memory-limit-bytes; preserve SQL, snapshot bindings and other limits.",
                    ),
                    "spill_limit_exceeded" => (
                        "--spill-limit-bytes",
                        "a larger spill budget in bytes",
                        "Discard stdout. Increase only --spill-limit-bytes; preserve SQL, snapshot bindings and other limits.",
                    ),
                    _ if source.reader_cause().is_none()
                        && matches!(
                            classification.source_kind,
                            Some("storage_full" | "storage_quota_exceeded")
                        ) =>
                    {
                        (
                            "--spill-dir",
                            "writable temporary storage with available space and filesystem quota",
                            "Discard stdout. Free space or choose another private spill directory; preserve SQL, snapshot bindings and budgets.",
                        )
                    }
                    _ => return None,
                };
                return Some(InputCorrection {
                    argument: argument.into(),
                    expected: expected.into(),
                    hint,
                    field_path: None,
                    supplied: None,
                });
            }
            Self::Reader { source } => return InputCorrection::from_reader(source),
            Self::OutputLimit {
                reason: "plan_output_limit_exceeded",
            } => {
                return Some(InputCorrection {
                    argument: "--max-output-bytes".to_owned(),
                    expected: "enough bytes for the complete selected plan detail".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Select fewer --field values or explicitly increase --max-output-bytes. Preserve SQL, authorization, bindings, actual versions and other budgets. The summary still includes every binding.",
                });
            }
            Self::OutputLimit {
                reason: "inspection_output_limit_exceeded",
            } => {
                return Some(InputCorrection {
                    argument: "--max-output-bytes".to_owned(),
                    expected: "a budget large enough for the complete selected inspection"
                        .to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "For types only, omit --include-metadata. Narrow the selection or increase --max-output-bytes; keep the same authorization and --table-version N.",
                });
            }
            Self::OutputLimit {
                reason: "json_output_limit_exceeded",
            } => {
                return Some(InputCorrection {
                    argument: "--max-output-bytes".to_owned(),
                    expected: "enough bytes for the complete JSON result".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "For a large complete result, use --output PATH; if already saving a file, increase --max-output-bytes explicitly. Preserve SQL, authorization, bindings and actual versions.",
                });
            }
            Self::OutputLimit {
                reason: "output_byte_limit_exceeded",
            } => {
                return Some(InputCorrection {
                    argument: "--max-output-bytes".to_owned(),
                    expected: "enough bytes for the complete Arrow stream".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Increase --max-output-bytes explicitly; preserve the request, authorization, table bindings, and snapshot versions.",
                });
            }
            Self::OutputLimit {
                reason: "preview_schema_limit_exceeded",
            } => {
                return Some(InputCorrection {
                    argument: "--column".to_owned(),
                    expected: "a selected schema within the preview byte budget".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Select fewer --column values, or explicitly increase --max-output-bytes for the required schema; preserve authorization and --table-version N.",
                });
            }
            Self::OutputLimit {
                reason: "output_row_limit_exceeded",
            } => {
                return Some(InputCorrection {
                    argument: "--max-rows".to_owned(),
                    expected: "enough rows for the complete requested result".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Explicitly increase --max-rows; preserve the request, authorization, table bindings, and snapshot versions.",
                });
            }
            Self::OutputLimit {
                reason: "preview_row_too_large",
            } => {
                return Some(InputCorrection {
                    argument: "--max-output-bytes".to_owned(),
                    expected: "enough bytes for the selected schema and first row".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Explicitly increase --max-output-bytes; keep the same projection, predicate, row limit, authorization, --table-version N, and --deadline-ms.",
                });
            }
            Self::UnsupportedJsonType { command: "preview" } => {
                return Some(InputCorrection {
                    argument: "--column".to_owned(),
                    expected: "selected values that fit typed JSON".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Use scan with redirected stdout and the same projection, predicate, row limit, authorization, and --table-version N.",
                });
            }
            Self::DeadlineExceeded => {
                return Some(InputCorrection {
                    argument: "--deadline-ms".to_owned(),
                    expected: "enough milliseconds to finish the command".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Discard stdout; check source responsiveness, then retry the same pinned request with an explicit --deadline-ms. Keep authorization and all other limits.",
                });
            }
            Self::UnsupportedJsonType { .. } => {
                return Some(InputCorrection {
                    argument: "--format".to_owned(),
                    expected: "arrow for this result type".to_owned(),
                    field_path: None,
                    supplied: None,
                    hint: "Use --format arrow, redirecting stdout unless --output is set. Explicitly preserve --max-output-bytes, SQL, authorization, bindings and versions.",
                });
            }
            _ => return None,
        };
        Some(input_kind.correction(hint))
    }
}
