//! Explanations for rejected SQL expressions.
//!
//! For `SELECT get_json_object(value, '$.state') FROM t`, report the missing
//! function and its location. A SELECT with several invalid expressions can
//! report them together, so callers can repair the query in one pass.

use datafusion::common::{DataFusionError, Diagnostic, SchemaError};
use delta_arrow_reader::{DeltaReaderError, error_source_kind};
use serde::Serialize;
use serde_json::{Map, Value, json};

const MAX_PLANNING_ERRORS: usize = 8;
const MAX_FUNCTION_NAME_BYTES: usize = 64;

/// Bounded explanations for one failed SQL plan, in the planner's reporting order.
pub(crate) struct PlanningDiagnostics<'a> {
    causes: Vec<PlanningCause<'a>>,
    truncated: bool,
}

#[derive(Serialize)]
struct PlanningCause<'a> {
    reason: &'static str,
    source_kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    function: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    line: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    column: Option<u64>,
}

impl<'a> PlanningDiagnostics<'a> {
    pub(super) fn from_error(source: &'a DataFusionError) -> Self {
        let mut causes = Vec::new();
        let mut pending = vec![(source, None)];
        let mut truncated = false;
        while let Some((error, diagnostic)) = pending.pop() {
            match error {
                DataFusionError::Context(_, source) => pending.push((source, diagnostic)),
                DataFusionError::Shared(source) => pending.push((source, diagnostic)),
                DataFusionError::Diagnostic(diagnostic, source) => {
                    pending.push((source, Some(diagnostic.as_ref())));
                }
                // find_root follows only the first member of a DataFusion collection.
                DataFusionError::Collection(errors) => {
                    pending.extend(errors.iter().rev().map(|error| (error, None)));
                }
                _ => {
                    if causes.len() == MAX_PLANNING_ERRORS {
                        truncated = true;
                        break;
                    }
                    causes.push(PlanningCause::from_error(error, diagnostic));
                }
            }
        }
        Self { causes, truncated }
    }

    pub(super) fn reason(&self) -> &'static str {
        match self.causes.as_slice() {
            [] => "sql_planning",
            [cause] if !self.truncated => cause.reason,
            _ => "multiple_planning_errors",
        }
    }

    pub(crate) fn all_causes_are_input_errors(&self) -> bool {
        !self.truncated
            && !self.causes.is_empty()
            && self.causes.iter().all(|cause| {
                matches!(
                    cause.reason,
                    "unknown_function" | "column_not_found" | "ambiguous_column"
                )
            })
    }

    pub(crate) fn to_json_fields(&self) -> Map<String, Value> {
        let mut fields = Map::new();
        match self.causes.as_slice() {
            [] => {}
            [cause] if !self.truncated => {
                if let Some(function) = cause.function {
                    fields.insert("function".into(), function.into());
                }
                if let (Some(line), Some(column)) = (cause.line, cause.column) {
                    fields.insert("line".into(), line.into());
                    fields.insert("column".into(), column.into());
                }
            }
            _ => {
                fields.insert("planning_errors".into(), json!(self.causes));
                if self.truncated {
                    fields.insert("planning_errors_truncated".into(), true.into());
                }
            }
        }
        fields
    }
}

impl<'a> PlanningCause<'a> {
    fn from_error(error: &'a DataFusionError, diagnostic: Option<&'a Diagnostic>) -> Self {
        let mut cause = Self {
            reason: "sql_planning",
            source_kind: error_source_kind(error),
            function: None,
            line: None,
            column: None,
        };
        if DeltaReaderError::find_source(error).is_some() {
            return cause;
        }
        match error.find_root() {
            DataFusionError::SchemaError(error, _) => {
                cause.reason = match error.as_ref() {
                    SchemaError::FieldNotFound { .. } => "column_not_found",
                    SchemaError::AmbiguousReference { .. } => "ambiguous_column",
                    _ => "sql_planning",
                };
            }
            DataFusionError::Plan(message) => {
                // DataFusion 54 has no typed unknown-function error. Match only its
                // diagnostic template, corroborated by the associated plan error.
                if let Some(diagnostic) = diagnostic
                    && let Some(name) = diagnostic
                        .message
                        .strip_prefix("Invalid function '")
                        .and_then(|name| name.strip_suffix('\''))
                    && message
                        .strip_prefix(diagnostic.message.as_str())
                        .is_some_and(|suffix| {
                            suffix.starts_with(".\nDid you mean '") && suffix.ends_with("'?")
                        })
                {
                    cause.reason = "unknown_function";
                    // Quoted names can contain URLs or arbitrary values; keep only
                    // short conventional identifiers, never an excerpt of the name.
                    if name.len() <= MAX_FUNCTION_NAME_BYTES
                        && name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
                        && name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
                    {
                        cause.function = Some(name);
                    }
                }
            }
            _ => {}
        }
        if let Some(span) = diagnostic.and_then(|diagnostic| diagnostic.span)
            && span.start.line > 0
            && span.start.column > 0
        {
            cause.line = Some(span.start.line);
            cause.column = Some(span.start.column);
        }
        cause
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::common::Column;

    use super::*;

    fn unavailable_function(name: &str) -> DataFusionError {
        DataFusionError::Plan(format!(
            "Invalid function '{name}'.\nDid you mean 'secret_suggestion'?"
        ))
        .with_diagnostic(Diagnostic::new_error(
            format!("Invalid function '{name}'"),
            None,
        ))
    }

    #[test]
    fn wrapped_collections_preserve_all_causes_without_messages() {
        let error = DataFusionError::Shared(Arc::new(DataFusionError::Context(
            "secret SQL".into(),
            Box::new(DataFusionError::Collection(vec![
                unavailable_function("get_json_object"),
                DataFusionError::Collection(vec![
                    unavailable_function("sha2"),
                    DataFusionError::SchemaError(
                        Box::new(SchemaError::AmbiguousReference {
                            field: Box::new(Column::new_unqualified("secret_column")),
                        }),
                        Box::new(None),
                    ),
                ]),
            ])),
        )));
        let details = PlanningDiagnostics::from_error(&error);
        assert_eq!(details.reason(), "multiple_planning_errors");
        assert!(details.all_causes_are_input_errors());
        let fields = details.to_json_fields();
        let causes = fields["planning_errors"].as_array().unwrap();
        assert_eq!(causes.len(), 3);
        assert_eq!(causes[0]["function"], "get_json_object");
        assert_eq!(causes[1]["function"], "sha2");
        assert_eq!(causes[2]["reason"], "ambiguous_column");
        assert!(!json!(fields).to_string().contains("secret"));
    }

    #[test]
    fn unknown_messages_and_non_planning_errors_are_not_function_errors() {
        for error in [
            DataFusionError::Plan("Invalid function 'secret_name'.\nDid you mean 'x'?".into()),
            DataFusionError::Execution("Invalid function 'secret_name'.\nDid you mean 'x'?".into())
                .with_diagnostic(Diagnostic::new_error(
                    "Invalid function 'secret_name'",
                    None,
                )),
            DataFusionError::Plan("unrecognized secret message".into()).with_diagnostic(
                Diagnostic::new_error("Invalid function 'secret_name'", None),
            ),
            DataFusionError::Plan("Invalid function 'secret_name'.\nDid you mean 'x'?".into())
                .with_diagnostic(Diagnostic::new_error("different secret diagnostic", None)),
        ] {
            let details = PlanningDiagnostics::from_error(&error);
            assert_eq!(details.reason(), "sql_planning");
            assert!(!details.all_causes_are_input_errors());
            assert!(details.to_json_fields().is_empty());
        }
    }

    #[test]
    fn mixed_and_truncated_failures_do_not_promise_an_input_only_repair() {
        let mixed = DataFusionError::Collection(vec![
            unavailable_function("get_json_object"),
            DataFusionError::Internal("secret engine failure".into()),
        ]);
        let details = PlanningDiagnostics::from_error(&mixed);
        assert!(!details.all_causes_are_input_errors());
        assert_eq!(
            details.to_json_fields()["planning_errors"][1]["source_kind"],
            "datafusion_internal"
        );
        assert!(
            !json!(details.to_json_fields())
                .to_string()
                .contains("secret")
        );

        let many = DataFusionError::Collection(
            (0..20)
                .map(|i| unavailable_function(&format!("missing_{i}")))
                .collect(),
        );
        let details = PlanningDiagnostics::from_error(&many);
        assert!(!details.all_causes_are_input_errors());
        assert_eq!(details.to_json_fields()["planning_errors_truncated"], true);
    }
}
