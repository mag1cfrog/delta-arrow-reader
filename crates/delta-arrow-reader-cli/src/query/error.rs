//! Query failures, safe classifications, and exit statuses.

use std::{error::Error as _, fmt};

use datafusion::common::{DataFusionError, SchemaError};
use delta_arrow_reader::{DeltaReaderError, error_source_kind};
use snafu::Snafu;

use crate::diagnostics::ErrorClassification;

/// SQL failures retain typed sources without printing query text or dependency messages.
#[derive(Snafu)]
#[snafu(visibility(pub(super)))]
pub(crate) enum QueryError {
    #[snafu(display(
        "Invalid SQL input (reason={reason}). Use one read-only query over the selected tables."
    ))]
    Input { reason: &'static str },
    #[snafu(display("Could not create private temporary storage in --spill-dir."))]
    SpillDirectory { source: std::io::Error },
    #[snafu(display("Could not remove the query's temporary spill files."))]
    SpillCleanup { source: std::io::Error },
    #[snafu(display("Could not initialize the SQL runtime."))]
    Initialization { source: DataFusionError },
    #[snafu(display("Could not plan the SQL query."))]
    Planning { source: DataFusionError },
    #[snafu(display("Could not execute the SQL query."))]
    Execution { source: DataFusionError },
}

impl fmt::Debug for QueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl QueryError {
    pub(crate) fn reader_cause(&self) -> Option<&DeltaReaderError> {
        DeltaReaderError::find_source(self.source()?)
    }

    pub(crate) fn classification(&self) -> ErrorClassification {
        match self {
            Self::Input { reason } => ErrorClassification {
                phase: "configuration",
                code: "invalid_sql_input",
                reason,
                source_kind: None,
            },
            Self::SpillDirectory { source } | Self::SpillCleanup { source } => {
                let (phase, code) = if matches!(self, Self::SpillDirectory { .. }) {
                    ("configuration", "spill_directory")
                } else {
                    ("datafusion", "spill_cleanup")
                };
                ErrorClassification {
                    phase,
                    code,
                    reason: code,
                    source_kind: Some(error_source_kind(source)),
                }
            }
            Self::Initialization { source }
            | Self::Planning { source }
            | Self::Execution { source } => {
                let code = match self {
                    Self::Initialization { .. } => "sql_initialization",
                    Self::Planning { .. } => "sql_planning",
                    _ => "sql_execution",
                };
                let source_kind =
                    if matches!(source.find_root(), DataFusionError::ResourcesExhausted(_)) {
                        "resource_exhausted"
                    } else {
                        error_source_kind(source)
                    };
                let reason = match source.find_root() {
                    DataFusionError::SchemaError(error, _)
                        if matches!(self, Self::Planning { .. }) && self.reader_cause().is_none() =>
                    {
                        match error.as_ref() {
                            SchemaError::FieldNotFound { .. } => "column_not_found",
                            SchemaError::AmbiguousReference { .. } => "ambiguous_column",
                            _ => code,
                        }
                    }
                    // DataFusion 54 uses one string variant for both budgets. Unknown messages stay unclassified.
                    DataFusionError::ResourcesExhausted(message)
                        if message.starts_with("The used disk space during the spilling process has exceeded the allowable limit of ") => "spill_limit_exceeded",
                    DataFusionError::ResourcesExhausted(message)
                        if message.starts_with("Memory Exhausted while ")
                            && message.ends_with("(DiskManager is disabled)") => "spill_disabled",
                    DataFusionError::ResourcesExhausted(message)
                        if message.starts_with("Failed to allocate additional ") => "memory_limit_exceeded",
                    _ => code,
                };
                ErrorClassification {
                    phase: "datafusion",
                    code,
                    reason,
                    source_kind: Some(source_kind),
                }
            }
        }
    }

    pub(crate) fn exit_status(&self) -> u8 {
        match self {
            Self::Input { .. } | Self::SpillDirectory { .. } => 2,
            _ => 1,
        }
    }
}

#[cfg(test)]
mod tests;
