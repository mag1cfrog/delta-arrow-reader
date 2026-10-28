//! Look up a JSON format, then the fields or literal types needed to use it.
//!
//! For example, `--schema predicate` shows a row-filter example and lists the
//! available operators and literal types. `--detail timestamp_us` explains that
//! literal's fields. `--full` returns the complete validation document.

mod guides;
mod output;

use clap::ValueEnum;
use serde::Serialize;
use serde_json::{Value, json};

use crate::input::JsonInputKind;

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DiscoverySchema {
    StorageOptions,
    Predicate,
    ExecutionOptions,
    Tables,
    Inspection,
    InspectionSummary,
    Diagnostic,
    Completion,
    Rows,
    QueryPlan,
    ResultArtifact,
    ResultManifest,
}

impl DiscoverySchema {
    pub(crate) fn schema_document(self) -> Value {
        let (mut schema, stream) = match self {
            Self::StorageOptions => return JsonInputKind::StorageOptions.schema_document(),
            Self::Predicate => return JsonInputKind::Predicate.schema_document(),
            Self::ExecutionOptions => return JsonInputKind::ExecutionOptions.schema_document(),
            Self::Tables => return JsonInputKind::Tables.schema_document(),
            Self::Inspection => (output::inspection_schema(), "stdout"),
            Self::InspectionSummary => (output::inspection_summary_schema(), "stdout"),
            Self::Rows => (output::rows_schema(), "stdout"),
            Self::QueryPlan => (output::query_plan::query_plan_schema(), "stdout"),
            Self::ResultArtifact => (output::artifacts::result_artifact_schema(), "stdout"),
            Self::ResultManifest => (output::artifacts::result_manifest_schema(), "file"),
            Self::Diagnostic => (output::diagnostic_schema(), "stderr"),
            Self::Completion => (output::completion_schema(), "stderr"),
        };
        schema["$schema"] = json!("https://json-schema.org/draft/2020-12/schema");
        json!({
            "format_version": 1,
            "kind": "output_schema",
            "cli_version": env!("CARGO_PKG_VERSION"),
            "name": self,
            "stream": stream,
            "schema": schema,
        })
    }
}

#[cfg(test)]
mod tests;
