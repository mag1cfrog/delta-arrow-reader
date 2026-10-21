//! Named schemas for constructing file inputs and reading command results.
//!
//! `dar capabilities --schema predicate` describes an input file, while
//! `--schema completion` describes the final success record on stderr. Both
//! return a discovery envelope; its `schema` field is the validation document.

use clap::ValueEnum;
use serde::Serialize;
use serde_json::{Value, json};

use super::output_schemas;
use crate::input::JsonInputKind;

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DiscoverySchema {
    StorageOptions,
    Predicate,
    ExecutionOptions,
    Inspection,
    Diagnostic,
    Completion,
}

impl DiscoverySchema {
    pub(crate) fn schema_document(self) -> Value {
        let (mut schema, stream) = match self {
            Self::StorageOptions => return JsonInputKind::StorageOptions.schema_document(),
            Self::Predicate => return JsonInputKind::Predicate.schema_document(),
            Self::ExecutionOptions => return JsonInputKind::ExecutionOptions.schema_document(),
            Self::Inspection => (output_schemas::inspection_schema(), "stdout"),
            Self::Diagnostic => (output_schemas::diagnostic_schema(), "stderr"),
            Self::Completion => (output_schemas::completion_schema(), "stderr"),
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
