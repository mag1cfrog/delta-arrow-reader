//! Validation documents returned by `dar capabilities --schema NAME --full`.
//!
//! Each response contains a JSON Schema, examples, and the checks that remain
//! outside schema validation. For example, the execution-options schema permits
//! an integer prefetch count, but the file parser also requires integer notation:
//! `{"prefetch_files_per_partition":1}` is accepted, while `1.0` is rejected.
//! Table-dependent checks, such as resolving predicate columns, happen later.

use delta_arrow_reader::{DeltaScanExecutionOptions, ParquetReaderBackend};
use serde_json::{Value, json};

use super::{JsonInputKind, MAX_INPUT_BYTES, predicate, tables};

impl JsonInputKind {
    pub(crate) fn schema_document(self) -> Value {
        let mut schema = match self {
            Self::StorageOptions => json!({
                "title": "Storage options file",
                "description": "Backend option names mapped to string values. The selected storage backend checks option names and their meanings.",
                "type": "object",
                "additionalProperties": {"type": "string"},
                "examples": [{}, {"aws_region": "eu-west-1"}],
            }),
            Self::ExecutionOptions => execution_options_schema(),
            Self::Predicate => predicate::predicate_schema(),
            Self::Tables => tables::tables_manifest_schema(),
        };
        schema["$schema"] = json!("https://json-schema.org/draft/2020-12/schema");
        let mut parser_constraints = json!({
            "max_file_bytes": MAX_INPUT_BYTES,
            "duplicate_object_keys": "rejected",
            "integer_notation": "Fields with schema type integer require JSON integer notation: no fraction, exponent, or negative zero.",
        });
        if matches!(self, Self::Predicate) {
            parser_constraints["max_predicate_nodes"] = json!(predicate::MAX_PREDICATE_NODES);
            parser_constraints["max_predicate_depth"] = json!(predicate::MAX_PREDICATE_DEPTH);
            parser_constraints["predicate_rules"] = json!([
                "Node count and depth include the root.",
                "Column resolution, type compatibility, and decimal precision/scale checks require the selected table.",
            ]);
        }
        if matches!(self, Self::Tables) {
            parser_constraints["table_rules"] = json!([
                "Aliases must be unique and not reserved names or SQL keywords.",
                "Version strings must fit u64; schema validation alone does not check that range.",
                "Paths, URLs, credential references and source compatibility are validated before table IO.",
                "uc://catalog.schema.table requires source databricks and a nonblank profile without control characters. Direct locations reject databricks profiles.",
            ]);
        }
        json!({
            "format_version": 1,
            "kind": "input_schema",
            "cli_version": env!("CARGO_PKG_VERSION"),
            "name": self,
            "argument": self.argument(),
            "schema": schema,
            "parser_constraints": parser_constraints,
        })
    }
}

fn execution_options_schema() -> Value {
    let defaults = DeltaScanExecutionOptions::new();
    let default_backend = match defaults.parquet_backend() {
        ParquetReaderBackend::Direct => "direct",
        ParquetReaderBackend::DeltaKernel => "delta_kernel",
    };
    let max_permits = tokio::sync::Semaphore::MAX_PERMITS;
    json!({
        "title": "Scan execution options file",
        "description": "Omitted fields keep the reader's defaults. Null clears an optional setting; it does not mean the field was omitted.",
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "parquet_backend": {
                "type": "string", "enum": ["direct", "delta_kernel"],
                "default": default_backend,
            },
            "max_concurrent_file_reads_per_scan": {
                "type": ["integer", "null"], "minimum": 1, "maximum": max_permits,
                "default": defaults.max_concurrent_file_reads_per_scan(),
                "description": "Null derives the scan limit from the partition settings; it does not disable the limit.",
            },
            "max_concurrent_file_reads_per_partition": {
                "type": "integer", "minimum": 1, "maximum": max_permits,
                "default": defaults.max_concurrent_file_reads_per_partition(),
            },
            "output_buffer_batches_per_partition": {
                "type": "integer", "minimum": 1, "maximum": max_permits,
                "default": defaults.output_buffer_batches_per_partition(),
            },
            "prefetch_files_per_partition": {
                "type": "integer", "minimum": 0, "maximum": usize::MAX,
                "default": defaults.prefetch_files_per_partition(),
            },
            "parquet_metadata_size_hint_bytes": {
                "type": ["integer", "null"], "minimum": 1, "maximum": usize::MAX,
                "default": defaults.parquet_metadata_size_hint_bytes(),
                "description": "Null clears the size hint; omitting this field keeps the default.",
            },
            "parquet_full_file_read_threshold_bytes": {
                "type": ["integer", "null"], "minimum": 1, "maximum": usize::MAX,
                "default": defaults.parquet_full_file_read_threshold_bytes(),
                "description": "Null disables full-file reads by size; omitting this field keeps the default.",
            },
        },
        "examples": [{}, {"prefetch_files_per_partition": 0}, {
            "parquet_backend": "delta_kernel",
            "max_concurrent_file_reads_per_scan": 8,
            "max_concurrent_file_reads_per_partition": 2,
            "output_buffer_batches_per_partition": 1,
            "prefetch_files_per_partition": 0,
            "parquet_metadata_size_hint_bytes": null,
            "parquet_full_file_read_threshold_bytes": null,
        }],
    })
}

#[cfg(test)]
mod tests;
