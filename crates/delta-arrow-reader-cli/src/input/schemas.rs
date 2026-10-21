//! File formats returned by `dar capabilities --schema NAME`.
//!
//! Each response contains a JSON Schema, examples, and the checks that remain
//! outside schema validation. For example, the execution-options schema permits
//! an integer prefetch count, but the file parser also requires integer notation:
//! `{"prefetch_files_per_partition":1}` is accepted, while `1.0` is rejected.
//! Table-dependent checks, such as resolving predicate columns, happen later.

use delta_arrow_reader::{DeltaScanExecutionOptions, ParquetReaderBackend};
use serde_json::{Value, json};

use super::{JsonInputKind, MAX_JSON_BYTES, predicate};

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
        };
        schema["$schema"] = json!("https://json-schema.org/draft/2020-12/schema");
        let mut parser_constraints = json!({
            "max_file_bytes": MAX_JSON_BYTES,
            "duplicate_object_keys": "rejected",
            "integer_notation": "Fields with schema type integer require JSON integer notation: no fraction, exponent, or negative zero.",
        });
        if matches!(self, Self::Predicate) {
            parser_constraints["max_predicate_nodes"] = json!(predicate::MAX_PREDICATE_NODES);
            parser_constraints["max_predicate_depth"] = json!(predicate::MAX_PREDICATE_DEPTH);
            parser_constraints["predicate_rules"] = json!([
                "Node count and depth include the root.",
                "Integer strings must fit the signed width stated in each value's description.",
                "date32 values count days since 1970-01-01. timestamp_us values count microseconds since the Unix epoch; timezone is required and may be null.",
                "decimal128 values are unscaled integers; precision and scale must be valid for the column's Decimal128 type.",
                "Float values must remain finite after conversion to the declared width.",
                "A fixed_size_binary size must equal the number of bytes in its value.",
                "Column resolution, type compatibility, and decimal precision/scale checks require the selected table.",
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
            },
            "parquet_full_file_read_threshold_bytes": {
                "type": ["integer", "null"], "minimum": 1, "maximum": usize::MAX,
                "default": defaults.parquet_full_file_read_threshold_bytes(),
            },
        },
        "examples": [{}, {
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
