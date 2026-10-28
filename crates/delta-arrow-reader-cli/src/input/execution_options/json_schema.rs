//! Discoverable scan settings and limits derived from the reader's defaults.

use delta_arrow_reader::{DeltaScanExecutionOptions, ParquetReaderBackend};
use serde_json::{Value, json};

pub(in crate::input) fn execution_options_schema() -> Value {
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
