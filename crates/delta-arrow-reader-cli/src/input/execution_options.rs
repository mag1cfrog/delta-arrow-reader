//! CLI overrides for the reader's scan execution settings.
//!
//! Execution settings control how a scan reads and buffers data, independently
//! of which rows and columns it selects. A file can specify a subset of settings:
//! omitted fields retain the reader's defaults, and supported null values clear
//! optional settings. Validation limits come from the reader's configuration API.
//!
//! For example, a file passed to `--execution-options-file` can contain:
//!
//! ```json
//! {
//!   "prefetch_files_per_partition": 0,
//!   "parquet_metadata_size_hint_bytes": null
//! }
//! ```
//!
//! This sets prefetch to zero and clears the metadata size hint. Omitting
//! `parquet_metadata_size_hint_bytes` instead keeps the reader's default hint.

use delta_arrow_reader::{DeltaReaderError, DeltaScanExecutionOptions, ParquetReaderBackend};
use serde::{Deserialize, Deserializer, de::value::StringDeserializer};

use crate::{diagnostics::InputCorrection, input::JsonInputKind};

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ExecutionOptionsInput {
    #[serde(deserialize_with = "deserialize_backend")]
    parquet_backend: Option<ParquetReaderBackend>,
    #[serde(deserialize_with = "deserialize_present_field")]
    max_concurrent_file_reads_per_scan: Option<Option<usize>>,
    #[serde(deserialize_with = "deserialize_present_field")]
    max_concurrent_file_reads_per_partition: Option<usize>,
    #[serde(deserialize_with = "deserialize_present_field")]
    output_buffer_batches_per_partition: Option<usize>,
    #[serde(deserialize_with = "deserialize_present_field")]
    prefetch_files_per_partition: Option<usize>,
    #[serde(deserialize_with = "deserialize_present_field")]
    parquet_metadata_size_hint_bytes: Option<Option<usize>>,
    #[serde(deserialize_with = "deserialize_present_field")]
    parquet_full_file_read_threshold_bytes: Option<Option<usize>>,
}

impl ExecutionOptionsInput {
    pub(crate) fn into_execution_options(
        self,
    ) -> Result<DeltaScanExecutionOptions, DeltaReaderError> {
        let mut options = DeltaScanExecutionOptions::new();
        if let Some(backend) = self.parquet_backend {
            options = options.with_parquet_backend(backend);
        }
        if let Some(limit) = self.max_concurrent_file_reads_per_scan {
            options = options.with_max_concurrent_file_reads_per_scan(limit)?;
        }
        if let Some(limit) = self.max_concurrent_file_reads_per_partition {
            options = options.with_max_concurrent_file_reads_per_partition(limit)?;
        }
        if let Some(capacity) = self.output_buffer_batches_per_partition {
            options = options.with_output_buffer_batches_per_partition(capacity)?;
        }
        if let Some(count) = self.prefetch_files_per_partition {
            options = options.with_prefetch_files_per_partition(count);
        }
        if let Some(bytes) = self.parquet_metadata_size_hint_bytes {
            options = options.with_parquet_metadata_size_hint_bytes(bytes)?;
        }
        if let Some(bytes) = self.parquet_full_file_read_threshold_bytes {
            options = options.with_parquet_full_file_read_threshold_bytes(bytes)?;
        }
        Ok(options)
    }
}

pub(crate) fn execution_options_correction(reason: &str) -> Option<InputCorrection> {
    let field = match reason {
        "max_concurrent_file_reads_per_scan_must_be_positive"
        | "max_concurrent_file_reads_per_scan_exceeds_tokio_max_permits" => {
            "max_concurrent_file_reads_per_scan"
        }
        "max_concurrent_file_reads_per_partition_must_be_positive"
        | "max_concurrent_file_reads_per_partition_exceeds_tokio_max_permits" => {
            "max_concurrent_file_reads_per_partition"
        }
        "output_buffer_batches_per_partition_must_be_positive"
        | "output_buffer_batches_per_partition_exceeds_tokio_max_permits" => {
            "output_buffer_batches_per_partition"
        }
        "parquet_metadata_size_hint_bytes_must_be_positive" => "parquet_metadata_size_hint_bytes",
        "parquet_full_file_read_threshold_bytes_must_be_positive" => {
            "parquet_full_file_read_threshold_bytes"
        }
        _ => return None,
    };
    Some(InputCorrection {
        argument: JsonInputKind::ExecutionOptions.argument().to_owned(),
        field_path: Some(format!("/{field}")),
        expected: describe_integer_requirement(field)?,
        hint: "Set this field within the accepted range, or omit it to use the default.",
    })
}

pub(super) fn describe_integer_requirement(field: &str) -> Option<String> {
    let (minimum, maximum, nullable) = match field {
        "max_concurrent_file_reads_per_scan" => (1, tokio::sync::Semaphore::MAX_PERMITS, true),
        "max_concurrent_file_reads_per_partition" | "output_buffer_batches_per_partition" => {
            (1, tokio::sync::Semaphore::MAX_PERMITS, false)
        }
        "parquet_metadata_size_hint_bytes" | "parquet_full_file_read_threshold_bytes" => {
            (1, usize::MAX, true)
        }
        "prefetch_files_per_partition" => (0, usize::MAX, false),
        _ => return None,
    };
    Some(format!(
        "JSON integer from {minimum} to {maximum}{}",
        if nullable { ", or null" } else { "" },
    ))
}

// The outer Option records presence, keeping omission separate from explicit null.
fn deserialize_present_field<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

fn deserialize_backend<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<ParquetReaderBackend>, D::Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum BackendInput {
        Direct,
        DeltaKernel,
    }

    let name = String::deserialize(deserializer)?;
    let backend = match BackendInput::deserialize(StringDeserializer::<D::Error>::new(name))? {
        BackendInput::Direct => ParquetReaderBackend::Direct,
        BackendInput::DeltaKernel => ParquetReaderBackend::DeltaKernel,
    };
    Ok(Some(backend))
}

#[cfg(test)]
mod tests;
