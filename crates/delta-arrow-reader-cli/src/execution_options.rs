use delta_arrow_reader::{DeltaReaderError, DeltaScanExecutionOptions, ParquetReaderBackend};
use serde::{Deserialize, Deserializer, de};

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
    let backend = match String::deserialize(deserializer)?.as_str() {
        "direct" => ParquetReaderBackend::Direct,
        "delta_kernel" => ParquetReaderBackend::DeltaKernel,
        _ => return Err(de::Error::custom("invalid parquet backend")),
    };
    Ok(Some(backend))
}

#[cfg(test)]
mod tests;
