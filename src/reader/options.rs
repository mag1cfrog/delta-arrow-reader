//! Reader backend, snapshot, storage, and execution options.

use std::collections::BTreeMap;

use crate::{DeltaReaderError, error::InvalidConfigurationSnafu};

const DEFAULT_MAX_CONCURRENT_FILE_READS_PER_PARTITION: usize = 3;
const DEFAULT_OUTPUT_BUFFER_CAPACITY_PER_PARTITION: usize = 1;
const DEFAULT_PREFETCH_FILE_COUNT_PER_PARTITION: usize = 2;
const DEFAULT_PARQUET_METADATA_SIZE_HINT_BYTES: usize = 64 * 1024;

/// Storage options forwarded to Delta object-store construction.
pub type DeltaStorageOptions = BTreeMap<String, String>;

/// Delta snapshot selected for a table load.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DeltaSnapshotSelection {
    /// Load the latest available snapshot.
    #[default]
    Latest,
    /// Load one exact Delta log version.
    Version(u64),
}

/// Backend used to read Parquet data files.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ParquetReaderBackend {
    /// Delegate data-file reads to Delta Kernel's Parquet handler.
    DeltaKernel,
    /// Read data files directly through the asynchronous Parquet API.
    #[default]
    DirectParquet,
}

/// Bounded execution settings for one Delta scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaReaderExecutionOptions {
    reader_backend: ParquetReaderBackend,
    max_concurrent_file_reads_per_scan: Option<usize>,
    max_concurrent_file_reads_per_partition: usize,
    output_buffer_capacity_per_partition: usize,
    prefetch_file_count_per_partition: usize,
    parquet_metadata_size_hint_bytes: Option<usize>,
    parquet_full_file_read_threshold_bytes: Option<usize>,
}

impl DeltaReaderExecutionOptions {
    /// Returns the baseline execution settings.
    pub const fn new() -> Self {
        Self {
            reader_backend: ParquetReaderBackend::DirectParquet,
            max_concurrent_file_reads_per_scan: None,
            max_concurrent_file_reads_per_partition:
                DEFAULT_MAX_CONCURRENT_FILE_READS_PER_PARTITION,
            output_buffer_capacity_per_partition: DEFAULT_OUTPUT_BUFFER_CAPACITY_PER_PARTITION,
            prefetch_file_count_per_partition: DEFAULT_PREFETCH_FILE_COUNT_PER_PARTITION,
            parquet_metadata_size_hint_bytes: Some(DEFAULT_PARQUET_METADATA_SIZE_HINT_BYTES),
            parquet_full_file_read_threshold_bytes: None,
        }
    }

    /// Returns the selected Parquet reader backend.
    pub const fn reader_backend(&self) -> ParquetReaderBackend {
        self.reader_backend
    }

    /// Returns the optional scan-wide file-read limit.
    pub const fn max_concurrent_file_reads_per_scan(&self) -> Option<usize> {
        self.max_concurrent_file_reads_per_scan
    }

    /// Returns the per-partition file-read limit.
    pub const fn max_concurrent_file_reads_per_partition(&self) -> usize {
        self.max_concurrent_file_reads_per_partition
    }

    /// Returns the per-partition output buffer capacity.
    pub const fn output_buffer_capacity_per_partition(&self) -> usize {
        self.output_buffer_capacity_per_partition
    }

    /// Returns the direct Parquet reader's per-partition file prefetch depth.
    pub const fn prefetch_file_count_per_partition(&self) -> usize {
        self.prefetch_file_count_per_partition
    }

    /// Returns the direct Parquet reader's metadata size hint in bytes.
    pub const fn parquet_metadata_size_hint_bytes(&self) -> Option<usize> {
        self.parquet_metadata_size_hint_bytes
    }

    /// Returns the direct Parquet reader's full-file read threshold in bytes.
    pub const fn parquet_full_file_read_threshold_bytes(&self) -> Option<usize> {
        self.parquet_full_file_read_threshold_bytes
    }

    /// Selects a Parquet reader backend.
    pub const fn with_reader_backend(mut self, value: ParquetReaderBackend) -> Self {
        self.reader_backend = value;
        self
    }

    /// Sets or clears the scan-wide file-read limit.
    pub fn with_max_concurrent_file_reads_per_scan(
        mut self,
        value: Option<usize>,
    ) -> Result<Self, DeltaReaderError> {
        validate_optional_positive(value, "max_concurrent_file_reads_per_scan_must_be_positive")?;
        self.max_concurrent_file_reads_per_scan = value;
        Ok(self)
    }

    /// Sets the per-partition file-read limit.
    pub fn with_max_concurrent_file_reads_per_partition(
        mut self,
        value: usize,
    ) -> Result<Self, DeltaReaderError> {
        validate_positive(
            value,
            "max_concurrent_file_reads_per_partition_must_be_positive",
        )?;
        self.max_concurrent_file_reads_per_partition = value;
        Ok(self)
    }

    /// Sets the per-partition output buffer capacity.
    pub fn with_output_buffer_capacity_per_partition(
        mut self,
        value: usize,
    ) -> Result<Self, DeltaReaderError> {
        validate_positive(
            value,
            "output_buffer_capacity_per_partition_must_be_positive",
        )?;
        self.output_buffer_capacity_per_partition = value;
        Ok(self)
    }

    /// Sets the direct Parquet reader's per-partition file prefetch depth.
    pub const fn with_prefetch_file_count_per_partition(mut self, value: usize) -> Self {
        self.prefetch_file_count_per_partition = value;
        self
    }

    /// Sets or clears the direct Parquet reader's metadata size hint in bytes.
    pub fn with_parquet_metadata_size_hint_bytes(
        mut self,
        value: Option<usize>,
    ) -> Result<Self, DeltaReaderError> {
        validate_optional_positive(value, "parquet_metadata_size_hint_bytes_must_be_positive")?;
        self.parquet_metadata_size_hint_bytes = value;
        Ok(self)
    }

    /// Sets or clears the direct Parquet reader's full-file read threshold in bytes.
    pub fn with_parquet_full_file_read_threshold_bytes(
        mut self,
        value: Option<usize>,
    ) -> Result<Self, DeltaReaderError> {
        validate_optional_positive(
            value,
            "parquet_full_file_read_threshold_bytes_must_be_positive",
        )?;
        self.parquet_full_file_read_threshold_bytes = value;
        Ok(self)
    }

    pub(crate) fn resolved_max_concurrent_file_reads_per_scan(
        &self,
        target_partitions: usize,
    ) -> usize {
        self.max_concurrent_file_reads_per_scan.unwrap_or_else(|| {
            target_partitions
                .saturating_mul(self.max_concurrent_file_reads_per_partition)
                .max(1)
        })
    }
}

impl Default for DeltaReaderExecutionOptions {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_positive(value: usize, reason: &'static str) -> Result<(), DeltaReaderError> {
    if value == 0 {
        return InvalidConfigurationSnafu { reason }.fail();
    }
    Ok(())
}

fn validate_optional_positive(
    value: Option<usize>,
    reason: &'static str,
) -> Result<(), DeltaReaderError> {
    if value == Some(0) {
        return InvalidConfigurationSnafu { reason }.fail();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::DeltaReaderPhase;

    use super::{
        DeltaReaderExecutionOptions, DeltaSnapshotSelection, DeltaStorageOptions,
        ParquetReaderBackend,
    };

    #[test]
    fn public_defaults_match_the_frozen_baseline() {
        let options = DeltaReaderExecutionOptions::new();

        assert_eq!(
            DeltaSnapshotSelection::default(),
            DeltaSnapshotSelection::Latest
        );
        assert_eq!(
            ParquetReaderBackend::default(),
            ParquetReaderBackend::DirectParquet
        );
        assert_eq!(DeltaReaderExecutionOptions::default(), options);
        assert_eq!(
            options.reader_backend(),
            ParquetReaderBackend::DirectParquet
        );
        assert_eq!(options.max_concurrent_file_reads_per_scan(), None);
        assert_eq!(options.max_concurrent_file_reads_per_partition(), 3);
        assert_eq!(options.output_buffer_capacity_per_partition(), 1);
        assert_eq!(options.prefetch_file_count_per_partition(), 2);
        assert_eq!(options.parquet_metadata_size_hint_bytes(), Some(65_536));
        assert_eq!(options.parquet_full_file_read_threshold_bytes(), None);
        assert_eq!(DeltaStorageOptions::default(), DeltaStorageOptions::new());
        assert_eq!(
            DeltaSnapshotSelection::Version(7),
            DeltaSnapshotSelection::Version(7)
        );
    }

    #[test]
    fn builders_set_every_public_option() -> Result<(), Box<dyn std::error::Error>> {
        let options = DeltaReaderExecutionOptions::new()
            .with_reader_backend(ParquetReaderBackend::DeltaKernel)
            .with_max_concurrent_file_reads_per_scan(Some(8))?
            .with_max_concurrent_file_reads_per_partition(4)?
            .with_output_buffer_capacity_per_partition(2)?
            .with_prefetch_file_count_per_partition(0)
            .with_parquet_metadata_size_hint_bytes(None)?
            .with_parquet_full_file_read_threshold_bytes(Some(1024))?;

        assert_eq!(options.reader_backend(), ParquetReaderBackend::DeltaKernel);
        assert_eq!(options.max_concurrent_file_reads_per_scan(), Some(8));
        assert_eq!(options.max_concurrent_file_reads_per_partition(), 4);
        assert_eq!(options.output_buffer_capacity_per_partition(), 2);
        assert_eq!(options.prefetch_file_count_per_partition(), 0);
        assert_eq!(options.parquet_metadata_size_hint_bytes(), None);
        assert_eq!(options.parquet_full_file_read_threshold_bytes(), Some(1024));
        Ok(())
    }

    #[test]
    fn invalid_bounds_return_redacted_configuration_errors() {
        let invalid = [
            DeltaReaderExecutionOptions::new().with_max_concurrent_file_reads_per_scan(Some(0)),
            DeltaReaderExecutionOptions::new().with_max_concurrent_file_reads_per_partition(0),
            DeltaReaderExecutionOptions::new().with_output_buffer_capacity_per_partition(0),
            DeltaReaderExecutionOptions::new().with_parquet_metadata_size_hint_bytes(Some(0)),
            DeltaReaderExecutionOptions::new().with_parquet_full_file_read_threshold_bytes(Some(0)),
        ];

        for result in invalid {
            let error = result.expect_err("invalid execution options must fail");
            assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
            assert_eq!(error.code(), "invalid_configuration");
        }
    }

    #[test]
    fn independent_bounds_preserve_the_frozen_reader_behavior()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = DeltaReaderExecutionOptions::new()
            .with_max_concurrent_file_reads_per_scan(Some(2))?
            .with_prefetch_file_count_per_partition(4);

        assert_eq!(options.max_concurrent_file_reads_per_scan(), Some(2));
        assert_eq!(options.max_concurrent_file_reads_per_partition(), 3);
        assert_eq!(options.prefetch_file_count_per_partition(), 4);
        Ok(())
    }

    #[test]
    fn scan_capacity_resolves_once_from_the_fixed_partition_target()
    -> Result<(), Box<dyn std::error::Error>> {
        let defaults = DeltaReaderExecutionOptions::new();
        assert_eq!(defaults.resolved_max_concurrent_file_reads_per_scan(4), 12);
        assert_eq!(
            defaults.resolved_max_concurrent_file_reads_per_scan(usize::MAX),
            usize::MAX
        );

        let explicit = defaults.with_max_concurrent_file_reads_per_scan(Some(7))?;
        assert_eq!(explicit.resolved_max_concurrent_file_reads_per_scan(4), 7);
        Ok(())
    }
}
