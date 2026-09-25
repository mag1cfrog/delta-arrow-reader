//! Parquet backend, snapshot, storage, and execution options.

use std::collections::BTreeMap;

use tokio::sync::Semaphore;

use crate::{DeltaReaderError, error::InvalidConfigurationSnafu};

const DEFAULT_MAX_CONCURRENT_FILE_READS_PER_PARTITION: usize = 3;
const DEFAULT_OUTPUT_BUFFER_BATCHES_PER_PARTITION: usize = 1;
const DEFAULT_PREFETCH_FILES_PER_PARTITION: usize = 2;
const DEFAULT_PARQUET_METADATA_SIZE_HINT_BYTES: usize = 64 * 1024;
pub(crate) const MAX_CONCURRENT_PARQUET_RANGE_READS: usize = 10;

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
    Direct,
}

/// Parquet range-read policy used by internal diagnostics and benchmarks.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ParquetRangeReadPolicy {
    /// Choose automatically for built-in remote stores and preserve other store implementations.
    #[default]
    Automatic,
    /// Read only the normalized ranges requested by Parquet.
    ExactRanges,
    /// Merge ranges separated by at most one MiB before reading them.
    MergeRangesWithinOneMegabyte,
    /// Pass the requested ranges to the object store's own multi-range implementation.
    StoreImplementation,
}

/// Bounded execution settings for one Delta scan.
#[must_use = "execution options do nothing unless passed to a table or scan"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaScanExecutionOptions {
    parquet_backend: ParquetReaderBackend,
    max_concurrent_file_reads_per_scan: Option<usize>,
    max_concurrent_file_reads_per_partition: usize,
    output_buffer_batches_per_partition: usize,
    prefetch_files_per_partition: usize,
    parquet_metadata_size_hint_bytes: Option<usize>,
    parquet_full_file_read_threshold_bytes: Option<usize>,
    parquet_range_read_policy: ParquetRangeReadPolicy,
}

impl DeltaScanExecutionOptions {
    /// Returns the baseline execution settings.
    pub const fn new() -> Self {
        Self {
            parquet_backend: ParquetReaderBackend::Direct,
            max_concurrent_file_reads_per_scan: None,
            max_concurrent_file_reads_per_partition:
                DEFAULT_MAX_CONCURRENT_FILE_READS_PER_PARTITION,
            output_buffer_batches_per_partition: DEFAULT_OUTPUT_BUFFER_BATCHES_PER_PARTITION,
            prefetch_files_per_partition: DEFAULT_PREFETCH_FILES_PER_PARTITION,
            parquet_metadata_size_hint_bytes: Some(DEFAULT_PARQUET_METADATA_SIZE_HINT_BYTES),
            parquet_full_file_read_threshold_bytes: None,
            parquet_range_read_policy: ParquetRangeReadPolicy::Automatic,
        }
    }

    /// Returns the selected Parquet reader backend.
    pub const fn parquet_backend(&self) -> ParquetReaderBackend {
        self.parquet_backend
    }

    /// Returns the optional scan-wide file-read limit.
    pub const fn max_concurrent_file_reads_per_scan(&self) -> Option<usize> {
        self.max_concurrent_file_reads_per_scan
    }

    /// Returns the per-partition file-read limit.
    pub const fn max_concurrent_file_reads_per_partition(&self) -> usize {
        self.max_concurrent_file_reads_per_partition
    }

    /// Returns the number of output batches buffered per partition.
    pub const fn output_buffer_batches_per_partition(&self) -> usize {
        self.output_buffer_batches_per_partition
    }

    /// Returns the number of future direct Parquet files prepared per partition.
    pub const fn prefetch_files_per_partition(&self) -> usize {
        self.prefetch_files_per_partition
    }

    /// Returns the direct Parquet reader's metadata size hint in bytes.
    pub const fn parquet_metadata_size_hint_bytes(&self) -> Option<usize> {
        self.parquet_metadata_size_hint_bytes
    }

    /// Returns the direct Parquet reader's full-file read threshold in bytes.
    pub const fn parquet_full_file_read_threshold_bytes(&self) -> Option<usize> {
        self.parquet_full_file_read_threshold_bytes
    }

    pub(crate) const fn parquet_range_read_policy(&self) -> ParquetRangeReadPolicy {
        self.parquet_range_read_policy
    }

    /// Selects a Parquet reader backend.
    pub const fn with_parquet_backend(mut self, parquet_backend: ParquetReaderBackend) -> Self {
        self.parquet_backend = parquet_backend;
        self
    }

    /// Selects a Parquet range-read policy for diagnostics and benchmarks.
    #[doc(hidden)]
    pub const fn with_parquet_range_read_policy(mut self, policy: ParquetRangeReadPolicy) -> Self {
        self.parquet_range_read_policy = policy;
        self
    }

    /// Sets or clears the scan-wide file-read limit.
    ///
    /// Explicit limits must be in `1..=tokio::sync::Semaphore::MAX_PERMITS`.
    /// `None` derives the limit from the partition target and per-partition limit,
    /// capped at Tokio's maximum.
    pub fn with_max_concurrent_file_reads_per_scan(
        mut self,
        max_concurrent_file_reads: Option<usize>,
    ) -> Result<Self, DeltaReaderError> {
        if let Some(capacity) = max_concurrent_file_reads {
            validate_capacity(
                capacity,
                "max_concurrent_file_reads_per_scan_must_be_positive",
                "max_concurrent_file_reads_per_scan_exceeds_tokio_max_permits",
            )?;
        }
        self.max_concurrent_file_reads_per_scan = max_concurrent_file_reads;
        Ok(self)
    }

    /// Sets the per-partition file-read limit.
    ///
    /// The limit must be in `1..=tokio::sync::Semaphore::MAX_PERMITS`.
    pub fn with_max_concurrent_file_reads_per_partition(
        mut self,
        max_concurrent_file_reads: usize,
    ) -> Result<Self, DeltaReaderError> {
        validate_capacity(
            max_concurrent_file_reads,
            "max_concurrent_file_reads_per_partition_must_be_positive",
            "max_concurrent_file_reads_per_partition_exceeds_tokio_max_permits",
        )?;
        self.max_concurrent_file_reads_per_partition = max_concurrent_file_reads;
        Ok(self)
    }

    /// Sets the number of output batches buffered per partition.
    ///
    /// The capacity must be in `1..=tokio::sync::Semaphore::MAX_PERMITS`.
    pub fn with_output_buffer_batches_per_partition(
        mut self,
        output_buffer_batches: usize,
    ) -> Result<Self, DeltaReaderError> {
        validate_capacity(
            output_buffer_batches,
            "output_buffer_batches_per_partition_must_be_positive",
            "output_buffer_batches_per_partition_exceeds_tokio_max_permits",
        )?;
        self.output_buffer_batches_per_partition = output_buffer_batches;
        Ok(self)
    }

    /// Sets the number of future direct Parquet files prepared per partition.
    pub const fn with_prefetch_files_per_partition(mut self, prefetch_files: usize) -> Self {
        self.prefetch_files_per_partition = prefetch_files;
        self
    }

    /// Sets or clears the direct Parquet reader's metadata size hint in bytes.
    pub fn with_parquet_metadata_size_hint_bytes(
        mut self,
        metadata_size_hint_bytes: Option<usize>,
    ) -> Result<Self, DeltaReaderError> {
        validate_optional_positive(
            metadata_size_hint_bytes,
            "parquet_metadata_size_hint_bytes_must_be_positive",
        )?;
        self.parquet_metadata_size_hint_bytes = metadata_size_hint_bytes;
        Ok(self)
    }

    /// Sets or clears the direct Parquet reader's full-file read threshold in bytes.
    pub fn with_parquet_full_file_read_threshold_bytes(
        mut self,
        full_file_read_threshold_bytes: Option<usize>,
    ) -> Result<Self, DeltaReaderError> {
        validate_optional_positive(
            full_file_read_threshold_bytes,
            "parquet_full_file_read_threshold_bytes_must_be_positive",
        )?;
        self.parquet_full_file_read_threshold_bytes = full_file_read_threshold_bytes;
        Ok(self)
    }

    pub(crate) fn resolved_max_concurrent_file_reads_per_scan(
        &self,
        target_partitions: usize,
    ) -> usize {
        self.max_concurrent_file_reads_per_scan.unwrap_or_else(|| {
            target_partitions
                .saturating_mul(self.max_concurrent_file_reads_per_partition)
                .clamp(1, Semaphore::MAX_PERMITS)
        })
    }
}

impl Default for DeltaScanExecutionOptions {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_capacity(
    value: usize,
    zero_reason: &'static str,
    overflow_reason: &'static str,
) -> Result<(), DeltaReaderError> {
    if value == 0 {
        return InvalidConfigurationSnafu {
            reason: zero_reason,
        }
        .fail();
    }
    if value > Semaphore::MAX_PERMITS {
        return InvalidConfigurationSnafu {
            reason: overflow_reason,
        }
        .fail();
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
        DeltaScanExecutionOptions, DeltaSnapshotSelection, DeltaStorageOptions,
        ParquetReaderBackend,
    };

    #[test]
    fn public_defaults_match_the_frozen_baseline() {
        let options = DeltaScanExecutionOptions::new();

        assert_eq!(
            DeltaSnapshotSelection::default(),
            DeltaSnapshotSelection::Latest
        );
        assert_eq!(
            ParquetReaderBackend::default(),
            ParquetReaderBackend::Direct
        );
        assert_eq!(DeltaScanExecutionOptions::default(), options);
        assert_eq!(options.parquet_backend(), ParquetReaderBackend::Direct);
        assert_eq!(options.max_concurrent_file_reads_per_scan(), None);
        assert_eq!(options.max_concurrent_file_reads_per_partition(), 3);
        assert_eq!(options.output_buffer_batches_per_partition(), 1);
        assert_eq!(options.prefetch_files_per_partition(), 2);
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
        let options = DeltaScanExecutionOptions::new()
            .with_parquet_backend(ParquetReaderBackend::DeltaKernel)
            .with_max_concurrent_file_reads_per_scan(Some(8))?
            .with_max_concurrent_file_reads_per_partition(4)?
            .with_output_buffer_batches_per_partition(2)?
            .with_prefetch_files_per_partition(0)
            .with_parquet_metadata_size_hint_bytes(None)?
            .with_parquet_full_file_read_threshold_bytes(Some(1024))?;

        assert_eq!(options.parquet_backend(), ParquetReaderBackend::DeltaKernel);
        assert_eq!(options.max_concurrent_file_reads_per_scan(), Some(8));
        assert_eq!(options.max_concurrent_file_reads_per_partition(), 4);
        assert_eq!(options.output_buffer_batches_per_partition(), 2);
        assert_eq!(options.prefetch_files_per_partition(), 0);
        assert_eq!(options.parquet_metadata_size_hint_bytes(), None);
        assert_eq!(options.parquet_full_file_read_threshold_bytes(), Some(1024));
        Ok(())
    }

    #[test]
    fn invalid_bounds_return_redacted_configuration_errors() {
        let invalid = [
            DeltaScanExecutionOptions::new().with_max_concurrent_file_reads_per_scan(Some(0)),
            DeltaScanExecutionOptions::new().with_max_concurrent_file_reads_per_partition(0),
            DeltaScanExecutionOptions::new().with_output_buffer_batches_per_partition(0),
            DeltaScanExecutionOptions::new().with_parquet_metadata_size_hint_bytes(Some(0)),
            DeltaScanExecutionOptions::new().with_parquet_full_file_read_threshold_bytes(Some(0)),
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
        let options = DeltaScanExecutionOptions::new()
            .with_max_concurrent_file_reads_per_scan(Some(2))?
            .with_prefetch_files_per_partition(4);

        assert_eq!(options.max_concurrent_file_reads_per_scan(), Some(2));
        assert_eq!(options.max_concurrent_file_reads_per_partition(), 3);
        assert_eq!(options.prefetch_files_per_partition(), 4);
        Ok(())
    }

    #[test]
    fn scan_capacity_resolves_once_from_the_fixed_partition_target()
    -> Result<(), Box<dyn std::error::Error>> {
        let max = tokio::sync::Semaphore::MAX_PERMITS;
        for partition_capacity in [1, 3, max] {
            let options = DeltaScanExecutionOptions::new()
                .with_max_concurrent_file_reads_per_partition(partition_capacity)?;
            for target in [0, 1, 4, max / 3, max / 3 + 1, max, max + 1, usize::MAX] {
                // Widen the product to check both capacity overflow and usize overflow.
                let product = (target as u128) * (partition_capacity as u128);
                let expected = usize::try_from(product.clamp(1, max as u128))?;
                assert_eq!(
                    options.resolved_max_concurrent_file_reads_per_scan(target),
                    expected
                );
                for explicit in [1, 7, max] {
                    let options =
                        options.with_max_concurrent_file_reads_per_scan(Some(explicit))?;
                    assert_eq!(
                        options.resolved_max_concurrent_file_reads_per_scan(target),
                        explicit
                    );
                    assert_eq!(
                        options
                            .with_max_concurrent_file_reads_per_scan(None)?
                            .resolved_max_concurrent_file_reads_per_scan(target),
                        expected,
                    );
                }
            }
        }
        Ok(())
    }
}
