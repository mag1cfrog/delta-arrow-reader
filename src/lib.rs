#![doc = include_str!("../README.md")]
#![cfg_attr(test, allow(clippy::panic))]

#[cfg(feature = "datafusion")]
mod datafusion_dynamic_filters;
#[cfg(feature = "datafusion")]
mod datafusion_dynamic_partition_pruning;
#[cfg(feature = "datafusion")]
mod datafusion_execution;
#[cfg(feature = "datafusion")]
mod datafusion_planning;
#[cfg(feature = "datafusion")]
mod datafusion_provider;
mod deletion_vector;
mod delta;
mod error;
#[cfg(feature = "native-async")]
#[allow(dead_code)]
mod metered_object_store;
#[cfg(feature = "native-async")]
#[allow(dead_code)]
mod native_async_reader;
#[cfg(feature = "native-async")]
#[allow(dead_code)]
mod native_async_row_group_pruning;
#[cfg(feature = "official-kernel")]
#[allow(dead_code)]
mod official_kernel_reader;
mod partition_target;
mod planning;
mod predicate;
mod reader;
#[allow(dead_code)]
mod scheduling;
mod transform;

#[cfg(feature = "datafusion")]
pub use datafusion_execution::{
    DeltaDataFusionMetrics, DeltaDataFusionMetricsSnapshot, DeltaFileRepartitioning,
    collect_delta_datafusion_metrics,
};
#[cfg(feature = "datafusion")]
pub use datafusion_provider::{
    DeltaDataFusionScanOptions, DeltaTableProvider, RegisteredDeltaTable, register_delta_table,
};
pub use delta::DeltaProtocolInfo;
pub use error::{DeltaReaderError, DeltaReaderPhase};
#[doc(hidden)]
pub use partition_target::{
    DeltaScanPartitionTargetDiagnosticInput, DeltaScanPartitionTargetDiagnosticOutput,
    DeltaScanPartitionTargetDiagnosticSource, DeltaScanPartitionTargetLocalEnvironmentDiagnostic,
    DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus,
    delta_scan_partition_target_local_environment_diagnostic,
    derive_delta_scan_partition_target_diagnostic,
};
pub use predicate::{DeltaComparison, DeltaPredicate, DeltaScalar};
pub use reader::{
    DeltaBatchStream, DeltaReadMetrics, DeltaReadMetricsSnapshot, DeltaReaderBackend,
    DeltaReaderExecutionOptions, DeltaScan, DeltaScanBuilder, DeltaSnapshotSelection,
    DeltaStorageOptions, DeltaTable, DeltaTableBuilder, DeltaTableSnapshot,
};

/// The crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the crate version.
pub const fn version() -> &'static str {
    VERSION
}
