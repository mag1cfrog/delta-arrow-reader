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
mod delta;
mod error;
mod predicate;
mod reader;

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
pub use predicate::{DeltaComparison, DeltaPredicate, DeltaScalar};
#[doc(hidden)]
pub use reader::partition_target::{
    DeltaScanPartitionTargetDiagnosticInput, DeltaScanPartitionTargetDiagnosticOutput,
    DeltaScanPartitionTargetDiagnosticSource, DeltaScanPartitionTargetLocalEnvironmentDiagnostic,
    DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus,
    delta_scan_partition_target_local_environment_diagnostic,
    derive_delta_scan_partition_target_diagnostic,
};
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
