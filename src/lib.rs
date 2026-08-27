#![doc = include_str!("../README.md")]
#![cfg_attr(test, allow(clippy::panic))]

mod delta;
mod error;
mod reader;

pub use delta::DeltaProtocolInfo;
pub use error::{DeltaReaderError, DeltaReaderPhase};
#[doc(hidden)]
pub use reader::partition_target::{
    DeltaScanPartitionTargetDiagnosticInput, DeltaScanPartitionTargetDiagnosticOutput,
    DeltaScanPartitionTargetDiagnosticSource, DeltaScanPartitionTargetLocalEnvironmentDiagnostic,
    DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus,
    delta_scan_partition_target_local_environment_diagnostic,
    derive_delta_scan_partition_target_diagnostic,
};
pub use reader::{
    DeltaBatchStream, DeltaComparison, DeltaPredicate, DeltaReadMetrics, DeltaReadMetricsSnapshot,
    DeltaReaderBackend, DeltaReaderExecutionOptions, DeltaScalar, DeltaScan, DeltaScanBuilder,
    DeltaSnapshotSelection, DeltaStorageOptions, DeltaTable, DeltaTableBuilder, DeltaTableSnapshot,
};
#[cfg(feature = "datafusion")]
pub use reader::{
    DeltaDataFusionMetrics, DeltaDataFusionMetricsSnapshot, DeltaDataFusionScanOptions,
    DeltaFileRepartitioning, DeltaTableProvider, RegisteredDeltaTable,
    collect_delta_datafusion_metrics, register_delta_table,
};

/// The crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the crate version.
pub const fn version() -> &'static str {
    VERSION
}
