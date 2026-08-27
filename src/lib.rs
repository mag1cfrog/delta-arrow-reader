#![doc = include_str!("../README.md")]
#![cfg_attr(test, allow(clippy::panic))]

mod delta;
mod error;
mod reader;

#[cfg(feature = "datafusion")]
pub use reader::datafusion;

pub use delta::DeltaProtocolInfo;
pub use error::{DeltaReaderError, DeltaReaderPhase};
#[doc(hidden)]
pub mod diagnostics {
    pub mod partition_target {
        pub use crate::reader::partition_target::{
            DeltaScanPartitionTargetDiagnosticInput as Input,
            DeltaScanPartitionTargetDiagnosticOutput as Output,
            DeltaScanPartitionTargetDiagnosticSource as Source,
            DeltaScanPartitionTargetLocalEnvironmentDiagnostic as LocalEnvironment,
            DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus as UnixFileDescriptorLimitStatus,
            delta_scan_partition_target_local_environment_diagnostic as collect_local_environment,
            derive_delta_scan_partition_target_diagnostic as derive,
        };
    }
}
pub use reader::{
    DeltaBatchStream, DeltaComparison, DeltaPredicate, DeltaReaderExecutionOptions, DeltaScalar,
    DeltaScan, DeltaScanBuilder, DeltaScanMetrics, DeltaScanMetricsSnapshot,
    DeltaSnapshotSelection, DeltaStorageOptions, DeltaTable, DeltaTableBuilder, DeltaTableSnapshot,
    ParquetReaderBackend,
};
/// The crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
