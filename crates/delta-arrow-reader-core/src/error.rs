mod source;

pub use source::source_kind;

use std::{error::Error as _, fmt};

use arrow::datatypes::DataType;
use snafu::Snafu;

/// A step from the root predicate to a rejected comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaPredicatePathStep {
    /// Zero-based child of an `And` predicate.
    AndChild(usize),
    /// Zero-based child of an `Or` predicate.
    OrChild(usize),
    /// The child of a `Not` predicate.
    NotChild,
}

/// Reader operation phase associated with an error.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaReaderPhase {
    /// Reader configuration validation.
    Configuration,
    /// Delta table path or URL parsing and normalization.
    TableLocation,
    /// Object-store initialization.
    Storage,
    /// Delta snapshot loading.
    Snapshot,
    /// Delta protocol validation.
    Protocol,
    /// Delta-to-Arrow schema conversion.
    Schema,
    /// Delta scan planning.
    ScanPlanning,
    /// Delta data-file reading.
    DataFileRead,
    /// Delta deletion-vector handling.
    DeletionVector,
    /// Physical-to-logical data transformation.
    Transform,
    /// Reader execution.
    Execution,
    /// Optional DataFusion integration.
    DataFusion,
}

impl DeltaReaderPhase {
    /// Returns the stable snake_case phase name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Configuration => "configuration",
            Self::TableLocation => "table_location",
            Self::Storage => "storage",
            Self::Snapshot => "snapshot",
            Self::Protocol => "protocol",
            Self::Schema => "schema",
            Self::ScanPlanning => "scan_planning",
            Self::DataFileRead => "data_file_read",
            Self::DeletionVector => "deletion_vector",
            Self::Transform => "transform",
            Self::Execution => "execution",
            Self::DataFusion => "datafusion",
        }
    }
}

/// Redacted failure returned by reader APIs.
#[non_exhaustive]
#[derive(Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum DeltaReaderError {
    /// Reader configuration is invalid.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=configuration code=invalid_configuration reason={reason}"
    ))]
    InvalidConfiguration {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// The table path or URL is invalid.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=table_location code=invalid_table_location reason={reason}"
    ))]
    InvalidTableLocation {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// Object-store initialization failed.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=storage code=storage_initialization reason={reason}"
    ))]
    StorageInitialization {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// Snapshot loading failed.
    #[non_exhaustive]
    #[snafu(display("delta reader error: phase=snapshot code=snapshot_load reason={reason}"))]
    SnapshotLoad {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// The table protocol is unsupported.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=protocol code=unsupported_protocol reason={reason}"
    ))]
    UnsupportedProtocol {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// Delta-to-Arrow schema conversion failed.
    #[non_exhaustive]
    #[snafu(display("delta reader error: phase=schema code=schema_conversion reason={reason}"))]
    SchemaConversion {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// A requested projection is invalid.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=scan_planning code=invalid_projection reason={reason}"
    ))]
    InvalidProjection {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// A requested predicate is unsupported.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=scan_planning code=unsupported_predicate reason={reason}"
    ))]
    UnsupportedPredicate {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// A comparison's scalar type does not match its column.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=scan_planning code=unsupported_predicate reason=scalar_type_mismatch"
    ))]
    PredicateTypeMismatch {
        /// Column type. Nested names and timezone strings may contain private data.
        expected: DataType,
        /// Scalar type, without the literal value. Timezone strings may contain private data.
        supplied: DataType,
        /// Steps from the root to the comparison, in root-to-leaf order.
        path: Vec<DeltaPredicatePathStep>,
    },
    /// Delta scan planning failed.
    #[non_exhaustive]
    #[snafu(display("delta reader error: phase=scan_planning code=scan_planning reason={reason}"))]
    ScanPlanning {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// Delta scan file tasks could not be grouped into partitions.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=scan_planning code=scan_partition_planning reason={reason}"
    ))]
    ScanPartitionPlanning {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// A Delta data file could not be read.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=data_file_read code=data_file_read reason={reason}"
    ))]
    DataFileRead {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// A deletion vector could not be read.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=deletion_vector code=deletion_vector_read reason={reason}"
    ))]
    DeletionVectorRead {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// A physical-to-logical transform failed.
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=transform code=physical_to_logical_transform reason={reason}"
    ))]
    PhysicalToLogicalTransform {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying dependency failure.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// Reader execution was cancelled.
    #[non_exhaustive]
    #[snafu(display("delta reader error: phase=execution code=cancelled reason={reason}"))]
    Cancelled {
        /// Fixed redacted reason category.
        reason: &'static str,
    },
    /// An execution worker failed before it could return its result.
    ///
    /// The `cancelled` code is retained for compatibility with scheduler errors.
    /// Inspect the source kind to distinguish worker failure from caller cancellation.
    #[non_exhaustive]
    #[snafu(display("delta reader error: phase=execution code=cancelled reason={reason}"))]
    ExecutionTaskFailed {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying task failure, including its panic or cancellation category.
        #[snafu(source(from(exact)))]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// Optional DataFusion integration failed.
    #[cfg(feature = "datafusion")]
    #[non_exhaustive]
    #[snafu(display(
        "delta reader error: phase=datafusion code=datafusion_adapter reason={reason}"
    ))]
    DataFusionAdapter {
        /// Fixed redacted reason category.
        reason: &'static str,
        /// Underlying DataFusion failure.
        #[snafu(source(from(datafusion::common::DataFusionError, Box::new)))]
        source: Box<datafusion::common::DataFusionError>,
    },
}

impl DeltaReaderError {
    /// Returns the stable snake_case error code.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidConfiguration { .. } => "invalid_configuration",
            Self::InvalidTableLocation { .. } => "invalid_table_location",
            Self::StorageInitialization { .. } => "storage_initialization",
            Self::SnapshotLoad { .. } => "snapshot_load",
            Self::UnsupportedProtocol { .. } => "unsupported_protocol",
            Self::SchemaConversion { .. } => "schema_conversion",
            Self::InvalidProjection { .. } => "invalid_projection",
            Self::UnsupportedPredicate { .. } | Self::PredicateTypeMismatch { .. } => {
                "unsupported_predicate"
            }
            Self::ScanPlanning { .. } => "scan_planning",
            Self::ScanPartitionPlanning { .. } => "scan_partition_planning",
            Self::DataFileRead { .. } => "data_file_read",
            Self::DeletionVectorRead { .. } => "deletion_vector_read",
            Self::PhysicalToLogicalTransform { .. } => "physical_to_logical_transform",
            Self::Cancelled { .. } | Self::ExecutionTaskFailed { .. } => "cancelled",
            #[cfg(feature = "datafusion")]
            Self::DataFusionAdapter { .. } => "datafusion_adapter",
        }
    }

    /// Returns the reader phase that failed.
    pub const fn phase(&self) -> DeltaReaderPhase {
        match self {
            Self::InvalidConfiguration { .. } => DeltaReaderPhase::Configuration,
            Self::InvalidTableLocation { .. } => DeltaReaderPhase::TableLocation,
            Self::StorageInitialization { .. } => DeltaReaderPhase::Storage,
            Self::SnapshotLoad { .. } => DeltaReaderPhase::Snapshot,
            Self::UnsupportedProtocol { .. } => DeltaReaderPhase::Protocol,
            Self::SchemaConversion { .. } => DeltaReaderPhase::Schema,
            Self::InvalidProjection { .. }
            | Self::UnsupportedPredicate { .. }
            | Self::PredicateTypeMismatch { .. }
            | Self::ScanPlanning { .. }
            | Self::ScanPartitionPlanning { .. } => DeltaReaderPhase::ScanPlanning,
            Self::Cancelled { .. } | Self::ExecutionTaskFailed { .. } => {
                DeltaReaderPhase::Execution
            }
            Self::DataFileRead { .. } => DeltaReaderPhase::DataFileRead,
            Self::DeletionVectorRead { .. } => DeltaReaderPhase::DeletionVector,
            Self::PhysicalToLogicalTransform { .. } => DeltaReaderPhase::Transform,
            #[cfg(feature = "datafusion")]
            Self::DataFusionAdapter { .. } => DeltaReaderPhase::DataFusion,
        }
    }

    /// Returns the fixed, redacted reason supplied by the reader.
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::PredicateTypeMismatch { .. } => "scalar_type_mismatch",
            Self::InvalidConfiguration { reason }
            | Self::InvalidTableLocation { reason }
            | Self::StorageInitialization { reason, .. }
            | Self::SnapshotLoad { reason, .. }
            | Self::UnsupportedProtocol { reason }
            | Self::SchemaConversion { reason, .. }
            | Self::InvalidProjection { reason }
            | Self::UnsupportedPredicate { reason }
            | Self::ScanPlanning { reason, .. }
            | Self::ScanPartitionPlanning { reason }
            | Self::DataFileRead { reason, .. }
            | Self::DeletionVectorRead { reason, .. }
            | Self::PhysicalToLogicalTransform { reason, .. }
            | Self::Cancelled { reason }
            | Self::ExecutionTaskFailed { reason, .. } => reason,
            #[cfg(feature = "datafusion")]
            Self::DataFusionAdapter { reason, .. } => reason,
        }
    }

    /// Classifies the dependency failure without formatting its message.
    ///
    /// Returns `None` when this error has no source. `Some("unknown")` means the
    /// source exposes no recognized category; `Some("source_chain_limit")` means
    /// traversal stopped at its depth bound. Categories describe observed error
    /// types, not retry advice. New categories may be added.
    ///
    /// See the [tracing reference](crate::guides::reference::tracing) for the
    /// categories and wrapper traversal rules. The original source is unchanged.
    pub fn source_kind(&self) -> Option<&'static str> {
        self.source().map(source_kind)
    }

    /// Finds the first reader error in a source chain, including boxed or shared errors.
    ///
    /// Inspects at most 16 links without formatting messages. Returns `None` for
    /// chains without a reader error or when the traversal limit is reached.
    pub fn find_source<'a>(mut source: &'a (dyn std::error::Error + 'static)) -> Option<&'a Self> {
        for _ in 0..16 {
            if let Some(reader) = source
                .downcast_ref::<Self>()
                .or_else(|| source.downcast_ref::<Box<Self>>().map(AsRef::as_ref))
                .or_else(|| {
                    source
                        .downcast_ref::<std::sync::Arc<Self>>()
                        .map(AsRef::as_ref)
                })
            {
                return Some(reader);
            }
            source = source.source()?;
        }
        None
    }
}

impl fmt::Debug for DeltaReaderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error as _, io};

    use super::{DeltaReaderError, DeltaReaderPhase};

    #[test]
    fn phase_names_are_stable() {
        let cases = [
            (DeltaReaderPhase::Configuration, "configuration"),
            (DeltaReaderPhase::TableLocation, "table_location"),
            (DeltaReaderPhase::Storage, "storage"),
            (DeltaReaderPhase::Snapshot, "snapshot"),
            (DeltaReaderPhase::Protocol, "protocol"),
            (DeltaReaderPhase::Schema, "schema"),
            (DeltaReaderPhase::ScanPlanning, "scan_planning"),
            (DeltaReaderPhase::DataFileRead, "data_file_read"),
            (DeltaReaderPhase::DeletionVector, "deletion_vector"),
            (DeltaReaderPhase::Transform, "transform"),
            (DeltaReaderPhase::Execution, "execution"),
            (DeltaReaderPhase::DataFusion, "datafusion"),
        ];

        for (phase, expected) in cases {
            assert_eq!(phase.as_str(), expected);
        }
    }

    #[test]
    fn variants_map_to_stable_accessors_and_sources() {
        let errors = [
            (
                DeltaReaderError::InvalidConfiguration {
                    reason: "invalid_configuration",
                },
                "invalid_configuration",
                DeltaReaderPhase::Configuration,
                false,
            ),
            (
                DeltaReaderError::InvalidTableLocation {
                    reason: "invalid_table_location",
                },
                "invalid_table_location",
                DeltaReaderPhase::TableLocation,
                false,
            ),
            (
                DeltaReaderError::StorageInitialization {
                    reason: "storage_initialization",
                    source: dependency_source(),
                },
                "storage_initialization",
                DeltaReaderPhase::Storage,
                true,
            ),
            (
                DeltaReaderError::SnapshotLoad {
                    reason: "snapshot_load",
                    source: dependency_source(),
                },
                "snapshot_load",
                DeltaReaderPhase::Snapshot,
                true,
            ),
            (
                DeltaReaderError::UnsupportedProtocol {
                    reason: "unsupported_protocol",
                },
                "unsupported_protocol",
                DeltaReaderPhase::Protocol,
                false,
            ),
            (
                DeltaReaderError::SchemaConversion {
                    reason: "schema_conversion",
                    source: dependency_source(),
                },
                "schema_conversion",
                DeltaReaderPhase::Schema,
                true,
            ),
            (
                DeltaReaderError::InvalidProjection {
                    reason: "invalid_projection",
                },
                "invalid_projection",
                DeltaReaderPhase::ScanPlanning,
                false,
            ),
            (
                DeltaReaderError::UnsupportedPredicate {
                    reason: "unsupported_predicate",
                },
                "unsupported_predicate",
                DeltaReaderPhase::ScanPlanning,
                false,
            ),
            (
                DeltaReaderError::ScanPlanning {
                    reason: "scan_planning",
                    source: dependency_source(),
                },
                "scan_planning",
                DeltaReaderPhase::ScanPlanning,
                true,
            ),
            (
                DeltaReaderError::ScanPartitionPlanning {
                    reason: "scan_partition_planning",
                },
                "scan_partition_planning",
                DeltaReaderPhase::ScanPlanning,
                false,
            ),
            (
                DeltaReaderError::DataFileRead {
                    reason: "data_file_read",
                    source: dependency_source(),
                },
                "data_file_read",
                DeltaReaderPhase::DataFileRead,
                true,
            ),
            (
                DeltaReaderError::DeletionVectorRead {
                    reason: "deletion_vector_read",
                    source: dependency_source(),
                },
                "deletion_vector_read",
                DeltaReaderPhase::DeletionVector,
                true,
            ),
            (
                DeltaReaderError::PhysicalToLogicalTransform {
                    reason: "physical_to_logical_transform",
                    source: dependency_source(),
                },
                "physical_to_logical_transform",
                DeltaReaderPhase::Transform,
                true,
            ),
            (
                DeltaReaderError::Cancelled {
                    reason: "cancelled",
                },
                "cancelled",
                DeltaReaderPhase::Execution,
                false,
            ),
            (
                DeltaReaderError::ExecutionTaskFailed {
                    reason: "cancelled",
                    source: dependency_source(),
                },
                "cancelled",
                DeltaReaderPhase::Execution,
                true,
            ),
            #[cfg(feature = "datafusion")]
            (
                DeltaReaderError::DataFusionAdapter {
                    reason: "datafusion_adapter",
                    source: Box::new(datafusion::common::DataFusionError::Execution(
                        "sensitive dependency detail".into(),
                    )),
                },
                "datafusion_adapter",
                DeltaReaderPhase::DataFusion,
                true,
            ),
        ];

        for (error, name, phase, has_source) in errors {
            assert_eq!(error.source().is_some(), has_source);
            assert_eq!(error.code(), name);
            assert_eq!(error.phase(), phase);
            assert_eq!(error.reason(), name);
            assert_eq!(error.source_kind().is_some(), has_source);
            let display = error.to_string();
            let debug = format!("{error:?}");
            assert!(display.contains(&format!("phase={}", phase.as_str())));
            assert!(display.contains(&format!("code={name}")));
            assert!(!display.contains("sensitive dependency detail"));
            assert!(!debug.contains("sensitive dependency detail"));
        }
    }

    fn dependency_source() -> Box<dyn std::error::Error + Send + Sync + 'static> {
        Box::new(io::Error::other("sensitive dependency detail"))
    }

    #[test]
    fn boxed_source_preserves_its_concrete_type() {
        let error = DeltaReaderError::DataFileRead {
            reason: "data_file_read",
            source: dependency_source(),
        };

        assert!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .is_some()
        );
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn datafusion_source_preserves_its_boxed_type() {
        let error = DeltaReaderError::DataFusionAdapter {
            reason: "datafusion_adapter",
            source: Box::new(datafusion::common::DataFusionError::Execution(
                "failure".into(),
            )),
        };

        assert!(
            error
                .source()
                .and_then(|source| {
                    source.downcast_ref::<Box<datafusion::common::DataFusionError>>()
                })
                .is_some()
        );
    }
}
