//! Bounded, message-free classification of dependency errors.

use std::{error::Error, io};

use arrow::error::ArrowError;
use delta_kernel::Error as KernelError;
use object_store::{
    Error as StoreError,
    client::{HttpError, HttpErrorKind},
};
use parquet::errors::ParquetError;

/// Classifies an error source without formatting dependency messages.
///
/// This is the classifier used by [`crate::DeltaReaderError::source_kind`]. Hosts
/// can also use it for their own I/O or Arrow errors. Unrecognized sources return
/// `"unknown"`; traversal stops after 16 links with `"source_chain_limit"`.
/// Categories may be extended. The original error is not changed.
pub fn source_kind(mut error: &(dyn Error + 'static)) -> &'static str {
    let mut fallback = "unknown";
    // External source implementations can contain cycles or arbitrarily deep chains.
    for _ in 0..16 {
        if let Some(reader) = error.downcast_ref::<crate::DeltaReaderError>() {
            match reader.source() {
                Some(source) => {
                    error = source;
                    continue;
                }
                None => return reader.code(),
            }
        }
        if let Some(kernel) = error.downcast_ref::<KernelError>() {
            // Kernel 0.25's generic snapshot failures stay unknown; do not parse their text.
            // Kernel 0.29 adds typed cases: https://github.com/delta-io/delta-kernel-rs/pull/3195
            // After lifting the pins in Cargo.toml, revisit these mappings and snapshot tests.
            // Some Kernel wrappers hide or skip their inner error in Error::source().
            error = match kernel {
                KernelError::Backtraced { source, .. } => source.as_ref(),
                KernelError::GenericError { source } => source.as_ref(),
                KernelError::IOError(source) => source,
                KernelError::ObjectStore(source) => source,
                KernelError::Arrow(source) => source,
                KernelError::Parquet(source) => source,
                KernelError::ObjectStorePath(_) => return "invalid_path",
                KernelError::Reqwest(source) => {
                    if source.is_timeout() {
                        return "timeout";
                    }
                    if source.is_connect() {
                        fallback = "http_connect";
                    } else if source.is_decode() {
                        fallback = "http_decode";
                    }
                    source
                }
                KernelError::MissingVersion => return "missing_version",
                KernelError::FileNotFound(_) => return "not_found",
                KernelError::MalformedJson(_) => return "malformed_json",
                KernelError::MissingMetadata => return "missing_metadata",
                KernelError::MissingProtocol => return "missing_protocol",
                KernelError::MissingMetadataAndProtocol => return "missing_metadata_and_protocol",
                KernelError::InvalidProtocol(_) => return "invalid_protocol",
                KernelError::InvalidCheckpoint(_) => return "invalid_checkpoint",
                KernelError::MissingColumn(_) => return "missing_column",
                KernelError::UnexpectedColumnType(_) => return "unexpected_column_type",
                KernelError::InvalidPartitionValues(_) => return "invalid_partition_values",
                KernelError::MissingData(_) => return "missing_data",
                KernelError::DeletionVector(_) => return "deletion_vector",
                KernelError::InvalidSelectionVector(_) => return "invalid_selection_vector",
                KernelError::Schema(_) => return "schema",
                KernelError::Unsupported(_) => return "unsupported",
                KernelError::InternalError(_) => return "kernel_internal",
                KernelError::JoinFailure(_) => return "worker_failed",
                KernelError::EngineDataType(_) => return "engine_data_type",
                KernelError::Extract(..) => return "type_extraction",
                KernelError::InvalidUrl(_) | KernelError::InvalidTableLocation(_) => {
                    return "invalid_table_location";
                }
                KernelError::ParseError(..) => return "invalid_scalar",
                KernelError::InvalidDecimal(_) => return "invalid_decimal",
                KernelError::InvalidStructData(_) => return "invalid_struct_data",
                KernelError::InvalidExpressionEvaluation(_) => return "invalid_expression",
                KernelError::InvalidColumnMappingMode(_) => return "invalid_column_mapping",
                KernelError::InvalidLogPath(_) => return "invalid_log_path",
                KernelError::Utf8Error(_) => return "invalid_utf8",
                KernelError::ParseIntError(_) => return "invalid_integer",
                KernelError::ParseIntervalError(_) => return "invalid_interval",
                KernelError::LiteralExpressionTransformError(_) => return "literal_transform",
                KernelError::StatsValidation(_) => return "invalid_statistics",
                KernelError::InvalidTransactionState(_) => return "invalid_transaction_state",
                KernelError::FileAlreadyExists(_) => return "already_exists",
                KernelError::CheckpointWrite(_) => return "checkpoint_write",
                KernelError::ChecksumWriteUnsupported(_)
                | KernelError::ChangeDataFeedUnsupported(_) => return "unsupported",
                KernelError::ChangeDataFeedIncompatibleSchema(..) => return "schema",
                KernelError::LogHistory(source) => source.as_ref(),
                _ => match error.source() {
                    Some(source) => source,
                    None => return fallback,
                },
            };
            continue;
        }

        if let Some(history) =
            error.downcast_ref::<delta_kernel::history_manager::error::LogHistoryError>()
        {
            use delta_kernel::history_manager::error::LogHistoryError;
            match history {
                LogHistoryError::NoCommitsFound { .. } => return "missing_version",
                LogHistoryError::NoRecreatableCommit { .. } => return "missing_log_history",
                LogHistoryError::InvalidTimestampRange { .. } => return "invalid_timestamp_range",
                LogHistoryError::EmptyTimestampRange { .. } => return "empty_timestamp_range",
                LogHistoryError::TimestampOutOfRange { .. } => return "timestamp_out_of_range",
                LogHistoryError::Internal {
                    source: Some(source),
                    ..
                } => {
                    error = source.as_ref();
                    continue;
                }
                LogHistoryError::Internal { source: None, .. } => return "kernel_internal",
            }
        }

        if let Some(store) = error.downcast_ref::<StoreError>() {
            match store {
                StoreError::NotFound { .. } => return "not_found",
                StoreError::PermissionDenied { .. } => return "permission_denied",
                StoreError::Unauthenticated { .. } => return "unauthenticated",
                StoreError::InvalidPath { .. } => return "invalid_path",
                StoreError::UnknownConfigurationKey { .. } => {
                    return "invalid_storage_configuration";
                }
                StoreError::NotSupported { .. } | StoreError::NotImplemented { .. } => {
                    return "unsupported";
                }
                StoreError::AlreadyExists { .. } => return "already_exists",
                StoreError::Precondition { .. } => return "precondition",
                StoreError::NotModified { .. } => return "not_modified",
                _ => {}
            }
        }

        if let Some(io) = error.downcast_ref::<io::Error>() {
            match io.kind() {
                io::ErrorKind::NotFound => return "not_found",
                io::ErrorKind::PermissionDenied => return "permission_denied",
                io::ErrorKind::TimedOut => return "timeout",
                io::ErrorKind::ConnectionRefused => return "connection_refused",
                io::ErrorKind::ConnectionReset => return "connection_reset",
                io::ErrorKind::ConnectionAborted => return "connection_aborted",
                io::ErrorKind::NotConnected => return "not_connected",
                io::ErrorKind::BrokenPipe => return "broken_pipe",
                io::ErrorKind::UnexpectedEof => return "unexpected_eof",
                io::ErrorKind::InvalidData => return "invalid_data",
                io::ErrorKind::InvalidInput => return "invalid_input",
                io::ErrorKind::Interrupted => return "interrupted",
                io::ErrorKind::OutOfMemory => return "out_of_memory",
                io::ErrorKind::AlreadyExists => return "already_exists",
                io::ErrorKind::Unsupported => return "unsupported",
                _ => {}
            }
            if let Some(inner) = io.get_ref() {
                error = inner;
                continue;
            }
        }

        if let Some(join) = error.downcast_ref::<tokio::task::JoinError>() {
            return if join.is_panic() {
                "worker_panicked"
            } else if join.is_cancelled() {
                "worker_cancelled"
            } else {
                "worker_failed"
            };
        }

        if error.is::<tokio::time::error::Elapsed>() {
            return "timeout";
        }
        if let Some(http) = error.downcast_ref::<HttpError>() {
            match http.kind() {
                HttpErrorKind::Timeout => return "timeout",
                HttpErrorKind::Interrupted => fallback = "interrupted",
                HttpErrorKind::Connect => fallback = "http_connect",
                HttpErrorKind::Request => fallback = "http_request",
                HttpErrorKind::Decode => fallback = "http_decode",
                _ => {}
            }
        }

        #[cfg(feature = "datafusion")]
        {
            use datafusion::common::DataFusionError;
            let datafusion = error
                .downcast_ref::<DataFusionError>()
                .or_else(|| {
                    error
                        .downcast_ref::<Box<DataFusionError>>()
                        .map(AsRef::as_ref)
                })
                .or_else(|| {
                    error
                        .downcast_ref::<std::sync::Arc<DataFusionError>>()
                        .map(AsRef::as_ref)
                });
            match datafusion {
                Some(DataFusionError::SQL(..)) => return "sql_parse",
                Some(DataFusionError::Plan(_)) => return "datafusion_plan",
                Some(DataFusionError::Execution(_)) => return "datafusion_execution",
                Some(DataFusionError::Internal(_)) => return "datafusion_internal",
                Some(DataFusionError::Configuration(_)) => return "invalid_configuration",
                Some(DataFusionError::ResourcesExhausted(_)) => return "resources_exhausted",
                Some(DataFusionError::NotImplemented(_)) => return "unsupported",
                Some(DataFusionError::SchemaError(..)) => return "schema",
                Some(DataFusionError::Collection(_)) => return "multiple_errors",
                _ => {}
            }
        }
        if let Some(http) = error.downcast_ref::<hyper::Error>() {
            if http.is_timeout() {
                return "timeout";
            }
            if http.is_parse() {
                return "http_parse";
            }
            if http.is_incomplete_message() {
                return "unexpected_eof";
            }
            if http.is_closed() {
                return "connection_closed";
            }
            if http.is_canceled() {
                return "http_cancelled";
            }
        }

        if let Some(arrow) = error.downcast_ref::<ArrowError>() {
            match arrow {
                ArrowError::ExternalError(_) | ArrowError::IoError(..) => {}
                ArrowError::NotYetImplemented(_) => return "unsupported",
                ArrowError::CastError(_) => return "arrow_cast",
                ArrowError::MemoryError(_) => return "arrow_memory",
                ArrowError::ParseError(_) => return "arrow_parse",
                ArrowError::SchemaError(_) => return "schema",
                ArrowError::ComputeError(_) => return "arrow_compute",
                ArrowError::DivideByZero => return "divide_by_zero",
                ArrowError::ArithmeticOverflow(_)
                | ArrowError::DictionaryKeyOverflowError
                | ArrowError::RunEndIndexOverflowError
                | ArrowError::OffsetOverflowError(_) => return "arithmetic_overflow",
                ArrowError::InvalidArgumentError(_) => return "invalid_input",
                ArrowError::JsonError(_) => return "arrow_json",
                ArrowError::ParquetError(_) => return "parquet",
                ArrowError::IpcError(_) => return "arrow_ipc",
                ArrowError::CDataInterface(_) => return "arrow_c_data",
                ArrowError::CsvError(_) => return "arrow_csv",
                ArrowError::AvroError(_) => return "arrow_avro",
            }
        }
        if let Some(parquet) = error.downcast_ref::<ParquetError>() {
            match parquet {
                ParquetError::External(_) => {}
                ParquetError::NYI(_) => return "unsupported",
                ParquetError::EOF(_) => return "unexpected_eof",
                ParquetError::IndexOutOfBound(..) => return "index_out_of_bounds",
                ParquetError::NeedMoreData(_) | ParquetError::NeedMoreDataRange(_) => {
                    return "need_more_data";
                }
                ParquetError::ArrowError(_) => return "arrow",
                ParquetError::General(_) => return "parquet",
                _ => {}
            }
        }

        error = match error.source() {
            Some(source) => source,
            None => return fallback,
        };
    }
    "source_chain_limit"
}

#[cfg(test)]
mod tests {
    use std::{backtrace::Backtrace, fmt};

    use super::*;
    use crate::DeltaReaderError;

    struct Unprintable;

    impl fmt::Debug for Unprintable {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("diagnostics must not format dependency Debug");
        }
    }

    impl fmt::Display for Unprintable {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("diagnostics must not format dependency Display");
        }
    }

    impl Error for Unprintable {}

    fn snapshot_error(source: impl Error + Send + Sync + 'static) -> DeltaReaderError {
        DeltaReaderError::SnapshotLoad {
            reason: "snapshot_load_failed",
            source: Box::new(source),
        }
    }

    #[test]
    fn kernel_wrappers_preserve_storage_and_io_categories_without_formatting() {
        for (store, expected) in [
            (
                StoreError::NotFound {
                    path: "secret-object".into(),
                    source: Box::new(Unprintable),
                },
                "not_found",
            ),
            (
                StoreError::PermissionDenied {
                    path: "secret-object".into(),
                    source: Box::new(Unprintable),
                },
                "permission_denied",
            ),
            (
                StoreError::Unauthenticated {
                    path: "secret-object".into(),
                    source: Box::new(Unprintable),
                },
                "unauthenticated",
            ),
        ] {
            let error = snapshot_error(KernelError::Backtraced {
                source: Box::new(KernelError::ObjectStore(store)),
                backtrace: Box::new(Backtrace::disabled()),
            });
            assert_eq!(error.source_kind(), Some(expected));
            assert!(
                error
                    .source()
                    .is_some_and(|source| source.is::<KernelError>())
            );
            assert!(!format!("{error:?}").contains("secret-object"));
        }

        let error = snapshot_error(KernelError::IOError(io::Error::new(
            io::ErrorKind::TimedOut,
            Unprintable,
        )));
        assert_eq!(error.source_kind(), Some("timeout"));
    }

    #[test]
    fn typed_snapshot_failures_and_unknown_sources_remain_distinct() {
        let malformed = serde_json::from_str::<serde_json::Value>("secret-invalid-json")
            .err()
            .unwrap_or_else(|| panic!("fixture must be invalid JSON"));
        for (source, expected) in [
            (KernelError::MissingVersion, "missing_version"),
            (KernelError::MalformedJson(malformed), "malformed_json"),
            (KernelError::MissingMetadata, "missing_metadata"),
            (
                KernelError::LogHistory(Box::new(
                    delta_kernel::history_manager::error::LogHistoryError::Internal {
                        context: "secret-history-context",
                        source: Some(Box::new(KernelError::MissingMetadata)),
                    },
                )),
                "missing_metadata",
            ),
            (
                KernelError::LogHistory(Box::new(
                    delta_kernel::history_manager::error::LogHistoryError::Internal {
                        context: "secret-history-context",
                        source: None,
                    },
                )),
                "kernel_internal",
            ),
            (KernelError::Generic("secret-message".into()), "unknown"),
            (KernelError::generic_err(Unprintable), "unknown"),
        ] {
            let error = snapshot_error(source);
            assert_eq!(error.reason(), "snapshot_load_failed");
            assert_eq!(error.source_kind(), Some(expected));
        }
    }

    #[test]
    fn external_wrappers_and_http_context_do_not_hide_a_typed_timeout() {
        let error = snapshot_error(ArrowError::ExternalError(Box::new(ParquetError::External(
            Box::new(io::Error::other(HttpError::new(
                HttpErrorKind::Connect,
                io::Error::new(io::ErrorKind::TimedOut, Unprintable),
            ))),
        ))));
        assert_eq!(error.source_kind(), Some("timeout"));
        assert_eq!(
            source_kind(&HttpError::new(HttpErrorKind::Connect, Unprintable)),
            "http_connect"
        );
        assert_eq!(
            source_kind(&ArrowError::JsonError("secret-json".into())),
            "arrow_json"
        );
        assert_eq!(
            source_kind(&ParquetError::EOF("secret-path".into())),
            "unexpected_eof"
        );
    }

    #[test]
    fn interrupted_http_errors_preserve_typed_causes_and_fallback() {
        for (kind, expected) in [
            (io::ErrorKind::ConnectionReset, "connection_reset"),
            (io::ErrorKind::ConnectionAborted, "connection_aborted"),
            (io::ErrorKind::BrokenPipe, "broken_pipe"),
            (io::ErrorKind::UnexpectedEof, "unexpected_eof"),
        ] {
            let error = snapshot_error(HttpError::new(
                HttpErrorKind::Interrupted,
                io::Error::new(kind, Unprintable),
            ));
            assert_eq!(error.source_kind(), Some(expected));
        }
        let error = snapshot_error(HttpError::new(HttpErrorKind::Interrupted, Unprintable));
        assert_eq!(error.source_kind(), Some("interrupted"));
    }

    #[test]
    fn source_traversal_stops_on_cycles_and_deep_wrappers() {
        #[derive(Debug)]
        struct Cycle;

        impl fmt::Display for Cycle {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("diagnostics must not format cyclic errors");
            }
        }

        impl Error for Cycle {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                Some(self)
            }
        }

        assert_eq!(source_kind(&Cycle), "source_chain_limit");
        let mut error: Box<dyn Error + Send + Sync> = Box::new(KernelError::MissingVersion);
        for _ in 0..32 {
            error = Box::new(ArrowError::ExternalError(error));
        }
        assert_eq!(source_kind(error.as_ref()), "source_chain_limit");
    }

    #[cfg(feature = "datafusion")]
    #[test]
    fn datafusion_boxing_preserves_categories_and_does_not_choose_one_of_many_causes() {
        use datafusion::common::DataFusionError;
        use datafusion::sql::sqlparser::parser::ParserError;

        let parse = DataFusionError::Shared(std::sync::Arc::new(DataFusionError::Context(
            "secret-context".into(),
            Box::new(DataFusionError::SQL(
                Box::new(ParserError::ParserError("secret-query".into())),
                Some("secret-backtrace".into()),
            )),
        )));
        assert_eq!(source_kind(&parse), "sql_parse");
        assert_eq!(
            source_kind(&DataFusionError::External(Box::new(Unprintable))),
            "unknown"
        );

        let error = DeltaReaderError::DataFusionAdapter {
            reason: "scan_output_projection_failed",
            source: Box::new(DataFusionError::External(Box::new(snapshot_error(
                KernelError::MissingMetadata,
            )))),
        };
        assert_eq!(error.source_kind(), Some("missing_metadata"));
        let error = DeltaReaderError::DataFusionAdapter {
            reason: "test",
            source: Box::new(DataFusionError::External(Box::new(
                DeltaReaderError::InvalidProjection {
                    reason: "unknown_projection_column",
                },
            ))),
        };
        assert_eq!(error.source_kind(), Some("invalid_projection"));
        let error = DeltaReaderError::DataFusionAdapter {
            reason: "test",
            source: Box::new(DataFusionError::Execution("secret-query".into())),
        };
        assert_eq!(error.source_kind(), Some("datafusion_execution"));
        assert_eq!(
            source_kind(&DataFusionError::Collection(vec![
                DataFusionError::IoError(io::ErrorKind::NotFound.into()),
                DataFusionError::IoError(io::ErrorKind::PermissionDenied.into()),
            ])),
            "multiple_errors"
        );
        let collection = DataFusionError::External(Box::new(std::sync::Arc::new(
            DataFusionError::Collection(vec![
                DataFusionError::IoError(io::ErrorKind::NotFound.into()),
                DataFusionError::IoError(io::ErrorKind::PermissionDenied.into()),
            ]),
        )));
        assert_eq!(source_kind(&collection), "multiple_errors");
    }
}
