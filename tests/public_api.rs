//! Compile-time and behavioral coverage for the public reader API.

use std::{error::Error as _, future::Future};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use delta_arrow_reader::{
    DeltaBatchStream, DeltaComparison, DeltaPredicate, DeltaProtocolInfo, DeltaReadMetrics,
    DeltaReadMetricsSnapshot, DeltaReaderError, DeltaReaderExecutionOptions, DeltaReaderPhase,
    DeltaScalar, DeltaScan, DeltaScanBuilder, DeltaScanPartitionTargetDiagnosticInput,
    DeltaScanPartitionTargetDiagnosticOutput, DeltaScanPartitionTargetDiagnosticSource,
    DeltaScanPartitionTargetLocalEnvironmentDiagnostic,
    DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus, DeltaSnapshotSelection,
    DeltaStorageOptions, DeltaTable, DeltaTableBuilder, DeltaTableSnapshot, ParquetReaderBackend,
    delta_scan_partition_target_local_environment_diagnostic,
    derive_delta_scan_partition_target_diagnostic,
};
use futures_util::Stream;

#[test]
fn configuration_and_error_contract_is_public() -> Result<(), DeltaReaderError> {
    let snapshot: fn(&DeltaReadMetrics) -> DeltaReadMetricsSnapshot = DeltaReadMetrics::snapshot;
    fn inspect_reader_metrics(snapshot: DeltaReadMetricsSnapshot) {
        let _: Option<u64> = snapshot.estimated_input_rows;
        let _: Option<u64> = snapshot.estimated_input_bytes;
        let _: u64 = snapshot.file_tasks_started;
        let _: u64 = snapshot.file_tasks_completed;
        let _: Option<u64> = snapshot.parquet_task_bytes_admitted;
    }
    let _ = snapshot;
    let _ = inspect_reader_metrics;
    let snapshot_version: fn(&DeltaProtocolInfo) -> u64 = DeltaProtocolInfo::snapshot_version;
    let min_reader_version: fn(&DeltaProtocolInfo) -> i32 = DeltaProtocolInfo::min_reader_version;
    let min_writer_version: fn(&DeltaProtocolInfo) -> i32 = DeltaProtocolInfo::min_writer_version;
    let reader_features: for<'a> fn(&'a DeltaProtocolInfo) -> &'a [String] =
        DeltaProtocolInfo::reader_features;
    let writer_features: for<'a> fn(&'a DeltaProtocolInfo) -> &'a [String] =
        DeltaProtocolInfo::writer_features;
    let first_unsupported_reader_feature: for<'a> fn(&'a DeltaProtocolInfo) -> Option<&'a str> =
        DeltaProtocolInfo::first_unsupported_reader_feature;
    let _ = (
        snapshot_version,
        min_reader_version,
        min_writer_version,
        reader_features,
        writer_features,
        first_unsupported_reader_feature,
    );

    let mut storage_options = DeltaStorageOptions::new();
    storage_options.insert("region".into(), "example".into());
    assert_eq!(storage_options.len(), 1);

    assert_eq!(
        DeltaSnapshotSelection::Version(3),
        DeltaSnapshotSelection::Version(3)
    );

    let options = DeltaReaderExecutionOptions::new()
        .with_reader_backend(ParquetReaderBackend::DeltaKernel)
        .with_max_concurrent_file_reads_per_scan(Some(6))?
        .with_max_concurrent_file_reads_per_partition(3)?
        .with_output_buffer_capacity_per_partition(1)?
        .with_prefetch_file_count_per_partition(2)
        .with_parquet_metadata_size_hint_bytes(Some(65_536))?
        .with_parquet_full_file_read_threshold_bytes(None)?;

    assert_eq!(options.reader_backend(), ParquetReaderBackend::DeltaKernel);

    let error = DeltaReaderExecutionOptions::new()
        .with_output_buffer_capacity_per_partition(0)
        .expect_err("zero output capacity must fail");
    assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
    assert_eq!(error.as_str(), "invalid_configuration");
    assert!(error.source().is_none());

    Ok(())
}

#[test]
fn scan_partition_target_diagnostic_contract_is_public() -> Result<(), DeltaReaderError> {
    let _: DeltaScanPartitionTargetDiagnosticInput = Default::default();
    let local: DeltaScanPartitionTargetLocalEnvironmentDiagnostic =
        delta_scan_partition_target_local_environment_diagnostic();
    let _: DeltaScanPartitionTargetDiagnosticInput = local.policy_input;
    let _: Option<u64> = local.memory_total_bytes;
    let _: Option<u64> = local.memory_available_bytes;
    let _: Option<u64> = local.unix_soft_file_descriptor_limit;
    let _: DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus =
        local.unix_soft_file_descriptor_limit_status;
    let _ = [
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unsupported,
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unknown,
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Finite,
        DeltaScanPartitionTargetLocalUnixFileDescriptorLimitStatus::Unlimited,
    ];
    let input = DeltaScanPartitionTargetDiagnosticInput {
        explicit_target_partitions: None,
        datafusion_target_partitions: Some(8),
        available_parallelism: Some(4),
        available_memory_bytes: None,
        unix_soft_file_descriptor_limit: None,
        min_default_partitions: 1,
        parallelism_multiplier: 1,
        file_descriptors_per_partition: 16,
        available_memory_bytes_per_partition: 256 * 1024 * 1024,
    };
    let output: DeltaScanPartitionTargetDiagnosticOutput =
        derive_delta_scan_partition_target_diagnostic(input)?;

    assert_eq!(output.target_partitions, 4);
    assert_eq!(
        output.source,
        DeltaScanPartitionTargetDiagnosticSource::AvailableParallelismFallback
    );
    let _ = [
        DeltaScanPartitionTargetDiagnosticSource::ExplicitOverride,
        DeltaScanPartitionTargetDiagnosticSource::AvailableParallelismFallback,
        DeltaScanPartitionTargetDiagnosticSource::StaticFallback,
    ];
    assert_eq!(output.explicit_target_partitions, None);
    assert_eq!(output.datafusion_target_partitions, Some(8));
    assert_eq!(output.available_parallelism, Some(4));
    assert_eq!(output.datafusion_target_cap, Some(8));
    assert_eq!(output.unix_file_descriptor_cap, None);
    assert_eq!(output.memory_cap, None);
    Ok(())
}

#[test]
fn exact_predicate_model_is_public() {
    let comparisons = [
        DeltaComparison::Eq,
        DeltaComparison::NotEq,
        DeltaComparison::Lt,
        DeltaComparison::LtEq,
        DeltaComparison::Gt,
        DeltaComparison::GtEq,
    ];
    let copied_comparisons = comparisons;
    assert_eq!(copied_comparisons, comparisons);

    let scalars = vec![
        DeltaScalar::Boolean(true),
        DeltaScalar::Int8(1),
        DeltaScalar::Int16(2),
        DeltaScalar::Int32(3),
        DeltaScalar::Int64(4),
        DeltaScalar::Float32(5.0),
        DeltaScalar::Float64(6.0),
        DeltaScalar::Date32(7),
        DeltaScalar::Decimal128 {
            value: 8,
            precision: 9,
            scale: 1,
        },
        DeltaScalar::Utf8("utf8".into()),
        DeltaScalar::LargeUtf8("large utf8".into()),
        DeltaScalar::Binary(vec![10]),
        DeltaScalar::LargeBinary(vec![11]),
        DeltaScalar::FixedSizeBinary {
            size: 2,
            value: vec![12, 13],
        },
        DeltaScalar::TimestampMicrosecond {
            value: 14,
            timezone: Some("UTC".into()),
        },
    ];
    assert_eq!(scalars, scalars.clone());

    let predicates = vec![
        DeltaPredicate::Boolean(true),
        DeltaPredicate::Compare {
            column: "id".into(),
            op: DeltaComparison::Eq,
            value: DeltaScalar::Int64(1),
        },
        DeltaPredicate::IsNull {
            column: "optional".into(),
        },
        DeltaPredicate::IsNotNull {
            column: "required".into(),
        },
        DeltaPredicate::And(Vec::new()),
        DeltaPredicate::Or(Vec::new()),
        DeltaPredicate::Not(Box::new(DeltaPredicate::Boolean(false))),
    ];
    assert_eq!(predicates, predicates.clone());
    assert!(format!("{predicates:?}").contains("Compare"));
}

#[test]
fn direct_reader_contract_is_public() {
    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_clone<T: Clone>() {}
    fn assert_batch_stream<T: Stream<Item = Result<RecordBatch, DeltaReaderError>>>() {}
    fn assert_future<T>(_: impl Future<Output = T>) {}
    const fn table_version(table: &DeltaTable) -> u64 {
        table.version()
    }
    const fn scan_partition_count(scan: &DeltaScan) -> usize {
        scan.partition_count()
    }

    assert_send_sync::<DeltaTable>();
    assert_clone::<DeltaTable>();
    assert_send::<DeltaBatchStream>();
    assert_batch_stream::<DeltaBatchStream>();

    let builder = DeltaTableBuilder::new("file:///tmp/table")
        .with_storage_options(DeltaStorageOptions::new())
        .with_snapshot_selection(DeltaSnapshotSelection::Version(1))
        .with_execution_options(DeltaReaderExecutionOptions::new());
    assert_future::<Result<DeltaTable, DeltaReaderError>>(builder.load_table());
    let snapshot_builder = DeltaTableBuilder::new("file:///tmp/table");
    assert_future::<Result<DeltaTableSnapshot, DeltaReaderError>>(snapshot_builder.load_snapshot());

    let snapshot_version: fn(&DeltaTableSnapshot) -> u64 = DeltaTableSnapshot::version;
    let snapshot_protocol: for<'a> fn(&'a DeltaTableSnapshot) -> &'a DeltaProtocolInfo =
        DeltaTableSnapshot::protocol;
    let snapshot_table_uri: for<'a> fn(&'a DeltaTableSnapshot) -> &'a str =
        DeltaTableSnapshot::table_uri;
    let validate_snapshot_protocol: fn(&DeltaTableSnapshot) -> Result<(), DeltaReaderError> =
        DeltaTableSnapshot::validate_protocol;
    let into_table: fn(DeltaTableSnapshot) -> Result<DeltaTable, DeltaReaderError> =
        DeltaTableSnapshot::into_table;
    let _ = (
        snapshot_version,
        snapshot_protocol,
        snapshot_table_uri,
        validate_snapshot_protocol,
        into_table,
    );

    let version: fn(&DeltaTable) -> u64 = DeltaTable::version;
    let schema: for<'a> fn(&'a DeltaTable) -> &'a SchemaRef = DeltaTable::schema;
    let protocol: for<'a> fn(&'a DeltaTable) -> &'a DeltaProtocolInfo = DeltaTable::protocol;
    let table_uri: for<'a> fn(&'a DeltaTable) -> &'a str = DeltaTable::table_uri;
    let validate_protocol: fn(&DeltaTable) -> Result<(), DeltaReaderError> =
        DeltaTable::validate_protocol;
    let scan: for<'a> fn(&'a DeltaTable) -> DeltaScanBuilder<'a> = DeltaTable::scan;
    let _ = (
        version,
        table_version,
        schema,
        protocol,
        table_uri,
        validate_protocol,
        scan,
    );

    fn configure_scan<'a>(
        builder: DeltaScanBuilder<'a>,
        predicate: DeltaPredicate,
        options: DeltaReaderExecutionOptions,
    ) -> Result<DeltaScanBuilder<'a>, DeltaReaderError> {
        Ok(builder
            .with_projection(vec!["id".into()])
            .with_predicate(predicate)
            .with_limit(1)
            .with_target_partitions(1)?
            .with_execution_options(options))
    }
    fn assert_scan_futures(builder: DeltaScanBuilder<'_>, scan: DeltaScan) {
        assert_future::<Result<DeltaScan, DeltaReaderError>>(builder.build());
        assert_future::<Result<DeltaBatchStream, DeltaReaderError>>(scan.execute());
    }
    let _ = configure_scan;
    let _ = assert_scan_futures;

    let scan_schema: for<'a> fn(&'a DeltaScan) -> &'a SchemaRef = DeltaScan::schema;
    let partition_count: fn(&DeltaScan) -> usize = DeltaScan::partition_count;
    let _ = (scan_schema, partition_count, scan_partition_count);

    let stream_schema: for<'a> fn(&'a DeltaBatchStream) -> &'a SchemaRef = DeltaBatchStream::schema;
    let metrics: fn(&DeltaBatchStream) -> DeltaReadMetrics = DeltaBatchStream::metrics;
    let _ = (stream_schema, metrics);
}

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_metrics_contract_is_public() {
    use delta_arrow_reader::datafusion::{Metrics, MetricsSnapshot, collect_metrics};

    fn assert_clone<T: Clone>() {}
    fn assert_snapshot_traits<T: std::fmt::Debug + Clone + PartialEq + Eq>() {}
    fn inspect(snapshot: MetricsSnapshot) {
        let _: DeltaReadMetricsSnapshot = snapshot.reader;
        let _: bool = snapshot.use_arrow_view_types;
        let _: Option<u64> = snapshot.output_batch_size;
        let _: u64 = snapshot.dynamic_partition_tasks_pruned;
        let _: u64 = snapshot.dynamic_partition_tasks_kept;
        let _: u64 = snapshot.dynamic_filters_received;
        let _: u64 = snapshot.dynamic_filters_accepted;
        let _: u64 = snapshot.dynamic_filters_unsupported;
        let _: u64 = snapshot.dynamic_filter_snapshots;
        let _: u64 = snapshot.dynamic_partition_tasks_kept_missing_metadata;
        let _: u64 = snapshot.dynamic_partition_tasks_kept_unsupported_expression;
    }

    assert_clone::<Metrics>();
    assert_snapshot_traits::<MetricsSnapshot>();
    let registration_name: for<'a> fn(&'a Metrics) -> Option<&'a str> = Metrics::registration_name;
    let snapshot: fn(&Metrics) -> MetricsSnapshot = Metrics::snapshot;
    let same_instance: fn(&Metrics, &Metrics) -> bool = Metrics::same_instance;
    let collect: fn(&dyn datafusion::physical_plan::ExecutionPlan) -> Vec<Metrics> =
        collect_metrics;
    let _ = (registration_name, snapshot, same_instance, collect, inspect);
}

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_provider_contract_is_public() {
    use delta_arrow_reader::{
        DeltaReaderError, DeltaTable,
        datafusion::{
            DeltaTableProvider, IntraFileRepartitioning, RegisteredTable, ScanOptions,
            register_table,
        },
    };

    fn assert_clone<T: Clone>() {}
    fn assert_debug_clone<T: std::fmt::Debug + Clone>() {}
    fn assert_result_traits<T: std::fmt::Debug + Clone + PartialEq + Eq>() {}

    assert_debug_clone::<ScanOptions>();
    assert!(ScanOptions::default().use_arrow_view_types);
    assert_result_traits::<IntraFileRepartitioning>();
    assert_eq!(
        ScanOptions::default().intra_file_repartitioning,
        IntraFileRepartitioning::WhenBelowTarget
    );
    assert_clone::<DeltaTableProvider>();
    assert_result_traits::<RegisteredTable>();
    let construct: fn(DeltaTable, ScanOptions) -> Result<DeltaTableProvider, DeltaReaderError> =
        DeltaTableProvider::try_new;
    fn register(
        context: &datafusion::execution::context::SessionContext,
        name: String,
        table: DeltaTable,
        options: ScanOptions,
    ) -> Result<RegisteredTable, DeltaReaderError> {
        register_table(context, name, table, options)
    }
    let _ = (construct, register);
}
