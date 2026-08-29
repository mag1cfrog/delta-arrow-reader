//! Compile-time and behavioral coverage for the public reader API.

use std::{error::Error as _, future::Future};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use delta_arrow_reader::{
    DeltaBatchStream, DeltaComparison, DeltaPredicate, DeltaProtocol, DeltaReaderError,
    DeltaReaderPhase, DeltaScalar, DeltaScan, DeltaScanBuilder, DeltaScanExecutionOptions,
    DeltaScanMetrics, DeltaScanMetricsSnapshot, DeltaSnapshotSelection, DeltaStorageOptions,
    DeltaTable, DeltaTableBuilder, DeltaTableSnapshot, ParquetReaderBackend,
    diagnostics::partition_target::{
        Input, LocalEnvironment, Output, Source, UnixFileDescriptorLimitStatus,
        collect_local_environment, derive,
    },
};
use futures_util::Stream;

#[test]
fn configuration_and_error_contract_is_public() -> Result<(), DeltaReaderError> {
    let snapshot: fn(&DeltaScanMetrics) -> DeltaScanMetricsSnapshot = DeltaScanMetrics::snapshot;
    fn inspect_scan_metrics(snapshot: DeltaScanMetricsSnapshot) {
        let _: Option<u64> = snapshot.add_actions_excluded_during_planning;
        let _: Option<u64> = snapshot.estimated_input_rows;
        let _: Option<u64> = snapshot.estimated_input_bytes;
        let _: u64 = snapshot.file_tasks_started;
        let _: u64 = snapshot.file_tasks_completed;
        let _: u64 = snapshot.scheduler_batches_emitted;
        let _: u64 = snapshot.scheduler_rows_emitted;
        let _: u64 = snapshot.deletion_vector_coordinate_rejections;
        let _: Option<u64> = snapshot.parquet_data_file_exact_ranges_requested;
        let _: Option<u64> = snapshot.parquet_data_file_exact_range_bytes_requested;
        let _: Option<u64> = snapshot.parquet_data_file_physical_range_requests_planned;
        let _: Option<u64> = snapshot.parquet_data_file_physical_range_bytes_planned;
        let _: Option<u64> = snapshot.parquet_data_file_cold_start_range_plans;
        let _: Option<u64> = snapshot.parquet_data_file_cost_based_exact_range_plans;
        let _: Option<u64> = snapshot.parquet_data_file_cost_based_merged_range_plans;
        let _: Option<u64> = snapshot.parquet_data_file_store_delegated_range_plans;
        let _: Option<u64> = snapshot.estimated_parquet_task_bytes_admitted;
    }
    let _ = snapshot;
    let _ = inspect_scan_metrics;
    let min_reader_version: fn(&DeltaProtocol) -> i32 = DeltaProtocol::min_reader_version;
    let min_writer_version: fn(&DeltaProtocol) -> i32 = DeltaProtocol::min_writer_version;
    let reader_features: for<'a> fn(&'a DeltaProtocol) -> &'a [String] =
        DeltaProtocol::reader_features;
    let writer_features: for<'a> fn(&'a DeltaProtocol) -> &'a [String] =
        DeltaProtocol::writer_features;
    let first_unsupported_reader_feature: for<'a> fn(&'a DeltaProtocol) -> Option<&'a str> =
        DeltaProtocol::first_unsupported_reader_feature;
    let _ = (
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

    let options = DeltaScanExecutionOptions::new()
        .with_parquet_backend(ParquetReaderBackend::DeltaKernel)
        .with_max_concurrent_file_reads_per_scan(Some(6))?
        .with_max_concurrent_file_reads_per_partition(3)?
        .with_output_buffer_batches_per_partition(1)?
        .with_prefetch_files_per_partition(2)
        .with_parquet_metadata_size_hint_bytes(Some(65_536))?
        .with_parquet_full_file_read_threshold_bytes(None)?;

    assert_eq!(options.parquet_backend(), ParquetReaderBackend::DeltaKernel);

    let error = DeltaScanExecutionOptions::new()
        .with_output_buffer_batches_per_partition(0)
        .expect_err("zero output capacity must fail");
    assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
    assert_eq!(error.code(), "invalid_configuration");
    assert!(error.source().is_none());

    Ok(())
}

#[test]
fn scan_partition_target_diagnostic_contract_is_public() -> Result<(), DeltaReaderError> {
    let _: Input = Default::default();
    let local: LocalEnvironment = collect_local_environment();
    let _: Input = local.policy_input;
    let _: Option<u64> = local.memory_total_bytes;
    let _: Option<u64> = local.memory_available_bytes;
    let _: Option<u64> = local.unix_soft_file_descriptor_limit;
    let _: UnixFileDescriptorLimitStatus = local.unix_soft_file_descriptor_limit_status;
    let _ = [
        UnixFileDescriptorLimitStatus::Unsupported,
        UnixFileDescriptorLimitStatus::Unknown,
        UnixFileDescriptorLimitStatus::Finite,
        UnixFileDescriptorLimitStatus::Unlimited,
    ];
    let input = Input {
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
    let output: Output = derive(input)?;

    assert_eq!(output.target_partitions, 4);
    assert_eq!(output.source, Source::AvailableParallelismFallback);
    let _ = [
        Source::ExplicitOverride,
        Source::AvailableParallelismFallback,
        Source::StaticFallback,
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
        DeltaPredicate::Constant(true),
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
        DeltaPredicate::Not(Box::new(DeltaPredicate::Constant(false))),
    ];
    assert_eq!(predicates, predicates.clone());
    assert!(format!("{predicates:?}").contains("Compare"));
}

#[test]
fn streaming_reader_contract_is_public() {
    fn assert_debug<T: std::fmt::Debug>() {}
    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_clone<T: Clone>() {}
    fn assert_batch_stream<T: Stream<Item = Result<RecordBatch, DeltaReaderError>>>() {}
    fn assert_future<T>(_: impl Future<Output = T>) {}
    fn table_version(table: &DeltaTable) -> u64 {
        table.version()
    }
    fn scan_partition_count(scan: &DeltaScan) -> usize {
        scan.partition_count()
    }

    assert_send_sync::<DeltaTable>();
    assert_clone::<DeltaTable>();
    assert_debug::<DeltaScanMetrics>();
    assert_send::<DeltaBatchStream>();
    assert_batch_stream::<DeltaBatchStream>();

    let builder = DeltaTableBuilder::new("file:///tmp/table")
        .with_storage_options(DeltaStorageOptions::new())
        .with_snapshot_selection(DeltaSnapshotSelection::Version(1))
        .with_execution_options(DeltaScanExecutionOptions::new());
    assert_future::<Result<DeltaTable, DeltaReaderError>>(builder.load_table());
    let eager_builder = DeltaTableBuilder::new("file:///tmp/table");
    assert_future::<Result<DeltaTable, DeltaReaderError>>(
        eager_builder.load_table_with_eager_scan_metadata(),
    );
    let snapshot_builder = DeltaTableBuilder::new("file:///tmp/table");
    assert_future::<Result<DeltaTableSnapshot, DeltaReaderError>>(snapshot_builder.load_snapshot());

    let snapshot_version: fn(&DeltaTableSnapshot) -> u64 = DeltaTableSnapshot::version;
    let snapshot_protocol: for<'a> fn(&'a DeltaTableSnapshot) -> &'a DeltaProtocol =
        DeltaTableSnapshot::protocol;
    let snapshot_table_url: for<'a> fn(&'a DeltaTableSnapshot) -> &'a str =
        DeltaTableSnapshot::table_url;
    let validate_snapshot_protocol: fn(&DeltaTableSnapshot) -> Result<(), DeltaReaderError> =
        DeltaTableSnapshot::validate_protocol;
    let into_table: fn(DeltaTableSnapshot) -> Result<DeltaTable, DeltaReaderError> =
        DeltaTableSnapshot::into_table;
    let _ = (
        snapshot_version,
        snapshot_protocol,
        snapshot_table_url,
        validate_snapshot_protocol,
        into_table,
    );

    let version: fn(&DeltaTable) -> u64 = DeltaTable::version;
    let schema: fn(&DeltaTable) -> SchemaRef = DeltaTable::schema;
    let protocol: for<'a> fn(&'a DeltaTable) -> &'a DeltaProtocol = DeltaTable::protocol;
    let table_url: for<'a> fn(&'a DeltaTable) -> &'a str = DeltaTable::table_url;
    let validate_protocol: fn(&DeltaTable) -> Result<(), DeltaReaderError> =
        DeltaTable::validate_protocol;
    let scan: for<'a> fn(&'a DeltaTable) -> DeltaScanBuilder<'a> = DeltaTable::scan;
    let _ = (
        version,
        table_version,
        schema,
        protocol,
        table_url,
        validate_protocol,
        scan,
    );

    fn configure_scan<'a>(
        builder: DeltaScanBuilder<'a>,
        predicate: DeltaPredicate,
        options: DeltaScanExecutionOptions,
    ) -> Result<DeltaScanBuilder<'a>, DeltaReaderError> {
        Ok(builder
            .with_projection(["id"])
            .with_predicate(predicate)
            .with_limit(1)
            .with_target_partitions(1)?
            .with_execution_options(options))
    }
    fn assert_scan_contract(builder: DeltaScanBuilder<'_>, scan: DeltaScan) {
        assert_future::<Result<DeltaScan, DeltaReaderError>>(builder.build());
        let _: DeltaBatchStream = scan.into_stream();
    }
    let _ = configure_scan;
    let _ = assert_scan_contract;

    let scan_schema: fn(&DeltaScan) -> SchemaRef = DeltaScan::schema;
    let partition_count: fn(&DeltaScan) -> usize = DeltaScan::partition_count;
    let _ = (scan_schema, partition_count, scan_partition_count);

    let stream_schema: fn(&DeltaBatchStream) -> SchemaRef = DeltaBatchStream::schema;
    let metrics: fn(&DeltaBatchStream) -> DeltaScanMetrics = DeltaBatchStream::metrics;
    let _ = (stream_schema, metrics);
}

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_metrics_contract_is_public() {
    use delta_arrow_reader::datafusion::{ScanMetrics, ScanMetricsSnapshot, collect_scan_metrics};

    fn assert_clone<T: Clone>() {}
    fn assert_snapshot_traits<T: std::fmt::Debug + Clone + PartialEq + Eq>() {}
    fn inspect(snapshot: ScanMetricsSnapshot) {
        let _: DeltaScanMetricsSnapshot = snapshot.reader_metrics;
        let _: bool = snapshot.uses_arrow_view_types;
        let _: Option<u64> = snapshot.configured_batch_size_rows;
        let _: u64 = snapshot.dynamic_partition_tasks_pruned;
        let _: u64 = snapshot.dynamic_partition_tasks_kept;
        let _: u64 = snapshot.dynamic_filters_received;
        let _: u64 = snapshot.dynamic_filters_accepted;
        let _: u64 = snapshot.dynamic_filters_rejected;
        let _: u64 = snapshot.dynamic_partition_filter_checks;
        let _: u64 = snapshot.dynamic_partition_tasks_kept_unusable_metadata;
        let _: u64 = snapshot.dynamic_partition_tasks_kept_unevaluable_filter;
    }

    assert_clone::<ScanMetrics>();
    assert_snapshot_traits::<ScanMetricsSnapshot>();
    let registration_name: for<'a> fn(&'a ScanMetrics) -> Option<&'a str> =
        ScanMetrics::registration_name;
    let snapshot: fn(&ScanMetrics) -> ScanMetricsSnapshot = ScanMetrics::snapshot;
    let collect: fn(&dyn datafusion::physical_plan::ExecutionPlan) -> Vec<ScanMetrics> =
        collect_scan_metrics;
    let _ = (registration_name, snapshot, collect, inspect);
}

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_provider_contract_is_public() {
    use delta_arrow_reader::{
        DeltaReaderError, DeltaTable,
        datafusion::{
            DeltaTableProvider, IntraFileRepartitioning, ScanOptions, TableRegistration,
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
    assert_result_traits::<TableRegistration>();
    fn inspect_registration(registration: TableRegistration) {
        let _: String = registration.name;
        let _: u64 = registration.snapshot_version;
    }
    let construct: fn(DeltaTable, ScanOptions) -> Result<DeltaTableProvider, DeltaReaderError> =
        DeltaTableProvider::try_new;
    fn register(
        context: &datafusion::execution::context::SessionContext,
        name: String,
        table: DeltaTable,
        options: ScanOptions,
    ) -> Result<TableRegistration, DeltaReaderError> {
        register_table(context, name, table, options)
    }
    let _ = (construct, register, inspect_registration);
}
