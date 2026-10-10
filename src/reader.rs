//! Public Delta-to-Arrow reader and optional DataFusion adapter.

pub(crate) mod backend;
#[cfg(feature = "datafusion")]
pub mod datafusion;
pub(crate) mod deletion_vector;
pub(crate) mod metrics;
mod options;
pub(crate) mod partition_target;
pub(crate) mod planning;
pub(crate) mod predicate;
#[allow(dead_code)]
pub(crate) mod scheduling;
pub(crate) mod transform;

#[doc(hidden)]
pub use metrics::ParquetRangePlanningDiagnosticSnapshot;
pub use metrics::{DeltaScanMetrics, DeltaScanMetricsSnapshot};
#[doc(hidden)]
pub use options::ParquetRangeReadPolicy;
pub use options::{
    DeltaScanExecutionOptions, DeltaSnapshotSelection, DeltaStorageOptions, ParquetReaderBackend,
};
pub use predicate::{DeltaComparison, DeltaPredicate, DeltaScalar};

use std::{
    fmt,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use arrow::{
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use futures_util::Stream;
use snafu::ResultExt;

use self::{
    backend::direct_parquet::ParquetRangeReadEstimator,
    planning::{
        DeltaScanPartitionTargetOptions, DeltaScanPlan, build_physical_row_predicate, plan_scan,
    },
    predicate::{column_data_type, evaluate_predicate, referenced_columns, validate_predicate},
    scheduling::{
        DeltaScanScheduler, FileAdmissionDecision, FileAdmissionPolicy, FileBatchStream,
        FileExecutor, OrderedPartitionStream,
    },
};

use crate::{
    DeltaProtocol, DeltaReaderError,
    delta::{
        kernel::{
            DeltaKernelEngineContext, DeltaKernelPredicate, kernel_pruning_predicate,
            kernel_row_predicate,
        },
        protocol::validate_protocol,
        snapshot::{
            ArrowTableSnapshot, KernelTableSnapshot, load_delta_table_snapshot,
            load_kernel_table_snapshot,
        },
    },
    error::{DataFileReadSnafu, InvalidConfigurationSnafu, ScanPlanningSnafu, SnapshotLoadSnafu},
};

const TRACING_TARGET: &str = "delta_arrow_reader";
const DELTA_LOG_SCAN_METADATA_SOURCE: &str = "delta_log";
const QUERY_PLANNING_CACHE_SCAN_METADATA_SOURCE: &str = "query_planning_cache";

/// Selects how much work table loading performs before returning.
///
/// The default prepares supported S3 tables for repeated queries. Explicit modes select
/// how much work to do before the first query.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WarmupMode {
    /// Uses network warmup for supported S3 stores when partial-page reads are enabled.
    ///
    /// Network sampling has a five-second limit after metadata loading. Other stores,
    /// the Delta Kernel backend, and explicit range policies perform no warmup.
    #[default]
    Automatic,
    /// Performs no warmup.
    None,
    /// Loads and retains the active-file metadata used during query planning.
    QueryPlanning,
    /// Retains query-planning metadata and samples remote data-file reads.
    ///
    /// The time limit applies to network sampling, after metadata loading. Sampling is
    /// best effort, uses at most three files, and schedules at most 24 MiB of payload.
    /// Only the Direct backend with an automatic range policy and a supported S3
    /// transport performs network sampling. Other settings keep metadata warmup only.
    Network {
        /// Maximum time spent sampling network reads. Zero skips sampling.
        max_duration: Duration,
    },
}

impl WarmupMode {
    fn for_table(
        self,
        context: &DeltaKernelEngineContext,
        options: DeltaScanExecutionOptions,
    ) -> Self {
        if self != Self::Automatic {
            return self;
        }
        if options.experimental_intra_page_reads()
            && options.parquet_backend() == ParquetReaderBackend::Direct
            && options.parquet_range_read_policy() == ParquetRangeReadPolicy::Automatic
            && context.supports_partial_reads()
        {
            Self::Network {
                max_duration: Duration::from_secs(5),
            }
        } else {
            Self::None
        }
    }
}

/// Configures and loads one immutable Delta table snapshot.
///
/// The asynchronous path uses the caller's Tokio runtime. Scans return a
/// pull-driven stream and do not materialize the whole table.
///
/// # Example
///
/// ```no_run
/// use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTableBuilder};
///
/// # async fn read_table() -> Result<(), Box<dyn std::error::Error>> {
/// let table = DeltaTableBuilder::new("/tmp/example-delta-table")
///     .load_table()
///     .await?;
/// let scan = table
///     .scan()
///     .with_projection(["id", "name"])
///     .with_predicate(DeltaPredicate::Compare {
///         column: "id".into(),
///         op: DeltaComparison::GtEq,
///         value: DeltaScalar::Int64(10),
///     })
///     .with_limit(100)
///     .build()
///     .await?;
/// let mut batches = scan.into_stream();
///
/// while let Some(batch) = batches.next_batch().await? {
///     println!("rows={}", batch.num_rows());
/// }
/// # Ok(())
/// # }
/// ```
#[must_use = "table builder settings do nothing unless the builder is loaded"]
pub struct DeltaTableBuilder {
    table_location: String,
    storage_options: DeltaStorageOptions,
    snapshot_selection: DeltaSnapshotSelection,
    execution_options: DeltaScanExecutionOptions,
    warmup: WarmupMode,
}

impl DeltaTableBuilder {
    /// Creates a builder for the latest snapshot with default execution settings.
    pub fn new(table_location: impl Into<String>) -> Self {
        Self {
            table_location: table_location.into(),
            storage_options: DeltaStorageOptions::new(),
            snapshot_selection: DeltaSnapshotSelection::Latest,
            execution_options: DeltaScanExecutionOptions::new(),
            warmup: WarmupMode::Automatic,
        }
    }

    /// Replaces the storage options forwarded during table loading.
    pub fn with_storage_options(mut self, storage_options: DeltaStorageOptions) -> Self {
        self.storage_options = storage_options;
        self
    }

    /// Selects the Delta snapshot to load.
    pub const fn with_snapshot_selection(
        mut self,
        snapshot_selection: DeltaSnapshotSelection,
    ) -> Self {
        self.snapshot_selection = snapshot_selection;
        self
    }

    /// Replaces the default execution settings used by scans of this table.
    pub const fn with_execution_options(
        mut self,
        execution_options: DeltaScanExecutionOptions,
    ) -> Self {
        self.execution_options = execution_options;
        self
    }

    /// Selects work to finish during [`Self::load_table`] for reuse by later queries.
    ///
    /// Defaults to [`WarmupMode::Automatic`]. [`Self::load_snapshot`] does not warm up
    /// a table and accepts only `Automatic` or [`WarmupMode::None`].
    pub const fn with_warmup(mut self, warmup: WarmupMode) -> Self {
        self.warmup = warmup;
        self
    }

    /// Loads the table through the caller-owned Tokio runtime.
    ///
    /// The default [`WarmupMode::Automatic`] prepares query-planning metadata and samples
    /// the network for supported S3 stores. Other stores remain lazy. Select
    /// [`WarmupMode::None`] to skip this preparation, including all network probes.
    ///
    /// With [`WarmupMode::None`], this method loads the snapshot and converts its schema. Protocol
    /// validation and scan metadata loading remain deferred until a scan is built. This allows an
    /// application to inspect the version, schema, and protocol of a table that the reader cannot
    /// scan.
    ///
    /// A warmup mode validates the protocol while loading because it builds reusable query-planning
    /// state. Query-planning warmup retains active-file metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the table location, storage, snapshot, or schema cannot be loaded. A
    /// warmup can also fail if the table protocol is unsupported or its reusable metadata cannot be
    /// built.
    pub async fn load_table(self) -> Result<DeltaTable, DeltaReaderError> {
        let snapshot = load_delta_table_snapshot(
            self.table_location,
            self.storage_options,
            self.snapshot_selection,
        )
        .await?;
        let warmup = self
            .warmup
            .for_table(snapshot.engine_context(), self.execution_options);
        let snapshot = if warmup == WarmupMode::None {
            snapshot
        } else {
            validate_protocol(snapshot.protocol())?;
            materialize_eager_scan_metadata(snapshot).await?
        };
        let mut table = DeltaTable::new(snapshot, self.execution_options);
        if let WarmupMode::Network { max_duration } = warmup
            && let Some(estimator) = backend::direct_parquet::warmup_network(
                table.snapshot.as_ref(),
                self.execution_options,
                max_duration,
            )
            .await
        {
            table.range_read_estimator = estimator;
        }
        Ok(table)
    }

    /// Loads a Delta Kernel snapshot without converting its logical Arrow schema.
    ///
    /// This method never performs warmup. The default [`WarmupMode::Automatic`] and
    /// explicit [`WarmupMode::None`] are accepted; requesting metadata or network
    /// warmup returns a configuration error.
    ///
    /// # Errors
    ///
    /// Returns an error if warmup was requested or if the table location, storage, or snapshot
    /// cannot be loaded.
    pub async fn load_snapshot(self) -> Result<DeltaTableSnapshot, DeltaReaderError> {
        if !matches!(self.warmup, WarmupMode::Automatic | WarmupMode::None) {
            return InvalidConfigurationSnafu {
                reason: "snapshot_load_does_not_support_table_warmup",
            }
            .fail();
        }
        let snapshot = load_kernel_table_snapshot(
            self.table_location,
            self.storage_options,
            self.snapshot_selection,
        )
        .await?;
        Ok(DeltaTableSnapshot::new(snapshot, self.execution_options))
    }
}

async fn materialize_eager_scan_metadata(
    snapshot: ArrowTableSnapshot,
) -> Result<ArrowTableSnapshot, DeltaReaderError> {
    tokio::task::spawn_blocking(move || snapshot.materialize_eager_scan_metadata())
        .await
        .boxed()
        .context(ScanPlanningSnafu {
            reason: "query_planning_warmup_task_failed",
        })?
}

impl fmt::Debug for DeltaTableBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaTableBuilder")
            .field("table_location", &"<redacted>")
            .field("storage_options", &"<redacted>")
            .field("snapshot_selection", &self.snapshot_selection)
            .field("execution_options", &self.execution_options)
            .field("warmup", &self.warmup)
            .finish()
    }
}

/// Loaded Delta snapshot metadata awaiting logical Arrow schema conversion.
pub struct DeltaTableSnapshot {
    snapshot: KernelTableSnapshot,
    execution_options: DeltaScanExecutionOptions,
}

impl DeltaTableSnapshot {
    fn new(snapshot: KernelTableSnapshot, execution_options: DeltaScanExecutionOptions) -> Self {
        Self {
            snapshot,
            execution_options,
        }
    }

    /// Returns the loaded Delta snapshot version.
    pub fn version(&self) -> u64 {
        self.snapshot.version()
    }

    /// Returns the loaded Delta protocol metadata.
    pub fn protocol(&self) -> &DeltaProtocol {
        self.snapshot.protocol()
    }

    /// Returns the normalized table URL.
    ///
    /// This value may contain sensitive caller input. Do not log or expose it.
    pub fn table_url(&self) -> &str {
        self.snapshot.table_url()
    }

    /// Validates the loaded snapshot against the supported reader protocol.
    pub fn validate_protocol(&self) -> Result<(), DeltaReaderError> {
        validate_protocol(self.protocol())
    }

    /// Converts the logical Arrow schema and finishes constructing the table.
    pub fn into_table(self) -> Result<DeltaTable, DeltaReaderError> {
        Ok(DeltaTable::new(
            self.snapshot.into_arrow_snapshot()?,
            self.execution_options,
        ))
    }
}

impl fmt::Debug for DeltaTableSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaTableSnapshot")
            .field("version", &self.version())
            .finish_non_exhaustive()
    }
}

/// One immutable loaded Delta table snapshot.
#[derive(Clone)]
pub struct DeltaTable {
    snapshot: Arc<ArrowTableSnapshot>,
    execution_options: DeltaScanExecutionOptions,
    range_read_estimator: Arc<ParquetRangeReadEstimator>,
}

impl DeltaTable {
    fn new(snapshot: ArrowTableSnapshot, execution_options: DeltaScanExecutionOptions) -> Self {
        Self {
            snapshot: Arc::new(snapshot),
            execution_options,
            range_read_estimator: Arc::default(),
        }
    }

    /// Returns the loaded Delta snapshot version.
    pub fn version(&self) -> u64 {
        self.snapshot.version()
    }

    /// Returns a shared handle to the logical Arrow schema.
    pub fn schema(&self) -> SchemaRef {
        self.snapshot.schema()
    }

    /// Returns a predicate column's logical Arrow type from the loaded schema.
    ///
    /// The column must be an unqualified, top-level logical name. This lookup
    /// uses the same column validation as scan predicates and performs no I/O.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, dotted, missing, or ambiguous column names.
    pub fn predicate_column_type(&self, column: &str) -> Result<DataType, DeltaReaderError> {
        column_data_type(self.schema().as_ref(), column).cloned()
    }

    /// Returns the loaded Delta protocol metadata.
    pub fn protocol(&self) -> &DeltaProtocol {
        self.snapshot.protocol()
    }

    /// Returns the normalized table URL.
    ///
    /// This value may contain sensitive caller input. Do not log or expose it.
    pub fn table_url(&self) -> &str {
        self.snapshot.table_url()
    }

    #[allow(dead_code)]
    pub(crate) fn partition_columns(&self) -> &[String] {
        self.snapshot.partition_columns()
    }

    #[allow(dead_code)]
    pub(crate) fn snapshot(&self) -> &ArrowTableSnapshot {
        self.snapshot.as_ref()
    }

    /// Validates the loaded snapshot against the supported reader protocol.
    pub fn validate_protocol(&self) -> Result<(), DeltaReaderError> {
        validate_protocol(self.protocol())
    }

    /// Loads the latest version from this table and returns it as a new immutable table.
    ///
    /// The current table and any scans built from it remain fixed at their original version. A
    /// query-planning metadata cache is refreshed for the new version and reused directly when the
    /// version has not changed.
    ///
    /// # Errors
    ///
    /// Returns an error if the newer snapshot, schema, or retained query-planning metadata cannot
    /// be loaded. No partially refreshed table is returned.
    pub async fn refresh(&self) -> Result<Self, DeltaReaderError> {
        let snapshot = Arc::clone(&self.snapshot);
        let snapshot = tokio::task::spawn_blocking(move || snapshot.refresh())
            .await
            .boxed()
            .context(SnapshotLoadSnafu {
                reason: "snapshot_refresh_task_failed",
            })
            .and_then(|result| result)?;
        Ok(Self {
            snapshot: Arc::new(snapshot),
            execution_options: self.execution_options,
            range_read_estimator: Arc::clone(&self.range_read_estimator),
        })
    }

    /// Starts configuring a new single-use scan.
    pub fn scan(&self) -> DeltaScanBuilder<'_> {
        DeltaScanBuilder {
            table: self,
            projection: None,
            predicate: None,
            limit: None,
            target_partitions: None,
            execution_options: self.execution_options,
        }
    }
}

impl fmt::Debug for DeltaTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaTable")
            .field("version", &self.version())
            .finish_non_exhaustive()
    }
}

/// Configures one single-use streaming Delta scan.
#[must_use = "scan builder settings do nothing unless the scan is built"]
pub struct DeltaScanBuilder<'table> {
    table: &'table DeltaTable,
    projection: Option<Vec<String>>,
    predicate: Option<DeltaPredicate>,
    limit: Option<usize>,
    target_partitions: Option<usize>,
    execution_options: DeltaScanExecutionOptions,
}

impl<'table> DeltaScanBuilder<'table> {
    /// Selects visible logical columns in caller order.
    pub fn with_projection(
        mut self,
        logical_columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.projection = Some(logical_columns.into_iter().map(Into::into).collect());
        self
    }

    /// Replaces the exact logical row predicate.
    pub fn with_predicate(mut self, predicate: DeltaPredicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Sets the maximum number of output rows.
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Overrides the number of planned scan partitions.
    pub fn with_target_partitions(
        mut self,
        target_partitions: usize,
    ) -> Result<Self, DeltaReaderError> {
        if target_partitions == 0 {
            return InvalidConfigurationSnafu {
                reason: "scan_partition_target_must_be_positive",
            }
            .fail();
        }
        self.target_partitions = Some(target_partitions);
        Ok(self)
    }

    /// Replaces the execution settings for this scan.
    pub const fn with_execution_options(
        mut self,
        execution_options: DeltaScanExecutionOptions,
    ) -> Self {
        self.execution_options = execution_options;
        self
    }

    /// Builds one immutable single-use scan plan without reading data files.
    pub async fn build(self) -> Result<DeltaScan, DeltaReaderError> {
        self.table.validate_protocol()?;
        if let Some(predicate) = self.predicate.as_ref() {
            validate_predicate(predicate, self.table.schema().as_ref())?;
        }

        let snapshot_version = self.table.version();
        let backend = self.execution_options.parquet_backend();
        let scan_metadata_source = if self.table.snapshot.eager_scan_metadata().is_some() {
            QUERY_PLANNING_CACHE_SCAN_METADATA_SOURCE
        } else {
            DELTA_LOG_SCAN_METADATA_SOURCE
        };
        trace_planning_started(snapshot_version, backend, scan_metadata_source);
        let snapshot = Arc::clone(&self.table.snapshot);
        let projection = self.projection;
        let predicate = self.predicate;
        let hidden_columns = predicate
            .as_ref()
            .map(referenced_columns)
            .unwrap_or_default();
        let row_predicate = predicate
            .as_ref()
            .filter(|_| backend == ParquetReaderBackend::Direct)
            .and_then(|predicate| kernel_row_predicate(predicate, snapshot.partition_columns()));
        let kernel_predicate = predicate.as_ref().and_then(kernel_pruning_predicate);
        let include_stats = kernel_predicate.is_some();
        let execution_options = self.execution_options;
        let target_partitions = self.target_partitions;
        let result = tokio::task::spawn_blocking(move || {
            // Reuse the already-mapped predicate for ordinary data-only filters.
            let same_predicate = row_predicate.is_some() && row_predicate == kernel_predicate;
            let plan = plan_scan(
                snapshot.as_ref(),
                projection.as_deref(),
                &hidden_columns,
                kernel_predicate,
                include_stats,
                execution_options,
                DeltaScanPartitionTargetOptions {
                    explicit_target_partitions: target_partitions,
                    datafusion_target_partitions: None,
                },
            )?;
            let physical_row_predicate = if plan.partitions.is_empty() {
                None
            } else if same_predicate {
                plan.physical_predicate.clone()
            } else {
                build_physical_row_predicate(
                    snapshot.as_ref(),
                    projection.as_deref(),
                    &hidden_columns,
                    row_predicate,
                )?
            };
            Ok((plan, physical_row_predicate))
        })
        .await
        .boxed()
        .context(ScanPlanningSnafu {
            reason: "scan_planning_task_failed",
        })
        .and_then(|result| result);

        match result {
            Ok((plan, physical_row_predicate)) => {
                trace_planning_completed(
                    snapshot_version,
                    backend,
                    plan.partitions.len(),
                    scan_metadata_source,
                );
                Ok(DeltaScan {
                    plan: Arc::new(plan),
                    predicate,
                    limit: self.limit,
                    physical_row_predicate,
                    range_read_estimator: Arc::clone(&self.table.range_read_estimator),
                })
            }
            Err(error) => {
                trace_planning_failed(snapshot_version, backend, scan_metadata_source, &error);
                Err(error)
            }
        }
    }
}

/// One immutable, single-use streaming Delta scan plan.
///
/// A scan cannot be cloned or converted into a stream twice.
///
/// ```compile_fail
/// use delta_arrow_reader::DeltaScan;
///
/// fn stream_twice(scan: DeltaScan) {
///     let _ = scan.into_stream();
///     let _ = scan.into_stream();
/// }
/// ```
///
/// ```compile_fail
/// use delta_arrow_reader::DeltaScan;
///
/// fn clone_scan(scan: DeltaScan) {
///     let _ = scan.clone();
/// }
/// ```
#[must_use = "scans do nothing unless converted into a stream"]
pub struct DeltaScan {
    plan: Arc<DeltaScanPlan>,
    predicate: Option<DeltaPredicate>,
    limit: Option<usize>,
    physical_row_predicate: Option<DeltaKernelPredicate>,
    range_read_estimator: Arc<ParquetRangeReadEstimator>,
}

impl DeltaScan {
    /// Returns a shared handle to the visible logical output schema.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.plan.projected_schema)
    }

    /// Returns the number of planned execution partitions.
    pub fn partition_count(&self) -> usize {
        self.plan.partitions.len()
    }

    /// Converts the scan into a pull-driven Arrow batch stream.
    ///
    /// Data-file reads begin only when the stream is polled.
    pub fn into_stream(self) -> DeltaBatchStream {
        let metrics = self.plan.metrics.clone();
        let schema = Arc::clone(&self.plan.projected_schema);
        let partition_count = self.plan.partitions.len();
        let snapshot_version = self.plan.snapshot_version;
        let backend = self.plan.execution_options.parquet_backend();
        let projection = (self.plan.logical_schema.as_ref() != schema.as_ref())
            .then(|| (0..schema.fields().len()).collect::<Vec<_>>());
        let partitions = if self.limit == Some(0) {
            OrderedPartitionStream::default()
        } else {
            let scheduler = DeltaScanScheduler::new(Arc::clone(&self.plan));
            let admission: FileAdmissionPolicy<_> = Arc::new(|_| Ok(FileAdmissionDecision::Admit));
            let executor = match backend {
                ParquetReaderBackend::Direct => {
                    backend::direct_parquet::direct_parquet_file_executor(
                        &self.plan,
                        None,
                        self.physical_row_predicate,
                        self.range_read_estimator,
                        None,
                    )
                }
                ParquetReaderBackend::DeltaKernel => delta_kernel_executor(&self.plan),
            };
            scheduler.partition_streams(admission, executor)
        };

        DeltaBatchStream {
            schema,
            metrics,
            partitions,
            predicate: self.predicate,
            projection,
            remaining: self.limit,
            snapshot_version,
            backend,
            partition_count,
            started: false,
            done: false,
        }
    }
}

/// Pull-driven stream of finalized logical Arrow batches from one Delta scan.
///
/// The stream has no inherent whole-result collection method. Callers that
/// intentionally materialize a result can import [`TryStreamExt`](crate::TryStreamExt).
///
/// ```compile_fail
/// use delta_arrow_reader::DeltaBatchStream;
///
/// fn collect_without_opt_in(stream: DeltaBatchStream) {
///     let _ = stream.collect();
/// }
/// ```
#[must_use = "streams do nothing unless polled"]
pub struct DeltaBatchStream {
    schema: SchemaRef,
    metrics: DeltaScanMetrics,
    partitions: OrderedPartitionStream,
    predicate: Option<DeltaPredicate>,
    projection: Option<Vec<usize>>,
    remaining: Option<usize>,
    snapshot_version: u64,
    backend: ParquetReaderBackend,
    partition_count: usize,
    started: bool,
    done: bool,
}

impl DeltaBatchStream {
    /// Reads the next batch, returning `None` when the stream is exhausted.
    ///
    /// # Errors
    ///
    /// Propagates errors from reading or processing a batch. The stream ends after an error.
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>, DeltaReaderError> {
        futures_util::TryStreamExt::try_next(self).await
    }

    /// Returns a shared handle to the visible logical output schema.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Returns a lightweight shared handle to live scan metrics.
    pub fn metrics(&self) -> DeltaScanMetrics {
        self.metrics.clone()
    }

    fn start(&mut self) {
        if self.started {
            return;
        }
        self.started = true;
        trace_execution_started(self.snapshot_version, self.backend, self.partition_count);
    }

    fn complete(&mut self) {
        if self.done {
            return;
        }
        self.partitions.clear();
        self.done = true;
        trace_execution_completed(self.snapshot_version, self.backend, self.partition_count);
    }

    fn fail(&mut self, error: &DeltaReaderError) {
        self.partitions.clear();
        self.done = true;
        trace_execution_failed(
            self.snapshot_version,
            self.backend,
            self.partition_count,
            error,
        );
    }

    fn finalize_batch(&self, mut batch: RecordBatch) -> Result<RecordBatch, DeltaReaderError> {
        if let Some(predicate) = self.predicate.as_ref() {
            batch = evaluate_predicate(&batch, predicate)?;
        }
        if let Some(projection) = self.projection.as_ref() {
            batch = batch
                .project(projection)
                .boxed()
                .context(DataFileReadSnafu {
                    reason: "direct_projection_failed",
                })?;
        }
        Ok(batch)
    }
}

impl Stream for DeltaBatchStream {
    type Item = Result<RecordBatch, DeltaReaderError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        this.start();

        match Pin::new(&mut this.partitions).poll_next(context) {
            Poll::Ready(Some(Ok(batch))) => {
                let mut batch = match this.finalize_batch(batch) {
                    Ok(batch) => batch,
                    Err(error) => {
                        this.fail(&error);
                        return Poll::Ready(Some(Err(error)));
                    }
                };
                if let Some(remaining) = this.remaining.as_mut() {
                    if batch.num_rows() >= *remaining {
                        batch = batch.slice(0, *remaining);
                        *remaining = 0;
                        this.complete();
                    } else {
                        *remaining -= batch.num_rows();
                    }
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.fail(&error);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.complete();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for DeltaBatchStream {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        self.partitions.clear();
        self.done = true;
        trace_execution_dropped(self.snapshot_version, self.backend, self.partition_count);
    }
}

pub(crate) fn delta_kernel_executor(
    plan: &Arc<DeltaScanPlan>,
) -> FileExecutor<planning::DeltaScanFileTask, FileBatchStream> {
    backend::kernel_reader::delta_kernel_file_executor(plan)
}

fn trace_planning_started(
    snapshot_version: u64,
    backend: ParquetReaderBackend,
    scan_metadata_source: &'static str,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_planning.started",
        snapshot_version,
        backend = ?backend,
        partition_count = tracing::field::Empty,
        scan_metadata_source,
        outcome = "started"
    );
}

fn trace_planning_completed(
    snapshot_version: u64,
    backend: ParquetReaderBackend,
    partition_count: usize,
    scan_metadata_source: &'static str,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_planning.completed",
        snapshot_version,
        backend = ?backend,
        partition_count,
        scan_metadata_source,
        outcome = "completed"
    );
}

fn trace_planning_failed(
    snapshot_version: u64,
    backend: ParquetReaderBackend,
    scan_metadata_source: &'static str,
    error: &DeltaReaderError,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_planning.failed",
        snapshot_version,
        backend = ?backend,
        partition_count = tracing::field::Empty,
        scan_metadata_source,
        outcome = "failed",
        error_code = error.code(),
        error_phase = error.phase().as_str()
    );
}

fn trace_execution_started(
    snapshot_version: u64,
    backend: ParquetReaderBackend,
    partition_count: usize,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_execution.started",
        snapshot_version,
        backend = ?backend,
        partition_count,
        outcome = "started"
    );
}

fn trace_execution_completed(
    snapshot_version: u64,
    backend: ParquetReaderBackend,
    partition_count: usize,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_execution.completed",
        snapshot_version,
        backend = ?backend,
        partition_count,
        outcome = "completed"
    );
}

fn trace_execution_failed(
    snapshot_version: u64,
    backend: ParquetReaderBackend,
    partition_count: usize,
    error: &DeltaReaderError,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_execution.failed",
        snapshot_version,
        backend = ?backend,
        partition_count,
        outcome = "failed",
        error_code = error.code(),
        error_phase = error.phase().as_str()
    );
}

fn trace_execution_dropped(
    snapshot_version: u64,
    backend: ParquetReaderBackend,
    partition_count: usize,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event = "scan_execution.dropped",
        snapshot_version,
        backend = ?backend,
        partition_count,
        outcome = "dropped"
    );
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        fmt, fs,
        future::pending,
        path::{Path, PathBuf},
        sync::{Arc, Mutex, Once},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use futures_util::{FutureExt, StreamExt, stream};
    use tokio::{sync::Notify, time::timeout};
    use tracing::{
        Event, Level, Metadata, Subscriber,
        field::{Field as TracingField, Visit},
        span::{Attributes, Id, Record},
        subscriber::{Interest, with_default},
    };

    use super::{
        DeltaBatchStream, DeltaTable, trace_execution_completed, trace_execution_dropped,
        trace_execution_failed, trace_execution_started, trace_planning_completed,
        trace_planning_failed, trace_planning_started,
    };
    use crate::{
        DeltaScanExecutionOptions, DeltaScanMetrics, DeltaSnapshotSelection, DeltaStorageOptions,
        ParquetReaderBackend,
        delta::snapshot::load_delta_table_snapshot_blocking,
        error::InvalidConfigurationSnafu,
        reader::{
            metrics::DeltaScanMetricsConfig,
            scheduling::{
                FileAdmissionDecision, FileAdmissionPolicy, FileBatchStream, FileExecutor,
                FileReadPermit, OrderedPartitionStream, PartitionStream, ScanCancellation,
                ScanReadLimiter,
            },
        },
    };

    static TRACING_TEST_LOCK: Mutex<()> = Mutex::new(());
    static TRACING_TEST_GLOBAL_SUBSCRIBER: Once = Once::new();

    #[tokio::test]
    async fn automatic_warmup_respects_storage_and_explicit_options()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::{
            DeltaKernelEngineContext, DeltaStorageOptions, DeltaTableBuilder,
            ParquetRangeReadPolicy, WarmupMode,
        };

        let options = DeltaScanExecutionOptions::default();
        let automatic = DeltaTableBuilder::new("s3://bucket/table").warmup;
        assert_eq!(automatic, WarmupMode::default());
        let network = WarmupMode::Network {
            max_duration: Duration::from_secs(5),
        };
        for (location, extra, supported) in [
            ("s3://bucket/table", None, true),
            ("s3a://bucket/table", None, true),
            ("s3://bucket/table", Some(("timeout", "17s")), false),
            ("https://example.com/table/", None, false),
        ] {
            let mut storage = DeltaStorageOptions::from([
                ("aws_region".into(), "us-west-2".into()),
                ("aws_skip_signature".into(), "true".into()),
            ]);
            if let Some((key, value)) = extra {
                storage.insert(key.into(), value.into());
            }
            let context = DeltaKernelEngineContext::try_new(url::Url::parse(location)?, &storage)?;
            assert_eq!(
                automatic.for_table(&context, options),
                if supported { network } else { WarmupMode::None },
                "{location}, {extra:?}",
            );
            for disabled in [
                options.with_experimental_intra_page_reads(false),
                options.with_parquet_backend(ParquetReaderBackend::DeltaKernel),
                options.with_parquet_range_read_policy(ParquetRangeReadPolicy::ExactRanges),
            ] {
                assert_eq!(automatic.for_table(&context, disabled), WarmupMode::None);
            }
            for explicit in [WarmupMode::None, WarmupMode::QueryPlanning, network] {
                assert_eq!(explicit.for_table(&context, options), explicit);
                assert_eq!(
                    explicit.for_table(&context, options.with_experimental_intra_page_reads(false)),
                    explicit
                );
            }
        }
        Ok(())
    }

    #[derive(Clone, Default)]
    struct EventFields(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

    impl Subscriber for EventFields {
        fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
            if metadata.target() == "delta_arrow_reader" && *metadata.level() == Level::DEBUG {
                Interest::always()
            } else {
                Interest::sometimes()
            }
        }

        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.target() == "delta_arrow_reader" && *metadata.level() == Level::DEBUG
        }

        fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, event: &Event<'_>) {
            let metadata = event.metadata();
            assert_eq!(metadata.target(), "delta_arrow_reader");
            let mut fields = metadata
                .fields()
                .iter()
                .map(|field| (field.name().to_owned(), "<empty>".to_owned()))
                .collect();
            event.record(&mut FieldVisitor(&mut fields));
            self.0.lock().expect("event lock").push(fields);
        }

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    struct FieldVisitor<'fields>(&'fields mut BTreeMap<String, String>);

    impl Visit for FieldVisitor<'_> {
        fn record_debug(&mut self, field: &TracingField, value: &dyn fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &TracingField, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }

        fn record_u64(&mut self, field: &TracingField, value: u64) {
            self.0.insert(field.name().to_owned(), value.to_string());
        }

        fn record_i64(&mut self, field: &TracingField, value: i64) {
            self.0.insert(field.name().to_owned(), value.to_string());
        }

        fn record_u128(&mut self, field: &TracingField, value: u128) {
            self.0.insert(field.name().to_owned(), value.to_string());
        }
    }

    struct DeltaLogTable(PathBuf);

    impl DeltaLogTable {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let path = Path::new("target")
                .join("delta-arrow-reader-tracing-tests")
                .join(format!("{}-{name}-{nanos}", std::process::id()));
            fs::create_dir_all(path.join("_delta_log"))?;
            fs::write(
                path.join("_delta_log/00000000000000000000.json"),
                r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"tracing-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}
{"add":{"path":"secret-planning-object.parquet","partitionValues":{},"size":10,"modificationTime":1587968586000,"dataChange":true}}
"#,
            )?;
            Ok(Self(path))
        }
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn capture_events<T>(run: impl FnOnce() -> T) -> (T, Vec<BTreeMap<String, String>>) {
        let _lock = TRACING_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let events = Arc::new(Mutex::new(Vec::new()));
        let subscriber = EventFields(Arc::clone(&events));
        TRACING_TEST_GLOBAL_SUBSCRIBER.call_once(|| {
            let _ = tracing::subscriber::set_global_default(EventFields::default());
        });
        let result = with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            run()
        });
        tracing::callsite::rebuild_interest_cache();
        let captured = events
            .lock()
            .map(|events| events.clone())
            .unwrap_or_default();
        (result, captured)
    }

    struct ControlledMerge {
        stream: DeltaBatchStream,
        limiter: Arc<ScanReadLimiter>,
        cancellation: ScanCancellation,
        metrics: DeltaScanMetrics,
        first_partition_gate: Arc<Notify>,
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn batch(id: i32) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(vec![id]))])
            .expect("valid test batch")
    }

    fn batch_id(batch: &RecordBatch) -> i32 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 id")
            .value(0)
    }

    fn execution_options() -> Result<DeltaScanExecutionOptions, crate::DeltaReaderError> {
        DeltaScanExecutionOptions::new()
            .with_prefetch_files_per_partition(0)
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(2))?
            .with_output_buffer_batches_per_partition(1)
    }

    fn metrics() -> DeltaScanMetrics {
        DeltaScanMetrics::new(DeltaScanMetricsConfig {
            snapshot_version: 7,
            parquet_backend: ParquetReaderBackend::Direct,
            scan_partitions_planned: 2,
            files_planned: 2,
            add_actions_excluded_during_planning: Some(0),
            estimated_input_rows: Some(4),
            estimated_input_bytes: Some(4),
        })
    }

    fn file_stream(permit: FileReadPermit, batches: Vec<RecordBatch>) -> FileBatchStream {
        Box::pin(stream::unfold(
            (VecDeque::from(batches), permit),
            |(mut batches, permit)| async move {
                batches
                    .pop_front()
                    .map(|batch| (Ok(batch), (batches, permit)))
            },
        ))
    }

    fn gated_file_stream(
        permit: FileReadPermit,
        batches: Vec<RecordBatch>,
        gate: Arc<Notify>,
    ) -> FileBatchStream {
        Box::pin(stream::unfold(
            (false, VecDeque::from(batches), permit, gate),
            |(wait, mut batches, permit, gate)| async move {
                let batch = batches.pop_front()?;
                if wait {
                    gate.notified().await;
                }
                Some((Ok(batch), (true, batches, permit, gate)))
            },
        ))
    }

    fn direct_stream(
        partitions: VecDeque<PartitionStream>,
        metrics: DeltaScanMetrics,
        scan_capacity: usize,
    ) -> DeltaBatchStream {
        DeltaBatchStream {
            schema: schema(),
            metrics,
            partitions: OrderedPartitionStream::new(partitions, scan_capacity),
            predicate: None,
            projection: None,
            remaining: None,
            snapshot_version: 7,
            backend: ParquetReaderBackend::Direct,
            partition_count: 2,
            started: false,
            done: false,
        }
    }

    fn controlled_merge() -> Result<ControlledMerge, Box<dyn std::error::Error>> {
        let options = execution_options()?;
        let limiter = ScanReadLimiter::new(options, 2, 2);
        let cancellation = ScanCancellation::new();
        let metrics = metrics();
        let first_partition_gate = Arc::new(Notify::new());
        let executor: FileExecutor<i32, FileBatchStream> = {
            let gate = Arc::clone(&first_partition_gate);
            Arc::new(move |task, permit, _| {
                let gate = Arc::clone(&gate);
                async move {
                    let batches = vec![batch(task), batch(task * 2)];
                    Ok(if task == 1 {
                        gated_file_stream(permit, batches, gate)
                    } else {
                        file_stream(permit, batches)
                    })
                }
                .boxed()
            })
        };
        let admission: FileAdmissionPolicy<i32> =
            Arc::new(|_: &i32| Ok(FileAdmissionDecision::Admit));
        let first = PartitionStream::new(
            vec![1],
            limiter.partition(0)?,
            options,
            admission.clone(),
            Arc::clone(&executor),
            metrics.clone(),
            cancellation.clone(),
        );
        let second = PartitionStream::new(
            vec![10],
            limiter.partition(1)?,
            options,
            admission,
            executor,
            metrics.clone(),
            cancellation.clone(),
        );

        Ok(ControlledMerge {
            stream: direct_stream(VecDeque::from([first, second]), metrics.clone(), 2),
            limiter,
            cancellation,
            metrics,
            first_partition_gate,
        })
    }

    async fn wait_for_batches(metrics: &DeltaScanMetrics, expected: u64) {
        timeout(Duration::from_secs(5), async {
            while metrics.snapshot().scheduler_batches_emitted < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("batch production reached expected bound");
    }

    #[test]
    fn lifecycle_tracing_has_only_bounded_fields() {
        let error = InvalidConfigurationSnafu { reason: "test" }.build();

        let (_, events) = capture_events(|| {
            trace_planning_started(7, ParquetReaderBackend::Direct, "query_planning_cache");
            trace_planning_completed(7, ParquetReaderBackend::Direct, 2, "query_planning_cache");
            trace_planning_failed(
                7,
                ParquetReaderBackend::Direct,
                "query_planning_cache",
                &error,
            );
            trace_execution_started(7, ParquetReaderBackend::Direct, 2);
            trace_execution_completed(7, ParquetReaderBackend::Direct, 2);
            trace_execution_failed(7, ParquetReaderBackend::Direct, 2, &error);
            trace_execution_dropped(7, ParquetReaderBackend::Direct, 2);
        });

        assert_eq!(events.len(), 7);
        let allowed = [
            "backend",
            "error_phase",
            "error_code",
            "event",
            "outcome",
            "partition_count",
            "scan_metadata_source",
            "snapshot_version",
        ];
        for fields in events.iter() {
            assert!(fields.keys().all(|field| allowed.contains(&field.as_str())));
            assert!(fields.contains_key("event"));
            assert!(fields.contains_key("snapshot_version"));
            assert!(fields.contains_key("backend"));
            assert!(fields.contains_key("partition_count"));
            assert!(fields.contains_key("outcome"));
            if fields
                .get("event")
                .is_some_and(|event| event.starts_with("scan_planning."))
            {
                assert_eq!(
                    fields.get("scan_metadata_source").map(String::as_str),
                    Some("query_planning_cache")
                );
            }
        }
    }

    #[test]
    fn planning_tracing_reports_the_table_metadata_source_on_success_and_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        const OBJECT_KEY: &str = "secret-planning-object.parquet";
        const STORAGE_VALUE: &str = "secret-planning-storage-value";
        let fixture = DeltaLogTable::new("planning-source")?;
        let mut storage_options = DeltaStorageOptions::new();
        storage_options.insert("secret-option".to_owned(), STORAGE_VALUE.to_owned());
        let snapshot = load_delta_table_snapshot_blocking(
            &fixture.0.to_string_lossy(),
            &storage_options,
            DeltaSnapshotSelection::Latest,
        )?;
        let eager_snapshot = snapshot.clone().materialize_eager_scan_metadata()?;
        let lazy = DeltaTable::new(snapshot, DeltaScanExecutionOptions::new());
        let eager = DeltaTable::new(eager_snapshot, DeltaScanExecutionOptions::new());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let (eager_result, eager_events) =
            capture_events(|| runtime.block_on(eager.scan().build()));
        let _ = eager_result?;
        assert_eq!(eager_events.len(), 2);
        assert_eq!(
            eager_events[0].get("event").map(String::as_str),
            Some("scan_planning.started")
        );
        assert_eq!(
            eager_events[1].get("event").map(String::as_str),
            Some("scan_planning.completed")
        );
        assert!(eager_events.iter().all(|event| {
            event.get("scan_metadata_source").map(String::as_str) == Some("query_planning_cache")
        }));

        let (failed_result, failed_events) =
            capture_events(|| runtime.block_on(eager.scan().with_projection(["missing"]).build()));
        assert!(failed_result.is_err());
        assert_eq!(failed_events.len(), 2);
        assert_eq!(
            failed_events[0].get("event").map(String::as_str),
            Some("scan_planning.started")
        );
        assert_eq!(
            failed_events[1].get("event").map(String::as_str),
            Some("scan_planning.failed")
        );
        assert!(failed_events.iter().all(|event| {
            event.get("scan_metadata_source").map(String::as_str) == Some("query_planning_cache")
        }));

        let (lazy_result, lazy_events) = capture_events(|| runtime.block_on(lazy.scan().build()));
        let _ = lazy_result?;
        assert_eq!(lazy_events.len(), 2);
        assert!(lazy_events.iter().all(|event| {
            event.get("scan_metadata_source").map(String::as_str) == Some("delta_log")
        }));

        let captured = format!("{eager_events:?}{failed_events:?}{lazy_events:?}");
        assert!(!captured.contains(&fixture.0.to_string_lossy().into_owned()));
        assert!(!captured.contains(OBJECT_KEY));
        assert!(!captured.contains(STORAGE_VALUE));
        Ok(())
    }

    #[tokio::test]
    async fn merged_stream_stops_without_admitting_waiting_partitions()
    -> Result<(), Box<dyn std::error::Error>> {
        for stop in ["limit", "drop", "error"] {
            let options = execution_options()?.with_max_concurrent_file_reads_per_scan(Some(1))?;
            let limiter = ScanReadLimiter::new(options, 3, 3);
            let cancellation = ScanCancellation::new();
            let metrics = metrics();
            let executor: FileExecutor<i32, FileBatchStream> = Arc::new(move |task, permit, _| {
                async move {
                    Ok(if stop == "error" {
                        Box::pin(stream::once(async move {
                            let _permit = permit;
                            Err(InvalidConfigurationSnafu {
                                reason: "controlled_partition_failure",
                            }
                            .build())
                        })) as FileBatchStream
                    } else {
                        gated_file_stream(permit, vec![batch(task); 3], Arc::new(Notify::new()))
                    })
                }
                .boxed()
            });
            let partitions = (0..3)
                .map(|index| {
                    Ok(PartitionStream::new(
                        vec![index as i32; 2],
                        limiter.partition(index)?,
                        options,
                        Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
                        Arc::clone(&executor),
                        metrics.clone(),
                        cancellation.clone(),
                    ))
                })
                .collect::<Result<VecDeque<_>, crate::DeltaReaderError>>()?;
            let mut stream = direct_stream(partitions, metrics.clone(), 1);
            if stop == "limit" {
                stream.remaining = Some(1);
            }
            let first = timeout(Duration::from_secs(5), stream.next())
                .await?
                .ok_or("missing result")?;
            if stop == "error" {
                assert_eq!(
                    first.expect_err("controlled failure").code(),
                    "invalid_configuration"
                );
            } else {
                assert_eq!(batch_id(&first?), 0);
            }
            if stop != "drop" {
                assert!(stream.next().await.is_none());
            }
            drop(stream);
            timeout(Duration::from_secs(5), async {
                while limiter.active_file_reads() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
            assert!(cancellation.is_cancelled());
            assert_eq!(metrics.snapshot().file_tasks_started, 1);
            assert_eq!(metrics.snapshot().scan_partitions_started, 1);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn merged_stream_progresses_with_small_scan_read_limits()
    -> Result<(), Box<dyn std::error::Error>> {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            for cap in [1, 2, 4, 8] {
                for prefetch in [0, 2, usize::MAX] {
                    let options = DeltaScanExecutionOptions::new()
                        .with_parquet_backend(backend)
                        .with_max_concurrent_file_reads_per_scan(Some(cap))?
                        .with_prefetch_files_per_partition(prefetch);
                    let limiter = ScanReadLimiter::new(options, 5, 5);
                    let cancellation = ScanCancellation::new();
                    let metrics = metrics();
                    let executor: FileExecutor<i32, FileBatchStream> =
                        Arc::new(|task, permit, _| {
                            async move {
                                // Multiple batches fill later partitions' output queues while
                                // the front partition still needs permits for subsequent files.
                                Ok(file_stream(permit, vec![batch(task); 3]))
                            }
                            .boxed()
                        });
                    let partitions = (0..5)
                        .map(|partition| {
                            Ok(PartitionStream::new(
                                (partition * 4..partition * 4 + 4)
                                    .map(|id| id as i32)
                                    .collect(),
                                limiter.partition(partition)?,
                                options,
                                Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
                                Arc::clone(&executor),
                                metrics.clone(),
                                cancellation.clone(),
                            ))
                        })
                        .collect::<Result<VecDeque<_>, crate::DeltaReaderError>>()?;
                    let mut stream = direct_stream(partitions, metrics.clone(), cap);
                    let ids = timeout(Duration::from_secs(2), async {
                        let mut ids = Vec::new();
                        while let Some(batch) = stream.next().await {
                            ids.push(batch_id(&batch?));
                            assert!(limiter.active_file_reads() <= cap);
                            tokio::task::yield_now().await;
                        }
                        Ok::<_, crate::DeltaReaderError>(ids)
                    })
                    .await
                    .unwrap_or_else(|_| {
                        panic!("stalled: {backend:?}, cap={cap}, prefetch={prefetch}")
                    })?;
                    assert_eq!(ids, (0..20).flat_map(|id| [id; 3]).collect::<Vec<_>>());
                    assert_eq!(limiter.active_file_reads(), 0);
                    assert_eq!(metrics.snapshot().file_tasks_completed, 20);
                    assert_eq!(metrics.snapshot().scan_partitions_completed, 5);
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn merged_stream_is_ordered_and_bounds_later_partition_queues()
    -> Result<(), Box<dyn std::error::Error>> {
        let ControlledMerge {
            mut stream,
            limiter,
            metrics,
            first_partition_gate,
            ..
        } = controlled_merge()?;

        let first = stream.next().await.ok_or("first batch missing")??;
        assert_eq!(batch_id(&first), 1);
        wait_for_batches(&metrics, 2).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        assert_eq!(metrics.snapshot().scheduler_batches_emitted, 2);
        assert_eq!(limiter.active_file_reads(), 2);

        // Cancelling a pending read must leave the next batch available.
        assert!(stream.next_batch().now_or_never().is_none());
        first_partition_gate.notify_one();
        let mut ids = vec![batch_id(
            &stream.next_batch().await?.ok_or("second batch missing")?,
        )];
        while let Some(batch) = stream.next_batch().await? {
            ids.push(batch_id(&batch));
        }
        assert_eq!(ids, [2, 10, 20]);
        assert_eq!(metrics.snapshot().scheduler_batches_emitted, 4);
        assert_eq!(metrics.snapshot().scan_partitions_completed, 2);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn merged_stream_drop_cancels_blocked_partitions_and_releases_permits()
    -> Result<(), Box<dyn std::error::Error>> {
        let ControlledMerge {
            mut stream,
            limiter,
            cancellation,
            metrics,
            ..
        } = controlled_merge()?;

        let first = stream.next().await.ok_or("first batch missing")??;
        assert_eq!(batch_id(&first), 1);
        wait_for_batches(&metrics, 2).await;
        assert_eq!(limiter.active_file_reads(), 2);
        drop(stream);

        assert!(cancellation.is_cancelled());
        timeout(Duration::from_secs(5), async {
            while limiter.active_file_reads() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(metrics.snapshot().scheduler_batches_emitted, 2);
        assert_eq!(metrics.snapshot().scan_partitions_completed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn merged_stream_forwards_one_concurrent_error_and_releases_permits()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = execution_options()?;
        let limiter = ScanReadLimiter::new(options, 3, 3);
        let cancellation = ScanCancellation::new();
        let metrics = metrics();
        let executor: FileExecutor<i32, FileBatchStream> = Arc::new(|task, permit, _| {
            async move {
                Ok(if task == 1 {
                    Box::pin(stream::once(async move {
                        let _permit = permit;
                        pending::<Result<RecordBatch, crate::DeltaReaderError>>().await
                    })) as FileBatchStream
                } else {
                    Box::pin(stream::once(async move {
                        let _permit = permit;
                        Err(InvalidConfigurationSnafu {
                            reason: "controlled_partition_failure",
                        }
                        .build())
                    })) as FileBatchStream
                })
            }
            .boxed()
        });
        let admission = Arc::new(|_: &i32| Ok(FileAdmissionDecision::Admit));
        let first = PartitionStream::new(
            vec![1],
            limiter.partition(0)?,
            options,
            admission.clone(),
            Arc::clone(&executor),
            metrics.clone(),
            cancellation.clone(),
        );
        let second = PartitionStream::new(
            vec![2],
            limiter.partition(1)?,
            options,
            admission.clone(),
            Arc::clone(&executor),
            metrics.clone(),
            cancellation.clone(),
        );
        let third = PartitionStream::new(
            vec![3],
            limiter.partition(2)?,
            options,
            admission,
            executor,
            metrics.clone(),
            cancellation.clone(),
        );
        let mut stream = direct_stream(VecDeque::from([first, second, third]), metrics.clone(), 2);

        let error = timeout(Duration::from_secs(5), stream.next())
            .await?
            .ok_or("error item missing")?
            .expect_err("controlled partition must fail");
        assert_eq!(error.code(), "invalid_configuration");
        assert!(stream.next().await.is_none());
        assert!(cancellation.is_cancelled());
        assert_eq!(metrics.snapshot().file_tasks_started, 2);
        timeout(Duration::from_secs(5), async {
            while limiter.active_file_reads() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }
}
