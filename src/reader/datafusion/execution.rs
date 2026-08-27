//! Optional DataFusion physical execution adapter.
//!
//! # Intra-file repartitioning
//!
//! Scan planning first balances whole Delta data files across the resolved
//! partition target. By default, DataFusion's `FileGroupPartitioner` only
//! receives those groups when they do not fill the target. The provider can
//! instead allow repartitioning at any partition count. In either mode, the
//! partitioner flattens the groups, divides their total bytes across the target,
//! and returns new groups containing whole files or byte ranges. Its
//! `repartition_file_min_size` setting is the minimum total input size needed
//! to attempt this operation, not the size of each generated range.
//!
//! Each returned range is stored on its `DeltaScanFileTask`. During direct
//! Parquet execution, the range containing a row group's first column chunk
//! page offset owns that complete row group. Range ownership and
//! footer-statistics pruning are intersected before the selected row groups
//! are passed to the Parquet reader, so a byte boundary never splits a row
//! group.

use std::{
    collections::HashSet,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    common::{DataFusionError, Result as DataFusionResult, config::ConfigOptions},
    datasource::{
        listing::{FileRange, PartitionedFile},
        physical_plan::{FileGroup, FileGroupPartitioner},
    },
    execution::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType, SchedulingType},
        filter_pushdown::{
            ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
        },
        stream::RecordBatchStreamAdapter,
    },
};
use futures_util::{StreamExt, stream};

use super::{
    dynamic_filters::{
        DeltaDynamicFilterOutcome, DeltaDynamicFilterPlan, DeltaRetainedDynamicFilter,
    },
    dynamic_partition_pruning::{
        DeltaDynamicPartitionKeepReason, DeltaDynamicPartitionPruningDecision,
        evaluate_dynamic_partition_filter,
    },
    planning::DataFusionScanPlanning,
};

use crate::reader::backend::direct_parquet::{
    DirectParquetMetadataCache, direct_parquet_file_executor,
    direct_parquet_file_executor_with_metadata_cache,
};
use crate::{
    DeltaReaderError, DeltaScanMetrics, DeltaScanMetricsSnapshot, ParquetReaderBackend,
    delta::kernel::DeltaKernelPredicate,
    reader::{
        delta_kernel_executor,
        metrics::saturating_fetch_add,
        planning::{DeltaScanFileTask, DeltaScanFileTaskPartition, DeltaScanPlan, build_partition},
        scheduling::{DeltaScanExecution, FileAdmission, FileAdmissionFn, ScanReadLimiter},
    },
};

/// Controls when DataFusion may split direct Parquet reads into ranged scan tasks.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IntraFileRepartitioning {
    /// Allow splitting only below the target partition count.
    #[default]
    WhenBelowTarget,
    /// Allow splitting at any partition count.
    Always,
}

impl IntraFileRepartitioning {
    fn allows_repartitioning(self, current_partitions: usize, target_partitions: usize) -> bool {
        match self {
            Self::WhenBelowTarget => current_partitions < target_partitions,
            Self::Always => true,
        }
    }
}

/// Immutable point-in-time DataFusion scan metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanMetricsSnapshot {
    /// Core Delta scan planning and execution metrics.
    pub core_metrics: DeltaScanMetricsSnapshot,
    /// Whether the provider requested Arrow view arrays for string and binary data columns.
    pub use_arrow_view_types: bool,
    /// Configured DataFusion batch row target observed at execution.
    pub configured_batch_size_rows: Option<u64>,
    /// File tasks pruned before admission by a dynamic partition filter.
    /// A task is either a whole physical file or one independently read file range.
    pub dynamic_partition_tasks_pruned: u64,
    /// File tasks kept after consulting retained dynamic partition filters.
    /// A task is either a whole physical file or one independently read file range.
    pub dynamic_partition_tasks_kept: u64,
    /// Physical filters offered to the post-optimization hook.
    pub dynamic_filters_received: u64,
    /// Offered filters retained for dynamic partition pruning.
    pub dynamic_filters_accepted: u64,
    /// Offered filters rejected by the dynamic partition policy.
    pub dynamic_filters_rejected: u64,
    /// Dynamic partition filters checked against file tasks during admission.
    pub dynamic_partition_filter_checks: u64,
    /// Kept file tasks with missing, invalid, or unparsable partition metadata.
    pub dynamic_partition_tasks_kept_unusable_metadata: u64,
    /// Kept file tasks whose dynamic filter was unavailable or unevaluable.
    pub dynamic_partition_tasks_kept_unevaluable_filter: u64,
}

/// Shared live metrics for one DataFusion physical scan plan.
#[derive(Clone)]
pub struct ScanMetrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    registration_name: Option<String>,
    core_metrics: DeltaScanMetrics,
    use_arrow_view_types: bool,
    configured_batch_size_rows: AtomicU64,
    dynamic_partition_tasks_pruned: AtomicU64,
    dynamic_partition_tasks_kept: AtomicU64,
    dynamic_filters_received: AtomicU64,
    dynamic_filters_accepted: AtomicU64,
    dynamic_filters_rejected: AtomicU64,
    dynamic_partition_filter_checks: AtomicU64,
    dynamic_partition_tasks_kept_unusable_metadata: AtomicU64,
    dynamic_partition_tasks_kept_unevaluable_filter: AtomicU64,
}

impl ScanMetrics {
    #[allow(dead_code)]
    fn new(
        registration_name: Option<String>,
        core_metrics: DeltaScanMetrics,
        use_arrow_view_types: bool,
    ) -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                registration_name,
                core_metrics,
                use_arrow_view_types,
                configured_batch_size_rows: AtomicU64::new(0),
                dynamic_partition_tasks_pruned: AtomicU64::new(0),
                dynamic_partition_tasks_kept: AtomicU64::new(0),
                dynamic_filters_received: AtomicU64::new(0),
                dynamic_filters_accepted: AtomicU64::new(0),
                dynamic_filters_rejected: AtomicU64::new(0),
                dynamic_partition_filter_checks: AtomicU64::new(0),
                dynamic_partition_tasks_kept_unusable_metadata: AtomicU64::new(0),
                dynamic_partition_tasks_kept_unevaluable_filter: AtomicU64::new(0),
            }),
        }
    }

    /// Returns the optional registration label supplied by the DataFusion provider.
    pub fn registration_name(&self) -> Option<&str> {
        self.inner.registration_name.as_deref()
    }

    /// Returns an immutable point-in-time copy of all DataFusion scan metrics.
    pub fn snapshot(&self) -> ScanMetricsSnapshot {
        let inner = self.inner.as_ref();
        ScanMetricsSnapshot {
            core_metrics: inner.core_metrics.snapshot(),
            use_arrow_view_types: inner.use_arrow_view_types,
            configured_batch_size_rows: nonzero_load(&inner.configured_batch_size_rows),
            dynamic_partition_tasks_pruned: load(&inner.dynamic_partition_tasks_pruned),
            dynamic_partition_tasks_kept: load(&inner.dynamic_partition_tasks_kept),
            dynamic_filters_received: load(&inner.dynamic_filters_received),
            dynamic_filters_accepted: load(&inner.dynamic_filters_accepted),
            dynamic_filters_rejected: load(&inner.dynamic_filters_rejected),
            dynamic_partition_filter_checks: load(&inner.dynamic_partition_filter_checks),
            dynamic_partition_tasks_kept_unusable_metadata: load(
                &inner.dynamic_partition_tasks_kept_unusable_metadata,
            ),
            dynamic_partition_tasks_kept_unevaluable_filter: load(
                &inner.dynamic_partition_tasks_kept_unevaluable_filter,
            ),
        }
    }

    fn record_configured_batch_size_rows(&self, value: usize) {
        self.inner
            .configured_batch_size_rows
            .store(u64::try_from(value).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn record_dynamic_partition_task_pruned(&self) {
        saturating_fetch_add(&self.inner.dynamic_partition_tasks_pruned, 1);
    }

    fn record_dynamic_partition_task_kept(&self) {
        saturating_fetch_add(&self.inner.dynamic_partition_tasks_kept, 1);
    }

    fn record_dynamic_filters_received(&self, value: usize) {
        saturating_fetch_add(
            &self.inner.dynamic_filters_received,
            u64::try_from(value).unwrap_or(u64::MAX),
        );
    }

    fn record_dynamic_filters_accepted(&self, value: usize) {
        saturating_fetch_add(
            &self.inner.dynamic_filters_accepted,
            u64::try_from(value).unwrap_or(u64::MAX),
        );
    }

    fn record_dynamic_filters_rejected(&self, value: usize) {
        saturating_fetch_add(
            &self.inner.dynamic_filters_rejected,
            u64::try_from(value).unwrap_or(u64::MAX),
        );
    }

    fn record_dynamic_partition_filter_check(&self) {
        saturating_fetch_add(&self.inner.dynamic_partition_filter_checks, 1);
    }

    fn record_unusable_metadata(&self) {
        saturating_fetch_add(
            &self.inner.dynamic_partition_tasks_kept_unusable_metadata,
            1,
        );
    }

    fn record_unevaluable_filter(&self) {
        saturating_fetch_add(
            &self.inner.dynamic_partition_tasks_kept_unevaluable_filter,
            1,
        );
    }

    fn identity(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }
}

impl fmt::Debug for ScanMetrics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScanMetrics")
            .finish_non_exhaustive()
    }
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

fn nonzero_load(counter: &AtomicU64) -> Option<u64> {
    match load(counter) {
        0 => None,
        value => Some(value),
    }
}

/// Collects distinct Delta DataFusion scan metrics in depth-first plan order.
pub fn collect_scan_metrics(plan: &dyn ExecutionPlan) -> Vec<ScanMetrics> {
    fn collect(
        plan: &dyn ExecutionPlan,
        seen_plans: &mut HashSet<usize>,
        seen_metrics: &mut HashSet<usize>,
        metrics: &mut Vec<ScanMetrics>,
    ) {
        let plan_identity = plan as *const dyn ExecutionPlan as *const () as usize;
        if !seen_plans.insert(plan_identity) {
            return;
        }
        if let Some(scan) = plan.downcast_ref::<DeltaDataFusionExec>() {
            let handle = scan.metrics.clone();
            if seen_metrics.insert(handle.identity()) {
                metrics.push(handle);
            }
        }
        for child in plan.children() {
            collect(child.as_ref(), seen_plans, seen_metrics, metrics);
        }
    }

    let mut metrics = Vec::new();
    collect(plan, &mut HashSet::new(), &mut HashSet::new(), &mut metrics);
    metrics
}

#[allow(dead_code)]
pub(crate) fn create_datafusion_execution_plan(
    plan: DeltaScanPlan,
    planning: DataFusionScanPlanning,
    row_predicate: Option<DeltaKernelPredicate>,
    registration_name: Option<String>,
    use_arrow_view_types: bool,
    intra_file_repartitioning: IntraFileRepartitioning,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(DeltaDataFusionExec::new(
        plan,
        planning,
        row_predicate,
        registration_name,
        use_arrow_view_types,
        intra_file_repartitioning,
    ))
}

#[derive(Clone)]
struct DeltaDataFusionExec {
    plan: Arc<DeltaScanPlan>,
    schema: SchemaRef,
    output_projection: Option<Arc<[usize]>>,
    row_predicate: Option<DeltaKernelPredicate>,
    properties: Arc<PlanProperties>,
    metrics: ScanMetrics,
    limiter: Arc<ScanReadLimiter>,
    dynamic_filters: Arc<[DeltaRetainedDynamicFilter]>,
    intra_file_repartitioning: IntraFileRepartitioning,
    intra_file_repartitioning_applied: bool,
    parquet_metadata_cache: Option<Arc<DirectParquetMetadataCache>>,
}

impl DeltaDataFusionExec {
    #[allow(dead_code)]
    fn new(
        plan: DeltaScanPlan,
        planning: DataFusionScanPlanning,
        row_predicate: Option<DeltaKernelPredicate>,
        registration_name: Option<String>,
        use_arrow_view_types: bool,
        intra_file_repartitioning: IntraFileRepartitioning,
    ) -> Self {
        let schema = planning.projection.output_schema;
        let output_projection = planning.projection.output_projection.map(Arc::from);
        let properties = scan_properties(&schema, plan.partitions.len());
        let metrics = ScanMetrics::new(
            registration_name,
            plan.metrics.clone(),
            use_arrow_view_types,
        );
        let limiter = ScanReadLimiter::new(
            plan.execution_options,
            plan.partition_target_diagnostic.target_partitions,
            plan.partitions.len(),
        );

        Self {
            plan: Arc::new(plan),
            schema,
            output_projection,
            row_predicate,
            properties,
            metrics,
            limiter,
            dynamic_filters: Arc::from([]),
            intra_file_repartitioning,
            intra_file_repartitioning_applied: false,
            parquet_metadata_cache: None,
        }
    }

    fn with_dynamic_filters(
        &self,
        dynamic_filters: Vec<DeltaRetainedDynamicFilter>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            dynamic_filters: Arc::from(dynamic_filters),
            ..self.clone()
        })
    }

    fn with_repartitioned_partitions(
        &self,
        partitions: Vec<DeltaScanFileTaskPartition>,
    ) -> Arc<dyn ExecutionPlan> {
        let partition_count = partitions.len();
        let mut plan = (*self.plan).clone();
        plan.partitions = partitions;
        plan.metrics.record_scan_partitions_planned(partition_count);
        let target_partitions = plan.partition_target_diagnostic.target_partitions;
        let limiter =
            ScanReadLimiter::new(plan.execution_options, target_partitions, partition_count);
        Arc::new(Self {
            plan: Arc::new(plan),
            properties: scan_properties(&self.schema, partition_count),
            limiter,
            intra_file_repartitioning_applied: true,
            parquet_metadata_cache: Some(Arc::new(DirectParquetMetadataCache::default())),
            ..self.clone()
        })
    }
}

fn scan_properties(schema: &SchemaRef, partition_count: usize) -> Arc<PlanProperties> {
    Arc::new(
        PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(schema)),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative),
    )
}

/// Uses DataFusion to split and regroup file tasks across a partition target.
///
/// DataFusion flattens the current groups and aims for
/// `ceil(total_input_bytes / target_partitions)` bytes per output group. The
/// `minimum_total_bytes` argument controls whether the complete input is large
/// enough to repartition; it is not a generated range size.
///
/// `Ok(None)` preserves the existing plan when the selected policy does not
/// apply, file sizes are unavailable, or DataFusion finds no useful
/// repartitioning.
fn repartition_file_tasks(
    partitions: &[DeltaScanFileTaskPartition],
    target_partitions: usize,
    minimum_total_bytes: usize,
    policy: IntraFileRepartitioning,
) -> DataFusionResult<Option<Vec<DeltaScanFileTaskPartition>>> {
    if target_partitions == 0 {
        return Err(adapter_error("scan_partition_target_must_be_positive"));
    }
    // Whole-file planning already balances up to the requested partition count.
    // The default avoids extra ranged reads unless the scan lacks parallelism.
    if !policy.allows_repartitioning(partitions.len(), target_partitions) {
        return Ok(None);
    }

    // DataFusion cannot safely split tasks whose physical file size is unknown.
    let Some(file_groups) = file_groups_from_partitions(partitions)? else {
        return Ok(None);
    };
    let Some(file_groups) = FileGroupPartitioner::new()
        .with_target_partitions(target_partitions)
        .with_repartition_file_min_size(minimum_total_bytes)
        .repartition_file_groups(&file_groups)
    else {
        // Preserve the original plan when DataFusion finds no useful split.
        return Ok(None);
    };

    partitions_from_file_groups(file_groups).map(Some)
}

fn file_groups_from_partitions(
    partitions: &[DeltaScanFileTaskPartition],
) -> DataFusionResult<Option<Vec<FileGroup>>> {
    let mut groups = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let mut files = Vec::with_capacity(partition.file_tasks.len());
        for task in &partition.file_tasks {
            let Some(file) = partitioned_file_from_task(task)? else {
                return Ok(None);
            };
            files.push(file);
        }
        groups.push(FileGroup::new(files));
    }
    Ok(Some(groups))
}

fn partitioned_file_from_task(
    task: &DeltaScanFileTask,
) -> DataFusionResult<Option<PartitionedFile>> {
    let Some(file_size) = task.file_size.filter(|size| *size > 0) else {
        return Ok(None);
    };
    let mut file = PartitionedFile::new(&task.path, file_size);
    if let Some(range) = &task.parquet_byte_range {
        if range.start >= range.end || range.end > file_size {
            return Err(adapter_error("scan_file_range_invalid"));
        }
        file.range = Some(FileRange {
            start: i64::try_from(range.start)
                .map_err(|_| adapter_error("scan_file_range_invalid"))?,
            end: i64::try_from(range.end).map_err(|_| adapter_error("scan_file_range_invalid"))?,
        });
    }
    // DataFusion copies extensions to every output range. Carry the Delta task
    // so its schema, partition, transform, and deletion-vector metadata survive.
    Ok(Some(file.with_extension(task.clone())))
}

fn partitions_from_file_groups(
    groups: Vec<FileGroup>,
) -> DataFusionResult<Vec<DeltaScanFileTaskPartition>> {
    let mut partitions = Vec::with_capacity(groups.len());
    for group in groups {
        let tasks = group
            .into_inner()
            .into_iter()
            .map(task_from_partitioned_file)
            .collect::<DataFusionResult<Vec<_>>>()?;
        partitions.push(build_partition(tasks).map_err(datafusion_error)?);
    }
    Ok(partitions)
}

fn task_from_partitioned_file(file: PartitionedFile) -> DataFusionResult<DeltaScanFileTask> {
    let mut task = file
        .extension::<DeltaScanFileTask>()
        .cloned()
        .ok_or_else(|| adapter_error("scan_file_task_extension_missing"))?;
    let range = file
        .range
        .ok_or_else(|| adapter_error("scan_file_range_missing"))?;
    let start = u64::try_from(range.start).map_err(|_| adapter_error("scan_file_range_invalid"))?;
    let end = u64::try_from(range.end).map_err(|_| adapter_error("scan_file_range_invalid"))?;
    let file_size = task
        .file_size
        .ok_or_else(|| adapter_error("scan_file_size_missing"))?;
    if start >= end || end > file_size {
        return Err(adapter_error("scan_file_range_invalid"));
    }
    task.parquet_byte_range = (start != 0 || end != file_size).then_some(start..end);
    if task.parquet_byte_range.is_some() {
        // A range covers an unknown subset of the file's rows, so the original
        // whole-file estimate is no longer valid for partition accounting.
        task.estimated_rows = None;
    }
    Ok(task)
}

impl fmt::Debug for DeltaDataFusionExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaDataFusionExec")
            .field("snapshot_version", &self.plan.snapshot_version)
            .field("partition_count", &self.plan.partitions.len())
            .field("dynamic_filter_count", &self.dynamic_filters.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DeltaDataFusionExec {
    fn fmt_as(
        &self,
        display_type: DisplayFormatType,
        formatter: &mut fmt::Formatter,
    ) -> fmt::Result {
        match display_type {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                formatter,
                "DeltaDataFusionExec: snapshot_version={}, partitions={}",
                self.plan.snapshot_version,
                self.plan.partitions.len()
            ),
            DisplayFormatType::TreeRender => write!(formatter, "DeltaDataFusionExec"),
        }
    }
}

impl ExecutionPlan for DeltaDataFusionExec {
    fn name(&self) -> &str {
        "DeltaDataFusionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "DeltaDataFusionExec does not accept child execution plans".to_owned(),
            ))
        }
    }

    fn repartitioned(
        &self,
        _datafusion_target_partitions: usize,
        config: &ConfigOptions,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        if self.intra_file_repartitioning_applied
            || self.plan.execution_options.reader_backend() != ParquetReaderBackend::Direct
        {
            return Ok(None);
        }
        // Scan planning already applied the provider override and resource caps.
        let target_partitions = self.plan.partition_target_diagnostic.target_partitions;
        let Some(partitions) = repartition_file_tasks(
            &self.plan.partitions,
            target_partitions,
            config.optimizer.repartition_file_min_size,
            self.intra_file_repartitioning,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(self.with_repartitioned_partitions(partitions)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition >= self.plan.partitions.len() {
            return Err(adapter_error("scan_partition_index_out_of_range"));
        }

        let configured_batch_size_rows = context.session_config().batch_size();
        self.metrics
            .record_configured_batch_size_rows(configured_batch_size_rows);
        let admission = dynamic_admission(self.metrics.clone(), Arc::clone(&self.dynamic_filters));
        let executor = match self.plan.execution_options.reader_backend() {
            ParquetReaderBackend::Direct => match &self.parquet_metadata_cache {
                Some(cache) => direct_parquet_file_executor_with_metadata_cache(
                    &self.plan,
                    Some(configured_batch_size_rows),
                    self.row_predicate.clone(),
                    Arc::clone(cache),
                ),
                None => direct_parquet_file_executor(
                    &self.plan,
                    Some(configured_batch_size_rows),
                    self.row_predicate.clone(),
                ),
            },
            ParquetReaderBackend::DeltaKernel => delta_kernel_executor(&self.plan),
        };
        let stream = DeltaScanExecution::with_shared_limiter(
            Arc::clone(&self.plan),
            Arc::clone(&self.limiter),
        )
        .partition_stream(partition, admission, executor)
        .map_err(datafusion_error)?;
        let schema = Arc::clone(&self.schema);
        let projection = self.output_projection.clone();
        let stream = stream::unfold(
            (Some(stream), projection),
            |(stream, projection)| async move {
                let mut stream = stream?;
                let result = stream.next().await?;
                let result = finalize_output_batch(result, projection.as_deref());
                let stream = result.is_ok().then_some(stream);
                Some((result, (stream, projection)))
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DataFusionResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let parent_filters = child_pushdown_result
            .parent_filters
            .iter()
            .map(|result| Arc::clone(&result.filter))
            .collect::<Vec<_>>();
        let unsupported = || {
            FilterPushdownPropagation::with_parent_pushdown_result(vec![
                PushedDown::No;
                parent_filters.len()
            ])
        };
        if phase != FilterPushdownPhase::Post || parent_filters.is_empty() {
            return Ok(unsupported());
        }

        let dynamic_filter_plan = DeltaDynamicFilterPlan::from_filters(
            &parent_filters,
            &self.schema,
            &self.plan.partition_columns,
        );
        let accepted = dynamic_filter_plan.accepted_filters.len();
        self.metrics
            .record_dynamic_filters_received(parent_filters.len());
        self.metrics.record_dynamic_filters_accepted(accepted);
        self.metrics
            .record_dynamic_filters_rejected(parent_filters.len().saturating_sub(accepted));
        if !dynamic_filter_plan.has_accepted_filters() {
            return Ok(unsupported());
        }

        let pushed = dynamic_filter_plan
            .decisions
            .iter()
            .map(|decision| match decision.outcome {
                DeltaDynamicFilterOutcome::Accepted => PushedDown::Yes,
                DeltaDynamicFilterOutcome::Rejected => PushedDown::No,
            })
            .collect();
        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(pushed)
                .with_updated_node(self.with_dynamic_filters(dynamic_filter_plan.accepted_filters)),
        )
    }
}

fn dynamic_admission(
    metrics: ScanMetrics,
    filters: Arc<[DeltaRetainedDynamicFilter]>,
) -> FileAdmissionFn<DeltaScanFileTask> {
    Arc::new(move |task| {
        if filters.is_empty() {
            return Ok(FileAdmission::Admit);
        }

        let mut unusable_metadata = false;
        let mut unevaluable_filter = false;
        for filter in filters.iter() {
            metrics.record_dynamic_partition_filter_check();
            match evaluate_dynamic_partition_filter(filter, task) {
                DeltaDynamicPartitionPruningDecision::Prune(_) => {
                    metrics.record_dynamic_partition_task_pruned();
                    return Ok(FileAdmission::Skip);
                }
                DeltaDynamicPartitionPruningDecision::Keep(reason) => {
                    unusable_metadata |= is_unusable_metadata(reason);
                    unevaluable_filter |= is_unevaluable_filter(reason);
                }
            }
        }
        if unusable_metadata {
            metrics.record_unusable_metadata();
        }
        if unevaluable_filter {
            metrics.record_unevaluable_filter();
        }
        metrics.record_dynamic_partition_task_kept();
        Ok(FileAdmission::Admit)
    })
}

fn is_unusable_metadata(reason: DeltaDynamicPartitionKeepReason) -> bool {
    matches!(
        reason,
        DeltaDynamicPartitionKeepReason::PartitionMetadataInvalid
            | DeltaDynamicPartitionKeepReason::PartitionValueMissing
            | DeltaDynamicPartitionKeepReason::PartitionValueUnparseable
    )
}

fn is_unevaluable_filter(reason: DeltaDynamicPartitionKeepReason) -> bool {
    matches!(
        reason,
        DeltaDynamicPartitionKeepReason::SnapshotUnavailable
            | DeltaDynamicPartitionKeepReason::UnsupportedPartitionType
            | DeltaDynamicPartitionKeepReason::EvaluationFailed
            | DeltaDynamicPartitionKeepReason::NonBooleanResult
    )
}

fn project_output_batch(
    batch: RecordBatch,
    projection: Option<&[usize]>,
) -> Result<RecordBatch, arrow::error::ArrowError> {
    match projection {
        Some(projection) => batch.project(projection),
        None => Ok(batch),
    }
}

fn finalize_output_batch(
    result: Result<RecordBatch, DeltaReaderError>,
    projection: Option<&[usize]>,
) -> DataFusionResult<RecordBatch> {
    let batch = result.map_err(datafusion_error)?;
    project_output_batch(batch, projection).map_err(|source| {
        datafusion_error(DeltaReaderError::DataFusionAdapter {
            reason: "scan_output_projection_failed",
            source: Box::new(DataFusionError::from(source)),
        })
    })
}

fn datafusion_error(error: DeltaReaderError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn adapter_error(reason: &'static str) -> DataFusionError {
    datafusion_error(DeltaReaderError::DataFusionAdapter {
        reason,
        source: Box::new(DataFusionError::Execution(reason.to_owned())),
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        error::Error,
        fs,
        path::{Path, PathBuf},
        thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow::array::StringArray;
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::{
        common::config::ConfigOptions,
        logical_expr::{Operator, col, lit},
        physical_expr::expressions::{
            BinaryExpr, Column, DynamicFilterPhysicalExpr, lit as physical_lit,
        },
        physical_plan::{
            ExecutionPlan,
            filter_pushdown::{
                ChildFilterPushdownResult, ChildPushdownResult, FilterPushdownPhase, PushedDown,
            },
            union::UnionExec,
        },
        prelude::{SessionConfig, SessionContext},
    };
    use futures_util::StreamExt;
    use parquet::arrow::ArrowWriter;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        DeltaReaderExecutionOptions, DeltaTable, DeltaTableBuilder,
        delta::kernel::delta_predicate_to_kernel_pruning,
        reader::datafusion::planning::{DataFusionFilterCapabilities, plan_datafusion_scan},
        reader::planning::{DeltaScanPartitionTargetOptions, plan_row_predicate, plan_scan},
    };

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    struct TestTable(PathBuf);

    impl TestTable {
        fn empty(name: &str) -> TestResult<Self> {
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let path = Path::new("target")
                .join("delta-arrow-reader-datafusion-tests")
                .join(format!("{}-{name}-{nanos}", std::process::id()));
            fs::create_dir_all(path.join("_delta_log"))?;
            let table = Self(path);
            table.write_log(&[protocol(), metadata()])?;
            Ok(table)
        }

        fn partitioned(name: &str) -> TestResult<Self> {
            let table = Self::empty(name)?;
            let west = table.write_parquet("west.parquet", &[1, 2])?;
            let east = table.write_parquet("east.parquet", &[3, 4])?;
            table.write_log(&[
                protocol(),
                metadata(),
                add("west.parquet", west, "west", 2, 1, 2),
                add("east.parquet", east, "east", 2, 3, 4),
            ])?;
            Ok(table)
        }

        fn late_dynamic(name: &str) -> TestResult<Self> {
            let table = Self::empty(name)?;
            let west = table.write_parquet("west.parquet", &[1, 2, 3])?;
            let east = table.write_parquet("east.parquet", &[4, 5])?;
            table.write_log(&[
                protocol(),
                metadata(),
                add("west.parquet", west, "west", 3, 1, 3),
                add("east.parquet", east, "east", 2, 4, 5),
            ])?;
            Ok(table)
        }

        fn missing(name: &str) -> TestResult<Self> {
            let table = Self::partitioned(name)?;
            table.write_log(&[
                protocol(),
                metadata(),
                add("missing.parquet", 100, "west", 1, 1, 1),
            ])?;
            Ok(table)
        }

        fn uri(&self) -> String {
            self.0.to_string_lossy().into_owned()
        }

        fn write_parquet(&self, name: &str, ids: &[i32]) -> TestResult<u64> {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(ids.to_vec()))],
            )?;
            let path = self.0.join(name);
            let mut writer = ArrowWriter::try_new(fs::File::create(&path)?, schema, None)?;
            writer.write(&batch)?;
            writer.close()?;
            Ok(fs::metadata(path)?.len())
        }

        fn write_log(&self, actions: &[Value]) -> TestResult {
            let contents = actions
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            fs::write(
                self.0.join("_delta_log/00000000000000000000.json"),
                format!("{contents}\n"),
            )?;
            Ok(())
        }
    }

    impl Drop for TestTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn protocol() -> Value {
        json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}})
    }

    fn metadata() -> Value {
        let schema = json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
                {"name": "region", "type": "string", "nullable": true, "metadata": {}}
            ]
        });
        json!({
            "metaData": {
                "id": "delta-arrow-reader-datafusion-test",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.to_string(),
                "partitionColumns": ["region"],
                "configuration": {},
                "createdTime": 1587968585495_i64
            }
        })
    }

    fn add(
        path: &str,
        size: u64,
        region: &str,
        num_records: u64,
        min_id: i32,
        max_id: i32,
    ) -> Value {
        let stats = json!({
            "numRecords": num_records,
            "minValues": {"id": min_id},
            "maxValues": {"id": max_id},
            "nullCount": {"id": 0}
        });
        json!({
            "add": {
                "path": path,
                "partitionValues": {"region": region},
                "size": size,
                "modificationTime": 1587968586000_i64,
                "dataChange": true,
                "stats": stats.to_string()
            }
        })
    }

    fn build_plan(
        table: &DeltaTable,
        projection: Option<&[usize]>,
        filters: &[datafusion::logical_expr::Expr],
        target_partitions: usize,
        execution_options: DeltaReaderExecutionOptions,
        registration_name: Option<String>,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaReaderError> {
        build_plan_with_repartitioning(
            table,
            projection,
            filters,
            target_partitions,
            execution_options,
            registration_name,
            IntraFileRepartitioning::default(),
        )
    }

    fn build_plan_with_repartitioning(
        table: &DeltaTable,
        projection: Option<&[usize]>,
        filters: &[datafusion::logical_expr::Expr],
        target_partitions: usize,
        execution_options: DeltaReaderExecutionOptions,
        registration_name: Option<String>,
        intra_file_repartitioning: IntraFileRepartitioning,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaReaderError> {
        let partition_columns = table
            .partition_columns()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let planning = plan_datafusion_scan(
            &table.schema(),
            &partition_columns,
            projection,
            &filter_refs,
            DataFusionFilterCapabilities {
                exact_predicate_evaluation: execution_options.reader_backend()
                    == ParquetReaderBackend::Direct,
            },
        )?;
        let physical_projection = planning.projection.physical_projection.clone();
        let hidden_columns = planning.projection.hidden_columns.clone();
        let kernel_predicate = planning
            .filters
            .predicate
            .as_ref()
            .and_then(delta_predicate_to_kernel_pruning);
        let row_predicate = match planning.filters.row_predicate.as_ref() {
            Some(predicate) => Some(delta_predicate_to_kernel_pruning(predicate).ok_or(
                DeltaReaderError::UnsupportedPredicate {
                    reason: "exact_row_predicate_not_kernel_safe",
                },
            )?),
            None => None,
        };
        let row_predicate = plan_row_predicate(
            table.snapshot(),
            physical_projection.as_deref(),
            &hidden_columns,
            row_predicate,
        )?;
        let include_stats = planning.filters.requires_statistics;
        let core = plan_scan(
            table.snapshot(),
            physical_projection.as_deref(),
            &hidden_columns,
            kernel_predicate,
            include_stats,
            execution_options,
            DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(target_partitions),
                caller_target_partitions: None,
            },
        )?;
        Ok(create_datafusion_execution_plan(
            core,
            planning,
            row_predicate,
            registration_name,
            true,
            intra_file_repartitioning,
        ))
    }

    fn session(batch_size: usize) -> SessionContext {
        SessionContext::new_with_config(SessionConfig::new().with_batch_size(batch_size))
    }

    fn sized_file_task(path: &str, size: Option<u64>) -> DeltaScanFileTask {
        use crate::{
            delta::kernel::KernelPhysicalToLogicalTransform,
            reader::deletion_vector::DeletionVectorMetadata,
        };

        DeltaScanFileTask {
            path: path.to_owned(),
            file_size: size,
            parquet_byte_range: None,
            estimated_rows: size,
            stats: None,
            modification_time_ms: None,
            partition_values: Default::default(),
            deletion_vector: DeletionVectorMetadata::default(),
            transform: KernelPhysicalToLogicalTransform::default(),
        }
    }

    #[allow(clippy::expect_used)]
    fn ids(batches: &[RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(batch.schema().index_of("id").expect("id column"))
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Int32 id")
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn file_repartitioning_balances_ranges_without_losing_file_identity() -> TestResult {
        let input = vec![build_partition(vec![
            sized_file_task("large.parquet", Some(100)),
            sized_file_task("small.parquet", Some(20)),
        ])?];
        let partitions = repartition_file_tasks(&input, 4, 1, IntraFileRepartitioning::default())?
            .ok_or("not repartitioned")?;

        assert_eq!(
            partitions
                .iter()
                .map(|partition| partition.estimated_bytes)
                .collect::<Vec<_>>(),
            vec![Some(30); 4]
        );
        let mut ranges = std::collections::BTreeMap::<_, Vec<_>>::new();
        for task in partitions
            .iter()
            .flat_map(|partition| &partition.file_tasks)
        {
            ranges.entry(task.path.as_str()).or_default().push(
                task.parquet_byte_range
                    .clone()
                    .unwrap_or(0..task.file_size.ok_or("missing file size")?),
            );
            assert_eq!(
                task.file_size,
                Some(if task.path == "large.parquet" {
                    100
                } else {
                    20
                })
            );
            if task.path == "small.parquet" {
                assert!(task.parquet_byte_range.is_none());
                assert_eq!(task.estimated_rows, Some(20));
            } else {
                assert_eq!(task.estimated_rows, None);
            }
        }
        assert_eq!(
            ranges.remove("large.parquet"),
            Some(vec![0..30, 30..60, 60..90, 90..100])
        );
        assert_eq!(
            ranges.remove("small.parquet"),
            Some(std::iter::once(0..20).collect())
        );
        assert!(ranges.is_empty());

        Ok(())
    }

    #[test]
    fn file_repartitioning_never_escapes_an_existing_range() -> TestResult {
        let mut task = sized_file_task("partial.parquet", Some(100));
        task.parquet_byte_range = Some(20..80);
        task.estimated_rows = None;
        let partitions = repartition_file_tasks(
            &[build_partition(vec![task])?],
            3,
            1,
            IntraFileRepartitioning::default(),
        )?
        .ok_or("partial range was not repartitioned")?;

        assert_eq!(
            partitions
                .iter()
                .flat_map(|partition| &partition.file_tasks)
                .map(|task| task.parquet_byte_range.clone())
                .collect::<Vec<_>>(),
            [Some(20..40), Some(40..60), Some(60..80)]
        );
        assert!(
            partitions
                .iter()
                .flat_map(|partition| &partition.file_tasks)
                .all(|task| task.file_size == Some(100) && task.estimated_rows.is_none())
        );

        Ok(())
    }

    #[test]
    fn file_repartitioning_policy_controls_full_whole_file_plans() -> TestResult {
        let input = vec![
            build_partition(vec![sized_file_task("huge.parquet", Some(1_000))])?,
            build_partition(vec![sized_file_task("small-0.parquet", Some(10))])?,
            build_partition(vec![sized_file_task("small-1.parquet", Some(10))])?,
            build_partition(vec![sized_file_task("small-2.parquet", Some(10))])?,
        ];

        assert!(
            repartition_file_tasks(&input, 4, 1, IntraFileRepartitioning::WhenBelowTarget)?
                .is_none()
        );
        let rebalanced = repartition_file_tasks(&input, 4, 1, IntraFileRepartitioning::Always)?
            .ok_or("full plan was not rebalanced")?;
        assert_eq!(
            rebalanced
                .iter()
                .map(|partition| partition.estimated_bytes)
                .collect::<Vec<_>>(),
            [Some(258), Some(258), Some(258), Some(256)]
        );
        assert!(
            rebalanced
                .iter()
                .flat_map(|partition| &partition.file_tasks)
                .any(|task| task.parquet_byte_range.is_some())
        );
        assert!(
            repartition_file_tasks(&input, 4, 1_031, IntraFileRepartitioning::Always)?.is_none()
        );
        assert!(repartition_file_tasks(&[], 4, 1, IntraFileRepartitioning::Always)?.is_none());
        assert!(
            input
                .iter()
                .flat_map(|partition| &partition.file_tasks)
                .all(|task| task.parquet_byte_range.is_none())
        );
        Ok(())
    }

    #[test]
    fn file_repartitioning_refuses_unsupported_inputs_and_rejects_invalid_ranges() -> TestResult {
        let input = vec![build_partition(vec![sized_file_task(
            "known.parquet",
            Some(120),
        )])?];

        assert!(
            repartition_file_tasks(&input, 4, 121, IntraFileRepartitioning::default())?.is_none()
        );
        assert!(
            repartition_file_tasks(
                &[build_partition(vec![sized_file_task(
                    "unknown.parquet",
                    None,
                )])?],
                4,
                1,
                IntraFileRepartitioning::default(),
            )?
            .is_none()
        );
        assert!(
            repartition_file_tasks(
                &[build_partition(vec![sized_file_task(
                    "empty.parquet",
                    Some(0),
                )])?],
                4,
                1,
                IntraFileRepartitioning::default(),
            )?
            .is_none()
        );
        assert!(repartition_file_tasks(&input, 0, 1, IntraFileRepartitioning::default()).is_err());

        for range in [90..110, std::ops::Range { start: 90, end: 80 }, 90..90] {
            let mut invalid = sized_file_task("invalid.parquet", Some(100));
            invalid.parquet_byte_range = Some(range);
            assert!(
                repartition_file_tasks(
                    &[build_partition(vec![invalid])?],
                    4,
                    1,
                    IntraFileRepartitioning::default(),
                )
                .is_err()
            );
        }

        let oversized = u64::try_from(i64::MAX)? + 1;
        assert!(
            repartition_file_tasks(
                &[build_partition(vec![sized_file_task(
                    "oversized.parquet",
                    Some(oversized),
                )])?],
                2,
                1,
                IntraFileRepartitioning::default(),
            )
            .is_err()
        );

        Ok(())
    }

    #[test]
    fn task_from_partitioned_file_rejects_malformed_partitioner_output() {
        let mut missing_extension = PartitionedFile::new("missing-extension.parquet", 100);
        missing_extension.range = Some(FileRange { start: 0, end: 100 });
        assert!(task_from_partitioned_file(missing_extension).is_err());

        let missing_range = PartitionedFile::new("missing-range.parquet", 100)
            .with_extension(sized_file_task("missing-range.parquet", Some(100)));
        assert!(task_from_partitioned_file(missing_range).is_err());

        for range in [
            FileRange {
                start: -1,
                end: 100,
            },
            FileRange { start: 50, end: 50 },
            FileRange { start: 0, end: 101 },
        ] {
            let mut invalid = PartitionedFile::new("invalid-range.parquet", 100)
                .with_extension(sized_file_task("invalid-range.parquet", Some(100)));
            invalid.range = Some(range);
            assert!(task_from_partitioned_file(invalid).is_err());
        }

        let mut missing_size = PartitionedFile::new("missing-size.parquet", 100)
            .with_extension(sized_file_task("missing-size.parquet", None));
        missing_size.range = Some(FileRange { start: 0, end: 100 });
        assert!(task_from_partitioned_file(missing_size).is_err());
    }

    #[tokio::test]
    async fn direct_repartitioning_reads_each_row_once_and_preserves_byte_accounting() -> TestResult
    {
        let fixture = TestTable::partitioned("file-repartitioning")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        let plan = build_plan_with_repartitioning(
            &table,
            None,
            &[],
            4,
            DeltaReaderExecutionOptions::new(),
            None,
            IntraFileRepartitioning::Always,
        )?;
        let expected_bytes = collect_scan_metrics(plan.as_ref())[0]
            .snapshot()
            .core_metrics
            .estimated_input_bytes;
        let mut config = ConfigOptions::new();
        config.optimizer.repartition_file_min_size = 1;
        let repartitioned = plan
            .repartitioned(4, &config)?
            .ok_or("Direct backend scan was not repartitioned")?;
        assert!(repartitioned.repartitioned(4, &config)?.is_none());

        assert_eq!(
            repartitioned
                .properties()
                .output_partitioning()
                .partition_count(),
            4
        );
        let mut actual_ids = ids(&datafusion::physical_plan::collect(
            Arc::clone(&repartitioned),
            session(1024).task_ctx(),
        )
        .await?);
        actual_ids.sort_unstable();
        assert_eq!(actual_ids, [1, 2, 3, 4]);
        let metrics = collect_scan_metrics(repartitioned.as_ref())[0].snapshot();
        assert_eq!(metrics.core_metrics.scan_partitions_planned, 4);
        assert_eq!(
            metrics.core_metrics.estimated_parquet_task_bytes_admitted,
            expected_bytes
        );

        let explicit_one = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        assert!(explicit_one.repartitioned(4, &config)?.is_none());

        {
            let kernel = build_plan_with_repartitioning(
                &table,
                None,
                &[],
                4,
                DeltaReaderExecutionOptions::new()
                    .with_reader_backend(ParquetReaderBackend::DeltaKernel),
                None,
                IntraFileRepartitioning::Always,
            )?;
            assert!(kernel.repartitioned(4, &config)?.is_none());
        }

        Ok(())
    }

    fn dynamic_filter(name: &str, index: usize) -> Arc<DynamicFilterPhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new(name, index))],
            physical_lit(true),
        ))
    }

    fn hook_input(
        filters: Vec<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
    ) -> ChildPushdownResult {
        ChildPushdownResult {
            parent_filters: filters
                .into_iter()
                .map(|filter| ChildFilterPushdownResult {
                    filter,
                    child_results: Vec::new(),
                })
                .collect(),
            self_filters: Vec::new(),
        }
    }

    #[tokio::test]
    async fn properties_projection_partitions_metrics_and_reexecution_match_provider_behavior()
    -> TestResult {
        let fixture = TestTable::partitioned("properties")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        let logical_filter = col("id").gt(lit(1_i32));
        let plan = build_plan(
            &table,
            Some(&[1, 0]),
            &[logical_filter],
            2,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;

        assert_eq!(plan.name(), "DeltaDataFusionExec");
        assert!(plan.children().is_empty());
        assert!(plan.metrics().is_none());
        assert_eq!(plan.schema().fields().len(), 2);
        assert_eq!(plan.schema().field(0).name(), "region");
        assert_eq!(plan.schema().field(1).name(), "id");
        assert_eq!(plan.properties().output_partitioning().partition_count(), 2);
        assert_eq!(
            plan.partition_statistics(None)?,
            Arc::new(datafusion::common::Statistics::new_unknown(&plan.schema()))
        );
        let context = session(1);
        let first =
            datafusion::physical_plan::collect(Arc::clone(&plan), context.task_ctx()).await?;
        assert!(first.iter().all(|batch| batch.num_rows() <= 1));
        let mut first_ids = ids(&first);
        first_ids.sort_unstable();
        assert_eq!(first_ids, [2, 3, 4]);

        let second =
            datafusion::physical_plan::collect(Arc::clone(&plan), context.task_ctx()).await?;
        let mut second_ids = ids(&second);
        second_ids.sort_unstable();
        assert_eq!(second_ids, first_ids);
        let handles = collect_scan_metrics(plan.as_ref());
        assert_eq!(handles.len(), 1);
        assert_eq!(handles[0].registration_name(), None);
        let metrics = handles[0].snapshot();
        assert_eq!(metrics.configured_batch_size_rows, Some(1));
        assert_eq!(metrics.core_metrics.scan_partitions_started, 4);
        assert_eq!(metrics.core_metrics.file_tasks_completed, 4);
        assert_eq!(metrics.core_metrics.scheduler_rows_emitted, 6);

        let hidden = build_plan(
            &table,
            Some(&[1]),
            &[col("id").gt(lit(1_i32))],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let hidden_batches = datafusion::physical_plan::collect(
            Arc::clone(&hidden),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert_eq!(hidden.schema().fields().len(), 1);
        assert_eq!(hidden.schema().field(0).name(), "region");
        assert!(hidden_batches.iter().all(|batch| batch.num_columns() == 1));
        assert_eq!(
            hidden_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            3
        );

        let partition_filter = build_plan(
            &table,
            None,
            &[col("region").eq(lit("west"))],
            2,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let partition_batches = datafusion::physical_plan::collect(
            Arc::clone(&partition_filter),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert_eq!(ids(&partition_batches), [1, 2]);
        assert_eq!(
            collect_scan_metrics(partition_filter.as_ref())[0]
                .snapshot()
                .core_metrics
                .file_tasks_started,
            1
        );

        let empty = build_plan(
            &table,
            Some(&[]),
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let empty_batches = datafusion::physical_plan::collect(
            Arc::clone(&empty),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert!(empty.schema().fields().is_empty());
        assert!(empty_batches.iter().all(|batch| batch.num_columns() == 0));
        assert_eq!(
            empty_batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            4
        );

        let empty_fixture = TestTable::empty("empty-scan")?;
        let empty_table = DeltaTableBuilder::new(empty_fixture.uri())
            .load_table()
            .await?;
        let empty_plan = build_plan(
            &empty_table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        assert_eq!(
            empty_plan
                .properties()
                .output_partitioning()
                .partition_count(),
            0
        );
        assert!(
            datafusion::physical_plan::collect(empty_plan, SessionContext::new().task_ctx(),)
                .await?
                .is_empty()
        );

        let invalid = plan.execute(2, context.task_ctx());
        let error = match invalid {
            Ok(_) => return Err("out-of-range partition unexpectedly executed".into()),
            Err(error) => error,
        };
        let DataFusionError::External(source) = error else {
            return Err("invalid partition did not preserve the reader error".into());
        };
        let reader = source
            .downcast_ref::<DeltaReaderError>()
            .ok_or("external error was not DeltaReaderError")?;
        assert_eq!(reader.code(), "datafusion_adapter");
        Ok(())
    }

    #[tokio::test]
    async fn dynamic_filter_hook_prunes_before_file_start_and_counts_once() -> TestResult {
        let fixture = TestTable::partitioned("dynamic")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        let plan = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let dynamic = dynamic_filter("region", 1);
        let physical: Arc<dyn datafusion::physical_plan::PhysicalExpr> = dynamic.clone();
        let rejected: Arc<dyn datafusion::physical_plan::PhysicalExpr> = dynamic_filter("id", 0);
        let pushed = plan.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![physical, rejected]),
            &ConfigOptions::new(),
        )?;
        assert!(matches!(
            pushed.filters.as_slice(),
            [PushedDown::Yes, PushedDown::No]
        ));
        let updated = pushed.updated_node.ok_or("dynamic plan was not retained")?;
        dynamic.update(Arc::new(BinaryExpr::new(
            Arc::new(Column::new("region", 1)),
            Operator::Eq,
            physical_lit("west"),
        )))?;

        let batches = datafusion::physical_plan::collect(
            Arc::clone(&updated),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert_eq!(ids(&batches), [1, 2]);
        let metrics = collect_scan_metrics(updated.as_ref())
            .pop()
            .ok_or("missing dynamic metrics")?
            .snapshot();
        assert_eq!(metrics.dynamic_filters_received, 2);
        assert_eq!(metrics.dynamic_filters_accepted, 1);
        assert_eq!(metrics.dynamic_filters_rejected, 1);
        assert_eq!(metrics.dynamic_partition_filter_checks, 2);
        assert_eq!(metrics.dynamic_partition_tasks_pruned, 1);
        assert_eq!(metrics.dynamic_partition_tasks_kept, 1);
        assert_eq!(metrics.core_metrics.file_tasks_started, 1);
        assert_eq!(metrics.core_metrics.file_tasks_completed, 1);
        assert_eq!(
            collect_scan_metrics(plan.as_ref())[0]
                .snapshot()
                .dynamic_filters_received,
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn physical_pushdown_preserves_dynamic_filters_across_plan_rebuild() -> TestResult {
        let fixture = TestTable::partitioned("dynamic-plan-rebuild")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        let plan = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let physical: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            dynamic_filter("region", 1);
        let pushed = plan.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![physical]),
            &ConfigOptions::new(),
        )?;
        let updated = pushed.updated_node.ok_or("expected updated scan")?;
        let rebuilt = Arc::clone(&updated).with_new_children(Vec::new())?;
        let reset = updated.reset_state()?;

        for candidate in [&rebuilt, &reset] {
            let debug = format!("{candidate:?}");
            assert!(debug.contains("dynamic_filter_count: 1"), "{debug}");
        }
        let display = datafusion::physical_plan::displayable(rebuilt.as_ref())
            .one_line()
            .to_string();
        assert!(display.contains("DeltaDataFusionExec:"), "{display}");
        assert!(display.contains("partitions="), "{display}");
        assert!(!display.contains("DynamicFilter"), "{display}");
        assert!(
            Arc::clone(&rebuilt)
                .with_new_children(vec![Arc::clone(&rebuilt)])
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn late_dynamic_filter_keeps_admitted_file_and_prunes_the_next() -> TestResult {
        let fixture = TestTable::late_dynamic("late-dynamic")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        let options = DeltaReaderExecutionOptions::new()
            .with_prefetch_files_per_partition(0)
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?
            .with_output_buffer_batches_per_partition(1)?;
        let plan = build_plan(&table, None, &[], 1, options, None)?;
        let dynamic = dynamic_filter("region", 1);
        let physical: Arc<dyn datafusion::physical_plan::PhysicalExpr> = dynamic.clone();
        let pushed = plan.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(vec![physical]),
            &ConfigOptions::new(),
        )?;
        let updated = pushed.updated_node.ok_or("dynamic plan was not retained")?;
        let mut stream = updated.execute(0, session(1).task_ctx())?;
        let first = stream.next().await.ok_or("missing first batch")??;
        assert_eq!(ids(std::slice::from_ref(&first)), [1]);

        dynamic.update(Arc::new(BinaryExpr::new(
            Arc::new(Column::new("region", 1)),
            Operator::Eq,
            physical_lit("none"),
        )))?;
        let mut batches = vec![first];
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        assert_eq!(ids(&batches), [1, 2, 3]);
        let metrics = collect_scan_metrics(updated.as_ref())[0].snapshot();
        assert_eq!(metrics.dynamic_partition_filter_checks, 2);
        assert_eq!(metrics.dynamic_partition_tasks_kept, 1);
        assert_eq!(metrics.dynamic_partition_tasks_pruned, 1);
        assert_eq!(metrics.core_metrics.file_tasks_started, 1);
        assert_eq!(metrics.core_metrics.file_tasks_completed, 1);
        Ok(())
    }

    #[tokio::test]
    async fn hook_is_post_only_empty_safe_and_collector_is_ordered_and_distinct() -> TestResult {
        let fixture = TestTable::partitioned("collector")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        let first = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            Some("first".to_owned()),
        )?;
        let second = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            Some("second".to_owned()),
        )?;
        let dynamic = dynamic_filter("region", 1);
        let physical: Arc<dyn datafusion::physical_plan::PhysicalExpr> = dynamic;
        let pre = first.handle_child_pushdown_result(
            FilterPushdownPhase::Pre,
            hook_input(vec![physical]),
            &ConfigOptions::new(),
        )?;
        assert!(pre.updated_node.is_none());
        assert!(matches!(pre.filters.as_slice(), [PushedDown::No]));
        let empty = first.handle_child_pushdown_result(
            FilterPushdownPhase::Post,
            hook_input(Vec::new()),
            &ConfigOptions::new(),
        )?;
        assert!(empty.updated_node.is_none());

        let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![
            Arc::clone(&first),
            Arc::clone(&second),
            Arc::clone(&first),
        ])?;
        let handles = collect_scan_metrics(union.as_ref());
        assert_eq!(handles.len(), 2);
        assert_eq!(handles[0].registration_name(), Some("first"));
        assert_eq!(handles[1].registration_name(), Some("second"));
        assert!(!format!("{:?}", handles[0]).contains("first"));
        let initial = handles[0].snapshot();
        assert_eq!(initial.configured_batch_size_rows, None);
        assert_eq!(
            [
                initial.dynamic_partition_tasks_pruned,
                initial.dynamic_partition_tasks_kept,
                initial.dynamic_filters_received,
                initial.dynamic_filters_accepted,
                initial.dynamic_filters_rejected,
                initial.dynamic_partition_filter_checks,
                initial.dynamic_partition_tasks_kept_unusable_metadata,
                initial.dynamic_partition_tasks_kept_unevaluable_filter,
            ],
            [0; 8]
        );

        let accepted: Arc<dyn datafusion::physical_plan::PhysicalExpr> =
            dynamic_filter("region", 1);
        let updated = first
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                hook_input(vec![accepted]),
                &ConfigOptions::new(),
            )?
            .updated_node
            .ok_or("expected updated scan")?;
        let shared_metrics_union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(vec![updated, Arc::clone(&first), Arc::clone(&second)])?;
        let shared_handles = collect_scan_metrics(shared_metrics_union.as_ref());
        assert_eq!(shared_handles.len(), 2);
        assert_eq!(shared_handles[0].registration_name(), Some("first"));
        assert_eq!(shared_handles[1].registration_name(), Some("second"));
        assert_eq!(handles[0].identity(), shared_handles[0].identity());
        assert_ne!(handles[0].identity(), shared_handles[1].identity());

        drop(shared_metrics_union);
        drop(union);
        drop(first);
        drop(second);
        assert_eq!(handles[0].snapshot().core_metrics.file_tasks_started, 0);
        assert_eq!(handles[0].registration_name(), Some("first"));
        Ok(())
    }

    #[tokio::test]
    async fn dynamic_admission_reason_counts_are_once_per_file_and_saturating() -> TestResult {
        use crate::{
            delta::kernel::KernelPhysicalToLogicalTransform,
            reader::datafusion::dynamic_filters::DeltaDynamicFilterPlan,
            reader::deletion_vector::DeletionVectorMetadata,
        };

        let fixture = TestTable::partitioned("dynamic-counters")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        let plan = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let metrics = collect_scan_metrics(plan.as_ref())
            .pop()
            .ok_or("missing metrics")?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, true),
        ]));
        let retained = |dynamic: Arc<DynamicFilterPhysicalExpr>| -> TestResult<_> {
            let physical: Arc<dyn datafusion::physical_plan::PhysicalExpr> = dynamic;
            Ok(DeltaDynamicFilterPlan::from_filters(
                std::slice::from_ref(&physical),
                &schema,
                &["region".to_owned()],
            )
            .accepted_filters
            .into_iter()
            .next()
            .ok_or("dynamic filter was not retained")?)
        };
        let first = retained(dynamic_filter("region", 1))?;
        let second = retained(dynamic_filter("region", 1))?;
        let missing = DeltaScanFileTask {
            path: "missing-partition.parquet".to_owned(),
            file_size: None,
            parquet_byte_range: None,
            estimated_rows: None,
            stats: None,
            modification_time_ms: None,
            partition_values: Default::default(),
            deletion_vector: DeletionVectorMetadata::default(),
            transform: KernelPhysicalToLogicalTransform::default(),
        };
        assert_eq!(
            dynamic_admission(metrics.clone(), Arc::from([first, second]))(&missing)?,
            FileAdmission::Admit
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.dynamic_partition_filter_checks, 2);
        assert_eq!(snapshot.dynamic_partition_tasks_kept, 1);
        assert_eq!(snapshot.dynamic_partition_tasks_kept_unusable_metadata, 1);

        let rejecting = dynamic_filter("region", 1);
        rejecting.update(physical_lit(false))?;
        let first = retained(rejecting)?;
        let second = retained(dynamic_filter("region", 1))?;
        let mut present = missing.clone();
        present
            .partition_values
            .insert("region".to_owned(), "west".to_owned());
        assert_eq!(
            dynamic_admission(metrics.clone(), Arc::from([first, second]))(&present)?,
            FileAdmission::Skip
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.dynamic_partition_filter_checks, 3);
        assert_eq!(snapshot.dynamic_partition_tasks_pruned, 1);
        assert_eq!(snapshot.dynamic_partition_tasks_kept, 1);
        assert_eq!(snapshot.dynamic_partition_tasks_kept_unusable_metadata, 1);

        let unsupported = dynamic_filter("region", 1);
        unsupported.update(physical_lit("not boolean"))?;
        assert_eq!(
            dynamic_admission(metrics.clone(), Arc::from([retained(unsupported)?]))(&present)?,
            FileAdmission::Admit
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.dynamic_partition_filter_checks, 4);
        assert_eq!(snapshot.dynamic_partition_tasks_kept, 2);
        assert_eq!(snapshot.dynamic_partition_tasks_kept_unevaluable_filter, 1);

        metrics
            .inner
            .dynamic_filters_received
            .store(u64::MAX - 1, Ordering::Relaxed);
        metrics.record_dynamic_filters_received(2);
        metrics.record_dynamic_filters_received(1);
        assert_eq!(metrics.snapshot().dynamic_filters_received, u64::MAX);
        Ok(())
    }

    #[tokio::test]
    async fn dynamic_metrics_updates_are_thread_safe() -> TestResult {
        const THREADS: usize = 4;
        const ITERATIONS: usize = 100;

        let fixture = TestTable::partitioned("dynamic-metrics-concurrency")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        let plan = build_plan(
            &table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let metrics = collect_scan_metrics(plan.as_ref())
            .pop()
            .ok_or("missing metrics")?;
        let mut handles = Vec::new();

        for _ in 0..THREADS {
            let metrics = metrics.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..ITERATIONS {
                    metrics.record_dynamic_partition_task_pruned();
                    metrics.record_dynamic_partition_task_kept();
                    metrics.record_dynamic_filters_received(3);
                    metrics.record_dynamic_filters_accepted(1);
                    metrics.record_dynamic_filters_rejected(2);
                    metrics.record_dynamic_partition_filter_check();
                    metrics.record_unusable_metadata();
                    metrics.record_unevaluable_filter();
                }
            }));
        }
        for handle in handles {
            handle.join().map_err(|_| "metrics worker panicked")?;
        }

        let calls = u64::try_from(THREADS * ITERATIONS)?;
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.dynamic_partition_tasks_pruned, calls);
        assert_eq!(snapshot.dynamic_partition_tasks_kept, calls);
        assert_eq!(snapshot.dynamic_filters_received, calls * 3);
        assert_eq!(snapshot.dynamic_filters_accepted, calls);
        assert_eq!(snapshot.dynamic_filters_rejected, calls * 2);
        assert_eq!(snapshot.dynamic_partition_filter_checks, calls);
        assert_eq!(
            snapshot.dynamic_partition_tasks_kept_unusable_metadata,
            calls
        );
        assert_eq!(
            snapshot.dynamic_partition_tasks_kept_unevaluable_filter,
            calls
        );
        Ok(())
    }

    #[tokio::test]
    async fn execution_error_and_stream_drop_preserve_partial_metrics() -> TestResult {
        let missing_fixture = TestTable::missing("error")?;
        let missing_table = DeltaTableBuilder::new(missing_fixture.uri())
            .load_table()
            .await?;
        let missing_plan = build_plan(
            &missing_table,
            None,
            &[],
            1,
            DeltaReaderExecutionOptions::new(),
            None,
        )?;
        let result = datafusion::physical_plan::collect(
            Arc::clone(&missing_plan),
            SessionContext::new().task_ctx(),
        )
        .await;
        let error = result.expect_err("missing file must fail");
        assert!(matches!(&error, DataFusionError::External(_)));
        assert!(!error.to_string().contains("missing.parquet"));
        let failed = collect_scan_metrics(missing_plan.as_ref())
            .pop()
            .ok_or("missing failure metrics")?;
        assert_eq!(failed.snapshot().core_metrics.file_tasks_started, 1);

        let fixture = TestTable::partitioned("drop")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        let options = DeltaReaderExecutionOptions::new()
            .with_prefetch_files_per_partition(0)
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?
            .with_output_buffer_batches_per_partition(1)?;
        let drop_plan = build_plan(&table, None, &[], 1, options, None)?;
        let handle = collect_scan_metrics(drop_plan.as_ref())
            .pop()
            .ok_or("missing drop metrics")?;
        let mut stream = drop_plan.execute(0, SessionContext::new().task_ctx())?;
        assert!(stream.next().await.transpose()?.is_some());
        drop(stream);
        tokio::task::yield_now().await;
        let stable = handle.snapshot();
        tokio::task::yield_now().await;
        assert_eq!(handle.snapshot(), stable);
        assert!(stable.core_metrics.file_tasks_started >= 1);
        let retry = datafusion::physical_plan::collect(
            Arc::clone(&drop_plan),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert_eq!(ids(&retry), [1, 2, 3, 4]);
        Ok(())
    }

    #[tokio::test]
    async fn reader_backends_produce_the_same_logical_rows() -> TestResult {
        let fixture = TestTable::partitioned("backends")?;
        let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
        let mut outputs = Vec::new();
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let options = DeltaReaderExecutionOptions::new().with_reader_backend(backend);
            let plan = build_plan(&table, Some(&[1, 0]), &[], 2, options, None)?;
            let mut batches =
                datafusion::physical_plan::collect(plan, SessionContext::new().task_ctx()).await?;
            batches.sort_by_key(|batch| {
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Int32 id")
                    .value(0)
            });
            outputs.push(
                batches
                    .iter()
                    .flat_map(|batch| {
                        let ids = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("Int32 id");
                        let regions = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("Utf8 region");
                        (0..batch.num_rows())
                            .map(|row| (regions.value(row).to_owned(), ids.value(row)))
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(outputs[0], outputs[1]);

        let kernel_options = DeltaReaderExecutionOptions::new()
            .with_reader_backend(ParquetReaderBackend::DeltaKernel);
        let inexact = build_plan(
            &table,
            None,
            &[col("id").gt(lit(1_i32))],
            1,
            kernel_options,
            None,
        )?;
        let unfiltered = datafusion::physical_plan::collect(
            Arc::clone(&inexact),
            SessionContext::new().task_ctx(),
        )
        .await?;
        assert_eq!(ids(&unfiltered), [1, 2, 3, 4]);

        let residual: Arc<dyn datafusion::physical_plan::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 0)),
            Operator::Gt,
            physical_lit(1_i32),
        ));
        let residual_plan: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(residual, inexact)?);
        let filtered =
            datafusion::physical_plan::collect(residual_plan, SessionContext::new().task_ctx())
                .await?;
        assert_eq!(ids(&filtered), [2, 3, 4]);
        Ok(())
    }
}
