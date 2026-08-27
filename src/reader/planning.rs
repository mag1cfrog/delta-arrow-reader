//! Private scan planning models.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BinaryHeap, HashSet},
    ops::Range,
    sync::Arc,
};

use arrow::datatypes::{Schema, SchemaRef};
use snafu::ResultExt;

use super::{
    deletion_vector::DeletionVectorMetadata,
    metrics::DeltaScanMetricsConfig,
    partition_target::{
        DeltaScanPartitionTargetDiagnosticOutput,
        delta_scan_partition_target_local_environment_diagnostic,
        derive_delta_scan_partition_target_diagnostic,
    },
};
use crate::{
    DeltaReaderError, DeltaScanExecutionOptions, DeltaScanMetrics,
    delta::{
        kernel::{
            DeltaKernelEngineContext, DeltaKernelPredicate, KernelPhysicalToLogicalTransform,
            KernelScan, KernelScanFileMetadata, KernelScanSchemas,
        },
        protocol::validate_protocol,
        snapshot::LoadedDeltaTableSnapshot,
    },
    error::{
        InvalidConfigurationSnafu, InvalidProjectionSnafu, ScanPartitionPlanningSnafu,
        ScanPlanningSnafu, UnsupportedPredicateSnafu,
    },
};

#[derive(Default)]
pub(crate) struct DeltaScanPartitionTargetOptions {
    pub(crate) explicit_target_partitions: Option<usize>,
    pub(crate) caller_target_partitions: Option<usize>,
}

#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct DeltaScanPlan {
    pub(crate) snapshot_version: u64,
    pub(crate) engine_context: Arc<DeltaKernelEngineContext>,
    pub(crate) logical_schema: SchemaRef,
    pub(crate) physical_schema: SchemaRef,
    pub(crate) projected_schema: SchemaRef,
    pub(crate) partition_columns: Vec<String>,
    pub(crate) kernel_schemas: KernelScanSchemas,
    pub(crate) partitions: Vec<DeltaScanFileTaskPartition>,
    pub(crate) partition_target_diagnostic: DeltaScanPartitionTargetDiagnosticOutput,
    pub(crate) add_actions_filtered_during_planning: Option<u64>,
    pub(crate) estimated_bytes: Option<u64>,
    pub(crate) estimated_rows: Option<u64>,
    pub(crate) physical_predicate: Option<DeltaKernelPredicate>,
    pub(crate) execution_options: DeltaScanExecutionOptions,
    pub(crate) metrics: DeltaScanMetrics,
}

#[allow(dead_code)]
pub(crate) struct DeltaUnpartitionedScanPlan {
    pub(crate) snapshot_version: u64,
    pub(crate) engine_context: Arc<DeltaKernelEngineContext>,
    pub(crate) logical_schema: SchemaRef,
    pub(crate) physical_schema: SchemaRef,
    pub(crate) projected_schema: SchemaRef,
    pub(crate) partition_columns: Vec<String>,
    pub(crate) kernel_schemas: KernelScanSchemas,
    pub(crate) file_tasks: Vec<DeltaScanFileTask>,
    pub(crate) add_actions_filtered_during_planning: Option<u64>,
    pub(crate) estimated_bytes: Option<u64>,
    pub(crate) estimated_rows: Option<u64>,
    pub(crate) physical_predicate: Option<DeltaKernelPredicate>,
    pub(crate) execution_options: DeltaScanExecutionOptions,
}

#[allow(dead_code)]
pub(crate) fn build_scan(
    snapshot: &LoadedDeltaTableSnapshot,
    projection: Option<&[String]>,
    predicate: Option<DeltaKernelPredicate>,
    include_stats: bool,
) -> Result<KernelScan, DeltaReaderError> {
    let _planning = tracing::debug_span!(
        target: "delta_arrow_reader::profile",
        "Delta Kernel scan construction"
    )
    .entered();
    validate_protocol(snapshot.protocol())?;
    validate_projection(snapshot.schema().as_ref(), projection)?;
    snapshot
        .kernel_snapshot()
        .build_scan(projection, predicate, include_stats)
        .boxed()
        .context(ScanPlanningSnafu {
            reason: "kernel_scan_build_failed",
        })
}

#[allow(dead_code)]
pub(crate) fn plan_scan(
    snapshot: &LoadedDeltaTableSnapshot,
    projection: Option<&[String]>,
    hidden_columns: &[String],
    kernel_predicate: Option<DeltaKernelPredicate>,
    include_stats: bool,
    execution_options: DeltaScanExecutionOptions,
    partition_target_options: DeltaScanPartitionTargetOptions,
) -> Result<DeltaScanPlan, DeltaReaderError> {
    let partition_target_diagnostic = local_partition_target_diagnostic(partition_target_options)?;
    let unpartitioned = build_unpartitioned_scan_plan(
        snapshot,
        projection,
        hidden_columns,
        kernel_predicate,
        include_stats,
        execution_options,
    )?;
    finalize_scan_plan(unpartitioned, partition_target_diagnostic)
}

#[allow(dead_code)]
pub(crate) fn plan_row_predicate(
    snapshot: &LoadedDeltaTableSnapshot,
    projection: Option<&[String]>,
    hidden_columns: &[String],
    predicate: Option<DeltaKernelPredicate>,
) -> Result<Option<DeltaKernelPredicate>, DeltaReaderError> {
    let Some(predicate) = predicate else {
        return Ok(None);
    };
    let projection = logical_projection(snapshot.schema().as_ref(), projection, hidden_columns)?;
    let predicate =
        build_scan(snapshot, projection.as_deref(), Some(predicate), false)?.physical_predicate();
    match predicate {
        Some(predicate) => Ok(Some(predicate)),
        None => UnsupportedPredicateSnafu {
            reason: "exact_row_predicate_not_physical",
        }
        .fail(),
    }
}

#[allow(dead_code)]
pub(crate) fn plan_unpartitioned_scan(
    snapshot: &LoadedDeltaTableSnapshot,
    projection: Option<&[String]>,
    hidden_columns: &[String],
    kernel_predicate: Option<DeltaKernelPredicate>,
    include_stats: bool,
    execution_options: DeltaScanExecutionOptions,
) -> Result<DeltaUnpartitionedScanPlan, DeltaReaderError> {
    build_unpartitioned_scan_plan(
        snapshot,
        projection,
        hidden_columns,
        kernel_predicate,
        include_stats,
        execution_options,
    )
}

fn build_unpartitioned_scan_plan(
    snapshot: &LoadedDeltaTableSnapshot,
    projection: Option<&[String]>,
    hidden_columns: &[String],
    kernel_predicate: Option<DeltaKernelPredicate>,
    include_stats: bool,
    execution_options: DeltaScanExecutionOptions,
) -> Result<DeltaUnpartitionedScanPlan, DeltaReaderError> {
    let logical_projection =
        logical_projection(snapshot.schema().as_ref(), projection, hidden_columns)?;
    let scan = build_scan(
        snapshot,
        logical_projection.as_deref(),
        kernel_predicate.clone(),
        include_stats,
    )?;
    let metadata = {
        let _planning = tracing::debug_span!(
            target: "delta_arrow_reader::profile",
            "Delta scan metadata expansion"
        )
        .entered();
        scan.file_metadata(snapshot.engine_context())
            .boxed()
            .context(ScanPlanningSnafu {
                reason: "kernel_scan_metadata_failed",
            })?
    };
    let file_tasks = metadata
        .files
        .into_iter()
        .map(DeltaScanFileTask::try_from_kernel)
        .collect::<Result<Vec<_>, _>>()?;
    let estimated_bytes = checked_sum(
        file_tasks.iter().map(|task| task.file_size),
        "scan_estimated_bytes_overflow",
    )?;
    let estimated_rows = checked_sum(
        file_tasks.iter().map(|task| task.estimated_rows),
        "scan_estimated_rows_overflow",
    )?;
    let logical_schema = scan.logical_schema();
    let physical_predicate = scan.physical_predicate();
    let projected_schema = match projection {
        None => Arc::clone(&logical_schema),
        Some(names) => Arc::new(Schema::new_with_metadata(
            logical_schema.fields()[..names.len()].to_vec(),
            logical_schema.metadata().clone(),
        )),
    };

    Ok(DeltaUnpartitionedScanPlan {
        snapshot_version: snapshot.version(),
        engine_context: Arc::clone(snapshot.engine_context()),
        logical_schema,
        physical_schema: scan.physical_schema(),
        projected_schema,
        partition_columns: snapshot.partition_columns().to_vec(),
        kernel_schemas: scan.schemas(),
        file_tasks,
        add_actions_filtered_during_planning: metadata.add_actions_filtered_during_planning,
        estimated_bytes,
        estimated_rows,
        physical_predicate,
        execution_options,
    })
}

fn finalize_scan_plan(
    unpartitioned: DeltaUnpartitionedScanPlan,
    partition_target_diagnostic: DeltaScanPartitionTargetDiagnosticOutput,
) -> Result<DeltaScanPlan, DeltaReaderError> {
    let files_planned = unpartitioned.file_tasks.len();
    let partitions = {
        let _planning = tracing::debug_span!(
            target: "delta_arrow_reader::profile",
            "Delta file task partitioning"
        )
        .entered();
        group_scan_file_tasks(
            unpartitioned.file_tasks,
            partition_target_diagnostic.target_partitions,
        )?
    };
    let metrics = DeltaScanMetrics::new(DeltaScanMetricsConfig {
        snapshot_version: unpartitioned.snapshot_version,
        parquet_backend: unpartitioned.execution_options.parquet_backend(),
        scan_partitions_planned: partitions.len(),
        files_planned,
        add_actions_filtered_during_planning: unpartitioned.add_actions_filtered_during_planning,
        estimated_input_rows: unpartitioned.estimated_rows,
        estimated_input_bytes: unpartitioned.estimated_bytes,
    });

    Ok(DeltaScanPlan {
        snapshot_version: unpartitioned.snapshot_version,
        engine_context: unpartitioned.engine_context,
        logical_schema: unpartitioned.logical_schema,
        physical_schema: unpartitioned.physical_schema,
        projected_schema: unpartitioned.projected_schema,
        partition_columns: unpartitioned.partition_columns,
        kernel_schemas: unpartitioned.kernel_schemas,
        partitions,
        partition_target_diagnostic,
        add_actions_filtered_during_planning: unpartitioned.add_actions_filtered_during_planning,
        estimated_bytes: unpartitioned.estimated_bytes,
        estimated_rows: unpartitioned.estimated_rows,
        physical_predicate: unpartitioned.physical_predicate,
        execution_options: unpartitioned.execution_options,
        metrics,
    })
}

fn local_partition_target_diagnostic(
    options: DeltaScanPartitionTargetOptions,
) -> Result<DeltaScanPartitionTargetDiagnosticOutput, DeltaReaderError> {
    let _planning = tracing::debug_span!(
        target: "delta_arrow_reader::profile",
        "Delta partition target selection"
    )
    .entered();
    let mut input = delta_scan_partition_target_local_environment_diagnostic().policy_input;
    input.explicit_target_partitions = options.explicit_target_partitions;
    if options.caller_target_partitions.is_some() {
        input.datafusion_target_partitions = options.caller_target_partitions;
    }
    derive_delta_scan_partition_target_diagnostic(input)
}

fn logical_projection(
    schema: &Schema,
    projection: Option<&[String]>,
    hidden_columns: &[String],
) -> Result<Option<Vec<String>>, DeltaReaderError> {
    validate_projection(schema, projection)?;
    for name in hidden_columns {
        if schema.index_of(name).is_err() {
            return InvalidProjectionSnafu {
                reason: "column_not_found",
            }
            .fail();
        }
    }

    Ok(projection.map(|projection| {
        let mut logical = projection.to_vec();
        for name in hidden_columns {
            if !logical.contains(name) {
                logical.push(name.clone());
            }
        }
        logical
    }))
}

/// File-read tasks executed together as one scan partition.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct DeltaScanFileTaskPartition {
    /// Whole-file or ranged Parquet reads assigned to this partition.
    pub(crate) file_tasks: Vec<DeltaScanFileTask>,
    /// Total bytes to read when every task has a known size.
    pub(crate) estimated_bytes: Option<u64>,
    /// Total output rows when every task has a valid whole-file estimate.
    pub(crate) estimated_rows: Option<u64>,
}

#[allow(dead_code)]
pub(crate) fn group_scan_file_tasks(
    file_tasks: Vec<DeltaScanFileTask>,
    target_partitions: usize,
) -> Result<Vec<DeltaScanFileTaskPartition>, DeltaReaderError> {
    if target_partitions == 0 {
        return InvalidConfigurationSnafu {
            reason: "scan_partition_target_must_be_positive",
        }
        .fail();
    }

    let estimated_bytes = checked_sum(
        file_tasks
            .iter()
            .map(DeltaScanFileTask::estimated_scan_bytes),
        "scan_estimated_bytes_overflow",
    )?;
    if file_tasks.is_empty() {
        return Ok(Vec::new());
    }
    if matches!(estimated_bytes, Some(0) | None) {
        group_by_file_count(file_tasks, target_partitions)
    } else {
        group_by_estimated_bytes(file_tasks, target_partitions)
    }
}

fn group_by_estimated_bytes(
    mut file_tasks: Vec<DeltaScanFileTask>,
    target_partitions: usize,
) -> Result<Vec<DeltaScanFileTaskPartition>, DeltaReaderError> {
    let output_limit = target_partitions.min(file_tasks.len());
    file_tasks.sort_by_key(|task| Reverse(task.estimated_scan_bytes()));
    let mut file_tasks = file_tasks.into_iter();
    let mut partition_tasks = Vec::with_capacity(output_limit);
    let mut partition_loads = BinaryHeap::with_capacity(output_limit);

    for partition_index in 0..output_limit {
        let Some(file_task) = file_tasks.next() else {
            return partition_planning_error("known_size_grouping_exhausted_tasks");
        };
        let Some(file_bytes) = file_task.estimated_scan_bytes() else {
            return partition_planning_error("known_size_grouping_missing_estimated_bytes");
        };
        partition_tasks.push(vec![file_task]);
        partition_loads.push(Reverse((file_bytes, partition_index)));
    }

    for file_task in file_tasks {
        let Some(file_bytes) = file_task.estimated_scan_bytes() else {
            return partition_planning_error("known_size_grouping_missing_estimated_bytes");
        };
        let Some(Reverse((partition_bytes, partition_index))) = partition_loads.pop() else {
            return partition_planning_error("known_size_grouping_missing_partition");
        };
        let Some(partition_bytes) = partition_bytes.checked_add(file_bytes) else {
            return partition_planning_error("partition_estimated_bytes_overflow");
        };
        partition_tasks[partition_index].push(file_task);
        partition_loads.push(Reverse((partition_bytes, partition_index)));
    }

    partition_tasks.into_iter().map(build_partition).collect()
}

fn group_by_file_count(
    file_tasks: Vec<DeltaScanFileTask>,
    target_partitions: usize,
) -> Result<Vec<DeltaScanFileTaskPartition>, DeltaReaderError> {
    let output_limit = target_partitions.min(file_tasks.len());
    let mut partitions = Vec::with_capacity(output_limit);
    let mut file_tasks = file_tasks.into_iter();
    let mut remaining_files = file_tasks.len();

    for partition_index in 0..output_limit {
        let remaining_partitions = output_limit - partition_index;
        let take_count = remaining_files.div_ceil(remaining_partitions);
        let partition_tasks = file_tasks.by_ref().take(take_count).collect::<Vec<_>>();
        if partition_tasks.len() != take_count {
            return partition_planning_error("file_count_grouping_exhausted_tasks");
        }
        remaining_files -= take_count;
        partitions.push(build_partition(partition_tasks)?);
    }

    Ok(partitions)
}

/// Builds a scan partition and its aggregate size and row estimates.
pub(crate) fn build_partition(
    file_tasks: Vec<DeltaScanFileTask>,
) -> Result<DeltaScanFileTaskPartition, DeltaReaderError> {
    let estimated_bytes = checked_sum(
        file_tasks
            .iter()
            .map(DeltaScanFileTask::estimated_scan_bytes),
        "partition_estimated_bytes_overflow",
    )?;
    let estimated_rows = checked_sum(
        file_tasks.iter().map(|task| task.estimated_rows),
        "partition_estimated_rows_overflow",
    )?;
    Ok(DeltaScanFileTaskPartition {
        file_tasks,
        estimated_bytes,
        estimated_rows,
    })
}

fn checked_sum(
    estimates: impl IntoIterator<Item = Option<u64>>,
    overflow_reason: &'static str,
) -> Result<Option<u64>, DeltaReaderError> {
    let mut total = 0_u64;
    for estimate in estimates {
        let Some(estimate) = estimate else {
            return Ok(None);
        };
        let Some(next) = total.checked_add(estimate) else {
            return partition_planning_error(overflow_reason);
        };
        total = next;
    }
    Ok(Some(total))
}

fn partition_planning_error<T>(reason: &'static str) -> Result<T, DeltaReaderError> {
    ScanPartitionPlanningSnafu { reason }.fail()
}

fn validate_projection(
    schema: &Schema,
    projection: Option<&[String]>,
) -> Result<(), DeltaReaderError> {
    let Some(projection) = projection else {
        return Ok(());
    };
    let mut seen = HashSet::with_capacity(projection.len());

    for name in projection {
        if !seen.insert(name) {
            return InvalidProjectionSnafu {
                reason: "duplicate_column",
            }
            .fail();
        }
        if schema.index_of(name).is_err() {
            return InvalidProjectionSnafu {
                reason: "column_not_found",
            }
            .fail();
        }
    }

    Ok(())
}

/// One physical Parquet read, covering either a whole data file or a byte range.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct DeltaScanFileTask {
    /// Object-store path of the physical data file.
    pub(crate) path: String,
    /// Size of the complete physical file, when known.
    pub(crate) file_size: Option<u64>,
    /// Byte range assigned by intra-file repartitioning, or `None` for the whole file.
    pub(crate) parquet_byte_range: Option<Range<u64>>,
    /// Expected output rows, available only for whole-file tasks with statistics.
    pub(crate) estimated_rows: Option<u64>,
    /// Delta file statistics used by planning and pruning.
    pub(crate) stats: Option<DeltaScanFileStats>,
    /// Data-file modification time from the Delta add action.
    pub(crate) modification_time_ms: Option<i64>,
    /// Logical partition-column values from the Delta add action.
    pub(crate) partition_values: BTreeMap<String, String>,
    /// Deletion vector associated with the complete physical file.
    pub(crate) deletion_vector: DeletionVectorMetadata,
    /// Physical-to-logical expression applied after reading the Parquet data.
    pub(crate) transform: KernelPhysicalToLogicalTransform,
}

/// Row-count statistics retained from a Delta add action.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct DeltaScanFileStats {
    /// Number of physical rows in the complete data file.
    pub(crate) num_records: u64,
}

#[allow(dead_code)]
impl DeltaScanFileTask {
    /// Returns bytes assigned to this task, using its range when split.
    pub(crate) fn estimated_scan_bytes(&self) -> Option<u64> {
        match &self.parquet_byte_range {
            Some(range) => range.end.checked_sub(range.start),
            None => self.file_size,
        }
    }

    pub(crate) fn try_from_kernel(file: KernelScanFileMetadata) -> Result<Self, DeltaReaderError> {
        let file_size = u64::try_from(file.size)
            .boxed()
            .context(ScanPlanningSnafu {
                reason: "negative_file_size",
            })?;

        let stats = file
            .estimated_rows
            .map(|num_records| DeltaScanFileStats { num_records });

        Ok(Self {
            path: file.path,
            file_size: Some(file_size),
            parquet_byte_range: None,
            estimated_rows: stats.as_ref().map(|stats| stats.num_records),
            stats,
            modification_time_ms: file.modification_time_ms,
            partition_values: file.partition_values,
            deletion_vector: DeletionVectorMetadata::from_kernel(file.deletion_vector),
            transform: file.transform,
        })
    }
}

#[cfg(test)]
mod tests {
    mod partition_pruning {
        use delta_kernel::expressions::{ColumnName, Expression, Predicate, Scalar};
        use serde_json::{Value, json};

        use super::{DeltaLogTable, PROTOCOL_JSON, plan_scan, planned_tasks};
        use crate::{
            DeltaComparison, DeltaPredicate, DeltaReaderError, DeltaReaderPhase, DeltaScalar,
            DeltaScanExecutionOptions, DeltaSnapshotSelection, DeltaStorageOptions,
            delta::{
                kernel::{DeltaKernelPredicate, delta_predicate_to_kernel_pruning},
                snapshot::{LoadedDeltaTableSnapshot, load_delta_table_snapshot_blocking},
            },
            reader::predicate::validate_predicate,
        };

        const TIMESTAMP_NTZ_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#;

        struct PartitionFixture {
            _table: DeltaLogTable,
            snapshot: LoadedDeltaTableSnapshot,
        }

        impl PartitionFixture {
            fn new(
                name: &str,
                protocol: &str,
                fields: Vec<Value>,
                partition_columns: &[&str],
                adds: Vec<String>,
            ) -> Result<Self, Box<dyn std::error::Error>> {
                let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
                    name,
                    protocol,
                    &metadata(fields, partition_columns),
                    &adds,
                )?;
                let snapshot = load_delta_table_snapshot_blocking(
                    &table.0.to_string_lossy(),
                    &DeltaStorageOptions::new(),
                    DeltaSnapshotSelection::Latest,
                )?;
                Ok(Self {
                    _table: table,
                    snapshot,
                })
            }

            fn all_paths(&self) -> Result<Vec<String>, DeltaReaderError> {
                self.kernel_paths(None)
            }

            fn selected_paths(
                &self,
                predicate: &DeltaPredicate,
            ) -> Result<Vec<String>, DeltaReaderError> {
                validate_predicate(predicate, self.snapshot.schema().as_ref())?;
                let predicate = delta_predicate_to_kernel_pruning(predicate)
                    .expect("characterization predicate has an exact Kernel representation");
                self.kernel_paths(Some(predicate))
            }

            fn selected_kernel_paths(
                &self,
                predicate: Predicate,
            ) -> Result<Vec<String>, DeltaReaderError> {
                self.kernel_paths(Some(DeltaKernelPredicate::from_test_predicate(predicate)))
            }

            fn kernel_paths(
                &self,
                predicate: Option<DeltaKernelPredicate>,
            ) -> Result<Vec<String>, DeltaReaderError> {
                let plan = plan_scan(
                    &self.snapshot,
                    None,
                    &[],
                    predicate,
                    false,
                    DeltaScanExecutionOptions::default(),
                )?;
                let mut paths = planned_tasks(&plan)
                    .map(|task| task.path.clone())
                    .collect::<Vec<_>>();
                paths.sort_unstable();
                Ok(paths)
            }
        }

        fn field(name: &str, data_type: &str) -> Value {
            json!({
                "name": name,
                "type": data_type,
                "nullable": true,
                "metadata": {}
            })
        }

        fn metadata(fields: Vec<Value>, partition_columns: &[&str]) -> String {
            let schema = json!({"type": "struct", "fields": fields}).to_string();
            json!({
                "metaData": {
                    "id": "delta-arrow-reader-partition-parity",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": schema,
                    "partitionColumns": partition_columns,
                    "configuration": {},
                    "createdTime": 1587968585495_i64
                }
            })
            .to_string()
        }

        fn add(path: &str, partition_values: Value) -> String {
            json!({
                "add": {
                    "path": path,
                    "partitionValues": partition_values,
                    "size": 0,
                    "modificationTime": 1587968586000_i64,
                    "dataChange": true
                }
            })
            .to_string()
        }

        fn compare(column: &str, op: DeltaComparison, value: DeltaScalar) -> DeltaPredicate {
            DeltaPredicate::Compare {
                column: column.to_owned(),
                op,
                value,
            }
        }

        fn is_null(column: &str) -> DeltaPredicate {
            DeltaPredicate::IsNull {
                column: column.to_owned(),
            }
        }

        fn is_not_null(column: &str) -> DeltaPredicate {
            DeltaPredicate::IsNotNull {
                column: column.to_owned(),
            }
        }

        fn and(left: DeltaPredicate, right: DeltaPredicate) -> DeltaPredicate {
            DeltaPredicate::And(vec![left, right])
        }

        fn or(left: DeltaPredicate, right: DeltaPredicate) -> DeltaPredicate {
            DeltaPredicate::Or(vec![left, right])
        }

        fn not(predicate: DeltaPredicate) -> DeltaPredicate {
            DeltaPredicate::Not(Box::new(predicate))
        }

        fn in_list(column: &str, values: Vec<DeltaScalar>) -> DeltaPredicate {
            DeltaPredicate::Or(
                values
                    .into_iter()
                    .map(|value| compare(column, DeltaComparison::Eq, value))
                    .collect(),
            )
        }

        fn not_in_list(column: &str, values: Vec<DeltaScalar>) -> DeltaPredicate {
            DeltaPredicate::And(
                values
                    .into_iter()
                    .map(|value| compare(column, DeltaComparison::NotEq, value))
                    .collect(),
            )
        }

        fn between(column: &str, low: DeltaScalar, high: DeltaScalar) -> DeltaPredicate {
            and(
                compare(column, DeltaComparison::GtEq, low),
                compare(column, DeltaComparison::LtEq, high),
            )
        }

        fn not_between(column: &str, low: DeltaScalar, high: DeltaScalar) -> DeltaPredicate {
            or(
                compare(column, DeltaComparison::Lt, low),
                compare(column, DeltaComparison::Gt, high),
            )
        }

        fn decimal(value: i128) -> DeltaScalar {
            DeltaScalar::Decimal128 {
                value,
                precision: 10,
                scale: 2,
            }
        }

        fn timestamp(value: i64, timezone: Option<&str>) -> DeltaScalar {
            DeltaScalar::TimestampMicrosecond {
                value,
                timezone: timezone.map(str::to_owned),
            }
        }

        fn kernel_column(name: &str) -> Expression {
            Expression::Column(ColumnName::new([name]))
        }

        fn kernel_compare(column: &str, op: DeltaComparison, value: Scalar) -> Predicate {
            let column = kernel_column(column);
            let value = Expression::Literal(value);
            match op {
                DeltaComparison::Eq => Predicate::eq(column, value),
                DeltaComparison::NotEq => Predicate::ne(column, value),
                DeltaComparison::Lt => Predicate::lt(column, value),
                DeltaComparison::LtEq => Predicate::le(column, value),
                DeltaComparison::Gt => Predicate::gt(column, value),
                DeltaComparison::GtEq => Predicate::ge(column, value),
            }
        }

        fn assert_invalid_partition(error: DeltaReaderError, secret: &str) {
            assert_eq!(error.code(), "scan_planning");
            assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
            assert!(!error.to_string().contains(secret));
            assert!(!format!("{error:?}").contains(secret));
        }

        macro_rules! assert_paths {
    ($fixture:expr, $predicate:expr, [$($path:literal),* $(,)?]) => {
        let expected: Vec<String> = vec![$($path.to_owned()),*];
        assert_eq!(
            $fixture.selected_paths(&$predicate)?,
            expected
        );
    };
}

        macro_rules! assert_kernel_paths {
    ($fixture:expr, $predicate:expr, [$($path:literal),* $(,)?]) => {
        assert_eq!(
            $fixture.selected_kernel_paths($predicate)?,
            vec![$($path.to_owned()),*]
        );
    };
}

        #[test]
        fn string_partition_pruning_matches_the_frozen_characterization()
        -> Result<(), Box<dyn std::error::Error>> {
            let fixture = PartitionFixture::new(
                "string-partition-parity",
                PROTOCOL_JSON,
                vec![field("id", "integer"), field("region", "string")],
                &["region"],
                vec![
                    add("region-us-west.parquet", json!({"region": "us-west"})),
                    add("region-us-east.parquet", json!({"region": "us-east"})),
                    add("region-null.parquet", json!({"region": null})),
                    add("region-missing.parquet", json!({})),
                    add("region-empty-string.parquet", json!({"region": ""})),
                ],
            )?;

            assert_eq!(
                fixture.all_paths()?,
                [
                    "region-empty-string.parquet",
                    "region-missing.parquet",
                    "region-null.parquet",
                    "region-us-east.parquet",
                    "region-us-west.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_null("region"),
                [
                    "region-empty-string.parquet",
                    "region-missing.parquet",
                    "region-null.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_not_null("region"),
                ["region-us-east.parquet", "region-us-west.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "region",
                    DeltaComparison::Eq,
                    DeltaScalar::Utf8("us-west".to_owned())
                ),
                ["region-us-west.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "region",
                    DeltaComparison::NotEq,
                    DeltaScalar::Utf8("us-west".to_owned())
                ),
                ["region-us-east.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "region",
                    DeltaComparison::Eq,
                    DeltaScalar::Utf8(String::new())
                ),
                []
            );
            assert_paths!(
                fixture,
                compare(
                    "region",
                    DeltaComparison::NotEq,
                    DeltaScalar::Utf8(String::new())
                ),
                ["region-us-east.parquet", "region-us-west.parquet"]
            );
            assert_paths!(
                fixture,
                in_list(
                    "region",
                    vec![
                        DeltaScalar::Utf8("us-west".to_owned()),
                        DeltaScalar::Utf8(String::new()),
                    ]
                ),
                ["region-us-west.parquet"]
            );
            assert_paths!(
                fixture,
                not_in_list(
                    "region",
                    vec![
                        DeltaScalar::Utf8("us-west".to_owned()),
                        DeltaScalar::Utf8(String::new()),
                    ]
                ),
                ["region-us-east.parquet"]
            );
            Ok(())
        }

        #[test]
        fn boolean_partition_pruning_matches_the_frozen_characterization()
        -> Result<(), Box<dyn std::error::Error>> {
            let fixture = PartitionFixture::new(
                "boolean-partition-parity",
                PROTOCOL_JSON,
                vec![field("id", "integer"), field("is_current", "boolean")],
                &["is_current"],
                vec![
                    add("boolean-true.parquet", json!({"is_current": "true"})),
                    add("boolean-false.parquet", json!({"is_current": "false"})),
                    add("boolean-null.parquet", json!({"is_current": null})),
                    add("boolean-empty.parquet", json!({"is_current": ""})),
                    add("boolean-missing.parquet", json!({})),
                ],
            )?;

            assert_eq!(
                fixture.all_paths()?,
                [
                    "boolean-empty.parquet",
                    "boolean-false.parquet",
                    "boolean-missing.parquet",
                    "boolean-null.parquet",
                    "boolean-true.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_null("is_current"),
                [
                    "boolean-empty.parquet",
                    "boolean-missing.parquet",
                    "boolean-null.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_not_null("is_current"),
                ["boolean-false.parquet", "boolean-true.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "is_current",
                    DeltaComparison::Eq,
                    DeltaScalar::Boolean(true)
                ),
                ["boolean-true.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "is_current",
                    DeltaComparison::Eq,
                    DeltaScalar::Boolean(false)
                ),
                ["boolean-false.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "is_current",
                    DeltaComparison::NotEq,
                    DeltaScalar::Boolean(true)
                ),
                ["boolean-false.parquet"]
            );
            assert_paths!(
                fixture,
                in_list(
                    "is_current",
                    vec![DeltaScalar::Boolean(true), DeltaScalar::Boolean(false)]
                ),
                ["boolean-false.parquet", "boolean-true.parquet"]
            );
            assert_paths!(
                fixture,
                not_in_list("is_current", vec![DeltaScalar::Boolean(true)]),
                ["boolean-false.parquet"]
            );
            assert_paths!(
                fixture,
                or(
                    compare(
                        "is_current",
                        DeltaComparison::Eq,
                        DeltaScalar::Boolean(true)
                    ),
                    is_null("is_current")
                ),
                [
                    "boolean-empty.parquet",
                    "boolean-missing.parquet",
                    "boolean-null.parquet",
                    "boolean-true.parquet",
                ]
            );
            assert_paths!(
                fixture,
                and(
                    compare(
                        "is_current",
                        DeltaComparison::Eq,
                        DeltaScalar::Boolean(true)
                    ),
                    is_not_null("is_current")
                ),
                ["boolean-true.parquet"]
            );
            assert_paths!(
                fixture,
                not(compare(
                    "is_current",
                    DeltaComparison::Eq,
                    DeltaScalar::Boolean(true)
                )),
                ["boolean-false.parquet"]
            );

            let invalid = PartitionFixture::new(
                "invalid-boolean-partition-parity",
                PROTOCOL_JSON,
                vec![field("id", "integer"), field("is_current", "boolean")],
                &["is_current"],
                vec![
                    add("boolean-valid.parquet", json!({"is_current": "true"})),
                    add(
                        "boolean-invalid.parquet",
                        json!({"is_current": "not-a-boolean"}),
                    ),
                ],
            )?;
            assert_invalid_partition(invalid.all_paths().unwrap_err(), "not-a-boolean");
            assert_invalid_partition(
                invalid
                    .selected_paths(&compare(
                        "is_current",
                        DeltaComparison::Eq,
                        DeltaScalar::Boolean(true),
                    ))
                    .unwrap_err(),
                "not-a-boolean",
            );
            Ok(())
        }

        #[test]
        fn date_partition_pruning_matches_the_frozen_characterization()
        -> Result<(), Box<dyn std::error::Error>> {
            let fixture = PartitionFixture::new(
                "date-partition-parity",
                PROTOCOL_JSON,
                vec![field("id", "integer"), field("event_date", "date")],
                &["event_date"],
                vec![
                    add(
                        "date-pre-epoch.parquet",
                        json!({"event_date": "1969-12-31"}),
                    ),
                    add("date-epoch.parquet", json!({"event_date": "1970-01-01"})),
                    add("date-leap-day.parquet", json!({"event_date": "2024-02-29"})),
                    add("date-new-year.parquet", json!({"event_date": "2026-01-01"})),
                    add("date-null.parquet", json!({"event_date": null})),
                    add("date-empty.parquet", json!({"event_date": ""})),
                    add("date-missing.parquet", json!({})),
                ],
            )?;

            assert_eq!(
                fixture.all_paths()?,
                [
                    "date-empty.parquet",
                    "date-epoch.parquet",
                    "date-leap-day.parquet",
                    "date-missing.parquet",
                    "date-new-year.parquet",
                    "date-null.parquet",
                    "date-pre-epoch.parquet",
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "event_date",
                    DeltaComparison::Gt,
                    DeltaScalar::Date32(19_782)
                ),
                ["date-new-year.parquet"]
            );
            assert_paths!(
                fixture,
                compare("event_date", DeltaComparison::LtEq, DeltaScalar::Date32(0)),
                ["date-epoch.parquet", "date-pre-epoch.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "event_date",
                    DeltaComparison::Lt,
                    DeltaScalar::Date32(20_454)
                ),
                [
                    "date-epoch.parquet",
                    "date-leap-day.parquet",
                    "date-pre-epoch.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_null("event_date"),
                [
                    "date-empty.parquet",
                    "date-missing.parquet",
                    "date-null.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_not_null("event_date"),
                [
                    "date-epoch.parquet",
                    "date-leap-day.parquet",
                    "date-new-year.parquet",
                    "date-pre-epoch.parquet",
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "event_date",
                    DeltaComparison::Eq,
                    DeltaScalar::Date32(20_454)
                ),
                ["date-new-year.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "event_date",
                    DeltaComparison::NotEq,
                    DeltaScalar::Date32(20_454)
                ),
                [
                    "date-epoch.parquet",
                    "date-leap-day.parquet",
                    "date-pre-epoch.parquet",
                ]
            );
            assert_paths!(
                fixture,
                in_list(
                    "event_date",
                    vec![DeltaScalar::Date32(-1), DeltaScalar::Date32(20_454)]
                ),
                ["date-new-year.parquet", "date-pre-epoch.parquet"]
            );
            assert_paths!(
                fixture,
                not_in_list("event_date", vec![DeltaScalar::Date32(19_782)]),
                [
                    "date-epoch.parquet",
                    "date-new-year.parquet",
                    "date-pre-epoch.parquet",
                ]
            );
            assert_paths!(
                fixture,
                between(
                    "event_date",
                    DeltaScalar::Date32(-1),
                    DeltaScalar::Date32(19_782)
                ),
                [
                    "date-epoch.parquet",
                    "date-leap-day.parquet",
                    "date-pre-epoch.parquet",
                ]
            );
            assert_paths!(
                fixture,
                not_between(
                    "event_date",
                    DeltaScalar::Date32(-1),
                    DeltaScalar::Date32(19_782)
                ),
                ["date-new-year.parquet"]
            );
            assert_paths!(
                fixture,
                and(
                    compare("event_date", DeltaComparison::GtEq, DeltaScalar::Date32(0)),
                    compare(
                        "event_date",
                        DeltaComparison::Lt,
                        DeltaScalar::Date32(20_454)
                    )
                ),
                ["date-epoch.parquet", "date-leap-day.parquet"]
            );
            assert_paths!(
                fixture,
                or(
                    compare("event_date", DeltaComparison::Eq, DeltaScalar::Date32(-1)),
                    compare(
                        "event_date",
                        DeltaComparison::Eq,
                        DeltaScalar::Date32(20_454)
                    )
                ),
                ["date-new-year.parquet", "date-pre-epoch.parquet"]
            );
            assert_paths!(
                fixture,
                not(compare(
                    "event_date",
                    DeltaComparison::Eq,
                    DeltaScalar::Date32(19_782)
                )),
                [
                    "date-epoch.parquet",
                    "date-new-year.parquet",
                    "date-pre-epoch.parquet",
                ]
            );

            let invalid = PartitionFixture::new(
                "invalid-date-partition-parity",
                PROTOCOL_JSON,
                vec![field("id", "integer"), field("event_date", "date")],
                &["event_date"],
                vec![
                    add("date-valid.parquet", json!({"event_date": "2026-01-01"})),
                    add("date-invalid.parquet", json!({"event_date": "not-a-date"})),
                ],
            )?;
            assert_invalid_partition(invalid.all_paths().unwrap_err(), "not-a-date");
            assert_invalid_partition(
                invalid
                    .selected_paths(&compare(
                        "event_date",
                        DeltaComparison::Eq,
                        DeltaScalar::Date32(20_454),
                    ))
                    .unwrap_err(),
                "not-a-date",
            );
            Ok(())
        }

        #[test]
        fn decimal_partition_pruning_matches_the_frozen_characterization()
        -> Result<(), Box<dyn std::error::Error>> {
            let fields = vec![field("id", "integer"), field("amount", "decimal(10,2)")];
            let fixture = PartitionFixture::new(
                "decimal-partition-parity",
                PROTOCOL_JSON,
                fields.clone(),
                &["amount"],
                vec![
                    add("decimal-negative.parquet", json!({"amount": "-1.23"})),
                    add("decimal-zero.parquet", json!({"amount": "0.00"})),
                    add("decimal-two.parquet", json!({"amount": "2.00"})),
                    add("decimal-ten.parquet", json!({"amount": "10.00"})),
                    add("decimal-large.parquet", json!({"amount": "123.45"})),
                    add("decimal-null.parquet", json!({"amount": null})),
                    add("decimal-empty.parquet", json!({"amount": ""})),
                    add("decimal-missing.parquet", json!({})),
                ],
            )?;

            assert_eq!(
                fixture.all_paths()?,
                [
                    "decimal-empty.parquet",
                    "decimal-large.parquet",
                    "decimal-missing.parquet",
                    "decimal-negative.parquet",
                    "decimal-null.parquet",
                    "decimal-ten.parquet",
                    "decimal-two.parquet",
                    "decimal-zero.parquet",
                ]
            );
            assert_paths!(
                fixture,
                compare("amount", DeltaComparison::Gt, decimal(200)),
                ["decimal-large.parquet", "decimal-ten.parquet"]
            );
            assert_paths!(
                fixture,
                compare("amount", DeltaComparison::Lt, decimal(1_000)),
                [
                    "decimal-negative.parquet",
                    "decimal-two.parquet",
                    "decimal-zero.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_null("amount"),
                [
                    "decimal-empty.parquet",
                    "decimal-missing.parquet",
                    "decimal-null.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_not_null("amount"),
                [
                    "decimal-large.parquet",
                    "decimal-negative.parquet",
                    "decimal-ten.parquet",
                    "decimal-two.parquet",
                    "decimal-zero.parquet",
                ]
            );
            assert_paths!(
                fixture,
                compare("amount", DeltaComparison::Eq, decimal(12_345)),
                ["decimal-large.parquet"]
            );
            assert_paths!(
                fixture,
                compare("amount", DeltaComparison::NotEq, decimal(12_345)),
                [
                    "decimal-negative.parquet",
                    "decimal-ten.parquet",
                    "decimal-two.parquet",
                    "decimal-zero.parquet",
                ]
            );
            assert_paths!(
                fixture,
                in_list("amount", vec![decimal(-123), decimal(12_345)]),
                ["decimal-large.parquet", "decimal-negative.parquet"]
            );
            assert_paths!(
                fixture,
                not_in_list("amount", vec![decimal(200)]),
                [
                    "decimal-large.parquet",
                    "decimal-negative.parquet",
                    "decimal-ten.parquet",
                    "decimal-zero.parquet",
                ]
            );
            assert_paths!(
                fixture,
                between("amount", decimal(-123), decimal(200)),
                [
                    "decimal-negative.parquet",
                    "decimal-two.parquet",
                    "decimal-zero.parquet",
                ]
            );
            assert_paths!(
                fixture,
                not_between("amount", decimal(-123), decimal(200)),
                ["decimal-large.parquet", "decimal-ten.parquet"]
            );
            assert_paths!(
                fixture,
                and(
                    compare("amount", DeltaComparison::GtEq, decimal(0)),
                    compare("amount", DeltaComparison::Lt, decimal(12_345))
                ),
                [
                    "decimal-ten.parquet",
                    "decimal-two.parquet",
                    "decimal-zero.parquet",
                ]
            );
            assert_paths!(
                fixture,
                or(
                    compare("amount", DeltaComparison::Eq, decimal(-123)),
                    compare("amount", DeltaComparison::Eq, decimal(12_345))
                ),
                ["decimal-large.parquet", "decimal-negative.parquet"]
            );
            assert_paths!(
                fixture,
                not(compare("amount", DeltaComparison::Eq, decimal(200))),
                [
                    "decimal-large.parquet",
                    "decimal-negative.parquet",
                    "decimal-ten.parquet",
                    "decimal-zero.parquet",
                ]
            );

            for (name, invalid_value) in [
                ("invalid-decimal-partition-parity", "not-a-decimal"),
                ("invalid-decimal-scale-partition-parity", "123.450"),
            ] {
                let invalid = PartitionFixture::new(
                    name,
                    PROTOCOL_JSON,
                    fields.clone(),
                    &["amount"],
                    vec![
                        add("decimal-valid.parquet", json!({"amount": "123.45"})),
                        add("decimal-invalid.parquet", json!({"amount": invalid_value})),
                    ],
                )?;
                if invalid_value == "not-a-decimal" {
                    assert_invalid_partition(invalid.all_paths().unwrap_err(), invalid_value);
                }
                assert_invalid_partition(
                    invalid
                        .selected_paths(&compare("amount", DeltaComparison::Eq, decimal(12_345)))
                        .unwrap_err(),
                    invalid_value,
                );
            }
            Ok(())
        }

        #[test]
        fn floating_partition_pruning_matches_the_frozen_characterization()
        -> Result<(), Box<dyn std::error::Error>> {
            let fields = vec![
                field("id", "integer"),
                field("float_part", "float"),
                field("double_part", "double"),
            ];
            let fixture = PartitionFixture::new(
                "floating-partition-parity",
                PROTOCOL_JSON,
                fields.clone(),
                &["float_part", "double_part"],
                vec![
                    add(
                        "floating-neg.parquet",
                        json!({"float_part": "-1.5", "double_part": "-2.25"}),
                    ),
                    add(
                        "floating-neg-zero.parquet",
                        json!({"float_part": "-0.0", "double_part": "-0.0"}),
                    ),
                    add(
                        "floating-pos-zero.parquet",
                        json!({"float_part": "0.0", "double_part": "0.0"}),
                    ),
                    add(
                        "floating-one.parquet",
                        json!({"float_part": "1.5", "double_part": "2.25"}),
                    ),
                    add(
                        "floating-ten.parquet",
                        json!({"float_part": "10.0", "double_part": "10.0"}),
                    ),
                    add(
                        "floating-null.parquet",
                        json!({"float_part": null, "double_part": null}),
                    ),
                    add(
                        "floating-empty.parquet",
                        json!({"float_part": "", "double_part": ""}),
                    ),
                    add("floating-missing.parquet", json!({})),
                ],
            )?;

            assert_eq!(
                fixture.all_paths()?,
                [
                    "floating-empty.parquet",
                    "floating-missing.parquet",
                    "floating-neg-zero.parquet",
                    "floating-neg.parquet",
                    "floating-null.parquet",
                    "floating-one.parquet",
                    "floating-pos-zero.parquet",
                    "floating-ten.parquet",
                ]
            );
            assert_paths!(
                fixture,
                compare("float_part", DeltaComparison::Gt, DeltaScalar::Float32(1.5)),
                ["floating-ten.parquet"]
            );
            assert_kernel_paths!(
                fixture,
                kernel_compare("double_part", DeltaComparison::Lt, Scalar::Double(0.0)),
                ["floating-neg-zero.parquet", "floating-neg.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "float_part",
                    DeltaComparison::Lt,
                    DeltaScalar::Float32(10.0)
                ),
                [
                    "floating-neg-zero.parquet",
                    "floating-neg.parquet",
                    "floating-one.parquet",
                    "floating-pos-zero.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_null("float_part"),
                [
                    "floating-empty.parquet",
                    "floating-missing.parquet",
                    "floating-null.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_not_null("double_part"),
                [
                    "floating-neg-zero.parquet",
                    "floating-neg.parquet",
                    "floating-one.parquet",
                    "floating-pos-zero.parquet",
                    "floating-ten.parquet",
                ]
            );
            assert_kernel_paths!(
                fixture,
                kernel_compare("float_part", DeltaComparison::Eq, Scalar::Float(-0.0)),
                ["floating-neg-zero.parquet"]
            );
            assert_kernel_paths!(
                fixture,
                kernel_compare("float_part", DeltaComparison::Eq, Scalar::Float(0.0)),
                ["floating-pos-zero.parquet"]
            );
            assert_kernel_paths!(
                fixture,
                kernel_compare("double_part", DeltaComparison::NotEq, Scalar::Double(0.0)),
                [
                    "floating-neg-zero.parquet",
                    "floating-neg.parquet",
                    "floating-one.parquet",
                    "floating-ten.parquet",
                ]
            );
            assert_paths!(
                fixture,
                in_list(
                    "float_part",
                    vec![DeltaScalar::Float32(-1.5), DeltaScalar::Float32(1.5)]
                ),
                ["floating-neg.parquet", "floating-one.parquet"]
            );
            assert_paths!(
                fixture,
                not_in_list("double_part", vec![DeltaScalar::Float64(2.25)]),
                [
                    "floating-neg-zero.parquet",
                    "floating-neg.parquet",
                    "floating-pos-zero.parquet",
                    "floating-ten.parquet",
                ]
            );
            assert_kernel_paths!(
                fixture,
                Predicate::and(
                    kernel_compare("float_part", DeltaComparison::GtEq, Scalar::Float(-0.0)),
                    kernel_compare("float_part", DeltaComparison::LtEq, Scalar::Float(1.5)),
                ),
                [
                    "floating-neg-zero.parquet",
                    "floating-one.parquet",
                    "floating-pos-zero.parquet",
                ]
            );
            assert_kernel_paths!(
                fixture,
                Predicate::or(
                    kernel_compare("double_part", DeltaComparison::Lt, Scalar::Double(0.0)),
                    kernel_compare("double_part", DeltaComparison::Gt, Scalar::Double(2.25)),
                ),
                [
                    "floating-neg-zero.parquet",
                    "floating-neg.parquet",
                    "floating-ten.parquet",
                ]
            );
            assert_kernel_paths!(
                fixture,
                Predicate::and(
                    kernel_compare("float_part", DeltaComparison::GtEq, Scalar::Float(0.0)),
                    kernel_compare("float_part", DeltaComparison::Lt, Scalar::Float(10.0)),
                ),
                ["floating-one.parquet", "floating-pos-zero.parquet"]
            );
            assert_paths!(
                fixture,
                or(
                    compare(
                        "double_part",
                        DeltaComparison::Eq,
                        DeltaScalar::Float64(-2.25)
                    ),
                    compare(
                        "double_part",
                        DeltaComparison::Eq,
                        DeltaScalar::Float64(10.0)
                    )
                ),
                ["floating-neg.parquet", "floating-ten.parquet"]
            );
            assert_paths!(
                fixture,
                not(compare(
                    "float_part",
                    DeltaComparison::Eq,
                    DeltaScalar::Float32(1.5)
                )),
                [
                    "floating-neg-zero.parquet",
                    "floating-neg.parquet",
                    "floating-pos-zero.parquet",
                    "floating-ten.parquet",
                ]
            );

            let invalid = PartitionFixture::new(
                "invalid-floating-partition-parity",
                PROTOCOL_JSON,
                fields.clone(),
                &["float_part", "double_part"],
                vec![
                    add(
                        "floating-valid.parquet",
                        json!({"float_part": "1.5", "double_part": "2.25"}),
                    ),
                    add(
                        "floating-invalid.parquet",
                        json!({"float_part": "not-a-float", "double_part": "not-a-double"}),
                    ),
                ],
            )?;
            assert_invalid_partition(invalid.all_paths().unwrap_err(), "not-a-float");
            assert_invalid_partition(
                invalid
                    .selected_paths(&compare(
                        "float_part",
                        DeltaComparison::Eq,
                        DeltaScalar::Float32(1.5),
                    ))
                    .unwrap_err(),
                "not-a-float",
            );

            let nonfinite = PartitionFixture::new(
                "nonfinite-floating-partition-parity",
                PROTOCOL_JSON,
                fields,
                &["float_part", "double_part"],
                vec![
                    add(
                        "floating-valid.parquet",
                        json!({"float_part": "1.5", "double_part": "2.25"}),
                    ),
                    add(
                        "floating-nan.parquet",
                        json!({"float_part": "NaN", "double_part": "NaN"}),
                    ),
                    add(
                        "floating-inf.parquet",
                        json!({"float_part": "Infinity", "double_part": "Infinity"}),
                    ),
                    add(
                        "floating-neg-inf.parquet",
                        json!({"float_part": "-Infinity", "double_part": "-Infinity"}),
                    ),
                ],
            )?;
            assert_eq!(
                nonfinite.all_paths()?,
                [
                    "floating-inf.parquet",
                    "floating-nan.parquet",
                    "floating-neg-inf.parquet",
                    "floating-valid.parquet",
                ]
            );
            assert_kernel_paths!(
                nonfinite,
                kernel_compare("float_part", DeltaComparison::Gt, Scalar::Float(0.0)),
                [
                    "floating-inf.parquet",
                    "floating-nan.parquet",
                    "floating-valid.parquet",
                ]
            );
            assert_kernel_paths!(
                nonfinite,
                kernel_compare("double_part", DeltaComparison::Lt, Scalar::Double(0.0)),
                ["floating-neg-inf.parquet"]
            );
            assert_kernel_paths!(
                nonfinite,
                kernel_compare("float_part", DeltaComparison::Eq, Scalar::Float(f32::NAN)),
                ["floating-nan.parquet"]
            );
            Ok(())
        }

        #[test]
        fn binary_partition_pruning_matches_the_frozen_characterization()
        -> Result<(), Box<dyn std::error::Error>> {
            let fixture = PartitionFixture::new(
                "binary-partition-parity",
                PROTOCOL_JSON,
                vec![field("id", "integer"), field("payload", "binary")],
                &["payload"],
                vec![
                    add("binary-HELLO.parquet", json!({"payload": "HELLO"})),
                    add("binary-hello.parquet", json!({"payload": "hello"})),
                    add("binary-special.parquet", json!({"payload": "/=%"})),
                    add("binary-null.parquet", json!({"payload": null})),
                    add("binary-empty.parquet", json!({"payload": ""})),
                    add("binary-missing.parquet", json!({})),
                ],
            )?;

            assert_eq!(
                fixture.all_paths()?,
                [
                    "binary-HELLO.parquet",
                    "binary-empty.parquet",
                    "binary-hello.parquet",
                    "binary-missing.parquet",
                    "binary-null.parquet",
                    "binary-special.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_null("payload"),
                [
                    "binary-empty.parquet",
                    "binary-missing.parquet",
                    "binary-null.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_not_null("payload"),
                [
                    "binary-HELLO.parquet",
                    "binary-hello.parquet",
                    "binary-special.parquet",
                ]
            );
            for (value, expected) in [
                (b"HELLO".to_vec(), vec!["binary-HELLO.parquet"]),
                (b"hello".to_vec(), vec!["binary-hello.parquet"]),
                (Vec::new(), vec![]),
            ] {
                assert_eq!(
                    fixture.selected_paths(&compare(
                        "payload",
                        DeltaComparison::Eq,
                        DeltaScalar::Binary(value),
                    ))?,
                    expected
                );
            }
            assert_paths!(
                fixture,
                compare(
                    "payload",
                    DeltaComparison::NotEq,
                    DeltaScalar::Binary(b"hello".to_vec())
                ),
                ["binary-HELLO.parquet", "binary-special.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "payload",
                    DeltaComparison::NotEq,
                    DeltaScalar::Binary(Vec::new())
                ),
                [
                    "binary-HELLO.parquet",
                    "binary-hello.parquet",
                    "binary-special.parquet",
                ]
            );
            assert_paths!(
                fixture,
                in_list(
                    "payload",
                    vec![
                        DeltaScalar::Binary(b"HELLO".to_vec()),
                        DeltaScalar::Binary(b"/=%".to_vec()),
                    ]
                ),
                ["binary-HELLO.parquet", "binary-special.parquet"]
            );
            assert_paths!(
                fixture,
                not_in_list(
                    "payload",
                    vec![
                        DeltaScalar::Binary(b"hello".to_vec()),
                        DeltaScalar::Binary(b"/=%".to_vec()),
                    ]
                ),
                ["binary-HELLO.parquet"]
            );
            assert_paths!(
                fixture,
                and(
                    is_not_null("payload"),
                    compare(
                        "payload",
                        DeltaComparison::NotEq,
                        DeltaScalar::Binary(b"HELLO".to_vec())
                    )
                ),
                ["binary-hello.parquet", "binary-special.parquet"]
            );
            assert_paths!(
                fixture,
                or(
                    compare(
                        "payload",
                        DeltaComparison::Eq,
                        DeltaScalar::Binary(b"HELLO".to_vec())
                    ),
                    is_null("payload")
                ),
                [
                    "binary-HELLO.parquet",
                    "binary-empty.parquet",
                    "binary-missing.parquet",
                    "binary-null.parquet",
                ]
            );
            assert_paths!(
                fixture,
                not(compare(
                    "payload",
                    DeltaComparison::Eq,
                    DeltaScalar::Binary(b"hello".to_vec())
                )),
                ["binary-HELLO.parquet", "binary-special.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "payload",
                    DeltaComparison::Eq,
                    DeltaScalar::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF])
                ),
                []
            );
            assert_paths!(
                fixture,
                compare(
                    "payload",
                    DeltaComparison::NotEq,
                    DeltaScalar::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF])
                ),
                [
                    "binary-HELLO.parquet",
                    "binary-hello.parquet",
                    "binary-special.parquet",
                ]
            );
            Ok(())
        }

        fn assert_timestamp_characterization(
            fixture: &PartitionFixture,
            column: &str,
            timezone: Option<&str>,
            paths: [&str; 7],
        ) -> Result<(), DeltaReaderError> {
            let [
                empty,
                high_path,
                low_path,
                missing,
                null,
                pre_epoch_path,
                target_path,
            ] = paths;
            let pre_epoch = -1_i64;
            let low = 1_767_225_599_999_999_i64;
            let target = 1_767_225_600_123_456_i64;
            let high = 1_767_225_600_123_457_i64;

            assert_eq!(
                fixture.all_paths()?,
                [
                    empty,
                    high_path,
                    low_path,
                    missing,
                    null,
                    pre_epoch_path,
                    target_path
                ]
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::Lt,
                    timestamp(target, timezone),
                ))?,
                [low_path, pre_epoch_path]
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::GtEq,
                    timestamp(target, timezone),
                ))?,
                [high_path, target_path]
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::Lt,
                    timestamp(high, timezone),
                ))?,
                [low_path, pre_epoch_path, target_path]
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::Eq,
                    timestamp(pre_epoch, timezone),
                ))?,
                [pre_epoch_path]
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::Eq,
                    timestamp(target, timezone),
                ))?,
                [target_path]
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::NotEq,
                    timestamp(target, timezone),
                ))?,
                [high_path, low_path, pre_epoch_path]
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::Eq,
                    timestamp(high, timezone),
                ))?,
                [high_path]
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::Eq,
                    timestamp(low, timezone),
                ))?,
                [low_path]
            );
            assert_eq!(
                fixture.selected_paths(&is_null(column))?,
                [empty, missing, null]
            );
            assert_eq!(
                fixture.selected_paths(&is_not_null(column))?,
                [high_path, low_path, pre_epoch_path, target_path]
            );
            assert_eq!(
                fixture.selected_paths(&in_list(
                    column,
                    vec![timestamp(low, timezone), timestamp(target, timezone)],
                ))?,
                [low_path, target_path]
            );
            assert_eq!(
                fixture.selected_paths(&not_in_list(
                    column,
                    vec![timestamp(low, timezone), timestamp(target, timezone)],
                ))?,
                [high_path, pre_epoch_path]
            );
            assert_eq!(
                fixture.selected_paths(&between(
                    column,
                    timestamp(low, timezone),
                    timestamp(target, timezone),
                ))?,
                [low_path, target_path]
            );
            assert_eq!(
                fixture.selected_paths(&not_between(
                    column,
                    timestamp(low, timezone),
                    timestamp(target, timezone),
                ))?,
                [high_path, pre_epoch_path]
            );
            assert_eq!(
                fixture.selected_paths(&and(
                    compare(column, DeltaComparison::Gt, timestamp(low, timezone)),
                    compare(column, DeltaComparison::LtEq, timestamp(high, timezone)),
                ))?,
                [high_path, target_path]
            );
            assert_eq!(
                fixture.selected_paths(&or(
                    compare(column, DeltaComparison::Eq, timestamp(low, timezone)),
                    is_null(column),
                ))?,
                [empty, low_path, missing, null]
            );
            assert_eq!(
                fixture.selected_paths(&not(compare(
                    column,
                    DeltaComparison::Eq,
                    timestamp(target, timezone),
                )))?,
                [high_path, low_path, pre_epoch_path]
            );
            Ok(())
        }

        #[test]
        fn timestamp_partition_pruning_matches_the_frozen_characterization()
        -> Result<(), Box<dyn std::error::Error>> {
            let fields = vec![field("id", "integer"), field("event_ts", "timestamp")];
            let fixture = PartitionFixture::new(
                "timestamp-partition-parity",
                PROTOCOL_JSON,
                fields.clone(),
                &["event_ts"],
                vec![
                    add(
                        "timestamp-pre-epoch.parquet",
                        json!({"event_ts": "1969-12-31T23:59:59.999999Z"}),
                    ),
                    add(
                        "timestamp-low.parquet",
                        json!({"event_ts": "2025-12-31T23:59:59.999999Z"}),
                    ),
                    add(
                        "timestamp-target.parquet",
                        json!({"event_ts": "2026-01-01T00:00:00.123456Z"}),
                    ),
                    add(
                        "timestamp-high.parquet",
                        json!({"event_ts": "2026-01-01T00:00:00.123457Z"}),
                    ),
                    add("timestamp-null.parquet", json!({"event_ts": null})),
                    add("timestamp-empty.parquet", json!({"event_ts": ""})),
                    add("timestamp-missing.parquet", json!({})),
                ],
            )?;
            assert_timestamp_characterization(
                &fixture,
                "event_ts",
                Some("UTC"),
                [
                    "timestamp-empty.parquet",
                    "timestamp-high.parquet",
                    "timestamp-low.parquet",
                    "timestamp-missing.parquet",
                    "timestamp-null.parquet",
                    "timestamp-pre-epoch.parquet",
                    "timestamp-target.parquet",
                ],
            )?;

            let invalid = PartitionFixture::new(
                "invalid-timestamp-partition-parity",
                PROTOCOL_JSON,
                fields,
                &["event_ts"],
                vec![
                    add(
                        "timestamp-valid.parquet",
                        json!({"event_ts": "2026-01-01T00:00:00.123456Z"}),
                    ),
                    add(
                        "timestamp-invalid.parquet",
                        json!({"event_ts": "not-a-timestamp"}),
                    ),
                ],
            )?;
            assert_invalid_partition(invalid.all_paths().unwrap_err(), "not-a-timestamp");
            assert_invalid_partition(
                invalid
                    .selected_paths(&compare(
                        "event_ts",
                        DeltaComparison::Eq,
                        timestamp(1_767_225_600_123_456, Some("UTC")),
                    ))
                    .unwrap_err(),
                "not-a-timestamp",
            );
            Ok(())
        }

        #[test]
        fn timestamp_ntz_partition_pruning_matches_the_frozen_characterization()
        -> Result<(), Box<dyn std::error::Error>> {
            let fields = vec![
                field("id", "integer"),
                field("event_ts_ntz", "timestamp_ntz"),
            ];
            let fixture = PartitionFixture::new(
                "timestamp-ntz-partition-parity",
                TIMESTAMP_NTZ_PROTOCOL_JSON,
                fields.clone(),
                &["event_ts_ntz"],
                vec![
                    add(
                        "timestamp-ntz-pre-epoch.parquet",
                        json!({"event_ts_ntz": "1969-12-31 23:59:59.999999"}),
                    ),
                    add(
                        "timestamp-ntz-low-space.parquet",
                        json!({"event_ts_ntz": "2025-12-31 23:59:59.999999"}),
                    ),
                    add(
                        "timestamp-ntz-target-space.parquet",
                        json!({"event_ts_ntz": "2026-01-01 00:00:00.123456"}),
                    ),
                    add(
                        "timestamp-ntz-high.parquet",
                        json!({"event_ts_ntz": "2026-01-01 00:00:00.123457"}),
                    ),
                    add("timestamp-ntz-null.parquet", json!({"event_ts_ntz": null})),
                    add("timestamp-ntz-empty.parquet", json!({"event_ts_ntz": ""})),
                    add("timestamp-ntz-missing.parquet", json!({})),
                ],
            )?;
            assert_timestamp_characterization(
                &fixture,
                "event_ts_ntz",
                None,
                [
                    "timestamp-ntz-empty.parquet",
                    "timestamp-ntz-high.parquet",
                    "timestamp-ntz-low-space.parquet",
                    "timestamp-ntz-missing.parquet",
                    "timestamp-ntz-null.parquet",
                    "timestamp-ntz-pre-epoch.parquet",
                    "timestamp-ntz-target-space.parquet",
                ],
            )?;

            for (name, invalid_value) in [
                ("invalid-timestamp-ntz-partition-parity", "not-a-timestamp"),
                (
                    "t-separator-timestamp-ntz-partition-parity",
                    "2026-01-01T00:00:00.123456",
                ),
                (
                    "zone-timestamp-ntz-partition-parity",
                    "2026-01-01T00:00:00.123456Z",
                ),
            ] {
                let invalid = PartitionFixture::new(
                    name,
                    TIMESTAMP_NTZ_PROTOCOL_JSON,
                    fields.clone(),
                    &["event_ts_ntz"],
                    vec![
                        add(
                            "timestamp-ntz-valid.parquet",
                            json!({"event_ts_ntz": "2026-01-01 00:00:00.123456"}),
                        ),
                        add(
                            "timestamp-ntz-invalid.parquet",
                            json!({"event_ts_ntz": invalid_value}),
                        ),
                    ],
                )?;
                assert_invalid_partition(invalid.all_paths().unwrap_err(), invalid_value);
                assert_invalid_partition(
                    invalid
                        .selected_paths(&compare(
                            "event_ts_ntz",
                            DeltaComparison::Eq,
                            timestamp(1_767_225_600_123_456, None),
                        ))
                        .unwrap_err(),
                    invalid_value,
                );
            }
            Ok(())
        }

        #[test]
        fn integer_partition_pruning_matches_the_frozen_characterization()
        -> Result<(), Box<dyn std::error::Error>> {
            let fields = vec![
                field("id", "integer"),
                field("byte_part", "byte"),
                field("short_part", "short"),
                field("int_part", "integer"),
                field("long_part", "long"),
            ];
            let integer_add = |path: &str, value: &str| {
                add(
                    path,
                    json!({
                        "byte_part": value,
                        "short_part": value,
                        "int_part": value,
                        "long_part": value,
                    }),
                )
            };
            let fixture = PartitionFixture::new(
                "integer-partition-parity",
                PROTOCOL_JSON,
                fields.clone(),
                &["byte_part", "short_part", "int_part", "long_part"],
                vec![
                    integer_add("integer--1.parquet", "-1"),
                    integer_add("integer-2.parquet", "2"),
                    integer_add("integer-10.parquet", "10"),
                    add(
                        "integer-null.parquet",
                        json!({
                            "byte_part": null,
                            "short_part": null,
                            "int_part": null,
                            "long_part": null,
                        }),
                    ),
                    integer_add("integer-empty.parquet", ""),
                    add("integer-missing.parquet", json!({})),
                ],
            )?;

            assert_eq!(
                fixture.all_paths()?,
                [
                    "integer--1.parquet",
                    "integer-10.parquet",
                    "integer-2.parquet",
                    "integer-empty.parquet",
                    "integer-missing.parquet",
                    "integer-null.parquet",
                ]
            );
            assert_paths!(
                fixture,
                compare("long_part", DeltaComparison::Gt, DeltaScalar::Int64(2)),
                ["integer-10.parquet"]
            );
            assert_paths!(
                fixture,
                compare("long_part", DeltaComparison::Lt, DeltaScalar::Int64(10)),
                ["integer--1.parquet", "integer-2.parquet"]
            );
            assert_paths!(
                fixture,
                is_null("long_part"),
                [
                    "integer-empty.parquet",
                    "integer-missing.parquet",
                    "integer-null.parquet",
                ]
            );
            assert_paths!(
                fixture,
                is_not_null("long_part"),
                [
                    "integer--1.parquet",
                    "integer-10.parquet",
                    "integer-2.parquet",
                ]
            );
            assert_paths!(
                fixture,
                compare("long_part", DeltaComparison::Eq, DeltaScalar::Int64(2)),
                ["integer-2.parquet"]
            );
            assert_paths!(
                fixture,
                compare("long_part", DeltaComparison::NotEq, DeltaScalar::Int64(2)),
                ["integer--1.parquet", "integer-10.parquet"]
            );
            assert_paths!(
                fixture,
                in_list(
                    "long_part",
                    vec![DeltaScalar::Int64(-1), DeltaScalar::Int64(10)]
                ),
                ["integer--1.parquet", "integer-10.parquet"]
            );
            assert_paths!(
                fixture,
                not_in_list("long_part", vec![DeltaScalar::Int64(2)]),
                ["integer--1.parquet", "integer-10.parquet"]
            );
            assert_paths!(
                fixture,
                between("long_part", DeltaScalar::Int64(-1), DeltaScalar::Int64(2)),
                ["integer--1.parquet", "integer-2.parquet"]
            );
            assert_paths!(
                fixture,
                not_between("long_part", DeltaScalar::Int64(-1), DeltaScalar::Int64(2)),
                ["integer-10.parquet"]
            );
            assert_paths!(
                fixture,
                and(
                    compare("long_part", DeltaComparison::GtEq, DeltaScalar::Int64(-1)),
                    compare("long_part", DeltaComparison::Lt, DeltaScalar::Int64(10))
                ),
                ["integer--1.parquet", "integer-2.parquet"]
            );
            assert_paths!(
                fixture,
                or(
                    compare("long_part", DeltaComparison::Eq, DeltaScalar::Int64(-1)),
                    compare("long_part", DeltaComparison::Eq, DeltaScalar::Int64(10))
                ),
                ["integer--1.parquet", "integer-10.parquet"]
            );
            assert_paths!(
                fixture,
                not(compare(
                    "long_part",
                    DeltaComparison::Eq,
                    DeltaScalar::Int64(2)
                )),
                ["integer--1.parquet", "integer-10.parquet"]
            );

            let widths = PartitionFixture::new(
                "integer-width-partition-parity",
                PROTOCOL_JSON,
                fields.clone(),
                &["byte_part", "short_part", "int_part", "long_part"],
                vec![
                    add(
                        "width-byte-min.parquet",
                        json!({"byte_part": "-128", "short_part": "0", "int_part": "0", "long_part": "0"}),
                    ),
                    add(
                        "width-short-max.parquet",
                        json!({"byte_part": "0", "short_part": "32767", "int_part": "0", "long_part": "0"}),
                    ),
                    add(
                        "width-int-max.parquet",
                        json!({"byte_part": "0", "short_part": "0", "int_part": "2147483647", "long_part": "0"}),
                    ),
                    add(
                        "width-long-max.parquet",
                        json!({"byte_part": "0", "short_part": "0", "int_part": "0", "long_part": "9223372036854775807"}),
                    ),
                ],
            )?;
            assert_paths!(
                widths,
                compare("byte_part", DeltaComparison::Eq, DeltaScalar::Int8(i8::MIN)),
                ["width-byte-min.parquet"]
            );
            assert_paths!(
                widths,
                compare(
                    "short_part",
                    DeltaComparison::Eq,
                    DeltaScalar::Int16(i16::MAX)
                ),
                ["width-short-max.parquet"]
            );
            assert_paths!(
                widths,
                compare(
                    "int_part",
                    DeltaComparison::Eq,
                    DeltaScalar::Int32(i32::MAX)
                ),
                ["width-int-max.parquet"]
            );
            assert_paths!(
                widths,
                compare(
                    "long_part",
                    DeltaComparison::Eq,
                    DeltaScalar::Int64(i64::MAX)
                ),
                ["width-long-max.parquet"]
            );

            let invalid = PartitionFixture::new(
                "invalid-integer-partition-parity",
                PROTOCOL_JSON,
                fields,
                &["byte_part", "short_part", "int_part", "long_part"],
                vec![
                    integer_add("integer-valid.parquet", "7"),
                    add(
                        "integer-invalid.parquet",
                        json!({
                            "byte_part": "0",
                            "short_part": "0",
                            "int_part": "0",
                            "long_part": "not-an-integer",
                        }),
                    ),
                ],
            )?;
            assert_invalid_partition(invalid.all_paths().unwrap_err(), "not-an-integer");
            assert_invalid_partition(
                invalid
                    .selected_paths(&compare(
                        "long_part",
                        DeltaComparison::Eq,
                        DeltaScalar::Int64(7),
                    ))
                    .unwrap_err(),
                "not-an-integer",
            );
            Ok(())
        }
    }

    mod statistics_pruning {
        use serde_json::{Map, Value, json};

        use super::{DeltaLogTable, PROTOCOL_JSON, plan_scan, planned_tasks};
        use crate::{
            DeltaComparison, DeltaPredicate, DeltaScalar, DeltaScanExecutionOptions,
            DeltaSnapshotSelection, DeltaStorageOptions,
            delta::{
                kernel::delta_predicate_to_kernel_pruning,
                snapshot::{LoadedDeltaTableSnapshot, load_delta_table_snapshot_blocking},
            },
            reader::predicate::validate_predicate,
        };

        const TIMESTAMP_NTZ_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"],"writerFeatures":["timestampNtz"]}}"#;

        struct StatisticsFixture {
            _table: DeltaLogTable,
            snapshot: LoadedDeltaTableSnapshot,
        }

        impl StatisticsFixture {
            fn new(
                name: &str,
                protocol: &str,
                fields: Vec<Value>,
                adds: Vec<String>,
            ) -> Result<Self, Box<dyn std::error::Error>> {
                let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
                    name,
                    protocol,
                    &metadata(fields),
                    &adds,
                )?;
                let snapshot = load_delta_table_snapshot_blocking(
                    &table.0.to_string_lossy(),
                    &DeltaStorageOptions::new(),
                    DeltaSnapshotSelection::Latest,
                )?;
                Ok(Self {
                    _table: table,
                    snapshot,
                })
            }

            fn selected_paths(
                &self,
                predicate: &DeltaPredicate,
            ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
                validate_predicate(predicate, self.snapshot.schema().as_ref())?;
                let kernel_predicate = delta_predicate_to_kernel_pruning(predicate)
                    .ok_or("predicate has no safe Kernel pruning representation")?;
                let plan = plan_scan(
                    &self.snapshot,
                    None,
                    &[],
                    Some(kernel_predicate),
                    true,
                    DeltaScanExecutionOptions::default(),
                )?;
                let mut paths = planned_tasks(&plan)
                    .map(|task| task.path.clone())
                    .collect::<Vec<_>>();
                paths.sort_unstable();
                Ok(paths)
            }
        }

        fn field(name: &str, data_type: &str) -> Value {
            json!({
                "name": name,
                "type": data_type,
                "nullable": true,
                "metadata": {}
            })
        }

        fn metadata(fields: Vec<Value>) -> String {
            let schema = json!({"type": "struct", "fields": fields}).to_string();
            json!({
                "metaData": {
                    "id": "delta-arrow-reader-statistics-parity",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": schema,
                    "partitionColumns": [],
                    "configuration": {},
                    "createdTime": 1587968585495_i64
                }
            })
            .to_string()
        }

        fn stats_add(
            path: &str,
            min_values: Option<Value>,
            max_values: Option<Value>,
            null_count: Option<Value>,
        ) -> String {
            let mut stats = Map::from_iter([("numRecords".to_owned(), json!(10))]);
            if let Some(values) = min_values {
                stats.insert("minValues".to_owned(), values);
            }
            if let Some(values) = max_values {
                stats.insert("maxValues".to_owned(), values);
            }
            if let Some(values) = null_count {
                stats.insert("nullCount".to_owned(), values);
            }
            json!({
                "add": {
                    "path": path,
                    "partitionValues": {},
                    "size": 10,
                    "modificationTime": 1587968586000_i64,
                    "dataChange": true,
                    "stats": Value::Object(stats).to_string()
                }
            })
            .to_string()
        }

        fn missing_stats_add(path: &str) -> String {
            json!({
                "add": {
                    "path": path,
                    "partitionValues": {},
                    "size": 10,
                    "modificationTime": 1587968586000_i64,
                    "dataChange": true
                }
            })
            .to_string()
        }

        fn compare(column: &str, op: DeltaComparison, value: DeltaScalar) -> DeltaPredicate {
            DeltaPredicate::Compare {
                column: column.to_owned(),
                op,
                value,
            }
        }

        fn is_null(column: &str) -> DeltaPredicate {
            DeltaPredicate::IsNull {
                column: column.to_owned(),
            }
        }

        fn is_not_null(column: &str) -> DeltaPredicate {
            DeltaPredicate::IsNotNull {
                column: column.to_owned(),
            }
        }

        fn decimal(value: i128) -> DeltaScalar {
            DeltaScalar::Decimal128 {
                value,
                precision: 10,
                scale: 2,
            }
        }

        fn timestamp(value: i64, timezone: Option<&str>) -> DeltaScalar {
            DeltaScalar::TimestampMicrosecond {
                value,
                timezone: timezone.map(str::to_owned),
            }
        }

        macro_rules! assert_paths {
    ($fixture:expr, $predicate:expr, [$($path:literal),* $(,)?]) => {
        let expected: Vec<String> = vec![$($path.to_owned()),*];
        assert_eq!(
            $fixture.selected_paths(&$predicate)?,
            expected
        );
    };
}

        #[test]
        fn integer_statistics_pruning_matches_the_frozen_selected_files()
        -> Result<(), Box<dyn std::error::Error>> {
            let fixture = StatisticsFixture::new(
                "integer-statistics-parity",
                PROTOCOL_JSON,
                vec![field("id", "integer")],
                vec![
                    stats_add(
                        "id-impossible.parquet",
                        Some(json!({"id": 1})),
                        Some(json!({"id": 50})),
                        Some(json!({"id": 0})),
                    ),
                    stats_add(
                        "id-possible.parquet",
                        Some(json!({"id": 101})),
                        Some(json!({"id": 150})),
                        Some(json!({"id": 0})),
                    ),
                    missing_stats_add("id-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                fixture,
                compare("id", DeltaComparison::Gt, DeltaScalar::Int32(100)),
                ["id-missing-stats.parquet", "id-possible.parquet"]
            );

            let all_low = StatisticsFixture::new(
                "integer-all-impossible-parity",
                PROTOCOL_JSON,
                vec![field("id", "integer")],
                vec![
                    stats_add(
                        "id-low-a.parquet",
                        Some(json!({"id": 1})),
                        Some(json!({"id": 50})),
                        Some(json!({"id": 0})),
                    ),
                    stats_add(
                        "id-low-b.parquet",
                        Some(json!({"id": 51})),
                        Some(json!({"id": 100})),
                        Some(json!({"id": 0})),
                    ),
                ],
            )?;
            assert_paths!(
                all_low,
                compare("id", DeltaComparison::Gt, DeltaScalar::Int32(100)),
                []
            );
            Ok(())
        }

        #[test]
        fn boolean_statistics_pruning_matches_complete_and_partial_frozen_boundaries()
        -> Result<(), Box<dyn std::error::Error>> {
            let fixture = StatisticsFixture::new(
                "boolean-statistics-parity",
                PROTOCOL_JSON,
                vec![field("is_current", "boolean")],
                vec![
                    stats_add(
                        "boolean-false-only.parquet",
                        Some(json!({"is_current": false})),
                        Some(json!({"is_current": false})),
                        Some(json!({"is_current": 0})),
                    ),
                    stats_add(
                        "boolean-true-only.parquet",
                        Some(json!({"is_current": true})),
                        Some(json!({"is_current": true})),
                        Some(json!({"is_current": 0})),
                    ),
                    stats_add(
                        "boolean-mixed.parquet",
                        Some(json!({"is_current": false})),
                        Some(json!({"is_current": true})),
                        Some(json!({"is_current": 0})),
                    ),
                    stats_add(
                        "boolean-false-with-null.parquet",
                        Some(json!({"is_current": false})),
                        Some(json!({"is_current": false})),
                        Some(json!({"is_current": 2})),
                    ),
                    stats_add(
                        "boolean-true-with-null.parquet",
                        Some(json!({"is_current": true})),
                        Some(json!({"is_current": true})),
                        Some(json!({"is_current": 2})),
                    ),
                    stats_add(
                        "boolean-all-null.parquet",
                        None,
                        None,
                        Some(json!({"is_current": 10})),
                    ),
                    missing_stats_add("boolean-missing-stats.parquet"),
                ],
            )?;
            let conservative = [
                "boolean-false-only.parquet",
                "boolean-false-with-null.parquet",
                "boolean-missing-stats.parquet",
                "boolean-mixed.parquet",
                "boolean-true-only.parquet",
                "boolean-true-with-null.parquet",
            ];
            for (op, value) in [
                (DeltaComparison::Eq, true),
                (DeltaComparison::Eq, false),
                (DeltaComparison::NotEq, true),
                (DeltaComparison::NotEq, false),
            ] {
                assert_eq!(
                    fixture.selected_paths(&compare(
                        "is_current",
                        op,
                        DeltaScalar::Boolean(value),
                    ))?,
                    conservative.map(str::to_owned)
                );
            }
            assert_paths!(
                fixture,
                is_null("is_current"),
                [
                    "boolean-all-null.parquet",
                    "boolean-false-with-null.parquet",
                    "boolean-missing-stats.parquet",
                    "boolean-true-with-null.parquet"
                ]
            );
            assert_eq!(
                fixture.selected_paths(&is_not_null("is_current"))?,
                conservative.map(str::to_owned)
            );

            let partial = StatisticsFixture::new(
                "boolean-partial-statistics-parity",
                PROTOCOL_JSON,
                vec![field("is_current", "boolean")],
                vec![
                    stats_add(
                        "boolean-min-only-false.parquet",
                        Some(json!({"is_current": false})),
                        None,
                        Some(json!({"is_current": 0})),
                    ),
                    stats_add(
                        "boolean-max-only-true.parquet",
                        None,
                        Some(json!({"is_current": true})),
                        Some(json!({"is_current": 0})),
                    ),
                    stats_add(
                        "boolean-counts-only.parquet",
                        None,
                        None,
                        Some(json!({"is_current": 0})),
                    ),
                    stats_add(
                        "boolean-missing-null-count.parquet",
                        Some(json!({"is_current": false})),
                        Some(json!({"is_current": true})),
                        None,
                    ),
                    missing_stats_add("boolean-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                partial,
                is_null("is_current"),
                [
                    "boolean-missing-null-count.parquet",
                    "boolean-missing-stats.parquet"
                ]
            );
            assert_paths!(
                partial,
                is_not_null("is_current"),
                [
                    "boolean-counts-only.parquet",
                    "boolean-max-only-true.parquet",
                    "boolean-min-only-false.parquet",
                    "boolean-missing-null-count.parquet",
                    "boolean-missing-stats.parquet"
                ]
            );
            Ok(())
        }

        #[test]
        fn date_statistics_pruning_matches_complete_and_partial_frozen_boundaries()
        -> Result<(), Box<dyn std::error::Error>> {
            let fixture = StatisticsFixture::new(
                "date-statistics-parity",
                PROTOCOL_JSON,
                vec![field("event_date", "date")],
                vec![
                    stats_add(
                        "date-pre-epoch-only.parquet",
                        Some(json!({"event_date": "1969-12-31"})),
                        Some(json!({"event_date": "1969-12-31"})),
                        Some(json!({"event_date": 0})),
                    ),
                    stats_add(
                        "date-leap-only.parquet",
                        Some(json!({"event_date": "2024-02-29"})),
                        Some(json!({"event_date": "2024-02-29"})),
                        Some(json!({"event_date": 0})),
                    ),
                    stats_add(
                        "date-new-year-only.parquet",
                        Some(json!({"event_date": "2026-01-01"})),
                        Some(json!({"event_date": "2026-01-01"})),
                        Some(json!({"event_date": 0})),
                    ),
                    stats_add(
                        "date-range.parquet",
                        Some(json!({"event_date": "2024-02-29"})),
                        Some(json!({"event_date": "2026-01-01"})),
                        Some(json!({"event_date": 0})),
                    ),
                    stats_add(
                        "date-new-year-with-null.parquet",
                        Some(json!({"event_date": "2026-01-01"})),
                        Some(json!({"event_date": "2026-01-01"})),
                        Some(json!({"event_date": 2})),
                    ),
                    stats_add(
                        "date-all-null.parquet",
                        None,
                        None,
                        Some(json!({"event_date": 10})),
                    ),
                    missing_stats_add("date-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                fixture,
                compare(
                    "event_date",
                    DeltaComparison::Gt,
                    DeltaScalar::Date32(19_782)
                ),
                [
                    "date-missing-stats.parquet",
                    "date-new-year-only.parquet",
                    "date-new-year-with-null.parquet",
                    "date-range.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "event_date",
                    DeltaComparison::GtEq,
                    DeltaScalar::Date32(20_454)
                ),
                [
                    "date-missing-stats.parquet",
                    "date-new-year-only.parquet",
                    "date-new-year-with-null.parquet",
                    "date-range.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare("event_date", DeltaComparison::LtEq, DeltaScalar::Date32(-1)),
                ["date-missing-stats.parquet", "date-pre-epoch-only.parquet"]
            );
            assert_paths!(
                fixture,
                compare(
                    "event_date",
                    DeltaComparison::Lt,
                    DeltaScalar::Date32(20_454)
                ),
                [
                    "date-leap-only.parquet",
                    "date-missing-stats.parquet",
                    "date-pre-epoch-only.parquet",
                    "date-range.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "event_date",
                    DeltaComparison::Eq,
                    DeltaScalar::Date32(20_454)
                ),
                [
                    "date-missing-stats.parquet",
                    "date-new-year-only.parquet",
                    "date-new-year-with-null.parquet",
                    "date-range.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "event_date",
                    DeltaComparison::NotEq,
                    DeltaScalar::Date32(20_454)
                ),
                [
                    "date-leap-only.parquet",
                    "date-missing-stats.parquet",
                    "date-pre-epoch-only.parquet",
                    "date-range.parquet"
                ]
            );
            assert_paths!(
                fixture,
                is_null("event_date"),
                [
                    "date-all-null.parquet",
                    "date-missing-stats.parquet",
                    "date-new-year-with-null.parquet"
                ]
            );
            assert_paths!(
                fixture,
                is_not_null("event_date"),
                [
                    "date-leap-only.parquet",
                    "date-missing-stats.parquet",
                    "date-new-year-only.parquet",
                    "date-new-year-with-null.parquet",
                    "date-pre-epoch-only.parquet",
                    "date-range.parquet"
                ]
            );

            let partial = StatisticsFixture::new(
                "date-partial-statistics-parity",
                PROTOCOL_JSON,
                vec![field("event_date", "date")],
                vec![
                    stats_add(
                        "date-min-only-high.parquet",
                        Some(json!({"event_date": "2026-01-01"})),
                        None,
                        Some(json!({"event_date": 0})),
                    ),
                    stats_add(
                        "date-max-only-low.parquet",
                        None,
                        Some(json!({"event_date": "2024-02-29"})),
                        Some(json!({"event_date": 0})),
                    ),
                    stats_add(
                        "date-counts-only.parquet",
                        None,
                        None,
                        Some(json!({"event_date": 0})),
                    ),
                    stats_add(
                        "date-missing-null-count.parquet",
                        Some(json!({"event_date": "2024-02-29"})),
                        Some(json!({"event_date": "2026-01-01"})),
                        None,
                    ),
                    missing_stats_add("date-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                partial,
                compare(
                    "event_date",
                    DeltaComparison::Gt,
                    DeltaScalar::Date32(19_782)
                ),
                [
                    "date-counts-only.parquet",
                    "date-min-only-high.parquet",
                    "date-missing-null-count.parquet",
                    "date-missing-stats.parquet"
                ]
            );
            assert_paths!(
                partial,
                is_null("event_date"),
                [
                    "date-missing-null-count.parquet",
                    "date-missing-stats.parquet"
                ]
            );
            assert_paths!(
                partial,
                is_not_null("event_date"),
                [
                    "date-counts-only.parquet",
                    "date-max-only-low.parquet",
                    "date-min-only-high.parquet",
                    "date-missing-null-count.parquet",
                    "date-missing-stats.parquet"
                ]
            );
            Ok(())
        }

        #[test]
        fn decimal_statistics_pruning_matches_complete_and_partial_frozen_boundaries()
        -> Result<(), Box<dyn std::error::Error>> {
            let fixture = StatisticsFixture::new(
                "decimal-statistics-parity",
                PROTOCOL_JSON,
                vec![field("amount", "decimal(10,2)")],
                vec![
                    stats_add(
                        "decimal-negative-only.parquet",
                        Some(json!({"amount": "-1.23"})),
                        Some(json!({"amount": "-1.23"})),
                        Some(json!({"amount": 0})),
                    ),
                    stats_add(
                        "decimal-zero-only.parquet",
                        Some(json!({"amount": "0.00"})),
                        Some(json!({"amount": "0.00"})),
                        Some(json!({"amount": 0})),
                    ),
                    stats_add(
                        "decimal-two-only.parquet",
                        Some(json!({"amount": "2.00"})),
                        Some(json!({"amount": "2.00"})),
                        Some(json!({"amount": 0})),
                    ),
                    stats_add(
                        "decimal-ten-only.parquet",
                        Some(json!({"amount": "10.00"})),
                        Some(json!({"amount": "10.00"})),
                        Some(json!({"amount": 0})),
                    ),
                    stats_add(
                        "decimal-large-only.parquet",
                        Some(json!({"amount": "123.45"})),
                        Some(json!({"amount": "123.45"})),
                        Some(json!({"amount": 0})),
                    ),
                    stats_add(
                        "decimal-range.parquet",
                        Some(json!({"amount": "0.00"})),
                        Some(json!({"amount": "10.00"})),
                        Some(json!({"amount": 0})),
                    ),
                    stats_add(
                        "decimal-two-with-null.parquet",
                        Some(json!({"amount": "2.00"})),
                        Some(json!({"amount": "2.00"})),
                        Some(json!({"amount": 2})),
                    ),
                    stats_add(
                        "decimal-all-null.parquet",
                        None,
                        None,
                        Some(json!({"amount": 10})),
                    ),
                    missing_stats_add("decimal-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                fixture,
                compare("amount", DeltaComparison::Gt, decimal(200)),
                [
                    "decimal-large-only.parquet",
                    "decimal-missing-stats.parquet",
                    "decimal-range.parquet",
                    "decimal-ten-only.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare("amount", DeltaComparison::GtEq, decimal(1_000)),
                [
                    "decimal-large-only.parquet",
                    "decimal-missing-stats.parquet",
                    "decimal-range.parquet",
                    "decimal-ten-only.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare("amount", DeltaComparison::LtEq, decimal(-123)),
                [
                    "decimal-missing-stats.parquet",
                    "decimal-negative-only.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare("amount", DeltaComparison::Lt, decimal(1_000)),
                [
                    "decimal-missing-stats.parquet",
                    "decimal-negative-only.parquet",
                    "decimal-range.parquet",
                    "decimal-two-only.parquet",
                    "decimal-two-with-null.parquet",
                    "decimal-zero-only.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare("amount", DeltaComparison::Eq, decimal(200)),
                [
                    "decimal-missing-stats.parquet",
                    "decimal-range.parquet",
                    "decimal-two-only.parquet",
                    "decimal-two-with-null.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare("amount", DeltaComparison::NotEq, decimal(200)),
                [
                    "decimal-large-only.parquet",
                    "decimal-missing-stats.parquet",
                    "decimal-negative-only.parquet",
                    "decimal-range.parquet",
                    "decimal-ten-only.parquet",
                    "decimal-zero-only.parquet"
                ]
            );
            assert_paths!(
                fixture,
                is_null("amount"),
                [
                    "decimal-all-null.parquet",
                    "decimal-missing-stats.parquet",
                    "decimal-two-with-null.parquet"
                ]
            );
            assert_paths!(
                fixture,
                is_not_null("amount"),
                [
                    "decimal-large-only.parquet",
                    "decimal-missing-stats.parquet",
                    "decimal-negative-only.parquet",
                    "decimal-range.parquet",
                    "decimal-ten-only.parquet",
                    "decimal-two-only.parquet",
                    "decimal-two-with-null.parquet",
                    "decimal-zero-only.parquet"
                ]
            );

            let partial = StatisticsFixture::new(
                "decimal-partial-statistics-parity",
                PROTOCOL_JSON,
                vec![field("amount", "decimal(10,2)")],
                vec![
                    stats_add(
                        "decimal-min-only-high.parquet",
                        Some(json!({"amount": "10.00"})),
                        None,
                        Some(json!({"amount": 0})),
                    ),
                    stats_add(
                        "decimal-max-only-low.parquet",
                        None,
                        Some(json!({"amount": "0.00"})),
                        Some(json!({"amount": 0})),
                    ),
                    stats_add(
                        "decimal-counts-only.parquet",
                        None,
                        None,
                        Some(json!({"amount": 0})),
                    ),
                    stats_add(
                        "decimal-missing-null-count.parquet",
                        Some(json!({"amount": "0.00"})),
                        Some(json!({"amount": "10.00"})),
                        None,
                    ),
                    missing_stats_add("decimal-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                partial,
                compare("amount", DeltaComparison::Gt, decimal(200)),
                [
                    "decimal-counts-only.parquet",
                    "decimal-min-only-high.parquet",
                    "decimal-missing-null-count.parquet",
                    "decimal-missing-stats.parquet"
                ]
            );
            assert_paths!(
                partial,
                is_null("amount"),
                [
                    "decimal-missing-null-count.parquet",
                    "decimal-missing-stats.parquet"
                ]
            );
            assert_paths!(
                partial,
                is_not_null("amount"),
                [
                    "decimal-counts-only.parquet",
                    "decimal-max-only-low.parquet",
                    "decimal-min-only-high.parquet",
                    "decimal-missing-null-count.parquet",
                    "decimal-missing-stats.parquet"
                ]
            );
            Ok(())
        }

        #[test]
        fn binary_statistics_pruning_matches_complete_and_partial_frozen_boundaries()
        -> Result<(), Box<dyn std::error::Error>> {
            let fixture = StatisticsFixture::new(
                "binary-statistics-parity",
                PROTOCOL_JSON,
                vec![field("payload", "binary")],
                vec![
                    stats_add(
                        "binary-HELLO.parquet",
                        Some(json!({"payload": "HELLO"})),
                        Some(json!({"payload": "HELLO"})),
                        Some(json!({"payload": 0})),
                    ),
                    stats_add(
                        "binary-empty.parquet",
                        Some(json!({"payload": ""})),
                        Some(json!({"payload": ""})),
                        Some(json!({"payload": 0})),
                    ),
                    stats_add(
                        "binary-hello.parquet",
                        Some(json!({"payload": "hello"})),
                        Some(json!({"payload": "hello"})),
                        Some(json!({"payload": 0})),
                    ),
                    stats_add(
                        "binary-range.parquet",
                        Some(json!({"payload": "a"})),
                        Some(json!({"payload": "z"})),
                        Some(json!({"payload": 0})),
                    ),
                    stats_add(
                        "binary-special.parquet",
                        Some(json!({"payload": "/=%"})),
                        Some(json!({"payload": "/=%"})),
                        Some(json!({"payload": 0})),
                    ),
                    stats_add(
                        "binary-with-null.parquet",
                        Some(json!({"payload": "hello"})),
                        Some(json!({"payload": "hello"})),
                        Some(json!({"payload": 2})),
                    ),
                    stats_add(
                        "binary-all-null.parquet",
                        None,
                        None,
                        Some(json!({"payload": 10})),
                    ),
                    missing_stats_add("binary-missing-stats.parquet"),
                ],
            )?;
            let conservative = [
                "binary-HELLO.parquet",
                "binary-empty.parquet",
                "binary-hello.parquet",
                "binary-missing-stats.parquet",
                "binary-range.parquet",
                "binary-special.parquet",
                "binary-with-null.parquet",
            ];
            for (op, value) in [
                (DeltaComparison::Eq, b"hello".to_vec()),
                (DeltaComparison::NotEq, b"hello".to_vec()),
                (DeltaComparison::Gt, b"hello".to_vec()),
                (DeltaComparison::Lt, b"hello".to_vec()),
                (DeltaComparison::Eq, Vec::new()),
            ] {
                assert_eq!(
                    fixture.selected_paths(&compare("payload", op, DeltaScalar::Binary(value)))?,
                    conservative.map(str::to_owned)
                );
            }
            assert_paths!(
                fixture,
                is_null("payload"),
                [
                    "binary-all-null.parquet",
                    "binary-missing-stats.parquet",
                    "binary-with-null.parquet"
                ]
            );
            assert_eq!(
                fixture.selected_paths(&is_not_null("payload"))?,
                conservative.map(str::to_owned)
            );

            let partial = StatisticsFixture::new(
                "binary-partial-statistics-parity",
                PROTOCOL_JSON,
                vec![field("payload", "binary")],
                vec![
                    stats_add(
                        "binary-min-only-high.parquet",
                        Some(json!({"payload": "m"})),
                        None,
                        Some(json!({"payload": 0})),
                    ),
                    stats_add(
                        "binary-max-only-low.parquet",
                        None,
                        Some(json!({"payload": "a"})),
                        Some(json!({"payload": 0})),
                    ),
                    stats_add(
                        "binary-counts-only.parquet",
                        None,
                        None,
                        Some(json!({"payload": 0})),
                    ),
                    stats_add(
                        "binary-missing-null-count.parquet",
                        Some(json!({"payload": "a"})),
                        Some(json!({"payload": "z"})),
                        None,
                    ),
                    missing_stats_add("binary-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                partial,
                compare(
                    "payload",
                    DeltaComparison::Gt,
                    DeltaScalar::Binary(b"hello".to_vec())
                ),
                [
                    "binary-counts-only.parquet",
                    "binary-max-only-low.parquet",
                    "binary-min-only-high.parquet",
                    "binary-missing-null-count.parquet",
                    "binary-missing-stats.parquet"
                ]
            );
            assert_paths!(
                partial,
                is_null("payload"),
                [
                    "binary-missing-null-count.parquet",
                    "binary-missing-stats.parquet"
                ]
            );
            assert_paths!(
                partial,
                is_not_null("payload"),
                [
                    "binary-counts-only.parquet",
                    "binary-max-only-low.parquet",
                    "binary-min-only-high.parquet",
                    "binary-missing-null-count.parquet",
                    "binary-missing-stats.parquet"
                ]
            );
            Ok(())
        }

        #[test]
        fn floating_statistics_pruning_matches_safe_frozen_boundaries()
        -> Result<(), Box<dyn std::error::Error>> {
            let fixture = StatisticsFixture::new(
                "floating-statistics-parity",
                PROTOCOL_JSON,
                vec![
                    field("float_score", "float"),
                    field("double_score", "double"),
                ],
                vec![
                    stats_add(
                        "floating-neg.parquet",
                        Some(json!({"float_score": -1.5, "double_score": -2.25})),
                        Some(json!({"float_score": -1.5, "double_score": -2.25})),
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-neg-zero.parquet",
                        Some(json!({"float_score": -0.0, "double_score": -0.0})),
                        Some(json!({"float_score": -0.0, "double_score": -0.0})),
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-pos-zero.parquet",
                        Some(json!({"float_score": 0.0, "double_score": 0.0})),
                        Some(json!({"float_score": 0.0, "double_score": 0.0})),
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-one.parquet",
                        Some(json!({"float_score": 1.5, "double_score": 2.25})),
                        Some(json!({"float_score": 1.5, "double_score": 2.25})),
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-range.parquet",
                        Some(json!({"float_score": -1.0, "double_score": -2.0})),
                        Some(json!({"float_score": 2.0, "double_score": 3.0})),
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-ten.parquet",
                        Some(json!({"float_score": 10.0, "double_score": 10.0})),
                        Some(json!({"float_score": 10.0, "double_score": 10.0})),
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-one-with-null.parquet",
                        Some(json!({"float_score": 1.5, "double_score": 2.25})),
                        Some(json!({"float_score": 1.5, "double_score": 2.25})),
                        Some(json!({"float_score": 2, "double_score": 2})),
                    ),
                    stats_add(
                        "floating-all-null.parquet",
                        None,
                        None,
                        Some(json!({"float_score": 10, "double_score": 10})),
                    ),
                    missing_stats_add("floating-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                fixture,
                compare(
                    "float_score",
                    DeltaComparison::Gt,
                    DeltaScalar::Float32(1.5)
                ),
                [
                    "floating-missing-stats.parquet",
                    "floating-range.parquet",
                    "floating-ten.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "float_score",
                    DeltaComparison::Lt,
                    DeltaScalar::Float32(10.0)
                ),
                [
                    "floating-missing-stats.parquet",
                    "floating-neg-zero.parquet",
                    "floating-neg.parquet",
                    "floating-one-with-null.parquet",
                    "floating-one.parquet",
                    "floating-pos-zero.parquet",
                    "floating-range.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "float_score",
                    DeltaComparison::Eq,
                    DeltaScalar::Float32(1.5)
                ),
                [
                    "floating-missing-stats.parquet",
                    "floating-one-with-null.parquet",
                    "floating-one.parquet",
                    "floating-range.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "float_score",
                    DeltaComparison::NotEq,
                    DeltaScalar::Float32(1.5)
                ),
                [
                    "floating-missing-stats.parquet",
                    "floating-neg-zero.parquet",
                    "floating-neg.parquet",
                    "floating-pos-zero.parquet",
                    "floating-range.parquet",
                    "floating-ten.parquet"
                ]
            );
            assert_paths!(
                fixture,
                is_null("float_score"),
                [
                    "floating-all-null.parquet",
                    "floating-missing-stats.parquet",
                    "floating-one-with-null.parquet"
                ]
            );
            assert_paths!(
                fixture,
                is_not_null("double_score"),
                [
                    "floating-missing-stats.parquet",
                    "floating-neg-zero.parquet",
                    "floating-neg.parquet",
                    "floating-one-with-null.parquet",
                    "floating-one.parquet",
                    "floating-pos-zero.parquet",
                    "floating-range.parquet",
                    "floating-ten.parquet"
                ]
            );

            let partial = StatisticsFixture::new(
                "floating-partial-statistics-parity",
                PROTOCOL_JSON,
                vec![
                    field("float_score", "float"),
                    field("double_score", "double"),
                ],
                vec![
                    stats_add(
                        "floating-min-only-high.parquet",
                        Some(json!({"float_score": 2.0, "double_score": 2.0})),
                        None,
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-max-only-low.parquet",
                        None,
                        Some(json!({"float_score": 0.0, "double_score": 0.0})),
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-counts-only.parquet",
                        None,
                        None,
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-missing-null-count.parquet",
                        Some(json!({"float_score": -1.0, "double_score": -1.0})),
                        Some(json!({"float_score": 2.0, "double_score": 2.0})),
                        None,
                    ),
                    missing_stats_add("floating-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                partial,
                compare(
                    "float_score",
                    DeltaComparison::Gt,
                    DeltaScalar::Float32(1.0)
                ),
                [
                    "floating-counts-only.parquet",
                    "floating-min-only-high.parquet",
                    "floating-missing-null-count.parquet",
                    "floating-missing-stats.parquet"
                ]
            );
            assert_paths!(
                partial,
                is_null("float_score"),
                [
                    "floating-missing-null-count.parquet",
                    "floating-missing-stats.parquet"
                ]
            );
            assert_paths!(
                partial,
                is_not_null("float_score"),
                [
                    "floating-counts-only.parquet",
                    "floating-max-only-low.parquet",
                    "floating-min-only-high.parquet",
                    "floating-missing-null-count.parquet",
                    "floating-missing-stats.parquet"
                ]
            );

            let nonfinite = StatisticsFixture::new(
                "floating-nonfinite-statistics-parity",
                PROTOCOL_JSON,
                vec![
                    field("float_score", "float"),
                    field("double_score", "double"),
                ],
                vec![
                    stats_add(
                        "floating-valid.parquet",
                        Some(json!({"float_score": 1.5, "double_score": 2.25})),
                        Some(json!({"float_score": 1.5, "double_score": 2.25})),
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-nan.parquet",
                        Some(json!({"float_score": "NaN", "double_score": "NaN"})),
                        Some(json!({"float_score": "NaN", "double_score": "NaN"})),
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-inf.parquet",
                        Some(json!({"float_score": "Infinity", "double_score": "Infinity"})),
                        Some(json!({"float_score": "Infinity", "double_score": "Infinity"})),
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    stats_add(
                        "floating-neg-inf.parquet",
                        Some(json!({"float_score": "-Infinity", "double_score": "-Infinity"})),
                        Some(json!({"float_score": "-Infinity", "double_score": "-Infinity"})),
                        Some(json!({"float_score": 0, "double_score": 0})),
                    ),
                    missing_stats_add("floating-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                nonfinite,
                compare(
                    "float_score",
                    DeltaComparison::Gt,
                    DeltaScalar::Float32(1.0)
                ),
                [
                    "floating-inf.parquet",
                    "floating-missing-stats.parquet",
                    "floating-nan.parquet",
                    "floating-valid.parquet"
                ]
            );
            assert_paths!(
                nonfinite,
                compare(
                    "float_score",
                    DeltaComparison::Eq,
                    DeltaScalar::Float32(1.5)
                ),
                ["floating-missing-stats.parquet", "floating-valid.parquet"]
            );
            assert_paths!(
                nonfinite,
                compare(
                    "float_score",
                    DeltaComparison::NotEq,
                    DeltaScalar::Float32(1.5)
                ),
                [
                    "floating-inf.parquet",
                    "floating-missing-stats.parquet",
                    "floating-nan.parquet",
                    "floating-neg-inf.parquet"
                ]
            );
            Ok(())
        }

        #[test]
        fn string_statistics_pruning_matches_complete_partial_and_unicode_frozen_boundaries()
        -> Result<(), Box<dyn std::error::Error>> {
            let fixture = StatisticsFixture::new(
                "string-statistics-parity",
                PROTOCOL_JSON,
                vec![field("customer_name", "string")],
                vec![
                    stats_add(
                        "string-empty-only.parquet",
                        Some(json!({"customer_name": ""})),
                        Some(json!({"customer_name": ""})),
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-mixed-case-only.parquet",
                        Some(json!({"customer_name": "Alice"})),
                        Some(json!({"customer_name": "Alice"})),
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-alice-only.parquet",
                        Some(json!({"customer_name": "alice"})),
                        Some(json!({"customer_name": "alice"})),
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-bob-only.parquet",
                        Some(json!({"customer_name": "bob"})),
                        Some(json!({"customer_name": "bob"})),
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-range.parquet",
                        Some(json!({"customer_name": "alice"})),
                        Some(json!({"customer_name": "morgan"})),
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-zed-only.parquet",
                        Some(json!({"customer_name": "zed"})),
                        Some(json!({"customer_name": "zed"})),
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-alice-with-null.parquet",
                        Some(json!({"customer_name": "alice"})),
                        Some(json!({"customer_name": "alice"})),
                        Some(json!({"customer_name": 2})),
                    ),
                    stats_add(
                        "string-all-null.parquet",
                        None,
                        None,
                        Some(json!({"customer_name": 10})),
                    ),
                    missing_stats_add("string-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                fixture,
                compare(
                    "customer_name",
                    DeltaComparison::Gt,
                    DeltaScalar::Utf8("m".to_owned())
                ),
                [
                    "string-missing-stats.parquet",
                    "string-range.parquet",
                    "string-zed-only.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "customer_name",
                    DeltaComparison::GtEq,
                    DeltaScalar::Utf8("morgan".to_owned())
                ),
                [
                    "string-missing-stats.parquet",
                    "string-range.parquet",
                    "string-zed-only.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "customer_name",
                    DeltaComparison::LtEq,
                    DeltaScalar::Utf8("Alice".to_owned())
                ),
                [
                    "string-empty-only.parquet",
                    "string-missing-stats.parquet",
                    "string-mixed-case-only.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "customer_name",
                    DeltaComparison::Lt,
                    DeltaScalar::Utf8("m".to_owned())
                ),
                [
                    "string-alice-only.parquet",
                    "string-alice-with-null.parquet",
                    "string-bob-only.parquet",
                    "string-empty-only.parquet",
                    "string-missing-stats.parquet",
                    "string-mixed-case-only.parquet",
                    "string-range.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "customer_name",
                    DeltaComparison::Eq,
                    DeltaScalar::Utf8("alice".to_owned())
                ),
                [
                    "string-alice-only.parquet",
                    "string-alice-with-null.parquet",
                    "string-missing-stats.parquet",
                    "string-range.parquet"
                ]
            );
            assert_paths!(
                fixture,
                compare(
                    "customer_name",
                    DeltaComparison::NotEq,
                    DeltaScalar::Utf8("alice".to_owned())
                ),
                [
                    "string-bob-only.parquet",
                    "string-empty-only.parquet",
                    "string-missing-stats.parquet",
                    "string-mixed-case-only.parquet",
                    "string-range.parquet",
                    "string-zed-only.parquet"
                ]
            );
            assert_paths!(
                fixture,
                is_null("customer_name"),
                [
                    "string-alice-with-null.parquet",
                    "string-all-null.parquet",
                    "string-missing-stats.parquet"
                ]
            );
            assert_paths!(
                fixture,
                is_not_null("customer_name"),
                [
                    "string-alice-only.parquet",
                    "string-alice-with-null.parquet",
                    "string-bob-only.parquet",
                    "string-empty-only.parquet",
                    "string-missing-stats.parquet",
                    "string-mixed-case-only.parquet",
                    "string-range.parquet",
                    "string-zed-only.parquet"
                ]
            );

            let partial = StatisticsFixture::new(
                "string-partial-statistics-parity",
                PROTOCOL_JSON,
                vec![field("customer_name", "string")],
                vec![
                    stats_add(
                        "string-min-only-morgan.parquet",
                        Some(json!({"customer_name": "morgan"})),
                        None,
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-max-only-alice.parquet",
                        None,
                        Some(json!({"customer_name": "alice"})),
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-counts-only.parquet",
                        None,
                        None,
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-missing-null-count.parquet",
                        Some(json!({"customer_name": "alice"})),
                        Some(json!({"customer_name": "morgan"})),
                        None,
                    ),
                    missing_stats_add("string-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                partial,
                compare(
                    "customer_name",
                    DeltaComparison::Gt,
                    DeltaScalar::Utf8("m".to_owned())
                ),
                [
                    "string-counts-only.parquet",
                    "string-min-only-morgan.parquet",
                    "string-missing-null-count.parquet",
                    "string-missing-stats.parquet"
                ]
            );
            assert_paths!(
                partial,
                is_null("customer_name"),
                [
                    "string-missing-null-count.parquet",
                    "string-missing-stats.parquet"
                ]
            );
            assert_paths!(
                partial,
                is_not_null("customer_name"),
                [
                    "string-counts-only.parquet",
                    "string-max-only-alice.parquet",
                    "string-min-only-morgan.parquet",
                    "string-missing-null-count.parquet",
                    "string-missing-stats.parquet"
                ]
            );

            let unicode = StatisticsFixture::new(
                "string-unicode-statistics-parity",
                PROTOCOL_JSON,
                vec![field("customer_name", "string")],
                vec![
                    stats_add(
                        "string-ascii-cafe.parquet",
                        Some(json!({"customer_name": "cafe"})),
                        Some(json!({"customer_name": "cafe"})),
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-ascii-zulu.parquet",
                        Some(json!({"customer_name": "zulu"})),
                        Some(json!({"customer_name": "zulu"})),
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-eclair.parquet",
                        Some(json!({"customer_name": "\u{00e9}clair"})),
                        Some(json!({"customer_name": "\u{00e9}clair"})),
                        Some(json!({"customer_name": 0})),
                    ),
                    stats_add(
                        "string-emile.parquet",
                        Some(json!({"customer_name": "\u{00e9}mile"})),
                        Some(json!({"customer_name": "\u{00e9}mile"})),
                        Some(json!({"customer_name": 0})),
                    ),
                    missing_stats_add("string-missing-stats.parquet"),
                ],
            )?;
            assert_paths!(
                unicode,
                compare(
                    "customer_name",
                    DeltaComparison::Eq,
                    DeltaScalar::Utf8("\u{00e9}clair".to_owned())
                ),
                ["string-eclair.parquet", "string-missing-stats.parquet"]
            );
            assert_paths!(
                unicode,
                compare(
                    "customer_name",
                    DeltaComparison::GtEq,
                    DeltaScalar::Utf8("\u{00e9}clair".to_owned())
                ),
                [
                    "string-eclair.parquet",
                    "string-emile.parquet",
                    "string-missing-stats.parquet"
                ]
            );
            assert_paths!(
                unicode,
                compare(
                    "customer_name",
                    DeltaComparison::Lt,
                    DeltaScalar::Utf8("\u{00e9}clair".to_owned())
                ),
                [
                    "string-ascii-cafe.parquet",
                    "string-ascii-zulu.parquet",
                    "string-missing-stats.parquet"
                ]
            );
            assert_paths!(
                unicode,
                compare(
                    "customer_name",
                    DeltaComparison::Gt,
                    DeltaScalar::Utf8("\u{00e9}clair".to_owned())
                ),
                ["string-emile.parquet", "string-missing-stats.parquet"]
            );
            Ok(())
        }

        fn timestamp_add(
            path: &str,
            column: &str,
            min: Option<&str>,
            max: Option<&str>,
            null_count: Option<u64>,
        ) -> String {
            let min_values = min.map(|value| json!({column: value}));
            let max_values = max.map(|value| json!({column: value}));
            let null_count = null_count.map(|value| json!({column: value}));
            stats_add(path, min_values, max_values, null_count)
        }

        fn timestamp_fixture(
            name: &str,
            protocol: &str,
            column: &str,
            data_type: &str,
            prefix: &str,
        ) -> Result<StatisticsFixture, Box<dyn std::error::Error>> {
            let (pre_epoch, low, target, high) = if data_type == "timestamp" {
                (
                    "1969-12-31T23:59:59.999999Z",
                    "2025-12-31T23:59:59.999999Z",
                    "2026-01-01T00:00:00.123456Z",
                    "2026-01-01T00:00:00.123457Z",
                )
            } else {
                (
                    "1969-12-31 23:59:59.999999",
                    "2025-12-31 23:59:59.999999",
                    "2026-01-01 00:00:00.123456",
                    "2026-01-01 00:00:00.123457",
                )
            };
            StatisticsFixture::new(
                name,
                protocol,
                vec![field(column, data_type)],
                vec![
                    timestamp_add(
                        &format!("{prefix}-pre-epoch-only.parquet"),
                        column,
                        Some(pre_epoch),
                        Some(pre_epoch),
                        Some(0),
                    ),
                    timestamp_add(
                        &format!("{prefix}-low-only.parquet"),
                        column,
                        Some(low),
                        Some(low),
                        Some(0),
                    ),
                    timestamp_add(
                        &format!("{prefix}-target-only.parquet"),
                        column,
                        Some(target),
                        Some(target),
                        Some(0),
                    ),
                    timestamp_add(
                        &format!("{prefix}-high-only.parquet"),
                        column,
                        Some(high),
                        Some(high),
                        Some(0),
                    ),
                    timestamp_add(
                        &format!("{prefix}-range.parquet"),
                        column,
                        Some(low),
                        Some(target),
                        Some(0),
                    ),
                    timestamp_add(
                        &format!("{prefix}-target-with-null.parquet"),
                        column,
                        Some(target),
                        Some(target),
                        Some(2),
                    ),
                    timestamp_add(
                        &format!("{prefix}-all-null.parquet"),
                        column,
                        None,
                        None,
                        Some(10),
                    ),
                    missing_stats_add(&format!("{prefix}-missing-stats.parquet")),
                ],
            )
        }

        fn partial_timestamp_fixture(
            name: &str,
            protocol: &str,
            column: &str,
            data_type: &str,
            prefix: &str,
        ) -> Result<StatisticsFixture, Box<dyn std::error::Error>> {
            let (low, target) = if data_type == "timestamp" {
                ("2025-12-31T23:59:59.999999Z", "2026-01-01T00:00:00.123456Z")
            } else {
                ("2025-12-31 23:59:59.999999", "2026-01-01 00:00:00.123456")
            };
            StatisticsFixture::new(
                name,
                protocol,
                vec![field(column, data_type)],
                vec![
                    timestamp_add(
                        &format!("{prefix}-min-only-target.parquet"),
                        column,
                        Some(target),
                        None,
                        Some(0),
                    ),
                    timestamp_add(
                        &format!("{prefix}-max-only-low.parquet"),
                        column,
                        None,
                        Some(low),
                        Some(0),
                    ),
                    timestamp_add(
                        &format!("{prefix}-counts-only.parquet"),
                        column,
                        None,
                        None,
                        Some(0),
                    ),
                    timestamp_add(
                        &format!("{prefix}-missing-null-count.parquet"),
                        column,
                        Some(low),
                        Some(target),
                        None,
                    ),
                    missing_stats_add(&format!("{prefix}-missing-stats.parquet")),
                ],
            )
        }

        fn assert_timestamp_statistics_matrix(
            fixture: &StatisticsFixture,
            partial: &StatisticsFixture,
            column: &str,
            prefix: &str,
            timezone: Option<&str>,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let low = 1_767_225_599_999_999_i64;
            let target = 1_767_225_600_123_456_i64;
            let high = 1_767_225_600_123_457_i64;
            let paths = |names: &[&str]| {
                names
                    .iter()
                    .map(|name| format!("{prefix}-{name}.parquet"))
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::Lt,
                    timestamp(target, timezone)
                ))?,
                paths(&["low-only", "missing-stats", "pre-epoch-only", "range"])
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::GtEq,
                    timestamp(target, timezone)
                ))?,
                paths(&[
                    "high-only",
                    "missing-stats",
                    "range",
                    "target-only",
                    "target-with-null"
                ])
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::Lt,
                    timestamp(high, timezone)
                ))?,
                paths(&[
                    "low-only",
                    "missing-stats",
                    "pre-epoch-only",
                    "range",
                    "target-only",
                    "target-with-null"
                ])
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::Eq,
                    timestamp(low, timezone)
                ))?,
                paths(&["low-only", "missing-stats", "range"])
            );
            assert_eq!(
                fixture.selected_paths(&compare(
                    column,
                    DeltaComparison::NotEq,
                    timestamp(target, timezone)
                ))?,
                paths(&[
                    "high-only",
                    "low-only",
                    "missing-stats",
                    "pre-epoch-only",
                    "range",
                    "target-only",
                    "target-with-null"
                ])
            );
            assert_eq!(
                fixture.selected_paths(&is_null(column))?,
                paths(&["all-null", "missing-stats", "target-with-null"])
            );
            assert_eq!(
                fixture.selected_paths(&is_not_null(column))?,
                paths(&[
                    "high-only",
                    "low-only",
                    "missing-stats",
                    "pre-epoch-only",
                    "range",
                    "target-only",
                    "target-with-null"
                ])
            );
            assert_eq!(
                partial.selected_paths(&compare(
                    column,
                    DeltaComparison::Gt,
                    timestamp(low, timezone)
                ))?,
                paths(&[
                    "counts-only",
                    "max-only-low",
                    "min-only-target",
                    "missing-null-count",
                    "missing-stats"
                ])
            );
            assert_eq!(
                partial.selected_paths(&is_null(column))?,
                paths(&["missing-null-count", "missing-stats"])
            );
            assert_eq!(
                partial.selected_paths(&is_not_null(column))?,
                paths(&[
                    "counts-only",
                    "max-only-low",
                    "min-only-target",
                    "missing-null-count",
                    "missing-stats"
                ])
            );
            Ok(())
        }

        #[test]
        fn timestamp_statistics_pruning_matches_timezone_and_ntz_frozen_boundaries()
        -> Result<(), Box<dyn std::error::Error>> {
            let timestamp_complete = timestamp_fixture(
                "timestamp-statistics-parity",
                PROTOCOL_JSON,
                "event_ts",
                "timestamp",
                "timestamp",
            )?;
            let timestamp_partial = partial_timestamp_fixture(
                "timestamp-partial-statistics-parity",
                PROTOCOL_JSON,
                "event_ts",
                "timestamp",
                "timestamp",
            )?;
            assert_timestamp_statistics_matrix(
                &timestamp_complete,
                &timestamp_partial,
                "event_ts",
                "timestamp",
                Some("UTC"),
            )?;

            let ntz_fixture = timestamp_fixture(
                "timestamp-ntz-statistics-parity",
                TIMESTAMP_NTZ_PROTOCOL_JSON,
                "event_ts_ntz",
                "timestamp_ntz",
                "timestamp-ntz",
            )?;
            let ntz_partial = partial_timestamp_fixture(
                "timestamp-ntz-partial-statistics-parity",
                TIMESTAMP_NTZ_PROTOCOL_JSON,
                "event_ts_ntz",
                "timestamp_ntz",
                "timestamp-ntz",
            )?;
            assert_timestamp_statistics_matrix(
                &ntz_fixture,
                &ntz_partial,
                "event_ts_ntz",
                "timestamp-ntz",
                None,
            )?;
            Ok(())
        }
    }

    use std::{
        collections::HashMap,
        fs,
        future::{pending, poll_fn},
        path::{Path, PathBuf},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::Poll,
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow::{
        array::{Int32Array, StringArray, StructArray},
        datatypes::{DataType, Schema},
        record_batch::RecordBatch,
    };
    use delta_kernel::{
        actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType},
        expressions::{ColumnName, Expression},
        scan::state::{DvInfo, ScanFile, Stats},
    };
    use futures_util::{FutureExt, StreamExt, stream};

    use super::{
        DeltaScanFileStats, DeltaScanFileTask, DeltaScanFileTaskPartition, DeltaScanPlan,
        DeltaUnpartitionedScanPlan, KernelPhysicalToLogicalTransform, build_scan, checked_sum,
        group_scan_file_tasks,
    };
    use crate::{
        DeltaComparison, DeltaPredicate, DeltaReaderPhase, DeltaScalar, DeltaSnapshotSelection,
        DeltaStorageOptions,
        delta::{
            kernel::{KernelScanFileMetadata, delta_predicate_to_kernel_pruning},
            snapshot::load_delta_table_snapshot_blocking,
        },
        reader::{
            predicate::validate_predicate,
            scheduling::{
                DeltaScanScheduler, FileAdmission, FileAdmissionFn, FileBatchStream, FileExecutor,
                FileReadPermit,
            },
        },
    };

    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const DELETION_VECTOR_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}"#;
    const UNSUPPORTED_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["madeUpFeature"],"writerFeatures":["madeUpFeature"]}}"#;
    const COLUMN_MAPPING_PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["columnMapping"],"writerFeatures":["columnMapping"]}}"#;
    const METADATA_JSON: &str = r#"{"metaData":{"id":"scan-planning-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"label\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"hidden\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
    const PARTITIONED_METADATA_JSON: &str = r#"{"metaData":{"id":"scan-planning-partition-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["region"],"configuration":{},"createdTime":1587968585495}}"#;
    const INVALID_PARTITION_METADATA_JSON: &str = r#"{"metaData":{"id":"scan-planning-invalid-partition-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"long_part\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["long_part"],"configuration":{},"createdTime":1587968585495}}"#;
    const COLUMN_MAPPING_METADATA_JSON: &str = r#"{"metaData":{"id":"scan-planning-column-mapping-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"phys_id\"}},{\"name\":\"customer_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"phys_customer_name\"}},{\"name\":\"profile\",\"type\":{\"type\":\"struct\",\"fields\":[{\"name\":\"first_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":4,\"delta.columnMapping.physicalName\":\"phys_first_name\"}},{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":5,\"delta.columnMapping.physicalName\":\"phys_age\"}}]},\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":3,\"delta.columnMapping.physicalName\":\"phys_profile\"}}]}","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"name","delta.columnMapping.maxColumnId":"5"},"createdTime":1587968585495}}"#;

    struct DeltaLogTable(PathBuf);

    impl DeltaLogTable {
        fn new_with_metadata_and_adds(
            name: &str,
            metadata: &str,
            adds: &[String],
        ) -> Result<Self, Box<dyn std::error::Error>> {
            Self::new_with_protocol_metadata_and_adds(name, PROTOCOL_JSON, metadata, adds)
        }

        fn new_with_protocol_metadata_and_adds(
            name: &str,
            protocol: &str,
            metadata: &str,
            adds: &[String],
        ) -> Result<Self, Box<dyn std::error::Error>> {
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let path = Path::new("target")
                .join("delta-arrow-reader-planning-tests")
                .join(format!("{}-{name}-{nanos}", std::process::id()));
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{protocol}\n{metadata}\n{}", adds.join("\n")),
            )?;
            Ok(Self(path))
        }
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn loaded_snapshot(
        name: &str,
    ) -> Result<
        (
            DeltaLogTable,
            crate::delta::snapshot::LoadedDeltaTableSnapshot,
        ),
        Box<dyn std::error::Error>,
    > {
        loaded_snapshot_with_adds(name, &[])
    }

    fn loaded_snapshot_with_adds(
        name: &str,
        adds: &[String],
    ) -> Result<
        (
            DeltaLogTable,
            crate::delta::snapshot::LoadedDeltaTableSnapshot,
        ),
        Box<dyn std::error::Error>,
    > {
        loaded_snapshot_with_metadata_and_adds(name, METADATA_JSON, adds)
    }

    fn loaded_snapshot_with_metadata_and_adds(
        name: &str,
        metadata: &str,
        adds: &[String],
    ) -> Result<
        (
            DeltaLogTable,
            crate::delta::snapshot::LoadedDeltaTableSnapshot,
        ),
        Box<dyn std::error::Error>,
    > {
        let table = DeltaLogTable::new_with_metadata_and_adds(name, metadata, adds)?;
        let snapshot = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        Ok((table, snapshot))
    }

    fn add(path: &str, size: i64, rows: Option<u64>) -> String {
        let stats = rows.map(|rows| format!(r#"{{"numRecords":{rows}}}"#));
        add_with_stats(path, size, stats.as_deref())
    }

    fn add_with_stats(path: &str, size: i64, stats: Option<&str>) -> String {
        add_action(path, size, "{}", stats)
    }

    fn add_with_partition(path: &str, size: i64, rows: u64, region: &str) -> String {
        let stats = format!(r#"{{"numRecords":{rows}}}"#);
        let region = serde_json::to_string(region).expect("partition value is serializable");
        add_action(
            path,
            size,
            &format!(r#"{{"region":{region}}}"#),
            Some(&stats),
        )
    }

    fn add_action(path: &str, size: i64, partition_values: &str, stats: Option<&str>) -> String {
        let stats = stats.map_or_else(String::new, |stats| {
            format!(
                ",\"stats\":{}",
                serde_json::to_string(stats).expect("stats string is serializable")
            )
        });
        format!(
            r#"{{"add":{{"path":"{path}","partitionValues":{partition_values},"size":{size},"modificationTime":1587968586000,"dataChange":true{stats}}}}}"#
        )
    }

    fn dv_add_action(
        path: &str,
        size: i64,
        partition_values: serde_json::Value,
        stats: Option<&str>,
    ) -> String {
        let mut add = serde_json::json!({
            "path": path,
            "partitionValues": partition_values,
            "size": size,
            "modificationTime": 1587968586000_i64,
            "dataChange": true,
            "deletionVector": {
                "storageType": "u",
                "pathOrInlineDv": "vBn[lx{q8@P<9BNH/isA",
                "offset": 1,
                "sizeInBytes": 36,
                "cardinality": 2
            }
        });
        if let Some(stats) = stats {
            add.as_object_mut()
                .expect("add action is an object")
                .insert("stats".to_owned(), serde_json::json!(stats));
        }
        serde_json::json!({"add": add}).to_string()
    }

    fn field_names(schema: &arrow::datatypes::SchemaRef) -> Vec<&str> {
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect()
    }

    fn kernel_file(path: &str) -> ScanFile {
        ScanFile {
            path: path.to_owned(),
            size: 123,
            modification_time: 1_587_968_586_000,
            stats: Some(Stats { num_records: 7 }),
            dv_info: DvInfo::default(),
            transform: None,
            partition_values: HashMap::from([
                ("region".to_owned(), "us-west".to_owned()),
                ("day".to_owned(), "2026-06-11".to_owned()),
            ]),
        }
    }

    fn task(file: ScanFile) -> Result<DeltaScanFileTask, crate::DeltaReaderError> {
        DeltaScanFileTask::try_from_kernel(KernelScanFileMetadata::from_scan_file(file))
    }

    fn grouping_task(
        path: &str,
        estimated_bytes: Option<u64>,
        estimated_rows: Option<u64>,
    ) -> Result<DeltaScanFileTask, crate::DeltaReaderError> {
        let mut task = task(kernel_file(path))?;
        task.file_size = estimated_bytes;
        task.estimated_rows = estimated_rows;
        task.stats = estimated_rows.map(|num_records| DeltaScanFileStats { num_records });
        Ok(task)
    }

    fn partition_paths(partitions: &[DeltaScanFileTaskPartition]) -> Vec<Vec<&str>> {
        partitions
            .iter()
            .map(|partition| {
                partition
                    .file_tasks
                    .iter()
                    .map(|task| task.path.as_str())
                    .collect()
            })
            .collect()
    }

    fn planned_tasks(plan: &DeltaScanPlan) -> impl Iterator<Item = &DeltaScanFileTask> {
        plan.partitions
            .iter()
            .flat_map(|partition| partition.file_tasks.iter())
    }

    fn unpartitioned_tasks(
        plan: &DeltaUnpartitionedScanPlan,
    ) -> impl Iterator<Item = &DeltaScanFileTask> {
        plan.file_tasks.iter()
    }

    fn execution_file_stream(permit: FileReadPermit, batch: RecordBatch) -> FileBatchStream {
        Box::pin(stream::unfold(
            (Some(batch), permit),
            |(batch, permit)| async move { batch.map(|batch| (Ok(batch), (None, permit))) },
        ))
    }

    fn pending_execution_file_stream(permit: FileReadPermit) -> FileBatchStream {
        Box::pin(stream::once(async move {
            let _permit = permit;
            pending::<Result<RecordBatch, crate::DeltaReaderError>>().await
        }))
    }

    fn plan_scan(
        snapshot: &crate::delta::snapshot::LoadedDeltaTableSnapshot,
        projection: Option<&[String]>,
        hidden_columns: &[String],
        kernel_predicate: Option<crate::delta::kernel::DeltaKernelPredicate>,
        include_stats: bool,
        execution_options: crate::DeltaScanExecutionOptions,
    ) -> Result<DeltaScanPlan, crate::DeltaReaderError> {
        super::plan_scan(
            snapshot,
            projection,
            hidden_columns,
            kernel_predicate,
            include_stats,
            execution_options,
            Default::default(),
        )
    }

    #[test]
    fn file_task_preserves_kernel_metadata_without_execution()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut file = kernel_file("part-00000.parquet");
        file.dv_info = DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::Inline,
            "inline-payload",
            None,
            14,
            2,
        )?
        .into();
        file.transform = Some(Arc::new(Expression::Column(ColumnName::new([
            "physical_id",
        ]))));

        let task = task(file)?;

        assert_eq!(task.path, "part-00000.parquet");
        assert_eq!(task.file_size, Some(123));
        assert_eq!(task.estimated_rows, Some(7));
        assert_eq!(task.stats.as_ref().map(|stats| stats.num_records), Some(7));
        assert_eq!(task.modification_time_ms, Some(1_587_968_586_000));
        assert_eq!(
            task.partition_values.into_iter().collect::<Vec<_>>(),
            [
                ("day".to_owned(), "2026-06-11".to_owned()),
                ("region".to_owned(), "us-west".to_owned()),
            ]
        );
        assert!(task.deletion_vector.is_present());
        assert!(task.transform.is_required());
        assert!(task.transform.into_inner().is_some());

        Ok(())
    }

    #[test]
    fn file_task_preserves_zero_and_missing_estimates() -> Result<(), Box<dyn std::error::Error>> {
        let mut file = kernel_file("empty.parquet");
        file.size = 0;
        file.stats = None;

        let task = task(file)?;

        assert_eq!(task.file_size, Some(0));
        assert_eq!(task.estimated_rows, None);
        assert!(task.stats.is_none());
        assert!(!task.deletion_vector.is_present());
        assert!(!task.transform.is_required());

        Ok(())
    }

    #[test]
    fn file_task_rejects_negative_size_without_disclosing_path() {
        let mut file = kernel_file("secret-file.parquet");
        file.size = -1;

        let error = match task(file) {
            Ok(_) => panic!("negative size must fail"),
            Err(error) => error,
        };
        let display = error.to_string();

        assert_eq!(error.code(), "scan_planning");
        assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
        assert!(!display.contains("secret-file"));
        assert!(!format!("{error:?}").contains("secret-file"));
    }

    #[test]
    fn file_task_planning_exhausts_empty_single_and_multi_batch_scans()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_empty_table, empty_snapshot) = loaded_snapshot("empty-files")?;
        let execution_options = crate::DeltaScanExecutionOptions::default();
        let empty = plan_scan(&empty_snapshot, None, &[], None, true, execution_options)?;
        assert!(empty.partitions.is_empty());
        assert_eq!(empty.estimated_bytes, Some(0));
        assert_eq!(empty.estimated_rows, Some(0));
        let empty_metrics = empty.metrics.snapshot();
        assert_eq!(empty_metrics.snapshot_version, empty.snapshot_version);
        assert_eq!(
            empty_metrics.parquet_backend,
            crate::ParquetReaderBackend::Direct
        );
        assert_eq!(empty_metrics.scan_partitions_planned, 0);
        assert_eq!(empty_metrics.files_planned, 0);
        assert_eq!(
            empty_metrics.add_actions_filtered_during_planning,
            empty.add_actions_filtered_during_planning
        );
        assert_eq!(empty_metrics.estimated_input_bytes, Some(0));
        assert_eq!(empty_metrics.estimated_input_rows, Some(0));
        assert_eq!(empty_metrics.scan_partitions_started, 0);
        assert_eq!(empty_metrics.scan_partitions_completed, 0);
        assert_eq!(empty_metrics.file_tasks_started, 0);
        assert_eq!(empty_metrics.file_tasks_completed, 0);
        assert_eq!(empty_metrics.scheduler_batches_emitted, 0);
        assert_eq!(empty_metrics.scheduler_rows_emitted, 0);
        assert_eq!(empty_metrics.deletion_vector_payloads_loaded, 0);
        assert_eq!(empty_metrics.deletion_vectors_applied, 0);
        assert_eq!(empty_metrics.deletion_vector_rows_deleted, 0);
        assert_eq!(empty_metrics.deletion_vector_failures, 0);
        assert_eq!(empty_metrics.deletion_vector_rejections, 0);
        assert_eq!(
            empty_metrics.parquet_data_file_range_get_operations,
            Some(0)
        );
        assert_eq!(empty_metrics.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(empty_metrics.parquet_data_file_bytes_received, Some(0));
        assert_eq!(empty_metrics.estimated_parquet_task_bytes_admitted, Some(0));

        let single_add = [add("single.parquet", 0, None)];
        let (_single_table, single_snapshot) =
            loaded_snapshot_with_adds("single-file", &single_add)?;
        let single = plan_scan(&single_snapshot, None, &[], None, true, Default::default())?;
        let single_task = planned_tasks(&single).next().ok_or("expected one task")?;
        assert_eq!(planned_tasks(&single).count(), 1);
        assert_eq!(single_task.path, "single.parquet");
        assert_eq!(single_task.file_size, Some(0));
        assert_eq!(single.estimated_bytes, Some(0));
        assert_eq!(single.estimated_rows, None);

        let adds = (0_u32..1_001)
            .map(|index| {
                add(
                    &format!("part-{index:04}.parquet"),
                    i64::from(index),
                    (index % 2 == 0).then_some(u64::from(index)),
                )
            })
            .collect::<Vec<_>>();
        let (_many_table, many_snapshot) = loaded_snapshot_with_adds("many-files", &adds)?;
        let projection = ["label".to_owned(), "id".to_owned()];
        let many = plan_scan(
            &many_snapshot,
            Some(&projection),
            &[],
            None,
            true,
            Default::default(),
        )?;

        assert_eq!(planned_tasks(&many).count(), adds.len());
        let first = planned_tasks(&many)
            .find(|task| task.path == "part-0000.parquet")
            .ok_or("expected first task")?;
        let second = planned_tasks(&many)
            .find(|task| task.path == "part-0001.parquet")
            .ok_or("expected second task")?;
        let last = planned_tasks(&many)
            .find(|task| task.path == "part-1000.parquet")
            .ok_or("expected last task")?;
        assert_eq!(first.file_size, Some(0));
        assert_eq!(second.estimated_rows, None);
        assert_eq!(last.file_size, Some(1_000));
        assert_eq!(last.estimated_rows, Some(1_000));
        assert_eq!(many.add_actions_filtered_during_planning, Some(0));
        assert_eq!(many.estimated_bytes, Some(500_500));
        assert_eq!(many.estimated_rows, None);
        assert!(Arc::ptr_eq(
            &many.engine_context,
            many_snapshot.engine_context()
        ));
        assert_eq!(many.snapshot_version, many_snapshot.version());
        assert_eq!(field_names(&many.logical_schema), ["label", "id"]);
        assert_eq!(field_names(&many.physical_schema), ["label", "id"]);
        assert_eq!(field_names(&many.projected_schema), ["label", "id"]);
        assert!(many.physical_predicate.is_none());
        assert_eq!(many.execution_options, Default::default());

        Ok(())
    }

    #[test]
    fn unpartitioned_plan_preserves_kernel_task_order_before_grouping()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add("first.parquet", 30, Some(3)),
            add("second.parquet", 10, Some(1)),
            add("third.parquet", 20, Some(2)),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("ordered-handoff", &adds)?;

        let plan =
            super::plan_unpartitioned_scan(&snapshot, None, &[], None, true, Default::default())?;

        assert_eq!(
            unpartitioned_tasks(&plan)
                .map(|task| task.path.as_str())
                .collect::<Vec<_>>(),
            ["first.parquet", "second.parquet", "third.parquet"]
        );
        assert_eq!(plan.file_tasks.len(), 3);
        assert_eq!(plan.add_actions_filtered_during_planning, Some(0));
        assert_eq!(plan.estimated_bytes, Some(60));
        assert_eq!(plan.estimated_rows, Some(6));
        assert!(Arc::ptr_eq(&plan.engine_context, snapshot.engine_context()));
        Ok(())
    }

    #[test]
    fn statistics_pruning_preserves_surviving_row_counts_and_deletion_vectors()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            dv_add_action(
                "id-dv-impossible.parquet",
                10,
                serde_json::json!({}),
                Some(
                    r#"{"numRecords":10,"minValues":{"id":1},"maxValues":{"id":50},"nullCount":{"id":0}}"#,
                ),
            ),
            dv_add_action(
                "id-dv-possible.parquet",
                12,
                serde_json::json!({}),
                Some(
                    r#"{"numRecords":12,"minValues":{"id":101},"maxValues":{"id":150},"nullCount":{"id":0}}"#,
                ),
            ),
            add_with_stats(
                "id-plain-impossible.parquet",
                10,
                Some(
                    r#"{"numRecords":10,"minValues":{"id":1},"maxValues":{"id":50},"nullCount":{"id":0}}"#,
                ),
            ),
            add("id-plain-missing-stats.parquet", 5, None),
            dv_add_action(
                "id-dv-missing-stats.parquet",
                6,
                serde_json::json!({}),
                None,
            ),
        ];
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "stats-metadata-handoff",
            DELETION_VECTOR_PROTOCOL_JSON,
            METADATA_JSON,
            &adds,
        )?;
        let snapshot = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        let predicate = DeltaPredicate::Compare {
            column: "id".to_owned(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(100),
        };
        validate_predicate(&predicate, snapshot.schema().as_ref())?;
        let kernel_predicate =
            delta_predicate_to_kernel_pruning(&predicate).ok_or("expected Kernel predicate")?;

        let plan = super::plan_unpartitioned_scan(
            &snapshot,
            None,
            &[],
            Some(kernel_predicate),
            true,
            Default::default(),
        )?;

        assert_eq!(
            unpartitioned_tasks(&plan)
                .map(|task| task.path.as_str())
                .collect::<Vec<_>>(),
            [
                "id-dv-possible.parquet",
                "id-plain-missing-stats.parquet",
                "id-dv-missing-stats.parquet",
            ]
        );
        let possible = plan
            .file_tasks
            .iter()
            .find(|task| task.path == "id-dv-possible.parquet")
            .ok_or("expected surviving DV task")?;
        assert_eq!(possible.estimated_rows, Some(12));
        assert_eq!(
            possible.stats.as_ref().map(|stats| stats.num_records),
            Some(12)
        );
        assert!(possible.deletion_vector.is_present());
        let missing_stats = plan
            .file_tasks
            .iter()
            .find(|task| task.path == "id-dv-missing-stats.parquet")
            .ok_or("expected surviving missing-stats DV task")?;
        assert_eq!(missing_stats.estimated_rows, None);
        assert!(missing_stats.stats.is_none());
        assert!(missing_stats.deletion_vector.is_present());
        let plain = plan
            .file_tasks
            .iter()
            .find(|task| task.path == "id-plain-missing-stats.parquet")
            .ok_or("expected surviving plain task")?;
        assert!(!plain.deletion_vector.is_present());
        assert_eq!(plan.add_actions_filtered_during_planning, Some(2));
        assert_eq!(plan.estimated_rows, None);
        Ok(())
    }

    #[test]
    fn partition_pruning_preserves_surviving_deletion_vectors()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            dv_add_action(
                "region-us-west-dv.parquet",
                10,
                serde_json::json!({"region": "us-west"}),
                None,
            ),
            add_action(
                "region-us-west-plain.parquet",
                10,
                r#"{"region":"us-west"}"#,
                None,
            ),
            dv_add_action(
                "region-us-east-dv.parquet",
                10,
                serde_json::json!({"region": "us-east"}),
                None,
            ),
            add_action(
                "region-us-east-plain.parquet",
                10,
                r#"{"region":"us-east"}"#,
                None,
            ),
        ];
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "partition-metadata-handoff",
            DELETION_VECTOR_PROTOCOL_JSON,
            PARTITIONED_METADATA_JSON,
            &adds,
        )?;
        let snapshot = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        let predicate = DeltaPredicate::Compare {
            column: "region".to_owned(),
            op: DeltaComparison::Eq,
            value: DeltaScalar::Utf8("us-west".to_owned()),
        };
        validate_predicate(&predicate, snapshot.schema().as_ref())?;
        let kernel_predicate =
            delta_predicate_to_kernel_pruning(&predicate).ok_or("expected Kernel predicate")?;

        let plan = super::plan_unpartitioned_scan(
            &snapshot,
            None,
            &[],
            Some(kernel_predicate),
            false,
            Default::default(),
        )?;

        assert_eq!(
            unpartitioned_tasks(&plan)
                .map(|task| task.path.as_str())
                .collect::<Vec<_>>(),
            ["region-us-west-dv.parquet", "region-us-west-plain.parquet"]
        );
        assert!(plan.file_tasks[0].deletion_vector.is_present());
        assert!(!plan.file_tasks[1].deletion_vector.is_present());
        assert_eq!(plan.add_actions_filtered_during_planning, Some(2));
        Ok(())
    }

    #[test]
    fn file_task_planning_returns_no_partial_result() -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add("first.parquet", 1, Some(1)),
            add("secret-invalid.parquet", -1, Some(1)),
            add("last.parquet", 1, Some(1)),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("all-or-error", &adds)?;

        let error = match plan_scan(&snapshot, None, &[], None, true, Default::default()) {
            Ok(_) => return Err("invalid task must fail the plan".into()),
            Err(error) => error,
        };

        assert_eq!(error.code(), "scan_planning");
        assert!(!error.to_string().contains("secret-invalid"));

        Ok(())
    }

    #[test]
    fn scan_rejects_unsupported_protocol_before_metadata_expansion()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "unsupported-protocol",
            UNSUPPORTED_PROTOCOL_JSON,
            METADATA_JSON,
            &[add("secret-invalid.parquet", -1, Some(1))],
        )?;
        let snapshot = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;

        let error = match plan_scan(&snapshot, None, &[], None, true, Default::default()) {
            Ok(_) => return Err("unsupported protocol must fail before metadata expansion".into()),
            Err(error) => error,
        };

        assert_eq!(error.phase(), DeltaReaderPhase::Protocol);
        assert_eq!(error.code(), "unsupported_protocol");
        assert!(!error.to_string().contains("madeUpFeature"));
        assert!(!error.to_string().contains("secret-invalid.parquet"));
        Ok(())
    }

    #[test]
    fn scan_metadata_visitor_failure_returns_no_partial_plan_or_sensitive_context()
    -> Result<(), Box<dyn std::error::Error>> {
        const INVALID_VALUE: &str = "secret-not-an-integer";
        let adds = [add_action(
            "secret-invalid-partition.parquet",
            1,
            &format!(r#"{{"long_part":"{INVALID_VALUE}"}}"#),
            None,
        )];
        let (_table, snapshot) = loaded_snapshot_with_metadata_and_adds(
            "invalid-partition",
            INVALID_PARTITION_METADATA_JSON,
            &adds,
        )?;

        let error = match plan_scan(&snapshot, None, &[], None, false, Default::default()) {
            Ok(_) => return Err("invalid partition metadata must fail the whole plan".into()),
            Err(error) => error,
        };
        let display = error.to_string();
        let debug = format!("{error:?}");

        assert_eq!(error.code(), "scan_planning");
        assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
        assert!(!display.contains(INVALID_VALUE));
        assert!(!display.contains("secret-invalid-partition"));
        assert!(!debug.contains(INVALID_VALUE));
        assert!(!debug.contains("secret-invalid-partition"));

        Ok(())
    }

    #[test]
    fn aggregate_estimates_are_exact_unknown_or_rejected_on_overflow()
    -> Result<(), crate::DeltaReaderError> {
        assert_eq!(checked_sum([], "overflow")?, Some(0));
        assert_eq!(checked_sum([Some(2), Some(3)], "overflow")?, Some(5));
        assert_eq!(checked_sum([Some(2), None, Some(3)], "overflow")?, None);
        assert!(checked_sum([Some(u64::MAX), Some(1)], "overflow").is_err());
        Ok(())
    }

    #[test]
    fn partition_grouping_rejects_zero_target() {
        let error = match group_scan_file_tasks(Vec::new(), 0) {
            Ok(_) => panic!("zero partition target must fail"),
            Err(error) => error,
        };

        assert_eq!(error.code(), "invalid_configuration");
        assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
    }

    #[test]
    fn empty_file_task_list_returns_no_partitions() -> Result<(), Box<dyn std::error::Error>> {
        assert!(group_scan_file_tasks(Vec::new(), 4)?.is_empty());
        Ok(())
    }

    #[test]
    fn oversized_known_size_file_stays_whole() -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("huge.parquet", Some(1_000), Some(100))?,
                grouping_task("small-0.parquet", Some(10), Some(1))?,
                grouping_task("small-1.parquet", Some(10), Some(1))?,
            ],
            2,
        )?;

        assert_eq!(
            partition_paths(&partitions),
            vec![
                vec!["huge.parquet"],
                vec!["small-0.parquet", "small-1.parquet"]
            ]
        );
        assert_eq!(partitions[0].estimated_bytes, Some(1_000));
        assert_eq!(partitions[1].estimated_bytes, Some(20));
        Ok(())
    }

    #[test]
    fn known_size_files_group_by_estimated_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("large.parquet", Some(90), Some(9))?,
                grouping_task("small-1.parquet", Some(10), Some(1))?,
                grouping_task("small-2.parquet", Some(10), Some(1))?,
                grouping_task("small-3.parquet", Some(10), Some(1))?,
            ],
            2,
        )?;

        assert_eq!(
            partition_paths(&partitions),
            vec![
                vec!["large.parquet"],
                vec!["small-1.parquet", "small-2.parquet", "small-3.parquet"],
            ]
        );
        assert_eq!(
            partitions
                .iter()
                .map(|partition| partition.estimated_bytes)
                .collect::<Vec<_>>(),
            vec![Some(90), Some(30)]
        );
        assert_eq!(
            partitions
                .iter()
                .map(|partition| partition.estimated_rows)
                .collect::<Vec<_>>(),
            vec![Some(9), Some(3)]
        );
        Ok(())
    }

    #[test]
    fn known_size_files_use_stable_order_and_lowest_partition_tie_breaker()
    -> Result<(), Box<dyn std::error::Error>> {
        fn grouped_paths() -> Result<Vec<Vec<String>>, crate::DeltaReaderError> {
            group_scan_file_tasks(
                vec![
                    grouping_task("part-0.parquet", Some(6), Some(1))?,
                    grouping_task("part-1.parquet", Some(6), Some(1))?,
                    grouping_task("part-2.parquet", Some(4), Some(1))?,
                    grouping_task("part-3.parquet", Some(4), Some(1))?,
                ],
                2,
            )
            .map(|partitions| {
                partitions
                    .into_iter()
                    .map(|partition| {
                        partition
                            .file_tasks
                            .into_iter()
                            .map(|task| task.path)
                            .collect()
                    })
                    .collect()
            })
        }

        let expected = vec![
            vec!["part-0.parquet".to_owned(), "part-2.parquet".to_owned()],
            vec!["part-1.parquet".to_owned(), "part-3.parquet".to_owned()],
        ];
        assert_eq!(grouped_paths()?, expected);
        assert_eq!(grouped_paths()?, expected);
        Ok(())
    }

    #[test]
    fn known_size_files_do_not_accumulate_slack_in_the_last_partition()
    -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("part-0.parquet", Some(6), Some(1))?,
                grouping_task("part-1.parquet", Some(6), Some(1))?,
                grouping_task("part-2.parquet", Some(6), Some(1))?,
                grouping_task("part-3.parquet", Some(6), Some(1))?,
                grouping_task("part-4.parquet", Some(4), Some(1))?,
                grouping_task("part-5.parquet", Some(4), Some(1))?,
            ],
            2,
        )?;

        assert_eq!(
            partitions
                .iter()
                .map(|partition| partition.estimated_bytes)
                .collect::<Vec<_>>(),
            vec![Some(16), Some(16)]
        );
        Ok(())
    }

    #[test]
    fn mixed_zero_byte_files_keep_partitions_non_empty() -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("non-zero.parquet", Some(10), Some(1))?,
                grouping_task("zero-0.parquet", Some(0), Some(0))?,
                grouping_task("zero-1.parquet", Some(0), Some(0))?,
            ],
            3,
        )?;

        assert_eq!(
            partition_paths(&partitions),
            vec![
                vec!["non-zero.parquet"],
                vec!["zero-0.parquet"],
                vec!["zero-1.parquet"]
            ]
        );
        assert!(
            partitions
                .iter()
                .all(|partition| !partition.file_tasks.is_empty())
        );
        Ok(())
    }

    #[test]
    fn target_above_file_count_emits_one_partition_per_task()
    -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("part-0.parquet", Some(10), Some(1))?,
                grouping_task("part-1.parquet", Some(10), Some(1))?,
            ],
            8,
        )?;

        assert_eq!(partitions.len(), 2);
        assert_eq!(
            partition_paths(&partitions),
            vec![vec!["part-0.parquet"], vec!["part-1.parquet"]]
        );
        Ok(())
    }

    #[test]
    fn unknown_size_files_fallback_to_scan_order_file_count_balancing()
    -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("part-0.parquet", None, Some(1))?,
                grouping_task("part-1.parquet", Some(10), Some(1))?,
                grouping_task("part-2.parquet", Some(10), Some(1))?,
                grouping_task("part-3.parquet", Some(10), Some(1))?,
                grouping_task("part-4.parquet", Some(10), Some(1))?,
            ],
            2,
        )?;

        assert_eq!(
            partition_paths(&partitions),
            vec![
                vec!["part-0.parquet", "part-1.parquet", "part-2.parquet"],
                vec!["part-3.parquet", "part-4.parquet"]
            ]
        );
        assert_eq!(partitions[0].estimated_bytes, None);
        assert_eq!(partitions[1].estimated_bytes, Some(20));
        Ok(())
    }

    #[test]
    fn all_zero_byte_files_use_scan_order_file_count_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("zero-0.parquet", Some(0), Some(0))?,
                grouping_task("zero-1.parquet", Some(0), Some(0))?,
            ],
            4,
        )?;

        assert_eq!(
            partition_paths(&partitions),
            vec![vec!["zero-0.parquet"], vec!["zero-1.parquet"]]
        );
        assert_eq!(partitions[0].estimated_bytes, Some(0));
        assert_eq!(partitions[1].estimated_bytes, Some(0));
        assert_eq!(partitions[0].estimated_rows, Some(0));
        assert_eq!(partitions[1].estimated_rows, Some(0));
        Ok(())
    }

    #[test]
    fn each_input_file_task_appears_exactly_once() -> Result<(), Box<dyn std::error::Error>> {
        for target in [1, 4, 8] {
            let partitions = group_scan_file_tasks(
                vec![
                    grouping_task("part-0.parquet", Some(10), Some(1))?,
                    grouping_task("part-1.parquet", Some(10), Some(1))?,
                    grouping_task("part-2.parquet", Some(10), Some(1))?,
                    grouping_task("part-3.parquet", Some(10), Some(1))?,
                ],
                target,
            )?;
            let mut paths = partitions
                .iter()
                .flat_map(|partition| partition.file_tasks.iter())
                .map(|task| task.path.as_str())
                .collect::<Vec<_>>();
            paths.sort_unstable();

            assert_eq!(
                paths,
                vec![
                    "part-0.parquet",
                    "part-1.parquet",
                    "part-2.parquet",
                    "part-3.parquet"
                ]
            );
            assert!(
                partitions
                    .iter()
                    .all(|partition| !partition.file_tasks.is_empty())
            );
            assert!(partitions.len() <= target.min(4));
        }
        Ok(())
    }

    #[test]
    fn grouped_tasks_preserve_delta_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let mut file = kernel_file("part-with-delta-metadata.parquet");
        file.dv_info = DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::Inline,
            "inline-payload",
            None,
            14,
            2,
        )?
        .into();
        file.transform = Some(Arc::new(Expression::Column(ColumnName::new([
            "physical_id",
        ]))));
        let partitions = group_scan_file_tasks(vec![task(file)?], 1)?;
        let grouped = &partitions[0].file_tasks[0];

        assert_eq!(grouped.path, "part-with-delta-metadata.parquet");
        assert_eq!(grouped.file_size, Some(123));
        assert_eq!(grouped.estimated_rows, Some(7));
        assert_eq!(
            grouped.partition_values.get("region").map(String::as_str),
            Some("us-west")
        );
        assert!(grouped.deletion_vector.is_present());
        assert!(grouped.transform.is_required());
        Ok(())
    }

    #[test]
    fn unknown_rows_keep_partition_row_estimates_unknown() -> Result<(), Box<dyn std::error::Error>>
    {
        let partitions = group_scan_file_tasks(
            vec![
                grouping_task("part-0.parquet", Some(10), None)?,
                grouping_task("part-1.parquet", Some(10), Some(1))?,
            ],
            1,
        )?;

        assert_eq!(partitions[0].estimated_rows, None);
        Ok(())
    }

    #[test]
    fn grouping_reports_estimate_overflow_without_disclosing_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let byte_error = group_scan_file_tasks(
            vec![
                grouping_task("secret-byte.parquet", Some(u64::MAX), Some(1))?,
                grouping_task("other.parquet", Some(1), Some(1))?,
            ],
            1,
        )
        .err()
        .ok_or("byte overflow must fail")?;
        assert_eq!(byte_error.code(), "scan_partition_planning");
        assert_eq!(byte_error.phase(), DeltaReaderPhase::ScanPlanning);
        assert!(!byte_error.to_string().contains("secret-byte"));

        let row_error = group_scan_file_tasks(
            vec![
                grouping_task("secret-row.parquet", Some(1), Some(u64::MAX))?,
                grouping_task("other.parquet", Some(1), Some(1))?,
            ],
            1,
        )
        .err()
        .ok_or("row overflow must fail")?;
        assert_eq!(row_error.code(), "scan_partition_planning");
        assert!(!row_error.to_string().contains("secret-row"));
        Ok(())
    }

    #[test]
    fn final_scan_plan_groups_once_and_initializes_one_shared_metrics_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add("part-0.parquet", 40, Some(4)),
            add("part-1.parquet", 30, Some(3)),
            add("part-2.parquet", 20, Some(2)),
            add("part-3.parquet", 10, Some(1)),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("final-plan", &adds)?;
        let plan = super::plan_scan(
            &snapshot,
            None,
            &[],
            None,
            true,
            Default::default(),
            super::DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(2),
                caller_target_partitions: Some(1),
            },
        )?;

        assert_eq!(plan.snapshot_version, snapshot.version());
        assert!(Arc::ptr_eq(&plan.engine_context, snapshot.engine_context()));
        assert_eq!(plan.partition_target_diagnostic.target_partitions, 2);
        assert_eq!(
            plan.partition_target_diagnostic.source,
            crate::diagnostics::partition_target::Source::ExplicitOverride
        );
        assert_eq!(
            partition_paths(&plan.partitions),
            vec![
                vec!["part-0.parquet", "part-3.parquet"],
                vec!["part-1.parquet", "part-2.parquet"]
            ]
        );
        assert_eq!(plan.estimated_bytes, Some(100));
        assert_eq!(plan.estimated_rows, Some(10));

        let retained_metrics = plan.metrics.clone();
        let metrics = retained_metrics.snapshot();
        assert_eq!(metrics.snapshot_version, snapshot.version());
        assert_eq!(metrics.parquet_backend, crate::ParquetReaderBackend::Direct);
        assert_eq!(metrics.scan_partitions_planned, 2);
        assert_eq!(metrics.files_planned, 4);
        assert_eq!(metrics.add_actions_filtered_during_planning, Some(0));
        assert_eq!(metrics.estimated_input_rows, Some(10));
        assert_eq!(metrics.estimated_input_bytes, Some(100));
        assert_eq!(metrics.scan_partitions_started, 0);
        assert_eq!(metrics.scan_partitions_completed, 0);
        assert_eq!(metrics.file_tasks_started, 0);
        assert_eq!(metrics.file_tasks_completed, 0);
        assert_eq!(metrics.scheduler_batches_emitted, 0);
        assert_eq!(metrics.scheduler_rows_emitted, 0);
        assert_eq!(metrics.deletion_vector_payloads_loaded, 0);
        assert_eq!(metrics.deletion_vectors_applied, 0);
        assert_eq!(metrics.deletion_vector_rows_deleted, 0);
        assert_eq!(metrics.deletion_vector_failures, 0);
        assert_eq!(metrics.deletion_vector_rejections, 0);
        assert_eq!(metrics.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(metrics.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(metrics.parquet_data_file_bytes_received, Some(0));
        assert_eq!(metrics.estimated_parquet_task_bytes_admitted, Some(0));

        plan.metrics.record_deletion_vector_failure();
        assert_eq!(retained_metrics.snapshot().deletion_vector_failures, 1);
        Ok(())
    }

    #[tokio::test]
    async fn scan_execution_binds_plan_tasks_options_and_shared_metrics()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add("part-0.parquet", 20, Some(2)),
            add("part-1.parquet", 10, Some(1)),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("scan-execution", &adds)?;
        let execution_options = crate::DeltaScanExecutionOptions::new()
            .with_prefetch_files_per_partition(0)
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?
            .with_output_buffer_batches_per_partition(2)?;
        let plan = Arc::new(super::plan_scan(
            &snapshot,
            None,
            &[],
            None,
            true,
            execution_options,
            super::DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(2),
                caller_target_partitions: None,
            },
        )?);
        let paths = Arc::new(Mutex::new(Vec::new()));
        let admission: FileAdmissionFn<DeltaScanFileTask> = Arc::new(|_| Ok(FileAdmission::Admit));
        let executor: FileExecutor<DeltaScanFileTask, FileBatchStream> = {
            let paths = Arc::clone(&paths);
            let schema = Arc::clone(&plan.logical_schema);
            Arc::new(move |task, permit, _| {
                paths
                    .lock()
                    .expect("paths lock is available")
                    .push(task.path);
                let batch = RecordBatch::new_empty(Arc::clone(&schema));
                async move { Ok(execution_file_stream(permit, batch)) }.boxed()
            })
        };
        let scheduler = DeltaScanScheduler::new(Arc::clone(&plan));

        for partition in 0..plan.partitions.len() {
            let batches = scheduler
                .partition_stream(partition, Arc::clone(&admission), Arc::clone(&executor))?
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(batches.len(), 1);
        }
        let invalid = match scheduler.partition_stream(
            plan.partitions.len(),
            Arc::clone(&admission),
            Arc::clone(&executor),
        ) {
            Ok(_) => return Err("out-of-range execution partition must fail".into()),
            Err(error) => error,
        };
        assert_eq!(invalid.code(), "invalid_configuration");

        let cancelled_scheduler = DeltaScanScheduler::new(Arc::clone(&plan));
        drop(cancelled_scheduler.partition_stream(
            0,
            Arc::clone(&admission),
            Arc::clone(&executor),
        )?);
        let repeated_scheduler = DeltaScanScheduler::new(Arc::clone(&plan));
        let repeated = repeated_scheduler
            .partition_stream(0, admission, executor)?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(repeated.len(), 1);

        let mut paths = paths.lock().expect("paths lock is available").clone();
        paths.sort_unstable();
        assert_eq!(
            paths,
            ["part-0.parquet", "part-0.parquet", "part-1.parquet"]
        );
        let metrics = plan.metrics.snapshot();
        assert_eq!(metrics.scan_partitions_started, 3);
        assert_eq!(metrics.scan_partitions_completed, 3);
        assert_eq!(metrics.file_tasks_started, 3);
        assert_eq!(metrics.file_tasks_completed, 3);
        assert_eq!(metrics.scheduler_batches_emitted, 3);
        assert_eq!(metrics.scheduler_rows_emitted, 0);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_scan_executions_share_only_the_plan()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [add("part.parquet", 10, Some(1))];
        let (_table, snapshot) = loaded_snapshot_with_adds("concurrent-executions", &adds)?;
        let execution_options = crate::DeltaScanExecutionOptions::new()
            .with_prefetch_files_per_partition(0)
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?;
        let plan = Arc::new(super::plan_scan(
            &snapshot,
            None,
            &[],
            None,
            true,
            execution_options,
            super::DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(1),
                caller_target_partitions: None,
            },
        )?);
        let calls = Arc::new(AtomicUsize::new(0));
        let executor: FileExecutor<DeltaScanFileTask, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            Arc::new(move |_, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok(pending_execution_file_stream(permit)) }.boxed()
            })
        };
        let admission: FileAdmissionFn<DeltaScanFileTask> = Arc::new(|_| Ok(FileAdmission::Admit));
        let first_scheduler = DeltaScanScheduler::new(Arc::clone(&plan));
        let second_scheduler = DeltaScanScheduler::new(Arc::clone(&plan));
        let mut first =
            first_scheduler.partition_stream(0, Arc::clone(&admission), Arc::clone(&executor))?;
        let mut second = second_scheduler.partition_stream(0, admission, executor)?;
        let mut first_next = Box::pin(first.next());
        poll_fn(|context| {
            assert!(matches!(first_next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let mut second_next = Box::pin(second.next());
        poll_fn(|context| {
            assert!(matches!(second_next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        drop(first_next);
        drop(first);
        poll_fn(|context| {
            assert!(matches!(second_next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        drop(second_next);
        drop(second);
        let metrics = plan.metrics.snapshot();
        assert_eq!(metrics.scan_partitions_started, 2);
        assert_eq!(metrics.scan_partitions_completed, 0);
        assert_eq!(metrics.file_tasks_started, 2);
        assert_eq!(metrics.file_tasks_completed, 0);
        Ok(())
    }

    #[test]
    fn invalid_final_plan_target_fails_before_file_task_expansion()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [add("secret-invalid.parquet", -1, Some(1))];
        let (_table, snapshot) = loaded_snapshot_with_adds("invalid-final-target", &adds)?;
        let error = super::plan_scan(
            &snapshot,
            None,
            &[],
            None,
            true,
            Default::default(),
            super::DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(0),
                caller_target_partitions: None,
            },
        )
        .err()
        .ok_or("zero target must fail")?;

        assert_eq!(error.code(), "invalid_configuration");
        assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
        assert!(!error.to_string().contains("secret-invalid"));
        Ok(())
    }

    #[test]
    fn final_scan_plan_rejects_aggregate_overflow() -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add("secret-0.parquet", i64::MAX, Some(1)),
            add("secret-1.parquet", i64::MAX, Some(1)),
            add("secret-2.parquet", i64::MAX, Some(1)),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("overflow-final-plan", &adds)?;
        let error = plan_scan(&snapshot, None, &[], None, true, Default::default())
            .err()
            .ok_or("aggregate byte overflow must fail")?;

        assert_eq!(error.code(), "scan_partition_planning");
        assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
        assert!(!error.to_string().contains("secret-"));
        Ok(())
    }

    #[test]
    fn scan_plan_keeps_hidden_columns_and_applies_static_stats_pruning()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add_with_stats(
                "impossible.parquet",
                10,
                Some(
                    r#"{"numRecords":2,"minValues":{"hidden":1},"maxValues":{"hidden":10},"nullCount":{"hidden":0}}"#,
                ),
            ),
            add_with_stats(
                "possible.parquet",
                20,
                Some(
                    r#"{"numRecords":3,"minValues":{"hidden":101},"maxValues":{"hidden":200},"nullCount":{"hidden":0}}"#,
                ),
            ),
            add("missing-stats.parquet", 30, None),
        ];
        let (_table, snapshot) = loaded_snapshot_with_adds("stats-pruning", &adds)?;
        let predicate = DeltaPredicate::Compare {
            column: "hidden".to_owned(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(100),
        };
        validate_predicate(&predicate, snapshot.schema().as_ref())?;
        let kernel_predicate =
            delta_predicate_to_kernel_pruning(&predicate).ok_or("expected Kernel predicate")?;
        let projection = ["label".to_owned()];
        let hidden = ["hidden".to_owned()];

        let plan = plan_scan(
            &snapshot,
            Some(&projection),
            &hidden,
            Some(kernel_predicate),
            true,
            Default::default(),
        )?;

        assert_eq!(field_names(&plan.logical_schema), ["label", "hidden"]);
        assert_eq!(field_names(&plan.physical_schema), ["label", "hidden"]);
        assert_eq!(field_names(&plan.projected_schema), ["label"]);
        let mut paths = planned_tasks(&plan)
            .map(|task| task.path.as_str())
            .collect::<Vec<_>>();
        paths.sort_unstable();
        assert_eq!(paths, ["missing-stats.parquet", "possible.parquet"]);
        assert_eq!(plan.add_actions_filtered_during_planning, Some(1));
        assert_eq!(plan.estimated_bytes, Some(50));
        assert_eq!(plan.estimated_rows, None);
        assert!(plan.physical_predicate.is_some());

        let empty_projection = Vec::new();
        let empty = plan_scan(
            &snapshot,
            Some(&empty_projection),
            &hidden,
            None,
            false,
            Default::default(),
        )?;
        assert_eq!(field_names(&empty.logical_schema), ["hidden"]);
        assert_eq!(field_names(&empty.physical_schema), ["hidden"]);
        assert!(empty.projected_schema.fields().is_empty());

        Ok(())
    }

    #[test]
    fn scan_plan_applies_one_transform_without_copying_no_transform_batches()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [add("part.parquet", 10, Some(3))];
        let (_table, snapshot) = loaded_snapshot_with_adds("transforms", &adds)?;
        let projection = ["id".to_owned(), "label".to_owned()];
        let mut plan = plan_scan(
            &snapshot,
            Some(&projection),
            &[],
            None,
            false,
            Default::default(),
        )?;
        let mut task = plan
            .partitions
            .first_mut()
            .and_then(|partition| partition.file_tasks.pop())
            .ok_or("expected one task")?;
        let batch = || {
            RecordBatch::try_new(
                Arc::clone(&plan.physical_schema),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
        };

        let physical = batch()?;
        let first_column = Arc::clone(physical.column(0));
        let unchanged = plan.apply_transform(&task, physical)?;
        assert!(Arc::ptr_eq(&first_column, unchanged.column(0)));
        assert_eq!(unchanged.schema(), plan.logical_schema);

        let mismatch = plan
            .apply_transform(&task, RecordBatch::new_empty(Arc::new(Schema::empty())))
            .expect_err("wrong logical schema must fail");
        assert_eq!(mismatch.code(), "physical_to_logical_transform");
        assert_eq!(mismatch.phase(), DeltaReaderPhase::Transform);

        task.transform =
            KernelPhysicalToLogicalTransform::from_test_expression(Expression::struct_from([
                Expression::Column(ColumnName::new(["id"])),
                Expression::Literal(delta_kernel::expressions::Scalar::String(
                    "transformed".to_owned(),
                )),
            ]));
        let transformed = plan.apply_transform(&task, batch()?)?;
        let labels = transformed
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected string labels")?;

        assert_eq!(transformed.num_rows(), 3);
        assert_eq!(transformed.schema(), plan.logical_schema);
        assert_eq!(
            labels.iter().collect::<Vec<_>>(),
            [
                Some("transformed"),
                Some("transformed"),
                Some("transformed"),
            ]
        );

        task.transform = KernelPhysicalToLogicalTransform::from_test_expression(
            Expression::Column(ColumnName::new(["secret_missing"])),
        );
        let error = plan
            .apply_transform(&task, batch()?)
            .expect_err("invalid transform must fail");
        assert_eq!(error.code(), "physical_to_logical_transform");
        assert_eq!(error.phase(), DeltaReaderPhase::Transform);
        assert!(!error.to_string().contains("secret_missing"));
        assert!(!format!("{error:?}").contains("secret_missing"));

        Ok(())
    }

    #[test]
    fn scan_plan_applies_nested_kernel_column_mapping_transform()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [add("mapped.parquet", 10, Some(2))];
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "column-mapping-transform",
            COLUMN_MAPPING_PROTOCOL_JSON,
            COLUMN_MAPPING_METADATA_JSON,
            &adds,
        )?;
        let snapshot = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        let plan = plan_scan(&snapshot, None, &[], None, false, Default::default())?;
        let task = planned_tasks(&plan)
            .next()
            .ok_or("expected one mapped task")?;

        assert_eq!(
            field_names(&plan.physical_schema),
            ["phys_id", "phys_customer_name", "phys_profile"]
        );
        assert_eq!(
            field_names(&plan.logical_schema),
            ["id", "customer_name", "profile"]
        );
        assert!(task.transform.is_required());
        let DataType::Struct(profile_fields) = plan.physical_schema.field(2).data_type() else {
            return Err("expected a physical profile struct".into());
        };
        assert_eq!(profile_fields[0].name(), "phys_first_name");
        assert_eq!(profile_fields[1].name(), "phys_age");
        let profile = StructArray::new(
            profile_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("alice"), None])),
                Arc::new(Int32Array::from(vec![Some(30), None])),
            ],
            None,
        );

        let physical = RecordBatch::try_new(
            Arc::clone(&plan.physical_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("customer-a"), None])),
                Arc::new(profile),
            ],
        )?;
        let logical = plan.apply_transform(task, physical)?;
        let profile = logical
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected mapped profile")?;
        let first_names = profile
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected mapped first names")?;
        let ages = profile
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected mapped ages")?;

        assert_eq!(logical.schema(), plan.logical_schema);
        assert_eq!(logical.num_rows(), 2);
        assert_eq!(profile.fields()[0].name(), "first_name");
        assert_eq!(profile.fields()[1].name(), "age");
        assert_eq!(
            first_names.iter().collect::<Vec<_>>(),
            [Some("alice"), None]
        );
        assert_eq!(ages.iter().collect::<Vec<_>>(), [Some(30), None]);

        Ok(())
    }

    #[test]
    fn row_predicate_planning_maps_logical_columns_to_physical_columns()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [add("mapped.parquet", 10, Some(3))];
        let table = DeltaLogTable::new_with_protocol_metadata_and_adds(
            "column-mapping-row-predicate",
            COLUMN_MAPPING_PROTOCOL_JSON,
            COLUMN_MAPPING_METADATA_JSON,
            &adds,
        )?;
        let snapshot = load_delta_table_snapshot_blocking(
            &table.0.to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        let predicate = delta_predicate_to_kernel_pruning(&DeltaPredicate::Compare {
            column: "id".to_owned(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(1),
        })
        .ok_or("expected Kernel predicate")?;
        let projection = ["id".to_owned()];
        let predicate =
            super::plan_row_predicate(&snapshot, Some(&projection), &[], Some(predicate))?
                .ok_or("expected physical row predicate")?;
        let plan = plan_scan(
            &snapshot,
            Some(&projection),
            &[],
            None,
            false,
            Default::default(),
        )?;
        let batch = RecordBatch::try_new(
            Arc::clone(&plan.physical_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        let selection = snapshot.engine_context().evaluate_predicate(
            &plan.kernel_schemas,
            &predicate,
            batch,
        )?;

        assert_eq!(field_names(&plan.physical_schema), ["phys_id"]);
        assert_eq!(
            selection.iter().collect::<Vec<_>>(),
            [Some(false), Some(true), Some(true)]
        );
        Ok(())
    }

    #[test]
    fn scan_plan_preserves_partition_pruning_transform_and_final_478_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let adds = [
            add_with_partition("east.parquet", 10, 1, "us-east"),
            add_with_partition("west.parquet", 20, 2, "us-west"),
        ];
        let (_table, snapshot) = loaded_snapshot_with_metadata_and_adds(
            "partition-transform",
            PARTITIONED_METADATA_JSON,
            &adds,
        )?;
        let predicate = DeltaPredicate::Compare {
            column: "region".to_owned(),
            op: DeltaComparison::Eq,
            value: DeltaScalar::Utf8("us-west".to_owned()),
        };
        validate_predicate(&predicate, snapshot.schema().as_ref())?;
        let kernel_predicate =
            delta_predicate_to_kernel_pruning(&predicate).ok_or("expected Kernel predicate")?;
        let projection = ["id".to_owned()];
        let hidden = ["region".to_owned()];
        let plan = plan_scan(
            &snapshot,
            Some(&projection),
            &hidden,
            Some(kernel_predicate),
            false,
            Default::default(),
        )?;

        assert_eq!(field_names(&plan.logical_schema), ["id", "region"]);
        assert_eq!(field_names(&plan.physical_schema), ["id"]);
        assert_eq!(field_names(&plan.projected_schema), ["id"]);
        let final_state = (
            plan.partitions.len(),
            planned_tasks(&plan).count(),
            plan.add_actions_filtered_during_planning,
            plan.estimated_bytes,
            plan.estimated_rows,
        );
        assert_eq!(final_state, (1, 1, Some(1), Some(20), Some(2)));
        assert!(Arc::ptr_eq(&plan.engine_context, snapshot.engine_context()));

        let task = planned_tasks(&plan)
            .next()
            .ok_or("expected one selected task")?;
        assert_eq!(task.path, "west.parquet");
        assert_eq!(task.stats.as_ref().map(|stats| stats.num_records), Some(2));
        assert_eq!(
            task.partition_values.get("region").map(String::as_str),
            Some("us-west")
        );
        assert!(task.transform.is_required());
        let physical = RecordBatch::try_new(
            Arc::clone(&plan.physical_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )?;
        let logical = plan.apply_transform(task, physical)?;
        let regions = logical
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected partition values")?;

        assert_eq!(logical.schema(), plan.logical_schema);
        assert_eq!(
            regions.iter().collect::<Vec<_>>(),
            [Some("us-west"), Some("us-west"),]
        );

        Ok(())
    }

    #[test]
    fn scan_preserves_full_ordered_and_empty_projections() -> Result<(), Box<dyn std::error::Error>>
    {
        let (_table, snapshot) = loaded_snapshot("projections")?;

        let full = build_scan(&snapshot, None, None, false)?;
        assert_eq!(
            field_names(&full.logical_schema()),
            ["id", "label", "hidden"]
        );
        assert_eq!(
            field_names(&full.physical_schema()),
            ["id", "label", "hidden"]
        );

        let ordered_names = ["label".to_owned(), "id".to_owned()];
        let ordered = build_scan(&snapshot, Some(&ordered_names), None, false)?;
        assert_eq!(field_names(&ordered.logical_schema()), ["label", "id"]);
        assert_eq!(field_names(&ordered.physical_schema()), ["label", "id"]);

        let empty_names = Vec::<String>::new();
        let empty = build_scan(&snapshot, Some(&empty_names), None, false)?;
        assert!(empty.logical_schema().fields().is_empty());
        assert!(empty.physical_schema().fields().is_empty());

        Ok(())
    }

    #[test]
    fn scan_keeps_metadata_predicate_out_of_projected_schemas()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, snapshot) = loaded_snapshot("hidden-predicate")?;
        let predicate = DeltaPredicate::Compare {
            column: "hidden".to_owned(),
            op: DeltaComparison::Gt,
            value: DeltaScalar::Int32(1),
        };
        validate_predicate(&predicate, snapshot.schema().as_ref())?;
        let kernel_predicate =
            delta_predicate_to_kernel_pruning(&predicate).ok_or("expected Kernel predicate")?;
        let projection = ["label".to_owned()];

        let scan = build_scan(&snapshot, Some(&projection), Some(kernel_predicate), false)?;

        assert_eq!(field_names(&scan.logical_schema()), ["label"]);
        assert_eq!(field_names(&scan.physical_schema()), ["label"]);
        assert!(scan.has_physical_predicate());

        Ok(())
    }

    #[test]
    fn scan_rejects_missing_and_duplicate_projections_without_disclosure()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_table, snapshot) = loaded_snapshot("invalid-projection")?;

        for projection in [
            vec!["secret-missing".to_owned()],
            vec!["id".to_owned(), "id".to_owned()],
        ] {
            let error = match build_scan(&snapshot, Some(&projection), None, false) {
                Ok(_) => return Err("invalid projection must fail".into()),
                Err(error) => error,
            };
            let display = error.to_string();

            assert_eq!(error.code(), "invalid_projection");
            assert_eq!(error.phase(), DeltaReaderPhase::ScanPlanning);
            assert!(!display.contains("secret-missing"));
            assert!(!format!("{error:?}").contains("secret-missing"));
        }

        let projection = ["id".to_owned()];
        let hidden = ["secret-hidden".to_owned()];
        let error = match plan_scan(
            &snapshot,
            Some(&projection),
            &hidden,
            None,
            false,
            Default::default(),
        ) {
            Ok(_) => return Err("invalid hidden column must fail".into()),
            Err(error) => error,
        };
        assert_eq!(error.code(), "invalid_projection");
        assert!(!error.to_string().contains("secret-hidden"));

        Ok(())
    }

    #[test]
    fn planning_boundary_contains_no_execution_or_second_engine() {
        let planning_source = include_str!("planning.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production planning source");
        let transform_source = include_str!("transform.rs");

        for forbidden in [
            "DefaultEngineBuilder",
            "store_from_url_opts",
            "Runtime::",
            "block_on(",
            "read_parquet",
            "get_row_indexes",
            "datafusion::",
            "MetricBuilder",
            "ExecutionPlanMetricsSet",
        ] {
            assert!(!planning_source.contains(forbidden), "{forbidden}");
            assert!(!transform_source.contains(forbidden), "{forbidden}");
        }
        assert!(!transform_source.contains("tracing::"));
    }
}
