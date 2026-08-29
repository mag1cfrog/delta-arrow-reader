//! Direct asynchronous Parquet data-file reader.

mod metadata_cache;
mod metered_object_store;
mod range_planning;
mod row_group_pruning;
mod schema_alignment;

use std::{ops::Range, sync::Arc};

#[cfg(feature = "experimental-parquet-metadata-warmup")]
use std::collections::HashSet;

use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use delta_kernel::expressions::ColumnName;
use futures_util::{StreamExt, stream};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use parquet::arrow::{
    ProjectionMask, RowNumber,
    arrow_reader::{ArrowPredicateFn, ArrowReaderMetadata, ArrowReaderOptions, RowFilter},
    async_reader::{
        AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStream,
        ParquetRecordBatchStreamBuilder,
    },
};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use parquet::schema::types::SchemaDescriptor;
use snafu::{IntoError, ResultExt};

pub(crate) use self::metadata_cache::ParquetMetadataCache;
pub(crate) use self::metered_object_store::ParquetRangeReadEstimator;
use self::{
    metered_object_store::{MeteredParquetObjectStore, MultiRangeReadStrategy},
    row_group_pruning::pruned_row_groups,
    schema_alignment::{ParquetSchemaAlignment, build_schema_alignment},
};

const ORIGINAL_ROW_INDEX_COLUMN: &str = "__delta_arrow_reader_original_row_index";

use crate::{
    DeltaReaderError, DeltaScanExecutionOptions, DeltaScanMetrics,
    delta::kernel::{
        DeltaKernelEngineContext, DeltaKernelPredicate, KernelPhysicalToLogicalTransform,
        KernelScanSchemas,
    },
    error::{CancelledSnafu, DataFileReadSnafu, PhysicalToLogicalTransformSnafu},
    reader::{
        deletion_vector::{DeletionVectorMasker, load_deletion_vector_masker},
        planning::{DeltaScanFileTask, DeltaScanPlan},
        scheduling::{FileBatchStream, FileExecutor, FileReadPermit, ScanCancellation},
        transform::{
            align_batch_to_logical_schema, schema_uses_view_types, schema_with_view_types,
        },
    },
};

#[cfg(feature = "experimental-parquet-metadata-warmup")]
use crate::reader::ParquetMetadataPreparationLimits;

struct DirectParquetReader {
    engine_context: Arc<DeltaKernelEngineContext>,
    store: Arc<dyn ObjectStore>,
    execution_options: DeltaScanExecutionOptions,
    metrics: DeltaScanMetrics,
    metadata_cache: Option<Arc<ParquetMetadataCache>>,
}

#[cfg(feature = "experimental-parquet-metadata-warmup")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PreparedParquetMetadataSummary {
    pub(crate) file_count: usize,
    pub(crate) memory_bytes: usize,
}

struct ParquetFileObject {
    store: Arc<dyn ObjectStore>,
    path: Path,
    file_size: u64,
}

struct PhysicalParquetStream {
    stream: ParquetRecordBatchStream<ParquetObjectReader>,
    schema_alignment: ParquetSchemaAlignment,
    include_original_row_index: bool,
}

#[derive(Default)]
struct PhysicalParquetStreamOptions<'a> {
    output_batch_size_rows: Option<usize>,
    row_group_predicate: Option<&'a DeltaKernelPredicate>,
    row_filter: Option<RowFilterInput<'a>>,
    include_original_row_index: bool,
}

struct RowFilterInput<'a> {
    predicate: &'a DeltaKernelPredicate,
    kernel_schemas: &'a KernelScanSchemas,
}

struct LogicalDataFileStream {
    physical_stream: PhysicalParquetStream,
    engine_context: Arc<DeltaKernelEngineContext>,
    kernel_schemas: KernelScanSchemas,
    logical_schema: SchemaRef,
    transform: KernelPhysicalToLogicalTransform,
    deletion_vector: Option<DeletionVectorMasker>,
    cancellation: ScanCancellation,
    _permit: FileReadPermit,
}

struct LogicalDataFileReadRequest {
    task: DeltaScanFileTask,
    physical_schema: SchemaRef,
    logical_schema: SchemaRef,
    kernel_schemas: KernelScanSchemas,
    physical_predicate: Option<DeltaKernelPredicate>,
    row_predicate: Option<DeltaKernelPredicate>,
    output_batch_size_rows: Option<usize>,
    permit: FileReadPermit,
    cancellation: ScanCancellation,
}

impl DirectParquetReader {
    fn new(
        engine_context: Arc<DeltaKernelEngineContext>,
        execution_options: DeltaScanExecutionOptions,
        metrics: DeltaScanMetrics,
        range_read_estimator: Arc<ParquetRangeReadEstimator>,
    ) -> Self {
        let store = Arc::new(
            MeteredParquetObjectStore::new(
                engine_context.object_store(),
                metrics.clone(),
                MultiRangeReadStrategy::for_policy(
                    execution_options.parquet_range_read_policy(),
                    engine_context.table_url(),
                ),
            )
            .with_range_read_estimator(range_read_estimator),
        );
        Self {
            engine_context,
            store,
            execution_options,
            metrics,
            metadata_cache: None,
        }
    }

    fn with_metadata_cache(mut self, metadata_cache: Arc<ParquetMetadataCache>) -> Self {
        self.metadata_cache = Some(metadata_cache);
        self
    }

    async fn load_parquet_metadata(
        &self,
        path: &Path,
        file_size: u64,
        reader: &mut ParquetObjectReader,
        options: &ArrowReaderOptions,
    ) -> Result<Arc<ParquetMetaData>, DeltaReaderError> {
        let metadata = match self.metadata_cache.as_deref() {
            Some(cache) => cache
                .entry(path, file_size)
                .get_or_try_init(|| reader.get_metadata(Some(options)))
                .await
                .map(Arc::clone),
            None => reader.get_metadata(Some(options)).await,
        };
        metadata.boxed().context(DataFileReadSnafu {
            reason: "parquet_read_setup_failed",
        })
    }

    #[cfg(feature = "experimental-parquet-metadata-warmup")]
    async fn prepare_task_parquet_metadata(
        &self,
        task: &DeltaScanFileTask,
    ) -> Result<usize, DeltaReaderError> {
        let object = self.resolve_parquet_object(task)?;
        let mut reader = ParquetObjectReader::new(Arc::clone(&object.store), object.path.clone())
            .with_file_size(object.file_size);
        if let Some(hint) = self.execution_options.parquet_metadata_size_hint_bytes() {
            reader = reader.with_footer_size_hint(hint);
        }
        let options = arrow_reader_options(false, true)
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_row_index_setup_failed",
            })?;
        let metadata = self
            .load_parquet_metadata(&object.path, object.file_size, &mut reader, &options)
            .await?;
        Ok(metadata.memory_size())
    }

    async fn parquet_object_for_task(
        &self,
        task: &DeltaScanFileTask,
    ) -> Result<ParquetFileObject, DeltaReaderError> {
        let object = self.resolve_parquet_object(task)?;
        if task.parquet_byte_range.is_some() {
            return Ok(object);
        }
        self.buffer_small_parquet_object(object).await
    }

    /// Opens the physical Parquet stream for one file task.
    ///
    /// The stream reads batches in `target_schema`. Setup resolves the data object, buffers it when
    /// configured, loads the Parquet metadata needed for the read, matches the file schema to the
    /// target schema, and configures row-group pruning, row filtering, and batch size from
    /// `options`.
    ///
    /// This function stops at the physical read boundary. The caller applies the
    /// physical-to-logical transform and deletion-vector mask.
    async fn open_physical_parquet_stream(
        &self,
        task: &DeltaScanFileTask,
        target_schema: &SchemaRef,
        options: PhysicalParquetStreamOptions<'_>,
    ) -> Result<PhysicalParquetStream, DeltaReaderError> {
        let object = self.parquet_object_for_task(task).await?;
        let builder = self
            .create_stream_builder(
                &object,
                task.parquet_byte_range.as_ref(),
                target_schema,
                options.include_original_row_index,
                options.row_filter.is_some(),
            )
            .await?;
        let schema_alignment = build_schema_alignment(
            builder.parquet_schema(),
            builder.schema(),
            Arc::clone(target_schema),
        )
        .map_err(|error| data_file_error("parquet_schema_match_failed", error))?;
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), schema_alignment.projected_roots());
        let mut builder = Self::apply_row_group_selection(
            builder,
            task.parquet_byte_range.as_ref(),
            object.file_size,
            options.row_group_predicate,
        )?;

        // When a row predicate is present, build its filter with a projection limited to the
        // columns that predicate references.
        builder = self.apply_row_filter(builder, target_schema, options.row_filter)?;

        if let Some(batch_size) = options.output_batch_size_rows {
            builder = builder.with_batch_size(batch_size);
        }

        let stream = builder
            .with_projection(projection)
            .build()
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_read_setup_failed",
            })?;

        Ok(PhysicalParquetStream {
            stream,
            schema_alignment,
            include_original_row_index: options.include_original_row_index,
        })
    }

    /// Creates a stream builder for the resolved Parquet object.
    ///
    /// This chooses the metadata-loading path for a ranged or whole-file read. It also rewrites
    /// the Arrow schema before creating the builder when `target_schema` uses view types.
    async fn create_stream_builder(
        &self,
        object: &ParquetFileObject,
        parquet_byte_range: Option<&Range<u64>>,
        target_schema: &SchemaRef,
        include_original_row_index: bool,
        has_row_filter: bool,
    ) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>, DeltaReaderError> {
        let file_size = object.file_size;
        let path = &object.path;
        let reader = ParquetObjectReader::new(Arc::clone(&object.store), path.clone())
            .with_file_size(file_size);
        let reader = match self.execution_options.parquet_metadata_size_hint_bytes() {
            Some(hint) => reader.with_footer_size_hint(hint),
            None => reader,
        };
        let reader_options = arrow_reader_options(include_original_row_index, has_row_filter)
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_row_index_setup_failed",
            })?;
        if parquet_byte_range.is_some() || self.metadata_cache.is_some() {
            self.create_stream_builder_from_metadata(
                path,
                file_size,
                reader,
                reader_options,
                target_schema,
            )
            .await
        } else if schema_uses_view_types(target_schema) {
            Self::create_view_type_stream_builder(reader, reader_options).await
        } else {
            ParquetRecordBatchStreamBuilder::new_with_options(reader, reader_options)
                .await
                .boxed()
                .context(DataFileReadSnafu {
                    reason: "parquet_read_setup_failed",
                })
        }
    }

    /// Creates a stream builder from explicitly loaded Parquet metadata.
    ///
    /// Ranged tasks and prepared whole-file reads use this path so their metadata can be shared.
    /// The metadata schema is rewritten first when `target_schema` uses view types.
    async fn create_stream_builder_from_metadata(
        &self,
        path: &Path,
        file_size: u64,
        mut reader: ParquetObjectReader,
        reader_options: ArrowReaderOptions,
        target_schema: &SchemaRef,
    ) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>, DeltaReaderError> {
        let metadata = self
            .load_parquet_metadata(path, file_size, &mut reader, &reader_options)
            .await?;
        let metadata = ArrowReaderMetadata::try_new(metadata, reader_options.clone())
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_read_setup_failed",
            })?;
        let metadata = if schema_uses_view_types(target_schema) {
            metadata_with_view_types(metadata, reader_options)?
        } else {
            metadata
        };

        Ok(ParquetRecordBatchStreamBuilder::new_with_metadata(
            reader, metadata,
        ))
    }

    /// Creates a stream builder whose Arrow schema uses view types.
    ///
    /// The metadata schema must be rewritten before the builder is created. Rewriting it later
    /// would leave the stream configured with the original Arrow types.
    async fn create_view_type_stream_builder(
        reader: ParquetObjectReader,
        reader_options: ArrowReaderOptions,
    ) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>, DeltaReaderError> {
        let mut metadata_reader = reader.clone();
        let metadata =
            ArrowReaderMetadata::load_async(&mut metadata_reader, reader_options.clone())
                .await
                .boxed()
                .context(DataFileReadSnafu {
                    reason: "parquet_read_setup_failed",
                })?;

        Ok(ParquetRecordBatchStreamBuilder::new_with_metadata(
            reader,
            metadata_with_view_types(metadata, reader_options)?,
        ))
    }

    /// Applies row-group selection to a stream builder.
    ///
    /// Selection combines the task's approximate byte range with metadata pruning. A selected
    /// range always expands to complete Parquet row groups.
    fn apply_row_group_selection(
        builder: ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
        parquet_byte_range: Option<&Range<u64>>,
        file_size: u64,
        row_group_predicate: Option<&DeltaKernelPredicate>,
    ) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>, DeltaReaderError> {
        let row_groups = pruned_row_groups(
            builder.metadata(),
            file_size,
            parquet_byte_range,
            row_group_predicate,
        )
        .boxed()
        .context(DataFileReadSnafu {
            reason: "parquet_row_group_range_invalid",
        })?;
        Ok(match row_groups {
            Some(row_groups) => builder.with_row_groups(row_groups),
            None => builder,
        })
    }

    /// Builds and attaches the requested row filter.
    ///
    /// When no filter was requested, this returns the builder unchanged.
    fn apply_row_filter(
        &self,
        builder: ParquetRecordBatchStreamBuilder<ParquetObjectReader>,
        target_schema: &SchemaRef,
        row_filter: Option<RowFilterInput<'_>>,
    ) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>, DeltaReaderError> {
        let Some(row_filter) = row_filter else {
            return Ok(builder);
        };
        let row_filter = self
            .build_row_filter(
                row_filter.predicate,
                row_filter.kernel_schemas,
                target_schema,
                builder.parquet_schema(),
                builder.schema(),
            )
            .map_err(|error| data_file_error("parquet_schema_match_failed", error))?;

        Ok(builder.with_row_filter(row_filter))
    }

    async fn open_logical_data_file_stream(
        self: &Arc<Self>,
        request: LogicalDataFileReadRequest,
    ) -> Result<LogicalDataFileStream, DeltaReaderError> {
        let stream_options = PhysicalParquetStreamOptions {
            output_batch_size_rows: request.output_batch_size_rows,
            row_group_predicate: request.physical_predicate.as_ref(),
            row_filter: request
                .row_predicate
                .as_ref()
                .map(|predicate| RowFilterInput {
                    predicate,
                    kernel_schemas: &request.kernel_schemas,
                }),
            include_original_row_index: request.task.deletion_vector.is_present(),
        };
        let physical_stream = tokio::select! {
            biased;
            () = request.cancellation.cancelled() => return Err(cancelled_error()),
            result = self.open_physical_parquet_stream(
                &request.task,
                &request.physical_schema,
                stream_options,
            ) => result?,
        };
        if request.cancellation.is_cancelled() {
            return Err(cancelled_error());
        }
        let deletion_vector = load_deletion_vector_masker(
            &self.engine_context,
            request.task.deletion_vector.clone(),
            &self.metrics,
        )
        .await?;
        if request.cancellation.is_cancelled() {
            return Err(cancelled_error());
        }

        Ok(LogicalDataFileStream {
            physical_stream,
            engine_context: Arc::clone(&self.engine_context),
            kernel_schemas: request.kernel_schemas,
            logical_schema: request.logical_schema,
            transform: request.task.transform,
            deletion_vector,
            cancellation: request.cancellation,
            _permit: request.permit,
        })
    }

    /// Builds a Parquet row filter for an exact kernel predicate.
    ///
    /// The filter projects only the top-level columns referenced by the predicate. Its input
    /// batches are reshaped to that narrow target schema before the kernel evaluates them.
    fn build_row_filter(
        &self,
        predicate: &DeltaKernelPredicate,
        kernel_schemas: &KernelScanSchemas,
        target_schema: &SchemaRef,
        parquet_schema: &SchemaDescriptor,
        parquet_arrow_schema: &SchemaRef,
    ) -> Result<RowFilter, delta_kernel::Error> {
        let target_indices = predicate_root_indices(predicate, target_schema)?;
        let predicate_schema = Arc::new(target_schema.project(&target_indices)?);
        let schema_alignment =
            build_schema_alignment(parquet_schema, parquet_arrow_schema, predicate_schema)?;
        let projection = ProjectionMask::roots(parquet_schema, schema_alignment.projected_roots());
        let engine_context = Arc::clone(&self.engine_context);
        let predicate = predicate.clone();
        let kernel_schemas = kernel_schemas.clone();
        let predicate = ArrowPredicateFn::new(projection, move |batch| {
            let batch = schema_alignment
                .reshape_batch_to_target_schema(batch)
                .map_err(|error| arrow::error::ArrowError::ComputeError(error.to_string()))?;
            engine_context
                .evaluate_predicate(&kernel_schemas, &predicate, batch)
                .map_err(|error| arrow::error::ArrowError::ExternalError(Box::new(error)))
        });

        Ok(RowFilter::new(vec![Box::new(predicate)]))
    }

    fn resolve_parquet_object(
        &self,
        task: &DeltaScanFileTask,
    ) -> Result<ParquetFileObject, DeltaReaderError> {
        let location = self
            .engine_context
            .table_url()
            .join(&task.path)
            .boxed()
            .context(DataFileReadSnafu {
                reason: "data_file_path_resolution_failed",
            })?;
        let path = Path::from_url_path(location.path())
            .boxed()
            .context(DataFileReadSnafu {
                reason: "data_file_path_resolution_failed",
            })?;
        let file_size = task.file_size.ok_or_else(|| {
            data_file_error(
                "data_file_size_missing",
                delta_kernel::Error::generic("file size is required for direct Parquet reads"),
            )
        })?;

        Ok(ParquetFileObject {
            store: Arc::clone(&self.store),
            path,
            file_size,
        })
    }

    async fn buffer_small_parquet_object(
        &self,
        mut object: ParquetFileObject,
    ) -> Result<ParquetFileObject, DeltaReaderError> {
        let should_buffer = self
            .execution_options
            .parquet_full_file_read_threshold_bytes()
            .map(|threshold| u64::try_from(threshold).unwrap_or(u64::MAX))
            .is_some_and(|threshold| object.file_size <= threshold);
        if !should_buffer {
            return Ok(object);
        }

        let bytes = object
            .store
            .get(&object.path)
            .await
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_full_file_read_failed",
            })?
            .bytes()
            .await
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_full_file_read_failed",
            })?;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&object.path, bytes.into())
            .await
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_file_buffer_initialization_failed",
            })?;
        object.store = store;
        Ok(object)
    }
}

/// Returns the sorted, unique target-schema indices needed by a predicate.
///
/// Predicate references are unordered, and separate nested paths can use the same top-level root.
fn predicate_root_indices(
    predicate: &DeltaKernelPredicate,
    target_schema: &Schema,
) -> Result<Vec<usize>, delta_kernel::Error> {
    let mut indices = predicate
        .as_ref()
        .references()
        .into_iter()
        .map(|column| predicate_root_index(column, target_schema))
        .collect::<Result<Vec<_>, _>>()?;
    indices.sort_unstable();
    indices.dedup();
    Ok(indices)
}

/// Finds the target-schema index for a predicate column's top-level field.
///
/// Nested predicate paths use their first segment because Parquet row-filter projections select
/// complete top-level roots.
fn predicate_root_index(
    column: &ColumnName,
    target_schema: &Schema,
) -> Result<usize, delta_kernel::Error> {
    let root = column
        .path()
        .first()
        .ok_or_else(|| delta_kernel::Error::generic("empty predicate column path"))?;

    target_schema
        .fields()
        .find(root)
        .map(|(index, _)| index)
        .ok_or_else(|| {
            delta_kernel::Error::generic("predicate column is missing from the target schema")
        })
}

fn metadata_with_view_types(
    metadata: ArrowReaderMetadata,
    reader_options: ArrowReaderOptions,
) -> Result<ArrowReaderMetadata, DeltaReaderError> {
    let file_schema = Schema::new_with_metadata(
        metadata
            .schema()
            .fields()
            .iter()
            .filter(|field| field.name() != ORIGINAL_ROW_INDEX_COLUMN)
            .cloned()
            .collect::<Vec<_>>(),
        metadata.schema().metadata().clone(),
    );
    ArrowReaderMetadata::try_new(
        Arc::clone(metadata.metadata()),
        reader_options.with_schema(schema_with_view_types(&file_schema)),
    )
    .boxed()
    .context(DataFileReadSnafu {
        reason: "parquet_read_setup_failed",
    })
}

pub(crate) fn direct_parquet_file_executor(
    plan: &Arc<DeltaScanPlan>,
    output_batch_size_rows: Option<usize>,
    row_predicate: Option<DeltaKernelPredicate>,
    range_read_estimator: Arc<ParquetRangeReadEstimator>,
    metadata_cache: Option<Arc<ParquetMetadataCache>>,
) -> FileExecutor<DeltaScanFileTask, FileBatchStream> {
    let reader = DirectParquetReader::new(
        Arc::clone(&plan.engine_context),
        plan.execution_options,
        plan.metrics.clone(),
        range_read_estimator,
    );
    let reader = Arc::new(match metadata_cache {
        Some(cache) => reader.with_metadata_cache(cache),
        None => reader,
    });
    let physical_schema = Arc::clone(&plan.physical_schema);
    let logical_schema = Arc::clone(&plan.logical_schema);
    let kernel_schemas = plan.kernel_schemas.clone();
    let physical_predicate = plan.physical_predicate.clone();

    Arc::new(move |task, permit, cancellation| {
        if let Some(bytes) = task.estimated_scan_bytes() {
            reader
                .metrics
                .record_estimated_parquet_task_bytes_admitted(bytes);
        }
        let reader = Arc::clone(&reader);
        let physical_schema = Arc::clone(&physical_schema);
        let logical_schema = Arc::clone(&logical_schema);
        let kernel_schemas = kernel_schemas.clone();
        let physical_predicate = physical_predicate.clone();
        let row_predicate = row_predicate.clone();
        Box::pin(async move {
            let request = LogicalDataFileReadRequest {
                task,
                physical_schema,
                logical_schema,
                kernel_schemas,
                physical_predicate,
                row_predicate,
                output_batch_size_rows,
                permit,
                cancellation,
            };
            let file = reader.open_logical_data_file_stream(request).await?;
            let batches = stream::try_unfold(file, |mut file| async move {
                file.next_batch()
                    .await
                    .map(|batch| batch.map(|batch| (batch, file)))
            });
            Ok(Box::pin(batches) as FileBatchStream)
        })
    })
}

#[cfg(feature = "experimental-parquet-metadata-warmup")]
pub(crate) async fn prepare_parquet_metadata(
    plan: &DeltaScanPlan,
    metadata_cache: Arc<ParquetMetadataCache>,
    range_read_estimator: Arc<ParquetRangeReadEstimator>,
    limits: ParquetMetadataPreparationLimits,
) -> Result<PreparedParquetMetadataSummary, DeltaReaderError> {
    let mut seen = HashSet::new();
    let tasks = plan
        .partitions
        .iter()
        .flat_map(|partition| &partition.file_tasks)
        .filter(|task| seen.insert((task.path.clone(), task.file_size)))
        .cloned()
        .collect::<Vec<_>>();
    if tasks.len() > limits.max_files {
        return Err(DeltaReaderError::InvalidConfiguration {
            reason: "parquet_metadata_warmup_file_limit_exceeded",
        });
    }

    let file_count = tasks.len();
    let reader = Arc::new(
        DirectParquetReader::new(
            Arc::clone(&plan.engine_context),
            plan.execution_options,
            plan.metrics.clone(),
            range_read_estimator,
        )
        .with_metadata_cache(metadata_cache),
    );
    let concurrency = plan
        .execution_options
        .resolved_max_concurrent_file_reads_per_scan(
            plan.partition_target_diagnostic.target_partitions,
        );
    let mut loads = stream::iter(tasks)
        .map(|task| {
            let reader = Arc::clone(&reader);
            async move { reader.prepare_task_parquet_metadata(&task).await }
        })
        .buffer_unordered(concurrency);
    let mut memory_bytes = 0_usize;
    while let Some(result) = loads.next().await {
        memory_bytes = memory_bytes
            .checked_add(result?)
            .filter(|bytes| *bytes <= limits.max_retained_metadata_bytes)
            .ok_or(DeltaReaderError::InvalidConfiguration {
                reason: "parquet_metadata_warmup_memory_limit_exceeded",
            })?;
    }

    Ok(PreparedParquetMetadataSummary {
        file_count,
        memory_bytes,
    })
}

impl PhysicalParquetStream {
    #[cfg(test)]
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, DeltaReaderError> {
        self.next_batch_with_original_row_indexes()
            .await
            .map(|batch| batch.map(|(batch, _)| batch))
    }

    async fn next_batch_with_original_row_indexes(
        &mut self,
    ) -> Result<Option<(RecordBatch, Option<Int64Array>)>, DeltaReaderError> {
        let Some(batch) = self.stream.next().await else {
            return Ok(None);
        };
        let batch = batch.boxed().context(DataFileReadSnafu {
            reason: "parquet_batch_read_failed",
        })?;
        let row_indexes = if self.include_original_row_index {
            let index = batch
                .schema()
                .index_of(ORIGINAL_ROW_INDEX_COLUMN)
                .map_err(|error| data_file_error("parquet_row_index_missing", error))?;
            Some(
                batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        data_file_error(
                            "parquet_row_index_type_mismatch",
                            delta_kernel::Error::generic("original row index is not Int64"),
                        )
                    })?
                    .clone(),
            )
        } else {
            None
        };
        let batch = if self.include_original_row_index || self.schema_alignment.needs_batch_reshape
        {
            self.schema_alignment
                .reshape_batch_to_target_schema(batch)
                .map_err(|error| data_file_error("parquet_batch_reshape_failed", error))?
        } else {
            batch
        };

        Ok(Some((batch, row_indexes)))
    }
}

impl LogicalDataFileStream {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, DeltaReaderError> {
        let next = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(cancelled_error()),
            result = self.physical_stream.next_batch_with_original_row_indexes() => result?,
        };
        let Some((physical_batch, original_row_indexes)) = next else {
            if let Some(deletion_vector) = self.deletion_vector.as_mut() {
                deletion_vector.finish_original_row_indexes()?;
            }
            return Ok(None);
        };
        let logical_batch = self
            .transform
            .apply(
                self.engine_context.as_ref(),
                &self.kernel_schemas,
                physical_batch,
            )
            .boxed()
            .context(PhysicalToLogicalTransformSnafu {
                reason: "physical_to_logical_transform_failed",
            })?;
        let logical_batch = align_batch_to_logical_schema(
            logical_batch,
            &self.logical_schema,
            "Direct Parquet output does not match the planned logical schema",
        )?;
        let logical_batch = match self.deletion_vector.as_mut() {
            Some(deletion_vector) => deletion_vector
                .mask_original_row_indexes(logical_batch, original_row_indexes.as_ref())?,
            None => logical_batch,
        };
        Ok(Some(logical_batch))
    }
}

fn arrow_reader_options(
    include_original_row_index: bool,
    has_row_filter: bool,
) -> parquet::errors::Result<ArrowReaderOptions> {
    // A row filter turns the predicate result into a selection of row numbers. When an offset
    // index is available, parquet-rs can map that selection to data-page byte ranges and avoid
    // fetching pages that contain no selected rows. Without a row filter, there is no
    // predicate-derived selection, so the offset index is not loaded.
    let offset_index_policy = if has_row_filter {
        PageIndexPolicy::Optional
    } else {
        PageIndexPolicy::Skip
    };
    let options = ArrowReaderOptions::new().with_offset_index_policy(offset_index_policy);
    if !include_original_row_index {
        return Ok(options);
    }
    let row_number_field = Arc::new(
        Field::new(ORIGINAL_ROW_INDEX_COLUMN, DataType::Int64, false)
            .with_extension_type(RowNumber),
    );
    options.with_virtual_columns(vec![row_number_field])
}

fn cancelled_error() -> DeltaReaderError {
    CancelledSnafu {
        reason: "scan_execution_cancelled",
    }
    .build()
}

fn data_file_error(
    reason: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> DeltaReaderError {
    DataFileReadSnafu { reason }.into_error(Box::new(source))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fmt,
        fs::{self, File},
        io,
        path::{Path as FsPath, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow::{
        array::{Array, ArrayRef, Decimal128Array, Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use async_trait::async_trait;
    use delta_kernel::scan::state::{DvInfo, ScanFile};
    use delta_kernel::{
        actions::deletion_vector_writer::{KernelDeletionVector, StreamingDeletionVectorWriter},
        expressions::{ColumnName, Expression, Predicate, Scalar},
    };
    use futures_util::{StreamExt, stream::BoxStream};
    use object_store::{
        CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, GetResultPayload,
        ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, RenameOptions, Result as ObjectStoreResult,
        memory::InMemory, path::Path,
    };
    use parquet::arrow::{ArrowSchemaConverter, ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::metadata::ParquetMetaDataReader;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    #[cfg(feature = "experimental-parquet-metadata-warmup")]
    use super::prepare_parquet_metadata;
    use super::{
        DirectParquetReader, LogicalDataFileReadRequest, ParquetMetadataCache,
        PhysicalParquetStreamOptions, RowFilterInput, arrow_reader_options, data_file_error,
        direct_parquet_file_executor,
    };
    #[cfg(feature = "experimental-parquet-metadata-warmup")]
    use crate::reader::ParquetMetadataPreparationLimits;
    use crate::reader::backend::kernel_reader::delta_kernel_file_executor;
    use crate::{
        DeltaReaderError, DeltaScanExecutionOptions, DeltaScanMetrics, DeltaSnapshotSelection,
        DeltaStorageOptions, ParquetReaderBackend,
        delta::kernel::{
            DeltaKernelEngineContext, DeltaKernelPredicate, KernelPhysicalToLogicalTransform,
            KernelScanFileMetadata,
        },
        delta::snapshot::load_delta_table_snapshot_blocking,
        reader::{
            backend::direct_parquet::metered_object_store::{
                MeteredParquetObjectStore, MultiRangeReadStrategy,
            },
            metrics::DeltaScanMetricsConfig,
            planning::{DeltaScanFileTask, DeltaScanPartitionTargetOptions, plan_scan},
            scheduling::{
                DeltaScanScheduler, FileAdmissionDecision, FileReadPermit, ScanCancellation,
                ScanReadLimiter,
            },
        },
    };

    const DV_ID: &str = "vBn[lx{q8@P<9BNH/isA";
    const DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";

    #[test]
    fn arrow_reader_options_load_offset_indexes_only_for_row_filters()
    -> Result<(), Box<dyn std::error::Error>> {
        for include_original_row_index in [false, true] {
            for (has_row_filter, expected_offset_policy) in [
                (false, parquet::file::metadata::PageIndexPolicy::Skip),
                (true, parquet::file::metadata::PageIndexPolicy::Optional),
            ] {
                let options = arrow_reader_options(include_original_row_index, has_row_filter)?;
                assert_eq!(options.offset_index_policy(), expected_offset_policy);
                assert_eq!(
                    options.column_index_policy(),
                    parquet::file::metadata::PageIndexPolicy::Skip
                );
            }
        }
        Ok(())
    }

    pub(super) struct TestDir(PathBuf);

    impl TestDir {
        pub(super) fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let path = std::env::temp_dir().join(format!(
                "delta-arrow-reader-{name}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&path)?;
            Ok(Self(path))
        }

        pub(super) fn path(&self) -> &FsPath {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Clone, Copy)]
    enum GateRequest {
        FullGet,
        Range(usize),
    }

    struct GatedObjectStore {
        inner: Arc<dyn ObjectStore>,
        gate_full_get: bool,
        range_target: AtomicUsize,
        fail_range_target: AtomicUsize,
        corrupt_range_target: AtomicUsize,
        range_calls: AtomicUsize,
        multi_range_calls: AtomicUsize,
        started: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
        cancelled: Arc<AtomicBool>,
    }

    impl GatedObjectStore {
        fn new(inner: Arc<dyn ObjectStore>, request: GateRequest) -> Arc<Self> {
            let (gate_full_get, range_target) = match request {
                GateRequest::FullGet => (true, 0),
                GateRequest::Range(target) => (false, target),
            };
            Arc::new(Self {
                inner,
                gate_full_get,
                range_target: AtomicUsize::new(range_target),
                fail_range_target: AtomicUsize::new(0),
                corrupt_range_target: AtomicUsize::new(0),
                range_calls: AtomicUsize::new(0),
                multi_range_calls: AtomicUsize::new(0),
                started: tokio::sync::Semaphore::new(0),
                release: tokio::sync::Semaphore::new(0),
                cancelled: Arc::new(AtomicBool::new(false)),
            })
        }

        fn ungated(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
            Self::new(inner, GateRequest::Range(0))
        }

        async fn wait_started(&self) {
            self.started
                .acquire()
                .await
                .expect("gate remains open")
                .forget();
        }

        fn was_cancelled(&self) -> bool {
            self.cancelled.load(Ordering::Acquire)
        }

        fn multi_range_call_count(&self) -> usize {
            self.multi_range_calls.load(Ordering::Acquire)
        }

        fn gate_next_range(&self) {
            self.range_target.store(
                self.range_calls.load(Ordering::Acquire) + 1,
                Ordering::Release,
            );
        }

        fn fail_next_range(&self) {
            self.fail_range_target.store(
                self.range_calls.load(Ordering::Acquire) + 1,
                Ordering::Release,
            );
        }

        fn corrupt_next_range(&self) {
            self.corrupt_range_target.store(
                self.range_calls.load(Ordering::Acquire) + 1,
                Ordering::Release,
            );
        }
    }

    impl fmt::Debug for GatedObjectStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("GatedObjectStore")
        }
    }

    impl fmt::Display for GatedObjectStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("GatedObjectStore")
        }
    }

    struct GateGuard {
        cancelled: Arc<AtomicBool>,
        completed: bool,
    }

    impl Drop for GateGuard {
        fn drop(&mut self) {
            if !self.completed {
                self.cancelled.store(true, Ordering::Release);
            }
        }
    }

    #[async_trait]
    impl ObjectStore for GatedObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            let range_call = (!options.head)
                .then(|| {
                    options
                        .range
                        .as_ref()
                        .map(|_| self.range_calls.fetch_add(1, Ordering::AcqRel) + 1)
                })
                .flatten();
            if range_call.is_some_and(|call| self.fail_range_target.load(Ordering::Acquire) == call)
            {
                return Err(ObjectStoreError::Generic {
                    store: "gated-test-store",
                    source: io::Error::other("injected range failure").into(),
                });
            }
            let should_gate = if options.head {
                false
            } else if let Some(call) = range_call {
                !self.gate_full_get && self.range_target.load(Ordering::Acquire) == call
            } else {
                self.gate_full_get
            };
            if should_gate {
                let mut guard = GateGuard {
                    cancelled: Arc::clone(&self.cancelled),
                    completed: false,
                };
                self.started.add_permits(1);
                self.release
                    .acquire()
                    .await
                    .expect("gate remains open")
                    .forget();
                guard.completed = true;
            }
            let result = self.inner.get_opts(location, options).await?;
            if range_call
                .is_none_or(|call| self.corrupt_range_target.load(Ordering::Acquire) != call)
            {
                return Ok(result);
            }

            let meta = result.meta.clone();
            let range = result.range.clone();
            let attributes = result.attributes.clone();
            let mut bytes = result.bytes().await?.to_vec();
            bytes.fill(0);
            let chunk = PutPayload::from(bytes).into_iter().next().ok_or_else(|| {
                ObjectStoreError::Generic {
                    store: "gated-test-store",
                    source: io::Error::other("missing corrupted range payload").into(),
                }
            })?;
            Ok(GetResult {
                payload: GetResultPayload::Stream(
                    futures_util::stream::once(async move { Ok(chunk) }).boxed(),
                ),
                meta,
                range,
                attributes,
            })
        }

        async fn get_ranges(
            &self,
            location: &Path,
            ranges: &[std::ops::Range<u64>],
        ) -> ObjectStoreResult<Vec<bytes::Bytes>> {
            self.multi_range_calls.fetch_add(1, Ordering::AcqRel);
            object_store::coalesce_ranges(
                ranges,
                |range| self.get_range(location, range),
                object_store::OBJECT_STORE_COALESCE_DEFAULT,
            )
            .await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &Path,
            to: &Path,
            options: RenameOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    pub(super) fn metrics() -> DeltaScanMetrics {
        DeltaScanMetrics::new(DeltaScanMetricsConfig {
            snapshot_version: 1,
            parquet_backend: ParquetReaderBackend::Direct,
            scan_partitions_planned: 1,
            files_planned: 1,
            add_actions_excluded_during_planning: Some(0),
            estimated_input_rows: Some(1),
            estimated_input_bytes: Some(1),
        })
    }

    pub(super) fn reader(
        root: &TestDir,
        options: DeltaScanExecutionOptions,
        metrics: DeltaScanMetrics,
    ) -> Result<DirectParquetReader, Box<dyn std::error::Error>> {
        let table_url = url::Url::from_directory_path(root.path())
            .map_err(|()| "temporary table path cannot become a file URL")?;
        let engine_context = Arc::new(DeltaKernelEngineContext::try_new(
            table_url,
            &DeltaStorageOptions::default(),
        )?);
        Ok(DirectParquetReader::new(
            engine_context,
            options,
            metrics,
            Arc::default(),
        ))
    }

    pub(super) fn task(
        path: &str,
        file_size: Option<u64>,
    ) -> Result<DeltaScanFileTask, DeltaReaderError> {
        let size = file_size
            .map(i64::try_from)
            .transpose()
            .map_err(|error| data_file_error("test_file_size_overflow", error))?
            .unwrap_or(1);
        let mut task =
            DeltaScanFileTask::try_from_kernel(KernelScanFileMetadata::from_scan_file(ScanFile {
                path: path.to_owned(),
                size,
                modification_time: 0,
                stats: None,
                dv_info: DvInfo::default(),
                transform: None,
                partition_values: HashMap::new(),
            }))?;
        task.file_size = file_size;
        Ok(task)
    }

    async fn read_with_row_filter(
        root: &TestDir,
        parquet_file_size: usize,
        target_schema: &Arc<Schema>,
        predicate: &DeltaKernelPredicate,
    ) -> Result<(Vec<RecordBatch>, u64), Box<dyn std::error::Error>> {
        let plan =
            pipeline_plan_for_backend(root, None, None, ParquetReaderBackend::Direct, false)?;
        let metrics = metrics();
        let reader = reader(
            root,
            DeltaScanExecutionOptions::new().with_parquet_metadata_size_hint_bytes(None)?,
            metrics.clone(),
        )?;
        let task = task("part.parquet", Some(u64::try_from(parquet_file_size)?))?;
        let mut stream = reader
            .open_physical_parquet_stream(
                &task,
                target_schema,
                PhysicalParquetStreamOptions {
                    row_filter: Some(RowFilterInput {
                        predicate,
                        kernel_schemas: &plan.kernel_schemas,
                    }),
                    ..Default::default()
                },
            )
            .await?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next_batch().await? {
            batches.push(batch);
        }
        let bytes_received = metrics
            .snapshot()
            .parquet_data_file_bytes_received
            .ok_or("direct reader did not report Parquet bytes")?;
        Ok((batches, bytes_received))
    }

    pub(super) fn parquet_bytes_for(
        schema: Arc<Schema>,
        columns: Vec<ArrayRef>,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, None)?;
        writer.write(&batch)?;
        Ok(writer.into_inner()?)
    }

    fn parquet_bytes_with_properties(
        schema: Arc<Schema>,
        columns: Vec<ArrayRef>,
        properties: WriterProperties,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(properties))?;
        writer.write(&batch)?;
        Ok(writer.into_inner()?)
    }

    fn parquet_bytes() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        parquet_bytes_for(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
    }

    fn write_partitioned_dv_table(
        root: &TestDir,
        parquet_bytes: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_partitioned_table(root, parquet_bytes, TestDeletionVector::Relative)
    }

    fn write_partitioned_non_dv_table(
        root: &TestDir,
        parquet_bytes: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_partitioned_table(root, parquet_bytes, TestDeletionVector::None)
    }

    fn write_partitioned_inline_dv_table(
        root: &TestDir,
        parquet_bytes: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_partitioned_table(root, parquet_bytes, TestDeletionVector::Inline)
    }

    enum TestDeletionVector {
        None,
        Relative,
        Inline,
    }

    fn write_partitioned_table(
        root: &TestDir,
        parquet_bytes: &[u8],
        deletion_vector: TestDeletionVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        fs::write(root.path().join("part.parquet"), parquet_bytes)?;
        let deletion_vector = match deletion_vector {
            TestDeletionVector::None => None,
            TestDeletionVector::Relative => {
                let mut dv_bytes = Vec::new();
                let mut writer = StreamingDeletionVectorWriter::new(&mut dv_bytes);
                let mut deletion_vector = KernelDeletionVector::new();
                deletion_vector.add_deleted_row_indexes([4]);
                let result = writer.write_deletion_vector(deletion_vector)?;
                writer.finalize()?;
                fs::write(root.path().join(DV_FILE), dv_bytes)?;
                Some(serde_json::json!({
                    "storageType": "u",
                    "pathOrInlineDv": DV_ID,
                    "offset": result.offset,
                    "sizeInBytes": result.size_in_bytes,
                    "cardinality": result.cardinality
                }))
            }
            TestDeletionVector::Inline => Some(serde_json::json!({
                "storageType": "i",
                "pathOrInlineDv": "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
                "offset": 0,
                "sizeInBytes": 44,
                "cardinality": 6
            })),
        };

        let log = root.path().join("_delta_log");
        fs::create_dir_all(&log)?;
        let protocol = serde_json::json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["deletionVectors"],
                "writerFeatures": ["deletionVectors"]
            }
        });
        let schema = serde_json::json!({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
                {"name": "region", "type": "string", "nullable": true, "metadata": {}}
            ]
        });
        let metadata = serde_json::json!({
            "metaData": {
                "id": "direct-parquet-pipeline-test",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.to_string(),
                "partitionColumns": ["region"],
                "configuration": {},
                "createdTime": 1587968585495_i64
            }
        });
        let stats = serde_json::json!({
            "numRecords": 6,
            "minValues": {"id": 1},
            "maxValues": {"id": 6},
            "nullCount": {"id": 0}
        });
        let mut add = serde_json::json!({
            "path": "part.parquet",
            "partitionValues": {"region": "west"},
            "size": parquet_bytes.len(),
            "modificationTime": 1587968586000_i64,
            "dataChange": true,
            "stats": stats.to_string()
        });
        if let Some(deletion_vector) = deletion_vector {
            add["deletionVector"] = deletion_vector;
        }
        let add = serde_json::json!({"add": add});
        fs::write(
            log.join("00000000000000000000.json"),
            format!("{protocol}\n{metadata}\n{add}\n"),
        )?;
        Ok(())
    }

    fn remove_all_data_files_from_log(root: &TestDir) -> Result<(), Box<dyn std::error::Error>> {
        let path = root
            .path()
            .join("_delta_log")
            .join("00000000000000000000.json");
        let contents = fs::read_to_string(&path)?;
        let contents = contents
            .lines()
            .filter(|line| !line.contains("\"add\""))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(path, format!("{contents}\n"))?;
        Ok(())
    }

    fn add_second_partition_file(
        root: &TestDir,
        parquet_bytes: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        fs::write(root.path().join("part-east.parquet"), parquet_bytes)?;
        let path = root
            .path()
            .join("_delta_log")
            .join("00000000000000000000.json");
        let mut contents = fs::read_to_string(&path)?;
        let stats = serde_json::json!({
            "numRecords": 6,
            "minValues": {"id": 7},
            "maxValues": {"id": 12},
            "nullCount": {"id": 0}
        });
        let add = serde_json::json!({
            "add": {
                "path": "part-east.parquet",
                "partitionValues": {"region": "east"},
                "size": parquet_bytes.len(),
                "modificationTime": 1587968587000_i64,
                "dataChange": true,
                "stats": stats.to_string()
            }
        });
        contents.push_str(&format!("{add}\n"));
        fs::write(path, contents)?;
        Ok(())
    }

    fn pipeline_plan(
        root: &TestDir,
        full_file_read_threshold_bytes: Option<usize>,
    ) -> Result<Arc<crate::reader::planning::DeltaScanPlan>, Box<dyn std::error::Error>> {
        pipeline_plan_for_backend(
            root,
            full_file_read_threshold_bytes,
            Some(64 * 1024),
            ParquetReaderBackend::Direct,
            true,
        )
    }

    fn pipeline_plan_for_backend(
        root: &TestDir,
        full_file_read_threshold_bytes: Option<usize>,
        metadata_size_hint_bytes: Option<usize>,
        backend: ParquetReaderBackend,
        with_predicate: bool,
    ) -> Result<Arc<crate::reader::planning::DeltaScanPlan>, Box<dyn std::error::Error>> {
        pipeline_plan_for_backend_at(
            root,
            full_file_read_threshold_bytes,
            metadata_size_hint_bytes,
            backend,
            with_predicate,
            DeltaSnapshotSelection::Latest,
            1,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn pipeline_plan_for_backend_at(
        root: &TestDir,
        full_file_read_threshold_bytes: Option<usize>,
        metadata_size_hint_bytes: Option<usize>,
        backend: ParquetReaderBackend,
        with_predicate: bool,
        selection: DeltaSnapshotSelection,
        target_partitions: usize,
    ) -> Result<Arc<crate::reader::planning::DeltaScanPlan>, Box<dyn std::error::Error>> {
        let snapshot = load_delta_table_snapshot_blocking(
            &root.path().to_string_lossy(),
            &DeltaStorageOptions::new(),
            selection,
        )?;
        let options = DeltaScanExecutionOptions::new()
            .with_parquet_full_file_read_threshold_bytes(full_file_read_threshold_bytes)?
            .with_parquet_metadata_size_hint_bytes(metadata_size_hint_bytes)?
            .with_parquet_backend(backend);
        let predicate = with_predicate.then(|| {
            DeltaKernelPredicate::from_test_predicate(Predicate::gt(
                Expression::Column(ColumnName::new(["id"])),
                Expression::Literal(Scalar::Integer(3)),
            ))
        });
        Ok(Arc::new(plan_scan(
            &snapshot,
            Some(&["id".to_owned()]),
            &["region".to_owned()],
            predicate,
            true,
            options,
            DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(target_partitions),
                datafusion_target_partitions: None,
            },
        )?))
    }

    fn predicate_pipeline_plan_for_backend(
        root: &TestDir,
        backend: ParquetReaderBackend,
        predicate: Predicate,
    ) -> Result<Arc<crate::reader::planning::DeltaScanPlan>, Box<dyn std::error::Error>> {
        let snapshot = load_delta_table_snapshot_blocking(
            &root.path().to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        Ok(Arc::new(plan_scan(
            &snapshot,
            Some(&["id".to_owned()]),
            &["region".to_owned()],
            Some(DeltaKernelPredicate::from_test_predicate(predicate)),
            true,
            DeltaScanExecutionOptions::new().with_parquet_backend(backend),
            DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(1),
                datafusion_target_partitions: None,
            },
        )?))
    }

    fn non_dv_plan(
        root: &TestDir,
        projection: Option<&[String]>,
    ) -> Result<Arc<crate::reader::planning::DeltaScanPlan>, Box<dyn std::error::Error>> {
        let snapshot = load_delta_table_snapshot_blocking(
            &root.path().to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        Ok(Arc::new(plan_scan(
            &snapshot,
            projection,
            &[],
            None,
            false,
            DeltaScanExecutionOptions::new(),
            DeltaScanPartitionTargetOptions {
                explicit_target_partitions: Some(1),
                datafusion_target_partitions: None,
            },
        )?))
    }

    async fn execute_pipeline_plan(
        plan: Arc<crate::reader::planning::DeltaScanPlan>,
    ) -> Result<Vec<RecordBatch>, DeltaReaderError> {
        execute_pipeline_plan_with_row_predicate(plan, None).await
    }

    async fn execute_pipeline_plan_with_row_predicate(
        plan: Arc<crate::reader::planning::DeltaScanPlan>,
        row_predicate: Option<DeltaKernelPredicate>,
    ) -> Result<Vec<RecordBatch>, DeltaReaderError> {
        let scheduler = DeltaScanScheduler::new(Arc::clone(&plan));
        let executor =
            direct_parquet_file_executor(&plan, Some(2), row_predicate, Arc::default(), None);
        let mut batches = Vec::new();
        for partition in 0..plan.partitions.len() {
            let mut stream = scheduler.partition_stream(
                partition,
                Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
                Arc::clone(&executor),
            )?;
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }
        }
        Ok(batches)
    }

    async fn execute_kernel_plan(
        plan: Arc<crate::reader::planning::DeltaScanPlan>,
    ) -> Result<Vec<RecordBatch>, DeltaReaderError> {
        let scheduler = DeltaScanScheduler::new(Arc::clone(&plan));
        let executor = delta_kernel_file_executor(&plan);
        let mut batches = Vec::new();
        for partition in 0..plan.partitions.len() {
            let mut stream = scheduler.partition_stream(
                partition,
                Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
                Arc::clone(&executor),
            )?;
            while let Some(batch) = stream.next().await {
                batches.push(batch?);
            }
        }
        Ok(batches)
    }

    fn int32_ids(batches: &[RecordBatch]) -> Result<Vec<i32>, &'static str> {
        batches
            .iter()
            .map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .map(|ids| ids.values().to_vec())
                    .ok_or("expected Int32 ids")
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|ids| ids.into_iter().flatten().collect())
    }

    async fn gated_file_reader(
        plan: &Arc<crate::reader::planning::DeltaScanPlan>,
        parquet_bytes: &[u8],
        gate_request: GateRequest,
    ) -> Result<
        (
            Arc<DirectParquetReader>,
            Arc<GatedObjectStore>,
            DeltaScanFileTask,
        ),
        Box<dyn std::error::Error>,
    > {
        let task = plan.partitions[0].file_tasks[0].clone();
        let mut reader = DirectParquetReader::new(
            Arc::clone(&plan.engine_context),
            plan.execution_options,
            plan.metrics.clone(),
            Arc::default(),
        );
        let object = reader.resolve_parquet_object(&task)?;
        let inner = Arc::new(InMemory::new());
        inner
            .put(&object.path, parquet_bytes.to_vec().into())
            .await?;
        let gated = GatedObjectStore::new(inner, gate_request);
        reader.store = Arc::new(MeteredParquetObjectStore::new(
            Arc::clone(&gated) as Arc<dyn ObjectStore>,
            plan.metrics.clone(),
            MultiRangeReadStrategy::UseStoreImplementation,
        ));
        Ok((Arc::new(reader), gated, task))
    }

    async fn gated_ranged_metadata_reader(
        name: &str,
        gate_request: GateRequest,
    ) -> Result<
        (
            Arc<DirectParquetReader>,
            Arc<GatedObjectStore>,
            DeltaScanFileTask,
            Arc<Schema>,
        ),
        Box<dyn std::error::Error>,
    > {
        let root = TestDir::new(name)?;
        let parquet_bytes = parquet_bytes()?;
        let file_size = u64::try_from(parquet_bytes.len())?;
        let metrics = metrics();
        let mut reader = reader(&root, DeltaScanExecutionOptions::new(), metrics.clone())?;
        let mut task = task("part.parquet", Some(file_size))?;
        task.parquet_byte_range = Some(0..file_size);
        let object = reader.resolve_parquet_object(&task)?;
        let inner = Arc::new(InMemory::new());
        inner.put(&object.path, parquet_bytes.into()).await?;
        let gated = GatedObjectStore::new(inner, gate_request);
        reader.store = Arc::new(MeteredParquetObjectStore::new(
            Arc::clone(&gated) as Arc<dyn ObjectStore>,
            metrics,
            MultiRangeReadStrategy::UseStoreImplementation,
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        reader = reader.with_metadata_cache(Arc::new(ParquetMetadataCache::default()));
        Ok((Arc::new(reader), gated, task, schema))
    }

    fn file_read_request(
        plan: &Arc<crate::reader::planning::DeltaScanPlan>,
        task: DeltaScanFileTask,
        permit: FileReadPermit,
        cancellation: ScanCancellation,
    ) -> LogicalDataFileReadRequest {
        LogicalDataFileReadRequest {
            task,
            physical_schema: Arc::clone(&plan.physical_schema),
            logical_schema: Arc::clone(&plan.logical_schema),
            kernel_schemas: plan.kernel_schemas.clone(),
            physical_predicate: plan.physical_predicate.clone(),
            row_predicate: None,
            output_batch_size_rows: Some(2),
            permit,
            cancellation,
        }
    }

    fn one_file_limiter(
        options: DeltaScanExecutionOptions,
    ) -> Result<Arc<ScanReadLimiter>, DeltaReaderError> {
        let options = options
            .with_prefetch_files_per_partition(1)
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?;
        Ok(ScanReadLimiter::new(options, 1, 1))
    }

    #[tokio::test]
    async fn resolves_table_relative_paths_and_requires_file_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-object-resolution")?;
        fs::write(root.path().join("part.parquet"), b"data")?;
        let metrics = metrics();
        let reader = reader(&root, DeltaScanExecutionOptions::new(), metrics.clone())?;

        let object = reader
            .parquet_object_for_task(&task("part.parquet", Some(4))?)
            .await?;
        assert!(object.path.as_ref().ends_with("part.parquet"));
        assert_eq!(object.file_size, 4);
        assert_eq!(
            object.store.get_range(&object.path, 1..3).await?.as_ref(),
            b"at"
        );
        assert_eq!(
            metrics.snapshot().parquet_data_file_range_get_operations,
            Some(1)
        );

        let error = match reader
            .parquet_object_for_task(&task("secret-file.parquet", None)?)
            .await
        {
            Ok(_) => return Err("missing size must fail".into()),
            Err(error) => error,
        };
        assert_eq!(error.code(), "data_file_read");
        assert!(!error.to_string().contains("secret-file"));
        Ok(())
    }

    #[tokio::test]
    async fn automatic_range_planning_reads_parquet_without_inner_multi_range_calls()
    -> Result<(), Box<dyn std::error::Error>> {
        let table_url = url::Url::parse("memory:///table/root/")?;
        let engine_context = Arc::new(DeltaKernelEngineContext::try_new(
            table_url,
            &DeltaStorageOptions::default(),
        )?);
        let store = engine_context.object_store();
        let metrics = metrics();
        let mut reader = DirectParquetReader::new(
            engine_context,
            DeltaScanExecutionOptions::new(),
            metrics.clone(),
            Arc::default(),
        );
        let tracked_store = GatedObjectStore::ungated(Arc::clone(&store));
        reader.store = Arc::new(MeteredParquetObjectStore::new(
            Arc::clone(&tracked_store) as Arc<dyn ObjectStore>,
            metrics.clone(),
            MultiRangeReadStrategy::ChooseAutomatically,
        ));
        let bytes = parquet_bytes()?;
        let task = task("part-00000.parquet", Some(u64::try_from(bytes.len())?))?;
        let object = reader.resolve_parquet_object(&task)?;

        assert_eq!(object.path.as_ref(), "table/root/part-00000.parquet");
        store.put(&object.path, bytes.into()).await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut stream = reader
            .open_physical_parquet_stream(&task, &schema, Default::default())
            .await?;
        let batch = stream.next_batch().await?.ok_or("expected one batch")?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected Int32Array")?;

        assert_eq!(ids.values(), &[1, 2, 3]);
        assert!(stream.next_batch().await?.is_none());
        let snapshot = metrics.snapshot();
        assert!(
            snapshot
                .parquet_data_file_exact_ranges_requested
                .is_some_and(|ranges| ranges > 0)
        );
        assert!(
            snapshot
                .parquet_data_file_physical_range_requests_planned
                .is_some_and(|requests| requests > 0)
        );
        assert!(
            snapshot
                .parquet_data_file_cold_start_range_plans
                .is_some_and(|plans| plans > 0)
        );
        assert_eq!(
            snapshot.parquet_data_file_store_delegated_range_plans,
            Some(0)
        );
        assert_eq!(tracked_store.multi_range_call_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn regression_invalid_parquet_range_is_redacted_at_the_reader_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-invalid-parquet-range")?;
        let bytes = parquet_bytes()?;
        fs::write(root.path().join("secret.parquet"), &bytes)?;
        let file_size = u64::try_from(bytes.len())?;
        let mut task = task("secret.parquet", Some(file_size))?;
        task.parquet_byte_range = Some(0..file_size + 1);
        let reader = reader(&root, DeltaScanExecutionOptions::new(), metrics())?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let error = match reader
            .open_physical_parquet_stream(&task, &schema, Default::default())
            .await
        {
            Ok(_) => return Err("invalid Parquet range unexpectedly succeeded".into()),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=data_file_read code=data_file_read reason=parquet_row_group_range_invalid"
        );
        assert!(!error.to_string().contains("secret.parquet"));
        Ok(())
    }

    #[tokio::test]
    async fn ranged_tasks_share_one_concurrent_parquet_metadata_load()
    -> Result<(), Box<dyn std::error::Error>> {
        let (reader, gated, mut first_task, schema) = gated_ranged_metadata_reader(
            "direct-ranged-metadata-single-flight",
            GateRequest::Range(1),
        )
        .await?;
        let file_size = first_task
            .file_size
            .ok_or("test task must have a file size")?;
        first_task.parquet_byte_range = Some(0..file_size / 2);
        let mut second_task = first_task.clone();
        second_task.parquet_byte_range = Some(file_size / 2..file_size);

        let first_reader = Arc::clone(&reader);
        let first_schema = Arc::clone(&schema);
        let first = tokio::spawn(async move {
            first_reader
                .open_physical_parquet_stream(&first_task, &first_schema, Default::default())
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), gated.wait_started()).await?;

        let second_reader = Arc::clone(&reader);
        let second = tokio::spawn(async move {
            second_reader
                .open_physical_parquet_stream(&second_task, &schema, Default::default())
                .await
        });
        tokio::task::yield_now().await;
        assert_eq!(gated.range_calls.load(Ordering::Acquire), 1);
        assert!(!second.is_finished());

        gated.release.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(5), first).await???;
        tokio::time::timeout(std::time::Duration::from_secs(5), second).await???;
        assert_eq!(
            reader
                .metrics
                .snapshot()
                .parquet_data_file_range_get_operations,
            Some(1)
        );
        Ok(())
    }

    #[cfg(feature = "experimental-parquet-metadata-warmup")]
    #[tokio::test]
    async fn prepared_metadata_is_reused_by_a_whole_file_read()
    -> Result<(), Box<dyn std::error::Error>> {
        let (reader, gated, mut task, schema) = gated_ranged_metadata_reader(
            "direct-prepared-metadata-whole-file",
            GateRequest::Range(1),
        )
        .await?;
        task.parquet_byte_range = None;

        let prepare_reader = Arc::clone(&reader);
        let prepare_task = task.clone();
        let prepare = tokio::spawn(async move {
            prepare_reader
                .prepare_task_parquet_metadata(&prepare_task)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), gated.wait_started()).await?;
        gated.release.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(5), prepare).await???;

        let range_calls_after_prepare = gated.range_calls.load(Ordering::Acquire);
        assert!(range_calls_after_prepare > 0);
        let object = reader.resolve_parquet_object(&task)?;
        let cached = reader
            .metadata_cache
            .as_ref()
            .ok_or("prepared reader must retain its metadata cache")?
            .entry(&object.path, object.file_size);
        assert!(cached.get().is_some());

        reader
            .create_stream_builder(&object, None, &schema, false, true)
            .await?;
        assert_eq!(
            gated.range_calls.load(Ordering::Acquire),
            range_calls_after_prepare
        );
        Ok(())
    }

    #[cfg(feature = "experimental-parquet-metadata-warmup")]
    #[tokio::test]
    async fn cancelled_metadata_preparation_leaves_the_cache_retryable()
    -> Result<(), Box<dyn std::error::Error>> {
        let (reader, gated, task, _) = gated_ranged_metadata_reader(
            "direct-prepared-metadata-cancellation",
            GateRequest::Range(1),
        )
        .await?;
        let object = reader.resolve_parquet_object(&task)?;
        let cached = reader
            .metadata_cache
            .as_ref()
            .ok_or("prepared reader must retain its metadata cache")?
            .entry(&object.path, object.file_size);

        let prepare_reader = Arc::clone(&reader);
        let prepare_task = task.clone();
        let prepare = tokio::spawn(async move {
            prepare_reader
                .prepare_task_parquet_metadata(&prepare_task)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), gated.wait_started()).await?;
        prepare.abort();
        let join_error = prepare
            .await
            .expect_err("aborted metadata preparation unexpectedly completed");
        assert!(join_error.is_cancelled());
        assert!(gated.was_cancelled());
        assert!(cached.get().is_none());

        reader.prepare_task_parquet_metadata(&task).await?;
        assert!(cached.get().is_some());
        assert_eq!(gated.range_calls.load(Ordering::Acquire), 2);
        Ok(())
    }

    #[cfg(feature = "experimental-parquet-metadata-warmup")]
    #[tokio::test]
    async fn preparation_checks_the_file_limit_before_reading_parquet_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-prepare-file-limit")?;
        let parquet_bytes = parquet_bytes()?;
        write_partitioned_non_dv_table(&root, &parquet_bytes)?;
        add_second_partition_file(&root, &parquet_bytes)?;
        let plan = non_dv_plan(&root, None)?;

        let error = prepare_parquet_metadata(
            plan.as_ref(),
            Arc::new(ParquetMetadataCache::default()),
            Arc::default(),
            ParquetMetadataPreparationLimits {
                max_files: 1,
                max_retained_metadata_bytes: usize::MAX,
            },
        )
        .await
        .expect_err("two files must exceed a one-file preparation limit");

        assert_eq!(error.code(), "invalid_configuration");
        assert_eq!(
            plan.metrics
                .snapshot()
                .parquet_data_file_range_get_operations,
            Some(0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_ranged_metadata_load_can_be_retried() -> Result<(), Box<dyn std::error::Error>>
    {
        let (reader, gated, task, schema) = gated_ranged_metadata_reader(
            "direct-ranged-metadata-retry",
            GateRequest::Range(usize::MAX),
        )
        .await?;
        gated.fail_next_range();

        let error = match reader
            .open_physical_parquet_stream(&task, &schema, Default::default())
            .await
        {
            Ok(_) => return Err("injected metadata failure must fail the first split task".into()),
            Err(error) => error,
        };
        assert_eq!(error.code(), "data_file_read");
        reader
            .open_physical_parquet_stream(&task, &schema, Default::default())
            .await?;

        assert_eq!(gated.range_calls.load(Ordering::Acquire), 2);
        assert_eq!(
            reader
                .metrics
                .snapshot()
                .parquet_data_file_range_get_operations,
            Some(2)
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_ranged_metadata_load_wakes_a_retrying_task()
    -> Result<(), Box<dyn std::error::Error>> {
        let (reader, gated, task, schema) = gated_ranged_metadata_reader(
            "direct-ranged-metadata-cancellation",
            GateRequest::Range(1),
        )
        .await?;

        let first_reader = Arc::clone(&reader);
        let first_task = task.clone();
        let first_schema = Arc::clone(&schema);
        let first = tokio::spawn(async move {
            first_reader
                .open_physical_parquet_stream(&first_task, &first_schema, Default::default())
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), gated.wait_started()).await?;
        first.abort();
        let join_error = match first.await {
            Ok(_) => return Err("aborted metadata task unexpectedly completed".into()),
            Err(error) => error,
        };
        assert!(join_error.is_cancelled());
        assert!(gated.was_cancelled());

        gated.gate_next_range();
        let second_reader = Arc::clone(&reader);
        let second = tokio::spawn(async move {
            second_reader
                .open_physical_parquet_stream(&task, &schema, Default::default())
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), gated.wait_started()).await?;
        gated.release.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(5), second).await???;

        assert_eq!(gated.range_calls.load(Ordering::Acquire), 2);
        assert_eq!(
            reader
                .metrics
                .snapshot()
                .parquet_data_file_range_get_operations,
            Some(2)
        );
        Ok(())
    }

    #[tokio::test]
    async fn threshold_buffers_only_eligible_unsplit_files_with_one_metered_full_get()
    -> Result<(), Box<dyn std::error::Error>> {
        let bytes = b"0123456789abcdef";

        for (name, threshold, byte_range, expect_buffered) in [
            ("disabled", None, None, false),
            ("below", Some(bytes.len() - 1), None, false),
            ("exact", Some(bytes.len()), None, true),
            ("above", Some(bytes.len() + 1), None, true),
            ("split", Some(bytes.len()), Some(0..8), false),
        ] {
            let root = TestDir::new(name)?;
            fs::write(root.path().join("part.parquet"), bytes)?;
            let metrics = metrics();
            let options = DeltaScanExecutionOptions::new()
                .with_parquet_full_file_read_threshold_bytes(threshold)?;
            let reader = reader(&root, options, metrics.clone())?;
            let mut task = task("part.parquet", Some(u64::try_from(bytes.len())?))?;
            task.parquet_byte_range = byte_range;
            let object = reader.parquet_object_for_task(&task).await?;
            let snapshot = metrics.snapshot();
            assert_eq!(
                snapshot.parquet_data_file_full_get_operations,
                Some(u64::from(expect_buffered)),
                "{name}"
            );
            assert_eq!(
                snapshot.parquet_data_file_bytes_received,
                Some(if expect_buffered {
                    u64::try_from(bytes.len())?
                } else {
                    0
                }),
                "{name}"
            );

            assert_eq!(
                object.store.get_range(&object.path, 2..7).await?.as_ref(),
                b"23456",
                "{name}"
            );
            assert_eq!(
                metrics.snapshot().parquet_data_file_range_get_operations,
                Some(u64::from(!expect_buffered)),
                "{name}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn buffered_store_is_owned_only_by_its_file_object()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-file-local-buffer")?;
        let bytes = parquet_bytes()?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let metrics = metrics();
        let reader = reader(
            &root,
            DeltaScanExecutionOptions::new()
                .with_parquet_full_file_read_threshold_bytes(Some(bytes.len()))?,
            metrics.clone(),
        )?;
        let task = task("part.parquet", Some(u64::try_from(bytes.len())?))?;

        let object = reader.parquet_object_for_task(&task).await?;
        let buffered_store = Arc::downgrade(&object.store);
        assert_eq!(Arc::strong_count(&object.store), 1);
        drop(object);
        assert!(buffered_store.upgrade().is_none());

        let second = reader.parquet_object_for_task(&task).await?;
        assert_eq!(Arc::strong_count(&second.store), 1);
        assert_eq!(
            metrics.snapshot().parquet_data_file_full_get_operations,
            Some(2)
        );
        drop(second);
        Ok(())
    }

    #[tokio::test]
    async fn buffered_file_stream_holds_exactly_one_admission_permit_until_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-buffer-permit-bound")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes = parquet_bytes_with_properties(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(3))
                .build(),
        )?;
        write_partitioned_dv_table(&root, &parquet_bytes)?;
        let plan = pipeline_plan(&root, Some(parquet_bytes.len()))?;
        let reader = Arc::new(DirectParquetReader::new(
            Arc::clone(&plan.engine_context),
            plan.execution_options,
            plan.metrics.clone(),
            Arc::default(),
        ));
        let limiter = one_file_limiter(plan.execution_options)?;
        let partition = limiter.partition(0)?;
        let permit = partition.acquire().await?;
        let request = file_read_request(
            &plan,
            plan.partitions[0].file_tasks[0].clone(),
            permit,
            ScanCancellation::new(),
        );
        let file = reader.open_logical_data_file_stream(request).await?;

        let mut next_permit = Box::pin(partition.acquire());
        assert!(matches!(
            futures_util::poll!(&mut next_permit),
            std::task::Poll::Pending
        ));
        assert_eq!(
            plan.metrics
                .snapshot()
                .parquet_data_file_full_get_operations,
            Some(1)
        );
        drop(file);
        drop(next_permit.await?);
        Ok(())
    }

    #[tokio::test]
    async fn opens_projected_parquet_stream_with_configured_batch_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-projected-stream")?;
        let bytes = parquet_bytes()?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let reader = reader(&root, DeltaScanExecutionOptions::new(), metrics())?;
        let task = task("part.parquet", Some(u64::try_from(bytes.len())?))?;
        let target_schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, true)]));
        let mut stream = reader
            .open_physical_parquet_stream(
                &task,
                &target_schema,
                PhysicalParquetStreamOptions {
                    output_batch_size_rows: Some(2),
                    ..Default::default()
                },
            )
            .await?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next_batch().await? {
            batches.push(batch);
        }

        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert!(batches.iter().all(|batch| batch.num_columns() == 1));
        assert!(
            batches
                .iter()
                .all(|batch| batch.schema().field(0).name() == "name")
        );
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected projected StringArray")?;
        assert_eq!(names.value(0), "a");
        assert!(names.is_null(1));
        Ok(())
    }

    #[tokio::test]
    async fn reads_full_ordered_and_empty_physical_projections()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-projection-shapes")?;
        let bytes = parquet_bytes()?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let reader = reader(&root, DeltaScanExecutionOptions::new(), metrics())?;
        let task = task("part.parquet", Some(u64::try_from(bytes.len())?))?;

        for (schema, names, columns) in [
            (
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec!["id", "name"],
                2,
            ),
            (
                Arc::new(Schema::new(vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("id", DataType::Int32, false),
                ])),
                vec!["name", "id"],
                2,
            ),
            (Arc::new(Schema::empty()), Vec::new(), 0),
        ] {
            let mut stream = reader
                .open_physical_parquet_stream(&task, &schema, Default::default())
                .await?;
            let mut rows = 0;
            while let Some(batch) = stream.next_batch().await? {
                rows += batch.num_rows();
                assert_eq!(batch.num_columns(), columns);
                assert_eq!(
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.name().as_str())
                        .collect::<Vec<_>>(),
                    names
                );
            }
            assert_eq!(rows, 3);
        }
        Ok(())
    }

    #[tokio::test]
    async fn footer_hint_controls_metadata_request_count_and_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-footer-hint")?;
        let bytes = parquet_bytes()?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let file_size = u64::try_from(bytes.len())?;
        let mut snapshots = Vec::new();

        for options in [
            DeltaScanExecutionOptions::new(),
            DeltaScanExecutionOptions::new().with_parquet_metadata_size_hint_bytes(None)?,
            DeltaScanExecutionOptions::new().with_parquet_metadata_size_hint_bytes(Some(9))?,
        ] {
            let metrics = metrics();
            let reader = reader(&root, options, metrics.clone())?;
            let task = task("part.parquet", Some(file_size))?;
            let target_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ]));
            let _stream = reader
                .open_physical_parquet_stream(&task, &target_schema, Default::default())
                .await?;
            snapshots.push(metrics.snapshot());
        }

        assert_eq!(snapshots[0].parquet_data_file_range_get_operations, Some(1));
        assert_eq!(
            snapshots[0].parquet_data_file_bytes_received,
            Some(file_size)
        );
        // No row filter is attached, so neither fallback path loads the offset index.
        assert_eq!(snapshots[1].parquet_data_file_range_get_operations, Some(2));
        assert_eq!(snapshots[2].parquet_data_file_range_get_operations, Some(2));
        assert_eq!(
            snapshots[2].parquet_data_file_bytes_received,
            snapshots[1]
                .parquet_data_file_bytes_received
                .map(|bytes| bytes + 1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn row_group_pruning_is_conservative_and_preserves_rows_when_disabled()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-row-group-pruning")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let columns = || vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef];
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();
        let bytes = parquet_bytes_with_properties(Arc::clone(&schema), columns(), properties)?;
        fs::write(root.path().join("with-stats.parquet"), &bytes)?;
        let no_stats_properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let no_stats_bytes =
            parquet_bytes_with_properties(Arc::clone(&schema), columns(), no_stats_properties)?;
        fs::write(root.path().join("without-stats.parquet"), &no_stats_bytes)?;
        let predicate = DeltaKernelPredicate::from_test_predicate(Predicate::gt(
            Expression::Column(ColumnName::new(["id"])),
            Expression::Literal(Scalar::Integer(3)),
        ));
        let incompatible_stats_predicate =
            DeltaKernelPredicate::from_test_predicate(Predicate::gt(
                Expression::Column(ColumnName::new(["id"])),
                Expression::Literal(Scalar::String("not-an-integer".to_owned())),
            ));
        let reader = reader(&root, DeltaScanExecutionOptions::new(), metrics())?;

        for (path, file_size, predicate, expected) in [
            (
                "with-stats.parquet",
                bytes.len(),
                Some(&predicate),
                vec![4, 5, 6],
            ),
            (
                "with-stats.parquet",
                bytes.len(),
                None,
                vec![1, 2, 3, 4, 5, 6],
            ),
            (
                "without-stats.parquet",
                no_stats_bytes.len(),
                Some(&predicate),
                vec![1, 2, 3, 4, 5, 6],
            ),
            (
                "with-stats.parquet",
                bytes.len(),
                Some(&incompatible_stats_predicate),
                vec![1, 2, 3, 4, 5, 6],
            ),
        ] {
            let task = task(path, Some(u64::try_from(file_size)?))?;
            let mut stream = reader
                .open_physical_parquet_stream(
                    &task,
                    &schema,
                    PhysicalParquetStreamOptions {
                        row_group_predicate: predicate,
                        ..Default::default()
                    },
                )
                .await?;
            let mut ids = Vec::new();
            while let Some(batch) = stream.next_batch().await? {
                ids.extend_from_slice(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or("expected Int32Array")?
                        .values(),
                );
            }
            assert_eq!(ids, expected, "{path}");
        }

        let task = task("with-stats.parquet", Some(u64::try_from(bytes.len())?))?;
        let mut stream = reader
            .open_physical_parquet_stream(
                &task,
                &schema,
                PhysicalParquetStreamOptions {
                    row_group_predicate: Some(&predicate),
                    include_original_row_index: true,
                    ..Default::default()
                },
            )
            .await?;
        let mut original_indexes = Vec::new();
        while let Some((_batch, indexes)) = stream.next_batch_with_original_row_indexes().await? {
            original_indexes.extend(
                indexes
                    .ok_or("expected original row indexes")?
                    .values()
                    .iter()
                    .copied(),
            );
        }
        assert_eq!(original_indexes, [3, 4, 5]);
        Ok(())
    }

    #[tokio::test]
    async fn row_group_pruning_preserves_negative_fixed_len_decimal_stats()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-row-group-negative-decimal")?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            true,
        )]));
        let amounts =
            Decimal128Array::from(vec![Some(-100), Some(100)]).with_precision_and_scale(10, 2)?;
        let bytes = parquet_bytes_with_properties(
            Arc::clone(&schema),
            vec![Arc::new(amounts)],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(1))
                .build(),
        )?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let predicate = DeltaKernelPredicate::from_test_predicate(Predicate::lt(
            Expression::Column(ColumnName::new(["amount"])),
            Expression::Literal(Scalar::decimal(0, 10, 2)?),
        ));
        let reader = reader(&root, DeltaScanExecutionOptions::new(), metrics())?;
        let task = task("part.parquet", Some(u64::try_from(bytes.len())?))?;
        let mut stream = reader
            .open_physical_parquet_stream(
                &task,
                &schema,
                PhysicalParquetStreamOptions {
                    row_group_predicate: Some(&predicate),
                    ..Default::default()
                },
            )
            .await?;
        let batch = stream.next_batch().await?.ok_or("expected one batch")?;
        let amounts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or("expected Decimal128Array")?;

        assert_eq!(amounts.values(), &[-100]);
        assert!(stream.next_batch().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_pipeline_applies_transform_then_dv_and_preserves_hidden_columns()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-scheduler-pipeline")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(3))
            .build();
        let parquet_bytes = parquet_bytes_with_properties(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            properties,
        )?;
        write_partitioned_dv_table(&root, &parquet_bytes)?;

        let direct_plan = pipeline_plan(&root, None)?;
        let direct_metrics = direct_plan.metrics.clone();
        let direct = execute_pipeline_plan(direct_plan).await?;
        let buffered_plan = pipeline_plan(&root, Some(parquet_bytes.len()))?;
        let buffered_metrics = buffered_plan.metrics.clone();
        let buffered = execute_pipeline_plan(buffered_plan).await?;

        for batches in [&direct, &buffered] {
            let ids = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("planned id Int32Array")
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            assert_eq!(ids, [4, 6]);
            assert!(batches.iter().all(|batch| {
                batch.schema().fields()[0].name() == "id"
                    && batch.schema().fields()[1].name() == "region"
                    && batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .is_some_and(|regions| {
                            (0..regions.len()).all(|index| regions.value(index) == "west")
                        })
            }));
        }
        assert_eq!(direct, buffered);

        let direct_metrics = direct_metrics.snapshot();
        assert_eq!(direct_metrics.file_tasks_started, 1);
        assert_eq!(direct_metrics.file_tasks_completed, 1);
        assert_eq!(
            direct_metrics.estimated_parquet_task_bytes_admitted,
            Some(u64::try_from(parquet_bytes.len())?)
        );
        assert_eq!(direct_metrics.deletion_vector_payloads_loaded, 1);
        assert_eq!(direct_metrics.deletion_vectors_applied, 1);
        assert_eq!(direct_metrics.deletion_vector_rows_deleted, 1);
        assert_eq!(
            direct_metrics.parquet_data_file_full_get_operations,
            Some(0)
        );
        assert!(
            direct_metrics
                .parquet_data_file_range_get_operations
                .is_some_and(|count| count > 0)
        );
        let buffered_metrics = buffered_metrics.snapshot();
        assert_eq!(
            buffered_metrics.parquet_data_file_full_get_operations,
            Some(1)
        );
        assert_eq!(
            buffered_metrics.parquet_data_file_range_get_operations,
            Some(0)
        );
        assert_eq!(
            buffered_metrics.parquet_data_file_bytes_received,
            Some(u64::try_from(parquet_bytes.len())?)
        );
        assert_eq!(
            buffered_metrics.estimated_parquet_task_bytes_admitted,
            direct_metrics.estimated_parquet_task_bytes_admitted
        );
        Ok(())
    }

    #[tokio::test]
    async fn row_predicate_filters_before_scheduler_metrics_and_deletion_vector()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-row-predicate-dv")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes = parquet_bytes_with_properties(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(3))
                .build(),
        )?;
        write_partitioned_dv_table(&root, &parquet_bytes)?;

        let id = || Expression::Column(ColumnName::new(["id"]));
        let value = |value| Expression::Literal(Scalar::Integer(value));
        for (predicate, expected_ids, expected_deleted) in [
            (Predicate::gt(id(), value(3)), vec![4, 6], 1),
            (Predicate::eq(id(), value(5)), vec![], 1),
            (Predicate::eq(id(), value(4)), vec![4], 0),
            (Predicate::gt(id(), value(99)), vec![], 0),
        ] {
            let plan = pipeline_plan_for_backend(
                &root,
                None,
                Some(64 * 1024),
                ParquetReaderBackend::Direct,
                false,
            )?;
            let metrics = plan.metrics.clone();
            let batches = execute_pipeline_plan_with_row_predicate(
                plan,
                Some(DeltaKernelPredicate::from_test_predicate(predicate)),
            )
            .await?;
            let ids = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("planned id Int32Array")
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();

            assert_eq!(ids, expected_ids);
            let metrics = metrics.snapshot();
            assert_eq!(metrics.scheduler_rows_emitted, u64::try_from(ids.len())?);
            assert_eq!(metrics.deletion_vector_rows_deleted, expected_deleted);
        }
        Ok(())
    }

    #[tokio::test]
    async fn row_filter_projects_only_predicate_roots_and_preserves_wide_output()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-row-filter-projection")?;
        let mut fields = vec![Field::new("id", DataType::Int32, false)];
        fields.extend(
            (0..64).map(|index| Field::new(format!("payload_{index:03}"), DataType::Utf8, true)),
        );
        let schema = Arc::new(Schema::new(fields));
        let mut columns = vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef];
        columns.extend((0..64).map(|index| {
            Arc::new(StringArray::from(vec![
                format!("{index}-a"),
                format!("{index}-b"),
                format!("{index}-c"),
                format!("{index}-d"),
            ])) as ArrayRef
        }));
        let parquet_bytes = parquet_bytes_for(Arc::clone(&schema), columns)?;
        write_partitioned_non_dv_table(&root, &parquet_bytes)?;
        let plan = pipeline_plan_for_backend(
            &root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::Direct,
            false,
        )?;
        let reader = reader(&root, DeltaScanExecutionOptions::new(), metrics())?;
        let predicate = DeltaKernelPredicate::from_test_predicate(Predicate::gt(
            Expression::Column(ColumnName::new(["id"])),
            Expression::Literal(Scalar::Integer(2)),
        ));
        let multi_root_predicate = DeltaKernelPredicate::from_test_predicate(Predicate::and(
            Predicate::eq(
                Expression::Column(ColumnName::new(["payload_063"])),
                Expression::Literal(Scalar::String("63-c".to_owned())),
            ),
            Predicate::gt(
                Expression::Column(ColumnName::new(["id"])),
                Expression::Literal(Scalar::Integer(2)),
            ),
        ));
        assert_eq!(
            super::predicate_root_indices(&multi_root_predicate, &schema)?,
            [0, 64]
        );
        let parquet_schema = ArrowSchemaConverter::new().convert(schema.as_ref())?;
        let row_filter = reader.build_row_filter(
            &multi_root_predicate,
            &plan.kernel_schemas,
            &schema,
            &parquet_schema,
            &schema,
        )?;
        let projection = row_filter.predicates()[0].projection();
        assert!(projection.leaf_included(0));
        assert!((1..64).all(|index| !projection.leaf_included(index)));
        assert!(projection.leaf_included(64));

        let nested_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "profile",
                DataType::Struct(
                    vec![
                        Field::new("age", DataType::Int32, true),
                        Field::new("label", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let nested_parquet_schema = ArrowSchemaConverter::new().convert(nested_schema.as_ref())?;
        let nested_predicate = DeltaKernelPredicate::from_test_predicate(Predicate::and(
            Predicate::gt(
                Expression::Column(ColumnName::new(["profile", "age"])),
                Expression::Literal(Scalar::Integer(18)),
            ),
            Predicate::eq(
                Expression::Column(ColumnName::new(["profile", "label"])),
                Expression::Literal(Scalar::String("adult".to_owned())),
            ),
        ));
        assert_eq!(
            super::predicate_root_indices(&nested_predicate, &nested_schema)?,
            [1]
        );
        let nested_filter = reader.build_row_filter(
            &nested_predicate,
            &plan.kernel_schemas,
            &nested_schema,
            &nested_parquet_schema,
            &nested_schema,
        )?;
        let nested_projection = nested_filter.predicates()[0].projection();
        assert!(!nested_projection.leaf_included(0));
        assert!(nested_projection.leaf_included(1));
        assert!(nested_projection.leaf_included(2));
        assert!(!nested_projection.leaf_included(3));

        let field_id =
            |id: i32| HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_owned(), id.to_string())]);
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("stale_id", DataType::Int32, false).with_metadata(field_id(1)),
            Field::new("payload", DataType::Utf8, true).with_metadata(field_id(2)),
        ]));
        let mapped_schema = Arc::new(Schema::new(vec![
            Field::new("physical_id", DataType::Int32, false).with_metadata(field_id(1)),
            Field::new("payload", DataType::Utf8, true).with_metadata(field_id(2)),
        ]));
        let mapped_parquet_schema = ArrowSchemaConverter::new().convert(file_schema.as_ref())?;
        let mapped_predicate = DeltaKernelPredicate::from_test_predicate(Predicate::gt(
            Expression::Column(ColumnName::new(["physical_id"])),
            Expression::Literal(Scalar::Integer(2)),
        ));
        let mapped_filter = reader.build_row_filter(
            &mapped_predicate,
            &plan.kernel_schemas,
            &mapped_schema,
            &mapped_parquet_schema,
            &file_schema,
        )?;
        let mapped_projection = mapped_filter.predicates()[0].projection();
        assert!(mapped_projection.leaf_included(0));
        assert!(!mapped_projection.leaf_included(1));

        let task = task("part.parquet", Some(u64::try_from(parquet_bytes.len())?))?;
        let narrow_schema = Arc::new(schema.project(&[0, 1])?);
        let mut qualifying_ids = Vec::new();
        for target_schema in [narrow_schema, schema] {
            let mut stream = reader
                .open_physical_parquet_stream(
                    &task,
                    &target_schema,
                    PhysicalParquetStreamOptions {
                        row_filter: Some(RowFilterInput {
                            predicate: &predicate,
                            kernel_schemas: &plan.kernel_schemas,
                        }),
                        ..Default::default()
                    },
                )
                .await?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.next_batch().await? {
                assert_eq!(batch.schema(), target_schema);
                batches.push(batch);
            }
            qualifying_ids.push(int32_ids(&batches)?);
        }

        assert_eq!(qualifying_ids, [vec![3, 4], vec![3, 4]]);
        Ok(())
    }

    #[tokio::test]
    async fn optional_offset_indexes_reduce_localized_row_filter_reads()
    -> Result<(), Box<dyn std::error::Error>> {
        const ROWS: usize = 2_048;
        const PAYLOAD_COLUMNS: usize = 4;

        let indexed_root = TestDir::new("direct-row-filter-indexed")?;
        let unindexed_root = TestDir::new("direct-row-filter-unindexed")?;
        let mut fields = vec![Field::new("id", DataType::Int32, false)];
        fields.extend(
            (0..PAYLOAD_COLUMNS)
                .map(|index| Field::new(format!("payload_{index:03}"), DataType::Utf8, true)),
        );
        let schema = Arc::new(Schema::new(fields));
        let mut columns = vec![Arc::new(Int32Array::from_iter_values(0..ROWS as i32)) as ArrayRef];
        let payload_body = "abcdefghijklmnopqrstuvwxyz0123456789".repeat(64);
        columns.extend((0..PAYLOAD_COLUMNS).map(|column| {
            Arc::new(StringArray::from_iter((0..ROWS).map(|row| {
                ((row + column) % 3 != 0)
                    .then(|| format!("payload-{column:03}-{row:08}-{payload_body}"))
            }))) as ArrayRef
        }));
        let properties = |offset_index_disabled| {
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(1_024))
                .set_write_batch_size(64)
                .set_data_page_row_count_limit(64)
                .set_dictionary_enabled(false)
                .set_compression(Compression::UNCOMPRESSED)
                .set_offset_index_disabled(offset_index_disabled)
                .build()
        };
        let indexed_bytes =
            parquet_bytes_with_properties(Arc::clone(&schema), columns.clone(), properties(false))?;
        let unindexed_bytes =
            parquet_bytes_with_properties(Arc::clone(&schema), columns, properties(true))?;
        write_partitioned_non_dv_table(&indexed_root, &indexed_bytes)?;
        write_partitioned_non_dv_table(&unindexed_root, &unindexed_bytes)?;

        let predicate = DeltaKernelPredicate::from_test_predicate(Predicate::eq(
            Expression::Column(ColumnName::new(["id"])),
            Expression::Literal(Scalar::Integer(42)),
        ));
        let mut results = Vec::new();
        for (root, parquet_bytes) in [
            (&indexed_root, &indexed_bytes),
            (&unindexed_root, &unindexed_bytes),
        ] {
            results
                .push(read_with_row_filter(root, parquet_bytes.len(), &schema, &predicate).await?);
        }

        assert_eq!(results[0].0, results[1].0);
        assert_eq!(int32_ids(&results[0].0)?, [42]);
        assert!(
            results[0].1.saturating_mul(2) < results[1].1,
            "indexed read used {} bytes; unindexed read used {} bytes",
            results[0].1,
            results[1].1
        );
        Ok(())
    }

    #[tokio::test]
    async fn optional_offset_indexes_preserve_adversarial_row_selections()
    -> Result<(), Box<dyn std::error::Error>> {
        const ROWS: usize = 512;

        let indexed_root = TestDir::new("direct-row-selection-indexed")?;
        let unindexed_root = TestDir::new("direct-row-selection-unindexed")?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let columns = vec![
            Arc::new(Int32Array::from_iter_values(0..ROWS as i32)) as ArrayRef,
            Arc::new(StringArray::from_iter(
                (0..ROWS).map(|row| (row % 5 != 0).then(|| format!("payload-{row:08}"))),
            )),
        ];
        let properties = |offset_index_disabled| {
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(256))
                .set_write_batch_size(64)
                .set_data_page_row_count_limit(64)
                .set_offset_index_disabled(offset_index_disabled)
                .build()
        };
        let indexed_bytes =
            parquet_bytes_with_properties(Arc::clone(&schema), columns.clone(), properties(false))?;
        let unindexed_bytes =
            parquet_bytes_with_properties(Arc::clone(&schema), columns, properties(true))?;
        write_partitioned_non_dv_table(&indexed_root, &indexed_bytes)?;
        write_partitioned_non_dv_table(&unindexed_root, &unindexed_bytes)?;

        let id = || Expression::Column(ColumnName::new(["id"]));
        let literal = |value| Expression::Literal(Scalar::Integer(value));
        let all_ids = (0..ROWS)
            .map(i32::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let scattered_ids = all_ids.iter().copied().step_by(64).collect::<Vec<_>>();
        let cases = [
            ("no matches", Predicate::eq(id(), literal(-1)), Vec::new()),
            ("all matches", Predicate::gt(id(), literal(-1)), all_ids),
            (
                "cross-page matches",
                Predicate::and(
                    Predicate::gt(id(), literal(61)),
                    Predicate::lt(id(), literal(66)),
                ),
                vec![62, 63, 64, 65],
            ),
            (
                "scattered matches",
                Predicate::or_from(
                    scattered_ids
                        .iter()
                        .copied()
                        .map(|value| Predicate::eq(id(), literal(value))),
                ),
                scattered_ids,
            ),
        ];

        for (name, predicate, expected_ids) in cases {
            let predicate = DeltaKernelPredicate::from_test_predicate(predicate);
            let (indexed, _) =
                read_with_row_filter(&indexed_root, indexed_bytes.len(), &schema, &predicate)
                    .await?;
            let (unindexed, _) =
                read_with_row_filter(&unindexed_root, unindexed_bytes.len(), &schema, &predicate)
                    .await?;

            assert_eq!(indexed, unindexed, "{name}");
            assert_eq!(int32_ids(&indexed)?, expected_ids, "{name}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn malformed_optional_offset_index_falls_back_without_losing_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        const ROWS: usize = 128;

        let corrupt_root = TestDir::new("direct-row-selection-corrupt-index")?;
        let unindexed_root = TestDir::new("direct-row-selection-missing-index")?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        let columns = vec![
            Arc::new(Int32Array::from_iter_values(0..ROWS as i32)) as ArrayRef,
            Arc::new(StringArray::from_iter(
                (0..ROWS).map(|row| (row % 5 != 0).then(|| format!("payload-{row:08}"))),
            )),
        ];
        let properties = |offset_index_disabled| {
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(64))
                .set_write_batch_size(32)
                .set_data_page_row_count_limit(32)
                .set_offset_index_disabled(offset_index_disabled)
                .build()
        };
        let mut corrupt_bytes =
            parquet_bytes_with_properties(Arc::clone(&schema), columns.clone(), properties(false))?;
        let unindexed_bytes =
            parquet_bytes_with_properties(Arc::clone(&schema), columns, properties(true))?;
        write_partitioned_non_dv_table(&corrupt_root, &corrupt_bytes)?;
        write_partitioned_non_dv_table(&unindexed_root, &unindexed_bytes)?;

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&File::open(corrupt_root.path().join("part.parquet"))?)?;
        let payload = metadata.row_group(0).column(1);
        let index_start = usize::try_from(
            payload
                .offset_index_offset()
                .ok_or("test fixture did not contain a payload offset index")?,
        )?;
        let index_length = usize::try_from(
            payload
                .offset_index_length()
                .ok_or("test fixture did not contain a payload offset-index length")?,
        )?;
        let index_range = index_start..index_start.saturating_add(index_length);
        // Damage only one column's offset index. The data pages and footer remain valid, so the
        // optional policy must discard the unusable indexes and read the column chunks normally.
        corrupt_bytes[index_range].fill(0xff);
        fs::write(corrupt_root.path().join("part.parquet"), &corrupt_bytes)?;

        let predicate = DeltaKernelPredicate::from_test_predicate(Predicate::eq(
            Expression::Column(ColumnName::new(["id"])),
            Expression::Literal(Scalar::Integer(42)),
        ));
        let (corrupt, _) =
            read_with_row_filter(&corrupt_root, corrupt_bytes.len(), &schema, &predicate).await?;
        let (unindexed, _) =
            read_with_row_filter(&unindexed_root, unindexed_bytes.len(), &schema, &predicate)
                .await?;

        assert_eq!(corrupt, unindexed);
        assert_eq!(int32_ids(&corrupt)?, [42]);
        Ok(())
    }

    #[tokio::test]
    async fn delta_kernel_matches_direct_for_transform_relative_dv_and_controls()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("kernel-direct-relative-dv-parity")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes = parquet_bytes_with_properties(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(3))
                .build(),
        )?;
        write_partitioned_dv_table(&root, &parquet_bytes)?;

        let direct_plan = pipeline_plan_for_backend(
            &root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::Direct,
            false,
        )?;
        let direct = execute_pipeline_plan(direct_plan).await?;
        let kernel_default_plan =
            pipeline_plan_for_backend(&root, None, None, ParquetReaderBackend::DeltaKernel, false)?;
        let kernel_default_metrics = kernel_default_plan.metrics.clone();
        let kernel_default = execute_kernel_plan(kernel_default_plan).await?;
        let kernel_tuned_plan = pipeline_plan_for_backend(
            &root,
            Some(parquet_bytes.len()),
            Some(1),
            ParquetReaderBackend::DeltaKernel,
            false,
        )?;
        let kernel_tuned_metrics = kernel_tuned_plan.metrics.clone();
        let kernel_tuned = execute_kernel_plan(kernel_tuned_plan).await?;

        let rows = |batches: &[RecordBatch]| -> Result<Vec<(i32, String)>, &'static str> {
            let mut rows = Vec::new();
            for batch in batches {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("expected Int32 ids")?;
                let regions = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or("expected Utf8 regions")?;
                rows.extend(
                    (0..batch.num_rows())
                        .map(|index| (ids.value(index), regions.value(index).to_owned())),
                );
            }
            Ok(rows)
        };
        let expected = vec![
            (1, "west".to_owned()),
            (2, "west".to_owned()),
            (3, "west".to_owned()),
            (4, "west".to_owned()),
            (6, "west".to_owned()),
        ];
        assert_eq!(rows(&direct)?, expected);
        assert_eq!(rows(&kernel_default)?, expected);
        assert_eq!(rows(&kernel_tuned)?, expected);
        assert!(
            direct
                .iter()
                .chain(kernel_default.iter())
                .chain(kernel_tuned.iter())
                .all(|batch| batch.schema().fields()[0].name() == "id"
                    && batch.schema().fields()[1].name() == "region")
        );
        let expected_batches = u64::try_from(kernel_default.len())?;
        assert_eq!(kernel_tuned.len(), kernel_default.len());

        for metrics in [
            kernel_default_metrics.snapshot(),
            kernel_tuned_metrics.snapshot(),
        ] {
            assert_eq!(metrics.parquet_backend, ParquetReaderBackend::DeltaKernel);
            assert_eq!(metrics.scan_partitions_started, 1);
            assert_eq!(metrics.scan_partitions_completed, 1);
            assert_eq!(metrics.file_tasks_started, 1);
            assert_eq!(metrics.file_tasks_completed, 1);
            assert_eq!(metrics.scheduler_batches_emitted, expected_batches);
            assert_eq!(metrics.scheduler_rows_emitted, 5);
            assert_eq!(metrics.deletion_vector_payloads_loaded, 1);
            assert_eq!(metrics.deletion_vectors_applied, 1);
            assert_eq!(metrics.deletion_vector_rows_deleted, 1);
            assert_eq!(metrics.parquet_data_file_range_get_operations, None);
            assert_eq!(metrics.parquet_data_file_full_get_operations, None);
            assert_eq!(metrics.parquet_data_file_bytes_received, None);
            assert_eq!(metrics.estimated_parquet_task_bytes_admitted, None);
        }
        Ok(())
    }

    #[tokio::test]
    async fn delta_kernel_dv_predicate_fallback_preserves_rows_for_residual_filtering()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("kernel-dv-predicate-fallback")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes = parquet_bytes_with_properties(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(3))
                .build(),
        )?;
        write_partitioned_dv_table(&root, &parquet_bytes)?;

        let direct = execute_pipeline_plan(pipeline_plan_for_backend(
            &root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::Direct,
            true,
        )?)
        .await?;
        let kernel = execute_kernel_plan(pipeline_plan_for_backend(
            &root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::DeltaKernel,
            true,
        )?)
        .await?;
        let ids = |batches: &[RecordBatch]| -> Result<Vec<i32>, &'static str> {
            batches
                .iter()
                .map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .map(|ids| ids.values().to_vec())
                        .ok_or("expected Int32 ids")
                })
                .collect::<Result<Vec<_>, _>>()
                .map(|ids| ids.into_iter().flatten().collect())
        };
        let direct_ids = ids(&direct)?;
        let kernel_ids = ids(&kernel)?;

        assert_eq!(direct_ids, [4, 6]);
        assert_eq!(kernel_ids, [1, 2, 3, 4, 6]);
        assert_eq!(
            kernel_ids
                .into_iter()
                .filter(|id| *id > 3)
                .collect::<Vec<_>>(),
            direct_ids
        );
        Ok(())
    }

    #[tokio::test]
    async fn delta_kernel_matches_direct_for_snapshots_projection_and_non_dv_predicate()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("kernel-direct-snapshot-predicate-parity")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes = parquet_bytes_with_properties(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(3))
                .build(),
        )?;
        write_partitioned_non_dv_table(&root, &parquet_bytes)?;

        for selection in [
            DeltaSnapshotSelection::Latest,
            DeltaSnapshotSelection::Version(0),
        ] {
            let direct = execute_pipeline_plan(pipeline_plan_for_backend_at(
                &root,
                None,
                Some(64 * 1024),
                ParquetReaderBackend::Direct,
                true,
                selection,
                1,
            )?)
            .await?;
            let kernel = execute_kernel_plan(pipeline_plan_for_backend_at(
                &root,
                Some(parquet_bytes.len()),
                Some(1),
                ParquetReaderBackend::DeltaKernel,
                true,
                selection,
                1,
            )?)
            .await?;

            assert_eq!(int32_ids(&direct)?, [4, 5, 6]);
            assert_eq!(int32_ids(&kernel)?, [4, 5, 6]);
            assert!(
                direct
                    .iter()
                    .chain(kernel.iter())
                    .all(|batch| batch.schema().field(0).name() == "id"
                        && batch.schema().field(1).name() == "region")
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn delta_kernel_matches_direct_for_partition_statistics_and_zero_file_predicates()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("kernel-direct-pruning-parity")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let west = parquet_bytes_for(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from_iter_values(1..=6))],
        )?;
        let east = parquet_bytes_for(schema, vec![Arc::new(Int32Array::from_iter_values(7..=12))])?;
        write_partitioned_non_dv_table(&root, &west)?;
        add_second_partition_file(&root, &east)?;

        let cases = [
            (
                Predicate::eq(
                    Expression::Column(ColumnName::new(["region"])),
                    Expression::Literal(Scalar::String("east".to_owned())),
                ),
                (7..=12).collect::<Vec<_>>(),
                1,
            ),
            (
                Predicate::lt(
                    Expression::Column(ColumnName::new(["id"])),
                    Expression::Literal(Scalar::Integer(7)),
                ),
                (1..=6).collect(),
                1,
            ),
            (
                Predicate::gt(
                    Expression::Column(ColumnName::new(["id"])),
                    Expression::Literal(Scalar::Integer(100)),
                ),
                Vec::new(),
                0,
            ),
        ];

        for (predicate, expected, expected_files) in cases {
            let direct_plan = predicate_pipeline_plan_for_backend(
                &root,
                ParquetReaderBackend::Direct,
                predicate.clone(),
            )?;
            let kernel_plan = predicate_pipeline_plan_for_backend(
                &root,
                ParquetReaderBackend::DeltaKernel,
                predicate,
            )?;
            for plan in [&direct_plan, &kernel_plan] {
                assert_eq!(
                    plan.partitions
                        .iter()
                        .map(|partition| partition.file_tasks.len())
                        .sum::<usize>(),
                    expected_files
                );
            }
            let direct = execute_pipeline_plan(direct_plan).await?;
            let kernel = execute_kernel_plan(kernel_plan).await?;
            assert_eq!(int32_ids(&direct)?, expected);
            assert_eq!(int32_ids(&kernel)?, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn delta_kernel_matches_direct_for_inline_deletion_vectors()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("kernel-direct-inline-dv-parity")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes =
            parquet_bytes_for(schema, vec![Arc::new(Int32Array::from_iter_values(1..=30))])?;
        write_partitioned_inline_dv_table(&root, &parquet_bytes)?;

        let direct = execute_pipeline_plan(pipeline_plan_for_backend(
            &root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::Direct,
            false,
        )?)
        .await?;
        let kernel = execute_kernel_plan(pipeline_plan_for_backend(
            &root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::DeltaKernel,
            false,
        )?)
        .await?;
        let expected = (1..=30)
            .filter(|id| ![4, 5, 8, 12, 19, 30].contains(id))
            .collect::<Vec<_>>();

        assert_eq!(int32_ids(&direct)?, expected);
        assert_eq!(int32_ids(&kernel)?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn delta_kernel_matches_direct_for_empty_and_multiple_partitions()
    -> Result<(), Box<dyn std::error::Error>> {
        let empty_root = TestDir::new("kernel-direct-empty-parity")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let empty_bytes = parquet_bytes_for(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        )?;
        write_partitioned_non_dv_table(&empty_root, &empty_bytes)?;
        remove_all_data_files_from_log(&empty_root)?;
        let direct_empty_plan = pipeline_plan_for_backend_at(
            &empty_root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::Direct,
            false,
            DeltaSnapshotSelection::Latest,
            2,
        )?;
        let kernel_empty_plan = pipeline_plan_for_backend_at(
            &empty_root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::DeltaKernel,
            false,
            DeltaSnapshotSelection::Latest,
            2,
        )?;
        assert!(direct_empty_plan.partitions.is_empty());
        assert!(kernel_empty_plan.partitions.is_empty());
        let kernel_empty_metrics = kernel_empty_plan.metrics.clone();
        assert!(execute_pipeline_plan(direct_empty_plan).await?.is_empty());
        assert!(execute_kernel_plan(kernel_empty_plan).await?.is_empty());
        assert_eq!(kernel_empty_metrics.snapshot().scan_partitions_started, 0);
        assert_eq!(kernel_empty_metrics.snapshot().file_tasks_started, 0);

        let root = TestDir::new("kernel-direct-multi-partition-parity")?;
        let west = parquet_bytes_for(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from_iter_values(1..=6))],
        )?;
        let east = parquet_bytes_for(schema, vec![Arc::new(Int32Array::from_iter_values(7..=12))])?;
        write_partitioned_non_dv_table(&root, &west)?;
        add_second_partition_file(&root, &east)?;
        let direct_plan = pipeline_plan_for_backend_at(
            &root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::Direct,
            false,
            DeltaSnapshotSelection::Latest,
            2,
        )?;
        let kernel_first_plan = pipeline_plan_for_backend_at(
            &root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::DeltaKernel,
            false,
            DeltaSnapshotSelection::Latest,
            2,
        )?;
        let kernel_second_plan = pipeline_plan_for_backend_at(
            &root,
            Some(east.len()),
            Some(1),
            ParquetReaderBackend::DeltaKernel,
            false,
            DeltaSnapshotSelection::Latest,
            2,
        )?;
        assert_eq!(direct_plan.partitions.len(), 2);
        assert_eq!(kernel_first_plan.partitions.len(), 2);
        assert_eq!(kernel_second_plan.partitions.len(), 2);

        let kernel_first_metrics = kernel_first_plan.metrics.clone();
        let kernel_second_metrics = kernel_second_plan.metrics.clone();
        let direct = execute_pipeline_plan(direct_plan).await?;
        let (kernel_first, kernel_second) = tokio::try_join!(
            execute_kernel_plan(kernel_first_plan),
            execute_kernel_plan(kernel_second_plan)
        )?;
        assert_eq!(int32_ids(&direct)?, int32_ids(&kernel_first)?);
        assert_eq!(int32_ids(&kernel_first)?, int32_ids(&kernel_second)?);
        assert_eq!(int32_ids(&kernel_first)?.len(), 12);
        for metrics in [
            kernel_first_metrics.snapshot(),
            kernel_second_metrics.snapshot(),
        ] {
            assert_eq!(metrics.scan_partitions_started, 2);
            assert_eq!(metrics.scan_partitions_completed, 2);
            assert_eq!(metrics.file_tasks_started, 2);
            assert_eq!(metrics.file_tasks_completed, 2);
            assert_eq!(metrics.scheduler_rows_emitted, 12);
        }
        Ok(())
    }

    #[tokio::test]
    async fn delta_kernel_partition_drop_before_first_poll_starts_no_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("kernel-drop-before-poll")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes =
            parquet_bytes_for(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])?;
        write_partitioned_non_dv_table(&root, &parquet_bytes)?;
        let plan = pipeline_plan_for_backend(
            &root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::DeltaKernel,
            false,
        )?;
        let scheduler = DeltaScanScheduler::new(Arc::clone(&plan));
        let stream = scheduler.partition_stream(
            0,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            delta_kernel_file_executor(&plan),
        )?;

        drop(stream);
        tokio::task::yield_now().await;
        let metrics = plan.metrics.snapshot();
        assert_eq!(metrics.scan_partitions_started, 0);
        assert_eq!(metrics.scan_partitions_completed, 0);
        assert_eq!(metrics.file_tasks_started, 0);
        assert_eq!(metrics.file_tasks_completed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn delta_kernel_matches_direct_for_column_mapping_transform()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("kernel-direct-column-mapping-parity")?;
        let field = Field::new("phys_id", DataType::Int32, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            "1".to_owned(),
        )]));
        let parquet_bytes = parquet_bytes_for(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        fs::write(root.path().join("part.parquet"), &parquet_bytes)?;
        let log = root.path().join("_delta_log");
        fs::create_dir_all(&log)?;
        let protocol = serde_json::json!({
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["columnMapping"],
                "writerFeatures": ["columnMapping"]
            }
        });
        let schema = serde_json::json!({
            "type": "struct",
            "fields": [{
                "name": "id",
                "type": "integer",
                "nullable": false,
                "metadata": {
                    "delta.columnMapping.id": 1,
                    "delta.columnMapping.physicalName": "phys_id"
                }
            }]
        });
        let metadata = serde_json::json!({
            "metaData": {
                "id": "kernel-direct-column-mapping-parity",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.to_string(),
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": "name",
                    "delta.columnMapping.maxColumnId": "1"
                },
                "createdTime": 1587968585495_i64
            }
        });
        let add = serde_json::json!({
            "add": {
                "path": "part.parquet",
                "partitionValues": {},
                "size": parquet_bytes.len(),
                "modificationTime": 1587968586000_i64,
                "dataChange": true
            }
        });
        fs::write(
            log.join("00000000000000000000.json"),
            format!("{protocol}\n{metadata}\n{add}\n"),
        )?;
        let snapshot = load_delta_table_snapshot_blocking(
            &root.path().to_string_lossy(),
            &DeltaStorageOptions::new(),
            DeltaSnapshotSelection::Latest,
        )?;
        let plan = |backend| {
            plan_scan(
                &snapshot,
                None,
                &[],
                None,
                false,
                DeltaScanExecutionOptions::new().with_parquet_backend(backend),
                DeltaScanPartitionTargetOptions {
                    explicit_target_partitions: Some(1),
                    datafusion_target_partitions: None,
                },
            )
            .map(Arc::new)
        };
        let direct = execute_pipeline_plan(plan(ParquetReaderBackend::Direct)?).await?;
        let kernel = execute_kernel_plan(plan(ParquetReaderBackend::DeltaKernel)?).await?;

        assert_eq!(int32_ids(&direct)?, [1, 2, 3]);
        assert_eq!(int32_ids(&kernel)?, [1, 2, 3]);
        assert!(
            direct
                .iter()
                .chain(kernel.iter())
                .all(|batch| batch.schema().field(0).name() == "id")
        );
        Ok(())
    }

    #[tokio::test]
    async fn delta_kernel_reports_redacted_file_dv_transform_and_schema_failures()
    -> Result<(), Box<dyn std::error::Error>> {
        let assert_failed_metrics = |metrics: &crate::DeltaScanMetricsSnapshot,
                                     deletion_vector_failures| {
            assert_eq!(metrics.scan_partitions_started, 1);
            assert_eq!(metrics.scan_partitions_completed, 0);
            assert_eq!(metrics.file_tasks_started, 1);
            assert_eq!(metrics.file_tasks_completed, 0);
            assert_eq!(metrics.scheduler_batches_emitted, 0);
            assert_eq!(metrics.scheduler_rows_emitted, 0);
            assert_eq!(metrics.deletion_vector_failures, deletion_vector_failures);
            assert_eq!(metrics.parquet_data_file_range_get_operations, None);
            assert_eq!(metrics.parquet_data_file_full_get_operations, None);
            assert_eq!(metrics.parquet_data_file_bytes_received, None);
            assert_eq!(metrics.estimated_parquet_task_bytes_admitted, None);
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes = parquet_bytes_for(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;

        let metadata_root = TestDir::new("kernel-metadata-failure")?;
        write_partitioned_non_dv_table(&metadata_root, &parquet_bytes)?;
        let mut metadata_plan = pipeline_plan_for_backend(
            &metadata_root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::DeltaKernel,
            false,
        )?;
        Arc::get_mut(&mut metadata_plan)
            .ok_or("expected unique metadata plan")?
            .partitions[0]
            .file_tasks[0]
            .file_size = None;
        let metadata_metrics = metadata_plan.metrics.clone();
        let error = execute_kernel_plan(metadata_plan)
            .await
            .expect_err("missing size must fail");
        assert_eq!(error.code(), "data_file_read");
        assert_failed_metrics(&metadata_metrics.snapshot(), 0);
        assert!(
            !error
                .to_string()
                .contains(metadata_root.path().to_string_lossy().as_ref())
        );

        let parquet_root = TestDir::new("kernel-parquet-failure")?;
        write_partitioned_non_dv_table(&parquet_root, &parquet_bytes)?;
        let parquet_plan = pipeline_plan_for_backend(
            &parquet_root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::DeltaKernel,
            false,
        )?;
        let parquet_metrics = parquet_plan.metrics.clone();
        fs::write(
            parquet_root.path().join("part.parquet"),
            vec![0; parquet_bytes.len()],
        )?;
        let error = execute_kernel_plan(parquet_plan)
            .await
            .expect_err("corrupt Parquet must fail");
        assert_eq!(error.code(), "data_file_read");
        assert_failed_metrics(&parquet_metrics.snapshot(), 0);
        assert!(!error.to_string().contains("part.parquet"));

        let dv_root = TestDir::new("kernel-dv-failure")?;
        write_partitioned_dv_table(&dv_root, &parquet_bytes)?;
        let dv_plan = pipeline_plan_for_backend(
            &dv_root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::DeltaKernel,
            false,
        )?;
        let dv_metrics = dv_plan.metrics.clone();
        fs::remove_file(dv_root.path().join(DV_FILE))?;
        let error = execute_kernel_plan(dv_plan)
            .await
            .expect_err("missing DV must fail");
        assert_eq!(error.code(), "deletion_vector_read");
        assert_failed_metrics(&dv_metrics.snapshot(), 1);

        let transform_root = TestDir::new("kernel-transform-failure")?;
        write_partitioned_non_dv_table(&transform_root, &parquet_bytes)?;
        let mut transform_plan = pipeline_plan_for_backend(
            &transform_root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::DeltaKernel,
            false,
        )?;
        Arc::get_mut(&mut transform_plan)
            .ok_or("expected unique transform plan")?
            .partitions[0]
            .file_tasks[0]
            .transform = KernelPhysicalToLogicalTransform::from_test_expression(
            Expression::Column(ColumnName::new(["sensitive_missing_column"])),
        );
        let transform_metrics = transform_plan.metrics.clone();
        let error = execute_kernel_plan(transform_plan)
            .await
            .expect_err("invalid transform must fail");
        assert_eq!(error.code(), "physical_to_logical_transform");
        assert_failed_metrics(&transform_metrics.snapshot(), 0);
        assert!(!error.to_string().contains("sensitive_missing_column"));

        let schema_root = TestDir::new("kernel-schema-failure")?;
        write_partitioned_non_dv_table(&schema_root, &parquet_bytes)?;
        let mut schema_plan = pipeline_plan_for_backend(
            &schema_root,
            None,
            Some(64 * 1024),
            ParquetReaderBackend::DeltaKernel,
            false,
        )?;
        Arc::get_mut(&mut schema_plan)
            .ok_or("expected unique schema plan")?
            .logical_schema = Arc::new(Schema::empty());
        let schema_metrics = schema_plan.metrics.clone();
        let error = execute_kernel_plan(schema_plan)
            .await
            .expect_err("wrong logical schema must fail");
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=data_file_read code=data_file_read reason=backend_logical_schema_mismatch"
        );
        assert_failed_metrics(&schema_metrics.snapshot(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn scheduler_reads_full_and_projected_non_dv_logical_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-non-dv-logical-read")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes =
            parquet_bytes_for(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])?;
        write_partitioned_non_dv_table(&root, &parquet_bytes)?;

        let full_plan = non_dv_plan(&root, None)?;
        assert!(
            !full_plan.partitions[0].file_tasks[0]
                .deletion_vector
                .is_present()
        );
        let full = execute_pipeline_plan(full_plan).await?;
        let ids = full
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("planned Int32Array")
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();

        assert_eq!(ids, [1, 2, 3]);
        assert!(full.iter().all(|batch| {
            batch.num_columns() == 2
                && batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .is_some_and(|regions| {
                        (0..regions.len()).all(|index| regions.value(index) == "west")
                    })
        }));

        let projection = ["id".to_owned()];
        let projected = execute_pipeline_plan(non_dv_plan(&root, Some(&projection))?).await?;
        let projected_ids = projected
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("planned Int32Array")
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(projected_ids, [1, 2, 3]);
        assert!(
            projected
                .iter()
                .all(|batch| batch.num_columns() == 1 && batch.schema().field(0).name() == "id")
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_drops_metadata_and_full_gets_and_releases_permits()
    -> Result<(), Box<dyn std::error::Error>> {
        for (name, threshold, gate_request) in [
            ("metadata", None, GateRequest::Range(1)),
            ("full-get", Some(usize::MAX), GateRequest::FullGet),
        ] {
            let root = TestDir::new(&format!("direct-cancel-{name}"))?;
            let parquet_bytes = parquet_bytes()?;
            write_partitioned_dv_table(&root, &parquet_bytes)?;
            let plan = pipeline_plan(&root, threshold)?;
            let (reader, gated, task) =
                gated_file_reader(&plan, &parquet_bytes, gate_request).await?;
            let limiter = one_file_limiter(plan.execution_options)?;
            let partition = limiter.partition(0)?;
            let permit = partition.acquire().await?;
            let cancellation = ScanCancellation::new();
            let request = file_read_request(&plan, task, permit, cancellation.clone());
            let job =
                tokio::spawn(async move { reader.open_logical_data_file_stream(request).await });

            tokio::time::timeout(std::time::Duration::from_secs(5), gated.wait_started()).await?;
            assert!(cancellation.cancel());
            let result = tokio::time::timeout(std::time::Duration::from_secs(5), job).await??;
            let error = match result {
                Ok(_) => return Err(format!("{name} cancellation must fail the file read").into()),
                Err(error) => error,
            };
            assert_eq!(error.code(), "cancelled");
            assert!(gated.was_cancelled(), "{name} request was not dropped");
            drop(
                tokio::time::timeout(std::time::Duration::from_secs(5), partition.acquire())
                    .await??,
            );

            let metrics = plan.metrics.snapshot();
            assert_eq!(metrics.parquet_data_file_bytes_received, Some(0));
            match gate_request {
                GateRequest::FullGet => {
                    assert_eq!(metrics.parquet_data_file_full_get_operations, Some(1));
                    assert_eq!(metrics.parquet_data_file_range_get_operations, Some(0));
                }
                GateRequest::Range(_) => {
                    assert_eq!(metrics.parquet_data_file_full_get_operations, Some(0));
                    assert_eq!(metrics.parquet_data_file_range_get_operations, Some(1));
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_drops_batch_range_read_and_releases_permit()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-cancel-batch")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes = parquet_bytes_with_properties(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(3))
                .build(),
        )?;
        write_partitioned_dv_table(&root, &parquet_bytes)?;
        let plan = pipeline_plan(&root, None)?;
        let (reader, gated, task) =
            gated_file_reader(&plan, &parquet_bytes, GateRequest::Range(usize::MAX)).await?;
        let limiter = one_file_limiter(plan.execution_options)?;
        let partition = limiter.partition(0)?;
        let permit = partition.acquire().await?;
        let cancellation = ScanCancellation::new();
        let request = file_read_request(&plan, task, permit, cancellation.clone());
        let mut file = reader.open_logical_data_file_stream(request).await?;
        let before = plan.metrics.snapshot();
        gated.gate_next_range();
        let job = tokio::spawn(async move { file.next_batch().await });

        tokio::time::timeout(std::time::Duration::from_secs(5), gated.wait_started()).await?;
        assert!(cancellation.cancel());
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), job).await??;
        let error = match result {
            Ok(_) => return Err("batch cancellation must fail the file read".into()),
            Err(error) => error,
        };
        assert_eq!(error.code(), "cancelled");
        assert!(gated.was_cancelled());
        drop(tokio::time::timeout(std::time::Duration::from_secs(5), partition.acquire()).await??);

        let after = plan.metrics.snapshot();
        assert_eq!(
            after.parquet_data_file_range_get_operations,
            before
                .parquet_data_file_range_get_operations
                .map(|operations| operations + 1)
        );
        assert_eq!(
            after.parquet_data_file_bytes_received,
            before.parquet_data_file_bytes_received
        );
        Ok(())
    }

    #[tokio::test]
    async fn reports_redacted_setup_full_get_and_batch_read_errors()
    -> Result<(), Box<dyn std::error::Error>> {
        let corrupt_root = TestDir::new("direct-corrupt-setup")?;
        fs::write(corrupt_root.path().join("secret.parquet"), b"not parquet")?;
        let corrupt_metrics = metrics();
        let corrupt_reader = reader(
            &corrupt_root,
            DeltaScanExecutionOptions::new(),
            corrupt_metrics.clone(),
        )?;
        let corrupt_task = task("secret.parquet", Some(11))?;
        let empty_schema = Arc::new(Schema::empty());
        let error = match corrupt_reader
            .open_physical_parquet_stream(&corrupt_task, &empty_schema, Default::default())
            .await
        {
            Ok(_) => return Err("corrupt Parquet setup must fail".into()),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=data_file_read code=data_file_read reason=parquet_read_setup_failed"
        );
        assert!(!error.to_string().contains("secret.parquet"));
        assert_eq!(
            corrupt_metrics
                .snapshot()
                .parquet_data_file_range_get_operations,
            Some(1)
        );

        let missing_root = TestDir::new("direct-missing-full-get")?;
        let missing_metrics = metrics();
        let missing_reader = reader(
            &missing_root,
            DeltaScanExecutionOptions::new()
                .with_parquet_full_file_read_threshold_bytes(Some(64))?,
            missing_metrics.clone(),
        )?;
        let error = match missing_reader
            .parquet_object_for_task(&task("secret-missing.parquet", Some(12))?)
            .await
        {
            Ok(_) => return Err("missing full GET must fail".into()),
            Err(error) => error,
        };
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=data_file_read code=data_file_read reason=parquet_full_file_read_failed"
        );
        assert!(!error.to_string().contains("secret-missing.parquet"));
        assert_eq!(
            missing_metrics
                .snapshot()
                .parquet_data_file_full_get_operations,
            Some(1)
        );

        let range_root = TestDir::new("direct-batch-range-error")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes = parquet_bytes_with_properties(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(3))
                .build(),
        )?;
        write_partitioned_dv_table(&range_root, &parquet_bytes)?;
        let plan = pipeline_plan(&range_root, None)?;
        let (reader, gated, task) =
            gated_file_reader(&plan, &parquet_bytes, GateRequest::Range(usize::MAX)).await?;
        let limiter = one_file_limiter(plan.execution_options)?;
        let partition = limiter.partition(0)?;
        let permit = partition.acquire().await?;
        let decode_task = task.clone();
        let request = file_read_request(&plan, task, permit, ScanCancellation::new());
        let mut file = reader.open_logical_data_file_stream(request).await?;
        let before = plan.metrics.snapshot();
        gated.fail_next_range();
        let error = file
            .next_batch()
            .await
            .expect_err("injected batch range failure must fail");
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=data_file_read code=data_file_read reason=parquet_batch_read_failed"
        );
        let after = plan.metrics.snapshot();
        assert_eq!(
            after.parquet_data_file_range_get_operations,
            before
                .parquet_data_file_range_get_operations
                .map(|operations| operations + 1)
        );
        assert_eq!(
            after.parquet_data_file_bytes_received,
            before.parquet_data_file_bytes_received
        );
        drop(file);
        let permit =
            tokio::time::timeout(std::time::Duration::from_secs(5), partition.acquire()).await??;
        let request = file_read_request(&plan, decode_task, permit, ScanCancellation::new());
        let mut file = reader.open_logical_data_file_stream(request).await?;
        let before = plan.metrics.snapshot();
        gated.corrupt_next_range();
        let error = file
            .next_batch()
            .await
            .expect_err("corrupt Parquet data must fail decoding");
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=data_file_read code=data_file_read reason=parquet_batch_read_failed"
        );
        let after = plan.metrics.snapshot();
        assert_eq!(
            after.parquet_data_file_range_get_operations,
            before
                .parquet_data_file_range_get_operations
                .map(|operations| operations + 1)
        );
        assert!(
            after.parquet_data_file_bytes_received > before.parquet_data_file_bytes_received,
            "delivered corrupt bytes must remain visible after a decode error"
        );
        Ok(())
    }

    #[tokio::test]
    async fn logical_pipeline_reports_dv_transform_and_schema_errors()
    -> Result<(), Box<dyn std::error::Error>> {
        let dv_root = TestDir::new("direct-dv-error")?;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let parquet_bytes = parquet_bytes_with_properties(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(3))
                .build(),
        )?;
        write_partitioned_dv_table(&dv_root, &parquet_bytes)?;
        let dv_plan = pipeline_plan(&dv_root, None)?;
        fs::remove_file(dv_root.path().join(DV_FILE))?;
        let error = execute_pipeline_plan(Arc::clone(&dv_plan))
            .await
            .expect_err("missing deletion vector must fail");
        assert_eq!(error.code(), "deletion_vector_read");
        let dv_metrics = dv_plan.metrics.snapshot();
        assert_eq!(dv_metrics.file_tasks_started, 1);
        assert_eq!(dv_metrics.file_tasks_completed, 0);
        assert_eq!(dv_metrics.deletion_vector_failures, 1);
        assert_eq!(
            dv_metrics.estimated_parquet_task_bytes_admitted,
            Some(u64::try_from(parquet_bytes.len())?)
        );

        let transform_root = TestDir::new("direct-transform-errors")?;
        write_partitioned_dv_table(&transform_root, &parquet_bytes)?;
        let plan = pipeline_plan(&transform_root, None)?;
        let (reader, _gated, task) =
            gated_file_reader(&plan, &parquet_bytes, GateRequest::Range(usize::MAX)).await?;
        let limiter = one_file_limiter(plan.execution_options)?;
        let partition = limiter.partition(0)?;

        let mut invalid_transform_task = task.clone();
        invalid_transform_task.transform = KernelPhysicalToLogicalTransform::from_test_expression(
            Expression::Column(ColumnName::new(["secret_missing"])),
        );
        let permit = partition.acquire().await?;
        let request = file_read_request(
            &plan,
            invalid_transform_task,
            permit,
            ScanCancellation::new(),
        );
        let mut file = reader.open_logical_data_file_stream(request).await?;
        let error = file
            .next_batch()
            .await
            .expect_err("invalid transform must fail");
        assert_eq!(error.code(), "physical_to_logical_transform");
        assert!(!error.to_string().contains("secret_missing"));
        drop(file);

        let permit = partition.acquire().await?;
        let mut request = file_read_request(&plan, task, permit, ScanCancellation::new());
        request.logical_schema = Arc::new(Schema::empty());
        let mut file = reader.open_logical_data_file_stream(request).await?;
        let error = file
            .next_batch()
            .await
            .expect_err("wrong logical schema must fail");
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=data_file_read code=data_file_read reason=backend_logical_schema_mismatch"
        );
        Ok(())
    }
}
