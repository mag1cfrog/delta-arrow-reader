//! Private deletion-vector coordinate handling and Arrow masking.

use std::sync::{Arc, Weak};

use arrow::{
    array::{Array, BooleanArray, BooleanBufferBuilder, Int64Array},
    buffer::BooleanBuffer,
    compute::filter_record_batch,
    record_batch::RecordBatch,
};
use roaring::RoaringTreemap;
use snafu::{IntoError, ResultExt};

use crate::{
    DeltaReaderError, DeltaScanMetrics,
    delta::kernel::{DeltaKernelEngineContext, KernelDeletionVectorHandle},
    error::DeletionVectorReadSnafu,
};

/// A data file's deletion-vector handle and reusable decoded row coordinates.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct DeletionVectorMetadata {
    /// Kernel handle used to load the deletion-vector payload.
    handle: Option<KernelDeletionVectorHandle>,
    /// Weakly cached coordinates shared by overlapping readers of the same file.
    cached_rows: Arc<tokio::sync::Mutex<Option<Weak<DeletionVectorRows>>>>,
}

enum DeletionVectorRows {
    Bitmap { first: u64, bits: BooleanBuffer },
    Indexes(Box<[u64]>),
}

impl DeletionVectorRows {
    fn from_bitmap(indexes: RoaringTreemap) -> Self {
        let first = indexes.min().unwrap_or(0);
        let span = indexes
            .max()
            .and_then(|last| (last - first).checked_add(1))
            .filter(|len| len.div_ceil(8) <= indexes.len().saturating_mul(8))
            .and_then(|len| usize::try_from(len).ok());
        // Use a bitmap only when its payload is no larger than a u64 list.
        // Offsetting by the first deletion also keeps high but nearby rows compact.
        if let Some(len) = span {
            let mut bits = BooleanBufferBuilder::new(len);
            bits.resize(len);
            for (key, bitmap) in indexes.bitmaps() {
                let base = u64::from(key) << 32;
                bitmap
                    .iter()
                    .for_each(|row| bits.set_bit(((base | u64::from(row)) - first) as usize, true));
            }
            Self::Bitmap {
                first,
                bits: bits.finish(),
            }
        } else {
            Self::Indexes(indexes.into_iter().collect::<Vec<_>>().into_boxed_slice())
        }
    }

    #[cfg(test)]
    fn new(indexes: Vec<u64>) -> Self {
        Self::from_bitmap(indexes.into_iter().collect())
    }

    #[cfg(test)]
    fn all_indexes(&self) -> Vec<u64> {
        match self {
            Self::Bitmap { first, bits } => {
                bits.set_indices().map(|row| first + row as u64).collect()
            }
            Self::Indexes(indexes) => indexes.to_vec(),
        }
    }

    fn contains(&self, row: u64) -> bool {
        match self {
            Self::Bitmap { first, bits } => row
                .checked_sub(*first)
                .and_then(|row| usize::try_from(row).ok())
                .is_some_and(|row| row < bits.len() && bits.value(row)),
            Self::Indexes(indexes) => indexes.binary_search(&row).is_ok(),
        }
    }

    fn max(&self) -> Option<u64> {
        match self {
            Self::Bitmap { first, bits } => Some(first + (bits.len() as u64 - 1)),
            Self::Indexes(indexes) => indexes.last().copied(),
        }
    }

    // The caller has checked that start + keep.len() fits in u64.
    fn mask_ordered(&self, start: u64, keep: &mut [bool]) {
        if keep.is_empty() {
            return;
        }
        let end = start + keep.len() as u64 - 1;
        match self {
            Self::Indexes(indexes) => {
                let first = indexes.partition_point(|row| *row < start);
                let last = indexes.partition_point(|row| *row <= end);
                for row in &indexes[first..last] {
                    keep[(*row - start) as usize] = false;
                }
            }
            Self::Bitmap { first, bits } => {
                let lower = start.max(*first);
                let upper = end.min(first + (bits.len() as u64 - 1));
                if lower <= upper {
                    let values = bits.slice((lower - first) as usize, (upper - lower + 1) as usize);
                    let offset = (lower - start) as usize;
                    if values.count_set_bits() == values.len() {
                        keep[offset..offset + values.len()].fill(false);
                    } else {
                        for index in values.set_indices() {
                            keep[offset + index] = false;
                        }
                    }
                }
            }
        }
    }

    fn mask_original(&self, rows: &[u64], sorted: bool) -> Vec<bool> {
        if !sorted {
            return rows.iter().map(|row| !self.contains(*row)).collect();
        }
        let mut keep = vec![true; rows.len()];
        let Some(&first) = rows.first() else {
            return keep;
        };
        match self {
            Self::Indexes(indexes) => {
                let mut cursor = indexes.partition_point(|row| *row < first);
                for (row, keep) in rows.iter().zip(&mut keep) {
                    while indexes.get(cursor).is_some_and(|deleted| deleted < row) {
                        cursor += 1;
                    }
                    *keep = indexes.get(cursor) != Some(row);
                }
            }
            Self::Bitmap { .. } => {
                for (row, keep) in rows.iter().zip(&mut keep) {
                    *keep = !self.contains(*row);
                }
            }
        }
        keep
    }
}

impl Default for DeletionVectorMetadata {
    fn default() -> Self {
        Self::from_kernel(None)
    }
}

#[allow(dead_code)]
impl DeletionVectorMetadata {
    pub(crate) fn is_present(&self) -> bool {
        self.handle.is_some()
    }

    pub(crate) fn from_kernel(handle: Option<KernelDeletionVectorHandle>) -> Self {
        Self {
            handle,
            cached_rows: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
}

#[allow(dead_code)]
pub(crate) async fn load_deletion_vector_masker(
    engine_context: &Arc<DeltaKernelEngineContext>,
    metadata: DeletionVectorMetadata,
    metrics: &DeltaScanMetrics,
) -> Result<Option<DeletionVectorMasker>, DeltaReaderError> {
    let Some(handle) = metadata.handle.clone() else {
        return Ok(None);
    };
    // Serialize the first load across range tasks. Keeping only a weak entry
    // avoids retaining a potentially large payload after the last reader exits.
    let mut cached_rows = metadata.cached_rows.lock().await;
    let rows = match cached_rows.as_ref().and_then(Weak::upgrade) {
        Some(rows) => rows,
        None => {
            let engine_context = Arc::clone(engine_context);
            let metrics_for_task = metrics.clone();
            let rows = match tokio::task::spawn_blocking(move || {
                load_deletion_vector_rows(engine_context.as_ref(), &handle, &metrics_for_task)
            })
            .await
            {
                Ok(result) => result,
                Err(source) => {
                    metrics.record_deletion_vector_failure();
                    Err(dependency_error("deletion_vector_load_task_failed", source))
                }
            }?;
            *cached_rows = Some(Arc::downgrade(&rows));
            rows
        }
    };
    Ok(Some(DeletionVectorMasker::from_shared(
        rows,
        metrics.clone(),
    )))
}

#[allow(dead_code)]
pub(crate) fn load_deletion_vector_masker_blocking(
    engine_context: &DeltaKernelEngineContext,
    metadata: DeletionVectorMetadata,
    metrics: &DeltaScanMetrics,
) -> Result<Option<DeletionVectorMasker>, DeltaReaderError> {
    let Some(handle) = metadata.handle else {
        return Ok(None);
    };
    let row_indexes = load_deletion_vector_rows(engine_context, &handle, metrics)?;
    Ok(Some(DeletionVectorMasker::from_shared(
        row_indexes,
        metrics.clone(),
    )))
}

fn load_deletion_vector_rows(
    engine_context: &DeltaKernelEngineContext,
    handle: &KernelDeletionVectorHandle,
    metrics: &DeltaScanMetrics,
) -> Result<Arc<DeletionVectorRows>, DeltaReaderError> {
    let row_indexes = match engine_context.load_deletion_vector_rows(handle) {
        Ok(row_indexes) => row_indexes,
        Err(source) => {
            metrics.record_deletion_vector_failure();
            return Err(dependency_error(
                "deletion_vector_payload_read_failed",
                source,
            ));
        }
    };

    metrics.record_deletion_vector_payload_loaded();
    Ok(Arc::new(DeletionVectorRows::from_bitmap(row_indexes)))
}

#[allow(dead_code)]
pub(crate) struct DeletionVectorMasker {
    deleted_rows: Arc<DeletionVectorRows>,
    consumed_row_count: u64,
    access_mode: DeletionVectorAccessMode,
    metrics: DeltaScanMetrics,
    applied: bool,
    closed: bool,
}

#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum DeletionVectorAccessMode {
    Unused,
    Ordered,
    OriginalRowIndex,
}

#[allow(dead_code)]
impl DeletionVectorMasker {
    #[cfg(test)]
    pub(crate) fn try_new(
        deleted_row_indexes: Vec<u64>,
        metrics: DeltaScanMetrics,
    ) -> Result<Self, DeltaReaderError> {
        Ok(Self::from_shared(
            Arc::new(DeletionVectorRows::new(deleted_row_indexes)),
            metrics,
        ))
    }

    fn from_shared(deleted_rows: Arc<DeletionVectorRows>, metrics: DeltaScanMetrics) -> Self {
        Self {
            deleted_rows,
            consumed_row_count: 0,
            access_mode: DeletionVectorAccessMode::Unused,
            metrics,
            applied: false,
            closed: false,
        }
    }

    pub(crate) fn mask_ordered_batch(
        &mut self,
        batch: RecordBatch,
    ) -> Result<RecordBatch, DeltaReaderError> {
        let keep_mask = self.consume_ordered_batch(batch.num_rows())?;
        self.apply_keep_mask(batch, keep_mask)
    }

    pub(crate) fn mask_original_row_indexes(
        &mut self,
        batch: RecordBatch,
        row_indexes: Option<&Int64Array>,
    ) -> Result<RecordBatch, DeltaReaderError> {
        let Some(row_indexes) = row_indexes else {
            return self.reject(
                "invalid_deletion_vector_coordinates",
                "original row indexes are missing",
            );
        };
        if row_indexes.len() != batch.num_rows() {
            return self.reject(
                "invalid_deletion_vector_coordinates",
                "original row-index count does not match the batch row count",
            );
        }
        let keep_mask = self.select_original_row_indexes(row_indexes)?;
        self.apply_keep_mask(batch, keep_mask)
    }

    fn consume_ordered_batch(&mut self, batch_len: usize) -> Result<Vec<bool>, DeltaReaderError> {
        self.require_open()?;
        self.select_mode(DeletionVectorAccessMode::Ordered)?;
        let batch_len = match u64::try_from(batch_len) {
            Ok(batch_len) => batch_len,
            Err(_) => {
                return self.reject(
                    "invalid_deletion_vector_coordinates",
                    "batch length does not fit the deletion-vector coordinate type",
                );
            }
        };
        let Some(requested_end) = self.consumed_row_count.checked_add(batch_len) else {
            return self.reject(
                "invalid_deletion_vector_coordinates",
                "ordered deletion-vector coordinate overflow",
            );
        };
        let batch_len = match usize::try_from(batch_len) {
            Ok(batch_len) => batch_len,
            Err(_) => {
                return self.reject(
                    "invalid_deletion_vector_coordinates",
                    "batch length does not fit the host index type",
                );
            }
        };
        let mut keep_mask = vec![true; batch_len];
        self.deleted_rows
            .mask_ordered(self.consumed_row_count, &mut keep_mask);
        self.consumed_row_count = requested_end;

        Ok(keep_mask)
    }

    fn select_original_row_indexes(
        &mut self,
        row_indexes: &Int64Array,
    ) -> Result<Vec<bool>, DeltaReaderError> {
        self.require_open()?;
        let mut validated_row_indexes = Vec::with_capacity(row_indexes.len());
        let mut sorted = true;
        for index in 0..row_indexes.len() {
            if row_indexes.is_null(index) {
                return self.reject(
                    "invalid_deletion_vector_coordinates",
                    "original row index is missing",
                );
            }
            let Ok(row_index) = u64::try_from(row_indexes.value(index)) else {
                return self.reject(
                    "invalid_deletion_vector_coordinates",
                    "original row index is negative",
                );
            };
            sorted &= validated_row_indexes
                .last()
                .is_none_or(|last| *last <= row_index);
            validated_row_indexes.push(row_index);
        }
        self.select_mode(DeletionVectorAccessMode::OriginalRowIndex)?;

        Ok(self
            .deleted_rows
            .mask_original(&validated_row_indexes, sorted))
    }

    fn apply_keep_mask(
        &mut self,
        batch: RecordBatch,
        keep_mask: Vec<bool>,
    ) -> Result<RecordBatch, DeltaReaderError> {
        if !self.applied {
            self.metrics.record_deletion_vector_applied();
            self.applied = true;
        }
        let deleted_rows = keep_mask.iter().filter(|keep| !**keep).count();
        let result = if deleted_rows == 0 {
            Ok(batch)
        } else if deleted_rows == batch.num_rows() {
            Ok(RecordBatch::new_empty(batch.schema()))
        } else {
            filter_record_batch(&batch, &BooleanArray::from(keep_mask))
                .boxed()
                .context(DeletionVectorReadSnafu {
                    reason: "deletion_vector_masking_failed",
                })
        };

        match result {
            Ok(batch) => {
                self.metrics
                    .record_deletion_vector_rows_deleted(deleted_rows);
                Ok(batch)
            }
            Err(error) => {
                self.metrics.record_deletion_vector_failure();
                Err(error)
            }
        }
    }

    pub(crate) fn finish(&mut self) -> Result<(), DeltaReaderError> {
        self.require_open()?;
        self.closed = true;

        if self.access_mode == DeletionVectorAccessMode::OriginalRowIndex {
            return Ok(());
        }

        if self
            .deleted_rows
            .max()
            .is_some_and(|row| row >= self.consumed_row_count)
        {
            return self.reject(
                "invalid_deletion_vector_coordinates",
                "deletion-vector entries remain after physical file completion",
            );
        }

        Ok(())
    }

    pub(crate) fn finish_original_row_indexes(&mut self) -> Result<(), DeltaReaderError> {
        self.select_mode(DeletionVectorAccessMode::OriginalRowIndex)?;
        self.finish()
    }

    fn require_open(&self) -> Result<(), DeltaReaderError> {
        if self.closed {
            self.reject(
                "invalid_deletion_vector_coordinates",
                "deletion-vector masker is already closed",
            )
        } else {
            Ok(())
        }
    }

    fn select_mode(&mut self, mode: DeletionVectorAccessMode) -> Result<(), DeltaReaderError> {
        match self.access_mode {
            DeletionVectorAccessMode::Unused => {
                self.access_mode = mode;
                Ok(())
            }
            current if current == mode => Ok(()),
            _ => self.reject(
                "invalid_deletion_vector_coordinates",
                "deletion-vector coordinate modes cannot be mixed",
            ),
        }
    }

    fn reject<T>(&self, reason: &'static str, detail: &'static str) -> Result<T, DeltaReaderError> {
        self.metrics.record_deletion_vector_coordinate_rejection();
        Err(rejection_error(reason, detail))
    }
}

#[allow(dead_code)]
fn rejection_error(reason: &'static str, detail: &'static str) -> DeltaReaderError {
    dependency_error(reason, delta_kernel::Error::generic(detail))
}

fn dependency_error(
    reason: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> DeltaReaderError {
    DeletionVectorReadSnafu { reason }.into_error(Box::new(source))
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as _,
        fs,
        path::{Path, PathBuf},
        sync::{Arc, Weak},
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow::{
        array::{ArrayRef, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use delta_kernel::actions::deletion_vector::{
        DeletionVectorDescriptor, DeletionVectorStorageType,
    };
    use delta_kernel::actions::deletion_vector_writer::{
        KernelDeletionVector, StreamingDeletionVectorWriter,
    };

    use super::{
        DeletionVectorMasker, DeletionVectorMetadata, DeletionVectorRows,
        load_deletion_vector_masker,
    };
    use crate::{
        DeltaReaderError, DeltaReaderPhase, DeltaScanMetrics, DeltaSnapshotSelection,
        DeltaStorageOptions, ParquetReaderBackend,
        delta::{
            kernel::{KernelDeletionVectorHandle, is_kernel_error},
            snapshot::{ArrowTableSnapshot, load_delta_table_snapshot_blocking},
        },
        reader::metrics::DeltaScanMetricsConfig,
    };

    const INLINE_DV_DELETED_ROW_INDEXES: &[u64] = &[3, 4, 7, 11, 18, 29];
    const RELATIVE_DV_ID: &str = "vBn[lx{q8@P<9BNH/isA";
    const RELATIVE_DV_FILE: &str = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";
    const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
    const METADATA_JSON: &str = r#"{"metaData":{"id":"delta-arrow-reader-dv-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;

    struct DeltaLogTable(PathBuf);

    impl DeltaLogTable {
        fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = Path::new("target")
                .join("delta-arrow-reader-deletion-vector-tests")
                .join(unique_name(name)?);
            let log_path = path.join("_delta_log");
            fs::create_dir_all(&log_path)?;
            fs::write(
                log_path.join("00000000000000000000.json"),
                format!("{PROTOCOL_JSON}\n{METADATA_JSON}\n"),
            )?;
            Ok(Self(path))
        }

        fn snapshot(&self) -> Result<ArrowTableSnapshot, DeltaReaderError> {
            load_delta_table_snapshot_blocking(
                &self.0.to_string_lossy(),
                &DeltaStorageOptions::new(),
                DeltaSnapshotSelection::Latest,
            )
        }
    }

    impl Drop for DeltaLogTable {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn unique_name(name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        Ok(format!("{}-{name}-{nanos}", std::process::id()))
    }

    fn metadata(descriptor: DeletionVectorDescriptor) -> DeletionVectorMetadata {
        DeletionVectorMetadata::from_kernel(Some(KernelDeletionVectorHandle(descriptor)))
    }

    fn inline_metadata() -> Result<DeletionVectorMetadata, delta_kernel::Error> {
        DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::Inline,
            "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
            None,
            44,
            6,
        )
        .map(metadata)
    }

    fn write_relative_metadata(
        table: &DeltaLogTable,
        deleted_rows: impl IntoIterator<Item = u64>,
    ) -> Result<DeletionVectorMetadata, Box<dyn std::error::Error>> {
        let mut buffer = Vec::new();
        let mut writer = StreamingDeletionVectorWriter::new(&mut buffer);
        let mut deletion_vector = KernelDeletionVector::new();
        deletion_vector.add_deleted_row_indexes(deleted_rows);
        let write_result = writer.write_deletion_vector(deletion_vector)?;
        writer.finalize()?;
        fs::write(table.0.join(RELATIVE_DV_FILE), buffer)?;

        Ok(metadata(DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::PersistedRelative,
            RELATIVE_DV_ID,
            Some(write_result.offset),
            write_result.size_in_bytes,
            write_result.cardinality,
        )?))
    }

    fn missing_relative_metadata() -> Result<DeletionVectorMetadata, delta_kernel::Error> {
        DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::PersistedRelative,
            RELATIVE_DV_ID,
            Some(1),
            36,
            2,
        )
        .map(metadata)
    }

    #[test]
    fn regression_weak_cache_drops_the_payload_owner() {
        let rows = Arc::new(DeletionVectorRows::new(vec![3, 1, 3]));
        let cached_rows = Arc::downgrade(&rows);

        assert_eq!(rows.all_indexes(), [1, 3]);
        drop(rows);

        assert!(cached_rows.upgrade().is_none());
    }

    #[tokio::test]
    async fn lazy_loader_skips_absence_and_shares_inline_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("inline")?;
        let snapshot = table.snapshot()?;
        let engine_context = Arc::clone(snapshot.engine_context());

        let absent_metrics = metrics();
        let absent_metadata = DeletionVectorMetadata::default();
        let absent = load_deletion_vector_masker(
            snapshot.engine_context(),
            absent_metadata,
            &absent_metrics,
        )
        .await?;
        assert!(absent.is_none());
        assert_eq!(absent_metrics.snapshot().deletion_vector_payloads_loaded, 0);

        let inline_metrics = metrics();
        let inline_metadata = inline_metadata()?;
        assert!(inline_metadata.is_present());
        let cached_rows = Arc::clone(&inline_metadata.cached_rows);
        let second_metadata = inline_metadata.clone();
        let (masker, second_masker) = tokio::join!(
            load_deletion_vector_masker(
                snapshot.engine_context(),
                inline_metadata.clone(),
                &inline_metrics
            ),
            load_deletion_vector_masker(
                snapshot.engine_context(),
                second_metadata,
                &inline_metrics
            )
        );
        let masker = masker?.expect("inline descriptor must produce a masker");
        let second_masker = second_masker?.expect("inline descriptor must produce a second masker");
        for masker in [&masker, &second_masker] {
            assert_eq!(
                masker.deleted_rows.all_indexes(),
                INLINE_DV_DELETED_ROW_INDEXES
            );
        }
        assert!(Arc::ptr_eq(
            &masker.deleted_rows,
            &second_masker.deleted_rows
        ));
        assert!(
            cached_rows
                .lock()
                .await
                .as_ref()
                .and_then(Weak::upgrade)
                .is_some()
        );
        drop(masker);
        drop(second_masker);
        assert!(
            cached_rows
                .lock()
                .await
                .as_ref()
                .and_then(Weak::upgrade)
                .is_none()
        );

        let reloaded = load_deletion_vector_masker(
            snapshot.engine_context(),
            inline_metadata,
            &inline_metrics,
        )
        .await?
        .expect("released inline payload must reload");
        assert_eq!(
            reloaded.deleted_rows.all_indexes(),
            INLINE_DV_DELETED_ROW_INDEXES
        );
        let metrics = inline_metrics.snapshot();
        assert_eq!(metrics.deletion_vector_payloads_loaded, 2);
        assert_eq!(metrics.deletion_vector_failures, 0);
        assert_eq!(metrics.deletion_vector_coordinate_rejections, 0);
        assert!(Arc::ptr_eq(&engine_context, snapshot.engine_context()));
        Ok(())
    }

    #[test]
    fn lazy_loader_reads_relative_and_empty_kernel_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("relative")?;
        let snapshot = table.snapshot()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let relative_metrics = metrics();
        let relative_metadata = write_relative_metadata(&table, [0, 9])?;
        let masker = runtime
            .block_on(load_deletion_vector_masker(
                snapshot.engine_context(),
                relative_metadata,
                &relative_metrics,
            ))?
            .expect("relative descriptor must produce a masker");
        assert_eq!(masker.deleted_rows.all_indexes(), [0, 9]);
        assert_eq!(
            relative_metrics.snapshot().deletion_vector_payloads_loaded,
            1
        );

        let empty_metrics = metrics();
        let empty_metadata = write_relative_metadata(&table, [])?;
        let mut empty = runtime
            .block_on(load_deletion_vector_masker(
                snapshot.engine_context(),
                empty_metadata,
                &empty_metrics,
            ))?
            .expect("empty present descriptor must produce a masker");
        assert!(empty.deleted_rows.max().is_none());
        empty.finish()?;
        let metrics = empty_metrics.snapshot();
        assert_eq!(metrics.deletion_vector_payloads_loaded, 1);
        assert_eq!(metrics.deletion_vectors_applied, 0);
        Ok(())
    }

    #[test]
    fn lazy_loader_maps_payload_failures_once_and_redacts_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let table = DeltaLogTable::new("secret-token")?;
        let snapshot = table.snapshot()?;
        let missing = missing_relative_metadata()?;
        let metrics = metrics();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let error = runtime
            .block_on(load_deletion_vector_masker(
                snapshot.engine_context(),
                missing,
                &metrics,
            ))
            .err()
            .expect("missing payload must fail");

        assert_eq!(error.code(), "deletion_vector_read");
        assert_eq!(error.phase(), DeltaReaderPhase::DeletionVector);
        assert!(error.source().is_some_and(is_kernel_error));
        assert_eq!(
            error.to_string(),
            "delta reader error: phase=deletion_vector code=deletion_vector_read reason=deletion_vector_payload_read_failed"
        );
        let debug = format!("{error:?}");
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains(RELATIVE_DV_ID));
        let metrics = metrics.snapshot();
        assert_eq!(metrics.deletion_vector_payloads_loaded, 0);
        assert_eq!(metrics.deletion_vector_failures, 1);
        assert_eq!(metrics.deletion_vector_coordinate_rejections, 0);
        Ok(())
    }

    #[test]
    fn lazy_loader_leaves_malformed_and_truncated_payloads_to_kernel()
    -> Result<(), Box<dyn std::error::Error>> {
        const MALFORMED_INLINE: &str = "not-valid-inline-payload";

        let table = DeltaLogTable::new("hostile-payload")?;
        fs::write(table.0.join(RELATIVE_DV_FILE), [0_u8, 1, 2])?;
        let snapshot = table.snapshot()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let malformed = metadata(DeletionVectorDescriptor::try_new(
            DeletionVectorStorageType::Inline,
            MALFORMED_INLINE,
            None,
            4,
            1,
        )?);
        let truncated = missing_relative_metadata()?;

        for metadata in [malformed, truncated] {
            let metrics = metrics();
            let error = runtime
                .block_on(load_deletion_vector_masker(
                    snapshot.engine_context(),
                    metadata,
                    &metrics,
                ))
                .err()
                .expect("invalid Kernel payload must fail");
            assert_eq!(error.code(), "deletion_vector_read");
            assert!(error.source().is_some_and(is_kernel_error));
            assert!(!error.to_string().contains(MALFORMED_INLINE));
            assert!(!format!("{error:?}").contains(RELATIVE_DV_ID));
            let metrics = metrics.snapshot();
            assert_eq!(metrics.deletion_vector_payloads_loaded, 0);
            assert_eq!(metrics.deletion_vector_failures, 1);
            assert_eq!(metrics.deletion_vector_coordinate_rejections, 0);
            assert_eq!(metrics.parquet_data_file_range_get_operations, Some(0));
            assert_eq!(metrics.parquet_data_file_full_get_operations, Some(0));
        }
        Ok(())
    }

    fn metrics() -> DeltaScanMetrics {
        DeltaScanMetrics::new(DeltaScanMetricsConfig {
            snapshot_version: 7,
            parquet_backend: ParquetReaderBackend::Direct,
            scan_partitions_planned: 1,
            files_planned: 1,
            add_actions_excluded_during_planning: Some(0),
            estimated_input_rows: None,
            estimated_input_bytes: None,
        })
    }

    fn masker(deleted_row_indexes: Vec<u64>) -> Result<DeletionVectorMasker, DeltaReaderError> {
        DeletionVectorMasker::try_new(deleted_row_indexes, metrics())
    }

    fn batch(ids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let labels = ids.iter().map(|id| format!("row-{id}")).collect::<Vec<_>>();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(labels)) as ArrayRef,
            ],
        )
        .expect("valid test batch")
    }

    fn ids(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 id column")
            .values()
            .to_vec()
    }

    fn row_indexes(values: &[i64]) -> Int64Array {
        Int64Array::from(values.to_vec())
    }

    #[test]
    fn deleted_row_indexes_are_sorted_deduplicated_and_inverted() -> Result<(), DeltaReaderError> {
        let metrics = metrics();
        let mut masker = DeletionVectorMasker::try_new(vec![3, 1, 3], metrics.clone())?;

        assert_eq!(masker.deleted_rows.all_indexes(), [1, 3]);
        assert_eq!(
            masker.select_original_row_indexes(&row_indexes(&[0, 1, 2, 3, 4]))?,
            [true, false, true, false, true]
        );
        masker.finish()?;
        assert_eq!(metrics.snapshot().deletion_vector_coordinate_rejections, 0);
        Ok(())
    }

    #[test]
    fn ordered_mode_tracks_physical_rows_across_batches() -> Result<(), DeltaReaderError> {
        let mut masker = masker(vec![1, 4])?;

        assert_eq!(masker.consume_ordered_batch(2)?, [true, false]);
        assert_eq!(masker.consume_ordered_batch(0)?, Vec::<bool>::new());
        assert_eq!(masker.consume_ordered_batch(4)?, [true, true, false, true]);
        masker.finish()?;
        Ok(())
    }

    #[test]
    fn ordered_mode_handles_none_and_all_deleted() -> Result<(), DeltaReaderError> {
        let mut none = masker(Vec::new())?;
        let mut all = masker(vec![0, 1, 2])?;

        assert_eq!(none.consume_ordered_batch(3)?, [true; 3]);
        assert_eq!(all.consume_ordered_batch(3)?, [false; 3]);
        none.finish()?;
        all.finish()?;
        Ok(())
    }

    #[test]
    fn ordered_mode_pads_live_tail_and_rejects_unconsumed_entries_and_use_after_finish()
    -> Result<(), DeltaReaderError> {
        let mut padded = masker(vec![1])?;
        assert_eq!(
            padded.consume_ordered_batch(5)?,
            [true, false, true, true, true]
        );
        padded.finish()?;

        let mut overflow = masker(Vec::new())?;
        overflow.consumed_row_count = u64::MAX;
        assert!(overflow.consume_ordered_batch(1).is_err());

        let mut underrun = masker(vec![2])?;
        underrun.consume_ordered_batch(2)?;
        assert!(underrun.finish().is_err());
        assert!(underrun.consume_ordered_batch(1).is_err());
        Ok(())
    }

    #[test]
    fn original_index_mode_handles_sparse_monotonic_batches() -> Result<(), DeltaReaderError> {
        let mut masker = masker(vec![1, 4, 7])?;

        assert_eq!(
            masker.select_original_row_indexes(&row_indexes(&[0, 1, 3]))?,
            [true, false, true]
        );
        assert_eq!(
            masker.select_original_row_indexes(&row_indexes(&[4, 8, 9]))?,
            [false, true, true]
        );
        masker.finish()?;
        Ok(())
    }

    #[test]
    fn original_index_mode_can_finish_without_observing_rows() -> Result<(), DeltaReaderError> {
        let metrics = metrics();
        let mut masker = DeletionVectorMasker::try_new(vec![4], metrics.clone())?;

        masker.finish_original_row_indexes()?;

        assert_eq!(metrics.snapshot().deletion_vector_coordinate_rejections, 0);
        Ok(())
    }

    #[test]
    fn original_index_mode_falls_back_for_unsorted_and_duplicate_rows()
    -> Result<(), DeltaReaderError> {
        let mut masker = masker(vec![1, 3])?;

        assert_eq!(
            masker.select_original_row_indexes(&row_indexes(&[3, 1, 1, 4]))?,
            [false, false, false, true]
        );
        assert_eq!(
            masker.select_original_row_indexes(&row_indexes(&[1, 2]))?,
            [false, true]
        );
        masker.finish()?;
        Ok(())
    }

    #[test]
    fn original_index_mode_rejects_missing_and_negative_indexes() -> Result<(), DeltaReaderError> {
        for indexes in [Int64Array::from(vec![Some(0), None]), row_indexes(&[-1])] {
            let mut masker = masker(vec![1])?;
            assert!(masker.select_original_row_indexes(&indexes).is_err());
        }
        Ok(())
    }

    #[test]
    fn coordinate_modes_cannot_be_mixed() -> Result<(), DeltaReaderError> {
        let mut ordered = masker(vec![1])?;
        ordered.consume_ordered_batch(1)?;
        assert!(
            ordered
                .select_original_row_indexes(&row_indexes(&[1]))
                .is_err()
        );

        let mut original = masker(vec![1])?;
        original.select_original_row_indexes(&row_indexes(&[0]))?;
        assert!(original.consume_ordered_batch(1).is_err());
        Ok(())
    }

    #[test]
    fn ordered_mode_requires_no_upfront_physical_row_count() -> Result<(), DeltaReaderError> {
        let mut masker = DeletionVectorMasker::try_new(Vec::new(), metrics())?;
        assert_eq!(masker.consume_ordered_batch(0)?, Vec::<bool>::new());
        assert_eq!(masker.consume_ordered_batch(3)?, [true; 3]);
        masker.finish()?;
        Ok(())
    }

    #[test]
    fn ordered_masking_preserves_schema_row_order_and_exact_metrics() -> Result<(), DeltaReaderError>
    {
        let metrics = metrics();
        let mut masker = DeletionVectorMasker::try_new(vec![1, 3], metrics.clone())?;
        let first = batch(&[10, 11]);
        let schema = first.schema();

        let first = masker.mask_ordered_batch(first)?;
        let second = masker.mask_ordered_batch(batch(&[12, 13, 14]))?;
        masker.finish()?;

        assert_eq!(ids(&first), [10]);
        assert_eq!(ids(&second), [12, 14]);
        assert_eq!(first.schema(), schema);
        assert_eq!(second.schema(), schema);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.deletion_vectors_applied, 1);
        assert_eq!(snapshot.deletion_vector_rows_deleted, 2);
        assert_eq!(snapshot.deletion_vector_failures, 0);
        assert_eq!(snapshot.deletion_vector_coordinate_rejections, 0);
        Ok(())
    }

    #[test]
    fn dropping_masker_preserves_partial_masking_metrics() -> Result<(), DeltaReaderError> {
        let metrics = metrics();
        let mut masker = DeletionVectorMasker::try_new(vec![1, 3], metrics.clone())?;
        let masked = masker.mask_ordered_batch(batch(&[10, 11]))?;
        assert_eq!(ids(&masked), [10]);
        drop(masker);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.deletion_vectors_applied, 1);
        assert_eq!(snapshot.deletion_vector_rows_deleted, 1);
        assert_eq!(snapshot.deletion_vector_failures, 0);
        assert_eq!(snapshot.deletion_vector_coordinate_rejections, 0);
        Ok(())
    }

    #[test]
    fn masking_handles_none_all_and_sparse_original_rows() -> Result<(), DeltaReaderError> {
        let mut none = masker(Vec::new())?;
        let none_batch = none.mask_ordered_batch(batch(&[10, 11, 12]))?;
        none.finish()?;
        assert_eq!(ids(&none_batch), [10, 11, 12]);

        let mut all = masker(vec![0, 1, 2])?;
        let all_batch = all.mask_ordered_batch(batch(&[10, 11, 12]))?;
        all.finish()?;
        assert_eq!(all_batch.num_rows(), 0);
        assert_eq!(all_batch.schema().field(0).name(), "id");
        assert_eq!(all_batch.schema().field(1).name(), "label");

        let mut sparse = masker(vec![2])?;
        let sparse_batch = sparse
            .mask_original_row_indexes(batch(&[10, 12, 14]), Some(&row_indexes(&[0, 2, 4])))?;
        sparse.finish()?;
        assert_eq!(ids(&sparse_batch), [10, 14]);
        Ok(())
    }

    #[test]
    fn coordinate_rejections_increment_only_rejection_metrics() -> Result<(), DeltaReaderError> {
        let mismatch_metrics = metrics();
        let mut masker = DeletionVectorMasker::try_new(vec![1], mismatch_metrics.clone())?;
        let _ = masker
            .mask_original_row_indexes(batch(&[10, 11]), Some(&row_indexes(&[0])))
            .expect_err("row-index count mismatch must fail");
        let snapshot = mismatch_metrics.snapshot();
        assert_eq!(snapshot.deletion_vectors_applied, 0);
        assert_eq!(snapshot.deletion_vector_failures, 0);
        assert_eq!(snapshot.deletion_vector_coordinate_rejections, 1);

        let missing_metrics = metrics();
        let mut masker = DeletionVectorMasker::try_new(vec![1], missing_metrics.clone())?;
        let _ = masker
            .mask_original_row_indexes(batch(&[10, 11]), None)
            .expect_err("missing row indexes must fail");
        let snapshot = missing_metrics.snapshot();
        assert_eq!(snapshot.deletion_vectors_applied, 0);
        assert_eq!(snapshot.deletion_vector_failures, 0);
        assert_eq!(snapshot.deletion_vector_coordinate_rejections, 1);
        Ok(())
    }

    #[test]
    fn terminal_errors_increment_exactly_one_failure_or_rejection()
    -> Result<(), Box<dyn std::error::Error>> {
        let masker_metrics = metrics();
        let mut masker = DeletionVectorMasker::try_new(vec![2], masker_metrics.clone())?;
        let _ = masker.finish().expect_err("unconsumed masker must fail");

        let coordinate_metrics = metrics();
        let mut masker = DeletionVectorMasker::try_new(vec![1], coordinate_metrics.clone())?;
        let _ = masker
            .mask_original_row_indexes(batch(&[10]), None)
            .expect_err("missing coordinates must fail");

        let payload_metrics = metrics();
        let table = DeltaLogTable::new("terminal-classification")?;
        let snapshot = table.snapshot()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let _ = runtime
            .block_on(load_deletion_vector_masker(
                snapshot.engine_context(),
                missing_relative_metadata()?,
                &payload_metrics,
            ))
            .err()
            .expect("missing payload must fail");

        for (name, snapshot, failures, rejections) in [
            ("masker", masker_metrics.snapshot(), 0, 1),
            ("coordinate", coordinate_metrics.snapshot(), 0, 1),
            ("payload", payload_metrics.snapshot(), 1, 0),
        ] {
            assert_eq!(snapshot.deletion_vector_failures, failures, "{name}");
            assert_eq!(
                snapshot.deletion_vector_coordinate_rejections, rejections,
                "{name}"
            );
            assert_eq!(failures + rejections, 1, "{name}");
        }
        Ok(())
    }

    #[test]
    fn compact_coordinates_bound_memory_and_handle_the_full_u64_range()
    -> Result<(), Box<dyn std::error::Error>> {
        for first in [0, 1_u64 << 32, u64::MAX - 99_999] {
            let rows = DeletionVectorRows::new((first..=first + 99_999).collect());
            let DeletionVectorRows::Bitmap { bits, .. } = &rows else {
                return Err("dense coordinates should use a compact bitmap".into());
            };
            assert_eq!(bits.values().len(), 12_500);
            assert_eq!(rows.max(), Some(first + 99_999));
            assert!(rows.contains(first));
            assert!(rows.contains(first + 99_999));
            let mut ordered = DeletionVectorMasker::from_shared(Arc::new(rows), metrics());
            ordered.consumed_row_count = first;
            assert_eq!(ordered.consume_ordered_batch(127)?, vec![false; 127]);
            assert!(ordered.finish().is_err());
        }
        for deleted in [vec![], vec![0, u64::MAX], vec![0, 1_u64 << 40, 2_u64 << 40]] {
            let rows = DeletionVectorRows::new(deleted.clone());
            assert!(matches!(rows, DeletionVectorRows::Indexes(_)));
            assert_eq!(rows.all_indexes(), deleted);
        }
        Ok(())
    }

    #[test]
    fn coordinates_match_membership_across_bitmap_boundaries() -> Result<(), DeltaReaderError> {
        let boundary = 1_u64 << 32;
        let cases = [
            Vec::new(),
            vec![
                0,
                65_535,
                65_536,
                boundary - 1,
                boundary,
                boundary + 1,
                3 * boundary + 2,
                i64::MAX as u64,
            ],
            (65_520..65_570).collect(),
            (boundary - 20..boundary + 20).collect(),
            vec![boundary, 3 * boundary],
            (0..4096).step_by(2).collect(),
            (0..4096).map(|row| i64::MAX as u64 - 4095 + row).collect(),
            (boundary..boundary + 512)
                .chain(3 * boundary..3 * boundary + 512)
                .collect(),
        ];
        for deleted in cases {
            let mut original = masker(deleted.clone())?;
            for indexes in [
                vec![0, 1, 65_534, 65_535, 65_536, 65_537],
                vec![],
                vec![
                    boundary - 1,
                    boundary,
                    boundary + 1,
                    3 * boundary,
                    3 * boundary + 2,
                ],
                vec![i64::MAX as u64, boundary, 0, boundary, 65_535],
            ] {
                let expected: Vec<_> = indexes.iter().map(|row| !deleted.contains(row)).collect();
                let indexes = Int64Array::from(
                    indexes
                        .into_iter()
                        .map(|row| row as i64)
                        .collect::<Vec<_>>(),
                );
                assert_eq!(
                    original.select_original_row_indexes(&indexes)?,
                    expected,
                    "{deleted:?}"
                );
            }
            original.finish()?;
            for start in [
                0,
                65_520,
                boundary - 20,
                boundary,
                2 * boundary + 1,
                3 * boundary,
            ] {
                let mut ordered = masker(deleted.clone())?;
                ordered.consumed_row_count = start;
                for len in [0, 1, 19, 20] {
                    let start = ordered.consumed_row_count;
                    let expected: Vec<_> = (start..start + len)
                        .map(|row| !deleted.contains(&row))
                        .collect();
                    assert_eq!(
                        ordered.consume_ordered_batch(len as usize)?,
                        expected,
                        "{deleted:?} at {start}"
                    );
                }
                assert_eq!(
                    ordered.finish().is_err(),
                    deleted.iter().any(|row| *row >= ordered.consumed_row_count)
                );
            }
        }
        Ok(())
    }

    #[test]
    fn production_boundary_reuses_kernel_context_without_an_extra_decoder_or_runtime() {
        let deletion_vector_source = include_str!("deletion_vector.rs")
            .split_once("mod tests {")
            .expect("test module boundary")
            .0;
        let kernel_source = include_str!("../delta/kernel.rs");

        for forbidden in [
            "DefaultEngineBuilder",
            "store_from_url_opts",
            "Runtime::",
            "z85",
            "datafusion",
            "tracing::",
        ] {
            assert!(!deletion_vector_source.contains(forbidden), "{forbidden}");
        }
        assert_eq!(
            kernel_source.matches("DefaultEngineBuilder::new").count(),
            1
        );
        assert_eq!(kernel_source.matches("store_from_url_opts(").count(), 1);
        assert_eq!(kernel_source.matches("get_row_indexes(").count(), 0);
    }
}
