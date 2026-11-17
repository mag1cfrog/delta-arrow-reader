//! Observe real Delta provider requests and physical local-store reads.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    ops::Range,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    catalog::Session,
    common::Result as DataFusionResult,
    datasource::{TableProvider, TableType},
    logical_expr::{Expr, TableProviderFilterPushDown},
    object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
        local::LocalFileSystem, path::Path as StorePath,
    },
    physical_plan::ExecutionPlan,
};
use delta_arrow_reader::datafusion::DeltaTableProvider;
use futures_util::{
    TryStreamExt,
    stream::{self, BoxStream},
};
use tokio::sync::Semaphore;

use parquet::file::{
    reader::FileReader,
    serialized_reader::{ReadOptionsBuilder, SerializedFileReader},
};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone)]
pub(super) struct ScanRequest {
    pub session_target_partitions: usize,
    pub projected_columns: BTreeSet<String>,
    pub filters: Vec<Expr>,
}

#[derive(Debug, Default, Clone)]
pub(super) struct ProviderCalls {
    pub offered_filters: Vec<Expr>,
    pub scans: Vec<ScanRequest>,
}

pub(super) struct RecordingDeltaProvider {
    pub inner: DeltaTableProvider,
    pub calls: Arc<Mutex<ProviderCalls>>,
    pub schema_calls: Arc<AtomicUsize>,
}

impl fmt::Debug for RecordingDeltaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordingDeltaProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for RecordingDeltaProvider {
    fn schema(&self) -> SchemaRef {
        self.schema_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.schema()
    }
    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        self.calls
            .lock()
            .map_err(|_| {
                datafusion::common::DataFusionError::Internal("provider call lock poisoned".into())
            })?
            .offered_filters
            .extend(filters.iter().map(|expr| (*expr).clone()));
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        let columns = projection
            .cloned()
            .unwrap_or_else(|| (0..schema.fields().len()).collect())
            .into_iter()
            .map(|index| schema.field(index).name().clone())
            .collect();
        self.calls
            .lock()
            .map_err(|_| {
                datafusion::common::DataFusionError::Internal("provider call lock poisoned".into())
            })?
            .scans
            .push(ScanRequest {
                session_target_partitions: state.config_options().execution.target_partitions,
                projected_columns: columns,
                filters: filters.to_vec(),
            });
        self.inner.scan(state, projection, filters, limit).await
    }
}

#[derive(Debug, Clone)]
pub(super) struct ReadRange {
    pub path: String,
    pub range: Range<u64>,
}

/// Selects the real Parquet requests to block or fail in lifecycle tests.
#[derive(Debug)]
pub(super) enum ParquetReadAction {
    Observe,
    GateAll,
    GateFile(String),
    FailFileOnce(String),
}

#[derive(Debug, Default, Clone)]
pub(super) struct ParquetReadState {
    pub requests_started: usize,
    pub requests_finished: usize,
    pub requests_cancelled: usize,
    pub active_requests: usize,
    pub peak_active_files: usize,
    pub files_started: BTreeSet<String>,
    active_files: BTreeMap<String, usize>,
}

/// Acknowledges request start/finish and holds selected reads until released.
#[derive(Debug)]
pub(super) struct ParquetReadControl {
    action: ParquetReadAction,
    state: Mutex<ParquetReadState>,
    pub request_started: Semaphore,
    pub request_finished: Semaphore,
    release: Semaphore,
    failure_fired: AtomicBool,
    pub store_dropped: Semaphore,
}

impl ParquetReadControl {
    pub fn new(action: ParquetReadAction) -> Arc<Self> {
        Arc::new(Self {
            action,
            state: Mutex::new(ParquetReadState::default()),
            request_started: Semaphore::new(0),
            request_finished: Semaphore::new(0),
            release: Semaphore::new(0),
            failure_fired: AtomicBool::new(false),
            store_dropped: Semaphore::new(0),
        })
    }

    pub fn snapshot(&self) -> TestResult<ParquetReadState> {
        Ok(self
            .state
            .lock()
            .map_err(|_| "read control lock poisoned")?
            .clone())
    }

    pub fn release_all(&self) {
        self.release.close();
    }

    async fn begin(self: &Arc<Self>, path: &StorePath) -> StoreResult<ParquetReadGuard> {
        let path = path.to_string();
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| control_error("read control lock poisoned"))?;
            state.requests_started += 1;
            state.active_requests += 1;
            state.files_started.insert(path.clone());
            *state.active_files.entry(path.clone()).or_default() += 1;
            state.peak_active_files = state.peak_active_files.max(state.active_files.len());
        }
        let mut guard = ParquetReadGuard {
            control: Arc::clone(self),
            path,
            finished: false,
        };
        self.request_started.add_permits(1);
        match &self.action {
            ParquetReadAction::FailFileOnce(file)
                if file == &guard.path && !self.failure_fired.swap(true, Ordering::AcqRel) =>
            {
                guard.finished = true;
                return Err(control_error("injected late Parquet read failure"));
            }
            ParquetReadAction::GateAll => {
                let _ = self.release.acquire().await;
            }
            ParquetReadAction::GateFile(file) if file == &guard.path => {
                let _ = self.release.acquire().await;
            }
            _ => {}
        }
        Ok(guard)
    }
}

fn control_error(message: &str) -> datafusion::object_store::Error {
    datafusion::object_store::Error::Generic {
        store: "spark-delta-read-control",
        source: std::io::Error::other(message).into(),
    }
}

struct ParquetReadGuard {
    control: Arc<ParquetReadControl>,
    path: String,
    finished: bool,
}

impl Drop for ParquetReadGuard {
    fn drop(&mut self) {
        // Recover only to signal cleanup; a subsequent snapshot reports lock poisoning.
        let mut state = self
            .control
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.active_requests -= 1;
        if self.finished {
            state.requests_finished += 1;
        } else {
            state.requests_cancelled += 1;
        }
        if let Some(active) = state.active_files.get_mut(&self.path) {
            *active -= 1;
            if *active == 0 {
                state.active_files.remove(&self.path);
            }
        }
        self.control.request_finished.add_permits(1);
    }
}

#[derive(Debug)]
pub(super) struct RecordingLocalStore {
    inner: LocalFileSystem,
    pub reads: Mutex<Vec<ReadRange>>,
    pub read_calls: AtomicUsize,
    pub write_calls: AtomicUsize,
    control: Option<Arc<ParquetReadControl>>,
}

impl RecordingLocalStore {
    pub fn register(root: &Path) -> TestResult<(String, Arc<Self>)> {
        Self::register_with_control(root, None)
    }

    pub fn register_with_control(
        root: &Path,
        control: Option<Arc<ParquetReadControl>>,
    ) -> TestResult<(String, Arc<Self>)> {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let scheme = format!(
            "sparkdelta{}x{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let store = Arc::new(Self {
            inner: LocalFileSystem::new_with_prefix(root)?,
            reads: Mutex::new(vec![]),
            read_calls: AtomicUsize::new(0),
            write_calls: AtomicUsize::new(0),
            control,
        });
        let weak = Arc::downgrade(&store);
        delta_kernel_default_engine::storage::insert_url_handler(
            &scheme,
            Arc::new(move |_, _| {
                let store =
                    weak.upgrade()
                        .ok_or_else(|| datafusion::object_store::Error::Generic {
                            store: "spark-delta-read-store",
                            source: std::io::Error::other("store was released").into(),
                        })?;
                Ok((Box::new(store), StorePath::from("")))
            }),
        )?;
        Ok((format!("{scheme}://table/"), store))
    }

    async fn begin_read(&self, path: &StorePath) -> StoreResult<Option<ParquetReadGuard>> {
        match &self.control {
            Some(control) if path.as_ref().ends_with(".parquet") => {
                control.begin(path).await.map(Some)
            }
            _ => Ok(None),
        }
    }

    fn record(&self, path: &StorePath, range: Range<u64>) -> StoreResult<()> {
        if path.as_ref().ends_with(".parquet") {
            self.reads
                .lock()
                .map_err(|_| datafusion::object_store::Error::Generic {
                    store: "spark-delta-read-store",
                    source: std::io::Error::other("read log lock poisoned").into(),
                })?
                .push(ReadRange {
                    path: path.to_string(),
                    range,
                });
        }
        Ok(())
    }
}

impl Drop for RecordingLocalStore {
    fn drop(&mut self) {
        if let Some(control) = &self.control {
            control.store_dropped.add_permits(1);
        }
    }
}

impl fmt::Display for RecordingLocalStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("spark-delta-read-store")
    }
}

#[async_trait]
impl ObjectStore for RecordingLocalStore {
    async fn put_opts(
        &self,
        path: &StorePath,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.write_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.put_opts(path, payload, options).await
    }
    async fn put_multipart_opts(
        &self,
        path: &StorePath,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.write_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.put_multipart_opts(path, options).await
    }
    async fn get_opts(&self, path: &StorePath, options: GetOptions) -> StoreResult<GetResult> {
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        let head = options.head;
        let mut guard = if head {
            None
        } else {
            self.begin_read(path).await?
        };
        let result = self.inner.get_opts(path, options).await;
        if let Some(guard) = &mut guard {
            guard.finished = true;
        }
        let result = result?;
        if !head {
            self.record(path, result.range.clone())?;
        }
        Ok(result)
    }
    async fn get_ranges(&self, path: &StorePath, ranges: &[Range<u64>]) -> StoreResult<Vec<Bytes>> {
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.begin_read(path).await?;
        let result = self.inner.get_ranges(path, ranges).await;
        if let Some(guard) = &mut guard {
            guard.finished = true;
        }
        let result = result?;
        assert_eq!(result.len(), ranges.len());
        for (range, bytes) in ranges.iter().zip(&result) {
            assert_eq!(range.end - range.start, bytes.len() as u64);
            self.record(path, range.clone())?;
        }
        Ok(result)
    }
    fn delete_stream(
        &self,
        paths: BoxStream<'static, StoreResult<StorePath>>,
    ) -> BoxStream<'static, StoreResult<StorePath>> {
        self.write_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_stream(paths)
    }
    fn list(&self, prefix: Option<&StorePath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        let entries = self.inner.list(prefix);
        Box::pin(
            stream::once(async move {
                let mut entries = entries.try_collect::<Vec<_>>().await?;
                // Kernel assumes custom schemes are ordered; match its sorted file:// listing.
                entries.sort_unstable_by(|left, right| left.location.cmp(&right.location));
                Ok::<_, datafusion::object_store::Error>(stream::iter(entries.into_iter().map(Ok)))
            })
            .try_flatten(),
        )
    }
    async fn list_with_delimiter(&self, prefix: Option<&StorePath>) -> StoreResult<ListResult> {
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> StoreResult<()> {
        self.write_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug, Clone)]
pub(super) struct ColumnChunk {
    pub row_group: usize,
    pub root_column: String,
    pub column_path: Vec<String>,
    pub range: Range<u64>,
    pub pages: Vec<Range<u64>>,
}

#[derive(Debug)]
pub(super) struct ParquetReadLayout {
    pub path: String,
    pub size: u64,
    pub chunks: Vec<ColumnChunk>,
}

impl ParquetReadLayout {
    pub fn open(root: &Path, path: &str) -> TestResult<Self> {
        let file = std::fs::File::open(root.join(path))?;
        let size = file.metadata()?.len();
        let reader = SerializedFileReader::new_with_options(
            file,
            ReadOptionsBuilder::new().with_page_index().build(),
        )?;
        let metadata = reader.metadata();
        let mut chunks = vec![];
        for (group_index, group) in metadata.row_groups().iter().enumerate() {
            for (column_index, column) in group.columns().iter().enumerate() {
                let (start, length) = column.byte_range();
                let pages = metadata
                    .offset_index()
                    .map(|groups| {
                        groups[group_index][column_index]
                            .page_locations
                            .iter()
                            .map(|page| {
                                page.offset as u64
                                    ..page.offset as u64 + page.compressed_page_size as u64
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                chunks.push(ColumnChunk {
                    row_group: group_index,
                    root_column: column.column_path().parts()[0].clone(),
                    column_path: column.column_path().parts().to_vec(),
                    range: start..start + length,
                    pages,
                });
            }
        }
        Ok(Self {
            path: path.to_string(),
            size,
            chunks,
        })
    }

    pub fn touched_chunks(&self, reads: &[ReadRange]) -> BTreeSet<(usize, Vec<String>)> {
        // Include every overlapping physical request, even a footer probe or full-file GET.
        self.chunks
            .iter()
            .filter(|chunk| {
                reads
                    .iter()
                    .any(|read| read.path == self.path && overlaps(&read.range, &chunk.range))
            })
            .map(|chunk| (chunk.row_group, chunk.column_path.clone()))
            .collect()
    }

    pub fn touched_pages(
        &self,
        reads: &[ReadRange],
        root_column: &str,
    ) -> BTreeSet<(usize, Vec<String>, usize)> {
        self.chunks
            .iter()
            .filter(|chunk| chunk.root_column == root_column)
            .flat_map(|chunk| {
                chunk
                    .pages
                    .iter()
                    .enumerate()
                    .filter(|(_, page)| {
                        reads
                            .iter()
                            .any(|read| read.path == self.path && overlaps(&read.range, page))
                    })
                    .map(|(index, _)| (chunk.row_group, chunk.column_path.clone(), index))
            })
            .collect()
    }
}

fn overlaps(left: &Range<u64>, right: &Range<u64>) -> bool {
    left.start < right.end && right.start < left.end
}
