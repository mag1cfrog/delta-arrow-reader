//! Compare fresh and incremental Delta snapshot refresh strategies.
//!
//! Run with `cargo bench --bench incremental_refresh -- --files 4000 --repetitions 10`.
//! Fixture creation stays outside the measured interval, and strategy order reverses between
//! repetitions to reduce ordering bias.

use std::{
    env,
    error::Error,
    fmt,
    hint::black_box,
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use delta_kernel::{
    Engine, EngineData, Snapshot, SnapshotRef,
    engine::arrow_data::ArrowEngineData,
    scan::{ScanMetadata, state::ScanFile},
};
use delta_kernel_default_engine::{
    DefaultEngineBuilder, executor::tokio::TokioMultiThreadExecutor,
};
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use url::Url;

const DEFAULT_FILES: usize = 4_000;
const DEFAULT_REPETITIONS: usize = 10;
const TABLE_METADATA: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"incremental-refresh-benchmark","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"part\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":["part"],"configuration":{},"createdTime":0}}"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Config {
    files: usize,
    repetitions: usize,
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut files = DEFAULT_FILES;
        let mut repetitions = DEFAULT_REPETITIONS;
        let mut args = args.into_iter();
        while let Some(argument) = args.next() {
            if argument == "--bench" {
                continue;
            }
            let value = args.next().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{argument} requires a value"),
                )
            })?;
            match argument.as_str() {
                "--files" => files = value.parse()?,
                "--repetitions" => repetitions = value.parse()?,
                _ => return Err(invalid(format!("unknown argument: {argument}")).into()),
            }
        }
        if files == 0 || repetitions == 0 {
            return Err(invalid("files and repetitions must be greater than zero").into());
        }
        Ok(Self { files, repetitions })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefreshStrategy {
    FreshSnapshotAndMetadata,
    IncrementalSnapshotAndFreshMetadata,
    IncrementalSnapshotAndMetadataRefresh,
}

impl RefreshStrategy {
    const ALL: [Self; 3] = [
        Self::FreshSnapshotAndMetadata,
        Self::IncrementalSnapshotAndFreshMetadata,
        Self::IncrementalSnapshotAndMetadataRefresh,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::FreshSnapshotAndMetadata => "fresh_snapshot_and_metadata",
            Self::IncrementalSnapshotAndFreshMetadata => "incremental_snapshot_and_fresh_metadata",
            Self::IncrementalSnapshotAndMetadataRefresh => {
                "incremental_snapshot_and_metadata_refresh"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChangeWorkload {
    NoNewVersion,
    MetadataOnlyCommit,
    AddOneFile,
    RemoveOneFile,
    ReplaceOneFile,
    ChurnOnePercent,
    ChurnFiftyPercent,
}

impl ChangeWorkload {
    const ALL: [Self; 7] = [
        Self::NoNewVersion,
        Self::MetadataOnlyCommit,
        Self::AddOneFile,
        Self::RemoveOneFile,
        Self::ReplaceOneFile,
        Self::ChurnOnePercent,
        Self::ChurnFiftyPercent,
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::NoNewVersion => "no_new_version",
            Self::MetadataOnlyCommit => "metadata_only_commit",
            Self::AddOneFile => "add_one_file",
            Self::RemoveOneFile => "remove_one_file",
            Self::ReplaceOneFile => "replace_one_file",
            Self::ChurnOnePercent => "churn_one_percent",
            Self::ChurnFiftyPercent => "churn_fifty_percent",
        }
    }

    fn commit(self, base_files: usize) -> Option<String> {
        let actions = match self {
            Self::NoNewVersion => return None,
            Self::MetadataOnlyCommit => {
                vec![r#"{"commitInfo":{"operation":"incremental-refresh-benchmark"}}"#.to_owned()]
            }
            Self::AddOneFile => vec![add_action(base_files)],
            Self::RemoveOneFile => vec![remove_action(&file_path(0))],
            Self::ReplaceOneFile | Self::ChurnOnePercent | Self::ChurnFiftyPercent => {
                let changed_files = self.replaced_file_count(base_files);
                let mut actions = Vec::with_capacity(changed_files.saturating_mul(2));
                for index in 0..changed_files {
                    actions.push(remove_action(&file_path(index)));
                    actions.push(add_action(base_files.saturating_add(index)));
                }
                actions
            }
        };
        Some(actions.join("\n"))
    }

    fn replaced_file_count(self, base_files: usize) -> usize {
        match self {
            Self::ReplaceOneFile => 1,
            Self::ChurnOnePercent => (base_files / 100).max(1),
            Self::ChurnFiftyPercent => (base_files / 2).max(1),
            _ => 0,
        }
    }

    const fn target_version(self) -> u64 {
        match self {
            Self::NoNewVersion => 0,
            _ => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryShape {
    NoNewCheckpoint,
    NewCheckpoint,
}

impl HistoryShape {
    const ALL: [Self; 2] = [Self::NoNewCheckpoint, Self::NewCheckpoint];

    const fn name(self) -> &'static str {
        match self {
            Self::NoNewCheckpoint => "no_new_checkpoint",
            Self::NewCheckpoint => "new_checkpoint",
        }
    }

    fn applies_to(self, workload: ChangeWorkload) -> bool {
        self == Self::NoNewCheckpoint || workload.target_version() > 0
    }
}

struct Fixture {
    table_url: Url,
    storage: Arc<MeasuredStore>,
}

impl Fixture {
    async fn create(
        files: usize,
        workload: ChangeWorkload,
        history: HistoryShape,
    ) -> Result<Self, Box<dyn Error>> {
        let table_url = Url::parse("memory:///")?;
        let storage = Arc::new(MeasuredStore::default());
        let mut initial_commit = String::from(TABLE_METADATA);
        for index in 0..files {
            initial_commit.push('\n');
            initial_commit.push_str(&add_action(index));
        }
        write_commit(storage.as_ref(), 0, initial_commit).await?;
        let fixture = Self { table_url, storage };
        let engine = fixture.engine();
        let base_snapshot = Snapshot::builder_for(fixture.table_url.clone())
            .at_version(0)
            .build(engine.as_ref())?;
        base_snapshot.checkpoint(engine.as_ref(), None)?;

        if let Some(commit) = workload.commit(files) {
            write_commit(fixture.storage.as_ref(), 1, commit).await?;
        }
        if history == HistoryShape::NewCheckpoint {
            let snapshot =
                Snapshot::builder_for(fixture.table_url.clone()).build(engine.as_ref())?;
            snapshot.checkpoint(engine.as_ref(), None)?;
        }
        Ok(fixture)
    }

    fn engine(&self) -> Arc<dyn Engine> {
        let executor = Arc::new(TokioMultiThreadExecutor::new(
            tokio::runtime::Handle::current(),
        ));
        Arc::new(
            DefaultEngineBuilder::new(self.storage.clone())
                .with_task_executor(executor)
                .build(),
        )
    }
}

/// Counts object-store GET and listing operations while preparing each refresh.
#[derive(Debug, Default)]
struct MeasuredStore {
    inner: InMemory,
    read_operations: AtomicU64,
    bytes_read: AtomicU64,
}

impl MeasuredStore {
    fn reset(&self) {
        self.read_operations.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
    }

    fn snapshot(&self) -> IoMeasurement {
        IoMeasurement {
            read_operations: u128::from(self.read_operations.load(Ordering::Relaxed)),
            bytes_read: u128::from(self.bytes_read.load(Ordering::Relaxed)),
        }
    }
}

impl fmt::Display for MeasuredStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MeasuredStore")
    }
}

#[async_trait]
impl ObjectStore for MeasuredStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = self.inner.get_opts(location, options).await?;
        self.read_operations.fetch_add(1, Ordering::Relaxed);
        self.bytes_read.fetch_add(
            result.range.end.saturating_sub(result.range.start),
            Ordering::Relaxed,
        );
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.read_operations.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.read_operations.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug, Clone, Copy)]
struct IoMeasurement {
    read_operations: u128,
    bytes_read: u128,
}

struct PreparedRefresh {
    snapshot: SnapshotRef,
    metadata: Arc<[RecordBatch]>,
}

impl PreparedRefresh {
    fn scan_files(&self, engine: &dyn Engine) -> Result<Vec<ScanFile>, delta_kernel::Error> {
        scan_files_from_metadata(&self.snapshot, engine, &self.metadata)
    }
}

#[derive(Debug)]
struct Measurement {
    case: BenchmarkCase,
    elapsed_micros: u128,
    object_store_read_operations: u128,
    object_store_bytes_read: u128,
    active_files: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BenchmarkCase {
    workload: ChangeWorkload,
    history: HistoryShape,
    strategy: RefreshStrategy,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse(env::args().skip(1))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(config))
}

async fn run(config: Config) -> Result<(), Box<dyn Error>> {
    let mut measurements = Vec::with_capacity(
        config
            .repetitions
            .saturating_mul(RefreshStrategy::ALL.len())
            .saturating_mul(ChangeWorkload::ALL.len())
            .saturating_mul(HistoryShape::ALL.len()),
    );
    for workload in ChangeWorkload::ALL {
        for history in HistoryShape::ALL {
            if history.applies_to(workload) {
                measurements.extend(run_workload(config, workload, history).await?);
            }
        }
    }
    print_measurements(config.files, &measurements);
    Ok(())
}

async fn run_workload(
    config: Config,
    workload: ChangeWorkload,
    history: HistoryShape,
) -> Result<Vec<Measurement>, Box<dyn Error>> {
    let fixture = Fixture::create(config.files, workload, history).await?;
    let engine = fixture.engine();
    let base_snapshot = Snapshot::builder_for(fixture.table_url.clone())
        .at_version(0)
        .build(engine.as_ref())?;
    let base_metadata = materialize_fresh_metadata(&base_snapshot, engine.as_ref())?;
    let target_snapshot =
        Snapshot::builder_for(fixture.table_url.clone()).build(engine.as_ref())?;
    let expected_files = scan_files(&target_snapshot, engine.as_ref())?;

    let mut measurements = Vec::with_capacity(config.repetitions.saturating_mul(3));
    for repetition in 1..=config.repetitions {
        let mut strategies = RefreshStrategy::ALL;
        if repetition % 2 == 0 {
            strategies.reverse();
        }
        for strategy in strategies {
            let case = BenchmarkCase {
                workload,
                history,
                strategy,
            };
            measurements.push(measure(
                &fixture,
                engine.as_ref(),
                &base_snapshot,
                Arc::clone(&base_metadata),
                &expected_files,
                case,
            )?);
        }
    }
    Ok(measurements)
}

fn measure(
    fixture: &Fixture,
    engine: &dyn Engine,
    base_snapshot: &SnapshotRef,
    base_metadata: Arc<[RecordBatch]>,
    expected_files: &[ScanFile],
    case: BenchmarkCase,
) -> Result<Measurement, Box<dyn Error>> {
    fixture.storage.reset();
    let started_at = Instant::now();
    let prepared = match case.strategy {
        RefreshStrategy::FreshSnapshotAndMetadata => {
            prepare_fresh_refresh(&fixture.table_url, engine)?
        }
        RefreshStrategy::IncrementalSnapshotAndFreshMetadata => {
            prepare_incremental_refresh_with_fresh_metadata(base_snapshot, engine)?
        }
        RefreshStrategy::IncrementalSnapshotAndMetadataRefresh => {
            prepare_incremental_refresh_with_metadata_refresh(base_snapshot, base_metadata, engine)?
        }
    };
    let elapsed_micros = started_at.elapsed().as_micros();
    let io = fixture.storage.snapshot();

    let actual_files = prepared.scan_files(engine)?;
    if actual_files != expected_files
        || prepared.snapshot.version() != case.workload.target_version()
    {
        return Err(invalid(format!(
            "{} produced the wrong target snapshot for {} with {}",
            case.strategy.name(),
            case.workload.name(),
            case.history.name(),
        ))
        .into());
    }
    black_box(&prepared.metadata);
    Ok(Measurement {
        case,
        elapsed_micros,
        object_store_read_operations: io.read_operations,
        object_store_bytes_read: io.bytes_read,
        active_files: actual_files.len(),
    })
}

fn prepare_fresh_refresh(
    table_url: &Url,
    engine: &dyn Engine,
) -> Result<PreparedRefresh, delta_kernel::Error> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
    let metadata = materialize_fresh_metadata(&snapshot, engine)?;
    Ok(PreparedRefresh { snapshot, metadata })
}

fn prepare_incremental_refresh_with_fresh_metadata(
    base_snapshot: &SnapshotRef,
    engine: &dyn Engine,
) -> Result<PreparedRefresh, delta_kernel::Error> {
    let snapshot = Snapshot::builder_from(Arc::clone(base_snapshot)).build(engine)?;
    let metadata = materialize_fresh_metadata(&snapshot, engine)?;
    Ok(PreparedRefresh { snapshot, metadata })
}

fn prepare_incremental_refresh_with_metadata_refresh(
    base_snapshot: &SnapshotRef,
    base_metadata: Arc<[RecordBatch]>,
    engine: &dyn Engine,
) -> Result<PreparedRefresh, delta_kernel::Error> {
    let snapshot = Snapshot::builder_from(Arc::clone(base_snapshot)).build(engine)?;
    let metadata = if snapshot.version() == base_snapshot.version() {
        base_metadata
    } else {
        let scan = Arc::clone(&snapshot).scan_builder().build()?;
        let existing_data = base_metadata
            .iter()
            .cloned()
            .map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
            .collect::<Vec<_>>();
        materialize_metadata(scan.scan_metadata_from(
            engine,
            base_snapshot.version(),
            existing_data,
            None,
        )?)?
    };
    Ok(PreparedRefresh { snapshot, metadata })
}

fn scan_files(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
) -> Result<Vec<ScanFile>, delta_kernel::Error> {
    let scan = Arc::clone(snapshot).scan_builder().build()?;
    collect_scan_files(scan.scan_metadata(engine)?)
}

fn scan_files_from_metadata(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
    metadata: &[RecordBatch],
) -> Result<Vec<ScanFile>, delta_kernel::Error> {
    let scan = Arc::clone(snapshot).scan_builder().build()?;
    let existing_data = metadata
        .iter()
        .cloned()
        .map(|batch| Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>)
        .collect::<Vec<_>>();
    collect_scan_files(scan.scan_metadata_from(engine, snapshot.version(), existing_data, None)?)
}

fn collect_scan_files(
    metadata: impl Iterator<Item = Result<ScanMetadata, delta_kernel::Error>>,
) -> Result<Vec<ScanFile>, delta_kernel::Error> {
    fn collect(files: &mut Vec<ScanFile>, file: ScanFile) {
        files.push(file);
    }

    let mut files = Vec::new();
    for metadata in metadata {
        files = metadata?.visit_scan_files(files, collect)?;
    }
    files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    Ok(files)
}

fn materialize_fresh_metadata(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
) -> Result<Arc<[RecordBatch]>, delta_kernel::Error> {
    let scan = Arc::clone(snapshot).scan_builder().build()?;
    materialize_metadata(scan.scan_metadata(engine)?)
}

fn materialize_metadata(
    metadata: impl Iterator<Item = Result<ScanMetadata, delta_kernel::Error>>,
) -> Result<Arc<[RecordBatch]>, delta_kernel::Error> {
    let mut batches = Vec::new();
    for metadata in metadata {
        let data = metadata?.scan_files.apply_selection_vector()?;
        let mut batch: RecordBatch = ArrowEngineData::try_from_engine_data(data)?.into();
        if let Ok(index) = batch.schema().index_of("stats_parsed") {
            batch.remove_column(index);
        }
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    Ok(batches.into())
}

fn add_action(index: usize) -> String {
    let path = file_path(index);
    let partition = if index.is_multiple_of(2) {
        "even"
    } else {
        "odd"
    };
    format!(
        r#"{{"add":{{"path":"{path}","partitionValues":{{"part":"{partition}"}},"size":262,"modificationTime":0,"dataChange":true,"stats":"{{\"numRecords\":10}}"}}}}"#
    )
}

fn remove_action(path: &str) -> String {
    format!(r#"{{"remove":{{"path":"{path}","deletionTimestamp":0,"dataChange":true}}}}"#)
}

fn file_path(index: usize) -> String {
    format!("part-{index:08}.parquet")
}

async fn write_commit(
    storage: &MeasuredStore,
    version: u64,
    contents: String,
) -> Result<(), object_store::Error> {
    let path = Path::from(format!("_delta_log/{version:020}.json"));
    storage.put(&path, contents.into()).await?;
    Ok(())
}

fn print_measurements(base_files: usize, measurements: &[Measurement]) {
    println!(
        "base_files,workload,history,strategy,repetitions,median_elapsed_micros,median_object_store_read_operations,median_object_store_bytes_read,active_files"
    );
    for workload in ChangeWorkload::ALL {
        for history in HistoryShape::ALL {
            if !history.applies_to(workload) {
                continue;
            }
            for strategy in RefreshStrategy::ALL {
                let case = BenchmarkCase {
                    workload,
                    history,
                    strategy,
                };
                let matching = measurements
                    .iter()
                    .filter(|measurement| measurement.case == case);
                let Some(first) = matching.clone().next() else {
                    continue;
                };
                let elapsed = matching
                    .clone()
                    .map(|measurement| measurement.elapsed_micros)
                    .collect::<Vec<_>>();
                let read_operations = matching
                    .clone()
                    .map(|measurement| measurement.object_store_read_operations)
                    .collect::<Vec<_>>();
                let bytes_read = matching
                    .map(|measurement| measurement.object_store_bytes_read)
                    .collect::<Vec<_>>();
                println!(
                    "{},{},{},{},{},{},{},{},{}",
                    base_files,
                    workload.name(),
                    history.name(),
                    strategy.name(),
                    elapsed.len(),
                    median(elapsed),
                    median(read_operations),
                    median(bytes_read),
                    first.active_files,
                );
            }
        }
    }
}

fn median(mut values: Vec<u128>) -> u128 {
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2
    } else {
        values[middle]
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn config_accepts_the_cargo_bench_flag() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            Config::parse(["--files", "8", "--repetitions", "2", "--bench"].map(String::from))?,
            Config {
                files: 8,
                repetitions: 2,
            }
        );
        assert_eq!(median(vec![9, 1, 5]), 5);
        assert_eq!(median(vec![10, 2, 6, 4]), 5);
        Ok(())
    }

    #[test]
    fn metadata_refresh_avoids_rereading_the_base_checkpoint() -> Result<(), Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let fixture =
                Fixture::create(8, ChangeWorkload::AddOneFile, HistoryShape::NoNewCheckpoint)
                    .await?;
            let engine = fixture.engine();
            let base = Snapshot::builder_for(fixture.table_url.clone())
                .at_version(0)
                .build(engine.as_ref())?;
            let base_metadata = materialize_fresh_metadata(&base, engine.as_ref())?;
            let target = Snapshot::builder_for(fixture.table_url.clone()).build(engine.as_ref())?;
            let expected_files = scan_files(&target, engine.as_ref())?;

            let fresh = measure(
                &fixture,
                engine.as_ref(),
                &base,
                Arc::clone(&base_metadata),
                &expected_files,
                BenchmarkCase {
                    workload: ChangeWorkload::AddOneFile,
                    history: HistoryShape::NoNewCheckpoint,
                    strategy: RefreshStrategy::FreshSnapshotAndMetadata,
                },
            )?;
            let refreshed = measure(
                &fixture,
                engine.as_ref(),
                &base,
                base_metadata,
                &expected_files,
                BenchmarkCase {
                    workload: ChangeWorkload::AddOneFile,
                    history: HistoryShape::NoNewCheckpoint,
                    strategy: RefreshStrategy::IncrementalSnapshotAndMetadataRefresh,
                },
            )?;

            assert!(refreshed.object_store_read_operations < fresh.object_store_read_operations);
            assert!(refreshed.object_store_bytes_read < fresh.object_store_bytes_read);
            Ok(())
        })
    }

    #[test]
    fn refresh_strategies_produce_the_same_target_metadata() -> Result<(), Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            for workload in ChangeWorkload::ALL {
                for history in HistoryShape::ALL {
                    if !history.applies_to(workload) {
                        continue;
                    }
                    let fixture = Fixture::create(8, workload, history).await?;
                    let engine = fixture.engine();
                    let base = Snapshot::builder_for(fixture.table_url.clone())
                        .at_version(0)
                        .build(engine.as_ref())?;
                    let base_metadata = materialize_fresh_metadata(&base, engine.as_ref())?;
                    let expected = prepare_fresh_refresh(&fixture.table_url, engine.as_ref())?;
                    let expected_files = expected.scan_files(engine.as_ref())?;
                    let mut expected_paths = (0..8).map(file_path).collect::<Vec<_>>();
                    match workload {
                        ChangeWorkload::NoNewVersion | ChangeWorkload::MetadataOnlyCommit => {}
                        ChangeWorkload::AddOneFile => {
                            expected_paths.push(file_path(8));
                        }
                        ChangeWorkload::RemoveOneFile => {
                            expected_paths.retain(|path| path != &file_path(0));
                        }
                        ChangeWorkload::ReplaceOneFile
                        | ChangeWorkload::ChurnOnePercent
                        | ChangeWorkload::ChurnFiftyPercent => {
                            for index in 0..workload.replaced_file_count(8) {
                                expected_paths.retain(|path| path != &file_path(index));
                                expected_paths.push(file_path(8 + index));
                            }
                        }
                    }
                    expected_paths.sort_unstable();
                    assert_eq!(
                        expected_files
                            .iter()
                            .map(|file| file.path.clone())
                            .collect::<Vec<_>>(),
                        expected_paths,
                        "fixture produced the wrong target metadata for {} with {}",
                        workload.name(),
                        history.name(),
                    );

                    for strategy in RefreshStrategy::ALL {
                        let case = BenchmarkCase {
                            workload,
                            history,
                            strategy,
                        };
                        measure(
                            &fixture,
                            engine.as_ref(),
                            &base,
                            Arc::clone(&base_metadata),
                            &expected_files,
                            case,
                        )?;
                    }
                }
            }
            Ok(())
        })
    }
}
