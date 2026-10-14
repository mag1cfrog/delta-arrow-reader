//! Provider planning must leave the async worker available while Kernel reads metadata.

use std::{
    fmt,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use async_trait::async_trait;
use futures_util::{FutureExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult,
    local::LocalFileSystem, path::Path as StorePath,
};
use tokio::sync::Semaphore;
use tracing::{
    Event, Metadata, Subscriber,
    instrument::WithSubscriber,
    span::{Attributes, Id, Record},
};

use super::*;

const DEADLINE: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct ReadGate {
    armed: AtomicBool,
    started: Semaphore,
    release: Semaphore,
}

impl ReadGate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            armed: AtomicBool::new(false),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
        })
    }

    async fn wait_started(&self) -> TestResult {
        tokio::time::timeout(DEADLINE, self.started.acquire())
            .await??
            .forget();
        Ok(())
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
}

#[derive(Debug)]
struct PlanningStore {
    inner: LocalFileSystem,
    gate: Arc<ReadGate>,
    log_reads: AtomicUsize,
}

impl fmt::Display for PlanningStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("planning-test-store")
    }
}

#[async_trait]
impl ObjectStore for PlanningStore {
    async fn put_opts(
        &self,
        path: &StorePath,
        payload: PutPayload,
        options: PutOptions,
    ) -> StoreResult<PutResult> {
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &StorePath,
        options: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &StorePath, options: GetOptions) -> StoreResult<GetResult> {
        if !options.head && path.as_ref().ends_with("00000000000000000000.json") {
            self.log_reads.fetch_add(1, Ordering::Relaxed);
            if self.gate.armed.swap(false, Ordering::AcqRel) {
                self.gate.started.add_permits(1);
                self.gate
                    .release
                    .acquire()
                    .await
                    .expect("gate stays open")
                    .forget();
            }
        }
        self.inner.get_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, StoreResult<StorePath>>,
    ) -> BoxStream<'static, StoreResult<StorePath>> {
        self.inner.delete_stream(paths)
    }

    fn list(&self, prefix: Option<&StorePath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&StorePath>) -> StoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> StoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn planning_store(fixture: &TestTable) -> TestResult<(String, Arc<PlanningStore>)> {
    static NEXT_STORE: AtomicUsize = AtomicUsize::new(0);
    let scheme = format!(
        "darplanning{}x{}",
        std::process::id(),
        NEXT_STORE.fetch_add(1, Ordering::Relaxed)
    );
    let store = Arc::new(PlanningStore {
        inner: LocalFileSystem::new_with_prefix(&fixture.0)?,
        gate: ReadGate::new(),
        log_reads: AtomicUsize::new(0),
    });
    // The process-global URL registry must not retain the store after the test.
    let weak = Arc::downgrade(&store);
    delta_kernel_default_engine::storage::insert_url_handler(
        &scheme,
        Arc::new(move |_, _| {
            let store = weak.upgrade().ok_or_else(|| object_store::Error::Generic {
                store: "planning-test-store",
                source: std::io::Error::other("store was released").into(),
            })?;
            Ok((Box::new(store), StorePath::from("")))
        }),
    )?;
    Ok((format!("{scheme}://table/"), store))
}

// Runs outside Tokio so a regressed synchronous scan cannot block its own release.
// A dropped sender also releases the gate when a test returns early.
fn release_after_signal(gate: Arc<ReadGate>) -> (mpsc::Sender<()>, thread::JoinHandle<bool>) {
    let (sender, receiver) = mpsc::channel();
    let watchdog = thread::spawn(move || {
        let signalled = receiver.recv_timeout(DEADLINE).is_ok();
        gate.release.add_permits(1);
        signalled
    });
    (sender, watchdog)
}

#[test]
fn cold_planning_keeps_a_single_async_worker_responsive() -> TestResult {
    let fixture = TestTable::partitioned("planning-worker")?;
    let (uri, store) = planning_store(&fixture)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let table = runtime.block_on(DeltaTableBuilder::new(uri).load_table())?;
    let provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
    let context = SessionContext::new();
    let reads = store.log_reads.load(Ordering::Relaxed);
    store.gate.arm();
    let (heartbeat, watchdog) = release_after_signal(Arc::clone(&store.gate));
    let result = runtime.block_on(async {
        let state = context.state();
        let progress = async {
            store.gate.wait_started().await?;
            let _ = heartbeat.send(());
            Ok::<_, Box<dyn Error>>(())
        };
        let (plan, progress) =
            tokio::join!(biased; provider.scan(&state, None, &[], None), progress);
        progress?;
        let batches = collect_plan(&context, plan?).await?;
        let mut actual = ids(&batches);
        actual.sort_unstable();
        assert_eq!(actual, [1, 2, 3, 4]);
        Ok::<_, Box<dyn Error>>(())
    });
    assert!(
        watchdog.join().expect("watchdog completed"),
        "cold scan blocked the heartbeat until the external deadline"
    );
    result?;
    assert!(
        store.log_reads.load(Ordering::Relaxed) > reads,
        "cold planning must perform a real log read"
    );
    Ok(())
}

#[test]
fn dropping_blocked_planning_releases_owned_resources_after_io_finishes() -> TestResult {
    let fixture = TestTable::partitioned("planning-drop")?;
    let (uri, store) = planning_store(&fixture)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let table = runtime.block_on(DeltaTableBuilder::new(uri).load_table())?;
    let provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
    let weak = Arc::downgrade(&store);
    let gate = Arc::clone(&store.gate);
    gate.arm();
    let (release, watchdog) = release_after_signal(Arc::clone(&gate));
    let result = runtime.block_on(async move {
        let pending = tokio::spawn(async move {
            let context = SessionContext::new();
            provider.scan(&context.state(), None, &[], None).await
        });
        gate.wait_started().await?;
        assert!(!pending.is_finished(), "planning must still be blocked");
        pending.abort();
        assert!(
            pending
                .await
                .expect_err("waiting task was aborted")
                .is_cancelled()
        );
        drop(store);
        assert!(
            weak.upgrade().is_some(),
            "in-progress I/O still owns its store"
        );
        let _ = release.send(());
        tokio::time::timeout(DEADLINE, async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        Ok::<_, Box<dyn Error>>(())
    });
    assert!(
        watchdog.join().expect("watchdog completed"),
        "async worker stalled before cancellation"
    );
    result
}

#[test]
fn concurrent_refresh_and_planning_keep_each_snapshot_stable() -> TestResult {
    let fixture = TestTable::partitioned("planning-refresh")?;
    let (uri, store) = planning_store(&fixture)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let table = runtime.block_on(DeltaTableBuilder::new(uri).load_table())?;
    let provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
    let original = provider.clone();
    store.gate.arm();
    let (release, watchdog) = release_after_signal(Arc::clone(&store.gate));
    let result = runtime.block_on(async {
        let pending = tokio::spawn(async move {
            let context = SessionContext::new();
            original.scan(&context.state(), None, &[], None).await
        });
        store.gate.wait_started().await?;
        assert!(
            !pending.is_finished(),
            "original plan must remain blocked during refresh"
        );
        let north = fixture.write_parquet("north.parquet", &[5, 6])?;
        fixture.write_log_version(
            1,
            &[
                metadata_with_note(),
                add("north.parquet", north, "north", 2, 5, 6),
            ],
        )?;
        let refreshed = provider.refresh().await?;
        let context = SessionContext::new();
        let new_plan = refreshed.scan(&context.state(), None, &[], None).await?;
        assert_eq!(
            collect_scan_metrics(new_plan.as_ref())[0]
                .snapshot()
                .reader_metrics
                .snapshot_version,
            1
        );
        let mut new_ids = ids(&collect_plan(&context, new_plan).await?);
        new_ids.sort_unstable();
        assert_eq!(new_ids, [1, 2, 3, 4, 5, 6]);
        let _ = release.send(());
        let old_plan = pending.await??;
        assert_eq!(
            collect_scan_metrics(old_plan.as_ref())[0]
                .snapshot()
                .reader_metrics
                .snapshot_version,
            0
        );
        let mut old_ids = ids(&collect_plan(&context, old_plan).await?);
        old_ids.sort_unstable();
        assert_eq!(old_ids, [1, 2, 3, 4]);
        assert_eq!(provider.schema().fields().len(), 2);
        assert_eq!(refreshed.schema().fields().len(), 3);
        Ok::<_, Box<dyn Error>>(())
    });
    assert!(
        watchdog.join().expect("watchdog completed"),
        "async worker stalled before refresh"
    );
    result
}

#[derive(Clone, Default)]
struct PlanningTrace {
    spans: Arc<Mutex<Vec<(&'static str, thread::ThreadId)>>>,
    panic_on_planning: bool,
}

impl Subscriber for PlanningTrace {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == "delta_arrow_reader::profile"
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let name = attributes.metadata().name();
        // Inject a panic in our planning task, outside Kernel's own I/O tasks.
        assert!(
            !(self.panic_on_planning && name == "Delta scan planning"),
            "injected planning panic"
        );
        let mut spans = self.spans.lock().expect("trace lock");
        spans.push((name, thread::current().id()));
        Id::from_u64(spans.len() as u64)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, _: &Event<'_>) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

#[tokio::test]
async fn cold_and_warm_planning_preserve_the_callers_tracing_dispatch() -> TestResult {
    // Register untraced callers too, so concurrent scans cannot initialize a
    // disabled callsite through tracing's single-dispatcher optimization.
    let _untraced = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let fixture = TestTable::partitioned("planning-tracing")?;
    for warmup in [WarmupMode::None, WarmupMode::QueryPlanning] {
        let table = DeltaTableBuilder::new(fixture.uri())
            .with_warmup(warmup)
            .load_table()
            .await?;
        let provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
        let context = SessionContext::new();
        let caller = thread::current().id();
        let trace = PlanningTrace::default();
        provider
            .scan(&context.state(), None, &[], None)
            .with_subscriber(trace.clone())
            .await?;
        let spans = trace.spans.lock().expect("trace lock");
        for name in [
            "Delta scan planning",
            "Delta scan metadata expansion",
            "Delta scan execution setup",
        ] {
            let captured = spans
                .iter()
                .filter(|(actual, _)| *actual == name)
                .collect::<Vec<_>>();
            assert_eq!(captured.len(), 1, "missing or duplicated {name}: {spans:?}");
            assert_ne!(
                captured[0].1, caller,
                "{name} must run outside the async worker"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn planning_task_panics_keep_the_join_error_as_the_source() -> TestResult {
    let _untraced = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let fixture = TestTable::partitioned("planning-panic")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
    let provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
    let context = SessionContext::new();
    let trace = PlanningTrace {
        panic_on_planning: true,
        ..Default::default()
    };
    let outcome = std::panic::AssertUnwindSafe(
        provider
            .scan(&context.state(), None, &[], None)
            .with_subscriber(trace),
    )
    .catch_unwind()
    .await;
    let error = outcome
        .expect("planning panic must not unwind the caller")
        .expect_err("injected panic must fail planning");
    let reader = external_reader_error(&error)?;
    assert_eq!(reader.phase(), DeltaReaderPhase::ScanPlanning);
    let DeltaReaderError::ScanPlanning { reason, source, .. } = reader else {
        return Err("join failure was not a scan-planning error".into());
    };
    assert_eq!(*reason, "datafusion_scan_planning_task_failed");
    assert!(
        source
            .downcast_ref::<tokio::task::JoinError>()
            .ok_or("missing JoinError source")?
            .is_panic()
    );
    Ok(())
}

#[tokio::test]
async fn kernel_planning_failures_keep_the_original_reason_and_source() -> TestResult {
    let fixture = TestTable::partitioned("planning-error")?;
    let table = DeltaTableBuilder::new(fixture.uri()).load_table().await?;
    let provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
    fixture.disable_delta_log()?;
    let context = SessionContext::new();
    let error = provider
        .scan(&context.state(), None, &[], None)
        .await
        .expect_err("missing log must fail cold planning");
    let reader = external_reader_error(&error)?;
    assert_eq!(reader.phase(), DeltaReaderPhase::ScanPlanning);
    let DeltaReaderError::ScanPlanning { reason, source, .. } = reader else {
        return Err("original scan-planning error was replaced".into());
    };
    assert_eq!(*reason, "kernel_scan_metadata_failed");
    assert!(source.downcast_ref::<delta_kernel::Error>().is_some());
    Ok(())
}

#[tokio::test]
async fn cold_and_warm_plans_preserve_owned_inputs_and_session_settings() -> TestResult {
    for warmup in [WarmupMode::None, WarmupMode::QueryPlanning] {
        let fixture = TestTable::partitioned("planning-settings")?;
        let west = fs::metadata(fixture.0.join("west.parquet"))?.len();
        let east = fs::metadata(fixture.0.join("east.parquet"))?.len();
        fixture.write_log(&[
            protocol(1),
            metadata_with_note(),
            add("west.parquet", west, "west", 2, 1, 2),
            add("east.parquet", east, "east", 2, 3, 4),
        ])?;
        let table = DeltaTableBuilder::new(fixture.uri())
            .with_warmup(warmup)
            .load_table()
            .await?;
        if warmup == WarmupMode::QueryPlanning {
            fixture.disable_delta_log()?;
        }
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            for views in [false, true] {
                for target_partitions in [None, Some(1)] {
                    let provider = DeltaTableProvider::try_new(
                        table.clone(),
                        ScanOptions {
                            execution_options: DeltaScanExecutionOptions::new()
                                .with_parquet_backend(backend),
                            target_partitions,
                            use_arrow_view_types: views,
                            ..Default::default()
                        },
                    )?;
                    let context = SessionContext::new_with_config(
                        SessionConfig::new().with_target_partitions(2),
                    );
                    let filters = [col("id").gt(lit(1_i32))];
                    let expected = if backend == ParquetReaderBackend::Direct {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Inexact
                    };
                    assert_eq!(
                        provider.supports_filters_pushdown(&[&filters[0]])?,
                        [expected]
                    );
                    let plan = provider
                        .scan(&context.state(), Some(&vec![2, 0]), &filters, None)
                        .await?;
                    assert_eq!(
                        plan.properties().output_partitioning().partition_count(),
                        target_partitions.unwrap_or(2)
                    );
                    let expected_schema = provider.schema().project(&[2, 0])?;
                    assert_eq!(plan.schema().as_ref(), &expected_schema);
                    assert_eq!(
                        plan.schema().field(0).data_type(),
                        &if views {
                            DataType::Utf8View
                        } else {
                            DataType::Utf8
                        }
                    );
                    let metrics = collect_scan_metrics(plan.as_ref());
                    let batches = collect_plan(&context, plan).await?;
                    let mut actual = ids(&batches);
                    actual.sort_unstable();
                    let expected_ids = if backend == ParquetReaderBackend::Direct {
                        vec![2, 3, 4]
                    } else {
                        // Inexact pushdown leaves the residual filter to DataFusion.
                        vec![1, 2, 3, 4]
                    };
                    assert_eq!(actual, expected_ids);
                    assert!(
                        batches
                            .iter()
                            .all(|batch| batch.schema().as_ref() == &expected_schema
                                && batch.column(0).null_count() == batch.num_rows())
                    );
                    let snapshot = metrics[0].snapshot();
                    assert_eq!(snapshot.reader_metrics.snapshot_version, 0);
                    assert_eq!(snapshot.reader_metrics.parquet_backend, backend);
                    assert_eq!(snapshot.reader_metrics.files_planned, 2);
                    assert_eq!(snapshot.uses_arrow_view_types, views);
                }
            }
        }
    }
    Ok(())
}
