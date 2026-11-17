//! Real Delta planning, streaming, cancellation and host resource limits.

use std::{
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::{
    array::{Array, Int32Array, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use datafusion::{
    common::Result as DataFusionResult,
    execution::{
        SessionStateBuilder,
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        memory_pool::{GreedyMemoryPool, MemoryLimit, MemoryPool, MemoryReservation},
        runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
    },
    physical_plan::{ExecutionPlan, SendableRecordBatchStream, collect, execute_stream},
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader::{
    DeltaScanExecutionOptions, DeltaTableBuilder, WarmupMode,
    datafusion::{DeltaTableProvider, ScanOptions, collect_scan_metrics},
};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use futures_util::StreamExt;
use tokio::sync::Semaphore;

#[path = "support/delta_query.rs"]
mod delta_query;
#[allow(dead_code)]
#[path = "support/delta_reads.rs"]
mod delta_reads;
#[allow(dead_code)]
#[path = "../../delta-arrow-reader-core/tests/reader/support.rs"]
mod reader_support;

use delta_query::QueryPath;
use delta_reads::{
    ParquetReadAction, ParquetReadControl, RecordingDeltaProvider, RecordingLocalStore, TestResult,
};
use reader_support::RealParquetDeltaTable;

const ROWS_PER_FILE: usize = 20_000;
const BATCH_SIZE: usize = 256;
// A deadlock guard only: passing assertions depend on acknowledgments and counters.
const DEADLINE: Duration = Duration::from_secs(60);

struct StreamingFixture {
    _table_files: RealParquetDeltaTable,
    context: SessionContext,
    store: Arc<RecordingLocalStore>,
    provider: Arc<RecordingDeltaProvider>,
    control: Arc<ParquetReadControl>,
    second_file: String,
}

impl StreamingFixture {
    async fn new(
        action: ParquetReadAction,
        session_target_partitions: usize,
        provider_target_partitions: Option<usize>,
        concurrent_file_limit: usize,
        runtime: Option<Arc<RuntimeEnv>>,
    ) -> TestResult<Self> {
        let table_files = RealParquetDeltaTable::new_with_two_large_files(
            "spark-stream-contract",
            ROWS_PER_FILE,
        )?;
        let second_file = "part-00002.parquet".to_string();
        let control = ParquetReadControl::new(action);
        let (uri, store) =
            RecordingLocalStore::register_with_control(table_files.path(), Some(control.clone()))?;
        let table = DeltaTableBuilder::new(uri)
            .with_warmup(WarmupMode::QueryPlanning)
            .load_table()
            .await?;
        let runtime = match runtime {
            Some(runtime) => runtime,
            None => RuntimeEnvBuilder::new().build_arc()?,
        };
        let context = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_runtime_env(runtime)
                    .with_config(
                        SessionConfig::new()
                            .with_target_partitions(session_target_partitions)
                            .with_batch_size(BATCH_SIZE)
                            .with_repartition_file_scans(false),
                    ),
            )
            .build(),
        );
        let execution_options = DeltaScanExecutionOptions::new()
            .with_prefetch_files_per_partition(0)
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(concurrent_file_limit))?
            .with_output_buffer_batches_per_partition(1)?;
        let provider = Arc::new(RecordingDeltaProvider {
            inner: DeltaTableProvider::try_new(
                table,
                ScanOptions {
                    target_partitions: provider_target_partitions,
                    execution_options,
                    use_arrow_view_types: false,
                    ..Default::default()
                },
            )?,
            calls: Default::default(),
            schema_calls: Default::default(),
        });
        context.register_table("source", provider.clone())?;
        assert_eq!(
            control.snapshot()?.requests_started,
            0,
            "loading the snapshot must be metadata-only"
        );
        Ok(Self {
            _table_files: table_files,
            context,
            store,
            provider,
            control,
            second_file,
        })
    }
}

async fn wait_for_ack(semaphore: &Semaphore) -> TestResult {
    tokio::time::timeout(DEADLINE, semaphore.acquire())
        .await??
        .forget();
    Ok(())
}

async fn wait_for_reads_to_finish(control: &ParquetReadControl) -> TestResult {
    tokio::time::timeout(DEADLINE, async {
        while control.snapshot()?.active_requests != 0 {
            wait_for_ack(&control.request_finished).await?;
        }
        Ok::<_, Box<dyn Error>>(())
    })
    .await??;
    let state = control.snapshot()?;
    assert_eq!(
        state.requests_started,
        state.requests_finished + state.requests_cancelled
    );
    Ok(())
}

fn ids(batch: &RecordBatch) -> TestResult<Vec<i32>> {
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("expected INT result")?;
    assert_eq!(values.null_count(), 0);
    Ok(values.values().to_vec())
}

async fn collect_ids(
    context: &SessionContext,
    plan: Arc<dyn ExecutionPlan>,
) -> TestResult<Vec<i32>> {
    let mut rows = vec![];
    for batch in collect(plan, context.task_ctx()).await? {
        rows.extend(ids(&batch)?);
    }
    rows.sort_unstable();
    Ok(rows)
}

fn assert_no_data(
    control: &ParquetReadControl,
    physical: Option<&dyn ExecutionPlan>,
) -> TestResult {
    assert_eq!(control.snapshot()?.requests_started, 0);
    if let Some(physical) = physical {
        let scans = collect_scan_metrics(physical);
        assert_eq!(scans.len(), 1);
        let reader = scans[0].snapshot().reader_metrics;
        assert_eq!(reader.file_tasks_started, 0);
        assert_eq!(reader.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(reader.parquet_data_file_bytes_received, Some(0));
    }
    Ok(())
}

#[tokio::test]
async fn planning_and_first_batches_leave_later_files_unread() -> TestResult {
    for path in QueryPath::ALL {
        for (sql, first_value, step, limited) in [
            ("SELECT id FROM source", 1, 1, false),
            ("SELECT id FROM source WHERE id % 2 = 0", 2, 2, false),
            (
                "SELECT id + CAST(1 AS INT) AS next_id FROM source",
                2,
                1,
                false,
            ),
            ("SELECT id FROM source LIMIT 10", 1, 1, true),
        ] {
            let fixture = StreamingFixture::new(
                ParquetReadAction::GateFile("part-00002.parquet".into()),
                1,
                None,
                1,
                None,
            )
            .await?;
            let physical = match path {
                QueryPath::Native => {
                    let query = fixture.context.sql(sql).await?;
                    assert_eq!(query.schema().fields().len(), 1);
                    assert_no_data(&fixture.control, None)?;
                    query.create_physical_plan().await?
                }
                QueryPath::Spark { ansi_mode } => {
                    let query = plan_query(
                        &fixture.context,
                        sql,
                        &SparkSqlOptions {
                            ansi_mode,
                            ..Default::default()
                        },
                    )
                    .await?;
                    assert_eq!(query.logical_plan.schema().fields().len(), 1);
                    assert_no_data(&fixture.control, None)?;
                    query.create_physical_plan(&fixture.context).await?
                }
            };
            assert_eq!(physical.schema().field(0).data_type(), &DataType::Int32);
            assert_no_data(&fixture.control, Some(physical.as_ref()))?;
            let scans = collect_scan_metrics(physical.as_ref());
            let mut stream = execute_stream(physical, fixture.context.task_ctx())?;
            let first = tokio::time::timeout(DEADLINE, stream.next())
                .await?
                .ok_or("missing first batch")??;
            let first_ids = ids(&first)?;
            assert!(!first_ids.is_empty());
            assert!(first_ids.len() <= BATCH_SIZE);
            assert_eq!(
                first_ids,
                (0..first_ids.len())
                    .map(|index| first_value + index as i32 * step)
                    .collect::<Vec<_>>()
            );
            assert!(
                !fixture
                    .control
                    .snapshot()?
                    .files_started
                    .contains(&fixture.second_file)
            );
            assert!(
                scans[0].snapshot().reader_metrics.scheduler_rows_emitted
                    < (2 * ROWS_PER_FILE) as u64
            );
            if limited {
                let mut limited_ids = first_ids;
                while let Some(batch) = tokio::time::timeout(DEADLINE, stream.next()).await? {
                    limited_ids.extend(ids(&batch?)?);
                }
                assert_eq!(limited_ids, (1..=10).collect::<Vec<_>>());
                assert_eq!(scans[0].snapshot().reader_metrics.file_tasks_started, 1);
                assert!(
                    !fixture
                        .control
                        .snapshot()?
                        .files_started
                        .contains(&fixture.second_file)
                );
            }
            drop(stream);
            wait_for_reads_to_finish(&fixture.control).await?;
            assert!(
                !fixture
                    .control
                    .snapshot()?
                    .files_started
                    .contains(&fixture.second_file)
            );
            assert!(
                !fixture
                    .store
                    .reads
                    .lock()
                    .map_err(|_| "read log poisoned")?
                    .is_empty()
            );
            println!(
                "DELTA_STREAM_FIRST_BATCH path={path:?} sql={sql:?} rows={} limited={limited}",
                first.num_rows()
            );
        }
    }
    Ok(())
}

async fn drain_to_blocked_file(
    stream: &mut SendableRecordBatchStream,
    control: &ParquetReadControl,
    file: &str,
) -> TestResult<Vec<i32>> {
    tokio::time::timeout(DEADLINE, async {
        let mut rows = vec![];
        loop {
            if control.snapshot()?.files_started.contains(file) {
                return Ok(rows);
            }
            tokio::select! {
                item = stream.next() => rows.extend(ids(&item.ok_or("EOF before gated file")??)?),
                permit = control.request_started.acquire() => permit?.forget(),
            }
        }
    })
    .await?
}

#[tokio::test]
async fn stream_drop_releases_reads_and_same_plan_execution_permits() -> TestResult {
    for path in QueryPath::ALL {
        let fixture = StreamingFixture::new(
            ParquetReadAction::GateFile("part-00002.parquet".into()),
            1,
            None,
            1,
            None,
        )
        .await?;
        let physical = path.plan(&fixture.context, "SELECT id FROM source").await?;
        let mut first = execute_stream(physical.clone(), fixture.context.task_ctx())?;
        assert_eq!(
            ids(&first.next().await.ok_or("missing first execution")??)?[0],
            1
        );
        let mut second = execute_stream(physical.clone(), fixture.context.task_ctx())?;
        drop(first);
        let batch = tokio::time::timeout(DEADLINE, second.next())
            .await?
            .ok_or("dropping first cancelled second")??;
        assert_eq!(ids(&batch)?[0], 1);
        let delivered =
            drain_to_blocked_file(&mut second, &fixture.control, &fixture.second_file).await?;
        assert!(batch.num_rows() + delivered.len() < 2 * ROWS_PER_FILE);
        assert!(fixture.control.snapshot()?.active_requests > 0);
        drop(second);
        // Completion is acknowledged by the dropped request future, not a sleep.
        wait_for_reads_to_finish(&fixture.control).await?;
        let stopped = fixture.control.snapshot()?;
        assert!(stopped.requests_cancelled > 0);
        fixture.control.release_all();
        assert_eq!(
            tokio::time::timeout(DEADLINE, collect_ids(&fixture.context, physical)).await??,
            (1..=2 * ROWS_PER_FILE as i32).collect::<Vec<_>>()
        );
        wait_for_reads_to_finish(&fixture.control).await?;
        let weak = Arc::downgrade(&fixture.store);
        let control = fixture.control.clone();
        drop(fixture);
        wait_for_ack(&control.store_dropped).await?;
        assert!(weak.upgrade().is_none());
        println!(
            "DELTA_STREAM_DROP path={path:?} cancelled_reads={} same_plan_reexecution=complete store_released=true",
            stopped.requests_cancelled
        );
    }
    Ok(())
}

fn source_contains(error: &(dyn Error + 'static), text: &str) -> bool {
    let mut source = Some(error);
    while let Some(error) = source {
        if error.to_string().contains(text) {
            return true;
        }
        source = error.source();
    }
    false
}

#[tokio::test]
async fn injected_late_read_error_follows_delivered_batches_and_allows_retry() -> TestResult {
    for path in QueryPath::ALL {
        let fixture = StreamingFixture::new(
            ParquetReadAction::FailFileOnce("part-00002.parquet".into()),
            1,
            None,
            1,
            None,
        )
        .await?;
        let physical = path.plan(&fixture.context, "SELECT id FROM source").await?;
        let mut stream = execute_stream(physical.clone(), fixture.context.task_ctx())?;
        let mut rows = vec![];
        let mut batches = 0;
        let error = loop {
            match tokio::time::timeout(DEADLINE, stream.next())
                .await?
                .ok_or("silently truncated late failure")?
            {
                Ok(batch) => {
                    rows.extend(ids(&batch)?);
                    batches += 1;
                }
                Err(error) => break error,
            }
        };
        assert!(batches > 1);
        assert_eq!(rows, (1..=ROWS_PER_FILE as i32).collect::<Vec<_>>());
        assert!(source_contains(
            &error,
            "injected late Parquet read failure"
        ));
        assert!(stream.next().await.is_none());
        drop(stream);
        wait_for_reads_to_finish(&fixture.control).await?;
        assert_eq!(fixture.control.snapshot()?.files_started.len(), 2);
        assert_eq!(
            tokio::time::timeout(DEADLINE, collect_ids(&fixture.context, physical)).await??,
            (1..=2 * ROWS_PER_FILE as i32).collect::<Vec<_>>()
        );
        println!(
            "DELTA_STREAM_FAILURE path={path:?} delivered_rows={} delivered_batches={batches} retry=complete",
            rows.len()
        );
    }
    Ok(())
}

#[tokio::test]
async fn host_file_read_concurrency_caps_have_positive_overlap_controls() -> TestResult {
    for path in QueryPath::ALL {
        // Explicit targets keep the overlap control independent of automatic host caps.
        for (session_target, provider_target, concurrent_file_limit) in
            [(1, None, 2), (2, None, 2), (2, Some(2), 1), (2, Some(2), 2)]
        {
            let fixture = StreamingFixture::new(
                ParquetReadAction::GateAll,
                session_target,
                provider_target,
                concurrent_file_limit,
                None,
            )
            .await?;
            assert_eq!(
                fixture.context.state().config().target_partitions(),
                session_target
            );
            let physical = path.plan(&fixture.context, "SELECT id FROM source").await?;
            let scans = collect_scan_metrics(physical.as_ref());
            assert_eq!(scans.len(), 1);
            let planned_partitions =
                scans[0].snapshot().reader_metrics.scan_partitions_planned as usize;
            if let Some(target) = provider_target {
                assert_eq!(planned_partitions, target.min(2));
            } else {
                assert!(
                    (1..=session_target.min(2)).contains(&planned_partitions),
                    "actual scan exceeded the session cap"
                );
            }
            {
                let calls = fixture
                    .provider
                    .calls
                    .lock()
                    .map_err(|_| "provider call lock poisoned")?;
                assert!(!calls.scans.is_empty());
                assert!(
                    calls
                        .scans
                        .iter()
                        .all(|scan| scan.session_target_partitions == session_target),
                    "session cap was not forwarded to the real Delta provider"
                );
            }
            let expected_peak = planned_partitions.min(concurrent_file_limit);
            if provider_target == Some(2) && concurrent_file_limit == 2 {
                assert_eq!(expected_peak, 2);
            }
            let task_context = fixture.context.task_ctx();
            let pending = tokio::spawn(async move { collect(physical, task_context).await });
            tokio::time::timeout(DEADLINE, async {
                while fixture.control.snapshot()?.peak_active_files < expected_peak {
                    wait_for_ack(&fixture.control.request_started).await?;
                }
                Ok::<_, Box<dyn Error>>(())
            })
            .await??;
            let blocked = fixture.control.snapshot()?;
            assert_eq!(
                blocked.peak_active_files, expected_peak,
                "must exercise the configured capacity"
            );
            assert_eq!(blocked.files_started.len(), expected_peak);
            fixture.control.release_all();
            let batches = tokio::time::timeout(DEADLINE, pending).await???;
            let mut actual = vec![];
            for batch in batches {
                actual.extend(ids(&batch)?);
            }
            actual.sort_unstable();
            assert_eq!(actual, (1..=2 * ROWS_PER_FILE as i32).collect::<Vec<_>>());
            wait_for_reads_to_finish(&fixture.control).await?;
            assert_eq!(fixture.control.snapshot()?.peak_active_files, expected_peak);
            assert_eq!(
                scans[0].snapshot().reader_metrics.scan_partitions_started,
                planned_partitions as u64
            );
            println!(
                "DELTA_STREAM_CONCURRENCY path={path:?} session_target={session_target} provider_target={provider_target:?} file_limit={concurrent_file_limit} planned_partitions={planned_partitions} observed_peak={}",
                blocked.peak_active_files
            );
        }
    }
    Ok(())
}

#[derive(Debug)]
struct RecordingMemoryPool {
    inner: GreedyMemoryPool,
    peak_bytes: AtomicUsize,
    rejected_requests: AtomicUsize,
    released: Semaphore,
}

impl RecordingMemoryPool {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: GreedyMemoryPool::new(limit),
            peak_bytes: AtomicUsize::new(0),
            rejected_requests: AtomicUsize::new(0),
            released: Semaphore::new(0),
        })
    }

    fn runtime(self: &Arc<Self>) -> DataFusionResult<Arc<RuntimeEnv>> {
        RuntimeEnvBuilder::new()
            .with_memory_pool(self.clone())
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()
    }

    async fn wait_released(&self) -> TestResult {
        while self.reserved() > 0 {
            wait_for_ack(&self.released).await?;
        }
        Ok(())
    }
}

impl fmt::Display for RecordingMemoryPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(formatter)
    }
}

impl MemoryPool for RecordingMemoryPool {
    fn name(&self) -> &str {
        "recording-host-memory-pool"
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.peak_bytes
            .fetch_max(self.reserved(), Ordering::Relaxed);
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.released.add_permits(1);
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DataFusionResult<()> {
        let result = self.inner.try_grow(reservation, additional);
        if result.is_err() {
            self.rejected_requests.fetch_add(1, Ordering::Relaxed);
        }
        self.peak_bytes
            .fetch_max(self.reserved(), Ordering::Relaxed);
        result
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

const AGGREGATE: &str = "SELECT id, count(*) AS n FROM source GROUP BY id";

fn aggregate_rows(batches: Vec<RecordBatch>) -> TestResult<Vec<(i32, i64)>> {
    let mut rows = vec![];
    for batch in batches {
        assert_eq!(batch.schema().fields().len(), 2);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("expected BIGINT count")?;
        assert_eq!(counts.null_count(), 0);
        rows.extend(
            ids(&batch)?
                .into_iter()
                .zip(counts.values().iter().copied()),
        );
    }
    rows.sort_unstable();
    Ok(rows)
}

#[tokio::test]
async fn operators_keep_the_host_memory_pool_and_enforce_its_limit() -> TestResult {
    for path in QueryPath::ALL {
        for (limit, expect_error) in [(64 * 1024, true), (64 * 1024 * 1024, false)] {
            let pool = RecordingMemoryPool::new(limit);
            let runtime = pool.runtime()?;
            let fixture = StreamingFixture::new(
                ParquetReadAction::Observe,
                1,
                None,
                1,
                Some(runtime.clone()),
            )
            .await?;
            assert!(Arc::ptr_eq(&fixture.context.runtime_env(), &runtime));
            let erased_pool: Arc<dyn MemoryPool> = pool.clone();
            assert!(Arc::ptr_eq(&runtime.memory_pool, &erased_pool));
            let physical = path.plan(&fixture.context, AGGREGATE).await?;
            let result = collect(physical, fixture.context.task_ctx()).await;
            if expect_error {
                assert!(result.is_err(), "operator bypassed host memory limit");
                assert!(pool.rejected_requests.load(Ordering::Relaxed) > 0);
            } else {
                assert_eq!(
                    aggregate_rows(result?)?,
                    (1..=2 * ROWS_PER_FILE as i32)
                        .map(|id| (id, 1))
                        .collect::<Vec<_>>()
                );
                assert!(pool.peak_bytes.load(Ordering::Relaxed) > 0);
                assert_eq!(pool.rejected_requests.load(Ordering::Relaxed), 0);
            }
            pool.wait_released().await?;
            assert_eq!(pool.reserved(), 0);
            assert!(pool.peak_bytes.load(Ordering::Relaxed) <= limit);
            println!(
                "DELTA_STREAM_MEMORY path={path:?} limit={limit} rejected={} peak_bytes={} released=true",
                pool.rejected_requests.load(Ordering::Relaxed),
                pool.peak_bytes.load(Ordering::Relaxed)
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn aborting_a_task_releases_blocked_reads_memory_and_same_plan_permits() -> TestResult {
    for path in QueryPath::ALL {
        let pool = RecordingMemoryPool::new(64 * 1024 * 1024);
        let fixture = StreamingFixture::new(
            ParquetReadAction::GateFile("part-00002.parquet".into()),
            1,
            None,
            1,
            Some(pool.runtime()?),
        )
        .await?;
        let physical = path.plan(&fixture.context, AGGREGATE).await?;
        let plan = physical.clone();
        let task_context = fixture.context.task_ctx();
        let pending = tokio::spawn(async move { collect(plan, task_context).await });
        tokio::time::timeout(DEADLINE, async {
            while !fixture
                .control
                .snapshot()?
                .files_started
                .contains(&fixture.second_file)
            {
                wait_for_ack(&fixture.control.request_started).await?;
            }
            Ok::<_, Box<dyn Error>>(())
        })
        .await??;
        assert!(
            pool.reserved() > 0,
            "aggregate must hold real host-pool memory before cancellation"
        );
        assert!(fixture.control.snapshot()?.active_requests > 0);
        pending.abort();
        assert!(pending.await.is_err_and(|error| error.is_cancelled()));
        wait_for_reads_to_finish(&fixture.control).await?;
        pool.wait_released().await?;
        assert!(fixture.control.snapshot()?.requests_cancelled > 0);
        assert_eq!(pool.reserved(), 0);
        fixture.control.release_all();
        let retried =
            tokio::time::timeout(DEADLINE, collect(physical, fixture.context.task_ctx())).await??;
        assert_eq!(
            aggregate_rows(retried)?,
            (1..=2 * ROWS_PER_FILE as i32)
                .map(|id| (id, 1))
                .collect::<Vec<_>>()
        );
        pool.wait_released().await?;
        assert_eq!(pool.reserved(), 0);
        println!(
            "DELTA_STREAM_ABORT path={path:?} blocked_read_cancelled=true pool_released=true same_plan_reexecution=complete"
        );
    }
    Ok(())
}
