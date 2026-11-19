//! Compare equivalent native and Spark query costs over the frozen local Delta snapshot.

use std::{
    collections::BTreeSet,
    fmt, fs,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use arrow::{
    array::{ArrayRef, Int32Array, Int64Array, StringArray, StringViewArray},
    compute::cast,
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::view::ViewTable,
    common::Result as DataFusionResult,
    execution::{
        SessionStateBuilder,
        memory_pool::{
            GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
        },
        runtime_env::RuntimeEnvBuilder,
    },
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, coalesce_partitions::CoalescePartitionsExec,
        displayable, execute_stream, sorts::sort::SortExec,
    },
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader::{
    DeltaTableBuilder, WarmupMode,
    datafusion::{DeltaTableProvider, ScanOptions, collect_scan_metrics},
};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use futures_util::TryStreamExt;
use serde::Deserialize;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "../tests/support/delta_reads.rs"]
mod delta_reads;
#[allow(dead_code)]
#[path = "../tests/support/spark_results.rs"]
mod spark_results;
use delta_reads::{ParquetReadAction, ParquetReadControl, RecordingLocalStore};
use spark_results::TestResult;

#[derive(Deserialize)]
struct Query {
    id: String,
    native_sql: String,
    spark_sql: String,
    ordered: bool,
}
#[derive(Deserialize)]
struct Manifest {
    source_commit: String,
    common_view_sql: String,
    queries: Vec<Query>,
}
#[derive(Deserialize)]
struct Schedule {
    target_partitions: usize,
    batch_rows: usize,
    runtime_workers: usize,
    warmups: usize,
    samples: usize,
    single_pool_bytes: usize,
    preflight_pool_bytes: usize,
    concurrency_pool_bytes: usize,
    spill_limit_bytes: u64,
    profile_iterations: usize,
}

#[derive(Debug)]
struct PeakMemoryPool {
    inner: GreedyMemoryPool,
    updates: Mutex<()>,
    peak: AtomicUsize,
}
impl PeakMemoryPool {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: GreedyMemoryPool::new(limit),
            updates: Mutex::new(()),
            peak: AtomicUsize::new(0),
        })
    }
    fn reset_peak(&self) -> TestResult {
        let _guard = self
            .updates
            .lock()
            .map_err(|_| "memory tracker lock poisoned")?;
        if self.inner.reserved() != 0 {
            return Err("previous query still holds a memory reservation".into());
        }
        self.peak.store(0, Ordering::Relaxed);
        Ok(())
    }
    fn record_peak(&self) {
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
    }
}
impl fmt::Display for PeakMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}
impl MemoryPool for PeakMemoryPool {
    fn name(&self) -> &str {
        "execution-benchmark-peak-pool"
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        // Serialize updates so a concurrent shrink cannot hide a short reservation peak.
        let _guard = self
            .updates
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.inner.grow(reservation, additional);
        self.record_peak();
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DataFusionResult<()> {
        let _guard = self
            .updates
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let result = self.inner.try_grow(reservation, additional);
        self.record_peak();
        result
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let _guard = self
            .updates
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.inner.shrink(reservation, shrink);
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(target_os = "linux")]
fn process_memory_bytes(key: &str) -> TestResult<Option<u64>> {
    let status = fs::read_to_string("/proc/self/status")?;
    let line = status
        .lines()
        .find(|line| line.starts_with(key))
        .ok_or("missing Linux process memory field")?;
    let kib: u64 = line
        .split_whitespace()
        .nth(1)
        .ok_or("missing Linux memory value")?
        .parse()?;
    Ok(Some(kib * 1024))
}
#[cfg(not(target_os = "linux"))]
fn process_memory_bytes(_key: &str) -> TestResult<Option<u64>> {
    Ok(None)
}

struct RegisteredSession {
    context: SessionContext,
    store: Arc<RecordingLocalStore>,
    pool: Arc<PeakMemoryPool>,
    reads: Arc<ParquetReadControl>,
}
async fn build_registered_session(
    manifest: &Manifest,
    schedule: &Schedule,
    snapshot: &Path,
    work: &Path,
    pool_limit: usize,
) -> TestResult<RegisteredSession> {
    fs::create_dir_all(work.join("spill"))?;
    let pool = PeakMemoryPool::new(pool_limit);
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .with_temp_file_path(work.join("spill"))
        .with_max_temp_directory_size(schedule.spill_limit_bytes)
        .build_arc()?;
    let context = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(schedule.target_partitions)
                        .with_batch_size(schedule.batch_rows),
                )
                .with_runtime_env(runtime),
        )
        .build(),
    );
    let reads = ParquetReadControl::new(ParquetReadAction::Observe);
    let (uri, store) = RecordingLocalStore::register_with_control(snapshot, Some(reads.clone()))
        .map_err(|error| error.to_string())?;
    let table = DeltaTableBuilder::new(uri)
        .with_warmup(WarmupMode::QueryPlanning)
        .load_table()
        .await?;
    if table.version() != 0 {
        return Err("expected frozen snapshot version zero".into());
    }
    context.register_table(
        "raw_lineitem",
        Arc::new(DeltaTableProvider::try_new(
            table,
            ScanOptions {
                target_partitions: Some(schedule.target_partitions),
                ..Default::default()
            },
        )?),
    )?;
    let common = context
        .sql(&manifest.common_view_sql)
        .await?
        .into_unoptimized_plan();
    context.register_table(
        "bench",
        Arc::new(ViewTable::new(
            common,
            Some(manifest.common_view_sql.clone()),
        )),
    )?;
    let dimension = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("suppkey", DataType::Int64, false),
            Field::new("region", DataType::Int32, false),
            Field::new("label", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(1..=10000)),
            Arc::new(Int32Array::from_iter_values((1..=10000).map(|key| key % 5))),
            Arc::new(StringArray::from_iter_values(
                (1..=10000).map(|key| format!("supplier-{key}")),
            )),
        ],
    )?;
    context.register_batch("suppliers", dimension)?;
    Ok(RegisteredSession {
        context,
        store,
        pool,
        reads,
    })
}

fn options(configuration: &str) -> SparkSqlOptions {
    SparkSqlOptions {
        ansi_mode: configuration == "spark-on",
        session_timezone: "UTC".into(),
        ..Default::default()
    }
}
async fn create_query_plan(
    session: &SessionContext,
    query: &Query,
    configuration: &str,
) -> TestResult<Arc<dyn ExecutionPlan>> {
    Ok(if configuration == "native" {
        session
            .sql(&query.native_sql)
            .await?
            .create_physical_plan()
            .await?
    } else {
        plan_query(session, &query.spark_sql, &options(configuration))
            .await?
            .create_physical_plan(session)
            .await?
    })
}
fn clear_source_reads(session: &RegisteredSession) -> TestResult {
    if session
        .reads
        .snapshot()
        .map_err(|error| error.to_string())?
        .active_requests
        != 0
    {
        return Err("previous source requests are still active before recorder reset".into());
    }
    session
        .store
        .reads
        .lock()
        .map_err(|_| "source recorder lock poisoned")?
        .clear();
    session.store.read_calls.store(0, Ordering::Relaxed);
    Ok(())
}
fn source_evidence(
    plan: &dyn ExecutionPlan,
    store: &RecordingLocalStore,
    require_complete: bool,
) -> TestResult<Value> {
    let reads = store
        .reads
        .lock()
        .map_err(|_| "source recorder lock poisoned")?;
    let files = reads
        .iter()
        .map(|read| read.path.clone())
        .collect::<BTreeSet<_>>();
    let range_bytes = reads
        .iter()
        .map(|read| read.range.end - read.range.start)
        .sum::<u64>();
    let scans = collect_scan_metrics(plan);
    let snapshots = scans
        .iter()
        .map(|scan| scan.snapshot().reader_metrics)
        .collect::<Vec<_>>();
    if snapshots
        .iter()
        .any(|metrics| metrics.snapshot_version != 0)
    {
        return Err("scan snapshot changed".into());
    }
    let counters_complete = snapshots.iter().all(|metrics| {
        metrics.scan_partitions_started == metrics.scan_partitions_completed
            && metrics.file_tasks_started == metrics.file_tasks_completed
    });
    if require_complete && !counters_complete {
        return Err(format!(
            "full-input scan counters are not final after stream completion: {snapshots:?}"
        )
        .into());
    }
    let received = snapshots.iter().try_fold(0_u64, |sum, metrics| {
        metrics
            .parquet_data_file_bytes_received
            .map(|value| sum + value)
            .ok_or("missing direct reader byte metric")
    })?;
    let range_gets = snapshots.iter().try_fold(0_u64, |sum, metrics| {
        metrics
            .parquet_data_file_range_get_operations
            .map(|value| sum + value)
            .ok_or("missing direct reader range GET metric")
    })?;
    let full_gets = snapshots.iter().try_fold(0_u64, |sum, metrics| {
        metrics
            .parquet_data_file_full_get_operations
            .map(|value| sum + value)
            .ok_or("missing direct reader full GET metric")
    })?;
    Ok(
        json!({"distinct_source_files":files,"source_file_count":files.len(),"recorded_range_bytes":range_bytes,
        "source_bytes_received":received,
        "source_range_gets":range_gets,
        "source_full_gets":full_gets,
        "file_tasks_started":snapshots.iter().map(|m|m.file_tasks_started).sum::<u64>(),
        "file_tasks_completed":snapshots.iter().map(|m|m.file_tasks_completed).sum::<u64>(),
        "scan_partitions_started":snapshots.iter().map(|m|m.scan_partitions_started).sum::<u64>(),
        "scan_partitions_completed":snapshots.iter().map(|m|m.scan_partitions_completed).sum::<u64>(),
        "counters_complete":counters_complete,
        "files_planned":snapshots.iter().map(|m|m.files_planned).sum::<u64>(),
        "store_read_calls":store.read_calls.load(Ordering::Relaxed),
        "ranges":reads.iter().map(|read|json!({"path":read.path,"start":read.range.start,"end":read.range.end})).collect::<Vec<_>>(),
        "reader_metrics":snapshots.iter().map(|metrics|format!("{metrics:?}")).collect::<Vec<_>>()}),
    )
}
fn operator_metrics(plan: &dyn ExecutionPlan) -> Vec<Value> {
    let mut metrics =
        vec![json!({"operator":plan.name(),"metrics":format!("{:?}",plan.metrics())})];
    for child in plan.children() {
        metrics.extend(operator_metrics(child.as_ref()));
    }
    metrics
}
fn canonical_schema(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| {
                Field::new(
                    field.name(),
                    match field.data_type() {
                        DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
                        other => other.clone(),
                    },
                    true,
                )
            })
            .collect::<Vec<_>>(),
    ))
}
fn canonical_batch(batch: RecordBatch) -> TestResult<RecordBatch> {
    let schema = canonical_schema(batch.schema().as_ref());
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| {
            if array.data_type() == field.data_type() {
                Ok(array.clone())
            } else {
                cast(array, field.data_type())
            }
        })
        .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn compare_result_rows(
    left_schema: SchemaRef,
    left: impl Iterator<Item = Result<RecordBatch, ArrowError>>,
    right_schema: SchemaRef,
    right: impl Iterator<Item = Result<RecordBatch, ArrowError>>,
) -> TestResult<usize> {
    if canonical_schema(&left_schema) != canonical_schema(&right_schema) {
        return Err("executed SQL schemas differ".into());
    }
    let mut left = left.map(|batch| canonical_batch(batch?));
    let mut right = right.map(|batch| canonical_batch(batch?));
    let (mut a, mut b) = (None::<RecordBatch>, None::<RecordBatch>);
    let (mut ai, mut bi, mut rows) = (0, 0, 0);
    loop {
        if a.as_ref().is_none_or(|batch| ai == batch.num_rows()) {
            a = left
                .find_map(|batch| match batch {
                    Ok(batch) if batch.num_rows() == 0 => None,
                    other => Some(other),
                })
                .transpose()?;
            ai = 0;
        }
        if b.as_ref().is_none_or(|batch| bi == batch.num_rows()) {
            b = right
                .find_map(|batch| match batch {
                    Ok(batch) if batch.num_rows() == 0 => None,
                    other => Some(other),
                })
                .transpose()?;
            bi = 0;
        }
        match (&a, &b) {
            (None, None) => return Ok(rows),
            (Some(a), Some(b)) => {
                let count = (a.num_rows() - ai).min(b.num_rows() - bi);
                if a.slice(ai, count) != b.slice(bi, count) {
                    return Err(format!("result values/types differ at or after row {rows}").into());
                }
                ai += count;
                bi += count;
                rows += count;
            }
            _ => {
                return Err(
                    format!("result row count/multiplicity differs after {rows} rows").into(),
                );
            }
        }
    }
}
fn compare_ipc(left: &Path, right: &Path) -> TestResult<Value> {
    let a = StreamReader::try_new(fs::File::open(left)?, None)?;
    let b = StreamReader::try_new(fs::File::open(right)?, None)?;
    let schema = canonical_schema(a.schema().as_ref());
    let rows = compare_result_rows(a.schema(), a, b.schema(), b)?;
    Ok(json!({"rows_compared":rows,"canonical_arrow_schema":format!("{schema:?}"),"equal":true}))
}

fn run_instrumentation_self_checks() -> TestResult {
    let make = |values: Vec<i64>, view: bool| -> TestResult<RecordBatch> {
        let labels = values
            .iter()
            .map(|value| format!("row-{value}"))
            .collect::<Vec<_>>();
        let strings: ArrayRef = if view {
            Arc::new(StringViewArray::from_iter_values(labels))
        } else {
            Arc::new(StringArray::from_iter_values(labels))
        };
        Ok(RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(values)) as ArrayRef),
            ("label", strings),
        ])?)
    };
    let base = make(vec![1, 2, 2], false)?;
    let uneven = vec![make(vec![1], true)?, make(vec![2, 2], true)?];
    assert_eq!(
        compare_result_rows(
            base.schema(),
            vec![Ok(base.clone())].into_iter(),
            uneven[0].schema(),
            uneven.into_iter().map(Ok)
        )?,
        3
    );
    for invalid in [
        make(vec![1, 2, 3], false)?,
        make(vec![1, 2], false)?,
        make(vec![1, 2, 2, 2], false)?,
    ] {
        assert!(
            compare_result_rows(
                base.schema(),
                vec![Ok(base.clone())].into_iter(),
                invalid.schema(),
                vec![Ok(invalid)].into_iter()
            )
            .is_err()
        );
    }
    let nulls = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from(vec![None, Some(1), Some(1)])) as ArrayRef,
        ),
        (
            "label",
            Arc::new(StringArray::from(vec![
                Some("null-key"),
                None,
                Some("row-1"),
            ])),
        ),
    ])?;
    let null_views = RecordBatch::try_from_iter(vec![
        ("id", nulls.column(0).clone()),
        (
            "label",
            Arc::new(StringViewArray::from(vec![
                Some("null-key"),
                None,
                Some("row-1"),
            ])) as ArrayRef,
        ),
    ])?;
    assert_eq!(
        compare_result_rows(
            nulls.schema(),
            vec![Ok(nulls.clone())].into_iter(),
            null_views.schema(),
            vec![Ok(null_views.slice(0, 1)), Ok(null_views.slice(1, 2))].into_iter()
        )?,
        3
    );
    let changed_null = RecordBatch::try_from_iter(vec![
        ("id", nulls.column(0).clone()),
        (
            "label",
            Arc::new(StringViewArray::from(vec![
                Some("null-key"),
                Some("changed"),
                Some("row-1"),
            ])) as ArrayRef,
        ),
    ])?;
    assert!(
        compare_result_rows(
            nulls.schema(),
            vec![Ok(nulls)].into_iter(),
            changed_null.schema(),
            vec![Ok(changed_null)].into_iter()
        )
        .is_err()
    );
    let wrong_type = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2, 2])) as ArrayRef),
        ("label", base.column(1).clone()),
    ])?;
    assert!(
        compare_result_rows(
            base.schema(),
            vec![Ok(base)].into_iter(),
            wrong_type.schema(),
            vec![Ok(wrong_type)].into_iter()
        )
        .is_err()
    );
    let pool = PeakMemoryPool::new(16384);
    let dynamic: Arc<dyn MemoryPool> = pool.clone();
    let a = MemoryConsumer::new("self-check-a").register(&dynamic);
    let b = MemoryConsumer::new("self-check-b").register(&dynamic);
    a.try_grow(4096)?;
    b.try_grow(8192)?;
    a.shrink(1024);
    assert_eq!(pool.peak.load(Ordering::Relaxed), 12288);
    assert_eq!(pool.reserved(), 11264);
    assert!(b.try_grow(16384).is_err());
    assert_eq!(pool.peak.load(Ordering::Relaxed), 12288);
    drop(a);
    drop(b);
    pool.reset_peak()?;
    assert_eq!(pool.peak.load(Ordering::Relaxed), 0);
    Ok(())
}

async fn export_preflight_result(
    session: &RegisteredSession,
    query: &Query,
    configuration: &str,
    work: &Path,
) -> TestResult<Value> {
    clear_source_reads(session)?;
    session.pool.reset_peak()?;
    let plan = create_query_plan(&session.context, query, configuration).await?;
    let raw_plan = displayable(plan.as_ref()).indent(true).to_string();
    let partitions = plan.output_partitioning().partition_count();
    let actual_schema = plan.schema();
    let canonical = canonical_schema(actual_schema.as_ref());
    let ordering = LexOrdering::new((0..actual_schema.fields().len()).map(|index| {
        PhysicalSortExpr::new_default(Arc::new(Column::new(
            actual_schema.field(index).name(),
            index,
        )))
    }))
    .ok_or("empty result schema")?;
    let exported: Arc<dyn ExecutionPlan> = if query.ordered {
        plan.clone()
    } else {
        Arc::new(SortExec::new(
            ordering,
            Arc::new(CoalescePartitionsExec::new(plan.clone())),
        ))
    };
    let output = work.join("result.arrow");
    let mut writer = StreamWriter::try_new(fs::File::create(&output)?, canonical.as_ref())?;
    let mut stream = execute_stream(exported, session.context.task_ctx())?;
    let mut rows = 0usize;
    while let Some(batch) = stream.try_next().await? {
        spark_results::validate_batch(actual_schema.as_ref(), &batch)?;
        rows += batch.num_rows();
        writer.write(&canonical_batch(batch)?)?;
    }
    writer.finish()?;
    drop(writer);
    drop(stream);
    if session
        .reads
        .snapshot()
        .map_err(|error| error.to_string())?
        .active_requests
        != 0
    {
        return Err("source requests are still active after preflight stream completion".into());
    }
    fs::write(work.join("physical-plan.txt"), &raw_plan)?;
    let mut sql_schema = spark_results::capture_schema(actual_schema.fields(), None)?;
    spark_results::remove_nullability_and_metadata(&mut sql_schema);
    Ok(
        json!({"kind":"preflight_result","id":query.id,"configuration":configuration,"rows":rows,"sql_schema":sql_schema,"arrow_types":actual_schema.fields().iter().map(|field|field.data_type().to_string()).collect::<Vec<_>>(),"output_partitions":partitions,"ipc_path":output,"pool_peak_bytes":session.pool.peak.load(Ordering::Relaxed),"source_io":source_evidence(plan.as_ref(),&session.store,query.id!="ordered_limit")?,"operator_metrics":operator_metrics(plan.as_ref()),"physical_plan":raw_plan}),
    )
}

async fn drain_execution(
    plan: Arc<dyn ExecutionPlan>,
    context: &SessionContext,
    started: Instant,
) -> TestResult<(usize, Option<(u128, usize)>, u128)> {
    let mut stream = execute_stream(plan, context.task_ctx())?;
    let (mut rows, mut first) = (0usize, None);
    while let Some(batch) = stream.try_next().await? {
        if first.is_none() {
            first = Some((started.elapsed().as_nanos(), batch.num_rows()));
        }
        rows += batch.num_rows();
    }
    let complete = started.elapsed().as_nanos();
    drop(stream);
    Ok((rows, first, complete))
}
async fn measure_query(
    session: &RegisteredSession,
    query: &Query,
    configuration: &str,
) -> TestResult<Value> {
    clear_source_reads(session)?;
    session.pool.reset_peak()?;
    let query_started = Instant::now();
    let plan = create_query_plan(&session.context, query, configuration).await?;
    let planning_ns = query_started.elapsed().as_nanos();
    let execution_started = Instant::now();
    let execution_offset_ns = execution_started.duration_since(query_started).as_nanos();
    let (rows, first, execution_complete_ns) =
        drain_execution(plan.clone(), &session.context, execution_started).await?;
    let query_complete_ns = execution_offset_ns + execution_complete_ns;
    if session
        .reads
        .snapshot()
        .map_err(|error| error.to_string())?
        .active_requests
        != 0
    {
        return Err("source requests are still active after stream completion".into());
    }
    let io = source_evidence(plan.as_ref(), &session.store, query.id != "ordered_limit")?;
    let result = json!({"planning_ns":planning_ns,"execution_first_batch_ns":first.map(|value|value.0),"query_first_batch_ns":first.map(|value|execution_offset_ns+value.0),"first_batch_rows":first.map(|value|value.1),"execution_complete_ns":execution_complete_ns,"query_complete_ns":query_complete_ns,"rows":rows,"peak_pool_bytes":session.pool.peak.load(Ordering::Relaxed),"output_partitions":plan.output_partitioning().partition_count(),"source_io":io,"operator_metrics":operator_metrics(plan.as_ref())});
    drop(plan);
    Ok(result)
}

async fn measure_process(
    session: &RegisteredSession,
    manifest: &Manifest,
    schedule: &Schedule,
    query: &Query,
    configuration: &str,
) -> TestResult {
    let setup_rss = process_memory_bytes("VmRSS:")?;
    let mut expected = None;
    let mut samples = Vec::new();
    for index in 0..schedule.warmups + schedule.samples {
        let sample = measure_query(session, query, configuration).await?;
        if let Some(rows) = expected.as_ref() {
            if sample["rows"] != *rows {
                return Err("row count changed across iterations".into());
            }
        } else {
            expected = Some(sample["rows"].clone());
        }
        if index >= schedule.warmups {
            samples.push(sample);
        }
    }
    println!(
        "{}",
        json!({"kind":"query_samples","source_commit":manifest.source_commit,"id":query.id,"configuration":configuration,"setup_rss_bytes":setup_rss,"process_peak_rss_bytes":process_memory_bytes("VmHWM:")?,"samples":samples})
    );
    Ok(())
}

async fn concurrency_check(
    session: &RegisteredSession,
    query: &Query,
    configuration: &str,
) -> TestResult {
    let mut plans = Vec::new();
    for _ in 0..4 {
        plans.push(create_query_plan(&session.context, query, configuration).await?);
    }
    clear_source_reads(session)?;
    session.pool.reset_peak()?;
    let barrier = Arc::new(tokio::sync::Barrier::new(5));
    let mut tasks = Vec::new();
    for plan in plans {
        let context = session.context.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            drain_execution(plan, &context, Instant::now()).await
        }));
    }
    let started = Instant::now();
    barrier.wait().await;
    let mut outputs = Vec::new();
    for task in tasks {
        let (rows, first, complete) = task.await??;
        outputs.push(json!({"rows":rows,"execution_first_batch_ns":first.map(|value|value.0),"execution_complete_ns":complete}));
    }
    let complete = started.elapsed().as_nanos();
    println!(
        "{}",
        json!({"kind":"concurrency","id":query.id,"configuration":configuration,"executions":4,"group_complete_ns":complete,"peak_pool_bytes":session.pool.peak.load(Ordering::Relaxed),"process_peak_rss_bytes":process_memory_bytes("VmHWM:")?,"results":outputs})
    );
    Ok(())
}

async fn async_main(args: &[String], manifest: &Manifest, schedule: &Schedule) -> TestResult {
    if args.first().is_some_and(|mode| mode == "check") && args.len() == 1 {
        run_instrumentation_self_checks()?;
        println!("{}", json!({"kind":"self_check","status":"passed"}));
        return Ok(());
    }
    if args.first().is_some_and(|mode| mode == "compare") && args.len() == 3 {
        println!("{}", compare_ipc(Path::new(&args[1]), Path::new(&args[2]))?);
        return Ok(());
    }
    if args.len() != 5
        || !["preflight", "measure", "concurrency", "profile"].contains(&args[0].as_str())
        || !["native", "spark-off", "spark-on"].contains(&args[3].as_str())
    {
        return Err("usage: execution check | compare LEFT RIGHT | preflight/measure/concurrency/profile SNAPSHOT WORK CONFIGURATION QUERY".into());
    }
    let query = manifest
        .queries
        .iter()
        .find(|query| query.id == args[4])
        .ok_or("unknown frozen query")?;
    let limit = match args[0].as_str() {
        "preflight" => schedule.preflight_pool_bytes,
        "concurrency" => schedule.concurrency_pool_bytes,
        _ => schedule.single_pool_bytes,
    };
    let session = build_registered_session(
        manifest,
        schedule,
        Path::new(&args[1]),
        Path::new(&args[2]),
        limit,
    )
    .await?;
    match args[0].as_str() {
        "preflight" => {
            println!(
                "{}",
                export_preflight_result(&session, query, &args[3], Path::new(&args[2])).await?
            );
            Ok(())
        }
        "measure" => measure_process(&session, manifest, schedule, query, &args[3]).await,
        "concurrency" => concurrency_check(&session, query, &args[3]).await,
        "profile" => {
            for _ in 0..schedule.profile_iterations {
                std::hint::black_box(measure_query(&session, query, &args[3]).await?);
            }
            println!(
                "{}",
                json!({"kind":"profile","id":query.id,"configuration":args[3],"executions":schedule.profile_iterations})
            );
            Ok(())
        }
        _ => Err("invalid execution mode".into()),
    }
}
fn main() -> TestResult {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let casts = args.first().is_some_and(|arg| arg == "--casts-1385");
    let (manifest_json, schedule_json) = if casts {
        args.remove(0);
        if args.first().is_some_and(|mode| mode == "concurrency") {
            return Err("cast attribution does not run concurrency checks".into());
        }
        (
            include_str!("casts/queries.json"),
            include_str!("casts/schedule.json"),
        )
    } else {
        (
            include_str!("execution/queries.json"),
            include_str!("execution/schedule.json"),
        )
    };
    let manifest: Manifest = serde_json::from_str(manifest_json)?;
    let schedule: Schedule = serde_json::from_str(schedule_json)?;
    let workers = if args.first().is_some_and(|mode| mode == "concurrency") {
        4
    } else {
        schedule.runtime_workers
    };
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .max_blocking_threads(8)
        .enable_all()
        .build()?
        .block_on(async_main(&args, &manifest, &schedule))
}
