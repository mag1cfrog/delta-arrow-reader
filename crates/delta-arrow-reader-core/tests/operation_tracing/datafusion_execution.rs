//! Each requested DataFusion partition execution owns its stream outcome.

use super::{
    datafusion_provider::{options, reader_error},
    *,
};
use ::datafusion::{
    common::config::ConfigOptions,
    datasource::TableProvider,
    physical_plan::{ExecutionPlan, collect, limit::GlobalLimitExec},
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader::datafusion::{DeltaTableProvider, ScanOptions, collect_scan_metrics};
use futures_util::{FutureExt, StreamExt};
use tracing::instrument::WithSubscriber;

const EXECUTION: &str = "datafusion_partition_execution";

pub(super) async fn plan(
    fixture: &LogTable,
    backend: ParquetReaderBackend,
    partitions: usize,
) -> TestResult<(SessionContext, Arc<dyn ExecutionPlan>, u64)> {
    let table = fixture.builder(backend).load_table().await?;
    let provider = DeltaTableProvider::try_new(
        table,
        ScanOptions {
            target_partitions: Some(partitions),
            ..options(backend)
        },
    )?;
    let context = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    let collector = Collector::new(Level::INFO);
    let plan = provider
        .scan(&context.state(), None, &[], None)
        .with_subscriber(collector.clone())
        .await?;
    let events = collector.events();
    let completed = events
        .iter()
        .find(|event| event.fields.get("event") == Some(&json!("provider_scan.completed")))
        .expect("planning must have its own completed operation");
    let plan_id = completed.fields["operation_id"].as_u64().unwrap();
    assert_eq!(completed.fields["plan_id"], plan_id);
    Ok((context, plan, plan_id))
}

fn lifecycle(collector: &Collector, plan_id: u64, outcome: &str, output: bool) {
    let events = collector.events();
    assert_eq!(events.len(), 2, "{events:?}");
    for (event, outcome) in events.iter().zip(["started", outcome]) {
        assert_eq!(event.level, Level::INFO);
        assert_eq!(event.fields["event"], format!("{EXECUTION}.{outcome}"));
        assert_eq!(event.fields["operation"], EXECUTION);
        assert_eq!(event.fields["outcome"], outcome);
        assert_eq!(event.fields["plan_id"], plan_id);
        assert_ne!(event.fields["operation_id"], plan_id);
        assert_eq!(event.fields["partition"], 0);
        assert_eq!(event.fields["partition_count"], 1);
    }
    assert_eq!(
        events[0].fields["operation_id"],
        events[1].fields["operation_id"]
    );
    assert_eq!(events[0].fields["output_started"], false);
    assert_eq!(events[1].fields["output_started"], output);
    if outcome == "completed" {
        assert_eq!(events[1].fields["completion_reason"], "eof");
    } else {
        assert!(!events[1].fields.contains_key("completion_reason"));
    }
    assert!(!format!("{:?}", collector.records()).contains("secret-"));
}

#[test]
fn empty_eof_and_drop_have_distinct_partition_outcomes() -> TestResult {
    let runtime = runtime()?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for empty in [true, false] {
            for consume in ["none", "batch", "eof"] {
                if empty && consume == "batch" {
                    continue;
                }
                let fixture = LogTable::new()?;
                if !empty {
                    fixture.write(1, &[fixture.parquet("secret-data.parquet", &[1, 2])?])?;
                }
                let (context, plan, plan_id) = runtime.block_on(plan(&fixture, backend, 1))?;
                let collector = Collector::new(Level::INFO);
                let mut stream =
                    with_default(collector.clone(), || plan.execute(0, context.task_ctx()))?;
                assert_eq!(
                    collector.events().len(),
                    1,
                    "execute starts even an unpolled stream"
                );
                if consume != "none" && !empty {
                    assert_eq!(runtime.block_on(stream.next()).unwrap()?.num_rows(), 2);
                }
                if consume == "eof" {
                    assert!(runtime.block_on(stream.next()).is_none());
                    assert!(runtime.block_on(stream.next()).is_none());
                }
                // Terminal records belong to execute's caller, including out-of-scope drop.
                drop(stream);
                lifecycle(
                    &collector,
                    plan_id,
                    if consume == "eof" {
                        "completed"
                    } else {
                        "dropped"
                    },
                    consume != "none" && !empty,
                );
                let metrics = collect_scan_metrics(plan.as_ref())[0]
                    .snapshot()
                    .reader_metrics;
                if empty || consume == "none" {
                    assert_eq!(metrics.file_tasks_started, 0);
                } else if consume == "eof" {
                    assert_eq!(metrics.file_tasks_completed, 1);
                    assert_eq!(metrics.scan_partitions_completed, 1);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn upstream_limit_drops_the_partition_without_claiming_eof() -> TestResult {
    let runtime = runtime()?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        let fixture = LogTable::new()?;
        fixture.write(1, &[fixture.parquet("secret-data.parquet", &[1, 2, 3, 4])?])?;
        let (context, plan, plan_id) = runtime.block_on(plan(&fixture, backend, 1))?;
        let limited: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(plan, 0, Some(1)));
        let collector = Collector::new(Level::INFO);
        let batches = runtime
            .block_on(collect(limited, context.task_ctx()).with_subscriber(collector.clone()))?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        lifecycle(&collector, plan_id, "dropped", true);
    }
    Ok(())
}

#[test]
fn dropping_a_pending_partition_cannot_become_late_success() -> TestResult {
    let runtime = runtime()?;
    let fixture = LogTable::new()?;
    fixture.write(1, &[fixture.parquet("secret-data.parquet", &[1, 2])?])?;
    let (context, plan, plan_id) =
        runtime.block_on(plan(&fixture, ParquetReaderBackend::Direct, 1))?;
    let owner = Collector::new(Level::INFO);
    let mut stream = with_default(owner.clone(), || plan.execute(0, context.task_ctx()))?;
    let mut worker = Collector::new(Level::DEBUG);
    let observed = worker.clone();
    let (started, wait_started) = std::sync::mpsc::channel();
    let (release, wait_release) = std::sync::mpsc::channel();
    let wait_release = Mutex::new(wait_release);
    let blocked = std::sync::atomic::AtomicBool::new(false);
    worker.probe = Some(Arc::new(move || {
        if observed
            .records()
            .last()
            .is_some_and(|record| record.is_span && record.name == "Object store transport")
            && !blocked.swap(true, Ordering::Relaxed)
        {
            started.send(()).unwrap();
            wait_release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
    }));
    assert!(
        runtime
            .block_on(async { stream.next().with_subscriber(worker.clone()).now_or_never() })
            .is_none()
    );
    wait_started.recv_timeout(Duration::from_secs(5))?;
    assert_eq!(
        owner.events().len(),
        1,
        "dropping a borrowed next future keeps execution alive"
    );
    drop(stream);
    lifecycle(&owner, plan_id, "dropped", false);
    release.send(())?;
    drop(runtime);
    lifecycle(&owner, plan_id, "dropped", false);
    Ok(())
}

#[test]
fn data_and_deletion_vector_failures_after_output_keep_typed_causes() -> TestResult {
    let runtime = runtime()?;
    for level in [Level::WARN, Level::ERROR] {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            for cause in ["missing", "corrupt", "deletion_vector"] {
                let fixture = LogTable::new()?;
                let first = fixture.parquet("secret-first.parquet", &[1, 2])?;
                let mut second = fixture.parquet("secret-second.parquet", &[3])?;
                if cause == "deletion_vector" {
                    fixture.write(0, &[json!({"protocol": {
                        "minReaderVersion": 3, "minWriterVersion": 7,
                        "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"]
                    }}), metadata(false)])?;
                    // Valid relative descriptor; its referenced object is deliberately absent.
                    second["add"]["deletionVector"] = json!({
                        "storageType": "u", "pathOrInlineDv": "vBn[lx{q8@P<9BNH/isA",
                        "offset": 1, "sizeInBytes": 36, "cardinality": 1
                    });
                }
                fixture.write(1, &[first, second])?;
                let (context, plan, plan_id) = runtime.block_on(plan(&fixture, backend, 1))?;
                let path = fixture.0.join("secret-second.parquet");
                if cause == "missing" {
                    fs::remove_file(&path)?;
                }
                if cause == "corrupt" {
                    fs::write(&path, vec![0; path.metadata()?.len() as usize])?;
                }
                let collector = Collector::new(level);
                let mut stream =
                    with_default(collector.clone(), || plan.execute(0, context.task_ctx()))?;
                assert_eq!(runtime.block_on(stream.next()).unwrap()?.num_rows(), 2);
                let error = runtime.block_on(stream.next()).unwrap().unwrap_err();
                let error = reader_error(&error);
                assert!(error.source().is_some());
                assert_eq!(
                    error.source_kind(),
                    Some(if cause == "corrupt" {
                        "parquet"
                    } else {
                        "not_found"
                    })
                );
                if cause == "deletion_vector" {
                    assert_eq!(error.code(), "deletion_vector_read");
                }
                assert!(runtime.block_on(stream.next()).is_none());
                assert!(runtime.block_on(stream.next()).is_none());
                drop(stream);
                let event = failure(&collector, EXECUTION, error);
                assert_eq!(event.fields["event"], format!("{EXECUTION}.failed"));
                assert_eq!(event.fields["plan_id"], plan_id);
                assert_eq!(event.fields["snapshot_version"], 1);
                assert_eq!(event.fields["backend"], format!("{backend:?}"));
                assert_eq!(event.fields["partition_count"], 1);
                assert_eq!(event.fields["partition"], 0);
                assert_eq!(event.fields["output_started"], true);
                let metrics = collect_scan_metrics(plan.as_ref())[0]
                    .snapshot()
                    .reader_metrics;
                assert_eq!(metrics.file_tasks_started, 2);
                assert_eq!(metrics.file_tasks_completed, 1);
                assert_eq!(metrics.scan_partitions_completed, 0);
            }
        }
    }
    Ok(())
}

#[test]
fn repeated_and_concurrent_executions_report_independent_failures() -> TestResult {
    let runtime = runtime()?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        let fixture = LogTable::new()?;
        fixture.write(
            1,
            &[
                fixture.parquet("secret-first.parquet", &[1, 2])?,
                fixture.parquet("secret-second.parquet", &[3, 4])?,
            ],
        )?;
        let (context, plan, plan_id) = runtime.block_on(plan(&fixture, backend, 2))?;
        fs::remove_file(fixture.0.join("secret-first.parquet"))?;
        fs::remove_file(fixture.0.join("secret-second.parquet"))?;
        let mut identities = std::collections::HashSet::new();
        for partitions in [[0, 1], [0, 0], [0, 1]] {
            let collectors = [Collector::new(Level::WARN), Collector::new(Level::ERROR)];
            let mut streams = partitions
                .iter()
                .zip(&collectors)
                .map(|(&partition, collector)| {
                    with_default(collector.clone(), || {
                        plan.execute(partition, context.task_ctx())
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let (left, right) = streams.split_at_mut(1);
            let results = runtime.block_on(async { tokio::join!(left[0].next(), right[0].next()) });
            for ((result, collector), partition) in [results.0, results.1]
                .into_iter()
                .zip(&collectors)
                .zip(partitions)
            {
                let error = result.unwrap().unwrap_err();
                let error = reader_error(&error);
                assert_eq!(error.source_kind(), Some("not_found"));
                let event = failure(collector, EXECUTION, error);
                assert_eq!(event.fields["plan_id"], plan_id);
                assert_eq!(event.fields["partition"], partition);
                assert_eq!(event.fields["partition_count"], 2);
                assert_eq!(event.fields["output_started"], false);
                assert!(identities.insert(event.fields["operation_id"].as_u64().unwrap()));
            }
            for stream in &mut streams {
                assert!(runtime.block_on(stream.next()).is_none());
            }
            drop(streams);
            assert!(
                collectors
                    .iter()
                    .all(|collector| collector.events().is_empty())
            );
        }
    }
    Ok(())
}

#[test]
fn invalid_partition_reports_setup_failure_without_reading() -> TestResult {
    let runtime = runtime()?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        let fixture = LogTable::new()?;
        let (context, plan, plan_id) = runtime.block_on(plan(&fixture, backend, 1))?;
        let collector = Collector::new(Level::ERROR);
        let error = with_default(collector.clone(), || plan.execute(1, context.task_ctx()))
            .err()
            .unwrap();
        let error = reader_error(&error);
        assert_eq!(error.reason(), "scan_partition_index_out_of_range");
        let event = failure(&collector, EXECUTION, error);
        assert_eq!(event.fields["plan_id"], plan_id);
        assert_eq!(event.fields["partition"], 1);
        assert_eq!(event.fields["partition_count"], 1);
        assert_eq!(event.fields["output_started"], false);
        assert_eq!(
            collect_scan_metrics(plan.as_ref())[0]
                .snapshot()
                .reader_metrics
                .file_tasks_started,
            0
        );
    }
    Ok(())
}

#[test]
fn partition_task_panic_is_an_error_not_normal_cancellation() -> TestResult {
    let runtime = runtime()?;
    let fixture = LogTable::new()?;
    fixture.write(1, &[fixture.parquet("secret-data.parquet", &[1, 2])?])?;
    let (context, plan, plan_id) =
        runtime.block_on(plan(&fixture, ParquetReaderBackend::Direct, 1))?;
    let owner = Collector::new(Level::ERROR);
    let mut stream = with_default(owner.clone(), || plan.execute(0, context.task_ctx()))?;
    let mut injector = Collector::new(Level::DEBUG);
    let observed = injector.clone();
    injector.probe =
        Some(Arc::new(move || {
            assert!(
            !observed.records().last().is_some_and(|record|
                record.is_span && record.name == "Object store transport"),
            "secret-injected-transport-panic"
        );
        }));
    let error = runtime
        .block_on(stream.next().with_subscriber(injector.clone()))
        .unwrap()
        .unwrap_err();
    let error = reader_error(&error);
    assert_eq!(error.reason(), "partition_scheduler_task_failed");
    assert!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<tokio::task::JoinError>())
            .is_some_and(tokio::task::JoinError::is_panic)
    );
    assert!(runtime.block_on(stream.next()).is_none());
    drop(stream);
    let event = failure(&owner, EXECUTION, error);
    assert_eq!(event.fields["error_source_kind"], "worker_panicked");
    assert_eq!(event.fields["plan_id"], plan_id);
    assert_eq!(event.fields["output_started"], false);
    assert!(
        injector
            .events()
            .iter()
            .all(|event| event.level != Level::ERROR)
    );
    assert!(!format!("{:?}", injector.records()).contains("secret-"));
    injector.assert_balanced();
    Ok(())
}

#[test]
fn repartitioned_and_cloned_plans_keep_plan_identity_but_start_new_executions() -> TestResult {
    let runtime = runtime()?;
    let fixture = LogTable::new()?;
    fixture.write(
        1,
        &[
            fixture.parquet("secret-first.parquet", &[1, 2])?,
            fixture.parquet("secret-second.parquet", &[3, 4])?,
        ],
    )?;
    let (context, plan, plan_id) =
        runtime.block_on(plan(&fixture, ParquetReaderBackend::Direct, 4))?;
    let mut config = ConfigOptions::default();
    config.optimizer.repartition_file_min_size = 0;
    let repartitioned = plan
        .repartitioned(4, &config)?
        .expect("real files must be split");
    assert_eq!(
        repartitioned
            .properties()
            .output_partitioning()
            .partition_count(),
        4
    );
    let cloned = Arc::clone(&repartitioned).with_new_children(vec![])?;
    let collector = Collector::new(Level::INFO);
    let mut identities = std::collections::HashSet::new();
    for plan in [plan, repartitioned, cloned] {
        let partitions = plan.properties().output_partitioning().partition_count();
        let mut rows = 0;
        for partition in 0..partitions {
            let mut stream = with_default(collector.clone(), || {
                plan.execute(partition, context.task_ctx())
            })?;
            while let Some(batch) = runtime.block_on(stream.next()) {
                rows += batch?.num_rows();
            }
            assert!(runtime.block_on(stream.next()).is_none());
            drop(stream);
            let events = collector.events();
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].fields["event"], format!("{EXECUTION}.started"));
            assert_eq!(events[1].fields["event"], format!("{EXECUTION}.completed"));
            assert_eq!(
                events[0].fields["operation_id"],
                events[1].fields["operation_id"]
            );
            assert_eq!(events[1].fields["plan_id"], plan_id);
            assert_eq!(events[1].fields["partition"], partition);
            assert_eq!(events[1].fields["partition_count"], partitions);
            assert!(identities.insert(events[1].fields["operation_id"].as_u64().unwrap()));
            collector.clear();
        }
        assert_eq!(rows, 4);
    }
    Ok(())
}

#[test]
fn disabled_and_warn_datafusion_tracing_preserve_rows_and_read_metrics() -> TestResult {
    let runtime = runtime()?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        let fixture = LogTable::new()?;
        fixture.write(
            1,
            &[
                fixture.parquet("secret-first.parquet", &[1, 2])?,
                fixture.parquet("secret-second.parquet", &[3, 4])?,
            ],
        )?;
        let collector = Collector::new(Level::WARN);
        let mut results = Vec::new();
        for dispatch in [
            tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()),
            tracing::Dispatch::new(collector.clone()),
        ] {
            let (context, plan, _) = runtime.block_on(plan(&fixture, backend, 1))?;
            let metrics = collect_scan_metrics(plan.as_ref())[0].clone();
            let batches =
                runtime.block_on(collect(plan, context.task_ctx()).with_subscriber(dispatch))?;
            let rows: Vec<_> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int32Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                })
                .collect();
            assert_eq!(rows, [1, 2, 3, 4]);
            let snapshot = metrics.snapshot().reader_metrics;
            assert_eq!(snapshot.file_tasks_completed, 2);
            assert_eq!(snapshot.scan_partitions_completed, 1);
            if backend == ParquetReaderBackend::Direct {
                assert!(snapshot.parquet_data_file_range_get_operations.unwrap() > 0);
            }
            results.push((rows, snapshot));
        }
        assert_eq!(results[0], results[1]);
        assert!(collector.records().is_empty());
    }
    Ok(())
}
