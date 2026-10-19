use super::*;
use tracing::{Instrument, instrument::WithSubscriber};

fn caller(collector: &Collector) -> tracing::Span {
    with_default(
        collector.clone(),
        || tracing::info_span!(target: "delta_arrow_reader::test", "caller"),
    )
}

#[test]
fn unpolled_future_does_not_capture_its_creation_subscriber() -> TestResult {
    let creation = Collector::new(Level::TRACE);
    let lifetime = creation.lifetime();
    let future = with_default(creation.clone(), || DeltaTableBuilder::new("").load_table());
    assert!(creation.records().is_empty());
    drop(creation);
    assert!(lifetime.upgrade().is_none());
    let polling = Collector::new(Level::ERROR);
    let error = runtime()?
        .block_on(future.with_subscriber(polling.clone()))
        .unwrap_err();
    failure(&polling, "load_table", &error);
    Ok(())
}

#[test]
fn public_operations_propagate_dispatch_and_parent_to_workers() -> TestResult {
    for multi_thread in [false, true] {
        let runtime = if multi_thread {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .max_blocking_threads(1)
                .enable_all()
                .build()?
        } else {
            tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(1)
                .enable_all()
                .build()?
        };
        let fixture = LogTable::new()?;
        fixture.write(1, &[fixture.parquet("secret-data.parquet", &[1, 2])?])?;
        let collector = Collector::new(Level::TRACE);
        let span = caller(&collector);
        let parent = span.id().unwrap().into_u64();
        let table = runtime.block_on(
            async {
                let table = fixture
                    .builder(ParquetReaderBackend::Direct)
                    .with_warmup(WarmupMode::QueryPlanning)
                    .load_table()
                    .await?;
                let refreshed = table.refresh().await?;
                let mut stream = refreshed.scan().build().await?.into_stream();
                while stream.next_batch().await?.is_some() {}
                Ok::<_, DeltaReaderError>(refreshed)
            }
            .instrument(span)
            .with_subscriber(collector.clone()),
        )?;
        let records = collector.records();
        for event in [
            "snapshot_load.started",
            "snapshot_refresh.completed",
            "scan_metadata_cache_build.completed",
            "scan_execution.completed",
        ] {
            let event = records
                .iter()
                .find(|record| record.fields.get("event") == Some(&json!(event)))
                .unwrap_or_else(|| panic!("missing {event}"));
            assert!(collector.linked_to(event, parent), "{event:?}");
        }
        for name in [
            "Delta file task partitioning",
            "Delta partition task",
            "Object store transport",
        ] {
            let record = records
                .iter()
                .find(|record| record.name == name)
                .unwrap_or_else(|| panic!("missing {name}"));
            assert!(collector.linked_to(record, parent), "{record:?}");
        }
        collector.assert_balanced();
        runtime.block_on(async {
            assert!(
                !tokio::task::spawn_blocking(|| tracing::dispatcher::get_default(|dispatch| {
                    dispatch.is::<Collector>()
                }))
                .await
                .unwrap()
            );
        });
        let lifetime = collector.lifetime();
        drop(collector);
        assert!(
            lifetime.upgrade().is_none(),
            "the returned table retained its caller"
        );
        drop(table);
    }
    Ok(())
}

#[test]
fn snapshot_load_propagates_dispatch_and_parent_to_its_worker() -> TestResult {
    let fixture = LogTable::new()?;
    let collector = Collector::new(Level::TRACE);
    let parent = caller(&collector);
    let parent_id = parent.id().unwrap().into_u64();
    let caller_thread = std::thread::current().id();
    let snapshot = runtime()?.block_on(
        fixture
            .builder(ParquetReaderBackend::Direct)
            .load_snapshot()
            .instrument(parent)
            .with_subscriber(collector.clone()),
    )?;
    assert_eq!(snapshot.version(), 0);
    let events = collector.events();
    for name in ["snapshot_load.started", "snapshot_load.completed"] {
        let event = events
            .iter()
            .find(|record| record.fields.get("event") == Some(&json!(name)))
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_ne!(event.thread, caller_thread);
        assert!(collector.linked_to(event, parent_id), "{event:?}");
    }
    collector.assert_balanced();
    Ok(())
}

#[test]
fn streams_use_the_polling_caller_for_both_backends() -> TestResult {
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for complete in [false, true] {
            let runtime = runtime()?;
            let fixture = LogTable::new()?;
            fixture.write(1, &[fixture.parquet("secret-data.parquet", &[1, 2])?])?;
            let table = runtime.block_on(fixture.builder(backend).load_table())?;
            let plan = runtime.block_on(table.scan().build())?;
            let collector = Collector::new(Level::TRACE);
            let mut stream = plan.into_stream();
            assert!(collector.events().is_empty());
            let parent = caller(&collector);
            let parent_id = parent.id().unwrap().into_u64();
            runtime.block_on(
                async move {
                    assert_eq!(stream.next_batch().await?.unwrap().num_rows(), 2);
                    if complete {
                        assert!(stream.next_batch().await?.is_none());
                    }
                    drop(stream);
                    Ok::<_, DeltaReaderError>(())
                }
                .instrument(parent)
                .with_subscriber(collector.clone()),
            )?;
            drop(runtime);
            let events = collector.events();
            let execution = events
                .iter()
                .filter(|record| record.fields.get("operation") == Some(&json!("scan_execution")))
                .collect::<Vec<_>>();
            assert_eq!(execution.len(), 2, "start and terminal events expected");
            assert_eq!(execution[0].fields["event"], "scan_execution.started");
            assert_eq!(
                execution[1].fields["event"],
                if complete {
                    "scan_execution.completed"
                } else {
                    "scan_execution.dropped"
                }
            );
            assert_eq!(
                execution[0].fields["operation_id"],
                execution[1].fields["operation_id"]
            );
            assert!(
                events
                    .iter()
                    .all(|event| collector.linked_to(event, parent_id))
            );
            collector.assert_balanced();
        }
    }
    Ok(())
}

#[test]
fn concurrent_operations_keep_distinct_spans_on_a_reused_worker() -> TestResult {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let fixture = LogTable::new()?;
    let collector = Collector::new(Level::TRACE);
    let roots = [caller(&collector), caller(&collector)];
    let ids = roots.each_ref().map(|span| span.id().unwrap().into_u64());
    let (one, two) = runtime.block_on(
        async {
            futures_util::join!(
                fixture
                    .builder(ParquetReaderBackend::Direct)
                    .load_table()
                    .instrument(roots[0].clone()),
                fixture
                    .builder(ParquetReaderBackend::Direct)
                    .load_table()
                    .instrument(roots[1].clone()),
            )
        }
        .with_subscriber(collector.clone()),
    );
    let (one, _two) = (one?, two?);
    let cloned = one.clone();
    let _plan = runtime.block_on(
        cloned
            .scan()
            .build()
            .instrument(roots[1].clone())
            .with_subscriber(collector.clone()),
    )?;
    let events = collector.events();
    let worker = |id| {
        events
            .iter()
            .find(|record| {
                record.fields.get("event") == Some(&json!("snapshot_load.completed"))
                    && collector.linked_to(record, id)
            })
            .expect("snapshot worker lost its caller span")
            .thread
    };
    assert_eq!(
        worker(ids[0]),
        worker(ids[1]),
        "blocking worker should be reused"
    );
    assert!(events.iter().all(|record| {
        collector.linked_to(record, ids[0]) || collector.linked_to(record, ids[1])
    }));
    let planning = events
        .iter()
        .filter(|record| record.fields.get("operation") == Some(&json!("scan_planning")))
        .collect::<Vec<_>>();
    assert_eq!(planning.len(), 2);
    assert!(
        planning
            .iter()
            .all(|record| collector.linked_to(record, ids[1]))
    );
    collector.assert_balanced();
    Ok(())
}

#[test]
fn filtered_spans_keep_failures_visible_and_terminal_streams_release_resources() -> TestResult {
    for level in [Level::WARN, Level::ERROR] {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let runtime = runtime()?;
            let owner = Collector::new(level);
            let disabled = caller(&owner);
            assert!(disabled.is_disabled());
            let error = runtime
                .block_on(
                    DeltaTableBuilder::new("")
                        .load_table()
                        .instrument(disabled)
                        .with_subscriber(owner.clone()),
                )
                .unwrap_err();
            failure(&owner, "load_table", &error);
            let fixture = LogTable::new()?;
            fixture.write(1, &[fixture.parquet("secret-missing.parquet", &[1])?])?;
            let table = runtime.block_on(fixture.builder(backend).load_table())?;
            let plan = runtime.block_on(table.scan().build())?;
            fs::remove_file(fixture.0.join("secret-missing.parquet"))?;
            let mut stream = plan.into_stream();
            let error = runtime
                .block_on(stream.next_batch().with_subscriber(owner.clone()))
                .unwrap_err();
            failure(&owner, "scan_execution", &error);
            let lifetime = owner.lifetime();
            drop(owner);
            assert!(runtime.block_on(stream.next_batch())?.is_none());
            // Failure requests cleanup; wait for outstanding workers while retaining the stream.
            drop(runtime);
            assert!(
                lifetime.upgrade().is_none(),
                "terminal stream retained its context"
            );
        }
    }
    Ok(())
}

#[test]
fn pending_load_and_refresh_report_abandonment_before_workers_finish() -> TestResult {
    use futures_util::FutureExt;
    for (refresh, complete) in [(false, false), (true, false), (false, true), (true, true)] {
        let runtime = runtime()?;
        let fixture = LogTable::new()?;
        let table = runtime.block_on(fixture.builder(ParquetReaderBackend::Direct).load_table())?;
        let (entered, started) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let wait = Mutex::new(wait);
        let mut owner = Collector::new(Level::TRACE);
        let observed = owner.clone();
        owner.probe = Some(Arc::new(move || {
            let event = if refresh {
                "snapshot_refresh.started"
            } else {
                "snapshot_load.started"
            };
            if observed
                .records()
                .last()
                .is_some_and(|record| record.fields.get("event") == Some(&json!(event)))
            {
                entered.send(()).unwrap();
                wait.lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
        }));
        let parent = caller(&owner);
        let parent_id = parent.id().unwrap().into_u64();
        let future = async {
            if refresh {
                table.refresh().await
            } else {
                fixture
                    .builder(ParquetReaderBackend::Direct)
                    .with_warmup(WarmupMode::QueryPlanning)
                    .load_table()
                    .await
            }
        };
        let mut future = Box::pin(future);
        runtime.block_on(async {
            assert!(
                future
                    .as_mut()
                    .instrument(parent)
                    .with_subscriber(owner.clone())
                    .now_or_never()
                    .is_none()
            );
        });
        started.recv_timeout(Duration::from_secs(5))?;
        let lifetime = owner.lifetime();
        if complete {
            release.send(())?;
            let result = runtime.block_on(future.with_subscriber(owner.clone()))?;
            let events = owner.events();
            assert!(
                events
                    .iter()
                    .any(|record| record.fields.get("outcome") == Some(&json!("completed")))
            );
            assert!(
                events
                    .iter()
                    .all(|record| owner.linked_to(record, parent_id))
            );
            drop(result);
            owner.assert_balanced();
            drop(owner);
        } else {
            with_default(owner.clone(), || drop(future));
            assert!(
                owner
                    .events()
                    .iter()
                    .any(|record| record.fields.get("outcome") == Some(&json!("abandoned")))
            );
            drop(owner);
            assert!(
                lifetime.upgrade().is_some(),
                "outstanding worker lost its context"
            );
            release.send(())?;
        }
        drop(runtime);
        assert!(
            lifetime.upgrade().is_none(),
            "finished worker retained its context"
        );
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_terminal_error_releases_resources_without_an_extra_poll() -> TestResult {
    use datafusion::{datasource::TableProvider, prelude::SessionContext};
    use delta_arrow_reader::datafusion::{DeltaTableProvider, ScanOptions};
    use futures_util::{FutureExt, StreamExt};

    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for level in [Level::TRACE, Level::ERROR] {
            let runtime = runtime()?;
            let fixture = LogTable::new()?;
            fixture.write(1, &[fixture.parquet("secret-missing.parquet", &[1, 2])?])?;
            let table = runtime.block_on(fixture.builder(backend).load_table())?;
            let provider = DeltaTableProvider::try_new(
                table,
                ScanOptions {
                    execution_options: DeltaScanExecutionOptions::default()
                        .with_parquet_backend(backend),
                    ..ScanOptions::default()
                },
            )?;
            let session = SessionContext::new();
            let plan = runtime.block_on(provider.scan(&session.state(), None, &[], None))?;
            fs::remove_file(fixture.0.join("secret-missing.parquet"))?;
            let owner = Collector::new(level);
            let parent = caller(&owner);
            let mut stream = with_default(owner.clone(), || {
                parent.in_scope(|| plan.execute(0, session.task_ctx()))
            })?;
            let error = runtime
                .block_on(
                    stream
                        .next()
                        .instrument(parent)
                        .with_subscriber(owner.clone()),
                )
                .unwrap();
            assert!(error.is_err());
            // Outstanding workers may retain context until they finish; keep the stream alive.
            drop(runtime);
            owner.assert_balanced();
            let lifetime = owner.lifetime();
            drop(owner);
            assert!(
                lifetime.upgrade().is_none(),
                "terminal DataFusion stream retained its subscriber"
            );
            assert!(matches!(stream.next().now_or_never(), Some(None)));
        }
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
#[test]
fn reusable_datafusion_plan_uses_each_polling_caller() -> TestResult {
    use datafusion::{datasource::TableProvider, prelude::SessionContext};
    use delta_arrow_reader::datafusion::{DeltaTableProvider, ScanOptions};
    use futures_util::StreamExt;
    let runtime = runtime()?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        let fixture = LogTable::new()?;
        fixture.write(1, &[fixture.parquet("secret-data.parquet", &[1, 2])?])?;
        let table = runtime.block_on(fixture.builder(backend).load_table())?;
        let provider = DeltaTableProvider::try_new(
            table,
            ScanOptions {
                execution_options: DeltaScanExecutionOptions::default()
                    .with_parquet_backend(backend),
                ..ScanOptions::default()
            },
        )?;
        let session = SessionContext::new();
        let planning = Collector::new(Level::TRACE);
        let parent = caller(&planning);
        let parent_id = parent.id().unwrap().into_u64();
        let plan = runtime.block_on(
            provider
                .scan(&session.state(), None, &[], None)
                .instrument(parent)
                .with_subscriber(planning.clone()),
        )?;
        let records = planning.records();
        for name in ["Delta scan planning", "Delta scan execution setup"] {
            let span = records
                .iter()
                .find(|record| record.is_span && record.name == name)
                .unwrap_or_else(|| panic!("missing {name}"));
            assert!(planning.linked_to(span, parent_id), "{span:?}");
        }
        planning.assert_balanced();
        let lifetime = planning.lifetime();
        drop(planning);
        assert!(
            lifetime.upgrade().is_none(),
            "plan retained the planning subscriber"
        );
        for _ in 0..2 {
            let owner = Collector::new(Level::TRACE);
            let parent = caller(&owner);
            let id = parent.id().unwrap().into_u64();
            let mut stream = with_default(owner.clone(), || {
                parent.in_scope(|| plan.execute(0, session.task_ctx()))
            })?;
            runtime.block_on(
                async {
                    let mut rows = 0;
                    while let Some(batch) = stream.next().await {
                        rows += batch?.num_rows();
                    }
                    assert_eq!(rows, 2);
                    Ok::<_, Box<dyn Error>>(())
                }
                .instrument(parent)
                .with_subscriber(owner.clone()),
            )?;
            let records = owner.records();
            assert!(
                records
                    .iter()
                    .any(|record| record.name == "Delta partition task"
                        && owner.linked_to(record, id))
            );
            owner.assert_balanced();
            let lifetime = owner.lifetime();
            drop(owner);
            assert!(
                lifetime.upgrade().is_none(),
                "finished execution retained its subscriber"
            );
        }
    }
    Ok(())
}
