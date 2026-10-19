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
fn unparented_stream_does_not_inherit_a_later_request() -> TestResult {
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for level in [Level::TRACE, Level::ERROR] {
            let runtime = runtime()?;
            let fixture = LogTable::new()?;
            fixture.write(1, &[fixture.parquet("secret-data.parquet", &[1, 2])?])?;
            let table = runtime.block_on(fixture.builder(backend).load_table())?;
            let plan = runtime.block_on(table.scan().build())?;
            let collector = Collector::new(level);
            let dispatch = tracing::Dispatch::new(collector.clone());
            let mut stream = tracing::dispatcher::with_default(&dispatch, || plan.into_stream());
            if level == Level::ERROR {
                fs::remove_file(fixture.0.join("secret-data.parquet"))?;
            }
            let unrelated = tracing::dispatcher::with_default(
                &dispatch,
                || tracing::error_span!(target: "delta_arrow_reader::test", "unrelated request"),
            );
            let unrelated_id = unrelated.id().unwrap().into_u64();
            runtime.block_on(
                async {
                    if level == Level::ERROR {
                        assert!(stream.next_batch().await.is_err());
                    } else {
                        assert_eq!(stream.next_batch().await?.unwrap().num_rows(), 2);
                        assert!(stream.next_batch().await?.is_none());
                    }
                    drop(stream);
                    tracing::error!(target: "delta_arrow_reader::test", event = "unrelated.probe");
                    Ok::<_, DeltaReaderError>(())
                }
                .instrument(unrelated)
                .with_subscriber(dispatch),
            )?;
            drop(runtime);
            let records = collector.records();
            assert!(
                records
                    .iter()
                    .any(|record| record.fields.get("operation") == Some(&json!("scan_execution")))
            );
            for record in records {
                if record.fields.get("event") == Some(&json!("unrelated.probe")) {
                    assert_eq!(record.parent, Some(unrelated_id));
                } else if record.span_id != Some(unrelated_id) {
                    assert!(!collector.linked_to(&record, unrelated_id), "{record:?}");
                }
            }
            collector.assert_balanced();
        }
    }
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
fn stream_keeps_its_creation_context_when_polled_or_dropped_elsewhere() -> TestResult {
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
            let owner = Collector::new(Level::TRACE);
            let unrelated = Collector::new(Level::TRACE);
            let span = caller(&owner);
            let parent = span.id().unwrap().into_u64();
            let mut stream = with_default(owner.clone(), || span.in_scope(|| plan.into_stream()));
            drop(span);
            assert!(owner.events().is_empty());
            let consume = async move {
                assert_eq!(stream.next_batch().await?.unwrap().num_rows(), 2);
                if complete {
                    assert!(stream.next_batch().await?.is_none());
                }
                drop(stream);
                tracing::info!(target: "delta_arrow_reader::test", event = "unrelated.probe");
                Ok::<_, DeltaReaderError>(())
            }
            .with_subscriber(unrelated.clone());
            runtime.block_on(async { tokio::spawn(consume).await })??;
            drop(runtime);
            let events = owner.events();
            assert_ne!(events[0].thread, std::thread::current().id());
            assert!(events.iter().any(|record| record.fields.get("event")
                == Some(&json!(if complete {
                    "scan_execution.completed"
                } else {
                    "scan_execution.dropped"
                }))));
            assert!(
                events.iter().all(|record| owner.linked_to(record, parent)),
                "{events:?}"
            );
            let other = unrelated.events();
            assert_eq!(other.len(), 1, "{other:?}");
            assert_eq!(other[0].fields["event"], "unrelated.probe");
            owner.assert_balanced();
        }
    }
    Ok(())
}

#[test]
fn concurrent_and_later_operations_do_not_inherit_another_caller() -> TestResult {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let fixture = LogTable::new()?;
    let first = Collector::new(Level::TRACE);
    let second = Collector::new(Level::TRACE);
    let roots = [caller(&first), caller(&second)];
    let ids = roots.each_ref().map(|span| span.id().unwrap().into_u64());
    let (one, two) = runtime.block_on(async {
        futures_util::join!(
            fixture
                .builder(ParquetReaderBackend::Direct)
                .load_table()
                .instrument(roots[0].clone())
                .with_subscriber(first.clone()),
            fixture
                .builder(ParquetReaderBackend::Direct)
                .load_table()
                .instrument(roots[1].clone())
                .with_subscriber(second.clone()),
        )
    });
    let (one, _two) = (one?, two?);
    let cloned = one.clone();
    let _plan = runtime.block_on(
        cloned
            .scan()
            .build()
            .instrument(roots[1].clone())
            .with_subscriber(second.clone()),
    )?;
    for (collector, id) in [(&first, ids[0]), (&second, ids[1])] {
        assert!(
            collector
                .events()
                .iter()
                .all(|record| collector.linked_to(record, id))
        );
        collector.assert_balanced();
    }
    let worker = |collector: &Collector| {
        collector
            .events()
            .into_iter()
            .find(|record| record.fields.get("event") == Some(&json!("snapshot_load.completed")))
            .unwrap()
            .thread
    };
    assert_eq!(
        worker(&first),
        worker(&second),
        "blocking worker should be reused"
    );
    assert!(
        !first
            .events()
            .iter()
            .any(|record| record.fields.get("operation") == Some(&json!("scan_planning")))
    );
    assert!(
        second
            .events()
            .iter()
            .any(|record| record.fields.get("operation") == Some(&json!("scan_planning")))
    );
    Ok(())
}

#[test]
fn filtered_spans_do_not_redirect_failures_or_retain_completed_streams() -> TestResult {
    for level in [Level::WARN, Level::ERROR] {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let runtime = runtime()?;
            let owner = Collector::new(level);
            let other = Collector::new(Level::TRACE);
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
            let mut stream = with_default(owner.clone(), || plan.into_stream());
            let error = runtime
                .block_on(stream.next_batch().with_subscriber(other.clone()))
                .unwrap_err();
            failure(&owner, "scan_execution", &error);
            assert!(other.records().is_empty());
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
fn pending_load_and_refresh_keep_context_when_resumed_or_dropped() -> TestResult {
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
        let other = Collector::new(Level::TRACE);
        let lifetime = owner.lifetime();
        if complete {
            release.send(())?;
            let result = runtime.block_on(future.with_subscriber(other.clone()))?;
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
            with_default(other.clone(), || drop(future));
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
        assert!(other.records().is_empty());
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
#[test]
fn datafusion_terminal_error_releases_context_without_an_extra_poll() -> TestResult {
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
            let other = Collector::new(Level::TRACE);
            let parent = caller(&owner);
            let mut stream = with_default(owner.clone(), || {
                parent.in_scope(|| plan.execute(0, session.task_ctx()))
            })?;
            drop(parent);
            let error = runtime
                .block_on(stream.next().with_subscriber(other.clone()))
                .unwrap();
            assert!(error.is_err());
            // Outstanding workers may retain context until they finish; keep the stream alive.
            drop(runtime);
            assert!(other.records().is_empty());
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
fn reusable_datafusion_plan_captures_each_execution_caller() -> TestResult {
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
        let plan = runtime.block_on(
            provider
                .scan(&session.state(), None, &[], None)
                .instrument(caller(&planning))
                .with_subscriber(planning.clone()),
        )?;
        let lifetime = planning.lifetime();
        drop(planning);
        assert!(
            lifetime.upgrade().is_none(),
            "plan retained the planning subscriber"
        );
        for _ in 0..2 {
            let owner = Collector::new(Level::TRACE);
            let other = Collector::new(Level::TRACE);
            let parent = caller(&owner);
            let id = parent.id().unwrap().into_u64();
            let mut stream = with_default(owner.clone(), || {
                parent.in_scope(|| plan.execute(0, session.task_ctx()))
            })?;
            drop(parent);
            runtime.block_on(
                async {
                    let mut rows = 0;
                    while let Some(batch) = stream.next().await {
                        rows += batch?.num_rows();
                    }
                    assert_eq!(rows, 2);
                    Ok::<_, Box<dyn Error>>(())
                }
                .with_subscriber(other.clone()),
            )?;
            let records = owner.records();
            assert!(
                records
                    .iter()
                    .any(|record| record.name == "Delta partition task"
                        && owner.linked_to(record, id))
            );
            assert!(other.records().is_empty());
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
