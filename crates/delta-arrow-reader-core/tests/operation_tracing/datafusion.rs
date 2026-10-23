//! Provider boundaries own validation and planning failures before execution begins.

use super::*;
use ::datafusion::{
    common::DataFusionError,
    datasource::TableProvider,
    logical_expr::{TableProviderFilterPushDown, col, lit},
    prelude::SessionContext,
};
use delta_arrow_reader::datafusion::{DeltaTableProvider, ScanOptions, register_table};
use futures_util::FutureExt;
use tracing::{Instrument, instrument::WithSubscriber};

pub(super) fn scan_options(backend: ParquetReaderBackend) -> ScanOptions {
    ScanOptions {
        execution_options: DeltaScanExecutionOptions::default().with_parquet_backend(backend),
        ..ScanOptions::default()
    }
}

pub(super) fn expect_reader_error(error: &DataFusionError) -> &DeltaReaderError {
    let DataFusionError::External(source) = error else {
        panic!("adapter must return a typed external error: {error:?}");
    };
    source.downcast_ref().expect("DeltaReaderError source")
}

fn assert_and_take_provider_failure(
    collector: &TraceCollector,
    operation: &str,
    error: &DeltaReaderError,
    backend: ParquetReaderBackend,
    snapshot_version: u64,
) -> CapturedTraceRecord {
    let event = assert_and_take_failure_event(collector, operation, error);
    assert_eq!(event.fields["event"], format!("{operation}.failed"));
    assert_eq!(event.fields["snapshot_version"], snapshot_version);
    assert_eq!(event.fields["backend"], format!("{backend:?}"));
    event
}

#[test]
fn creation_and_registration_failures_have_one_owner() -> TestResult {
    let runtime = runtime()?;
    for level in [Level::WARN, Level::ERROR] {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let fixture = LogTable::new()?;
            let table = runtime.block_on(fixture.builder(backend).load_table())?;
            fixture.write_commit(1, &[protocol_action(4)])?;
            let unsupported = runtime
                .block_on(fixture.builder(backend).load_snapshot())?
                .into_table()?;
            let collector = TraceCollector::new(level);
            with_default(collector.clone(), || -> TestResult {
                let invalid_options = ScanOptions {
                    target_partitions: Some(0),
                    ..scan_options(backend)
                };
                let error = DeltaTableProvider::try_new(table.clone(), invalid_options.clone())
                    .unwrap_err();
                assert_eq!(error.reason(), "scan_partition_target_must_be_positive");
                assert_and_take_provider_failure(
                    &collector,
                    "provider_creation",
                    &error,
                    backend,
                    0,
                );
                let error = DeltaTableProvider::try_new(unsupported.clone(), scan_options(backend))
                    .unwrap_err();
                assert_eq!(error.reason(), "unsupported_reader_version");
                assert_and_take_provider_failure(
                    &collector,
                    "provider_creation",
                    &error,
                    backend,
                    1,
                );

                let context = SessionContext::new();
                for (name, reason) in [
                    ("", "table_registration_name_empty"),
                    ("secret-name", "table_registration_name_invalid"),
                ] {
                    let error =
                        register_table(&context, name, table.clone(), scan_options(backend))
                            .unwrap_err();
                    assert_eq!(error.reason(), reason);
                    assert_and_take_provider_failure(
                        &collector,
                        "provider_registration",
                        &error,
                        backend,
                        0,
                    );
                }
                let error =
                    register_table(&context, "secret_orders", table.clone(), invalid_options)
                        .unwrap_err();
                assert_eq!(error.reason(), "scan_partition_target_must_be_positive");
                assert_and_take_provider_failure(
                    &collector,
                    "provider_registration",
                    &error,
                    backend,
                    0,
                );
                assert!(!context.table_exist("secret_orders")?);
                let error = register_table(
                    &context,
                    "secret_orders",
                    unsupported.clone(),
                    scan_options(backend),
                )
                .unwrap_err();
                assert_eq!(error.reason(), "unsupported_reader_version");
                assert_and_take_provider_failure(
                    &collector,
                    "provider_registration",
                    &error,
                    backend,
                    1,
                );
                assert!(!context.table_exist("secret_orders")?);

                register_table(
                    &context,
                    "secret_orders",
                    table.clone(),
                    scan_options(backend),
                )?;
                let registered = runtime.block_on(context.table_provider("secret_orders"))?;
                let error = register_table(
                    &context,
                    "secret_orders",
                    table.clone(),
                    scan_options(backend),
                )
                .unwrap_err();
                assert_eq!(error.reason(), "table_registration_failed");
                assert!(
                    error
                        .source()
                        .unwrap()
                        .downcast_ref::<Box<DataFusionError>>()
                        .is_some()
                );
                assert!(!format!("{:?}", collector.records()).contains("secret_orders"));
                assert_and_take_provider_failure(
                    &collector,
                    "provider_registration",
                    &error,
                    backend,
                    0,
                );
                assert!(Arc::ptr_eq(
                    &registered,
                    &runtime.block_on(context.table_provider("secret_orders"))?
                ));
                Ok(())
            })?;
        }
    }
    Ok(())
}

#[test]
fn planning_validation_and_metadata_failures_remain_distinguishable() -> TestResult {
    let runtime = runtime()?;
    for level in [Level::WARN, Level::ERROR] {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let fixture = LogTable::new()?;
            let table = runtime.block_on(fixture.builder(backend).load_table())?;
            let provider = DeltaTableProvider::try_new(table, scan_options(backend))?;
            let collector = TraceCollector::new(level);
            let context = SessionContext::new();
            with_default(collector.clone(), || -> TestResult {
                let unsupported = col("secret-column").eq(lit("secret-value"));
                assert_eq!(
                    provider.supports_filters_pushdown(&[&unsupported])?,
                    [TableProviderFilterPushDown::Unsupported]
                );
                assert!(collector.events().is_empty());
                let error = runtime
                    .block_on(provider.scan(&context.state(), None, &[unsupported], None))
                    .unwrap_err();
                assert_eq!(
                    expect_reader_error(&error).reason(),
                    "datafusion_scan_contains_unsupported_filter"
                );
                assert_and_take_provider_failure(
                    &collector,
                    "provider_scan",
                    expect_reader_error(&error),
                    backend,
                    0,
                );

                for (projection, reason) in [
                    (vec![1], "projection_index_out_of_bounds"),
                    (vec![0, 0], "duplicate_projection_index"),
                ] {
                    let error = runtime
                        .block_on(provider.scan(&context.state(), Some(&projection), &[], None))
                        .unwrap_err();
                    let error = expect_reader_error(&error);
                    assert_eq!(error.reason(), reason);
                    let event = assert_and_take_provider_failure(
                        &collector,
                        "provider_scan",
                        error,
                        backend,
                        0,
                    );
                    assert_eq!(event.fields["scan_metadata_source"], "delta_log");
                    assert_eq!(event.fields["plan_id"], event.fields["operation_id"]);
                    assert!(!event.fields.contains_key("partition_count"));
                }
                let log = fixture.0.join("_delta_log/00000000000000000000.json");
                for cause in ["not_found", "arrow_json"] {
                    if cause == "not_found" {
                        fs::remove_file(&log)?;
                    } else {
                        fs::write(&log, "secret-invalid-json\n")?;
                    }
                    let error = runtime
                        .block_on(provider.scan(&context.state(), None, &[], None))
                        .unwrap_err();
                    let error = expect_reader_error(&error);
                    assert_eq!(error.reason(), "kernel_scan_metadata_failed");
                    assert_eq!(error.source_kind(), Some(cause));
                    assert!(
                        error
                            .source()
                            .unwrap()
                            .downcast_ref::<delta_kernel::Error>()
                            .is_some()
                    );
                    assert_and_take_provider_failure(
                        &collector,
                        "provider_scan",
                        error,
                        backend,
                        0,
                    );
                }
                fixture.write_commit(0, &[protocol_action(1), metadata_action(false)])?;
                runtime.block_on(provider.scan(&context.state(), None, &[], None))?;
                assert!(collector.events().is_empty());
                Ok(())
            })?;
        }
    }
    Ok(())
}

#[test]
fn refresh_assigns_snapshot_and_provider_failures_to_their_own_boundaries() -> TestResult {
    let runtime = runtime()?;
    for level in [Level::WARN, Level::ERROR] {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let fixture = LogTable::new()?;
            let table = runtime.block_on(fixture.builder(backend).load_table())?;
            let provider = DeltaTableProvider::try_new(table, scan_options(backend))?;
            let collector = TraceCollector::new(level);
            with_default(collector.clone(), || -> TestResult {
                fs::write(
                    fixture.0.join("_delta_log/00000000000000000001.json"),
                    "secret-invalid-json\n",
                )?;
                let error = runtime.block_on(provider.refresh()).unwrap_err();
                assert_eq!(error.source_kind(), Some("arrow_json"));
                let event = assert_and_take_failure_event(&collector, "refresh", &error);
                assert_eq!(event.fields["previous_snapshot_version"], 0);

                fixture.write_commit(1, &[protocol_action(4)])?;
                let error = runtime.block_on(provider.refresh()).unwrap_err();
                assert_eq!(error.reason(), "unsupported_reader_version");
                let event = assert_and_take_provider_failure(
                    &collector,
                    "provider_refresh",
                    &error,
                    backend,
                    1,
                );
                assert_eq!(event.fields["previous_snapshot_version"], 0);
                assert!(format!("{provider:?}").contains("snapshot_version: 0"));
                let context = SessionContext::new();
                runtime.block_on(provider.scan(&context.state(), None, &[], None))?;
                fixture.write_commit(1, &[protocol_action(1)])?;
                let refreshed = runtime.block_on(provider.refresh())?;
                assert!(format!("{refreshed:?}").contains("snapshot_version: 1"));
                assert!(collector.events().is_empty());
                Ok(())
            })?;
        }
    }
    Ok(())
}

#[test]
fn provider_milestones_are_bounded_and_planning_ids_are_fresh() -> TestResult {
    let runtime = runtime()?;
    for backend in [
        ParquetReaderBackend::Direct,
        ParquetReaderBackend::DeltaKernel,
    ] {
        for warmup in [WarmupMode::None, WarmupMode::QueryPlanning] {
            let fixture = LogTable::new()?;
            let table =
                runtime.block_on(fixture.builder(backend).with_warmup(warmup).load_table())?;
            let collector = TraceCollector::new(Level::INFO);
            with_default(collector.clone(), || -> TestResult {
                let provider = DeltaTableProvider::try_new(table.clone(), scan_options(backend))?;
                let context = SessionContext::new();
                register_table(&context, "secret_orders", table, scan_options(backend))?;
                runtime.block_on(provider.refresh())?;
                for operation in [
                    "provider_creation",
                    "provider_registration",
                    "refresh",
                    "provider_refresh",
                ] {
                    let events: Vec<_> = collector
                        .events()
                        .into_iter()
                        .filter(|event| event.fields["operation"] == operation)
                        .collect();
                    assert_eq!(events.len(), 2, "{events:?}");
                    assert!(events.iter().all(|event| event.level == Level::INFO));
                    assert_eq!(events[0].fields["outcome"], "started");
                    assert_eq!(events[1].fields["outcome"], "completed");
                    assert_eq!(
                        events[0].fields["operation_id"],
                        events[1].fields["operation_id"]
                    );
                }
                collector.clear_records();
                runtime.block_on(async {
                    let state = context.state();
                    let (first, second) = futures_util::future::join(
                        provider.scan(&state, None, &[], None),
                        provider.scan(&state, None, &[], None),
                    )
                    .await;
                    assert_eq!(
                        first?.properties().output_partitioning().partition_count(),
                        1
                    );
                    assert_eq!(
                        second?.properties().output_partitioning().partition_count(),
                        1
                    );
                    provider.scan(&state, None, &[], None).await?;
                    Ok::<_, Box<dyn Error>>(())
                })?;
                let events = collector.events();
                assert_eq!(events.len(), 6, "{events:?}");
                let mut ids = std::collections::HashSet::new();
                for start in events
                    .iter()
                    .filter(|event| event.fields["outcome"] == "started")
                {
                    assert_eq!(start.fields["event"], "provider_scan.started");
                    assert!(ids.insert(start.fields["operation_id"].as_u64().unwrap()));
                    let completed: Vec<_> = events
                        .iter()
                        .filter(|event| {
                            event.fields["outcome"] == "completed"
                                && event.fields["operation_id"] == start.fields["operation_id"]
                        })
                        .collect();
                    assert_eq!(completed.len(), 1);
                    assert_eq!(completed[0].fields["event"], "provider_scan.completed");
                    assert_eq!(completed[0].fields["partition_count"], 1);
                    assert_eq!(
                        completed[0].fields["scan_metadata_source"],
                        if warmup == WarmupMode::None {
                            "delta_log"
                        } else {
                            "query_planning_cache"
                        }
                    );
                    assert_eq!(completed[0].fields["plan_id"], start.fields["operation_id"]);
                }
                assert_eq!(ids.len(), 3);
                Ok(())
            })?;
        }
    }
    Ok(())
}

#[test]
fn debug_provider_diagnostics_do_not_expose_inputs_or_sources() -> TestResult {
    let runtime = runtime()?;
    let fixture = LogTable::new()?;
    let table = runtime.block_on(fixture.builder(ParquetReaderBackend::Direct).load_table())?;
    let collector = TraceCollector::new(Level::DEBUG);
    with_default(collector.clone(), || -> TestResult {
        let context = SessionContext::new();
        register_table(
            &context,
            "secret_orders",
            table.clone(),
            ScanOptions::default(),
        )?;
        assert!(
            register_table(
                &context,
                "secret_orders",
                table.clone(),
                ScanOptions::default()
            )
            .is_err()
        );
        let provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
        let filters = [col("secret-column").eq(lit("secret-value"))];
        assert!(
            runtime
                .block_on(provider.scan(&context.state(), None, &filters, None))
                .is_err()
        );
        fs::remove_file(fixture.0.join("_delta_log/00000000000000000000.json"))?;
        assert!(
            runtime
                .block_on(provider.scan(&context.state(), None, &[], None))
                .is_err()
        );
        let captured = format!("{:?}", collector.records());
        assert!(!captured.contains("secret-"), "{captured}");
        assert!(!captured.contains("secret_orders"), "{captured}");
        assert_eq!(
            collector
                .events()
                .iter()
                .filter(|event| event.level == Level::ERROR)
                .count(),
            3
        );
        Ok(())
    })
}

#[test]
fn planning_worker_panics_emit_one_typed_terminal_failure() -> TestResult {
    let runtime = runtime()?;
    let fixture = LogTable::new()?;
    let table = runtime.block_on(fixture.builder(ParquetReaderBackend::Direct).load_table())?;
    let provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
    for name in ["Delta scan planning", "Delta scan execution setup"] {
        let mut collector = TraceCollector::new(Level::DEBUG);
        let observed = collector.clone();
        collector.on_capture = Some(Arc::new(move || {
            assert!(
                !observed
                    .records()
                    .last()
                    .is_some_and(|record| record.is_span && record.name == name),
                "secret-worker-panic"
            );
        }));
        let context = SessionContext::new();
        let error = runtime
            .block_on(
                provider
                    .scan(&context.state(), None, &[], None)
                    .with_subscriber(collector.clone()),
            )
            .unwrap_err();
        let reader = expect_reader_error(&error);
        assert_eq!(reader.reason(), "datafusion_scan_planning_task_failed");
        assert!(
            reader
                .source()
                .unwrap()
                .downcast_ref::<tokio::task::JoinError>()
                .unwrap()
                .is_panic()
        );
        let failures: Vec<_> = collector
            .events()
            .into_iter()
            .filter(|event| event.level == Level::ERROR)
            .collect();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].fields["event"], "provider_scan.failed");
        assert_eq!(failures[0].fields["error_source_kind"], "worker_panicked");
        if name == "Delta scan execution setup" {
            assert_eq!(failures[0].fields["partition_count"], 1);
        } else {
            assert!(!failures[0].fields.contains_key("partition_count"));
        }
        assert!(
            !collector
                .events()
                .iter()
                .any(|event| event.fields.get("outcome") == Some(&json!("completed")))
        );
        assert!(!format!("{:?}", collector.records()).contains("secret-"));
        collector.assert_no_entered_spans();
    }
    Ok(())
}

#[test]
fn abandoned_provider_planning_cannot_emit_late_success() -> TestResult {
    let runtime = runtime()?;
    let fixture = LogTable::new()?;
    let table = runtime.block_on(fixture.builder(ParquetReaderBackend::Direct).load_table())?;
    let provider = DeltaTableProvider::try_new(table, ScanOptions::default())?;
    let (entered, started) = std::sync::mpsc::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let wait = Mutex::new(wait);
    let mut collector = TraceCollector::new(Level::DEBUG);
    let observed = collector.clone();
    collector.on_capture = Some(Arc::new(move || {
        if observed
            .records()
            .last()
            .is_some_and(|record| record.is_span && record.name == "Delta scan planning")
        {
            entered.send(()).unwrap();
            wait.lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }
    }));
    let context = SessionContext::new();
    let state = context.state();
    with_default(collector.clone(), || {
        drop(provider.scan(&state, None, &[], None))
    });
    with_default(collector.clone(), || drop(provider.refresh()));
    assert!(collector.events().is_empty());
    let parent = with_default(
        collector.clone(),
        || tracing::info_span!(target: "delta_arrow_reader::test", "caller"),
    );
    let parent_id = parent.id().unwrap().into_u64();
    let mut future = Box::pin(
        provider
            .scan(&state, None, &[], None)
            .instrument(parent)
            .with_subscriber(collector.clone()),
    );
    runtime.block_on(async {
        assert!(future.as_mut().now_or_never().is_none());
    });
    started.recv_timeout(Duration::from_secs(5))?;
    drop(future);
    release.send(())?;
    drop(runtime);
    let events: Vec<_> = collector
        .events()
        .into_iter()
        .filter(|event| event.fields.get("operation") == Some(&json!("provider_scan")))
        .collect();
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(events[0].fields["outcome"], "started");
    assert_eq!(events[1].fields["outcome"], "abandoned");
    assert_eq!(
        events[0].fields["operation_id"],
        events[1].fields["operation_id"]
    );
    assert!(
        events
            .iter()
            .all(|event| event.level == Level::INFO && collector.linked_to(event, parent_id))
    );
    collector.assert_no_entered_spans();
    Ok(())
}
