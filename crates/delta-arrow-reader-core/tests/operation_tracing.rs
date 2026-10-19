//! Public operation diagnostics with lower-level spans disabled.
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

#[path = "support/tracing.rs"]
mod capture;

use capture::{Captured, Collector};
use delta_arrow_reader::{
    DeltaComparison, DeltaPredicate, DeltaReaderError, DeltaScalar, DeltaScanExecutionOptions,
    DeltaSnapshotSelection, DeltaStorageOptions, DeltaTableBuilder, ParquetReaderBackend,
    WarmupMode,
};
use serde_json::{Value, json};
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tracing::{Level, subscriber::with_default};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

struct LogTable(PathBuf);
impl LogTable {
    fn new() -> TestResult<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = Path::new("target").join("operation-tracing").join(format!(
            "secret-table-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(root.join("_delta_log"))?;
        let table = Self(root);
        table.write(0, &[protocol(1), metadata(false)])?;
        Ok(table)
    }
    fn write(&self, version: u64, actions: &[Value]) -> TestResult {
        fs::write(
            self.0.join(format!("_delta_log/{version:020}.json")),
            actions
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )?;
        Ok(())
    }
    fn builder(&self, backend: ParquetReaderBackend) -> DeltaTableBuilder {
        DeltaTableBuilder::new(self.0.to_string_lossy())
            .with_warmup(WarmupMode::None)
            .with_execution_options(
                DeltaScanExecutionOptions::default().with_parquet_backend(backend),
            )
            .with_storage_options(DeltaStorageOptions::from([(
                "secret-option".into(),
                "secret-credential".into(),
            )]))
    }
}
impl Drop for LogTable {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn protocol(version: u64) -> Value {
    json!({"protocol":{"minReaderVersion":version,"minWriterVersion":2}})
}
fn metadata(invalid: bool) -> Value {
    let field = if invalid {
        json!({"name":"secret-column", "type":{"type":"array", "elementType":"string", "containsNull":true}, "nullable":true,"metadata":{"delta.columnMapping.nested.ids":"secret-invalid-metadata"}})
    } else {
        json!({"name":"id","type":"integer","nullable":true,"metadata":{}})
    };
    json!({"metaData":{"id":"test","format":{"provider":"parquet","options":{}},"schemaString":json!({"type":"struct","fields":[field]}).to_string(),"partitionColumns":[],"configuration":{},"createdTime":0}})
}
fn malformed_add() -> Value {
    json!({"add":{"path":"secret-file.parquet","partitionValues":{},"size":"secret-invalid-size","modificationTime":0,"dataChange":true}})
}
fn runtime() -> TestResult<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?)
}

fn failure(collector: &Collector, operation: &str, error: &DeltaReaderError) -> Captured {
    let events = collector.events();
    assert_eq!(events.len(), 1, "{events:?}");
    let event = events.into_iter().next().unwrap();
    assert_eq!(event.level, Level::ERROR);
    assert_eq!(event.target, "delta_arrow_reader");
    assert_eq!(event.fields["operation"], operation);
    assert_eq!(event.fields["outcome"], "failed");
    assert_eq!(event.fields["error_phase"], error.phase().as_str());
    assert_eq!(event.fields["error_code"], error.code());
    assert_eq!(event.fields["error_reason"], error.reason());
    assert_eq!(
        event
            .fields
            .get("error_source_kind")
            .and_then(Value::as_str),
        error.source_kind()
    );
    assert!(event.fields["operation_id"].as_u64().is_some());
    assert!(event.fields["elapsed_micros"].as_u64().is_some());
    assert!(collector.records().iter().all(|event| !event.is_span));
    assert!(!format!("{:?}", collector.records()).contains("secret-"));
    collector.clear();
    event
}

#[test]
fn load_failures_are_self_contained_at_warn_and_error() -> TestResult {
    let runtime = runtime()?;
    for level in [Level::WARN, Level::ERROR] {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let collector = Collector::new(level);
            let fixture = LogTable::new()?;
            with_default(collector.clone(), || -> TestResult {
                let error = runtime
                    .block_on(DeltaTableBuilder::new("").load_table())
                    .unwrap_err();
                assert_eq!(error.reason(), "empty_table_location");
                let event = failure(&collector, "load_table", &error);
                assert!(!event.fields.contains_key("snapshot_version"));
                assert!(!event.fields.contains_key("requested_snapshot_version"));

                let error = runtime
                    .block_on(
                        fixture
                            .builder(backend)
                            .with_warmup(WarmupMode::QueryPlanning)
                            .load_snapshot(),
                    )
                    .unwrap_err();
                assert_eq!(
                    error.reason(),
                    "snapshot_load_does_not_support_table_warmup"
                );
                failure(&collector, "load_snapshot", &error);

                let mut identities = Vec::new();
                for _ in 0..2 {
                    let error = runtime
                        .block_on(
                            fixture
                                .builder(backend)
                                .with_snapshot_selection(DeltaSnapshotSelection::Version(99))
                                .load_table(),
                        )
                        .unwrap_err();
                    assert_eq!(error.source_kind(), Some("missing_version"));
                    assert!(error.source().is_some());
                    let event = failure(&collector, "load_table", &error);
                    assert_eq!(event.fields["requested_snapshot_version"], 99);
                    assert!(!event.fields.contains_key("snapshot_version"));
                    identities.push(event.fields["operation_id"].clone());
                }
                assert_ne!(identities[0], identities[1]);

                fs::write(
                    fixture.0.join("_delta_log/00000000000000000000.json"),
                    "secret-malformed-json",
                )?;
                let error = runtime
                    .block_on(fixture.builder(backend).load_snapshot())
                    .unwrap_err();
                assert_eq!(error.source_kind(), Some("arrow_json"));
                failure(&collector, "load_snapshot", &error);
                fs::remove_file(fixture.0.join("_delta_log/00000000000000000000.json"))?;
                let error = runtime
                    .block_on(fixture.builder(backend).load_table())
                    .unwrap_err();
                assert_eq!(error.source_kind(), Some("unknown"));
                failure(&collector, "load_table", &error);
                Ok(())
            })?;
        }
    }
    Ok(())
}

#[test]
fn conversion_and_required_metadata_failures_keep_resolved_versions() -> TestResult {
    let runtime = runtime()?;
    for level in [Level::WARN, Level::ERROR] {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let collector = Collector::new(level);
            let fixture = LogTable::new()?;
            fixture.write(1, &[json!({"commitInfo":{"timestamp":1}})])?;
            fixture.write(2, &[json!({"commitInfo":{"timestamp":2}})])?;
            fixture.write(3, &[metadata(true)])?;
            with_default(collector.clone(), || -> TestResult {
                let snapshot = runtime.block_on(fixture.builder(backend).load_snapshot())?;
                assert_eq!(snapshot.version(), 3);
                let error = snapshot.into_table().unwrap_err();
                assert_eq!(error.code(), "schema_conversion");
                let event = failure(&collector, "into_table", &error);
                assert_eq!(event.fields["snapshot_version"], 3);

                let error = runtime
                    .block_on(fixture.builder(backend).load_table())
                    .unwrap_err();
                let event = failure(&collector, "load_table", &error);
                assert_eq!(event.fields["snapshot_version"], 3);
                assert!(!event.fields.contains_key("requested_snapshot_version"));

                fixture.write(3, &[metadata(false), malformed_add()])?;
                let error = runtime
                    .block_on(
                        fixture
                            .builder(backend)
                            .with_warmup(WarmupMode::QueryPlanning)
                            .load_table(),
                    )
                    .unwrap_err();
                assert_eq!(
                    error.reason(),
                    "query_planning_warmup_materialization_failed"
                );
                let event = failure(&collector, "load_table", &error);
                assert_eq!(event.fields["snapshot_version"], 3);
                Ok(())
            })?;
        }
    }
    Ok(())
}

#[test]
fn refresh_failures_leave_the_original_table_usable() -> TestResult {
    let runtime = runtime()?;
    for level in [Level::WARN, Level::ERROR] {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let collector = Collector::new(level);
            let fixture = LogTable::new()?;
            with_default(collector.clone(), || -> TestResult {
                let table = runtime.block_on(
                    fixture
                        .builder(backend)
                        .with_warmup(WarmupMode::QueryPlanning)
                        .load_table(),
                )?;
                for (actions, reason) in [
                    (vec![metadata(true)], "schema_conversion_failed"),
                    (vec![malformed_add()], "query_planning_cache_refresh_failed"),
                ] {
                    fixture.write(1, &actions)?;
                    let error = runtime.block_on(table.refresh()).unwrap_err();
                    assert_eq!(error.reason(), reason);
                    let event = failure(&collector, "refresh", &error);
                    assert_eq!(event.fields["previous_snapshot_version"], 0);
                    assert_eq!(event.fields["snapshot_version"], 1);
                    assert_eq!(table.version(), 0);
                    let mut stream = runtime.block_on(table.scan().build())?.into_stream();
                    assert!(runtime.block_on(stream.next_batch())?.is_none());
                }
                fixture.write(1, &[json!({"commitInfo":{"timestamp":1}})])?;
                assert_eq!(runtime.block_on(table.refresh())?.version(), 1);
                assert_eq!(table.version(), 0);
                assert!(collector.events().is_empty());
                Ok(())
            })?;
        }
    }
    Ok(())
}

#[test]
fn planning_owns_validation_failures_but_pure_helpers_are_quiet() -> TestResult {
    let runtime = runtime()?;
    for level in [Level::WARN, Level::ERROR] {
        for backend in [
            ParquetReaderBackend::Direct,
            ParquetReaderBackend::DeltaKernel,
        ] {
            let collector = Collector::new(level);
            let fixture = LogTable::new()?;
            with_default(collector.clone(), || -> TestResult {
                let table = runtime.block_on(fixture.builder(backend).load_table())?;
                assert!(
                    table
                        .predicate_column_type("secret-missing-column")
                        .is_err()
                );
                assert!(table.scan().with_target_partitions(0).is_err());
                assert!(collector.events().is_empty());
                for scan in [
                    table.scan().with_projection(["secret-missing-column"]),
                    table.scan().with_predicate(DeltaPredicate::Compare {
                        column: "secret-missing-column".into(),
                        op: DeltaComparison::Eq,
                        value: DeltaScalar::Int32(1),
                    }),
                ] {
                    let error = runtime.block_on(scan.build()).err().unwrap();
                    let event = failure(&collector, "scan_planning", &error);
                    assert_eq!(event.fields["snapshot_version"], 0);
                    assert_eq!(event.fields["plan_id"], event.fields["operation_id"]);
                    assert_eq!(event.fields["backend"], format!("{backend:?}"));
                    assert_eq!(event.fields["scan_metadata_source"], "delta_log");
                    assert!(!event.fields.contains_key("partition_count"));
                }
                fixture.write(1, &[protocol(4)])?;
                let unsupported = runtime.block_on(fixture.builder(backend).load_table())?;
                assert!(unsupported.validate_protocol().is_err());
                let snapshot = runtime.block_on(fixture.builder(backend).load_snapshot())?;
                assert!(snapshot.validate_protocol().is_err());
                assert!(collector.events().is_empty());
                let error = runtime.block_on(unsupported.scan().build()).err().unwrap();
                assert_eq!(error.code(), "unsupported_protocol");
                failure(&collector, "scan_planning", &error);
                let error = runtime
                    .block_on(
                        fixture
                            .builder(backend)
                            .with_warmup(WarmupMode::QueryPlanning)
                            .load_table(),
                    )
                    .unwrap_err();
                assert_eq!(error.code(), "unsupported_protocol");
                failure(&collector, "load_table", &error);
                Ok(())
            })?;
        }
    }
    Ok(())
}

#[test]
fn concurrent_plans_have_distinct_ids_and_link_to_execution() -> TestResult {
    let runtime = runtime()?;
    let fixture = LogTable::new()?;
    let collector = Collector::new(Level::TRACE);
    with_default(collector.clone(), || -> TestResult {
        let snapshot = runtime.block_on(
            fixture
                .builder(ParquetReaderBackend::Direct)
                .load_snapshot(),
        )?;
        let table = snapshot.into_table()?;
        let refreshed = runtime.block_on(table.refresh())?;
        let (first, second) = runtime.block_on(async {
            futures_util::join!(table.scan().build(), refreshed.scan().build())
        });
        for plan in [first?, second?] {
            assert!(runtime.block_on(plan.into_stream().next_batch())?.is_none());
        }
        let events = collector.events();
        let plans: Vec<_> = events
            .iter()
            .filter(|event| event.fields.get("event") == Some(&json!("scan_planning.completed")))
            .collect();
        assert_eq!(plans.len(), 2);
        assert_ne!(
            plans[0].fields["operation_id"],
            plans[1].fields["operation_id"]
        );
        for plan in plans {
            assert_eq!(plan.level, Level::INFO);
            assert_eq!(plan.fields["partition_count"], 0);
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.fields.get("event")
                        == Some(&json!("scan_execution.completed"))
                        && event.fields["plan_id"] == plan.fields["plan_id"])
                    .count(),
                1
            );
        }
        for operation in ["load_snapshot", "into_table", "refresh"] {
            let milestones: Vec<_> = events
                .iter()
                .filter(|event| event.fields.get("operation") == Some(&json!(operation)))
                .collect();
            assert_eq!(milestones.len(), 2);
            assert_eq!(milestones[0].fields["outcome"], "started");
            assert_eq!(milestones[1].fields["outcome"], "completed");
            assert_eq!(
                milestones[0].fields["operation_id"],
                milestones[1].fields["operation_id"]
            );
            assert!(milestones[1].fields["elapsed_micros"].as_u64().is_some());
        }
        assert!(!format!("{:?}", collector.records()).contains("secret-"));
        Ok(())
    })
}

#[test]
fn storage_setup_preserves_typed_causes_and_worker_panics() -> TestResult {
    use delta_kernel_default_engine::storage::insert_url_handler;
    insert_url_handler(
        "trace-denied",
        Arc::new(|_, _| {
            Err(object_store::Error::PermissionDenied {
                path: "secret-path".into(),
                source: "secret-source".into(),
            })
        }),
    )?;
    insert_url_handler(
        "trace-missing",
        Arc::new(|_, _| {
            Err(object_store::Error::NotFound {
                path: "secret-path".into(),
                source: "secret-source".into(),
            })
        }),
    )?;
    insert_url_handler(
        "trace-panic",
        Arc::new(|_, _| panic!("secret-panic-payload")),
    )?;
    let runtime = runtime()?;
    for level in [Level::WARN, Level::ERROR] {
        let collector = Collector::new(level);
        with_default(collector.clone(), || -> TestResult {
            for (scheme, cause) in [
                ("trace-denied", "permission_denied"),
                ("trace-missing", "not_found"),
                ("trace-panic", "worker_panicked"),
            ] {
                let error = runtime
                    .block_on(
                        DeltaTableBuilder::new(format!("{scheme}://secret-host/secret-table"))
                            .load_table(),
                    )
                    .unwrap_err();
                assert_eq!(error.source_kind(), Some(cause));
                assert!(error.source().is_some());
                let event = failure(&collector, "load_table", &error);
                assert!(!event.fields.contains_key("snapshot_version"));
            }
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn dropping_a_pending_load_never_reports_late_success() -> TestResult {
    let fixture = LogTable::new()?;
    let root = fixture.0.clone();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    delta_kernel_default_engine::storage::insert_url_handler(
        "trace-blocked",
        Arc::new(move |_, _| {
            entered_tx.send(()).unwrap();
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            Ok((
                Box::new(object_store::local::LocalFileSystem::new_with_prefix(
                    &root,
                )?),
                object_store::path::Path::default(),
            ))
        }),
    )?;
    let runtime = runtime()?;
    let owner = Collector::new(Level::INFO);
    let unrelated = Collector::new(Level::INFO);
    let mut future = Box::pin(DeltaTableBuilder::new("trace-blocked://secret-host/").load_table());
    with_default(owner.clone(), || {
        runtime.block_on(async {
            assert!(futures_util::poll!(&mut future).is_pending());
        })
    });
    entered_rx.recv_timeout(Duration::from_secs(5))?;
    with_default(unrelated.clone(), || drop(future));
    release_tx.send(())?;
    // Runtime shutdown waits for the blocking load, making late completion deterministic.
    drop(runtime);
    let events = owner.events();
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(events[0].fields["outcome"], "started");
    assert_eq!(events[1].fields["outcome"], "abandoned");
    assert_eq!(
        events[0].fields["operation_id"],
        events[1].fields["operation_id"]
    );
    assert!(unrelated.events().is_empty());
    with_default(owner.clone(), || {
        owner.clear();
        drop(DeltaTableBuilder::new("").load_table());
        drop(DeltaTableBuilder::new("").load_snapshot());
        assert!(owner.events().is_empty());
    });
    Ok(())
}
