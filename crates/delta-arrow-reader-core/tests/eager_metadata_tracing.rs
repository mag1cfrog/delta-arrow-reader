//! End-to-end tracing coverage for public eager metadata initialization.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use delta_arrow_reader::{DeltaStorageOptions, DeltaTableBuilder};
use tracing::{
    Event, Level, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    subscriber::Interest,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const TRACING_TARGET: &str = "delta_arrow_reader";
const PROTOCOL_JSON: &str = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#;
const METADATA_JSON: &str = r#"{"metaData":{"id":"tracing-test","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1587968585495}}"#;
const FIRST_OBJECT_KEY: &str = "secret-first-object.parquet";
const SECOND_OBJECT_KEY: &str = "secret-second-object.parquet";
const MALFORMED_OBJECT_KEY: &str = "secret-malformed-object.parquet";
const INVALID_SIZE: &str = "secret-not-a-file-size";
const STORAGE_VALUE: &str = "secret-storage-value";

#[derive(Clone, Default)]
struct EventCollector(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

impl EventCollector {
    fn events(&self) -> Vec<BTreeMap<String, String>> {
        self.0
            .lock()
            .map(|events| events.clone())
            .unwrap_or_default()
    }
}

impl Subscriber for EventCollector {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if metadata.target() == TRACING_TARGET && *metadata.level() == Level::DEBUG {
            Interest::always()
        } else {
            Interest::sometimes()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == TRACING_TARGET && *metadata.level() == Level::DEBUG
    }

    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        if visitor
            .0
            .get("event")
            .is_some_and(|name| name.starts_with("scan_metadata_cache_build."))
            && let Ok(mut events) = self.0.lock()
        {
            events.push(visitor.0);
        }
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

#[derive(Default)]
struct FieldVisitor(BTreeMap<String, String>);

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.0.insert(field.name().to_owned(), value.to_string());
    }
}

struct DeltaLogTable(PathBuf);

impl DeltaLogTable {
    fn new(name: &str, add_actions: &str) -> TestResult<Self> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = Path::new("target")
            .join("delta-arrow-reader-eager-tracing-tests")
            .join(format!("{}-{name}-{nanos}", std::process::id()));
        fs::create_dir_all(path.join("_delta_log"))?;
        fs::write(
            path.join("_delta_log/00000000000000000000.json"),
            [PROTOCOL_JSON, METADATA_JSON, add_actions, ""].join("\n"),
        )?;
        Ok(Self(path))
    }

    fn uri(&self) -> String {
        self.0.to_string_lossy().into_owned()
    }
}

impl Drop for DeltaLogTable {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn storage_options() -> DeltaStorageOptions {
    [("secret-option".to_owned(), STORAGE_VALUE.to_owned())]
        .into_iter()
        .collect()
}

#[test]
fn public_eager_initialization_emits_complete_structured_lifecycle_events() -> TestResult {
    let success = DeltaLogTable::new(
        "success",
        concat!(
            "{\"add\":{\"path\":\"secret-first-object.parquet\",\"partitionValues\":{},\"size\":10,\"modificationTime\":1587968586000,\"dataChange\":true}}\n",
            "{\"add\":{\"path\":\"secret-second-object.parquet\",\"partitionValues\":{},\"size\":10,\"modificationTime\":1587968586001,\"dataChange\":true}}"
        ),
    )?;
    let failure = DeltaLogTable::new(
        "failure",
        "{\"add\":{\"path\":\"secret-malformed-object.parquet\",\"partitionValues\":{},\"size\":\"secret-not-a-file-size\",\"modificationTime\":1587968586000,\"dataChange\":true}}",
    )?;
    let collector = EventCollector::default();
    tracing::subscriber::set_global_default(collector.clone())?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let table = runtime.block_on(
        DeltaTableBuilder::new(success.uri())
            .with_storage_options(storage_options())
            .with_warmup(delta_arrow_reader::WarmupMode::QueryPlanning)
            .load_table(),
    )?;
    assert_eq!(table.version(), 0);
    let error = match runtime.block_on(
        DeltaTableBuilder::new(failure.uri())
            .with_storage_options(storage_options())
            .with_warmup(delta_arrow_reader::WarmupMode::QueryPlanning)
            .load_table(),
    ) {
        Ok(_) => return Err("malformed eager metadata should fail".into()),
        Err(error) => error,
    };
    assert_eq!(error.code(), "scan_planning");

    let events = collector.events();
    let [started, completed, failure_started, failed] = events.as_slice() else {
        return Err(format!("expected four cache-build events, got {}", events.len()).into());
    };
    assert_eq!(
        started,
        &BTreeMap::from([
            (
                "event".to_owned(),
                "scan_metadata_cache_build.started".to_owned(),
            ),
            ("outcome".to_owned(), "started".to_owned()),
            ("snapshot_version".to_owned(), "0".to_owned()),
        ])
    );
    assert_eq!(
        completed.get("event").map(String::as_str),
        Some("scan_metadata_cache_build.completed")
    );
    assert_eq!(completed.len(), 6);
    assert_eq!(
        completed.get("outcome").map(String::as_str),
        Some("completed")
    );
    assert_eq!(
        completed.get("snapshot_version").map(String::as_str),
        Some("0")
    );
    assert_eq!(
        completed.get("cached_file_count").map(String::as_str),
        Some("2")
    );
    assert!(
        completed
            .get("cached_batch_count")
            .ok_or("cached batch count missing")?
            .parse::<usize>()?
            > 0
    );
    completed
        .get("elapsed_micros")
        .ok_or("success elapsed time missing")?
        .parse::<u128>()?;
    assert_eq!(
        failure_started,
        &BTreeMap::from([
            (
                "event".to_owned(),
                "scan_metadata_cache_build.started".to_owned(),
            ),
            ("outcome".to_owned(), "started".to_owned()),
            ("snapshot_version".to_owned(), "0".to_owned()),
        ])
    );
    assert_eq!(
        failed.get("event").map(String::as_str),
        Some("scan_metadata_cache_build.failed")
    );
    assert_eq!(failed.len(), 6);
    assert_eq!(failed.get("outcome").map(String::as_str), Some("failed"));
    assert_eq!(
        failed.get("snapshot_version").map(String::as_str),
        Some("0")
    );
    assert_eq!(
        failed.get("error_code").map(String::as_str),
        Some("scan_planning")
    );
    assert_eq!(
        failed.get("error_phase").map(String::as_str),
        Some("scan_planning")
    );
    failed
        .get("elapsed_micros")
        .ok_or("failure elapsed time missing")?
        .parse::<u128>()?;

    let captured = format!("{events:?}");
    for sensitive in [
        success.uri(),
        failure.uri(),
        FIRST_OBJECT_KEY.to_owned(),
        SECOND_OBJECT_KEY.to_owned(),
        MALFORMED_OBJECT_KEY.to_owned(),
        INVALID_SIZE.to_owned(),
        STORAGE_VALUE.to_owned(),
    ] {
        assert!(!captured.contains(&sensitive));
    }
    Ok(())
}
