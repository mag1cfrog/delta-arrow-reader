//! Read-only admission rejects commands before real Delta scans or host mutations.

use arrow::{
    array::{Array, Int32Array},
    datatypes::DataType,
};
use bytes::Bytes;
use datafusion::{
    catalog::{MemorySchemaProvider, TableFunctionArgs, TableFunctionImpl},
    common::{ScalarValue, TableReference},
    datasource::TableProvider,
    execution::SessionStateBuilder,
    logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
    object_store::{ObjectStore, ObjectStoreExt, path::Path as StorePath},
    physical_plan::collect,
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader::{
    DeltaTableBuilder, WarmupMode,
    datafusion::{DeltaTableProvider, ScanOptions},
};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[allow(dead_code)]
#[path = "support/delta_reads.rs"]
mod delta_reads;
#[allow(dead_code)]
#[path = "../../delta-arrow-reader-core/tests/reader/support.rs"]
mod reader_support;
#[path = "support/spark_results.rs"]
mod spark_results;
use delta_reads::{ProviderCalls, RecordingDeltaProvider, RecordingLocalStore, TestResult};
use reader_support::RealParquetDeltaTable;
use spark_results::{
    capture_schema, encode_value, remove_nullability_and_metadata, validate_batch,
};

struct AdmissionFixture {
    table: RealParquetDeltaTable,
    context: SessionContext,
    store: Arc<RecordingLocalStore>,
    source: Arc<RecordingDeltaProvider>,
    path_canary: Arc<RecordingDeltaProvider>,
    scalar_calls: Arc<AtomicUsize>,
    table_function_calls: Arc<AtomicUsize>,
    host_file_bytes: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct FileReadingHostDeltaTableFunction {
    provider: Arc<RecordingDeltaProvider>,
    calls: Arc<AtomicUsize>,
    data_file: PathBuf,
    bytes_read: Arc<AtomicUsize>,
}

impl TableFunctionImpl for FileReadingHostDeltaTableFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        assert!(args.exprs().is_empty());
        self.calls.fetch_add(1, Ordering::Relaxed);
        let bytes = fs::read(&self.data_file)
            .map_err(|error| datafusion::common::DataFusionError::External(Box::new(error)))?;
        assert_eq!(&bytes[..4], b"PAR1");
        self.bytes_read.fetch_add(bytes.len(), Ordering::Relaxed);
        Ok(self.provider.clone())
    }
}

fn native_add_one(name: &str, calls: Arc<AtomicUsize>) -> ScalarUDF {
    create_udf(
        name,
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(move |args| {
            calls.fetch_add(1, Ordering::Relaxed);
            match &args[0] {
                ColumnarValue::Array(array) => {
                    let values = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                        datafusion::common::DataFusionError::Execution("expected INT input".into())
                    })?;
                    Ok(ColumnarValue::Array(Arc::new(Int32Array::from_iter(
                        values.iter().map(|value| value.map(|value| value + 1)),
                    ))))
                }
                ColumnarValue::Scalar(ScalarValue::Int32(value)) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Int32(value.map(|value| value + 1)),
                )),
                _ => Err(datafusion::common::DataFusionError::Execution(
                    "expected INT input".into(),
                )),
            }
        }),
    )
}

fn case_key(case: &Value) -> TestResult<(&str, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing case ID")?,
        case["ansi"].as_bool().ok_or("missing ANSI mode")?,
    ))
}

impl AdmissionFixture {
    async fn new() -> TestResult<Self> {
        let table = RealParquetDeltaTable::new_default("read-only-admission")?;
        let (uri, store) = RecordingLocalStore::register(table.path())?;
        let loaded = DeltaTableBuilder::new(uri)
            .with_warmup(WarmupMode::QueryPlanning)
            .load_table()
            .await?;
        let provider = DeltaTableProvider::try_new(
            loaded,
            ScanOptions {
                target_partitions: Some(1),
                use_arrow_view_types: false,
                ..Default::default()
            },
        )?;
        let recording = |inner| {
            Arc::new(RecordingDeltaProvider {
                inner,
                calls: Arc::new(Mutex::new(ProviderCalls::default())),
                schema_calls: Default::default(),
            })
        };
        let source = recording(provider.clone());
        let path_canary = recording(provider);
        let context = SessionContext::new_with_state(
            configure_spark_sql(
                SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(SessionConfig::new().with_target_partitions(1)),
            )
            .build(),
        );
        context.register_table("source", source.clone())?;
        for schema in [
            "parquet",
            "delta",
            "json",
            "csv",
            "orc",
            "avro",
            "text",
            "global_temp",
        ] {
            context
                .catalog("datafusion")
                .ok_or("missing catalog")?
                .register_schema(schema, Arc::new(MemorySchemaProvider::new()))?;
            for path in [
                "/guarded/source.parquet",
                "/guarded/source.delta",
                "relative/source.parquet",
                "source.parquet",
                "file:/guarded/source.parquet",
                "s3://bucket/secret_payload.parquet",
                "./relative_table",
            ] {
                context
                    .register_table(TableReference::partial(schema, path), path_canary.clone())?;
            }
            context.register_table(
                TableReference::partial(schema, "registered_table"),
                source.clone(),
            )?;
        }
        for name in ["events.json", "orders.csv"] {
            context.register_table(TableReference::partial("public", name), source.clone())?;
        }
        let view = context.sql("SELECT id FROM source").await?.into_view();
        context.register_table("source_view", view)?;
        let scalar_calls = Arc::new(AtomicUsize::new(0));
        for name in ["host_add_one", "external_func"] {
            context.register_udf(native_add_one(name, Arc::clone(&scalar_calls)));
        }
        let table_function_calls = Arc::new(AtomicUsize::new(0));
        let host_file_bytes = Arc::new(AtomicUsize::new(0));
        context.register_udtf(
            "host_rows",
            Arc::new(FileReadingHostDeltaTableFunction {
                provider: source.clone(),
                calls: table_function_calls.clone(),
                data_file: table.path().join(table.data_file_path()),
                bytes_read: host_file_bytes.clone(),
            }),
        );
        let fixture = Self {
            table,
            context,
            store,
            source,
            path_canary,
            scalar_calls,
            table_function_calls,
            host_file_bytes,
        };
        fixture.reset_calls()?;
        Ok(fixture)
    }

    fn reset_calls(&self) -> TestResult {
        for provider in [&self.source, &self.path_canary] {
            *provider
                .calls
                .lock()
                .map_err(|_| "provider lock poisoned")? = Default::default();
            provider.schema_calls.store(0, Ordering::Relaxed);
        }
        self.store
            .reads
            .lock()
            .map_err(|_| "store read lock poisoned")?
            .clear();
        self.store.read_calls.store(0, Ordering::Relaxed);
        self.store.write_calls.store(0, Ordering::Relaxed);
        self.scalar_calls.store(0, Ordering::Relaxed);
        self.table_function_calls.store(0, Ordering::Relaxed);
        self.host_file_bytes.store(0, Ordering::Relaxed);
        Ok(())
    }

    fn assert_no_io(&self, label: &str) -> TestResult {
        for provider in [&self.source, &self.path_canary] {
            let calls = provider
                .calls
                .lock()
                .map_err(|_| "provider lock poisoned")?;
            assert!(calls.scans.is_empty(), "{label}: created a Delta scan");
            assert!(
                calls.offered_filters.is_empty(),
                "{label}: reached physical filter pushdown"
            );
        }
        assert_eq!(
            self.store.read_calls.load(Ordering::Relaxed),
            0,
            "{label}: object-store read"
        );
        assert_eq!(
            self.store.write_calls.load(Ordering::Relaxed),
            0,
            "{label}: object-store write"
        );
        assert!(
            self.store
                .reads
                .lock()
                .map_err(|_| "store read lock poisoned")?
                .is_empty()
        );
        assert_eq!(
            self.scalar_calls.load(Ordering::Relaxed),
            0,
            "{label}: ran a host scalar function"
        );
        assert_eq!(
            self.table_function_calls.load(Ordering::Relaxed),
            0,
            "{label}: invoked a host table function"
        );
        assert_eq!(
            self.host_file_bytes.load(Ordering::Relaxed),
            0,
            "{label}: host callback read fixture data"
        );
        assert_eq!(
            self.path_canary.schema_calls.load(Ordering::Relaxed),
            0,
            "{label}: resolved the forbidden path canary"
        );
        Ok(())
    }
}

async fn host_state(context: &SessionContext) -> TestResult<Value> {
    let mut catalogs = BTreeMap::new();
    for name in context.catalog_names() {
        let catalog = context.catalog(&name).ok_or("missing catalog")?;
        let mut schemas = BTreeMap::new();
        for schema_name in catalog.schema_names() {
            let schema = catalog.schema(&schema_name).ok_or("missing schema")?;
            let mut tables = BTreeMap::new();
            for table_name in schema.table_names() {
                let provider = schema.table(&table_name).await?.ok_or("missing provider")?;
                tables.insert(table_name, format!("{:p}", Arc::as_ptr(&provider)));
            }
            schemas.insert(schema_name, tables);
        }
        catalogs.insert(name, schemas);
    }
    let state = context.state();
    let scalar = state
        .scalar_functions()
        .iter()
        .map(|(name, function)| (name.clone(), format!("{:p}", Arc::as_ptr(function))))
        .collect::<BTreeMap<_, _>>();
    let table = state
        .table_functions()
        .iter()
        .map(|(name, function)| (name.clone(), format!("{:p}", Arc::as_ptr(function))))
        .collect::<BTreeMap<_, _>>();
    let settings = state
        .config()
        .options()
        .entries()
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect::<BTreeMap<_, _>>();
    Ok(
        json!({"catalogs":catalogs,"scalar_functions":scalar,"table_functions":table,"settings":settings}),
    )
}

fn fixture_files(root: &Path) -> TestResult<BTreeMap<String, Vec<u8>>> {
    fn visit(root: &Path, directory: &Path, files: &mut BTreeMap<String, Vec<u8>>) -> TestResult {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                visit(root, &path, files)?;
            } else {
                files.insert(
                    path.strip_prefix(root)?.to_string_lossy().into_owned(),
                    fs::read(path)?,
                );
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    visit(root, root, &mut files)?;
    Ok(files)
}

async fn calibrate_observers(fixture: &AdmissionFixture) -> TestResult {
    let path = StorePath::from("observer-control.txt");
    let copied = StorePath::from("observer-control-copy.txt");
    let before = fixture.store.write_calls.load(Ordering::Relaxed);
    fixture
        .store
        .put(&path, Bytes::from_static(b"observer-control").into())
        .await?;
    assert!(fixture.store.write_calls.load(Ordering::Relaxed) > before);
    assert_eq!(
        fixture.store.get(&path).await?.bytes().await?,
        Bytes::from_static(b"observer-control")
    );
    assert!(fixture.store.read_calls.load(Ordering::Relaxed) > 0);
    let before = fixture.store.write_calls.load(Ordering::Relaxed);
    fixture
        .store
        .copy_opts(&path, &copied, Default::default())
        .await?;
    assert!(fixture.store.write_calls.load(Ordering::Relaxed) > before);
    for path in [&path, &copied] {
        let before = fixture.store.write_calls.load(Ordering::Relaxed);
        fixture.store.delete(path).await?;
        assert!(fixture.store.write_calls.load(Ordering::Relaxed) > before);
    }
    fixture.reset_calls()?;
    let batches = fixture
        .context
        .sql("SELECT id FROM parquet.`/guarded/source.parquet` ORDER BY id")
        .await?
        .collect()
        .await?;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3
    );
    assert!(fixture.path_canary.schema_calls.load(Ordering::Relaxed) > 0);
    assert!(
        !fixture
            .path_canary
            .calls
            .lock()
            .map_err(|_| "provider lock poisoned")?
            .scans
            .is_empty()
    );
    assert!(fixture.store.read_calls.load(Ordering::Relaxed) > 0);
    assert!(
        !fixture
            .store
            .reads
            .lock()
            .map_err(|_| "store read lock poisoned")?
            .is_empty()
    );
    fixture.reset_calls()?;
    Ok(())
}

#[tokio::test]
async fn rejected_sql_never_scans_reads_writes_or_changes_host_state() -> TestResult {
    let fixture = AdmissionFixture::new().await?;
    calibrate_observers(&fixture).await?;
    let initial_state = host_state(&fixture.context).await?;
    let initial_files = fixture_files(fixture.table.path())?;
    for ansi_mode in [false, true] {
        let options = SparkSqlOptions {
            ansi_mode,
            variable_substitution_values: [
                (
                    "payload".to_string(),
                    "SELECT id FROM source; DELETE FROM source".to_string(),
                ),
                ("nested".to_string(), "${payload}".to_string()),
            ]
            .into(),
            ..Default::default()
        };
        for (name, sql) in rejected_queries() {
            fixture.reset_calls()?;
            let error = plan_query(&fixture.context, &sql, &options)
                .await
                .err()
                .ok_or_else(|| format!("admitted {name}"))?;
            let message = error.to_string().to_ascii_lowercase();
            assert!(
                message.contains("not supported")
                    || message.contains("unsupported")
                    || message.contains("not implemented")
                    || message.contains("one statement"),
                "{name}: rejection must explain its unsupported kind: {error}"
            );
            assert!(
                !message.contains("/guarded/") && !message.contains("secret_payload"),
                "{name}: diagnostic exposed SQL literals"
            );
            fixture.assert_no_io(&name)?;
            assert_eq!(
                host_state(&fixture.context).await?,
                initial_state,
                "{name}: host state changed"
            );
            assert_eq!(
                fixture_files(fixture.table.path())?,
                initial_files,
                "{name}: fixture bytes changed"
            );
            println!(
                "READ_ONLY_REJECTION {}",
                json!({"case":name,"ansi":ansi_mode,"error":error.to_string(),"scan_calls":0,"store_reads":0,"store_writes":0,"catalog_unchanged":true,"fixture_unchanged":true})
            );
        }
        for sql in [
            "SELECT (1 +",
            "WITH q AS (SELECT id FROM source) SELECT (id + FROM q",
            "SELECT transform(array(1),x ->) FROM source s JOIN (SELECT id FROM source) USING (id)",
        ] {
            fixture.reset_calls()?;
            let error = plan_query(&fixture.context, sql, &options)
                .await
                .err()
                .ok_or("invalid SELECT admitted")?;
            assert!(
                error.to_string().contains("error in SQL parser"),
                "ordinary query syntax errors must stay parser errors: {error}"
            );
            fixture.assert_no_io("invalid SELECT")?;
        }
    }
    Ok(())
}

async fn execute_control(
    fixture: &AdmissionFixture,
    sql: &str,
    options: &SparkSqlOptions,
    ordered: bool,
) -> TestResult<Value> {
    fixture.reset_calls()?;
    let query = plan_query(&fixture.context, sql, options).await?;
    assert!(
        fixture
            .source
            .calls
            .lock()
            .map_err(|_| "provider lock poisoned")?
            .scans
            .is_empty()
    );
    assert_eq!(
        fixture.store.read_calls.load(Ordering::Relaxed),
        0,
        "logical planning must not read objects"
    );
    assert_eq!(fixture.store.write_calls.load(Ordering::Relaxed), 0);
    let physical = query.create_physical_plan(&fixture.context).await?;
    let schema = physical.schema();
    let mut rows = vec![];
    for batch in collect(physical, fixture.context.task_ctx()).await? {
        validate_batch(schema.as_ref(), &batch)
            .map_err(|error| error as Box<dyn std::error::Error>)?;
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| encode_value(column.as_ref(), row))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error as Box<dyn std::error::Error>)?,
            );
        }
    }
    if !ordered {
        rows.sort_by_key(|row| json!(row).to_string());
    }
    let mut schema = capture_schema(schema.fields(), None)
        .map_err(|error| error as Box<dyn std::error::Error>)?;
    remove_nullability_and_metadata(&mut schema);
    assert_eq!(fixture.store.write_calls.load(Ordering::Relaxed), 0);
    Ok(json!({"schema":schema,"rows":rows}))
}

#[tokio::test]
async fn admitted_queries_and_host_native_functions_keep_spark_results() -> TestResult {
    let fixture = AdmissionFixture::new().await?;
    fixture.context.register_udf(native_add_one(
        "python_eval",
        Arc::clone(&fixture.scalar_calls),
    ));
    let request: Value =
        serde_json::from_str(include_str!("fixtures/read-only-admission/requests.json"))?;
    let records = include_str!("fixtures/read-only-admission/spark-reference.jsonl")
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records.len() - 1,
        request["expected_record_count"]
            .as_u64()
            .ok_or("missing count")? as usize
    );
    let requested = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing requested cases")?;
    let requested_keys = requested
        .iter()
        .map(case_key)
        .collect::<TestResult<BTreeSet<_>>>()?;
    let observed_keys = records[1..]
        .iter()
        .map(case_key)
        .collect::<TestResult<BTreeSet<_>>>()?;
    assert_eq!(
        requested_keys.len(),
        requested.len(),
        "duplicate requested control"
    );
    assert_eq!(
        observed_keys.len(),
        records.len() - 1,
        "duplicate captured control"
    );
    assert_eq!(
        observed_keys, requested_keys,
        "reference must cover every requested control and ANSI mode"
    );
    assert_eq!(
        records[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    for case in &records[1..] {
        assert_eq!(case["status"], "ok");
        let id = case["id"].as_str().ok_or("missing ID")?;
        let ansi_mode = case["ansi"].as_bool().ok_or("missing ANSI")?;
        let source = request["corpora"][0]["cases"]
            .as_array()
            .ok_or("missing requested cases")?
            .iter()
            .find(|source| source["id"] == case["id"] && source["ansi"] == case["ansi"])
            .ok_or("unexpected reference")?;
        assert_eq!(source["sql"], case["sql"]);
        let mut options = SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        };
        let sql = if id == "disabled_substitution_literal" {
            options.variable_substitution_enabled = false;
            options.variable_substitution_values.insert(
                "payload".to_string(),
                "SELECT 1; DELETE FROM source".to_string(),
            );
            "SELECT '${payload}' AS marker"
        } else {
            case["sql"].as_str().ok_or("missing SQL")?
        };
        let actual = execute_control(
            &fixture,
            sql,
            &options,
            case["ordered"].as_bool() == Some(true),
        )
        .await?;
        let mut expected_schema = case["schema_stages"]["executed"].clone();
        remove_nullability_and_metadata(&mut expected_schema);
        assert_eq!(
            actual["rows"], case["expected"]["rows"],
            "{id}/ansi={ansi_mode}"
        );
        assert_eq!(actual["schema"], expected_schema, "{id}/ansi={ansi_mode}");
        if source["reads_delta"] == true {
            assert!(
                !fixture
                    .source
                    .calls
                    .lock()
                    .map_err(|_| "provider lock poisoned")?
                    .scans
                    .is_empty()
            );
            assert!(fixture.store.read_calls.load(Ordering::Relaxed) > 0);
            assert!(
                !fixture
                    .store
                    .reads
                    .lock()
                    .map_err(|_| "store read lock poisoned")?
                    .is_empty()
            );
        }
        if id == "qualified" {
            for sql in [
                "SELECT id FROM parquet.registered_table ORDER BY id",
                "SELECT id FROM delta.registered_table ORDER BY id",
                "SELECT id FROM public.`events.json` ORDER BY id",
                "SELECT id FROM public.`orders.csv` ORDER BY id",
                "SELECT id FROM datafusion.public.source ORDER BY id",
                "SELECT id FROM IDENTIFIER('source') ORDER BY id",
                "SELECT id FROM IDENTIFIER(concat('sou','rce')) ORDER BY id",
                "SELECT id FROM host_rows() ORDER BY id",
            ] {
                assert_eq!(
                    execute_control(&fixture, sql, &options, true).await?,
                    actual,
                    "host registration control: {sql}"
                );
                if sql.contains("host_rows") {
                    assert!(fixture.table_function_calls.load(Ordering::Relaxed) > 0);
                    assert!(fixture.host_file_bytes.load(Ordering::Relaxed) > 0);
                }
            }
        }
        if id == "host_arithmetic_reference" {
            for function in ["host_add_one", "python_eval"] {
                assert_eq!(
                    execute_control(
                        &fixture,
                        &format!("SELECT {function}(id) AS value FROM source ORDER BY id"),
                        &options,
                        true
                    )
                    .await?,
                    actual,
                    "host-native arithmetic equivalent"
                );
                assert!(fixture.scalar_calls.load(Ordering::Relaxed) > 0);
            }
        }
        println!(
            "READ_ONLY_CONTROL {}",
            json!({"case":id,"ansi":ansi_mode,"reference_sql":case["sql"],"candidate_sql":sql,"actual":actual})
        );
    }
    Ok(())
}

fn rejected_queries() -> Vec<(String, String)> {
    let mut queries = REJECTED_STATEMENTS
        .iter()
        .map(|(name, sql)| ((*name).to_string(), (*sql).to_string()))
        .collect::<Vec<_>>();
    for (name, sql) in [
        (
            "mixed_raw_literal_identifier",
            "SELECT l.id FROM host_rows() l JOIN IDENTIFIER(r'parquet.`/guarded/source.parquet`') r ON l.id=r.id",
        ),
        (
            "mixed_adjacent_literal_identifier",
            "SELECT l.id FROM host_rows() l JOIN IDENTIFIER('parquet.' '`/guarded/source.parquet`') r ON l.id=r.id",
        ),
        (
            "mixed_nested_table_expression",
            "SELECT TABLE(parquet.`/guarded/source.parquet`) FROM host_rows()",
        ),
        (
            "mixed_static_join",
            "SELECT l.id FROM host_rows() l JOIN parquet.`/guarded/source.parquet` r ON l.id=r.id",
        ),
        (
            "mixed_static_union",
            "SELECT id FROM host_rows() UNION ALL SELECT id FROM parquet.`/guarded/source.parquet`",
        ),
        (
            "mixed_literal_identifier",
            "SELECT l.id FROM host_rows() l JOIN IDENTIFIER('parquet.`/guarded/source.parquet`') r ON l.id=r.id",
        ),
        (
            "mixed_parenthesized_literal_identifier",
            "SELECT l.id FROM host_rows() l JOIN IDENTIFIER(('parquet.`/guarded/source.parquet`')) r ON l.id=r.id",
        ),
        (
            "mixed_table_query",
            "SELECT id FROM host_rows() UNION ALL TABLE parquet.`/guarded/source.parquet`",
        ),
        (
            "mixed_table_expression",
            "SELECT TABLE parquet.`/guarded/source.parquet` FROM host_rows()",
        ),
        (
            "create_from_host_function",
            "CREATE TABLE created USING PARQUET AS SELECT id FROM host_rows()",
        ),
        (
            "parenthesized_script",
            "(SELECT TRANSFORM (id) USING 'python /guarded/script.py' AS (id INT) FROM source)",
        ),
        (
            "with_write",
            "WITH q AS (SELECT id FROM source) INSERT INTO source SELECT id FROM q",
        ),
        (
            "path_parquet",
            "SELECT id FROM parquet.`/guarded/source.parquet`",
        ),
        ("path_delta", "SELECT id FROM delta.`/guarded/source.delta`"),
        (
            "path_relative",
            "SELECT id FROM parquet.`relative/source.parquet`",
        ),
        ("path_filename", "SELECT id FROM parquet.`source.parquet`"),
        (
            "path_file_uri",
            "SELECT id FROM parquet.`file:/guarded/source.parquet`",
        ),
        (
            "path_remote_uri",
            "SELECT id FROM json.`s3://bucket/secret_payload.parquet`",
        ),
        (
            "path_delta_relative",
            "SELECT id FROM delta.`./relative_table`",
        ),
        (
            "path_dynamic",
            "SELECT id FROM IDENTIFIER('parquet.`/guarded/source.parquet`')",
        ),
        (
            "path_cte",
            "WITH q AS (SELECT id FROM parquet.`/guarded/source.parquet`) SELECT * FROM q",
        ),
        (
            "path_join",
            "SELECT s.id FROM source s JOIN parquet.`/guarded/source.parquet` p ON s.id=p.id",
        ),
        (
            "path_union",
            "SELECT id FROM source UNION ALL SELECT id FROM parquet.`/guarded/source.parquet`",
        ),
        (
            "path_scalar",
            "SELECT (SELECT max(id) FROM parquet.`/guarded/source.parquet`) AS value FROM source",
        ),
        (
            "with_script",
            "WITH q AS (SELECT TRANSFORM (id) USING 'python /guarded/script.py' AS (id INT) FROM source) SELECT * FROM q",
        ),
        ("external_table_function", "SELECT * FROM external_python()"),
        (
            "external_qualified_function",
            "SELECT system.python_udf(id) FROM source",
        ),
        (
            "external_java_method",
            "SELECT java_method('java.lang.System','getProperty','secret_payload') FROM source",
        ),
        (
            "multiple_literals",
            "SELECT 'TRUNCATE; VACUUM' AS marker; SELECT 2",
        ),
        ("recursive_substitution", "${nested}"),
        ("mixed_case_command", "/* SELECT 1 */ TrUnCaTe TABLE source"),
        ("use_database", "USE DATABASE public"),
        ("use_catalog", "USE CATALOG datafusion"),
        ("describe_table", "DESCRIBE TABLE source"),
        ("explain_analyze", "EXPLAIN ANALYZE SELECT * FROM source"),
        ("show_functions", "SHOW FUNCTIONS"),
    ] {
        queries.push((name.to_string(), sql.to_string()));
    }
    queries
}

const REJECTED_STATEMENTS: &[(&str, &str)] = &[
    (
        "jvm_reflection",
        "SELECT reflect('java.lang.System','getProperty','secret_payload') FROM source",
    ),
    ("insert", "INSERT INTO source SELECT * FROM source"),
    (
        "update",
        "UPDATE source SET customer_name='changed' WHERE id=1",
    ),
    ("delete", "DELETE FROM source WHERE id=1"),
    (
        "merge",
        "MERGE INTO source t USING source s ON t.id=s.id WHEN MATCHED THEN DELETE",
    ),
    ("truncate", "TRUNCATE TABLE source"),
    (
        "copy",
        "COPY INTO source FROM '/guarded/input' FILEFORMAT=PARQUET",
    ),
    ("create_table", "CREATE TABLE created (id INT)"),
    (
        "create_table_query",
        "CREATE TABLE created USING PARQUET AS SELECT id FROM source",
    ),
    ("alter_table", "ALTER TABLE source RENAME TO changed"),
    ("drop_table", "DROP TABLE source"),
    (
        "create_view",
        "CREATE VIEW created_view AS SELECT * FROM source",
    ),
    (
        "create_temp_view",
        "CREATE OR REPLACE TEMPORARY VIEW source_view AS SELECT * FROM source",
    ),
    (
        "alter_view",
        "ALTER VIEW source_view AS SELECT id FROM source",
    ),
    ("drop_view", "DROP VIEW source_view"),
    ("create_database", "CREATE DATABASE created_database"),
    (
        "alter_database",
        "ALTER DATABASE public SET DBPROPERTIES ('owner'='changed')",
    ),
    ("drop_database", "DROP DATABASE public CASCADE"),
    (
        "create_function",
        "CREATE TEMPORARY FUNCTION external_func AS 'example.External' USING JAR 'file:/guarded/external.jar'",
    ),
    (
        "alter_function",
        "ALTER FUNCTION external_func SET PROPERTIES ('a'='b')",
    ),
    (
        "drop_function",
        "DROP TEMPORARY FUNCTION IF EXISTS external_func",
    ),
    ("optimize", "OPTIMIZE source"),
    ("vacuum", "VACUUM source"),
    ("analyze", "ANALYZE TABLE source COMPUTE STATISTICS"),
    ("refresh", "REFRESH TABLE source"),
    ("cache", "CACHE TABLE source"),
    ("uncache", "UNCACHE TABLE source"),
    ("set", "SET spark.sql.ansi.enabled=false"),
    ("reset", "RESET spark.sql.ansi.enabled"),
    ("use", "USE public"),
    ("multiple", "SELECT id FROM source; DELETE FROM source"),
    ("substituted_multiple", "${payload}"),
    (
        "python_function",
        "CREATE TEMPORARY FUNCTION external_func AS 'def f(x): return x' LANGUAGE PYTHON",
    ),
    (
        "python_script",
        "SELECT TRANSFORM (id) USING 'python /guarded/script.py' AS (id INT) FROM source",
    ),
    ("explain", "EXPLAIN SELECT id FROM source"),
    ("describe", "DESCRIBE source"),
    ("show", "SHOW TABLES"),
];
