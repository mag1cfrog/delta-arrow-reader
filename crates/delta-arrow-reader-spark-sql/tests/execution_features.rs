//! Spark execution features over range sources and actual Delta tables.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, atomic::AtomicUsize},
};

use arrow::{
    array::{Array, Int32Array, Int64Array, ListArray, StringArray, StructArray},
    buffer::NullBuffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    execution::SessionStateBuilder,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, collect_partitioned,
        execution_plan::reset_plan_states,
    },
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader::{
    DeltaTableBuilder, WarmupMode,
    datafusion::{DeltaTableProvider, ScanOptions, collect_scan_metrics},
};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/delta_reads.rs"]
mod delta_reads;
#[allow(dead_code)]
#[path = "../../delta-arrow-reader-core/tests/reader/support.rs"]
mod reader_support;
#[allow(dead_code)]
#[path = "support/spark_inputs.rs"]
mod spark_inputs;
#[path = "support/spark_results.rs"]
mod spark_results;

use delta_reads::{ProviderCalls, RecordingDeltaProvider, RecordingLocalStore, TestResult};
use reader_support::RealParquetDeltaTable;
use spark_results::{
    capture_executed_query_result, capture_schema, remove_nullability_and_metadata, validate_batch,
};

const ROWS_PER_FILE: usize = 2_000;

fn context(partitions: usize, batch_size: usize) -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(partitions)
                        .with_repartition_file_min_size(1)
                        .with_batch_size(batch_size),
                ),
        )
        .build(),
    )
}

fn case_key(case: &Value) -> TestResult<(String, bool)> {
    Ok((
        case["id"].as_str().ok_or("missing ID")?.into(),
        case["ansi"].as_bool().ok_or("missing ANSI")?,
    ))
}

fn references() -> TestResult<Vec<Value>> {
    read_references(
        include_str!("fixtures/execution-features/requests.json"),
        include_str!("fixtures/execution-features/spark-reference.jsonl"),
    )
}

fn local_sort_filter_references() -> TestResult<Vec<Value>> {
    read_references(
        include_str!("fixtures/execution-features/local-sort-filter-requests.json"),
        include_str!("fixtures/execution-features/local-sort-filter-spark-reference.jsonl"),
    )
}

fn distribution_references() -> TestResult<Vec<Value>> {
    read_references(
        include_str!("fixtures/execution-features/distribution-requests.json"),
        include_str!("fixtures/execution-features/distribution-spark-reference.jsonl"),
    )
}

fn read_references(request: &str, records: &str) -> TestResult<Vec<Value>> {
    let request: Value = serde_json::from_str(request)?;
    let records = records
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(
        records[0]["corpora"][0]["metadata"],
        request["corpora"][0]["metadata"]
    );
    let requested = request["corpora"][0]["cases"]
        .as_array()
        .ok_or("missing cases")?;
    assert_eq!(
        records.len() - 1,
        request["expected_record_count"]
            .as_u64()
            .ok_or("missing count")? as usize
    );
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
        "duplicate requested case"
    );
    assert_eq!(
        observed_keys.len(),
        records.len() - 1,
        "duplicate captured case"
    );
    assert_eq!(
        observed_keys, requested_keys,
        "every case and ANSI mode must be captured"
    );
    for observed in &records[1..] {
        let requested = requested
            .iter()
            .find(|case| case["id"] == observed["id"] && case["ansi"] == observed["ansi"])
            .ok_or("unexpected case")?;
        assert_eq!(observed["sql"], requested["sql"]);
        assert_eq!(observed["ordered"], requested["ordered"]);
        assert_eq!(observed["range_partitions"], requested["range_partitions"]);
        assert_eq!(observed["status"], "ok");
    }
    Ok(records.into_iter().skip(1).collect())
}

fn reference<'a>(records: &'a [Value], id: &str, ansi_mode: bool) -> TestResult<&'a Value> {
    records
        .iter()
        .find(|case| case["id"] == id && case["ansi"] == ansi_mode)
        .ok_or_else(|| format!("missing {id}/ANSI={ansi_mode}").into())
}

fn assert_executed_schema(schema: &Schema, reference: &Value) -> TestResult {
    let mut actual = capture_schema(schema.fields(), None)
        .map_err(|error| error as Box<dyn std::error::Error>)?;
    let mut expected = reference["schema_stages"]["executed"].clone();
    remove_nullability_and_metadata(&mut actual);
    remove_nullability_and_metadata(&mut expected);
    assert_eq!(actual, expected);
    Ok(())
}

async fn create_spark_physical_plan(
    session: &SessionContext,
    sql: &str,
    ansi_mode: bool,
) -> TestResult<Arc<dyn ExecutionPlan>> {
    Ok(plan_query(
        session,
        sql,
        &SparkSqlOptions {
            ansi_mode,
            ..Default::default()
        },
    )
    .await?
    .create_physical_plan(session)
    .await?)
}

#[tokio::test]
async fn bounded_ranges_match_spark_values_types_and_requested_partitions() -> TestResult {
    let records = references()?;
    let session = context(3, 113);
    for case in records.iter().filter(|case| {
        case["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("range_"))
    }) {
        let actual = capture_executed_query_result(&session, case)
            .await
            .map_err(|error| error as Box<dyn std::error::Error>)?;
        assert_eq!(actual["rows"], case["expected"]["rows"], "{}", case["id"]);
        let mut actual_schema = actual["schema"].clone();
        let mut expected = case["schema_stages"]["executed"].clone();
        remove_nullability_and_metadata(&mut actual_schema);
        remove_nullability_and_metadata(&mut expected);
        assert_eq!(actual_schema, expected, "{}", case["id"]);
        if let Some(partitions) = case["range_partitions"].as_u64()
            && !actual["rows"].as_array().ok_or("missing rows")?.is_empty()
        {
            let sql = case["sql"]
                .as_str()
                .ok_or("missing SQL")?
                .strip_suffix(" ORDER BY id")
                .ok_or("owned range control must have the explicit ORDER BY suffix")?;
            let plan = create_spark_physical_plan(
                &session,
                sql,
                case["ansi"].as_bool().ok_or("missing ANSI")?,
            )
            .await?;
            assert_eq!(
                plan.output_partitioning().partition_count(),
                partitions as usize
            );
        }
        println!(
            "EXECUTION_RANGE case={} ansi={} rows={}",
            case["id"],
            case["ansi"],
            actual["rows"].as_array().ok_or("missing rows")?.len()
        );
    }
    Ok(())
}

fn stored_metadata_fixture() -> TestResult<RealParquetDeltaTable> {
    let children = vec![Arc::new(Field::new("file_path", DataType::Utf8, true))].into();
    let metadata = StructArray::new(
        children,
        vec![Arc::new(StringArray::from(vec![
            "ordinary-a",
            "hidden-under-null",
            "ordinary-c",
        ]))],
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("_metadata", metadata.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(metadata),
        ],
    )?;
    let mut bytes = vec![];
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    let metadata = json!({"metaData":{"id":"ordinary-metadata-field","format":{"provider":"parquet","options":{}},"schemaString":json!({"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"_metadata","type":{"type":"struct","fields":[{"name":"file_path","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}).to_string(),"partitionColumns":[],"configuration":{},"createdTime":0}});
    RealParquetDeltaTable::new_with_raw_parquet(
        "stored-metadata-field",
        &bytes,
        3,
        &json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        &metadata,
    )
}

struct DeltaFeatureFixture {
    _files: Vec<RealParquetDeltaTable>,
    session: SessionContext,
    stores: Vec<Arc<RecordingLocalStore>>,
    calls: Vec<Arc<Mutex<ProviderCalls>>>,
}

impl DeltaFeatureFixture {
    async fn new(partitions: usize, batch_size: usize) -> TestResult<Self> {
        let files = vec![
            RealParquetDeltaTable::new_default("execution-default")?,
            RealParquetDeltaTable::new_with_two_large_files("execution-events", ROWS_PER_FILE)?,
            stored_metadata_fixture()?,
            RealParquetDeltaTable::new_with_two_partition_values("execution-partitioned")?,
        ];
        let session = context(partitions, batch_size);
        let mut stores = vec![];
        let mut calls = vec![];
        for (name, fixture) in ["source", "events", "stored_source", "partitioned_events"]
            .into_iter()
            .zip(&files)
        {
            let (uri, store) = RecordingLocalStore::register(fixture.path())?;
            let table = DeltaTableBuilder::new(uri)
                .with_warmup(WarmupMode::QueryPlanning)
                .load_table()
                .await?;
            let provider_calls = Arc::new(Mutex::new(ProviderCalls::default()));
            session.register_table(
                name,
                Arc::new(RecordingDeltaProvider {
                    inner: DeltaTableProvider::try_new(
                        table,
                        ScanOptions {
                            target_partitions: Some(partitions),
                            use_arrow_view_types: false,
                            ..Default::default()
                        },
                    )?,
                    calls: provider_calls.clone(),
                    schema_calls: Arc::new(AtomicUsize::new(0)),
                }),
            )?;
            calls.push(provider_calls);
            assert!(
                store
                    .reads
                    .lock()
                    .map_err(|_| "read log poisoned")?
                    .is_empty()
            );
            stores.push(store);
        }
        Ok(Self {
            _files: files,
            session,
            stores,
            calls,
        })
    }
}

#[tokio::test]
async fn unsupported_execution_features_keep_ordinary_metadata_fields_available() -> TestResult {
    let fixture = DeltaFeatureFixture::new(3, 7).await?;
    for ansi_mode in [false, true] {
        for (sql, feature) in [
            ("SELECT input_file_name() FROM source", "input_file_name"),
            ("SELECT _metadata.file_path FROM source", "_metadata"),
            ("SELECT s._metadata.file_path FROM source s", "_metadata"),
        ] {
            let error = match plan_query(
                &fixture.session,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..Default::default()
                },
            )
            .await
            {
                Err(error) => error.to_string(),
                Ok(_) => return Err("feature unexpectedly admitted".into()),
            };
            assert!(error.contains(feature), "{error}");
            assert!(
                error.contains("not implemented")
                    || error.contains("not supported")
                    || error.contains("missing from the schema"),
                "{error}"
            );
            for store in &fixture.stores {
                assert!(
                    store
                        .reads
                        .lock()
                        .map_err(|_| "read log poisoned")?
                        .is_empty()
                );
            }
            println!("EXECUTION_UNSUPPORTED ansi={ansi_mode} feature={feature} error={error}");
        }
    }
    let records = references()?;
    for case in records.iter().filter(|case| {
        ["stored_metadata", "alias_metadata", "table_alias_metadata"]
            .iter()
            .any(|id| case["id"] == *id)
    }) {
        let actual = capture_executed_query_result(&fixture.session, case)
            .await
            .map_err(|error| error as Box<dyn std::error::Error>)?;
        assert_eq!(actual["rows"], case["expected"]["rows"]);
        let mut schema = actual["schema"].clone();
        let mut expected = case["schema_stages"]["executed"].clone();
        remove_nullability_and_metadata(&mut schema);
        remove_nullability_and_metadata(&mut expected);
        assert_eq!(schema, expected);
    }
    assert!(
        fixture
            .stores
            .iter()
            .any(|store| store.reads.lock().is_ok_and(|reads| !reads.is_empty()))
    );
    Ok(())
}

fn as_int32_array(array: &dyn Array) -> TestResult<&Int32Array> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| "expected INT array".into())
}

#[tokio::test]
async fn partition_ids_and_monotonic_ids_keep_multibatch_partition_invariants() -> TestResult {
    let records = references()?;
    let mut layouts = BTreeSet::new();
    let mut maximum_batch_sizes = BTreeSet::new();
    for ansi_mode in [false, true] {
        for partitions in [1, 3] {
            for batch_size in [7, 113] {
                let fixture = DeltaFeatureFixture::new(partitions, batch_size).await?;
                let reference = reference(&records, "partition_id_types", ansi_mode)?;
                let sql = reference["sql"]
                    .as_str()
                    .ok_or("missing SQL")?
                    .replace("FROM source", "FROM events");
                let plan = create_spark_physical_plan(&fixture.session, &sql, ansi_mode).await?;
                assert_executed_schema(plan.schema().as_ref(), reference)?;
                let output_partitions = plan.output_partitioning().partition_count();
                layouts.insert(output_partitions);
                let scans = collect_scan_metrics(plan.as_ref());
                assert_eq!(scans.len(), 1);
                let batches = collect_partitioned(plan, fixture.session.task_ctx()).await?;
                let mut source_ids = vec![];
                let mut seen_monotonic_ids = BTreeSet::new();
                let mut seen_partition_ids = BTreeSet::new();
                let mut nonempty_partitions = 0;
                let mut largest_batch = 0;
                for (partition, batches) in batches.iter().enumerate() {
                    if batches.iter().any(|batch| batch.num_rows() > 0) {
                        nonempty_partitions += 1;
                        assert!(batches.len() > 1);
                    }
                    let mut previous = None;
                    for batch in batches {
                        validate_batch(batch.schema().as_ref(), batch)
                            .map_err(|error| error as Box<dyn std::error::Error>)?;
                        largest_batch = largest_batch.max(batch.num_rows());
                        assert!(batch.num_rows() <= batch_size);
                        let ids = as_int32_array(batch.column(0).as_ref())?;
                        let pids = as_int32_array(batch.column(1).as_ref())?;
                        let mids = batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or("expected BIGINT monotonic ID")?;
                        assert_eq!(ids.null_count(), 0);
                        assert_eq!(pids.null_count(), 0);
                        assert_eq!(mids.null_count(), 0);
                        for row in 0..batch.num_rows() {
                            let pid = pids.value(row);
                            assert!((0..output_partitions as i32).contains(&pid));
                            // This plain projection has no shuffle after ID evaluation.
                            assert_eq!(pid, partition as i32);
                            seen_partition_ids.insert(pid);
                            let mid = mids.value(row);
                            assert!(previous.is_none_or(|previous| mid > previous));
                            previous = Some(mid);
                            assert!(
                                seen_monotonic_ids.insert(mid),
                                "duplicate monotonic ID across batches/partitions"
                            );
                            source_ids.push(ids.value(row));
                        }
                    }
                }
                source_ids.sort_unstable();
                assert_eq!(
                    source_ids,
                    (1..=2 * ROWS_PER_FILE as i32).collect::<Vec<_>>()
                );
                assert_eq!(seen_monotonic_ids.len(), 2 * ROWS_PER_FILE);
                if partitions > 1 {
                    assert!(nonempty_partitions > 1);
                    assert!(seen_partition_ids.len() > 1);
                }
                assert_eq!(
                    scans[0].snapshot().configured_batch_size_rows,
                    Some(batch_size as u64)
                );
                maximum_batch_sizes.insert(largest_batch);
                println!(
                    "EXECUTION_IDS ansi={ansi_mode} requested={partitions} actual={output_partitions} nonempty={nonempty_partitions} batch_size={batch_size} actual_max_batch={largest_batch}"
                );
            }
        }
    }
    assert!(layouts.contains(&1) && layouts.iter().any(|count| *count > 1));
    assert!(
        maximum_batch_sizes.len() > 1,
        "must observe distinct batch layouts"
    );
    Ok(())
}

#[tokio::test]
async fn sort_by_orders_each_output_partition_and_preserves_all_rows() -> TestResult {
    let records = references()?;
    let mut layouts = BTreeSet::new();
    for ansi_mode in [false, true] {
        for partitions in [1, 3] {
            for batch_size in [7, 113] {
                let fixture = DeltaFeatureFixture::new(partitions, batch_size).await?;
                let reference = reference(&records, "sort_key_types", ansi_mode)?;
                let sql = reference["sql"]
                    .as_str()
                    .ok_or("missing SQL")?
                    .replace("FROM source", "FROM events");
                let plan = create_spark_physical_plan(&fixture.session, &sql, ansi_mode).await?;
                assert_executed_schema(plan.schema().as_ref(), reference)?;
                let output_partitions = plan.output_partitioning().partition_count();
                layouts.insert(output_partitions);
                let mut all_ids = vec![];
                let mut nonempty = 0;
                for batches in collect_partitioned(plan, fixture.session.task_ctx()).await? {
                    let mut previous = None;
                    if batches.iter().any(|batch| batch.num_rows() > 0) {
                        nonempty += 1;
                    }
                    for batch in batches {
                        validate_batch(batch.schema().as_ref(), &batch)
                            .map_err(|error| error as Box<dyn std::error::Error>)?;
                        let keys = as_int32_array(batch.column(0).as_ref())?;
                        let ids = as_int32_array(batch.column(1).as_ref())?;
                        assert_eq!(ids.null_count(), 0);
                        for row in 0..batch.num_rows() {
                            let id = ids.value(row);
                            let key = (!keys.is_null(row)).then(|| keys.value(row));
                            assert_eq!(key, (id % 5 != 0).then_some(id % 7));
                            let order = (key.is_some(), Reverse(key.unwrap_or_default()), id);
                            assert!(
                                previous.is_none_or(|previous| previous <= order),
                                "SORT BY order reset at a batch boundary"
                            );
                            previous = Some(order);
                            all_ids.push(id);
                        }
                    }
                }
                all_ids.sort_unstable();
                assert_eq!(all_ids, (1..=2 * ROWS_PER_FILE as i32).collect::<Vec<_>>());
                if partitions > 1 {
                    assert!(nonempty > 1);
                }
                println!(
                    "EXECUTION_SORT ansi={ansi_mode} actual_partitions={output_partitions} nonempty={nonempty} batch_size={batch_size}"
                );
            }
        }
    }
    assert!(layouts.contains(&1) && layouts.iter().any(|count| *count > 1));
    Ok(())
}

#[tokio::test]
async fn unordered_first_last_and_collect_list_preserve_membership_and_counts() -> TestResult {
    let records = references()?;
    let mut expected = BTreeMap::<i32, Vec<i32>>::new();
    for id in 1..=2 * ROWS_PER_FILE as i32 {
        expected
            .entry(id % 5)
            .or_default()
            .push(100 * (id % 5) + id % 7);
    }
    for members in expected.values_mut() {
        members.sort_unstable();
    }
    for ansi_mode in [false, true] {
        for partitions in [1, 3] {
            for batch_size in [7, 113] {
                let fixture = DeltaFeatureFixture::new(partitions, batch_size).await?;
                let reference = reference(&records, "aggregate_types", ansi_mode)?;
                let sql = reference["sql"]
                    .as_str()
                    .ok_or("missing SQL")?
                    .replace("FROM source", "FROM events");
                let plan = create_spark_physical_plan(&fixture.session, &sql, ansi_mode).await?;
                assert_executed_schema(plan.schema().as_ref(), reference)?;
                let mut groups = BTreeSet::new();
                for batches in collect_partitioned(plan, fixture.session.task_ctx()).await? {
                    for batch in batches {
                        validate_batch(batch.schema().as_ref(), &batch)
                            .map_err(|error| error as Box<dyn std::error::Error>)?;
                        let keys = as_int32_array(batch.column(0).as_ref())?;
                        let first = as_int32_array(batch.column(1).as_ref())?;
                        let last = as_int32_array(batch.column(2).as_ref())?;
                        let lists = batch
                            .column(3)
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .ok_or("expected collect_list ARRAY")?;
                        let counts = batch
                            .column(4)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or("expected BIGINT count")?;
                        assert!(
                            batch
                                .columns()
                                .iter()
                                .all(|column| column.null_count() == 0)
                        );
                        for row in 0..batch.num_rows() {
                            let key = keys.value(row);
                            assert!(groups.insert(key));
                            let expected =
                                expected.get(&key).ok_or("unexpected aggregate group")?;
                            assert!(expected.contains(&first.value(row)));
                            assert!(expected.contains(&last.value(row)));
                            let list = lists.value(row);
                            let values = as_int32_array(list.as_ref())?;
                            assert_eq!(values.null_count(), 0);
                            let mut members = values.values().to_vec();
                            members.sort_unstable();
                            assert_eq!(
                                &members, expected,
                                "collect_list must preserve duplicate multiplicities"
                            );
                            assert_eq!(counts.value(row), expected.len() as i64);
                        }
                    }
                }
                assert_eq!(groups, expected.keys().copied().collect());
                println!(
                    "EXECUTION_AGGREGATES ansi={ansi_mode} requested_partitions={partitions} batch_size={batch_size} groups={}",
                    groups.len()
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn distribution_clauses_match_spark_values_and_executed_types() -> TestResult {
    let records = distribution_references()?;
    let request: Value = serde_json::from_str(include_str!(
        "fixtures/execution-features/distribution-requests.json"
    ))?;
    for partitions in [1, 3] {
        for batch_size in [2, 7] {
            let session = context(partitions, batch_size);
            spark_inputs::register_recorded_inputs_with_partition_count(
                &session,
                &request["corpora"][0]["metadata"],
                batch_size,
                partitions,
            )
            .map_err(|error| error as Box<dyn std::error::Error>)?;
            for case in &records {
                println!(
                    "EXECUTION_DISTRIBUTION_CASE partitions={partitions} batch_size={batch_size} id={} ansi={}",
                    case["id"], case["ansi"]
                );
                let actual = capture_executed_query_result(&session, case)
                    .await
                    .map_err(|error| error as Box<dyn std::error::Error>)?;
                assert_eq!(actual["rows"], case["expected"]["rows"], "{}", case["id"]);
                let mut actual_schema = actual["schema"].clone();
                let mut expected = case["schema_stages"]["executed"].clone();
                remove_nullability_and_metadata(&mut actual_schema);
                remove_nullability_and_metadata(&mut expected);
                assert_eq!(actual_schema, expected, "{}", case["id"]);
            }
            println!(
                "EXECUTION_DISTRIBUTION_REFERENCE partitions={partitions} batch_size={batch_size} cases={}",
                records.len()
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn distribution_keys_sorting_and_ids_keep_multibatch_partition_invariants() -> TestResult {
    for ansi_mode in [false, true] {
        for partitions in [1, 3] {
            for batch_size in [7, 113] {
                let fixture = DeltaFeatureFixture::new(partitions, batch_size).await?;
                for clause in [
                    "DISTRIBUTE BY key",
                    "DISTRIBUTE BY 1",
                    "CLUSTER BY key",
                    "DISTRIBUTE BY key,id % 3",
                    "CLUSTER BY key,id % 3",
                    "DISTRIBUTE BY key SORT BY id DESC",
                    "DISTRIBUTE BY array(key)",
                    "CLUSTER BY array(key)",
                    "DISTRIBUTE BY named_struct('k',key,'a',array(key))",
                    "CLUSTER BY named_struct('k',key,'a',array(key))",
                ] {
                    let compound = clause.contains("key,id % 3");
                    let sql = format!(
                        "SELECT id,key,spark_partition_id() AS pid,monotonically_increasing_id() AS mid \
                         FROM (SELECT id,CASE WHEN id % 5 = 0 THEN NULL ELSE id % 7 END AS key \
                         FROM events {clause}) q"
                    );
                    let plan =
                        create_spark_physical_plan(&fixture.session, &sql, ansi_mode).await?;
                    assert_eq!(plan.output_partitioning().partition_count(), partitions);
                    assert_eq!(
                        plan.schema()
                            .fields()
                            .iter()
                            .map(|field| field.data_type())
                            .collect::<Vec<_>>(),
                        vec![
                            &DataType::Int32,
                            &DataType::Int32,
                            &DataType::Int32,
                            &DataType::Int64
                        ]
                    );
                    for run in 0..2 {
                        let execution = reset_plan_states(plan.clone())?;
                        let mut owners = BTreeMap::new();
                        let mut all_ids = vec![];
                        let mut monotonic_ids = BTreeSet::new();
                        let mut nonempty = 0;
                        for (partition, batches) in
                            collect_partitioned(execution, fixture.session.task_ctx())
                                .await?
                                .into_iter()
                                .enumerate()
                        {
                            if batches.iter().any(|batch| batch.num_rows() > 0) {
                                nonempty += 1;
                                assert!(batches.len() > 1, "must exercise batch boundaries");
                            }
                            let mut previous_key = None;
                            let mut previous_id = None;
                            let mut previous_mid = None;
                            for batch in batches {
                                validate_batch(batch.schema().as_ref(), &batch)
                                    .map_err(|error| error as Box<dyn std::error::Error>)?;
                                let ids = as_int32_array(batch.column(0).as_ref())?;
                                let keys = as_int32_array(batch.column(1).as_ref())?;
                                let pids = as_int32_array(batch.column(2).as_ref())?;
                                let mids = batch
                                    .column(3)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .ok_or("expected BIGINT monotonic ID")?;
                                assert_eq!(ids.null_count(), 0);
                                assert_eq!(pids.null_count(), 0);
                                assert_eq!(mids.null_count(), 0);
                                for row in 0..batch.num_rows() {
                                    let id = ids.value(row);
                                    let key = (!keys.is_null(row)).then(|| keys.value(row));
                                    assert_eq!(key, (id % 5 != 0).then_some(id % 7));
                                    let distribution_key = if clause == "DISTRIBUTE BY 1" {
                                        None
                                    } else {
                                        key
                                    };
                                    let secondary = compound.then_some(id % 3);
                                    assert_eq!(
                                        *owners
                                            .entry((distribution_key, secondary))
                                            .or_insert(partition),
                                        partition,
                                        "equal keys must share an output partition, including NULL"
                                    );
                                    assert_eq!(pids.value(row), partition as i32);
                                    let mid = mids.value(row);
                                    assert!(previous_mid.is_none_or(|previous| previous < mid));
                                    assert!(monotonic_ids.insert(mid));
                                    previous_mid = Some(mid);
                                    if clause.starts_with("CLUSTER") {
                                        assert!(
                                            previous_key.is_none_or(|previous| previous <= (key, secondary)),
                                            "CLUSTER BY order reset across batches"
                                        );
                                    }
                                    if clause.contains("SORT BY") {
                                        assert!(
                                            previous_id.is_none_or(|previous| previous >= id),
                                            "SORT BY order reset across batches"
                                        );
                                    }
                                    previous_key = Some((key, secondary));
                                    previous_id = Some(id);
                                    all_ids.push(id);
                                }
                            }
                        }
                        assert_eq!(
                            owners.len(),
                            if clause == "DISTRIBUTE BY 1" {
                                1
                            } else if compound {
                                24
                            } else {
                                8
                            }
                        );
                        assert!(owners.keys().any(|(key, _)| key.is_none()));
                        all_ids.sort_unstable();
                        assert_eq!(all_ids, (1..=2 * ROWS_PER_FILE as i32).collect::<Vec<_>>());
                        if clause == "DISTRIBUTE BY 1" {
                            assert_eq!(nonempty, 1, "a literal distribution key is constant");
                        } else if partitions > 1 {
                            assert!(nonempty > 1, "must exercise a real distributed layout");
                        }
                        println!(
                            "EXECUTION_DISTRIBUTION ansi={ansi_mode} partitions={partitions} nonempty={nonempty} batch_size={batch_size} clause={clause} run={run}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn hidden_sort_columns_cross_restoring_projections_and_keep_validation() -> TestResult {
    use datafusion::common::Column;
    use datafusion::datasource::{empty::EmptyTable, provider_as_source};
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, Partitioning, col};

    let source = || {
        LogicalPlanBuilder::scan(
            "input",
            provider_as_source(Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("k", DataType::Int32, true),
                Field::new("s", DataType::Utf8, true),
            ]))))),
            None,
        )
    };
    let input = source()?
        .project(vec![col("id"), col("k")])?
        .repartition(Partitioning::Hash(vec![col("k")], 3))?
        .project(vec![col("id")])?
        .build()?;
    let sorted = LogicalPlanBuilder::from(input)
        .sort(vec![col("s").sort(true, true)])?
        .build()?;
    let LogicalPlan::Projection(output) = sorted else {
        return Err("hidden sort requires an output projection".into());
    };
    let LogicalPlan::Sort(sort) = output.input.as_ref() else {
        return Err("expected sort below output projection".into());
    };
    assert!(
        sort.input
            .schema()
            .has_column(&Column::new(Some("input"), "s"))
    );
    assert_eq!(output.schema.fields().len(), 1);
    assert!(
        source()?
            .project(vec![col("id")])?
            .distinct()?
            .sort(vec![col("s").sort(true, true)])
            .is_err()
    );
    assert!(
        source()?
            .project(vec![col("id")])?
            .sort(vec![col("absent").sort(true, true)])
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn local_sort_and_distribution_preserve_delta_pruning() -> TestResult {
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{Expr, Operator};
    for ansi_mode in [false, true] {
        let mut baseline_paths = None;
        for sql in [
            "SELECT id FROM partitioned_events WHERE region = 'us-west'",
            "SELECT id FROM (SELECT * FROM partitioned_events DISTRIBUTE BY id) q WHERE region = 'us-west'",
            "SELECT id FROM (SELECT * FROM partitioned_events SORT BY id) q WHERE region = 'us-west'",
            "SELECT id FROM (SELECT * FROM partitioned_events CLUSTER BY id) q WHERE region = 'us-west'",
        ] {
            let fixture = DeltaFeatureFixture::new(3, 7).await?;
            let case = json!({"sql":sql,"ansi":ansi_mode,"ordered":false});
            let actual = capture_executed_query_result(&fixture.session, &case)
                .await
                .map_err(|error| error as Box<dyn std::error::Error>)?;
            assert_eq!(actual["rows"], json!([["1"], ["2"]]));
            let mut schema = actual["schema"].clone();
            remove_nullability_and_metadata(&mut schema);
            assert_eq!(
                schema,
                json!({"type":"struct","fields":[{"name":"id","type":"integer"}]})
            );
            let calls = fixture.calls[3]
                .lock()
                .map_err(|_| "provider call lock poisoned")?;
            assert_eq!(calls.scans.len(), 1);
            assert_eq!(
                calls.scans[0].projected_columns,
                BTreeSet::from(["id".into()])
            );
            assert_eq!(calls.scans[0].filters.len(), 1);
            assert!(!calls.offered_filters.is_empty());
            for predicate in calls.offered_filters.iter().chain(&calls.scans[0].filters) {
                let Expr::BinaryExpr(predicate) = predicate else {
                    return Err("expected region comparison".into());
                };
                let Expr::Column(column) = predicate.left.as_ref() else {
                    return Err("expected region column".into());
                };
                let Expr::Literal(value, _) = predicate.right.as_ref() else {
                    return Err("expected region literal".into());
                };
                assert_eq!(column.name, "region");
                assert_eq!(predicate.op, Operator::Eq);
                assert_eq!(
                    value.cast_to(&DataType::Utf8)?,
                    ScalarValue::Utf8(Some("us-west".into()))
                );
            }
            let reads = fixture.stores[3]
                .reads
                .lock()
                .map_err(|_| "read log poisoned")?;
            let paths = reads
                .iter()
                .map(|read| read.path.clone())
                .collect::<BTreeSet<_>>();
            let read_bytes = reads
                .iter()
                .map(|read| read.range.end - read.range.start)
                .sum::<u64>();
            println!(
                "LOCAL_SORT_PRUNING ansi={ansi_mode} bytes={read_bytes} paths={paths:?} sql={sql}"
            );
            assert_eq!(
                paths.len(),
                1,
                "filter must prune one of the two Parquet files"
            );
            if let Some(baseline) = &baseline_paths {
                assert_eq!(&(paths, read_bytes), baseline);
            } else {
                baseline_paths = Some((paths, read_bytes));
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn distribution_uses_session_target_with_a_single_input_partition() -> TestResult {
    let request: Value = serde_json::from_str(include_str!(
        "fixtures/execution-features/distribution-requests.json"
    ))?;
    let session = context(3, 2);
    spark_inputs::register_recorded_inputs_with_partition_count(
        &session,
        &request["corpora"][0]["metadata"],
        2,
        1,
    )
    .map_err(|error| error as Box<dyn std::error::Error>)?;
    for ansi_mode in [false, true] {
        for clause in ["DISTRIBUTE BY k", "CLUSTER BY k", "DISTRIBUTE BY 1"] {
            let sql = format!("SELECT id,k FROM distribution_source {clause}");
            let physical = create_spark_physical_plan(&session, &sql, ansi_mode).await?;
            assert_eq!(
                physical.output_partitioning().partition_count(),
                3,
                "{clause}"
            );
            let rows = collect_partitioned(physical, session.task_ctx())
                .await?
                .iter()
                .flatten()
                .map(RecordBatch::num_rows)
                .sum::<usize>();
            assert_eq!(rows, 12);
        }
    }
    Ok(())
}

#[tokio::test]
async fn volatile_local_sort_keeps_row_key_associations_after_outer_filter() -> TestResult {
    for ansi_mode in [false, true] {
        let fixture = DeltaFeatureFixture::new(1, 7).await?;
        let baseline = create_spark_physical_plan(
            &fixture.session,
            "SELECT id FROM events SORT BY rand(19)",
            ansi_mode,
        )
        .await?;
        let mut expected = vec![];
        for batch in collect_partitioned(baseline, fixture.session.task_ctx())
            .await?
            .into_iter()
            .flatten()
        {
            expected.extend(
                as_int32_array(batch.column(0).as_ref())?
                    .values()
                    .iter()
                    .copied()
                    .filter(|id| id % 2 == 0),
            );
        }
        for sql in [
            "SELECT id FROM (SELECT id FROM events SORT BY rand(19)) q WHERE id % 2 = 0",
            "SELECT id FROM (SELECT id,rand(19) AS r FROM events SORT BY r) q WHERE id % 2 = 0",
            "SELECT id FROM (SELECT id FROM (SELECT id,rand(19) AS r FROM events) p SORT BY r) q WHERE id % 2 = 0",
        ] {
            let filtered = create_spark_physical_plan(&fixture.session, sql, ansi_mode).await?;
            let mut actual = vec![];
            for batch in collect_partitioned(filtered, fixture.session.task_ctx())
                .await?
                .into_iter()
                .flatten()
            {
                actual.extend_from_slice(as_int32_array(batch.column(0).as_ref())?.values());
            }
            assert_eq!(actual.len(), expected.len());
            assert!(
                actual == expected,
                "outer filter must preserve the materialized volatile sort keys: {sql}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn local_sort_filters_and_boundaries_match_spark_values_and_types() -> TestResult {
    let request: Value = serde_json::from_str(include_str!(
        "fixtures/execution-features/local-sort-filter-requests.json"
    ))?;
    let records = local_sort_filter_references()?;
    for batch_size in [1, 3] {
        let session = context(1, batch_size);
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &request["corpora"][0]["metadata"],
            batch_size,
            1,
        )
        .map_err(|error| error as Box<dyn std::error::Error>)?;
        for case in &records {
            let actual = capture_executed_query_result(&session, case)
                .await
                .map_err(|error| error as Box<dyn std::error::Error>)?;
            assert_eq!(actual["rows"], case["expected"]["rows"], "{}", case["id"]);
            let mut schema = actual["schema"].clone();
            let mut expected = case["schema_stages"]["executed"].clone();
            remove_nullability_and_metadata(&mut schema);
            remove_nullability_and_metadata(&mut expected);
            assert_eq!(schema, expected, "{}", case["id"]);
        }
    }
    Ok(())
}

#[tokio::test]
async fn volatile_outer_predicate_stays_after_local_sort() -> TestResult {
    use arrow::array::Float64Array;

    for ansi_mode in [false, true] {
        let fixture = DeltaFeatureFixture::new(1, 7).await?;
        let baseline = create_spark_physical_plan(
            &fixture.session,
            "SELECT id,rand(19) AS r FROM (SELECT id FROM events SORT BY id DESC) q",
            ansi_mode,
        )
        .await?;
        let mut expected = vec![];
        for batch in collect_partitioned(baseline, fixture.session.task_ctx())
            .await?
            .into_iter()
            .flatten()
        {
            let ids = as_int32_array(batch.column(0).as_ref())?;
            let random = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("expected DOUBLE rand")?;
            for row in 0..batch.num_rows() {
                if random.value(row) < 0.5 {
                    expected.push(ids.value(row));
                }
            }
        }
        let filtered = create_spark_physical_plan(
            &fixture.session,
            "SELECT id FROM (SELECT id FROM events SORT BY id DESC) q WHERE rand(19) < 0.5",
            ansi_mode,
        )
        .await?;
        let mut actual = vec![];
        for batch in collect_partitioned(filtered, fixture.session.task_ctx())
            .await?
            .into_iter()
            .flatten()
        {
            actual.extend_from_slice(as_int32_array(batch.column(0).as_ref())?.values());
        }
        assert!(!expected.is_empty() && expected.len() < 2 * ROWS_PER_FILE);
        assert_eq!(actual.len(), expected.len());
        assert!(
            actual == expected,
            "volatile outer predicate must observe sorted row order"
        );
    }
    Ok(())
}

#[tokio::test]
async fn fetched_local_sort_keeps_outer_filter_above_its_row_boundary() -> TestResult {
    use datafusion::common::{
        Column,
        tree_node::{Transformed, TransformedResult, TreeNode},
    };
    use datafusion::logical_expr::{Expr, Extension, LogicalPlan, LogicalPlanBuilder, lit};
    use sail_logical_plan::sort::SortWithinPartitionsNode;

    for ansi_mode in [false, true] {
        let fixture = DeltaFeatureFixture::new(1, 7).await?;
        let mut query = plan_query(
            &fixture.session,
            "SELECT id FROM partitioned_events SORT BY id",
            &SparkSqlOptions {
                ansi_mode,
                ..Default::default()
            },
        )
        .await?;
        query.logical_plan = query
            .logical_plan
            .transform_up(|plan| {
                let LogicalPlan::Extension(extension) = &plan else {
                    return Ok(Transformed::no(plan));
                };
                let Some(sort) = extension
                    .node
                    .as_any()
                    .downcast_ref::<SortWithinPartitionsNode>()
                else {
                    return Ok(Transformed::no(plan));
                };
                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                    node: Arc::new(SortWithinPartitionsNode::new(
                        sort.input().clone(),
                        sort.sort_expr().to_vec(),
                        Some(2),
                    )),
                })))
            })
            .data()?;
        let id = Expr::Column(Column::from(query.logical_plan.schema().qualified_field(0)));
        query.logical_plan = LogicalPlanBuilder::from(query.logical_plan)
            .filter(id.gt(lit(2)))?
            .build()?;
        let physical = query.create_physical_plan(&fixture.session).await?;
        let rows = collect_partitioned(physical, fixture.session.task_ctx())
            .await?
            .iter()
            .flatten()
            .map(RecordBatch::num_rows)
            .sum::<usize>();
        assert_eq!(
            rows, 0,
            "filtering before the fetched sort would return ids 3 and 4"
        );
        let calls = fixture.calls[3]
            .lock()
            .map_err(|_| "provider call lock poisoned")?;
        assert_eq!(calls.scans.len(), 1);
        assert!(
            calls.scans[0].filters.is_empty(),
            "fetched sort must retain its outer filter"
        );
    }
    Ok(())
}
