//! Public Spark SQL planning over real Delta files, referenced to equivalent DAR native SQL.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    sync::{Arc, Mutex},
};

use arrow::{
    array::{ArrayRef, Int32Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    execution::SessionStateBuilder,
    logical_expr::Expr,
    physical_plan::collect,
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader::{
    DeltaScanExecutionOptions, DeltaScanMetricsSnapshot, DeltaSnapshotSelection, DeltaTableBuilder,
    ParquetReaderBackend, WarmupMode,
    datafusion::{DeltaTableProvider, ScanOptions, collect_scan_metrics, register_table},
};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding},
    file::properties::{EnabledStatistics, WriterProperties, WriterVersion},
};
use serde_json::{Value, json};

#[path = "support/delta_reads.rs"]
mod delta_reads;
#[allow(dead_code)]
#[path = "../../delta-arrow-reader-core/tests/reader/support.rs"]
mod reader_support;
#[path = "support/spark_results.rs"]
mod spark_results;
use delta_reads::{
    ParquetReadLayout, ProviderCalls, ReadRange, RecordingDeltaProvider, RecordingLocalStore,
    TestResult,
};
use reader_support::RealParquetDeltaTable;
use spark_results::{encode_value, validate_batch};

// The pinned Spark capture environment has no Delta Lake package or jars.
const REFERENCE: &str = "DAR native equivalent SQL over the same Delta snapshot";

#[derive(Clone, Copy)]
enum QueryPath {
    Native,
    Spark { ansi_mode: bool },
}

fn context(frontend: QueryPath) -> SessionContext {
    let builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_batch_size(256),
        );
    SessionContext::new_with_state(
        match frontend {
            QueryPath::Native => builder,
            QueryPath::Spark { .. } => configure_spark_sql(builder),
        }
        .build(),
    )
}

struct DeltaBinding<'a> {
    name: &'static str,
    fixture: &'a RealParquetDeltaTable,
    snapshot: DeltaSnapshotSelection,
}

struct TableScanEvidence {
    calls: ProviderCalls,
    reads: Vec<ReadRange>,
    metrics: DeltaScanMetricsSnapshot,
}

struct QueryEvidence {
    result: Value,
    branches: BTreeMap<String, TableScanEvidence>,
}

fn executed_type(data_type: &DataType) -> Value {
    match data_type {
        DataType::Struct(fields) => {
            json!({"struct":fields.iter().map(|field|json!({"name":field.name(),"type":executed_type(field.data_type())})).collect::<Vec<_>>()})
        }
        DataType::List(field) => json!({"list":executed_type(field.data_type())}),
        DataType::Map(field, sorted) => {
            json!({"map":executed_type(field.data_type()),"sorted":sorted})
        }
        DataType::Dictionary(key, value) => {
            json!({"dictionary_key":executed_type(key),"dictionary_value":executed_type(value)})
        }
        other => json!(other.to_string()),
    }
}

async fn execute(
    frontend: QueryPath,
    sql: &str,
    inputs: &[DeltaBinding<'_>],
) -> TestResult<QueryEvidence> {
    let ctx = context(frontend);
    let mut observed = BTreeMap::new();
    for input in inputs {
        let (uri, store) = RecordingLocalStore::register(input.fixture.path())?;
        let table = DeltaTableBuilder::new(uri)
            .with_snapshot_selection(input.snapshot)
            .with_warmup(WarmupMode::QueryPlanning)
            .load_table()
            .await?;
        register_table(
            &ctx,
            input.name,
            table,
            ScanOptions {
                target_partitions: Some(1),
                ..Default::default()
            },
        )?;
        let provider = ctx
            .deregister_table(input.name)?
            .ok_or("missing registered provider")?;
        let inner = provider
            .downcast_ref::<DeltaTableProvider>()
            .ok_or("wrong provider type")?
            .clone();
        let calls = Arc::new(Mutex::new(ProviderCalls::default()));
        ctx.register_table(
            input.name,
            Arc::new(RecordingDeltaProvider {
                inner,
                calls: Arc::clone(&calls),
                schema_calls: Default::default(),
            }),
        )?;
        store
            .reads
            .lock()
            .map_err(|_| "read log lock poisoned")?
            .clear();
        observed.insert(input.name, (store, calls));
    }
    let physical = match frontend {
        QueryPath::Native => ctx.sql(sql).await?.create_physical_plan().await?,
        QueryPath::Spark { ansi_mode } => {
            plan_query(
                &ctx,
                sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..Default::default()
                },
            )
            .await?
            .create_physical_plan(&ctx)
            .await?
        }
    };
    let scans = collect_scan_metrics(physical.as_ref());
    let schema = physical.schema();
    let mut rows = vec![];
    for batch in collect(physical, ctx.task_ctx()).await? {
        validate_batch(schema.as_ref(), &batch)
            .map_err(|error| error as Box<dyn std::error::Error>)?;
        let values = batch
            .columns()
            .iter()
            .map(|column| {
                if let DataType::Dictionary(_, value_type) = column.data_type() {
                    arrow::compute::cast(column, value_type)
                } else {
                    Ok(Arc::clone(column))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            rows.push(
                values
                    .iter()
                    .map(|column| encode_value(column.as_ref(), row))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error as Box<dyn std::error::Error>)?,
            );
        }
    }
    let mut branches = BTreeMap::new();
    for scan in scans {
        let name = scan
            .registration_name()
            .ok_or("missing scan branch label")?;
        let (store, calls) = observed.get(name).ok_or("unknown scan branch")?;
        let metrics = scan.snapshot().reader_metrics;
        assert_eq!(metrics.parquet_backend, ParquetReaderBackend::Direct);
        // Concrete counters are required, including zero for an empty/pruned scan.
        metrics
            .parquet_data_file_range_get_operations
            .ok_or("missing range GET metric")?;
        metrics
            .parquet_data_file_bytes_received
            .ok_or("missing received-byte metric")?;
        let branch = TableScanEvidence {
            calls: calls
                .lock()
                .map_err(|_| "provider call lock poisoned")?
                .clone(),
            reads: store
                .reads
                .lock()
                .map_err(|_| "read log lock poisoned")?
                .clone(),
            metrics,
        };
        assert!(
            branches.insert(name.to_string(), branch).is_none(),
            "unexpected repeated scan branch"
        );
    }
    let schema = schema
        .fields()
        .iter()
        .map(|field| json!({"name":field.name(),"type":executed_type(field.data_type())}))
        .collect::<Vec<_>>();
    Ok(QueryEvidence {
        result: json!({"schema":schema,"rows":rows}),
        branches,
    })
}

fn columns(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| (*name).to_string()).collect()
}

fn check_requests(
    branch: &TableScanEvidence,
    expected: &BTreeSet<String>,
    filtered: bool,
) -> TestResult {
    assert_eq!(branch.calls.scans.len(), 1);
    assert!(
        branch.calls.scans[0].projected_columns.is_subset(expected),
        "projection contains an unreferenced stored column"
    );
    if filtered {
        assert!(
            !branch.calls.offered_filters.is_empty(),
            "predicate never reached the provider"
        );
        assert!(
            !branch.calls.scans[0].filters.is_empty(),
            "supported predicate never reached scan"
        );
    }
    for expression in branch
        .calls
        .offered_filters
        .iter()
        .chain(&branch.calls.scans[0].filters)
    {
        expression.apply(|expr| {
            if let Expr::Column(column) = expr {
                assert!(
                    expected.contains(&column.name),
                    "filter must use a referenced stored column: {expression}"
                );
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    Ok(())
}

fn parquet_files(fixture: &RealParquetDeltaTable) -> TestResult<Vec<ParquetReadLayout>> {
    fs::read_dir(fixture.path())?
        .filter_map(|entry| match entry {
            Ok(entry)
                if entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "parquet") =>
            {
                Some(ParquetReadLayout::open(
                    fixture.path(),
                    &entry.file_name().to_string_lossy(),
                ))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error.into())),
        })
        .collect()
}

fn physical_roots(
    fixture: &RealParquetDeltaTable,
    logical: &BTreeSet<String>,
) -> TestResult<BTreeSet<String>> {
    let log = fs::read_to_string(fixture.path().join("_delta_log/00000000000000000000.json"))?;
    let metadata = log
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?
        .into_iter()
        .find_map(|action| action.get("metaData").cloned())
        .ok_or("missing metadata")?;
    let schema: Value =
        serde_json::from_str(metadata["schemaString"].as_str().ok_or("missing schema")?)?;
    schema["fields"]
        .as_array()
        .ok_or("missing fields")?
        .iter()
        .filter(|field| {
            field["name"]
                .as_str()
                .is_some_and(|name| logical.contains(name))
        })
        .map(|field| {
            Ok(field["metadata"]["delta.columnMapping.physicalName"]
                .as_str()
                .or_else(|| field["name"].as_str())
                .ok_or("missing physical column name")?
                .to_string())
        })
        .collect()
}

fn record_column_reads(
    label: &str,
    branch: &TableScanEvidence,
    files: &[ParquetReadLayout],
    referenced: &BTreeSet<String>,
) -> TestResult {
    let hint = DeltaScanExecutionOptions::new()
        .parquet_metadata_size_hint_bytes()
        .ok_or("missing default metadata prefetch")? as u64;
    let mut extra = vec![];
    let mut observed = vec![];
    for file in files {
        for (group, path) in file.touched_chunks(&branch.reads) {
            observed.push(json!({"file":file.path,"row_group":group,"physical_path":path}));
            if !referenced.contains(&path[0]) {
                let chunk = file
                    .chunks
                    .iter()
                    .find(|chunk| chunk.row_group == group && chunk.column_path == path)
                    .ok_or("missing chunk")?;
                let reads = branch
                    .reads
                    .iter()
                    .filter(|read| {
                        read.path == file.path
                            && read.range.start < chunk.range.end
                            && chunk.range.start < read.range.end
                    })
                    .collect::<Vec<_>>();
                assert!(!reads.is_empty());
                assert!(
                    reads
                        .iter()
                        .all(|read| read.range == (file.size.saturating_sub(hint)..file.size)),
                    "{label}: an unreferenced chunk was read outside the known metadata prefetch"
                );
                extra.push(json!({"file":file.path,"row_group":group,"physical_path":path,"overlapping_ranges":reads.iter().map(|read|json!([read.range.start,read.range.end])).collect::<Vec<_>>() }));
            }
        }
    }
    println!(
        "REAL_DELTA_COLUMN_READS {}",
        json!({"case":label,"reference":REFERENCE,"referenced_physical_roots":referenced,"all_touched_chunks":observed,"unreferenced_chunks":extra,"only_referenced_columns_read":extra.is_empty(),"default_metadata_prefetch_bytes":hint,"core_limited":!extra.is_empty()})
    );
    Ok(())
}

fn compare_work(
    label: &str,
    spark: &TableScanEvidence,
    native: &TableScanEvidence,
    files: &[ParquetReadLayout],
) -> TestResult {
    assert!(
        spark.metrics.file_tasks_started <= native.metrics.file_tasks_started,
        "{label}: extra file tasks"
    );
    assert!(
        spark.metrics.files_planned <= native.metrics.files_planned,
        "{label}: extra planned files"
    );
    for file in files {
        assert!(
            file.chunks.iter().all(|chunk| !chunk.pages.is_empty()),
            "fixture must expose page locations"
        );
        let actual = file.touched_chunks(&spark.reads);
        let reference = file.touched_chunks(&native.reads);
        assert!(
            actual.is_subset(&reference),
            "{label}: extra column/row-group reads: {actual:?} vs {reference:?}"
        );
        for column in file
            .chunks
            .iter()
            .map(|chunk| &chunk.root_column)
            .collect::<BTreeSet<_>>()
        {
            let actual = file.touched_pages(&spark.reads, column);
            let reference = file.touched_pages(&native.reads, column);
            assert!(
                actual.is_subset(&reference),
                "{label}: extra {column} pages: {actual:?} vs {reference:?}"
            );
        }
    }
    println!(
        "REAL_DELTA_WORK {}",
        json!({"case":label,"reference":REFERENCE,"spark_files":spark.metrics.file_tasks_started,"native_files":native.metrics.file_tasks_started,"spark_range_gets":spark.metrics.parquet_data_file_range_get_operations,"native_range_gets":native.metrics.parquet_data_file_range_get_operations,"spark_bytes":spark.metrics.parquet_data_file_bytes_received,"native_bytes":native.metrics.parquet_data_file_bytes_received,"spark_ranges":spark.reads.iter().map(|read|json!([read.path,read.range.start,read.range.end])).collect::<Vec<_>>(),"native_ranges":native.reads.iter().map(|read|json!([read.path,read.range.start,read.range.end])).collect::<Vec<_>>() })
    );
    println!(
        "REAL_DELTA_PROVIDER_REQUESTS {}",
        json!({"case":label,"reference":REFERENCE,"spark_offered_filters":spark.calls.offered_filters.iter().map(ToString::to_string).collect::<BTreeSet<_>>(),"native_offered_filters":native.calls.offered_filters.iter().map(ToString::to_string).collect::<BTreeSet<_>>(),"spark_scans":spark.calls.scans.iter().map(|scan|json!({"projected_columns":scan.projected_columns,"stored_filters":scan.filters.iter().map(ToString::to_string).collect::<Vec<_>>()})).collect::<Vec<_>>(),"native_scans":native.calls.scans.iter().map(|scan|json!({"projected_columns":scan.projected_columns,"stored_filters":scan.filters.iter().map(ToString::to_string).collect::<Vec<_>>()})).collect::<Vec<_>>() })
    );
    Ok(())
}

fn typed_partition(
    name: &str,
    data_type: &str,
    values: [Option<&str>; 3],
) -> TestResult<RealParquetDeltaTable> {
    let fixture = RealParquetDeltaTable::new_with_null_partition_value(name)?;
    let log = fixture.path().join("_delta_log");
    let metadata_path = log.join("00000000000000000000.json");
    let mut actions = fs::read_to_string(&metadata_path)?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    for action in &mut actions {
        if let Some(metadata) = action.get_mut("metaData") {
            let mut schema: Value =
                serde_json::from_str(metadata["schemaString"].as_str().ok_or("missing schema")?)?;
            schema["fields"][2]["type"] = json!(data_type);
            metadata["schemaString"] = json!(schema.to_string());
        }
    }
    fs::write(
        metadata_path,
        actions
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )?;
    let add_path = log.join("00000000000000000001.json");
    let mut adds = fs::read_to_string(&add_path)?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    assert_eq!(adds.len(), values.len());
    for (add, value) in adds.iter_mut().zip(values) {
        add["add"]["partitionValues"]["region"] = json!(value);
    }
    fs::write(
        add_path,
        adds.iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )?;
    Ok(fixture)
}

struct MatrixTable {
    name: &'static str,
    fixture: RealParquetDeltaTable,
    snapshot: DeltaSnapshotSelection,
    additional_projected_columns: &'static [&'static str],
    partition_literal: Option<&'static str>,
}

fn mapped_table_with_stats() -> TestResult<RealParquetDeltaTable> {
    let fixture = RealParquetDeltaTable::new_with_column_mapping("sql-matrix-mapping")?;
    let path = fixture.path().join("_delta_log/00000000000000000001.json");
    let mut actions = fs::read_to_string(&path)?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    for action in &mut actions {
        let mut stats: Value =
            serde_json::from_str(action["add"]["stats"].as_str().ok_or("missing stats")?)?;
        // Delta column mapping uses physical names for transaction-log statistics.
        for section in ["minValues", "maxValues", "nullCount"] {
            let values = stats[section]
                .as_object_mut()
                .ok_or("missing column statistics")?;
            for (logical, physical) in [("id", "phys_id"), ("customer_name", "phys_customer_name")]
            {
                let value = values
                    .remove(logical)
                    .ok_or("missing logical fixture statistic")?;
                values.insert(physical.to_string(), value);
            }
        }
        action["add"]["stats"] = json!(stats.to_string());
    }
    fs::write(
        path,
        actions
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )?;
    Ok(fixture)
}

#[tokio::test]
async fn real_delta_table_kinds_match_native_rows_types_and_scan_work() -> TestResult {
    let pinned = RealParquetDeltaTable::new_with_two_files("sql-matrix-pinned")?;
    let commit = fs::read_to_string(pinned.path().join("_delta_log/00000000000000000001.json"))?;
    let first: Value = serde_json::from_str(commit.lines().next().ok_or("missing add")?)?;
    fs::write(
        pinned.path().join("_delta_log/00000000000000000002.json"),
        json!({"remove":{"path":first["add"]["path"],"deletionTimestamp":0,"dataChange":true}})
            .to_string()
            + "\n",
    )?;
    let cases = vec![
        MatrixTable {
            name: "string_partition",
            fixture: RealParquetDeltaTable::new_with_null_partition_value("sql-matrix-string")?,
            snapshot: DeltaSnapshotSelection::Latest,
            additional_projected_columns: &["region"],
            partition_literal: Some("'us-west'"),
        },
        MatrixTable {
            name: "date_partition",
            fixture: typed_partition(
                "sql-matrix-date",
                "date",
                [None, Some("2026-01-01"), Some("2026-01-02")],
            )?,
            snapshot: DeltaSnapshotSelection::Latest,
            additional_projected_columns: &["region"],
            partition_literal: Some("DATE '2026-01-01'"),
        },
        MatrixTable {
            name: "int_partition",
            fixture: typed_partition("sql-matrix-int", "integer", [None, Some("10"), Some("20")])?,
            snapshot: DeltaSnapshotSelection::Latest,
            additional_projected_columns: &["region"],
            partition_literal: Some("10"),
        },
        MatrixTable {
            name: "deletion_vectors",
            fixture: RealParquetDeltaTable::new_with_rows_and_deletion_vector(
                "sql-matrix-dv",
                6,
                &[1, 4],
            )?,
            snapshot: DeltaSnapshotSelection::Latest,
            additional_projected_columns: &[],
            partition_literal: None,
        },
        MatrixTable {
            name: "struct_and_array",
            fixture: RealParquetDeltaTable::new_with_supported_types("sql-matrix-types")?,
            snapshot: DeltaSnapshotSelection::Latest,
            additional_projected_columns: &[
                "active",
                "payload",
                "event_date",
                "event_ts",
                "amount",
                "score_f32",
                "score_f64",
                "attributes",
                "tags",
            ],
            partition_literal: None,
        },
        MatrixTable {
            name: "map",
            fixture: RealParquetDeltaTable::new_with_reordered_map_value_struct_fields(
                "sql-matrix-map",
            )?,
            snapshot: DeltaSnapshotSelection::Latest,
            additional_projected_columns: &["attributes"],
            partition_literal: None,
        },
        MatrixTable {
            name: "column_mapping",
            fixture: mapped_table_with_stats()?,
            snapshot: DeltaSnapshotSelection::Latest,
            additional_projected_columns: &[],
            partition_literal: None,
        },
        MatrixTable {
            name: "added_nullable_column",
            fixture: RealParquetDeltaTable::new_with_missing_nullable_column("sql-matrix-added")?,
            snapshot: DeltaSnapshotSelection::Latest,
            additional_projected_columns: &["loyalty_tier"],
            partition_literal: None,
        },
        MatrixTable {
            name: "pinned_version",
            fixture: pinned,
            snapshot: DeltaSnapshotSelection::Version(1),
            additional_projected_columns: &[],
            partition_literal: None,
        },
        MatrixTable {
            name: "empty_version",
            fixture: RealParquetDeltaTable::new_with_two_files("sql-matrix-empty")?,
            snapshot: DeltaSnapshotSelection::Version(0),
            additional_projected_columns: &[],
            partition_literal: None,
        },
    ];
    let lookup = RealParquetDeltaTable::new_default("sql-matrix-lookup")?;
    for table in &cases {
        let files = parquet_files(&table.fixture)?;
        if matches!(table.name, "struct_and_array" | "map") {
            for file in &files {
                let full = [ReadRange {
                    path: file.path.clone(),
                    range: 0..file.size,
                }];
                assert_eq!(
                    file.touched_chunks(&full).len(),
                    file.chunks.len(),
                    "physical sibling leaves must keep distinct identities"
                );
                let roots = file
                    .chunks
                    .iter()
                    .map(|chunk| chunk.root_column.as_str())
                    .collect::<BTreeSet<_>>();
                let mut sibling_roots = 0;
                for root in roots {
                    let leaves = file
                        .chunks
                        .iter()
                        .filter(|chunk| chunk.root_column == root)
                        .collect::<Vec<_>>();
                    if leaves.len() > 1 {
                        sibling_roots += 1;
                        assert!(leaves.iter().all(|leaf| leaf.column_path.len() > 1));
                        assert_eq!(
                            file.touched_pages(&full, root).len(),
                            leaves.iter().map(|leaf| leaf.pages.len()).sum::<usize>(),
                            "page identity must include each physical sibling path"
                        );
                    }
                }
                assert!(
                    sibling_roots > 0,
                    "nested fixture must contain multiple sibling leaves"
                );
            }
        }
        let lookup_files = parquet_files(&lookup)?;
        let mut projected = vec!["id", "customer_name"];
        projected.extend(table.additional_projected_columns);
        let projection = format!("SELECT {} FROM source ORDER BY id", projected.join(","));
        let mut queries=vec![
            ("projection",projection.clone(),projection,columns(&projected),false,false),
            ("data_filter","SELECT id,customer_name FROM source WHERE id>=2 ORDER BY id".into(),"SELECT id,customer_name FROM source WHERE id>=2 ORDER BY id".into(),columns(&["id","customer_name"]),true,false),
            ("join","SELECT s.id,s.customer_name,l.customer_name AS lookup_name FROM source s JOIN lookup l ON s.id=l.id WHERE s.id>=2 ORDER BY s.id".into(),"SELECT s.id,s.customer_name,l.customer_name AS lookup_name FROM source s JOIN lookup l ON s.id=l.id WHERE s.id>=2 ORDER BY s.id".into(),columns(&["id","customer_name"]),true,true),
            ("group_by","SELECT customer_name,count(*) AS n FROM source GROUP BY customer_name ORDER BY customer_name NULLS FIRST".into(),"SELECT customer_name,count(*) AS n FROM source GROUP BY customer_name ORDER BY customer_name NULLS FIRST".into(),columns(&["customer_name"]),false,false),
            ("order_limit","SELECT id,customer_name FROM source ORDER BY id DESC LIMIT 2".into(),"SELECT id,customer_name FROM source ORDER BY id DESC LIMIT 2".into(),columns(&["id","customer_name"]),false,false),
            ("window","SELECT id,row_number() OVER (ORDER BY id) AS rn FROM source ORDER BY id".into(),"SELECT id,CAST(row_number() OVER (ORDER BY id) AS INT) AS rn FROM source ORDER BY id".into(),columns(&["id"]),false,false),
            ("prune_all","SELECT id FROM source WHERE id<0 ORDER BY id".into(),"SELECT id FROM source WHERE id<0 ORDER BY id".into(),columns(&["id"]),true,false),
        ];
        if let Some(literal) = table.partition_literal {
            for (name, predicate) in [
                ("partition_filter", format!("region={literal}")),
                ("null_partition", "region IS NULL".into()),
            ] {
                let sql = format!("SELECT id,region FROM source WHERE {predicate} ORDER BY id");
                queries.push((
                    name,
                    sql.clone(),
                    sql,
                    columns(&["id", "region"]),
                    true,
                    false,
                ));
            }
        }
        for ansi in [false, true] {
            for (name, sql, native_sql, expected, filtered, join) in &queries {
                let label = format!("{}/{name}/ansi={ansi}", table.name);
                println!(
                    "REAL_DELTA_CASE_BEGIN {}",
                    json!({"case":label,"spark_sql":sql,"native_sql":native_sql,"reference":REFERENCE})
                );
                let mut inputs = vec![DeltaBinding {
                    name: "source",
                    fixture: &table.fixture,
                    snapshot: table.snapshot,
                }];
                if *join {
                    inputs.push(DeltaBinding {
                        name: "lookup",
                        fixture: &lookup,
                        snapshot: DeltaSnapshotSelection::Latest,
                    });
                }
                let native = execute(QueryPath::Native, native_sql, &inputs).await?;
                let spark = execute(QueryPath::Spark { ansi_mode: ansi }, sql, &inputs).await?;
                assert_eq!(
                    spark.result, native.result,
                    "{label}; reference={REFERENCE}"
                );
                println!(
                    "REAL_DELTA_RESULT {}",
                    json!({"case":label,"reference":REFERENCE,"native":native.result,"spark":spark.result})
                );
                assert_eq!(
                    spark.branches.keys().collect::<Vec<_>>(),
                    native.branches.keys().collect::<Vec<_>>()
                );
                for branch in spark.branches.keys() {
                    let expected = if branch == "source" {
                        expected.clone()
                    } else {
                        columns(&["id", "customer_name"])
                    };
                    check_requests(
                        &spark.branches[branch],
                        &expected,
                        *filtered && branch == "source",
                    )?;
                    check_requests(
                        &native.branches[branch],
                        &expected,
                        *filtered && branch == "source",
                    )?;
                    let branch_fixture = if branch == "source" {
                        &table.fixture
                    } else {
                        &lookup
                    };
                    let branch_files = if branch == "source" {
                        &files
                    } else {
                        &lookup_files
                    };
                    let physical = physical_roots(branch_fixture, &expected)?;
                    if !native.result["rows"]
                        .as_array()
                        .ok_or("missing native rows")?
                        .is_empty()
                    {
                        for outcome in [&native, &spark] {
                            let evidence = &outcome.branches[branch];
                            assert!(
                                evidence.metrics.file_tasks_started > 0,
                                "nonempty result must have a real scan"
                            );
                            assert!(
                                branch_files
                                    .iter()
                                    .any(|file| !file.touched_chunks(&evidence.reads).is_empty()),
                                "must observe concrete data payload reads"
                            );
                        }
                    }
                    record_column_reads(
                        &format!("{label}/{branch}/spark"),
                        &spark.branches[branch],
                        branch_files,
                        &physical,
                    )?;
                    record_column_reads(
                        &format!("{label}/{branch}/native"),
                        &native.branches[branch],
                        branch_files,
                        &physical,
                    )?;
                    compare_work(
                        &format!("{label}/{branch}"),
                        &spark.branches[branch],
                        &native.branches[branch],
                        if branch == "source" {
                            &files
                        } else {
                            &lookup_files
                        },
                    )?;
                }
                if *name == "prune_all" {
                    assert!(
                        spark.result["rows"]
                            .as_array()
                            .ok_or("missing rows")?
                            .is_empty()
                    );
                    assert_eq!(
                        spark.branches["source"].metrics.file_tasks_started, 0,
                        "{label}: must skip all files"
                    );
                    assert!(spark.branches["source"].reads.is_empty());
                }
                if *name == "partition_filter" || *name == "null_partition" {
                    assert_eq!(
                        spark.branches["source"].metrics.file_tasks_started, 1,
                        "{label}: must prune two partition files"
                    );
                }
                if *name == "projection" && table.name == "pinned_version" {
                    assert_eq!(
                        spark.result["rows"].as_array().ok_or("missing rows")?.len(),
                        4
                    );
                    assert_eq!(spark.branches["source"].metrics.snapshot_version, 1);
                }
                if *name == "projection" && table.name == "empty_version" {
                    assert!(
                        spark.result["rows"]
                            .as_array()
                            .ok_or("missing rows")?
                            .is_empty()
                    );
                    assert_eq!(spark.branches["source"].metrics.snapshot_version, 0);
                }
                if *name == "projection" && table.name == "deletion_vectors" {
                    assert_eq!(
                        spark.result["rows"]
                            .as_array()
                            .ok_or("missing rows")?
                            .iter()
                            .map(|row| row[0].clone())
                            .collect::<Vec<_>>(),
                        vec![json!("1"), json!("3"), json!("4"), json!("6")]
                    );
                    assert_eq!(
                        spark.branches["source"]
                            .metrics
                            .deletion_vector_rows_deleted,
                        2
                    );
                }
            }
        }
    }
    Ok(())
}

fn wide_table() -> TestResult<RealParquetDeltaTable> {
    let rows = 12_288;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Int64, false),
        Field::new("unused", DataType::Int64, false),
        Field::new("tail", DataType::Int64, false),
    ]));
    let mut random = 17_u64;
    let mut noise = || {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        random as i64
    };
    let columns = vec![
        Arc::new(Int32Array::from_iter_values(0..rows)) as ArrayRef,
        Arc::new(Int64Array::from_iter_values((0..rows).map(|_| noise()))),
        Arc::new(Int64Array::from_iter_values((0..rows).map(|_| noise()))),
        Arc::new(Int64Array::from_iter_values((0..rows).map(|_| noise()))),
    ];
    let batch = RecordBatch::try_new(schema, columns)?;
    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_offset_index_disabled(false)
        .set_max_row_group_row_count(Some(4096))
        .set_data_page_row_count_limit(512)
        .set_write_batch_size(512)
        // Keep the ordinary 64KiB metadata probe wholly beyond data chunks.
        .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
            "integration_footer_padding".to_string(),
            Some("p".repeat(131_072)),
        )]))
        .build();
    let mut bytes = vec![];
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    let fields = [
        ("id", "integer"),
        ("payload", "long"),
        ("unused", "long"),
        ("tail", "long"),
    ]
    .into_iter()
    .map(|(name, kind)| json!({"name":name,"type":kind,"nullable":false,"metadata":{}}))
    .collect::<Vec<_>>();
    RealParquetDeltaTable::new_with_raw_parquet(
        "sql-wide-pruning",
        &bytes,
        rows as usize,
        &json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        &json!({"metaData":{"id":"sql-wide-pruning","format":{"provider":"parquet","options":{}},"schemaString":json!({"type":"struct","fields":fields}).to_string(),"partitionColumns":[],"configuration":{}}}),
    )
}

#[tokio::test]
async fn real_delta_projection_row_groups_and_pages_are_actually_skipped() -> TestResult {
    let fixture = wide_table()?;
    let file = ParquetReadLayout::open(fixture.path(), fixture.data_file_path())?;
    assert_eq!(
        file.chunks
            .iter()
            .map(|chunk| chunk.row_group)
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
    assert!(file.chunks.iter().all(|chunk| chunk.pages.len() == 8));
    let full = vec![ReadRange {
        path: file.path.clone(),
        range: 0..file.size,
    }];
    assert_eq!(
        file.touched_chunks(&full).len(),
        12,
        "full-file reads must count every chunk, including when they overlap metadata"
    );
    for ansi in [false, true] {
        let inputs = [DeltaBinding {
            name: "source",
            fixture: &fixture,
            snapshot: DeltaSnapshotSelection::Latest,
        }];
        for (name, sql, expected) in [
            (
                "all_payload",
                "SELECT payload FROM source ORDER BY payload",
                columns(&["payload"]),
            ),
            (
                "positive_unused",
                "SELECT unused FROM source ORDER BY unused",
                columns(&["unused"]),
            ),
            (
                "filtered_payload",
                "SELECT payload FROM source WHERE id>=4096 AND id<4608 ORDER BY payload",
                columns(&["id", "payload"]),
            ),
        ] {
            let native = execute(QueryPath::Native, sql, &inputs).await?;
            let spark = execute(QueryPath::Spark { ansi_mode: ansi }, sql, &inputs).await?;
            assert_eq!(spark.result, native.result, "{name}; reference={REFERENCE}");
            for (path, outcome) in [("spark", &spark), ("native", &native)] {
                let branch = &outcome.branches["source"];
                check_requests(branch, &expected, name == "filtered_payload")?;
                assert!(
                    !file.touched_chunks(&branch.reads).is_empty(),
                    "must observe real data payload reads"
                );
                assert!(
                    branch
                        .metrics
                        .parquet_data_file_bytes_received
                        .ok_or("missing byte metric")?
                        > 0
                );
                assert!(
                    branch
                        .metrics
                        .parquet_data_file_range_get_operations
                        .ok_or("missing range metric")?
                        > 0
                );
                assert!(
                    file.touched_chunks(&branch.reads)
                        .iter()
                        .all(|(_, path)| expected.contains(&path[0])),
                    "must not fetch unreferenced column chunks"
                );
                match name {
                    "all_payload" => assert_eq!(
                        file.touched_pages(&branch.reads, "payload").len(),
                        24,
                        "positive control must fetch every payload page"
                    ),
                    "positive_unused" => assert_eq!(
                        file.touched_pages(&branch.reads, "unused").len(),
                        24,
                        "positive control must fetch every otherwise-unused page"
                    ),
                    "filtered_payload" => {
                        assert_eq!(
                            file.touched_chunks(&branch.reads)
                                .iter()
                                .map(|(group, _)| *group)
                                .collect::<BTreeSet<_>>(),
                            BTreeSet::from([1]),
                            "range predicate must skip both other row groups"
                        );
                        assert_eq!(
                            file.touched_pages(&branch.reads, "payload"),
                            BTreeSet::from([(1, vec!["payload".to_string()], 0)]),
                            "row filtering must skip the seven unneeded output pages"
                        );
                    }
                    _ => {}
                }
                println!(
                    "REAL_DELTA_WIDE_READS {}",
                    json!({"case":name,"ansi":ansi,"path":path,"reference":REFERENCE,"touched_chunks":file.touched_chunks(&branch.reads),"payload_pages":file.touched_pages(&branch.reads,"payload"),"unused_pages":file.touched_pages(&branch.reads,"unused")})
                );
            }
            compare_work(
                &format!("wide/{name}/ansi={ansi}"),
                &spark.branches["source"],
                &native.branches["source"],
                std::slice::from_ref(&file),
            )?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn real_delta_in_and_or_keep_known_core_pruning_limits_explicit() -> TestResult {
    let fixture = RealParquetDeltaTable::new_with_two_files("sql-known-pruning-limits")?;
    let files = parquet_files(&fixture)?;
    for ansi in [false, true] {
        for (predicate, expected_files, limited) in [
            ("id IN (3,4)", 2, true),
            ("id IN (3,4,NULL)", 2, true),
            ("id NOT IN (1,2)", 2, true),
            ("id>=3 OR customer_name IS NULL", 2, true),
            ("customer_name='file-b-3' OR customer_name IS NULL", 2, true),
            ("id>=3", 1, false),
            ("customer_name IS NULL", 0, false),
            ("id>=3 OR id IS NULL", 1, false),
        ] {
            let sql = format!("SELECT id FROM source WHERE {predicate} ORDER BY id");
            let inputs = [DeltaBinding {
                name: "source",
                fixture: &fixture,
                snapshot: DeltaSnapshotSelection::Latest,
            }];
            let native = execute(QueryPath::Native, &sql, &inputs).await?;
            let spark = execute(QueryPath::Spark { ansi_mode: ansi }, &sql, &inputs).await?;
            assert_eq!(spark.result, native.result, "reference={REFERENCE}");
            for outcome in [&native, &spark] {
                assert_eq!(
                    outcome.branches["source"].metrics.file_tasks_started,
                    expected_files
                );
                assert!(!outcome.branches["source"].calls.offered_filters.is_empty());
            }
            compare_work(
                &format!("known-core-limit/{predicate}/ansi={ansi}"),
                &spark.branches["source"],
                &native.branches["source"],
                &files,
            )?;
            println!(
                "REAL_DELTA_PRUNING_STATUS {}",
                json!({"predicate":predicate,"ansi":ansi,"reference":REFERENCE,"files_read":expected_files,"actual_file_skipping":!limited,"core_limitation":limited,"required_column_simplification":predicate=="id>=3 OR id IS NULL","record":"1313#issuecomment-6956036083"})
            );
        }
    }
    Ok(())
}
