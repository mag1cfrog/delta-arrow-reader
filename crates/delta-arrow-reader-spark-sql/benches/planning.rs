//! Measure logical and physical planning without executing queries.

use std::{collections::BTreeMap, fs, path::Path, sync::Arc, time::Instant};

use arrow::{
    array::{ArrayRef, Int32Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    execution::SessionStateBuilder,
    physical_plan::{ExecutionPlan, collect},
    prelude::{SessionConfig, SessionContext},
};
use delta_arrow_reader::{
    DeltaTableBuilder, WarmupMode,
    datafusion::{DeltaTableProvider, ScanOptions, collect_scan_metrics},
};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use parquet::arrow::ArrowWriter;
use serde::Deserialize;
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "../tests/support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "../tests/support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

const WARMUPS: usize = 2;
const SAMPLES: usize = 9;
const CONFIGURATIONS: [&str; 4] = ["native-plain", "native-configured", "spark-off", "spark-on"];

#[derive(Deserialize)]
struct Corpus {
    id: String,
    inputs: Vec<Value>,
}

#[derive(Deserialize)]
struct Query {
    id: String,
    group: String,
    corpus: String,
    sql: String,
    ansi: Vec<bool>,
    ordered: bool,
}

#[derive(Deserialize)]
struct Manifest {
    source_commit: String,
    corpora: Vec<Corpus>,
    queries: Vec<Query>,
}

fn options(configuration: &str) -> SparkSqlOptions {
    SparkSqlOptions {
        ansi_mode: configuration == "spark-on",
        session_timezone: "UTC".into(),
        ..Default::default()
    }
}

fn applies(query: &Query, configuration: &str, noise: bool) -> bool {
    if noise && query.group == "sweep" {
        return false;
    }
    if configuration.starts_with("spark-") {
        query.ansi.contains(&(configuration == "spark-on"))
    } else {
        query.group == "paired" || (query.group == "stress" && configuration == "native-configured")
    }
}

fn prepare_delta(root: &Path) -> TestResult {
    if root.exists() {
        return Err("Delta destination already exists; choose an empty destination".into());
    }
    fs::create_dir_all(root.join("_delta_log"))?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..1024)),
            Arc::new(Int64Array::from_iter_values(
                (0..1024).map(|n| i64::from(n) * 3),
            )),
            Arc::new(StringArray::from_iter_values(
                (0..1024).map(|n| format!("group-{}", n % 8)),
            )),
        ],
    )?;
    let file = root.join("part-00000.parquet");
    let mut writer = ArrowWriter::try_new(fs::File::create(&file)?, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    let fields = [ ("id", "integer"), ("value", "long"), ("label", "string") ]
        .into_iter().map(|(name, data_type)| json!({"name":name,"type":data_type,"nullable":false,"metadata":{}})).collect::<Vec<_>>();
    let actions = [
        json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        json!({"metaData":{"id":"planning-1379-v1","format":{"provider":"parquet","options":{}},"schemaString":json!({"type":"struct","fields":fields}).to_string(),"partitionColumns":[],"configuration":{},"createdTime":0}}),
        json!({"add":{"path":"part-00000.parquet","partitionValues":{},"size":fs::metadata(file)?.len(),"modificationTime":0,"dataChange":true,"stats":json!({"numRecords":1024,"minValues":{"id":0,"value":0,"label":"group-0"},"maxValues":{"id":1023,"value":3069,"label":"group-7"},"nullCount":{"id":0,"value":0,"label":0}}).to_string()}}),
    ];
    fs::write(
        root.join("_delta_log/00000000000000000000.json"),
        actions
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n",
    )?;
    println!(
        "{}",
        json!({"kind":"delta_prepared","rows":1024,"version":0,"root":root})
    );
    Ok(())
}

async fn build_registered_sessions(
    manifest: &Manifest,
    configuration: &str,
    root: &Path,
) -> TestResult<BTreeMap<String, SessionContext>> {
    let mut sessions = BTreeMap::new();
    for corpus in &manifest.corpora {
        if corpus.id != "paired" && !configuration.starts_with("spark-") {
            continue;
        }
        let builder = SessionStateBuilder::new()
            .with_default_features()
            .with_config(
                SessionConfig::new()
                    .with_target_partitions(1)
                    .with_batch_size(256),
            );
        let session = SessionContext::new_with_state(
            if configuration == "native-plain" {
                builder
            } else {
                configure_spark_sql(builder)
            }
            .build(),
        );
        spark_inputs::register_recorded_inputs_with_partition_count(
            &session,
            &json!({"inputs":corpus.inputs}),
            32,
            1,
        )?;
        if corpus.id == "paired" {
            let columns: Vec<ArrayRef> = (0..400)
                .map(|n| {
                    Arc::new(Int32Array::from_iter_values((0..4).map(|row| row + n))) as ArrayRef
                })
                .collect();
            let schema = Arc::new(Schema::new(
                (0..400)
                    .map(|n| Field::new(format!("c{n:03}"), DataType::Int32, false))
                    .collect::<Vec<_>>(),
            ));
            session.register_batch("wide_inputs", RecordBatch::try_new(schema, columns)?)?;
            let table = DeltaTableBuilder::new(root.to_string_lossy().into_owned())
                .with_warmup(WarmupMode::QueryPlanning)
                .load_table()
                .await?;
            session.register_table(
                "delta_snapshot",
                Arc::new(DeltaTableProvider::try_new(
                    table,
                    ScanOptions {
                        target_partitions: Some(1),
                        ..Default::default()
                    },
                )?),
            )?;
        }
        sessions.insert(corpus.id.clone(), session);
    }
    Ok(sessions)
}

async fn plan(
    session: &SessionContext,
    query: &Query,
    configuration: &str,
) -> TestResult<Arc<dyn ExecutionPlan>> {
    Ok(if configuration.starts_with("spark-") {
        plan_query(session, &query.sql, &options(configuration))
            .await?
            .create_physical_plan(session)
            .await?
    } else {
        session
            .sql(&query.sql)
            .await?
            .create_physical_plan()
            .await?
    })
}

fn scan_evidence(plan: &dyn ExecutionPlan) -> TestResult<Value> {
    let mut scans = Vec::new();
    for scan in collect_scan_metrics(plan) {
        let metrics = scan.snapshot().reader_metrics;
        if metrics.scan_partitions_started != 0
            || metrics.file_tasks_started != 0
            || metrics.parquet_data_file_bytes_received != Some(0)
        {
            return Err(format!("planning started a Delta scan: {metrics:?}").into());
        }
        scans.push(json!({"files_planned":metrics.files_planned,"scan_partitions_planned":metrics.scan_partitions_planned,"bytes_received":metrics.parquet_data_file_bytes_received}));
    }
    Ok(json!(scans))
}

async fn execute_preflight_plan(
    session: &SessionContext,
    physical: Arc<dyn ExecutionPlan>,
    ordered: bool,
) -> TestResult<Value> {
    let schema = physical.schema();
    let scans = collect_scan_metrics(physical.as_ref());
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        spark_results::validate_batch(schema.as_ref(), &batch)?;
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| spark_results::encode_value(column.as_ref(), row))
                    .collect::<TestResult<Vec<_>>>()?,
            );
        }
    }
    if !ordered {
        rows.sort_by_cached_key(|row| json!(row).to_string());
    }
    let mut logical_schema = spark_results::capture_schema(schema.fields(), None)?;
    spark_results::remove_nullability_and_metadata(&mut logical_schema);
    let arrow_types = schema
        .fields()
        .iter()
        .map(|field| field.data_type().to_string())
        .collect::<Vec<_>>();
    let delta_io = scans
        .iter()
        .map(|scan| {
            let metrics = scan.snapshot().reader_metrics;
            json!({
                "files_planned":metrics.files_planned,
                "file_tasks_started":metrics.file_tasks_started,
                "range_gets":metrics.parquet_data_file_range_get_operations,
                "full_gets":metrics.parquet_data_file_full_get_operations,
                "bytes_received":metrics.parquet_data_file_bytes_received,
                "reader_metrics":format!("{metrics:?}")
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"schema":logical_schema,"arrow_types":arrow_types,"rows":rows,"delta_io":delta_io}))
}

async fn preflight(manifest: &Manifest, root: &Path) -> TestResult {
    let mut expected = BTreeMap::<String, Value>::new();
    let mut mismatches = Vec::new();
    for configuration in CONFIGURATIONS {
        let sessions = build_registered_sessions(manifest, configuration, root).await?;
        for query in &manifest.queries {
            // Untimed correctness includes stress in both native sessions.
            if !applies(query, configuration, false)
                && !(configuration == "native-plain" && query.group == "stress")
            {
                continue;
            }
            let session = &sessions[&query.corpus];
            let physical = match plan(session, query, configuration).await {
                Ok(plan) => plan,
                Err(error) if query.group == "sweep" => {
                    println!(
                        "{}",
                        json!({"kind":"eligibility","configuration":configuration,"id":query.id,"status":"excluded","error":error.to_string()})
                    );
                    continue;
                }
                Err(error) => return Err(format!("{} {configuration}: {error}", query.id).into()),
            };
            let planning_io = scan_evidence(physical.as_ref())?;
            if query.group == "sweep" {
                println!(
                    "{}",
                    json!({"kind":"eligibility","configuration":configuration,"id":query.id,"status":"included"})
                );
            } else {
                let actual = execute_preflight_plan(session, physical, query.ordered).await?;
                let (equal, extra_io) = if let Some(reference) = expected.get(&query.id) {
                    let total = |result: &Value, key: &str| -> u64 {
                        result["delta_io"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(|scan| scan[key].as_u64())
                            .sum()
                    };
                    let extra_io = [
                        "file_tasks_started",
                        "range_gets",
                        "full_gets",
                        "bytes_received",
                    ]
                    .iter()
                    .any(|key| total(&actual, key) > total(reference, key));
                    (
                        actual["schema"] == reference["schema"]
                            && actual["rows"] == reference["rows"],
                        extra_io,
                    )
                } else {
                    expected.insert(query.id.clone(), actual.clone());
                    (true, false)
                };
                println!(
                    "{}",
                    json!({"kind":"equivalence","configuration":configuration,"id":query.id,"matches":equal,"extra_delta_io":extra_io,"planning_io":planning_io,"result":actual})
                );
                if !equal || extra_io {
                    mismatches.push(format!(
                        "{} {configuration}: values/types match={equal}, extra Delta IO={extra_io}",
                        query.id
                    ));
                }
            }
        }
    }
    if !mismatches.is_empty() {
        return Err(format!(
            "paired/stress preflight mismatch: {}",
            mismatches.join(", ")
        )
        .into());
    }
    Ok(())
}

async fn measure(
    manifest: &Manifest,
    root: &Path,
    configuration: &str,
    process: &str,
    noise: bool,
    eligibility: &Path,
) -> TestResult {
    let records: Vec<Value> = fs::read_to_string(eligibility)?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    let sessions = build_registered_sessions(manifest, configuration, root).await?;
    for query in &manifest.queries {
        if !applies(query, configuration, noise) {
            continue;
        }
        if query.group == "sweep" {
            let record = records
                .iter()
                .find(|r| r["configuration"] == configuration && r["id"] == query.id)
                .ok_or("missing frozen eligibility")?;
            if record["status"] == "excluded" {
                continue;
            }
            if record["status"] != "included" {
                return Err("invalid frozen eligibility".into());
            }
        }
        let session = &sessions[&query.corpus];
        let options = options(configuration);
        let spark = configuration.starts_with("spark-");
        let (mut logical, mut physical, mut total) = (Vec::new(), Vec::new(), Vec::new());
        for sample in 0..WARMUPS + SAMPLES {
            let started = Instant::now();
            let (plan, logical_ns, physical_ns) = if spark {
                let query = plan_query(session, &query.sql, &options).await?;
                let logical_ns = started.elapsed().as_nanos();
                let second = Instant::now();
                let plan = query.create_physical_plan(session).await?;
                (plan, logical_ns, second.elapsed().as_nanos())
            } else {
                let frame = session.sql(&query.sql).await?;
                let logical_ns = started.elapsed().as_nanos();
                let second = Instant::now();
                let plan = frame.create_physical_plan().await?;
                (plan, logical_ns, second.elapsed().as_nanos())
            };
            scan_evidence(plan.as_ref())?;
            std::hint::black_box(plan);
            if sample >= WARMUPS {
                logical.push(logical_ns);
                physical.push(physical_ns);
                total.push(logical_ns + physical_ns);
            }
        }
        println!(
            "{}",
            json!({"kind":"samples","source_commit":manifest.source_commit,"configuration":configuration,"process":process,"id":query.id,"group":query.group,"logical_ns":logical,"physical_ns":physical,"total_ns":total})
        );
    }
    Ok(())
}

async fn profile(
    manifest: &Manifest,
    root: &Path,
    configuration: &str,
    id: &str,
    phase: &str,
) -> TestResult {
    let query = manifest
        .queries
        .iter()
        .find(|query| query.id == id)
        .ok_or("unknown profile query")?;
    let sessions = build_registered_sessions(manifest, configuration, root).await?;
    let session = &sessions[&query.corpus];
    let logical = plan_query(session, &query.sql, &options(configuration)).await?;
    for _ in 0..100 {
        match phase {
            "logical" => {
                std::hint::black_box(
                    plan_query(session, &query.sql, &options(configuration)).await?,
                );
            }
            "physical" => {
                std::hint::black_box(logical.create_physical_plan(session).await?);
            }
            _ => return Err("profile phase must be logical or physical".into()),
        }
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let manifest: Manifest = serde_json::from_str(include_str!("planning/queries.json"))?;
    let mode = args
        .first()
        .ok_or("expected prepare, preflight, measure, or profile")?;
    let root = Path::new(args.get(1).ok_or("missing Delta root")?);
    match mode.as_str() {
        "prepare" if args.len() == 2 => prepare_delta(root),
        "preflight" if args.len() == 2 => preflight(&manifest, root).await,
        "measure" if args.len() == 6 => {
            if !CONFIGURATIONS.contains(&args[2].as_str()) {
                return Err("invalid configuration".into());
            }
            if !["noise", "main"].contains(&args[4].as_str()) {
                return Err("invalid series".into());
            }
            measure(
                &manifest,
                root,
                &args[2],
                &args[3],
                args[4] == "noise",
                Path::new(&args[5]),
            )
            .await
        }
        "profile" if args.len() == 5 && ["spark-off", "spark-on"].contains(&args[2].as_str()) => {
            profile(&manifest, root, &args[2], &args[3], &args[4]).await
        }
        _ => Err("invalid planning benchmark arguments".into()),
    }
}
