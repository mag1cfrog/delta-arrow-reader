//! Small public-API driver for independently generated numeric-page fixtures.
use datafusion::arrow::ipc::writer::StreamWriter;
use delta_arrow_reader::{
    DeltaComparison, DeltaPredicate, DeltaScalar, DeltaScanExecutionOptions, DeltaStorageOptions,
    DeltaTableBuilder, WarmupMode,
};
use serde_json::json;
use std::{fs::File, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 5 {
        return Err("TABLE_URI TYPE (sparse|dense) NEW_OUTPUT_DIRECTORY".into());
    }
    if !matches!(
        args[2].as_str(),
        "int32" | "int64" | "float" | "double" | "decimal20" | "decimal38"
    ) || !matches!(args[3].as_str(), "sparse" | "dense")
    {
        return Err("unknown type or selection".into());
    }
    let output = std::path::Path::new(&args[4]);
    std::fs::create_dir(output)?;
    let mut storage = DeltaStorageOptions::new();
    for (key, variable) in [
        ("aws_access_key_id", "AWS_ACCESS_KEY_ID"),
        ("aws_secret_access_key", "AWS_SECRET_ACCESS_KEY"),
        ("aws_region", "AWS_REGION"),
        ("aws_endpoint", "AWS_ENDPOINT_URL"),
        ("aws_allow_http", "AWS_ALLOW_HTTP"),
    ] {
        if let Ok(value) = std::env::var(variable) {
            storage.insert(key.into(), value);
        }
    }
    let started = Instant::now();
    let table = DeltaTableBuilder::new(&args[1])
        .with_storage_options(storage)
        .load_table()
        .await?;
    let initialization = started.elapsed();
    assert_eq!(table.version(), 0);
    let columns: Vec<_> = std::iter::once("row_id".to_owned())
        .chain((0..8).map(|i| format!("{}_{i}", args[2])))
        .collect();
    let validate = std::env::var_os("NUMERIC_PAGE_VALIDATE").is_some();
    let mut samples = Vec::new();
    for query in 0..2 {
        let started = Instant::now();
        let mut scan = table
            .scan()
            .with_projection(columns.clone())
            .with_target_partitions(8)?;
        if args[3] == "sparse" {
            scan = scan.with_predicate(DeltaPredicate::Compare {
                column: "is_match".into(),
                op: DeltaComparison::Eq,
                value: DeltaScalar::Int32(1),
            });
        }
        let mut stream = scan.build().await?.into_stream();
        let metrics = stream.metrics();
        let mut batches = Vec::new();
        let mut rows = 0;
        while let Some(batch) = stream.next_batch().await? {
            rows += batch.num_rows();
            if validate {
                batches.push(batch);
            }
        }
        let seconds = started.elapsed().as_secs_f64();
        // Export only in separate validation processes, outside the query interval.
        if let Some(first) = batches.first() {
            let mut writer = StreamWriter::try_new(
                File::create_new(output.join(format!("{query}.arrow")))?,
                &first.schema(),
            )?;
            for batch in &batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }
        samples.push(
            json!({"seconds":seconds,"rows":rows,"metrics":format!("{:?}",metrics.snapshot())}),
        );
    }
    let record = json!({"format":"numeric-page-observation-v1","initialization_seconds":initialization.as_secs_f64(),
        "settings":{"scan_options": format!("{:?}", DeltaScanExecutionOptions::default()), "warmup": format!("{:?}", WarmupMode::default()), "target_partitions":8}, "queries":samples});
    std::fs::write(
        output.join("record.json"),
        serde_json::to_vec_pretty(&record)?,
    )?;
    println!("{record}");
    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .init();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .max_blocking_threads(64)
        .enable_all()
        .build()?
        .block_on(run())
}
