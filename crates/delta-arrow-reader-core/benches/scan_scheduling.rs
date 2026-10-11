//! Process-isolated streaming measurements for issue #122.
//!
//! Build with `cargo bench --locked --bench scan_scheduling --no-run`, then use
//! `python3 benches/scan_scheduling.py --help` to run the matrix. Fixture creation,
//! table loading, and planning are outside the execution timer. Each run consumes
//! batches without collecting them and validates the full scan's row count and ID sum.
//! HTTP measurements include the existing controlled server in the child process.

use std::{
    env,
    error::Error,
    fs::{self, File},
    io::{self, Write},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::{Array, Int32Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use delta_arrow_reader::{
    DeltaScanExecutionOptions, DeltaTableBuilder, ParquetReaderBackend, WarmupMode,
};
use futures_util::StreamExt;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use serde_json::{Value, json};

// Reuse the range-planning benchmark's latency and shared-bandwidth model.
#[allow(dead_code)]
#[path = "range_planning/controlled_http.rs"]
mod controlled_http;

#[derive(Debug, Clone, Copy)]
struct TransportProfile {
    name: &'static str,
    request_latency: Duration,
    shared_throughput_bytes_per_second: u64,
}

const HTTP_PROFILE: TransportProfile = TransportProfile {
    name: "http_8ms_64mib",
    request_latency: Duration::from_millis(8),
    shared_throughput_bytes_per_second: 64 * 1024 * 1024,
};

type BenchResult<T = ()> = Result<T, Box<dyn Error>>;

fn main() -> BenchResult {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let result = match args.as_slice() {
        [command, root, shape] if command == "prepare" => prepare(Path::new(root), shape)?,
        [
            command,
            root,
            transport,
            backend,
            partitions,
            cap,
            prefetch,
            delay_us,
            limit,
            timeout_s,
        ] if command == "run" => {
            let options = DeltaScanExecutionOptions::new()
                .with_parquet_backend(match backend.as_str() {
                    "direct" => ParquetReaderBackend::Direct,
                    "kernel" => ParquetReaderBackend::DeltaKernel,
                    _ => return Err("unknown backend".into()),
                })
                .with_max_concurrent_file_reads_per_scan(optional_usize(cap)?)?
                .with_prefetch_files_per_partition(prefetch.parse()?);
            let config = RunConfig {
                options,
                partitions: partitions.parse()?,
                consumer_delay: Duration::from_micros(delay_us.parse()?),
                limit: optional_usize(limit)?,
                timeout: Duration::from_secs(timeout_s.parse()?),
            };
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?
                .block_on(measure(Path::new(root), transport, config))?
        }
        _ => {
            return Err(io::Error::other(
                "usage: scan_scheduling prepare DIR SHAPE | run DIR TRANSPORT BACKEND PARTITIONS CAP PREFETCH DELAY_US LIMIT TIMEOUT_S; use none for CAP/LIMIT defaults",
            ).into());
        }
    };
    println!("{result}");
    Ok(())
}

fn optional_usize(value: &str) -> BenchResult<Option<usize>> {
    Ok(if value == "none" {
        None
    } else {
        Some(value.parse()?)
    })
}

fn prepare(root: &Path, shape: &str) -> BenchResult<Value> {
    let rows_per_file: Vec<usize> = match shape {
        "tiny" => vec![512; 64],
        "unequal-tiny" => std::iter::once(262_144)
            .chain(std::iter::repeat_n(512, 63))
            .collect(),
        "small" => vec![4_096; 64],
        "batches" => vec![32_768; 64],
        "unequal" => std::iter::once(262_144)
            .chain(std::iter::repeat_n(4_096, 63))
            .collect(),
        "large" => vec![262_144; 8],
        _ => return Err(io::Error::other("unknown fixture shape").into()),
    };
    // Refuse to overwrite a fixture or a caller's directory.
    fs::create_dir(root)?;
    fs::create_dir(root.join("_delta_log"))?;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let mut log = File::create(root.join("_delta_log/00000000000000000000.json"))?;
    writeln!(
        log,
        "{}",
        json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}})
    )?;
    writeln!(
        log,
        "{}",
        json!({"metaData":{
            "id":"scan-scheduling-benchmark", "format":{"provider":"parquet","options":{}},
            "schemaString": json!({"type":"struct","fields":[
                {"name":"id","type":"integer","nullable":false,"metadata":{}}
            ]}).to_string(), "partitionColumns":[], "configuration":{}
        }})
    )?;
    let mut next_id = 0_i32;
    let mut data_bytes = 0_u64;
    for (index, rows) in rows_per_file.iter().copied().enumerate() {
        let end = next_id
            .checked_add(i32::try_from(rows)?)
            .ok_or("fixture ID overflow")?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from_iter_values(next_id..end))],
        )?;
        let name = format!("part-{index:03}.parquet");
        let properties = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_max_row_group_row_count(Some(8_192))
            .build();
        let mut writer = ArrowWriter::try_new(
            File::create(root.join(&name))?,
            Arc::clone(&schema),
            Some(properties),
        )?;
        writer.write(&batch)?;
        writer.close()?;
        let size = fs::metadata(root.join(&name))?.len();
        data_bytes += size;
        writeln!(
            log,
            "{}",
            json!({"add":{
                "path":name, "partitionValues":{}, "size":size, "modificationTime":0,
                "dataChange":true, "stats":json!({"numRecords":rows,
                    "minValues":{"id":next_id},"maxValues":{"id":end-1},
                    "nullCount":{"id":0}}).to_string()
            }})
        )?;
        next_id = end;
    }
    let rows = i64::from(next_id);
    let manifest = json!({"shape":shape,"files":rows_per_file.len(),"rows":rows,
        "id_sum":rows*(rows-1)/2,"data_bytes":data_bytes,"rows_per_file":rows_per_file});
    fs::write(root.join("fixture.json"), format!("{manifest}\n"))?;
    Ok(manifest)
}

#[derive(Clone, Copy)]
struct RunConfig {
    options: DeltaScanExecutionOptions,
    partitions: usize,
    consumer_delay: Duration,
    limit: Option<usize>,
    timeout: Duration,
}

async fn measure(root: &Path, transport: &str, config: RunConfig) -> BenchResult<Value> {
    let manifest: Value = serde_json::from_slice(&fs::read(root.join("fixture.json"))?)?;
    let expected_rows = manifest["rows"]
        .as_u64()
        .ok_or("missing fixture row count")?;
    let server = match transport {
        "local" => None,
        "http" => Some(controlled_http::ControlledHttpServer::start(
            root.to_owned(),
            HTTP_PROFILE,
        )?),
        _ => return Err(io::Error::other("unknown transport").into()),
    };
    let uri = server.as_ref().map_or_else(
        || root.to_string_lossy().into_owned(),
        |s| s.url().to_owned(),
    );
    let mut builder = DeltaTableBuilder::new(uri).with_warmup(WarmupMode::QueryPlanning);
    if server.is_some() {
        builder = builder.with_storage_options([("allow_http".into(), "true".into())].into());
    }
    let load_started = Instant::now();
    let table = builder.load_table().await?;
    let load_us = load_started.elapsed().as_micros();
    let planning_started = Instant::now();
    let mut builder = table
        .scan()
        .with_execution_options(config.options)
        .with_target_partitions(config.partitions)?;
    if let Some(limit) = config.limit {
        builder = builder.with_limit(limit);
    }
    let scan = builder.build().await?;
    let planning_us = planning_started.elapsed().as_micros();
    let schema = scan.schema();
    let partitions = scan.partition_count();
    if let Some(server) = &server {
        server.reset_stats();
    }
    let mut stream = scan.into_stream();
    let metrics = stream.metrics();
    let mut rows = 0_u64;
    let mut batches = 0_u64;
    let mut id_sum = 0_i64;
    let mut first_batch_us = None;
    let mut sampled_unfinished_tasks = 0;
    let mut sampled_emitted_ahead = 0;
    let started = Instant::now();
    let result = tokio::time::timeout(config.timeout, async {
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            first_batch_us.get_or_insert_with(|| started.elapsed().as_micros());
            if batch.schema() != schema {
                return Err("output schema changed".into());
            }
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("expected Int32 IDs")?;
            if ids.null_count() != 0 {
                return Err("unexpected null ID".into());
            }
            id_sum += ids.values().iter().map(|id| i64::from(*id)).sum::<i64>();
            rows += batch.num_rows() as u64;
            batches += 1;
            // These sampled counters are observations, not exact semaphore/queue high-water marks.
            let snapshot = metrics.snapshot();
            sampled_unfinished_tasks = sampled_unfinished_tasks.max(
                snapshot
                    .file_tasks_started
                    .saturating_sub(snapshot.file_tasks_completed),
            );
            sampled_emitted_ahead = sampled_emitted_ahead
                .max(snapshot.scheduler_batches_emitted.saturating_sub(batches));
            if !config.consumer_delay.is_zero() {
                tokio::time::sleep(config.consumer_delay).await;
            }
        }
        Ok::<(), Box<dyn Error>>(())
    })
    .await;
    let elapsed_us = started.elapsed().as_micros();
    let status = match result {
        Ok(result) => {
            result?;
            "ok"
        }
        Err(_) => "timeout",
    };
    let snapshot = metrics.snapshot();
    let cap = config
        .options
        .max_concurrent_file_reads_per_scan()
        .unwrap_or(config.partitions * config.options.max_concurrent_file_reads_per_partition());
    if status == "ok" {
        let wanted = config
            .limit
            .map_or(expected_rows, |n| expected_rows.min(n as u64));
        if rows != wanted {
            return Err(format!("expected {wanted} rows, got {rows}").into());
        }
        if config.limit.is_none() {
            if Some(id_sum) != manifest["id_sum"].as_i64() {
                return Err("full-scan ID sum mismatch".into());
            }
            if snapshot.file_tasks_completed
                != manifest["files"].as_u64().ok_or("missing file count")?
            {
                return Err("not every planned file completed".into());
            }
        }
    }
    drop(stream);
    Ok(json!({
        "status":status,"shape":manifest["shape"],"transport":transport,
        "backend":format!("{:?}", config.options.parquet_backend()),
        "http_request_latency_us":server.as_ref().map(|_| HTTP_PROFILE.request_latency.as_micros()),
        "http_shared_bytes_per_second":server.as_ref().map(|_| HTTP_PROFILE.shared_throughput_bytes_per_second),
        "transport_profile":if server.is_some() { HTTP_PROFILE.name } else { "local" },
        "files":manifest["files"],"expected_rows":expected_rows,"data_bytes":manifest["data_bytes"],
        "target_partitions":config.partitions,"actual_partitions":partitions,"scan_cap":cap,
        "per_partition_cap":config.options.max_concurrent_file_reads_per_partition(),
        "prefetch":config.options.prefetch_files_per_partition(),
        "output_buffer_batches":config.options.output_buffer_batches_per_partition(),
        "consumer_delay_us":config.consumer_delay.as_micros(),"limit":config.limit,
        "worker_threads":4,"timeout_seconds":config.timeout.as_secs(),
        "load_us":load_us,"planning_us":planning_us,"first_batch_us":first_batch_us,
        "elapsed_us":elapsed_us,"rows_per_second":if status == "ok" { Some(rows as f64 * 1_000_000.0 / elapsed_us.max(1) as f64) } else { None },
        "rows":rows,"batches":batches,"id_sum":id_sum,"process_peak_rss_bytes":peak_rss_bytes(),
        "file_tasks_started":snapshot.file_tasks_started,"file_tasks_completed":snapshot.file_tasks_completed,
        "partitions_started":snapshot.scan_partitions_started,"partitions_completed":snapshot.scan_partitions_completed,
        "scheduler_batches_emitted":snapshot.scheduler_batches_emitted,
        "sampled_unfinished_file_tasks":sampled_unfinished_tasks,"sampled_emitted_ahead_batches":sampled_emitted_ahead,
        "range_gets":snapshot.parquet_data_file_range_get_operations,"bytes_received":snapshot.parquet_data_file_bytes_received
    }))
}

fn peak_rss_bytes() -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()?
                .checked_mul(1_024)
        })
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_and_http_scans_validate_rows_and_preserve_timeout_results() -> BenchResult {
        let root =
            env::temp_dir().join(format!("dar-scheduling-bench-test-{}", std::process::id()));
        prepare(&root, "small")?;
        let config = RunConfig {
            options: DeltaScanExecutionOptions::new()
                .with_max_concurrent_file_reads_per_scan(Some(1))?,
            partitions: 1,
            consumer_delay: Duration::ZERO,
            limit: None,
            timeout: Duration::from_secs(20),
        };
        for transport in ["local", "http"] {
            let result = measure(&root, transport, config).await?;
            assert_eq!(result["status"], "ok");
            assert_eq!(result["rows"], 262_144);
            assert_eq!(result["file_tasks_completed"], 64);
        }
        let kernel = measure(
            &root,
            "local",
            RunConfig {
                options: config
                    .options
                    .with_parquet_backend(ParquetReaderBackend::DeltaKernel),
                ..config
            },
        )
        .await?;
        assert_eq!(kernel["status"], "ok");
        assert_eq!(kernel["rows"], 262_144);
        let timed_out = measure(
            &root,
            "local",
            RunConfig {
                consumer_delay: Duration::from_secs(1),
                timeout: Duration::from_millis(20),
                ..config
            },
        )
        .await?;
        assert_eq!(timed_out["status"], "timeout");
        assert!(timed_out["rows_per_second"].is_null());
        assert!(timed_out["rows"].as_u64().is_some_and(|n| n < 262_144));
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
