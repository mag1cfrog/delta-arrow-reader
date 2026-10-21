use super::*;
use crate::{
    selective_read::{
        array,
        campaign::evidence,
        object,
        validation::{Affinity, Worker},
    },
    selective_read_build::digest,
};

#[derive(clap::Args)]
pub(crate) struct Args {
    #[arg(long)]
    binary: PathBuf,
    #[arg(long)]
    request: PathBuf,
    #[arg(long)]
    reference: PathBuf,
    #[arg(long)]
    state: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    python: PathBuf,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
    #[arg(long)]
    baseline_binary: Option<PathBuf>,
    #[arg(long)]
    local_table: Option<PathBuf>,
    #[arg(long, default_value = "open", value_parser = ["open", "reuse"])]
    execution_mode: String,
    #[arg(long, value_parser = clap::value_parser!(u8).range(2..=4))]
    concurrent_queries: Option<u8>,
    #[arg(long, conflicts_with = "no_warmup")]
    network_warmup: bool,
    #[arg(long)]
    no_warmup: bool,
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u8).range(1..=5))]
    samples: u8,
}

fn seconds(value: &Value) -> Result<Value> {
    Ok(if value.is_null() {
        Value::Null
    } else {
        json!(value.as_u64().ok_or("invalid query duration")? as f64 / 1e9)
    })
}

fn sample(
    record: &Value,
    capture: &Value,
    payload: &Value,
    mode: &str,
    two_builds: bool,
) -> Result<Value> {
    require(
        record["status"] == "success",
        record["failure_reason"].as_str().unwrap_or("reader failed"),
    )?;
    if two_builds {
        let provider = &record["settings"]["provider"];
        require(
            provider["warmup"] == "Automatic"
                && text(&provider["scan_options"])?.contains("experimental_intra_page_reads: true"),
            "reader default settings changed",
        )?;
    }
    let queries = array(&record["queries"])?;
    let first = queries.first().ok_or("reader returned no queries")?;
    let duration = if two_builds {
        &first["completion_ns"]
    } else if payload["execution_mode"] == "reuse" {
        &record["session_elapsed_ns"]
    } else {
        &record["open_query_ns"]
    };
    Ok(
        json!({"mode":mode, "purpose":payload["purpose"], "repetition":payload["repetition"],
        "rows":first["output_rows"], "seconds":seconds(duration)?,
        "initialization_seconds":seconds(&record["initialization_ns"])?,
        "query_seconds":queries.iter().map(|q| seconds(&q["completion_ns"])).collect::<Result<Vec<_>>>()?,
        "network":capture, "process_cpu_ns":record["supervision"]["process_cpu_ns"], "peak_rss_bytes":record["supervision"]["peak_rss_bytes"],
        "record":format!("{}/reader/record.json", text(&payload["run_id"])?) }),
    )
}

fn invoke(
    args: &Args,
    worker: &mut Worker,
    request: &Value,
    mode: &str,
    purpose: &str,
    repetition: u8,
    protocols: &Path,
) -> Result<Value> {
    let name = format!("{mode}-{purpose}-{repetition}");
    let slot = args.output.join(&name);
    let certificate = args.output.join(format!("{mode}-correctness.json"));
    let mut payload = request.clone();
    payload.as_object_mut().ok_or("invalid request")?.extend(object(&json!({"purpose":purpose, "run_id":name,
        "campaign_id":"intra-page-networks", "repetition":repetition, "correctness_file":certificate,
        "validation_diagnostics":purpose == "validation"}))?.clone());
    println!("START {name}");
    let result = super::run(serde_json::from_value(
        json!({"directory":args.state, "output":slot, "binary":args.binary,
        "baseline_binary":args.baseline_binary, "reference":args.reference, "protocols":protocols, "payload":payload,
        "mode":mode, "remote":args.local_table.is_none(), "network_warmup":args.network_warmup, "no_warmup":args.no_warmup}),
    )?)?;
    let sample = sample(
        &result["record"],
        &result["network"],
        &payload,
        mode,
        args.baseline_binary.is_some(),
    )?;
    if purpose == "validation" {
        let proof = worker.call(&json!({"action":"check-intra", "record":result["record"], "reference":args.reference, "output":slot}))?;
        storage::save(&certificate, &proof)?;
    }
    storage::save(&slot.join("sample.json"), &sample)?;
    println!("{sample}");
    Ok(sample)
}

fn summarize(samples: &[Value], modes: &[&str; 2]) -> Result<Value> {
    let mut result = serde_json::Map::new();
    for mode in modes {
        let selected: Vec<_> = samples.iter().filter(|s| s["mode"] == *mode).collect();
        let times: Vec<_> = selected
            .iter()
            .map(|s| {
                s["seconds"]
                    .as_f64()
                    .ok_or_else(|| "missing sample duration".into())
            })
            .collect::<Result<_>>()?;
        let count = array(&selected.first().ok_or("no timing samples")?["query_seconds"])?.len();
        let queries = (0..count)
            .map(|index| {
                median(
                    selected
                        .iter()
                        .map(|s| {
                            s["query_seconds"][index]
                                .as_f64()
                                .ok_or_else(|| "missing query duration".into())
                        })
                        .collect::<Result<_>>()?,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        result.insert((*mode).to_owned(), json!({"median_seconds":median(times.clone())?,
            "min_seconds":times.iter().copied().reduce(f64::min), "max_seconds":times.iter().copied().reduce(f64::max), "query_medians_seconds":queries}));
    }
    Ok(json!({"samples":samples, "results":result}))
}

pub(in crate::selective_read) fn run(mut args: Args) -> Result<()> {
    require(
        args.baseline_binary.is_none()
            || args.execution_mode == "reuse"
                && args.concurrent_queries.is_none()
                && !args.network_warmup
                && !args.no_warmup,
        "--baseline-binary requires sequential reuse with default warmup",
    )?;
    require(
        args.concurrent_queries.is_none()
            || args.execution_mode == "reuse" && matches!(args.concurrent_queries, Some(2 | 4)),
        "--concurrent-queries requires reuse and a count of 2 or 4",
    )?;
    for path in [
        &mut args.binary,
        &mut args.request,
        &mut args.reference,
        &mut args.state,
        &mut args.harness,
    ] {
        *path = fs::canonicalize(&*path)?;
    }
    if let Some(binary) = &mut args.baseline_binary {
        *binary = fs::canonicalize(&*binary)?;
    }
    let protocols = args
        .harness
        .parent()
        .and_then(Path::parent)
        .ok_or("invalid harness directory")?
        .join("docs/content/benchmarks");
    let mut request = storage::read_json(&args.request)?;
    require(
        request["comparison_revision"] == 6,
        "use the retained published campaign requests",
    )?;
    request["execution_mode"] = json!(args.execution_mode);
    request
        .as_object_mut()
        .ok_or("invalid request")?
        .remove("concurrent_queries");
    if let Some(count) = args.concurrent_queries {
        request["concurrent_queries"] = json!(count);
    }
    let metadata = storage::read_json(&args.reference.join("reference.json"))?;
    require(
        evidence::same_comparison(
            &evidence::comparison(&metadata, &protocols)?,
            &evidence::comparison(&request, &protocols)?,
        ),
        "reference comparison identity mismatch",
    )?;
    require(
        [
            "fixture_manifest_sha256",
            "case_id",
            "snapshot_version",
            "canonical_sql",
        ]
        .iter()
        .all(|k| metadata[*k] == request[*k]),
        "reference identity mismatch",
    )?;
    require(
        metadata["reference_sha256"] == digest(&args.reference.join("reference.parquet"))?,
        "reference values changed",
    )?;
    let profile = if let Some(table) = &args.local_table {
        request["table_uri"] = json!(
            url::Url::from_directory_path(fs::canonicalize(table)?)
                .map_err(|()| "invalid local table")?
                .as_str()
                .trim_end_matches('/')
        );
        Value::Null
    } else {
        require(
            text(&request["table_uri"])?.starts_with("s3://"),
            "expected a remote table",
        )?;
        network::config(&args.state)?.ok_or("a recorded network profile is required")?
    };
    fs::create_dir(&args.output)?;
    args.output = fs::canonicalize(&args.output)?;
    let mut worker = Worker::start(&args.python, &args.harness, &args.output)?;
    let server = storage::state(&args.state)?;
    let _affinity = Affinity::pin(&server, &worker)?;
    let modes = if args.baseline_binary.is_some() {
        ["baseline", "candidate"]
    } else {
        ["off", "auto"]
    };
    let mut builds = serde_json::Map::new();
    for (mode, binary) in modes.iter().zip([
        args.baseline_binary.as_ref().unwrap_or(&args.binary),
        &args.binary,
    ]) {
        let build = storage::read_json(&binary.with_file_name("build.json"))?;
        require(
            build["executable_sha256"] == digest(binary)?,
            "binary differs from build record",
        )?;
        builds.insert((*mode).to_owned(), build);
    }
    let session = if let Some(count) = args.concurrent_queries {
        format!("table open, then {count} concurrent queries sharing the provider")
    } else if args.execution_mode == "open" {
        "one query including table open".into()
    } else {
        "table open, then two sequential queries sharing the provider".into()
    };
    storage::save(
        &args.output.join("experiment.json"),
        &json!({
        "source_request_sha256":digest(&args.request)?, "request":request, "build":builds[modes[1]], "builds":builds,
        "primary_metric":if args.baseline_binary.is_some() { "first query after initialization" } else { "session or open query" },
        "reference":metadata, "network":profile,
        "samples_per_mode":args.samples, "network_warmup":args.network_warmup, "no_warmup":args.no_warmup, "concurrent_queries":args.concurrent_queries,
        "modes":modes, "session":session, "order":(0..args.samples).map(|n| if n % 2 == 0 { modes } else { [modes[1],modes[0]] }).collect::<Vec<_>>(),
        "cache":"fresh processes; reused OS and MinIO caches; validation before timing", "cpu_affinity":server["cpus"]["reader"],
        "process_memory_bytes":8 * 1024u64.pow(3), "resource_boundary":"whole process including startup and cleanup; query time excludes them",
        "preparation":"build, data preparation and reference hashing finish before samples"}),
    )?;
    let _lock = storage::exclusive(&args.state)?;
    storage::verify_server(&args.state)?;
    for mode in modes {
        invoke(
            &args,
            &mut worker,
            &request,
            mode,
            "validation",
            0,
            &protocols,
        )?;
    }
    let mut samples = Vec::new();
    for repetition in 0..args.samples {
        for mode in if repetition % 2 == 0 {
            modes
        } else {
            [modes[1], modes[0]]
        } {
            samples.push(invoke(
                &args,
                &mut worker,
                &request,
                mode,
                "timing",
                repetition,
                &protocols,
            )?);
        }
    }
    let summary = summarize(&samples, &modes)?;
    storage::save(&args.output.join("summary.json"), &summary)?;
    println!("{}", summary["results"]);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_builds_report_first_queries_and_medians_keep_session_metrics_separate() -> Result<()> {
        let record = json!({"status":"success", "settings":{"provider":{"warmup":"Automatic","scan_options":"experimental_intra_page_reads: true"}},
            "queries":[{"completion_ns":7_000_000_000u64,"output_rows":3},{"completion_ns":4_000_000_000u64,"output_rows":3}],
            "initialization_ns":2_000_000_000u64, "session_elapsed_ns":13_000_000_000u64,
            "supervision":{"process_cpu_ns":9,"peak_rss_bytes":100}});
        let payload = json!({"execution_mode":"reuse","purpose":"timing","repetition":0,"run_id":"candidate-timing-0"});
        let mut samples = Vec::new();
        for mode in ["baseline", "candidate"] {
            let row = sample(&record, &json!({"requests":10}), &payload, mode, true)?;
            assert_eq!(row["seconds"], 7.0);
            assert_eq!(row["initialization_seconds"], 2.0);
            assert_eq!(row["query_seconds"], json!([7.0, 4.0]));
            samples.push(row);
        }
        assert_eq!(
            sample(&record, &Value::Null, &payload, "auto", false)?["seconds"],
            13.0
        );
        let summary = summarize(&samples, &["baseline", "candidate"])?;
        assert_eq!(
            summary["results"]["candidate"]["query_medians_seconds"],
            json!([7.0, 4.0])
        );
        assert_eq!(median(vec![3.0, 1.0])?, 2.0);
        assert!(
            sample(
                &json!({"status":"timeout"}),
                &Value::Null,
                &payload,
                "auto",
                false
            )
            .is_err()
        );
        Ok(())
    }
}
