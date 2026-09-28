//! The same request, clocks, batch consumption and output contract for both Rust readers.

use std::{
    collections::BTreeMap,
    error::Error,
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use datafusion::{
    arrow::ipc::writer::StreamWriter,
    common::DataFusionError,
    execution::{context::SQLOptions, runtime_env::RuntimeEnvBuilder},
    physical_plan::{displayable, execute_stream},
    prelude::{SessionConfig, SessionContext},
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
pub const TABLE: &str = "bench";
const PROTOCOL: &[u8] =
    include_bytes!("../../../docs/content/benchmarks/selective-read-protocol.md");
const ORACLE: &[u8] = include_bytes!("../oracle.py");
const LOCK: &[u8] = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.lock"));

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub table_uri: String,
    pub snapshot_version: u64,
    pub case_id: String,
    canonical_sql: String,
    comparison_revision: u64,
    protocol_sha256: String,
    fixture_manifest_sha256: String,
    profile: String,
    execution_mode: String,
    purpose: String,
    resource_budget: Value,
    correctness_file: Option<PathBuf>,
    campaign_id: Option<String>,
    run_id: String,
    repetition: Option<u64>,
    order: Option<u64>,
}

impl Request {
    pub fn reuse(&self) -> bool {
        self.execution_mode == "reuse"
    }
    fn query_count(&self) -> usize {
        if self.reuse() { 10 } else { 1 }
    }
    fn timed(&self) -> bool {
        self.purpose == "timing"
    }

    fn validate(&self) -> Result<()> {
        let uri = url::Url::parse(&self.table_uri)?;
        if !matches!(uri.scheme(), "file" | "s3")
            || !uri.username().is_empty()
            || uri.password().is_some()
            || uri.query().is_some()
            || uri.fragment().is_some()
        {
            return Err(
                "expected a file/s3 table URL without embedded credentials, query or fragment"
                    .into(),
            );
        }
        if !matches!(self.execution_mode.as_str(), "open" | "reuse")
            || !matches!(
                self.purpose.as_str(),
                "timing" | "validation" | "diagnostic"
            )
            || self.comparison_revision != 2
            || self.protocol_sha256 != digest(PROTOCOL)
            || !is_hash(&self.fixture_manifest_sha256)
            || self.case_id.is_empty()
            || self.run_id.is_empty()
            || self.canonical_sql.trim().is_empty()
            || self.resource_budget != budget()
        {
            return Err("invalid request or settings differ from the frozen protocol".into());
        }
        Ok(())
    }
}

fn budget() -> Value {
    json!({"worker_threads": 8, "max_blocking_threads": 64, "target_partitions": 8,
        "batch_rows": 8192, "datafusion_pool_bytes": 4_u64 * 1024 * 1024 * 1024,
        "process_memory_bytes": 8_u64 * 1024 * 1024 * 1024, "logical_cpus": 8})
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn digest(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn file_digest(path: &Path) -> Result<String> {
    let mut file = BufReader::new(File::open(path)?);
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hex(&hash.finalize()))
}

fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn read_json(path: &Path) -> Result<Value> {
    Ok(serde_json::from_reader(BufReader::new(File::open(path)?))?)
}

fn write_json(path: &Path, value: &Value) -> Result<()> {
    let mut file = BufWriter::new(File::create_new(path)?);
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

fn context() -> Result<SessionContext> {
    let mut config = SessionConfig::new();
    for (key, value) in [
        ("execution.target_partitions", "8"),
        ("execution.batch_size", "8192"),
        ("execution.parquet.pruning", "true"),
        ("execution.parquet.enable_page_index", "true"),
        ("execution.parquet.pushdown_filters", "true"),
        ("execution.parquet.reorder_filters", "true"),
        ("execution.parquet.schema_force_view_types", "true"),
        ("execution.parquet.metadata_size_hint", "65536"),
        ("optimizer.repartition_file_scans", "true"),
    ] {
        config
            .options_mut()
            .set(&format!("datafusion.{key}"), value)?;
    }
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(4 * 1024 * 1024 * 1024, 1.0)
        .with_metadata_cache_limit(64 * 1024 * 1024)
        .with_file_statistics_cache_limit(0)
        .with_object_list_cache_limit(0)
        .build_arc()?;
    Ok(SessionContext::new_with_config_rt(config, runtime))
}

fn identity(request: &Request, build_hash: &str, config_hash: &str) -> Value {
    json!({"reader_id": crate::READER, "reader_build_sha256": build_hash,
        "reader_config_sha256": config_hash, "comparison_revision": 2,
        "protocol_sha256": request.protocol_sha256,
        "fixture_manifest_sha256": request.fixture_manifest_sha256,
        "case_id": request.case_id, "snapshot_version": request.snapshot_version,
        "canonical_sql_sha256": digest(request.canonical_sql.as_bytes()),
        "native_expression_sha256": null})
}

fn correctness(request: &Request, identity: &Value) -> Result<Value> {
    if !request.timed() {
        return Ok(json!({"status": "not_checked", "reason": "untimed invocation"}));
    }
    let path = request
        .correctness_file
        .as_ref()
        .ok_or("timing requires a correctness certificate")?;
    let proof = read_json(path)?;
    let checks = proof["checks"]
        .as_array()
        .ok_or("missing correctness checks")?;
    if proof["status"] != "passed" || checks.len() != request.query_count() {
        return Err("failed or incomplete correctness certificate".into());
    }
    let mut expected_rows = Vec::new();
    for check in checks {
        if check["status"] != "passed" || check["oracle_sha256"] != digest(ORACLE) {
            return Err("failed or stale oracle check".into());
        }
        for (key, value) in identity.as_object().ok_or("invalid identity")? {
            if check.get(key) != Some(value) {
                return Err(format!("stale correctness field: {key}").into());
            }
        }
        expected_rows.push(
            check["output_rows"]
                .as_u64()
                .ok_or("missing validated row count")?,
        );
    }
    Ok(
        json!({"status": "passed", "artifact_sha256": file_digest(path)?, "path": path,
        "expected_output_rows": expected_rows}),
    )
}

fn failure_status(mut error: &(dyn Error + 'static)) -> &'static str {
    loop {
        if crate::unsupported(error)
            || matches!(
                error.downcast_ref::<DataFusionError>(),
                Some(DataFusionError::NotImplemented(_))
            )
        {
            return "unsupported";
        }
        match error.source() {
            Some(source) => error = source,
            None => return "operational_failure",
        }
    }
}

fn nanos(start: Instant) -> u64 {
    start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX)
}

async fn execute(
    request: &Request,
    context: SessionContext,
    output: &Path,
    record: &mut Value,
    cleanup_start: &mut Option<Instant>,
) -> Result<()> {
    record["phase"] = json!("snapshot_open");
    let session_start = Instant::now();
    crate::register(&context, request).await?;
    let initialization = nanos(session_start);
    if request.timed() && request.reuse() {
        record["initialization_ns"] = json!(initialization);
    }
    let mut durations = Vec::new();
    for index in 0..request.query_count() {
        record["phase"] = json!("query");
        let start = if request.reuse() {
            Instant::now()
        } else {
            session_start
        };
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let frame = context
            .sql_with_options(&request.canonical_sql, options)
            .await?;
        let plan = frame.create_physical_plan().await?;
        let mut stream = execute_stream(Arc::clone(&plan), context.task_ctx())?;
        let result_path = output.join(format!("query-{index}.arrow"));
        let mut writer = if request.purpose == "validation" {
            Some(StreamWriter::try_new(
                BufWriter::new(File::create_new(&result_path)?),
                &stream.schema(),
            )?)
        } else {
            None
        };
        let mut rows = 0_u64;
        let mut batches = 0_u64;
        let mut first = None;
        while let Some(batch) = stream.next().await {
            let batch = match batch {
                Ok(batch) => batch,
                Err(error) => {
                    record["partial_query"] = json!({"query_index": index, "output_rows": rows,
                        "output_batches": batches,
                        "elapsed_ns": if request.timed() { Some(nanos(start)) } else { None },
                        "first_batch_ns": if request.timed() { first } else { None }});
                    *cleanup_start = Some(Instant::now());
                    return Err(error.into());
                }
            };
            if batch.num_rows() > 0 && first.is_none() {
                first = Some(nanos(start));
            }
            rows += batch.num_rows() as u64;
            batches += 1;
            if let Some(writer) = writer.as_mut() {
                for offset in (0..batch.num_rows()).step_by(8192) {
                    writer.write(&batch.slice(offset, (batch.num_rows() - offset).min(8192)))?;
                }
            }
            // Drop each batch before polling the next one; timed mode retains no output values.
        }
        let completion = nanos(start);
        if index + 1 == request.query_count() {
            if request.timed() {
                record["session_elapsed_ns"] = json!(nanos(session_start));
            }
            *cleanup_start = Some(Instant::now());
        }
        drop(stream);
        durations.push(completion);
        let mut query = json!({"query_index": index, "output_rows": rows, "output_batches": batches,
            "completion_ns": if request.timed() { Some(completion) } else { None },
            "first_batch_ns": if request.timed() { first } else { None },
            "first_batch_unavailable_reason": if !request.timed() { Some("untimed invocation") } else if rows == 0 { Some("empty result") } else { None },
            "result": null, "identity": null, "physical_plan": null});
        if let Some(mut writer) = writer {
            writer.finish()?;
            writer.into_inner()?.flush()?;
            let mut exported = record["identity"].clone();
            exported["result_sha256"] = json!(file_digest(&result_path)?);
            let identity_name = format!("query-{index}.identity.json");
            write_json(&output.join(&identity_name), &exported)?;
            query["result"] = json!(format!("query-{index}.arrow"));
            query["identity"] = json!(identity_name);
        }
        if request.purpose == "diagnostic" {
            let name = format!("query-{index}.plan.txt");
            fs::write(
                output.join(&name),
                format!("{}", displayable(plan.as_ref()).indent(true)),
            )?;
            query["physical_plan"] = json!(name);
        }
        record["queries"]
            .as_array_mut()
            .ok_or("missing query records")?
            .push(query);
    }
    if request.timed() {
        if request.reuse() {
            record["initialization_plus_query1_ns"] = json!(initialization + durations[0]);
            record["initialization_plus_all_queries_ns"] =
                json!(initialization + durations.iter().sum::<u64>());
        } else {
            record["open_query_ns"] = json!(durations[0]);
        }
    } else {
        record["provider_evidence"] = crate::provider_evidence(&context, request.reuse()).await?;
    }
    record["capability"] = json!({"status": "supported", "scope": "requested query and snapshot", "evidence_run_id": request.run_id});
    record["phase"] = json!("complete");
    Ok(())
}

fn run(request_path: &Path, output: &Path) -> Result<bool> {
    let request: Request = serde_json::from_reader(BufReader::new(File::open(request_path)?))?;
    request.validate()?;
    fs::create_dir(output)?;
    let executable = std::env::current_exe()?;
    let build_path = executable
        .parent()
        .ok_or("missing executable parent")?
        .join("build.json");
    let build = read_json(&build_path)?;
    if build["reader_id"] != crate::READER
        || build["executable_sha256"] != file_digest(Path::new("/proc/self/exe"))?
        || build["lockfile_sha256"] != digest(LOCK)
    {
        return Err(
            "build record does not match the running executable and compiled lockfile".into(),
        );
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .max_blocking_threads(64)
        .enable_all()
        .build()?;
    let context = {
        let _guard = runtime.enter();
        context()?
    };
    let options = context
        .state()
        .config_options()
        .entries()
        .into_iter()
        .chain(context.runtime_env().config_entries())
        .map(|entry| (entry.key, entry.value))
        .collect::<BTreeMap<_, _>>();
    let settings = json!({"datafusion": options, "provider": crate::provider_settings(&context, request.reuse())?,
        "resource_budget": request.resource_budget, "table_uri": request.table_uri,
        "execution_mode": request.execution_mode, "output_delivery": "streaming"});
    let input_identity = identity(
        &request,
        &file_digest(&build_path)?,
        &digest(&serde_json::to_vec(&settings)?),
    );
    let mut record = json!({"format": "selective-read-observation-v1", "identity": input_identity,
        "campaign_id": request.campaign_id, "run_id": request.run_id, "repetition": request.repetition, "order": request.order,
        "profile": request.profile, "purpose": request.purpose, "execution_mode": request.execution_mode,
        "table_uri": request.table_uri, "canonical_sql": request.canonical_sql, "settings": settings,
        "build_record": build_path, "status": "success", "failure_reason": null, "phase": "correctness_gate",
        "capability": {"status": "not_checked", "scope": "requested query and snapshot"},
        "correctness": null, "provider_evidence": null, "queries": [], "partial_query": null,
        "open_query_ns": null, "initialization_ns": null, "session_elapsed_ns": null,
        "initialization_plus_query1_ns": null, "initialization_plus_all_queries_ns": null, "cleanup_ns": null,
        "external_metrics": {"requests": null, "response_bytes": null, "touched_parquet_objects": null,
            "process_cpu_ns": null, "peak_rss_bytes": null, "reason": "storage observer and process scheduler are separate roadmap slices"},
        "external_resource_limits": {"cpu_affinity": null, "process_memory_bytes": null,
            "reason": "launcher must enforce and record the CPU affinity and process memory limit"}});
    let mut cleanup_start = None;
    match correctness(&request, &record["identity"]) {
        Err(error) => {
            record["status"] = json!("validation_failed");
            record["failure_reason"] = json!(error.to_string());
        }
        Ok(proof) => {
            record["correctness"] = proof;
            if let Err(error) = runtime.block_on(execute(
                &request,
                context,
                output,
                &mut record,
                &mut cleanup_start,
            )) {
                record["status"] = json!(failure_status(error.as_ref()));
                record["failure_reason"] = json!(error.to_string());
                record["capability"]["status"] = json!(if record["status"] == "unsupported" {
                    "unsupported"
                } else {
                    "probe_failed"
                });
            }
        }
    }
    let cleanup_start = cleanup_start.unwrap_or_else(Instant::now);
    // Drop waits for runtime cleanup; the campaign launcher owns the cleanup deadline.
    drop(runtime);
    if request.timed() {
        record["cleanup_ns"] = json!(nanos(cleanup_start));
        if record["status"] == "success" {
            let queries = record["queries"]
                .as_array()
                .ok_or("missing query observations")?;
            if queries.iter().enumerate().any(|(index, query)| {
                query["output_rows"] != record["correctness"]["expected_output_rows"][index]
            }) {
                record["status"] = json!("validation_failed");
                record["failure_reason"] =
                    json!("timed output row count differs from the validated result");
            }
        }
    }
    write_json(&output.join("record.json"), &record)?;
    println!("{}", serde_json::to_string(&record)?);
    Ok(record["status"] == "success")
}

pub fn main() {
    let args = std::env::args_os().collect::<Vec<_>>();
    if args.len() == 2 && args[1] == "--help" {
        println!(
            "{} REQUEST.json NEW_OUTPUT_DIRECTORY",
            env!("CARGO_PKG_NAME")
        );
        return;
    }
    let result = if args.len() == 3 {
        run(Path::new(&args[1]), Path::new(&args[2]))
    } else {
        Err("expected REQUEST.json NEW_OUTPUT_DIRECTORY; see --help".into())
    };
    match result {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!(
                "{}",
                json!({"status": "invalid_input", "failure_reason": error.to_string()})
            );
            std::process::exit(1);
        }
    }
}
