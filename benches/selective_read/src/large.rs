//! Explicit, staged large-fixture preparation; reader workload adoption is separate.

use super::*;
use std::collections::BTreeMap;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use datafusion::execution::runtime_env::RuntimeEnv;

const AMENDMENT: &str =
    include_str!("../../../docs/content/benchmarks/selective-read-large-workloads.md");

#[derive(Clone)]
pub struct Options {
    pub scale: u16,
    pub fixture: String,
    pub source_from: Option<PathBuf>,
    pub elapsed_seconds: u32,
    pub preflight: bool,
}

impl Options {
    pub fn parse(mut args: BTreeMap<String, String>, preflight: bool) -> Result<Self> {
        let scale = args
            .remove("--scale-factor")
            .ok_or("large requires --scale-factor")?
            .parse()?;
        if ![1, 10, 30, 100, 300].contains(&scale) {
            return Err("large scale must be 1, 10, 30, 100 or 300".into());
        }
        let fixture = args.remove("--fixture").ok_or("large requires --fixture")?;
        if ![
            "source",
            "li.clustered",
            "li.shuffled",
            "wide.clustered",
            "wide.shuffled",
        ]
        .contains(&fixture.as_str())
        {
            return Err("large fixture must be source, li.clustered, li.shuffled, wide.clustered or wide.shuffled".into());
        }
        let elapsed_seconds = args
            .remove("--elapsed-limit-seconds")
            .ok_or("large requires --elapsed-limit-seconds")?
            .parse()?;
        if elapsed_seconds == 0 {
            return Err("elapsed limit must be positive".into());
        }
        Ok(Self {
            scale,
            fixture,
            elapsed_seconds,
            preflight,
            source_from: args.remove("--source-from").map(PathBuf::from),
        })
    }
}

pub fn set_deadline(seconds: u32) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: restore the standard process-termination handler, then arm a process-wide
        // wall-clock deadline before worker threads exist. No Rust runs in a signal handler.
        unsafe {
            if libc::signal(libc::SIGALRM, libc::SIG_DFL) == libc::SIG_ERR {
                return Err(std::io::Error::last_os_error().into());
            }
            libc::alarm(seconds);
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = seconds;
        Err("large preparation requires Linux elapsed-time enforcement".into())
    }
}

fn saved_source(input: &Path, scale: f64) -> Result<(Value, String)> {
    let bytes = fs::read(input.join("manifest.json"))?;
    let parent: Value = serde_json::from_slice(&bytes)?;
    if parent["status"] != "complete"
        || parent["protocol"] != "selective-read-v1"
        || parent["writer"] != fixtures::writer_settings()
        || parent["generator"]["tpchgen"] != "3.0.0"
        || parent["generator"]["tpchgen_git"] != "4f6bf4c5ab40511c8fdef5888fc8d022e5e546d7"
        || parent["generator"]["lockfile_sha256"]
            != fixtures::hash_file(&input.join("generator-Cargo.lock"))?
    {
        return Err("incompatible saved source manifest, generator or writer".into());
    }
    let sources = parent["sources"]
        .as_array()
        .ok_or("missing saved sources")?;
    let matching: Vec<_> = sources
        .iter()
        .filter(|s| s["scale_factor"] == scale)
        .collect();
    if matching.len() != 1 {
        return Err("saved source must contain exactly one entry for the requested scale".into());
    }
    let source = matching[0];
    let files = source["files"].as_array().ok_or("missing source files")?;
    let rows = source["rows"].as_u64().ok_or("invalid source row count")?;
    let (file_rows, file_bytes) =
        files
            .iter()
            .try_fold((0_u64, 0_u64), |(rows, bytes), file| -> Result<_> {
                Ok((
                    rows.checked_add(file["rows"].as_u64().ok_or("invalid source file rows")?)
                        .ok_or("source row total overflow")?,
                    bytes
                        .checked_add(file["bytes"].as_u64().ok_or("invalid source file bytes")?)
                        .ok_or("source byte total overflow")?,
                ))
            })?;
    if source["path"] != format!("sf{scale}/source")
        || source["schema"] != fixtures::delta_schema(&original_schema())?
        || rows == 0
        || rows > (10_500_000.0 * scale).ceil() as u64
        || source["file_count"] != files.len()
        || rows != file_rows
        || source["bytes"].as_u64() != Some(file_bytes)
    {
        return Err("saved source path, scale, rows or byte inventory differs".into());
    }
    Ok((source.clone(), fixtures::hash_bytes(&bytes)))
}

pub fn preflight(config: &Config) -> Result<Value> {
    let options = config.large.as_ref().ok_or("large options missing")?;
    let saved = options
        .source_from
        .as_ref()
        .map(|input| saved_source(input, f64::from(options.scale)))
        .transpose()?;
    // TPC-H has 1.5 million orders/SF and at most seven lineitems/order. Byte coefficients
    // are conservative planning allowances, not claims about compressed output sizes.
    let rows = saved
        .as_ref()
        .map_or(u64::from(options.scale) * 10_500_000, |(source, _)| {
            source["rows"].as_u64().unwrap()
        });
    let existing_source = saved
        .as_ref()
        .map_or(0, |(source, _)| source["bytes"].as_u64().unwrap());
    let source = if saved.is_some() {
        existing_source
    } else {
        rows * 256
    };
    let table_row_bytes = if options.fixture.starts_with("wide.") {
        768
    } else {
        256
    };
    let table = if options.fixture == "source" {
        0
    } else {
        rows * table_row_bytes
    };
    let metadata = u64::from(options.scale) * 64 * MIB;
    let sum = |parts: &[u64]| -> Result<u64> {
        parts.iter().try_fold(0_u64, |sum, part| {
            sum.checked_add(*part)
                .ok_or_else(|| "capacity arithmetic overflow".into())
        })
    };
    let output = sum(&[source, table, metadata])?;
    let spill = if table == 0 { 0 } else { rows * 512 };
    let headroom = config.sort_memory + 64 * MIB;
    let preparation = sum(&[existing_source, output, spill, headroom])?;
    // Later stages retain one fixture and MinIO copy, and one exact result at a time.
    // Reference and comparison SQLite allowances include indexes and journal headroom.
    let reference = if table == 0 {
        0
    } else {
        rows * if table_row_bytes == 768 { 4096 } else { 1024 }
    };
    let export = table;
    let validation = sum(&[existing_source, output, table, reference * 2, export])?;
    let derivatives = sum(&[existing_source, source, table * 3, metadata * 3])?;
    let peak = preparation.max(validation).max(derivatives);
    let additional = peak - existing_source;
    let parent = config
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let available = fixtures::available_disk(parent)?;
    let limit = config.disk_limit.ok_or("large disk limit missing")?;
    Ok(json!({
        "status": "preflight", "profile": "large", "scale_factor": options.scale,
        "fixture": options.fixture, "source_parent_manifest_sha256": saved.as_ref().map(|(_, hash)| hash),
        "rows_for_capacity": rows, "row_basis": if saved.is_some() { "saved source; verified before reuse" } else { "1,500,000 orders/SF times maximum 7 lineitems" },
        "allowances": {"new_source_bytes_per_row": 256, "table_bytes_per_row": table_row_bytes,
            "sort_spill_bytes_per_row": 512, "sqlite_bytes_per_row_per_database": if table_row_bytes == 768 { 4096 } else { 1024 },
            "metadata_bytes_per_sf": 64 * MIB},
        "estimated_bytes": {"existing_source": existing_source, "source_copy_or_generation": source,
            "selected_table": table, "metadata": metadata, "sort_spill": spill,
            "minio_copy": table, "reference_sqlite": reference, "validation_export": export,
            "comparison_sqlite": reference, "two_later_repack_dv_copies": table * 2},
        "phase_peak_bytes": {"preparation": preparation, "later_validation": validation, "later_derivatives": derivatives},
        "estimated_peak_bytes": peak, "additional_disk_required_bytes": additional,
        "disk_limit_bytes": limit, "output_limit_bytes": output, "spill_limit_bytes": spill,
        "spill_headroom_bytes": headroom, "memory_limit_bytes": config.profile.memory_bytes(),
        "sort_memory_bytes": config.sort_memory, "elapsed_limit_seconds": options.elapsed_seconds,
        "free_disk_before_bytes": available, "filesystem_headroom_bytes": 512 * MIB,
        "fits_budget": peak <= limit, "fits_available_disk": additional <= available.saturating_sub(512 * MIB),
        "scope": "one source and selected fixture; validation and derivatives run separately with staged retirement; build and unrelated data are outside this allowance"
    }))
}

pub fn reuse_source(input: &Path, output: &Path, scale: f64, budget: Arc<Budget>) -> Result<Value> {
    let (source, parent_hash) = saved_source(input, scale)?;
    let relative = format!("sf{scale}/source");
    let input_path = input.join(&relative);
    let output_path = output.join(&relative);
    let paths = repack::verified_files(&input_path, &source)?;
    fs::create_dir_all(&output_path)?;
    let mut generator = LineItemGenerator::new(scale, 1, 1).into_iter();
    let mut literals = BTreeSet::new();
    for (index, path) in paths.into_iter().enumerate() {
        let recorded = &source["files"][index];
        let mut actual = fixtures::inspect_file(
            &path,
            original_schema(),
            &recorded["delta_stats"],
            fixtures::GROUP_ROWS,
            8,
        )?;
        actual["path"] = recorded["path"].clone();
        // #314 manifests predate these two inspection fields. inspect_file still verifies
        // the real footer identity; retain the older manifest without inventing its fields.
        for field in ["created_by", "parquet_version"] {
            if recorded.get(field).is_none() {
                actual
                    .as_object_mut()
                    .ok_or("source file metadata")?
                    .remove(field);
            }
        }
        if &actual != recorded {
            return Err("saved source physical metadata differs from the full read".into());
        }
        // Verify all saved values against the pinned source stream, not just a self-reported hash.
        for batch in fixtures::read_batches(&path)? {
            let batch = batch?;
            let rows: Vec<_> = generator.by_ref().take(batch.num_rows()).collect();
            if batch != fixtures::source_batch(&rows)? {
                return Err("saved source values/order differ from the pinned generator".into());
            }
            find_literals(&batch, &mut literals)?;
        }
        budget.copy(
            &path,
            &output_path.join(path.file_name().ok_or("source filename")?),
        )?;
    }
    let literals: Vec<_> = literals.into_iter().collect();
    if generator.next().is_some()
        || literals.is_empty()
        || source["in_literals"] != json!(literals)
        || source["queries"] != queries(&literals, false)
        || source["wide_queries"] != queries(&literals, true)
    {
        return Err("saved source is incomplete or its literals/queries changed".into());
    }
    repack::verified_files(&output_path, &source)?;
    if fixtures::hash_file(&input.join("manifest.json"))? != parent_hash {
        return Err("source manifest changed during reuse".into());
    }
    Ok(source)
}

pub fn finish_manifest(
    manifest: &mut Value,
    config: &Config,
    plan: Value,
    budget: &Budget,
) -> Result<()> {
    let options = config.large.as_ref().ok_or("large options")?;
    manifest["protocol_sha256"] = json!(fixtures::hash_bytes(AMENDMENT.as_bytes()));
    manifest["base_protocol_sha256"] = json!(fixtures::hash_bytes(PROTOCOL.as_bytes()));
    manifest["generator_contract"] = json!("selective-read-large-fixtures-v1");
    manifest["scale_factor"] = json!(options.scale);
    manifest["selected_fixture"] = json!(options.fixture);
    manifest["preparation"]["capacity_plan"] = plan;
    manifest["preparation"]["elapsed_limit_seconds"] = json!(options.elapsed_seconds);
    manifest["preparation"]["output_bytes_before_manifest"] = json!(budget.written_bytes());
    for table in manifest["tables"].as_array_mut().ok_or("tables")? {
        let mut sizes = table["files"]
            .as_array()
            .ok_or("files")?
            .iter()
            .map(|file| file["bytes"].as_u64().ok_or("file bytes"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        sizes.sort_unstable();
        if sizes.is_empty() {
            return Err("large fixture is empty".into());
        }
        table["file_size_bytes"] = json!({"min": sizes[0], "max": sizes[sizes.len()-1],
            "median": (sizes[(sizes.len()-1)/2] + sizes[sizes.len()/2]) as f64 / 2.0});
    }
    Ok(())
}

pub fn sample_disk(
    runtime: Arc<RuntimeEnv>,
    budget: Arc<Budget>,
) -> (mpsc::Sender<()>, thread::JoinHandle<Value>) {
    let (stop, receiver) = mpsc::channel();
    let task = thread::spawn(move || {
        let mut peak_spill = 0;
        let mut peak_combined = 0;
        loop {
            let spill = runtime.disk_manager.spilling_progress().current_bytes;
            peak_spill = peak_spill.max(spill);
            peak_combined = peak_combined.max(spill + budget.written_bytes());
            if receiver.recv_timeout(Duration::from_millis(100))
                != Err(mpsc::RecvTimeoutError::Timeout)
            {
                break;
            }
        }
        json!({"sample_interval_ms": 100, "sampled_peak_spill_bytes": peak_spill,
            "sampled_peak_output_plus_spill_bytes": peak_combined,
            "scope": "budgeted output writes plus native active spill; sampled high-water marks, excludes final manifest and later phases"})
    });
    (stop, task)
}
