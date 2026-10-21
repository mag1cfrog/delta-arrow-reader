use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    os::unix::process::CommandExt,
    path::Component,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use super::*;
use crate::selective_read::{
    experiment::Process,
    pairs::free_space,
    storage::{read_json, save},
};

const GIB: u64 = 1024 * 1024 * 1024;
const SPILL: u64 = 16 * GIB;

#[derive(clap::Args)]
pub(crate) struct Args {
    #[arg(value_parser = ["probe", "generate"])]
    mode: String,
    #[arg(long)]
    source: PathBuf,
    #[arg(long)]
    plan: PathBuf,
    #[arg(long, value_parser = ["q2", "q4"])]
    shape: String,
    #[arg(long)]
    writer: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    python: PathBuf,
    #[arg(long)]
    probe: Option<PathBuf>,
    #[arg(long, value_parser = ["localized", "scattered"])]
    layout: Option<String>,
    #[arg(long, default_value_t = 196608)]
    disk_limit_mib: u64,
    #[arg(long, default_value_t = 3600)]
    elapsed_limit_seconds: u32,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

fn relative(path: &Path) -> Result<&Path> {
    require(
        !path.as_os_str().is_empty()
            && path
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "expected a relative fixture path",
    )?;
    Ok(path)
}

fn inside(root: &Path, path: &Path) -> Result<PathBuf> {
    let path = fs::canonicalize(root.join(relative(path)?))?;
    require(path.starts_with(root), "fixture path escapes its root")?;
    Ok(path)
}

fn verified(
    root: &Path,
    directory: &Value,
    item: &Value,
    hash: impl Fn(&Path) -> Result<String>,
) -> Result<PathBuf> {
    let path = inside(
        root,
        &Path::new(text(directory)?).join(text(&item["path"])?),
    )?;
    require(
        item["bytes"] == fs::metadata(&path)?.len() && item["sha256"] == hash(&path)?,
        "fixture object changed",
    )?;
    Ok(path)
}

pub(super) fn sum(files: &[Value], field: &str) -> Result<u64> {
    files.iter().try_fold(0u64, |total, file| {
        total
            .checked_add(count(&file[field])?)
            .ok_or_else(|| "size overflow".into())
    })
}

pub(super) fn source(
    root: &Path,
    hash: impl Fn(&Path) -> Result<String>,
) -> Result<(Value, Vec<PathBuf>)> {
    let manifest = read_json(&root.join("manifest.json"))?;
    require(
        manifest["status"] == "complete"
            && manifest["protocol"] == "selective-read-v1"
            && manifest["generator"]["tpchgen"] == "3.0.0"
            && manifest["generator"]["tpchgen_git"] == "4f6bf4c5ab40511c8fdef5888fc8d022e5e546d7"
            && manifest["generator"]["lockfile_sha256"]
                == hash(&root.join("generator-Cargo.lock"))?,
        "source does not identify the pinned completed generator",
    )?;
    let sources: Vec<_> = array(&manifest["sources"])?
        .iter()
        .filter(|source| source["scale_factor"] == 10)
        .collect();
    require(sources.len() == 1, "one verified SF10 source is required")?;
    let source = sources[0];
    let files = array(&source["files"])?;
    let paths: Vec<_> = files
        .iter()
        .map(|file| verified(root, &source["path"], file, &hash))
        .collect::<Result<_>>()?;
    let directory = inside(root, Path::new(text(&source["path"])?))?;
    let listed: BTreeSet<_> = fs::read_dir(directory)?
        .map(|entry| {
            let path = entry?.path();
            Ok(path
                .extension()
                .is_some_and(|ext| ext == "parquet")
                .then_some(fs::canonicalize(path)?))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    let unique: BTreeSet<_> = paths.iter().cloned().collect();
    require(
        unique.len() == paths.len()
            && source["file_count"] == paths.len()
            && listed == unique
            && sum(files, "bytes")? == count(&source["bytes"])?,
        "source inventory changed",
    )?;
    Ok((source.clone(), paths))
}

struct Inputs {
    plan: Value,
    source: Value,
    paths: Vec<PathBuf>,
    shape: Value,
    files: Vec<Value>,
}

fn inputs(args: &Args, root: &Path) -> Result<Inputs> {
    let plan = read_json(&args.plan)?;
    require(
        plan["format"] == "selective-read-production-shape-plan-v1"
            && plan["status"] == "logical_geometry_checked"
            && plan["source_scale"] == 10
            && plan["cases"] == cases()
            && plan["publication_ready"] == false
            && plan["native_campaign_ready"] == false,
        "expected an untimed production layout plan",
    )?;
    require(
        plan["source_manifest_sha256"] == digest(&root.join("manifest.json"))?,
        "source manifest changed",
    )?;
    let (source, paths) = source(root, digest)?;
    planner::check_counts(&plan["predicate_stage_rows"], &source)?;
    let shape = field(&plan["shapes"], &args.shape)?.clone();
    let definitions = shapes(
        count(&plan["file_target_mib"])?,
        count(&plan["data_page_rows"])?,
        count(&plan["data_page_bytes"])?,
        count(&plan["write_batch_rows"])?,
    )?;
    require(
        object(&definitions[&args.shape])?
            .iter()
            .all(|(k, v)| shape[k] == *v),
        "production SQL or shape changed",
    )?;
    let geometry_path = args
        .plan
        .with_file_name(format!("{}-files.json", args.shape));
    require(
        shape["file_geometry_sha256"] == digest(&geometry_path)?,
        "logical file geometry changed",
    )?;
    let files = array(&read_json(&geometry_path)?)?.clone();
    let (candidates, matching) =
        planner::check_geometry(&json!(files), &shape, &plan["predicate_stage_rows"])?;
    require(
        shape["candidate_files"] == candidates && shape["matching_files"] == matching,
        "file selection counts differ from the plan",
    )?;
    let total = count(&shape["files"])?;
    let stripes = count(&shape["stripes"])?;
    require(
        files.len() as u64 == total
            && sum(&files, "rows")? == count(&source["rows"])?
            && sum(&files, "matching_rows")? == count(&shape["output_rows"])?,
        "incomplete file geometry",
    )?;
    let expected = (0..stripes).flat_map(|stripe| {
        (0..(total / stripes + u64::from(stripe < total % stripes)))
            .map(move |index| (stripe, index))
    });
    for (file, (stripe, index)) in files.iter().zip(expected) {
        require(
            file["stripe"] == stripe && file["file_index"] == index,
            "wrong file membership order",
        )?;
        require(
            (1..=1048576).contains(&count(&file["rows"])?)
                && file["candidate"].is_boolean()
                && count(&file["matching_rows"])? <= count(&file["rows"])?
                && (file["matching_rows"] == 0 || file["candidate"] == true),
            "invalid file boundaries",
        )?;
    }
    Ok(Inputs {
        plan,
        source,
        paths,
        shape,
        files,
    })
}

fn check_probe(probe: &Value, plan: &Value, shape: &Value, name: &str) -> Result<()> {
    require(
        probe["protocol"] == "selective-read-production-fixtures-v1"
            && probe["mode"] == "probe"
            && probe["status"] == "complete"
            && probe["shape"] == name
            && probe["source_parent_manifest_sha256"] == plan["source_manifest_sha256"]
            && probe["shape_definition"] == *shape,
        "capacity probe has incompatible source data or layout",
    )
}

pub(super) fn contract(harness: &Path) -> PathBuf {
    harness.join("../../docs/content/benchmarks/selective-read-production-shapes.md")
}

fn probe_selection(files: &[Value], stripes: u64) -> Result<Vec<usize>> {
    let mut selected: BTreeSet<_> = files
        .iter()
        .enumerate()
        .filter_map(|(i, file)| (file["candidate"] == true).then_some(i))
        .collect();
    for stripe in 0..stripes {
        let ordinary: Vec<_> = files
            .iter()
            .enumerate()
            .filter_map(|(i, file)| {
                (file["stripe"] == stripe && file["candidate"] == false).then_some(i)
            })
            .collect();
        require(!ordinary.is_empty(), "stripe has no ordinary interior file")?;
        selected.insert(ordinary[ordinary.len() / 2]);
    }
    Ok(selected.into_iter().collect())
}

fn capacity(
    probe: &Value,
    files: &[Value],
    layout: &str,
    source_bytes: u64,
    probe_bytes: u64,
) -> Result<Value> {
    let table = array(&probe["writer"]["tables"])?
        .iter()
        .find(|table| table["layout"] == layout)
        .ok_or("probe misses the requested layout")?;
    let settings = &table["writer"];
    let batch = count(&settings["write_batch_rows"])?;
    require(batch > 0, "invalid writer batch size")?;
    let ceiling = count(&settings["data_page_rows"])?
        .div_ceil(batch)
        .checked_mul(batch)
        .ok_or("page size overflow")?;
    let samples = array(&table["files"])?;
    let evidence = array(&table["file_evidence"])?;
    require(samples.len() == evidence.len(), "incomplete probe evidence")?;
    let mut rates = BTreeMap::<u64, [(u64, u64); 2]>::new();
    for (file, evidence) in samples.iter().zip(evidence) {
        let ordinal = usize::try_from(count(&evidence["source_file_ordinal"])?)?;
        let planned = files.get(ordinal).ok_or("invalid probe file ordinal")?;
        require(
            evidence["planned"] == *planned && file["rows"] == planned["rows"],
            "probe geometry changed",
        )?;
        require(
            evidence["full_value_roundtrip"] == "passed"
                && count(&evidence["maximum_page_rows"])? <= ceiling,
            "probe checks failed",
        )?;
        // Pretty JSON also bounds compact Delta actions, including non-ASCII escapes.
        let size = |value: &Value| -> Result<u64> {
            Ok(serde_json::to_string_pretty(value)?
                .chars()
                .map(|c| {
                    if c.is_ascii() {
                        1
                    } else if c as u32 <= 0xffff {
                        6
                    } else {
                        12
                    }
                })
                .sum())
        };
        let metadata = count(&file["geometry"]["bytes"])?
            + 2 * size(file)?
            + 2 * size(evidence)?
            + size(&file["delta_stats"])?
            + 1024;
        let rows = count(&file["rows"])?;
        require(rows > 0, "empty probe file")?;
        let values = rates
            .entry(count(&planned["stripe"])?)
            .or_insert([(0, 1); 2]);
        for (rate, value) in values.iter_mut().zip([count(&file["bytes"])?, metadata]) {
            if u128::from(value) * u128::from(rate.1) > u128::from(rate.0) * u128::from(rows) {
                *rate = (value, rows);
            }
        }
    }
    let stripes: BTreeSet<_> = files
        .iter()
        .map(|file| count(&file["stripe"]))
        .collect::<Result<_>>()?;
    require(
        rates.keys().copied().collect::<BTreeSet<_>>() == stripes,
        "probe misses a stripe",
    )?;
    let mut estimates = [0u128; 2];
    for (stripe, values) in rates {
        let rows = files
            .iter()
            .filter(|file| file["stripe"] == stripe)
            .try_fold(0u128, |total, file| {
                Ok::<_, Box<dyn std::error::Error>>(total + u128::from(count(&file["rows"])?))
            })?;
        for (estimate, (numerator, denominator)) in estimates.iter_mut().zip(values) {
            *estimate += (rows * u128::from(numerator) * 5).div_ceil(u128::from(denominator) * 4);
        }
    }
    let [parquet, metadata] = estimates;
    let output = u64::try_from(parquet + metadata + 64 * 1024 * 1024)?;
    let fixed = u128::from(source_bytes) * 2 + u128::from(probe_bytes) + 256 * 1024 * 1024;
    let preparation = u64::try_from(fixed + u128::from(output) + u128::from(SPILL))?;
    let validation = u64::try_from(fixed + u128::from(output) + parquet + metadata)?;
    Ok(
        json!({"basis":"max measured bytes/row in each stripe; 25% headroom; actual writes remain capped",
        "layout":layout,"estimated_parquet_bytes":u64::try_from(parquet)?,"estimated_metadata_bytes":u64::try_from(metadata)?,
        "native_output_limit_bytes":output,"preparation_peak_bytes":preparation,"validation_with_minio_peak_bytes":validation,
        "estimated_phase_peak_bytes":preparation.max(validation),"probe_bytes":probe_bytes,
        "source_bytes_counted_twice":source_bytes * 2,"sort_spill_limit_bytes":SPILL,
        "scope":"one generated layout, retained probe/source and later MinIO/selected-output validation; other layouts/DV copies need separate budgets"}),
    )
}

fn probe_capacity(rows: u64, columns: u64, both: bool, source: u64) -> Result<Value> {
    let output = u64::try_from(
        u128::from(rows)
            .checked_mul(u128::from(columns))
            .and_then(|n| n.checked_mul(if both { 32 } else { 16 }))
            .and_then(|n| n.checked_add(256 * 1024 * 1024))
            .ok_or("size overflow")?,
    )?;
    let peak = u64::try_from(
        u128::from(source) * 2 + u128::from(SPILL) + u128::from(output) + u128::from(GIB),
    )?;
    Ok(
        json!({"estimated_phase_peak_bytes":peak,"native_output_limit_bytes":output,"scope":"bounded writer probe only"}),
    )
}

fn tree_bytes(root: &Path) -> Result<u64> {
    crate::repository::files(root)?
        .iter()
        .try_fold(0u64, |total, path| {
            total
                .checked_add(fs::metadata(path)?.len())
                .ok_or_else(|| "size overflow".into())
        })
}

fn deadline_check(deadline: Instant) -> Result<()> {
    require(
        Instant::now() < deadline,
        "production preparation deadline exceeded",
    )
}

struct TimedRead<R>(R, Instant);

impl<R: Read> Read for TimedRead<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        if Instant::now() >= self.1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "production preparation deadline exceeded",
            ));
        }
        self.0.read(bytes)
    }
}

pub(super) fn hash_before(path: &Path, deadline: Instant) -> Result<String> {
    Ok(reader_sha256(TimedRead(fs::File::open(path)?, deadline))?)
}

fn copy_before(from: &Path, to: &Path, deadline: Instant) -> Result<()> {
    let mut reader = TimedRead(fs::File::open(from)?, deadline);
    let mut writer = fs::File::create_new(to)?;
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let bytes = reader.read(&mut buffer)?;
        if bytes == 0 {
            return Ok(());
        }
        writer.write_all(&buffer[..bytes])?;
    }
}

pub(super) fn spawn(command: &mut Command, memory: u64) -> Result<Process> {
    // SAFETY: the child only changes its own address-space limit before exec.
    unsafe {
        command.pre_exec(move || {
            let limit = libc::rlimit {
                rlim_cur: memory,
                rlim_max: memory,
            };
            if libc::setrlimit(libc::RLIMIT_AS, &limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(Process(command.process_group(0).spawn()?))
}

fn wait(producer: &mut Process, writer: &mut Process, deadline: Instant) -> Result<()> {
    loop {
        deadline_check(deadline)?;
        let a = producer.0.try_wait()?;
        let b = writer.0.try_wait()?;
        require(
            a.is_none_or(|s| s.success()),
            "Arrow producer failed; see producer.stderr.log",
        )?;
        require(
            b.is_none_or(|s| s.success()),
            "native writer failed; see writer.log",
        )?;
        if a.is_some() && b.is_some() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

pub(crate) fn run(args: Args) -> Result<()> {
    require(
        args.disk_limit_mib > 0 && args.disk_limit_mib <= 196608 && args.elapsed_limit_seconds > 0,
        "positive resource limits within the 192 GiB allocation are required",
    )?;
    require(
        !args.output.try_exists()?,
        "output directory already exists",
    )?;
    let root = fs::canonicalize(&args.source)?;
    let harness = fs::canonicalize(&args.harness)?;
    let writer_binary = fs::canonicalize(&args.writer)?;
    let writer_hash = digest(&writer_binary)?;
    let driver_hash = digest(&harness.join("production_fixtures.py"))?;
    let controller_hash = digest(&std::env::current_exe()?)?;
    let plan_hash = digest(&args.plan)?;
    let Inputs {
        plan,
        source,
        paths,
        shape,
        files,
    } = inputs(&args, &root)?;
    let selected = if args.mode == "probe" {
        require(args.probe.is_none(), "invalid probe layout")?;
        probe_selection(&files, count(&shape["stripes"])?)?
    } else {
        (0..files.len()).collect()
    };
    let source_bytes = count(&source["bytes"])?;
    let mut phase = if args.mode == "probe" {
        let rows = selected.iter().try_fold(0u64, |total, i| -> Result<u64> {
            total
                .checked_add(count(&files[*i]["rows"])?)
                .ok_or_else(|| "size overflow".into())
        })?;
        probe_capacity(
            rows,
            count(&shape["stored_columns"])?,
            args.layout.is_none(),
            source_bytes,
        )?
    } else {
        let probe_root = fs::canonicalize(
            args.probe
                .as_ref()
                .ok_or("generate requires --probe and --layout")?,
        )?;
        let layout = args
            .layout
            .as_deref()
            .ok_or("generate requires --probe and --layout")?;
        let probe = read_json(&probe_root.join("manifest.json"))?;
        check_probe(&probe, &plan, &shape, &args.shape)?;
        require(
            probe["writer_result_sha256"] == digest(&probe_root.join("writer-result.json"))?
                && probe["writer"] == read_json(&probe_root.join("writer-result.json"))?,
            "probe writer result changed",
        )?;
        for table in array(&probe["writer"]["tables"])? {
            for file in array(&table["files"])? {
                verified(&probe_root, &table["path"], file, digest)?;
                verified(&probe_root, &table["path"], &file["geometry"], digest)?;
            }
        }
        capacity(
            &probe,
            &files,
            layout,
            source_bytes,
            tree_bytes(&probe_root)?,
        )?
    };
    let limit = args.disk_limit_mib * 1024 * 1024;
    let peak = count(&phase["estimated_phase_peak_bytes"])?;
    require(
        peak <= limit,
        &format!("production phase exceeds disk allowance: {phase}"),
    )?;
    let additional = peak
        .checked_sub(source_bytes)
        .and_then(|n| n.checked_sub(phase["probe_bytes"].as_u64().unwrap_or(0)))
        .ok_or("invalid phase budget")?;
    phase["additional_disk_bytes"] = json!(additional);
    let parent = args
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    require(
        free_space(parent)? >= additional + GIB,
        "insufficient free disk",
    )?;
    fs::create_dir(&args.output)?;
    let output = fs::canonicalize(&args.output)?;
    let mut allocation = phase.clone();
    allocation["disk_limit_bytes"] = json!(limit);
    save(&output.join("capacity.json"), &allocation)?;
    let request = json!({"format":"selective-read-production-write-v1","mode":args.mode,"shape":args.shape,
        "file_target_mib":shape["file_target_mib"],"data_page_rows":shape["data_page_rows"],"data_page_bytes":shape["data_page_bytes"],
        "write_batch_rows":shape["write_batch_rows"],"layout":args.layout,"contract_sha256":digest(&contract(&harness))?,
        "driver_sha256":driver_hash,"files":files,"selected":selected,"projection":shape["projection"],
        "output_limit_bytes":phase["native_output_limit_bytes"]});
    let request_path = output.join("writer-request.json");
    save(&request_path, &request)?;
    let request_hash = digest(&request_path)?;
    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.elapsed_limit_seconds.into());
    let copied = output.join(relative(Path::new(text(&source["path"])?))?);
    fs::create_dir_all(&copied)?;
    let mut copied_paths = Vec::new();
    for (path, file) in paths.iter().zip(array(&source["files"])?) {
        let to = copied.join(relative(Path::new(text(&file["path"])?))?);
        copy_before(path, &to, deadline)?;
        copied_paths.push(to);
    }
    copy_before(
        &root.join("generator-Cargo.lock"),
        &output.join("generator-Cargo.lock"),
        deadline,
    )?;
    save(
        &output.join("producer-request.json"),
        &json!({"paths":copied_paths,"query":format!("SELECT {} FROM source ORDER BY l_orderkey % {}, l_shipdate, l_shipmode, l_linenumber, l_orderkey",super::ORIGINAL.join(", "), count(&shape["stripes"])?)}),
    )?;
    let log = fs::File::create_new(output.join("writer.log"))?;
    let mut producer = spawn(
        Command::new(&args.python)
            .args(["-I", "-B"])
            .arg(harness.join("production_fixtures.py"))
            .arg("--stream")
            .arg(&output)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(fs::File::create_new(output.join("producer.stderr.log"))?),
        8 * GIB,
    )?;
    let mut writer = spawn(
        Command::new(&writer_binary)
            .arg("production-write")
            .arg(&request_path)
            .arg(&output)
            .arg(args.elapsed_limit_seconds.to_string())
            .stdin(producer.0.stdout.take().ok_or("missing producer output")?)
            .stdout(log.try_clone()?)
            .stderr(log),
        8 * GIB,
    )?;
    wait(&mut producer, &mut writer, deadline)?;
    require(
        hash_before(&root.join("manifest.json"), deadline)? == plan["source_manifest_sha256"],
        "source manifest changed during writing",
    )?;
    for (path, file) in copied_paths.iter().zip(array(&source["files"])?) {
        require(
            hash_before(path, deadline)? == file["sha256"],
            "copied source changed",
        )?;
    }
    let result_path = output.join("writer-result.json");
    let result = read_json(&result_path)?;
    require(
        result["status"] == "complete"
            && result["request_sha256"] == request_hash
            && hash_before(&request_path, deadline)? == request_hash,
        "incomplete native write",
    )?;
    require(
        result["generator"]["executable_sha256"] == writer_hash
            && hash_before(&writer_binary, deadline)? == writer_hash,
        "writer binary changed",
    )?;
    let sort = read_json(&output.join("producer-result.json"))?;
    require(
        sort["version"] == "1.5.5" && sort["pyarrow"] == "25.0.1",
        "use the pinned oracle Python environment",
    )?;
    let manifest = json!({"protocol":"selective-read-production-fixtures-v1","status":"complete","mode":args.mode,
        "shape":args.shape,"layout":args.layout,"plan_sha256":plan_hash,"contract_sha256":request["contract_sha256"],
        "driver_sha256":driver_hash,"controller_sha256":controller_hash,"source_parent_manifest_sha256":plan["source_manifest_sha256"],
        "sources":[source],"shape_definition":shape,"writer":result,"writer_result_sha256":hash_before(&result_path, deadline)?,
        "source_sort":{"engine":"duckdb","version":sort["version"],"pyarrow":sort["pyarrow"],"python":sort["python"],
            "threads":4,"memory_limit_bytes":2 * GIB,"spill_limit_bytes":SPILL,"ipc_batch_rows":8192},
        "producer_peak_rss_bytes":sort["peak_rss_bytes"],"capacity":phase,"preparation_seconds":started.elapsed().as_secs_f64(),
        "native_campaign_ready":false,"publication_ready":false});
    deadline_check(deadline)?;
    save(&output.join("manifest.json"), &manifest)?;
    println!(
        "{}",
        json!({"status":"complete","mode":args.mode,"shape":args.shape,
        "tables":array(&result["tables"])?.iter().map(|t|json!({"id":t["id"],"rows":t["rows"],"bytes":t["bytes"],"file_count":t["file_count"]})).collect::<Vec<_>>(),
        "preparation_seconds":manifest["preparation_seconds"]})
    );
    Ok(())
}

#[cfg(test)]
mod tests;
