//! Public input preparation for selective-read-v1; no reader timings.

mod control_rows;
mod controls;
mod dv;
mod fixtures;
mod large;
mod repack;

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Date32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::functions::{crypto::expr_fn::sha256, string::expr_fn::concat};
use datafusion::logical_expr::ExprSchemable;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::*;
use futures_util::TryStreamExt;
use serde_json::{Value, json};
use tpchgen::generators::LineItemGenerator;

use fixtures::{BATCH_ROWS, Budget, MIB, Result, TableWriter, original_schema};

const PROTOCOL: &str = include_str!("../../../docs/content/benchmarks/selective-read-protocol.md");
const LOCKFILE: &str = include_str!("../Cargo.lock");
const GENERATOR_SOURCE: &str = concat!(
    include_str!("main.rs"),
    include_str!("fixtures.rs"),
    include_str!("repack.rs"),
    include_str!("control_rows.rs"),
    include_str!("controls.rs"),
    include_str!("dv.rs"),
    include_str!("large.rs")
);

#[derive(Clone, Copy, Debug, PartialEq)]
enum Profile {
    Smoke,
    Development,
    Report,
    Large,
}

impl Profile {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "development" => Ok(Self::Development),
            "report" => Ok(Self::Report),
            "large" => Ok(Self::Large),
            _ => Err(format!("unknown profile: {value}").into()),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Development => "development",
            Self::Report => "report",
            Self::Large => "large",
        }
    }

    fn scales(self) -> (f64, f64) {
        match self {
            Self::Smoke => (0.01, 0.01),
            Self::Development => (1.0, 1.0),
            Self::Report => (10.0, 1.0),
            Self::Large => unreachable!("large scales require explicit configuration"),
        }
    }

    fn memory_bytes(self) -> u64 {
        (if self == Self::Smoke { 4 } else { 16 }) * 1024 * MIB
    }

    fn disk_bytes(self) -> u64 {
        if self == Self::Large {
            return u64::MAX;
        }
        (match self {
            Self::Smoke => 8,
            Self::Development => 64,
            Self::Report => 192,
            Self::Large => unreachable!(),
        }) * 1024
            * MIB
    }
}

#[derive(Clone)]
struct Config {
    profile: Profile,
    output: PathBuf,
    sort_memory: u64,
    disk_limit: Option<u64>,
    repack_from: Option<PathBuf>,
    controls: bool,
    dv_from: Vec<PathBuf>,
    dv_table: Option<String>,
    wide_files_from: Option<PathBuf>,
    large_file_pair: bool,
    large: Option<large::Options>,
}

impl Config {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self> {
        let mut args = args;
        let mut profile = Profile::Smoke;
        let mut output = None;
        let mut sort_memory = None;
        let mut disk_limit = None;
        let mut repack_from = None;
        let mut controls = false;
        let mut dv_from = Vec::new();
        let mut dv_table = None;
        let mut wide_files_from = None;
        let mut large_file_pair = false;
        let mut large_args = std::collections::BTreeMap::new();
        let mut preflight = false;
        while let Some(arg) = args.next() {
            if arg == "--help" || arg == "-h" {
                println!(
                    "selective-read-fixtures --profile smoke|development|report|large --output NEW_DIRECTORY\n\
                     Optional preparation limits: --sort-memory-mib N --disk-limit-mib N\n\
                     Repack existing clustered rows: --repack-from FIXTURE_DIRECTORY\n\
                     Generate fixed within-file controls: --controls\n\
                     Add paired DV snapshots: --dv-from PUBLIC_FIXTURES --dv-from CONTROLS\n\
                     Single wide DV pair: --dv-from FIXTURE_DIRECTORY --dv-table wide.clustered|wide.shuffled\n\
                     Wide file organizations: --wide-files-from FIXTURE_DIRECTORY [--large-file-pair]\n\
                     Large: --scale-factor 1|10|30|100|300 --fixture source|li.clustered|li.shuffled|wide.clustered|wide.shuffled\n\
                     Large limits (required): --disk-limit-mib N --elapsed-limit-seconds N\n\
                     Large options: --source-from FIXTURE_DIRECTORY --preflight\n\
                     Existing output directories are never overwritten."
                );
                std::process::exit(0);
            }
            if arg == "--controls" {
                controls = true;
                continue;
            }
            if arg == "--preflight" {
                preflight = true;
                continue;
            }
            if arg == "--large-file-pair" {
                large_file_pair = true;
                continue;
            }
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {arg}"))?;
            match arg.as_str() {
                "--profile" => profile = Profile::parse(&value)?,
                "--output" => output = Some(PathBuf::from(value)),
                "--repack-from" => repack_from = Some(PathBuf::from(value)),
                "--dv-from" => dv_from.push(PathBuf::from(value)),
                "--dv-table" => dv_table = Some(value),
                "--wide-files-from" => wide_files_from = Some(PathBuf::from(value)),
                "--scale-factor" | "--fixture" | "--source-from" | "--elapsed-limit-seconds" => {
                    if large_args.insert(arg.clone(), value).is_some() {
                        return Err(format!("duplicate option: {arg}").into());
                    }
                }
                "--sort-memory-mib" => {
                    sort_memory = Some(
                        value
                            .parse::<u64>()?
                            .checked_mul(MIB)
                            .ok_or("sort limit overflow")?,
                    )
                }
                "--disk-limit-mib" => {
                    disk_limit = Some(
                        value
                            .parse::<u64>()?
                            .checked_mul(MIB)
                            .ok_or("disk limit overflow")?,
                    )
                }
                _ => return Err(format!("unknown option: {arg}").into()),
            }
        }
        let output = output.ok_or("--output is required")?;
        let large = if profile == Profile::Large {
            if controls || repack_from.is_some() || disk_limit.is_none() {
                return Err("large requires --disk-limit-mib and cannot combine controls/repack preparation".into());
            }
            let options = large::Options::parse(large_args, preflight)?;
            if wide_files_from.is_some() {
                if options.fixture != "wide.files4096"
                    || options.scale == 1
                    || options.source_from.is_some()
                {
                    return Err("large file pairs require --fixture wide.files4096, SF10 or above, and no --source-from".into());
                }
                large_file_pair = true;
            } else if options.fixture == "wide.files4096" {
                return Err("wide.files4096 requires --wide-files-from".into());
            }
            if !dv_from.is_empty()
                && (dv_table.as_deref() != Some(options.fixture.as_str())
                    || options.source_from.is_some())
            {
                return Err("large DV preparation requires --dv-table matching --fixture and no --source-from".into());
            }
            Some(options)
        } else {
            if !large_args.is_empty() || preflight {
                return Err("scale, fixture, source reuse, elapsed limit and preflight require --profile large".into());
            }
            None
        };
        if large_file_pair && wide_files_from.is_none() {
            return Err("--large-file-pair requires --wide-files-from".into());
        }
        if large_file_pair && profile != Profile::Large && profile != Profile::Smoke {
            return Err(
                "large file pairs require a large profile or an explicit smoke check".into(),
            );
        }
        if let Some(table) = &dv_table
            && (dv_from.len() != 1
                || !["wide.clustered", "wide.shuffled"].contains(&table.as_str()))
        {
            return Err(
                "--dv-table requires one --dv-from and a wide clustered/shuffled table".into(),
            );
        }
        if usize::from(controls)
            + usize::from(repack_from.is_some())
            + usize::from(!dv_from.is_empty())
            + usize::from(wide_files_from.is_some())
            > 1
        {
            return Err(
                "controls, repack, DV and wide-file preparation are mutually exclusive".into(),
            );
        }
        let sort_memory =
            sort_memory.unwrap_or(if profile == Profile::Smoke { 512 } else { 4096 } * MIB);
        if !(16 * MIB..=profile.memory_bytes() / 2).contains(&sort_memory) {
            return Err(
                "sort memory must be at least 16 MiB and at most half the profile memory limit"
                    .into(),
            );
        }
        if disk_limit.is_some_and(|v| v == 0 || v > profile.disk_bytes()) {
            return Err(
                "disk limit must be positive and cannot exceed the protocol ceiling".into(),
            );
        }
        if output.exists() {
            return Err(format!("output already exists: {}", output.display()).into());
        }
        Ok(Self {
            profile,
            output,
            sort_memory,
            disk_limit,
            repack_from,
            controls,
            dv_from,
            dv_table,
            wide_files_from,
            large_file_pair,
            large,
        })
    }
}

fn main() -> Result<()> {
    let config = Config::parse(env::args().skip(1))?;
    if let Some(options) = &config.large {
        large::set_deadline(options.elapsed_seconds)?;
    }
    fixtures::limit_memory(config.profile.memory_bytes())?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(4)
        .enable_all()
        .build()?;
    let started = Instant::now();
    // Errors go to stderr; partial output remains without a completion manifest.
    runtime.block_on(generate(&config))?;
    eprintln!(
        "{} {} in {:.1}s",
        if config
            .large
            .as_ref()
            .is_some_and(|options| options.preflight)
        {
            "preflight passed for"
        } else {
            "completed"
        },
        config.output.display(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

async fn generate(config: &Config) -> Result<Value> {
    let capacity = config
        .large
        .as_ref()
        .map(|_| large::preflight(config))
        .transpose()?;
    if let Some(plan) = &capacity {
        if config
            .large
            .as_ref()
            .is_some_and(|options| options.preflight)
        {
            println!("{}", serde_json::to_string_pretty(plan)?);
        }
        if plan["fits_budget"] != true || plan["fits_available_disk"] != true {
            return Err(format!(
                "large preparation capacity check failed: {}",
                serde_json::to_string(plan)?
            )
            .into());
        }
        if config
            .large
            .as_ref()
            .is_some_and(|options| options.preflight)
        {
            return Ok(plan.clone());
        }
    }
    if !config.dv_from.is_empty() {
        return dv::generate(config, capacity);
    }
    if let Some(input) = &config.wide_files_from {
        return repack::wide_pair(config, input, capacity);
    }
    if config.controls {
        return controls::generate(config);
    }
    if let Some(input) = &config.repack_from {
        return repack::generate(config, input);
    }
    let started = Instant::now();
    let output_parent = config
        .output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(output_parent)?;
    let available = fixtures::available_disk(output_parent)?;
    // Leave space for the manifest, diagnostics, and other processes using the filesystem.
    let disk_limit = config
        .disk_limit
        .unwrap_or(config.profile.disk_bytes())
        .min(available.saturating_sub(512 * MIB));
    let spill_reserve = config.sort_memory + 64 * MIB;
    if disk_limit < spill_reserve + 256 * MIB {
        return Err(
            "insufficient free disk for the configured sort buffer and preparation output".into(),
        );
    }
    fs::create_dir(&config.output)?;
    let spill_path = config.output.join("spill");
    fs::create_dir(&spill_path)?;
    let output_limit = capacity
        .as_ref()
        .map_or((disk_limit - spill_reserve) * 2 / 3, |plan| {
            plan["output_limit_bytes"].as_u64().unwrap()
        });
    let spill_limit = capacity
        .as_ref()
        .map_or(disk_limit - spill_reserve - output_limit, |plan| {
            plan["spill_limit_bytes"].as_u64().unwrap()
        });
    let budget = Arc::new(Budget::new(output_limit));
    if let Some(plan) = &capacity {
        budget.write(
            &config.output.join("preparation.json"),
            &serde_json::to_vec_pretty(plan)?,
        )?;
    }
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(config.sort_memory as usize, 1.0)
        .with_temp_file_path(&spill_path)
        .with_max_temp_directory_size(spill_limit)
        .with_metadata_cache_limit(0)
        .with_object_list_cache_limit(0)
        .with_file_statistics_cache_limit(0)
        .build_arc()?;
    let mut session_config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(BATCH_ROWS);
    session_config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = false;
    session_config
        .options_mut()
        .execution
        .sort_spill_reservation_bytes = MIB as usize;
    let context = SessionContext::new_with_config_rt(session_config, runtime.clone());
    let sampler = capacity
        .as_ref()
        .map(|_| large::sample_disk(runtime.clone(), budget.clone()));
    let (original_scale, wide_scale) = config.large.as_ref().map_or_else(
        || config.profile.scales(),
        |options| (f64::from(options.scale), f64::from(options.scale)),
    );
    let mut scales = vec![original_scale];
    if wide_scale != original_scale {
        scales.push(wide_scale);
    }
    let mut sources = Vec::new();
    let mut tables = Vec::new();
    let mut sorts = Vec::new();
    for scale in scales {
        let source_path = config.output.join(format!("sf{scale}/source"));
        let reused = config
            .large
            .as_ref()
            .and_then(|options| options.source_from.as_ref());
        let mut source_manifest = if let Some(input) = reused {
            let source = large::reuse_source(input, &config.output, scale, budget.clone())?;
            if capacity.as_ref().ok_or("missing source capacity plan")?["source_parent_manifest_sha256"]
                != fixtures::hash_file(&input.join("manifest.json"))?
            {
                return Err("source manifest changed after capacity preflight".into());
            }
            source
        } else {
            let mut source = TableWriter::new(&source_path, original_schema(), budget.clone())?;
            let mut generator = LineItemGenerator::new(scale, 1, 1).into_iter();
            let mut literals = BTreeSet::new();
            let mut previous_key = None;
            loop {
                let rows: Vec<_> = generator.by_ref().take(BATCH_ROWS).collect();
                if rows.is_empty() {
                    break;
                }
                for row in &rows {
                    let key = (row.l_orderkey, row.l_linenumber);
                    if previous_key.is_some_and(|previous| previous >= key) {
                        return Err(
                            "source row keys are duplicated or out of generator order".into()
                        );
                    }
                    previous_key = Some(key);
                }
                let batch = fixtures::source_batch(&rows)?;
                find_literals(&batch, &mut literals)?;
                source.push(batch)?;
            }
            if literals.is_empty() || (config.profile != Profile::Smoke && literals.len() < 20) {
                return Err(
                    format!("SF{scale} does not supply the required IN literal set").into(),
                );
            }
            let literals: Vec<_> = literals.into_iter().collect();
            let mut source_manifest = source.finish(None)?;
            source_manifest["scale_factor"] = json!(scale);
            source_manifest["path"] = json!(format!("sf{scale}/source"));
            source_manifest["in_literals"] = json!(literals);
            source_manifest["queries"] = queries(&literals, false);
            source_manifest["wide_queries"] = queries(&literals, true);
            source_manifest
        };
        // Reuse carries identical source metadata and bytes; its provenance is recorded separately.
        source_manifest["scale_factor"] = json!(scale);
        let expected_rows = source_manifest["rows"]
            .as_u64()
            .ok_or("missing source rows")?;
        sources.push(source_manifest);

        for layout in ["clustered", "shuffled"] {
            if scale == original_scale
                && config
                    .large
                    .as_ref()
                    .is_none_or(|options| options.fixture == format!("li.{layout}"))
            {
                let id = format!("li.{layout}");
                let table_path = config.output.join(&id);
                eprintln!("writing {id}, SF{scale}");
                let table = sorted_table(
                    &context,
                    &source_path,
                    &table_path,
                    layout,
                    false,
                    budget.clone(),
                    &mut sorts,
                )
                .await?;
                tables.push(finish_table(
                    table,
                    &id,
                    config.profile,
                    scale,
                    layout,
                    expected_rows,
                )?);
            }
            if scale == wide_scale
                && config
                    .large
                    .as_ref()
                    .is_none_or(|options| options.fixture == format!("wide.{layout}"))
            {
                let id = format!("wide.{layout}");
                let table_path = config.output.join(&id);
                eprintln!("writing {id}, SF{scale}");
                let table = if scale == original_scale
                    && config.output.join(format!("li.{layout}")).is_dir()
                {
                    // Reuse the already sorted narrow files, preserving identical row/file boundaries.
                    let mut table =
                        TableWriter::new(&table_path, fixtures::wide_schema(), budget.clone())?;
                    for path in
                        fixtures::parquet_files(&config.output.join(format!("li.{layout}")))?
                    {
                        for batch in fixtures::read_batches(&path)? {
                            table.push(fixtures::add_payloads(&batch?)?)?;
                        }
                    }
                    table
                } else {
                    sorted_table(
                        &context,
                        &source_path,
                        &table_path,
                        layout,
                        true,
                        budget.clone(),
                        &mut sorts,
                    )
                    .await?
                };
                tables.push(finish_table(
                    table,
                    &id,
                    config.profile,
                    scale,
                    layout,
                    expected_rows,
                )?);
            }
        }
    }
    budget.write(
        &config.output.join("generator-Cargo.lock"),
        LOCKFILE.as_bytes(),
    )?;
    let spilling = runtime.disk_manager.spilling_progress();
    let mut manifest = json!({
        "protocol": "selective-read-v1",
        "protocol_sha256": fixtures::hash_bytes(PROTOCOL.as_bytes()),
        "profile": config.profile.name(), "status": "complete",
        "generator": generator_identity()?,
        "recipe": {
            "source": "LineItemGenerator::new(scale_factor, 1, 1); default distributions, seeds, text pool",
            "reference_order": "original generator order",
            "row_key": ["l_orderkey", "l_linenumber"],
            "clustered": {"ascending": ["l_shipdate", "l_shipmode", "l_partkey", "l_orderkey", "l_linenumber"],
                "string_order": "UTF-8 bytes"},
            "shuffled": {"ascending": ["SHA-256 digest bytes", "l_orderkey", "l_linenumber"],
                "hash_input": "dar-shuffle-v1/20260927/{orderkey}/{linenumber}", "encoding": "UTF-8, no newline"},
            "payload": {"columns": 64, "hash_input": "dar-wide-v1/{orderkey}/{linenumber}/{j:02}",
                "encoding": "UTF-8, no newline", "u": "first 8 SHA-256 bytes as unsigned little-endian",
                "null_when": "u % 17 == 0", "otherwise": "(u & 0x7fffffffffffffff) - 0x4000000000000000"}
        },
        "writer": fixtures::writer_settings(),
        "sources": sources, "tables": tables,
        "preparation": {
            "memory_limit_bytes": config.profile.memory_bytes(),
            "sort_memory_bytes": config.sort_memory,
            "disk_limit_bytes": disk_limit, "output_limit_bytes": output_limit,
            "spill_limit_bytes": spill_limit, "spill_headroom_bytes": spill_reserve,
            "free_disk_before_bytes": available,
            "peak_rss_bytes": fixtures::peak_rss()?,
            "spill_at_completion": {"current_bytes": spilling.current_bytes, "active_files": spilling.active_files_count},
            "sort_operators": sorts,
            "elapsed_ms": started.elapsed().as_millis()
        }
    });
    if let Some(plan) = capacity {
        large::finish_manifest(&mut manifest, config, plan, &budget)?;
    }
    if let Some((stop, sampler)) = sampler {
        drop(stop);
        manifest["preparation"]["disk_observations"] =
            sampler.join().map_err(|_| "disk sampler panicked")?;
    }
    drop(context);
    drop(runtime);
    fs::remove_dir_all(&spill_path)?;
    // This marker is written last; a failed preparation must never appear complete.
    budget.write(
        &config.output.join("manifest.json"),
        &serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(manifest)
}

fn generator_identity() -> Result<Value> {
    Ok(json!({
            "tpchgen": "3.0.0", "tpchgen_git": "4f6bf4c5ab40511c8fdef5888fc8d022e5e546d7",
            "arrow": "58.4.0", "parquet": "58.4.0", "datafusion_sort": "54.1.0",
            "source_sha256": fixtures::hash_bytes(GENERATOR_SOURCE.as_bytes()),
            "lockfile_sha256": fixtures::hash_bytes(LOCKFILE.as_bytes()),
            // Read the running inode even when a concurrent build replaces its pathname.
            "executable_sha256": fixtures::hash_file(Path::new("/proc/self/exe"))?,
            "target": format!("{}-{}", env::consts::ARCH, env::consts::OS)
    }))
}

async fn sorted_table(
    context: &SessionContext,
    source: &Path,
    output: &Path,
    layout: &str,
    wide: bool,
    budget: Arc<Budget>,
    sorts: &mut Vec<Value>,
) -> Result<TableWriter> {
    let frame = context
        .read_parquet(
            source.to_str().ok_or("non-UTF8 source path")?,
            ParquetReadOptions::default(),
        )
        .await?;
    let mut order = if layout == "clustered" {
        vec![
            col("l_shipdate").sort(true, false),
            col("l_shipmode").sort(true, false),
            col("l_partkey").sort(true, false),
        ]
    } else {
        vec![
            sha256(concat(vec![
                lit("dar-shuffle-v1/20260927/"),
                col("l_orderkey").cast_to(&DataType::Utf8, frame.schema())?,
                lit("/"),
                col("l_linenumber").cast_to(&DataType::Utf8, frame.schema())?,
            ]))
            .sort(true, false),
        ]
    };
    order.extend([
        col("l_orderkey").sort(true, false),
        col("l_linenumber").sort(true, false),
    ]);
    let plan = frame.sort(order)?.create_physical_plan().await?;
    let mut stream = execute_stream(plan.clone(), context.task_ctx())?;
    let mut writer = TableWriter::new(
        output,
        if wide {
            fixtures::wide_schema()
        } else {
            original_schema()
        },
        budget,
    )?;
    while let Some(batch) = stream.try_next().await? {
        let batch = fixtures::normalize(&batch, original_schema())?;
        writer.push(if wide {
            fixtures::add_payloads(&batch)?
        } else {
            batch
        })?;
    }
    drop(stream);
    collect_sort_metrics(
        &plan,
        output
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or("fixture name")?,
        sorts,
    );
    Ok(writer)
}

fn collect_sort_metrics(plan: &Arc<dyn ExecutionPlan>, fixture: &str, result: &mut Vec<Value>) {
    if let Some(metrics) = plan.metrics()
        && let Some(spills) = metrics.spill_count()
    {
        result.push(json!({"fixture": fixture, "operator": plan.name(), "spill_count": spills,
            "spilled_bytes": metrics.spilled_bytes(),
            "peak_reservation_bytes": metrics.sum_by_name("peak_mem_used").map(|value| value.as_usize())}));
    }
    for child in plan.children() {
        collect_sort_metrics(child, fixture, result);
    }
}

fn finish_table(
    writer: TableWriter,
    id: &str,
    profile: Profile,
    scale: f64,
    layout: &str,
    rows: u64,
) -> Result<Value> {
    let identity_profile = if profile == Profile::Large {
        format!("large-sf{scale}")
    } else {
        profile.name().to_owned()
    };
    let mut manifest = writer.finish(Some((&identity_profile, id)))?;
    if manifest["rows"] != json!(rows) {
        return Err(format!("{id} changed the source row count").into());
    }
    manifest["id"] = json!(id);
    manifest["path"] = json!(id);
    manifest["scale_factor"] = json!(scale);
    manifest["layout"] = json!(layout);
    manifest["snapshot_version"] = json!(0);
    manifest["deletion_vectors"] = json!(false);
    Ok(manifest)
}

fn find_literals(
    batch: &arrow::record_batch::RecordBatch,
    values: &mut BTreeSet<i64>,
) -> Result<()> {
    let dates = batch
        .column(10)
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or("shipdate type")?;
    let modes = batch
        .column(14)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("shipmode type")?;
    let parts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("partkey type")?;
    for row in 0..batch.num_rows() {
        if dates.value(row) == 9204 && modes.value(row) == "AIR" {
            values.insert(parts.value(row));
            if values.len() > 20 {
                values.pop_last();
            }
        }
    }
    Ok(())
}

fn queries(literals: &[i64], wide: bool) -> Value {
    let keys = "l_orderkey, l_linenumber".to_owned();
    let schema = original_schema();
    let original = schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let wide_columns = format!(
        "l_orderkey, l_linenumber, l_shipdate, l_shipmode, l_partkey, {}",
        (0..64)
            .map(|j| format!("payload_{j:02}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let eq1 = "l_shipdate = DATE '1995-03-15'";
    let eq2 = format!("{eq1} AND l_shipmode = 'AIR'");
    let list = literals
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let in1 = format!("{eq2} AND l_partkey IN ({})", literals[0]);
    let in20 = format!("{eq2} AND l_partkey IN ({list})");
    let date7 = "l_shipdate >= DATE '1995-03-15' AND l_shipdate < DATE '1995-03-22'";
    let q6 = "l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN CAST('0.05' AS DECIMAL(15,2)) AND CAST('0.07' AS DECIMAL(15,2)) AND l_quantity < CAST('24.00' AS DECIMAL(15,2))";
    let q6_projection = "l_orderkey, l_linenumber, l_extendedprice, l_discount".to_owned();
    let shapes = if wide {
        vec![
            ("eq1", &wide_columns, eq1, ""),
            ("eq2", &wide_columns, &eq2, ""),
            ("eq2-in1", &wide_columns, &in1, ""),
            ("eq2-in20", &wide_columns, &in20, ""),
            ("eq2-in20-keys", &keys, &in20, ""),
            ("all-wide", &wide_columns, "", ""),
        ]
    } else {
        vec![
            ("all-keys", &keys, "", ""),
            ("all-full", &original, "", ""),
            ("empty", &original, "l_shipdate < DATE '1990-01-01'", ""),
            ("date7-full", &original, date7, ""),
            ("date7-keys", &keys, date7, ""),
            ("date7-limit", &original, date7, " LIMIT 100"),
            ("q6-scan", &q6_projection, q6, ""),
            ("eq2-in1", &original, &in1, ""),
            ("eq2-in20", &original, &in20, ""),
        ]
    };
    Value::Object(
        shapes
            .into_iter()
            .map(|(name, projection, predicate, limit)| {
                let filter = if predicate.is_empty() {
                    String::new()
                } else {
                    format!(" WHERE {predicate}")
                };
                (
                    name.to_owned(),
                    json!(format!("SELECT {projection} FROM bench{filter}{limit}")),
                )
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests;
