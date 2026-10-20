use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, File},
    io::{self, Write},
    mem::MaybeUninit,
    os::unix::{fs::PermissionsExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use clap::Args;
use regex::Regex;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    Result,
    artifacts::{create_directory, file_sha256},
    command::{command_output, output_with_timeout},
    repository::cargo_workspace_root,
};

#[derive(Args)]
#[command(long_about = "Measure scan scheduling in fresh child processes.

Build with cargo bench --locked --bench scan_scheduling --no-run, then pass the
executable as --binary LABEL=PATH. The output directory must not exist.
Multiple binaries require --fixture-binary LABEL to choose their shared writer.
Execution timings exclude startup, fixture generation, table loading and planning.
CPU and peak RSS cover the whole child, including the controlled HTTP server.
Warmups prime the OS file cache; sampled task/batch counters are not strict bounds.
See docs/content/contributing.md for outputs and ordering metadata.")]
pub(crate) struct Arguments {
    #[arg(long = "binary", required = true, value_name = "LABEL=PATH", value_parser = parse_binary)]
    binaries: Vec<Binary>,
    #[arg(long, value_name = "LABEL")]
    fixture_binary: Option<String>,
    #[arg(long)]
    output_dir: PathBuf,
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..=128))]
    repetitions: u32,
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..=128))]
    probe_repetitions: u32,
    /// Deadline for performance controls.
    #[arg(long, default_value_t = 15, value_parser = clap::value_parser!(u32).range(1..=120))]
    timeout_seconds: u32,
    /// Deadline for progress probes.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u32).range(1..=120))]
    probe_timeout_seconds: u32,
    /// Rust regular expression matched against case names.
    #[arg(long, default_value = ".")]
    case_filter: Regex,
    #[arg(long, default_value = "all", value_parser = ["all", "performance", "progress"])]
    suite: String,
}

#[derive(Clone)]
struct Binary {
    label: String,
    path: PathBuf,
}

fn parse_binary(
    spec: &str,
) -> std::result::Result<Binary, Box<dyn std::error::Error + Send + Sync>> {
    let (label, path) = spec.split_once('=').ok_or("expected LABEL=PATH")?;
    if label.is_empty()
        || !label
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
    {
        return Err("binary labels must contain only letters, digits, '_' or '-'".into());
    }
    let path = fs::canonicalize(path)?;
    let metadata = fs::metadata(&path)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o111 == 0 {
        return Err(format!("not an executable: {}", path.display()).into());
    }
    Ok(Binary {
        label: label.to_owned(),
        path,
    })
}

#[derive(Clone, Debug)]
struct Case {
    role: &'static str,
    shape: &'static str,
    transport: &'static str,
    partitions: u32,
    cap: Option<u32>,
    prefetch: u32,
    delay_us: u32,
    limit: Option<u32>,
    backend: &'static str,
}

fn optional(value: Option<u32>) -> String {
    value.map_or_else(|| "none".into(), |value| value.to_string())
}

impl Case {
    fn name(&self) -> String {
        format!(
            "{}-{}-p{}-c{}-pf{}-d{}-l{}-{}",
            self.shape,
            self.transport,
            self.partitions,
            optional(self.cap),
            self.prefetch,
            self.delay_us,
            optional(self.limit),
            self.backend,
        )
    }
}

fn cases() -> Vec<Case> {
    let mut result = Vec::new();
    for shape in ["tiny", "unequal-tiny"] {
        for transport in ["local", "http"] {
            for backend in ["direct", "kernel"] {
                result.push(Case {
                    role: "performance",
                    shape,
                    transport,
                    partitions: 64,
                    cap: Some(8),
                    prefetch: 2,
                    delay_us: 0,
                    limit: None,
                    backend,
                });
            }
        }
    }
    for shape in ["small", "batches", "unequal", "large"] {
        for transport in ["local", "http"] {
            let base = Case {
                role: "performance",
                shape,
                transport,
                partitions: 16,
                cap: None,
                prefetch: 2,
                delay_us: 0,
                limit: None,
                backend: "direct",
            };
            // These controls have enough permits for every active partition.
            for (partitions, cap, prefetch) in [
                (1, Some(1), 0),
                (1, Some(8), 2),
                (16, None, 2),
                (16, None, 0),
                (4, Some(8), 0),
                (2, Some(8), 2),
            ] {
                result.push(Case {
                    partitions,
                    cap,
                    prefetch,
                    ..base.clone()
                });
            }
            if matches!(shape, "small" | "large") {
                let files = if shape == "small" { 64 } else { 8 };
                result.push(Case {
                    partitions: files,
                    cap: Some(files),
                    ..base.clone()
                });
            }
            if matches!(shape, "small" | "batches") {
                result.push(Case {
                    delay_us: 1000,
                    ..base.clone()
                });
                result.push(Case {
                    limit: Some(1),
                    ..base.clone()
                });
                for (cap, prefetch) in [(1, 2), (8, 0), (8, 2)] {
                    result.push(Case {
                        role: "progress",
                        cap: Some(cap),
                        prefetch,
                        ..base.clone()
                    });
                }
                // Kernel ignores prefetch but shares the concurrency limiter.
                for (partitions, cap) in [(16, None), (4, Some(8))] {
                    result.push(Case {
                        partitions,
                        cap,
                        backend: "kernel",
                        ..base.clone()
                    });
                }
                result.push(Case {
                    role: "progress",
                    cap: Some(8),
                    backend: "kernel",
                    ..base
                });
            }
        }
    }
    result
}

fn balanced_orders(count: usize) -> Vec<Vec<usize>> {
    let first: Vec<_> = (0..count)
        .map(|position| match position {
            0 => 0,
            p if p % 2 == 1 => p.div_ceil(2),
            p => count - p / 2,
        })
        .collect();
    let mut rows: Vec<Vec<_>> = (0..count)
        .map(|shift| first.iter().map(|index| (index + shift) % count).collect())
        .collect();
    rows.extend(
        rows.clone()
            .into_iter()
            .map(|row| row.into_iter().rev().collect()),
    );
    rows
}

fn ordered_cases(cases: &[Case], repetition: u32) -> Vec<usize> {
    let mut indices: Vec<_> = (0..cases.len()).collect();
    indices.sort_by_cached_key(|index| {
        Sha256::digest(format!("{}:{}", 122 + repetition, cases[*index].name()))
    });
    indices
}

fn files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut result = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            result.extend(files(&path)?);
        } else if path.is_file() {
            result.push(path);
        }
    }
    result.sort();
    Ok(result)
}

fn tree_hash(root: &Path) -> Result<String> {
    let mut digest = Sha256::new();
    for path in files(root)? {
        digest.update(
            path.strip_prefix(root)?
                .to_str()
                .ok_or("non-UTF-8 source path")?,
        );
        digest.update(b"\0");
        for byte in file_sha256(&path)?.as_bytes().as_chunks::<2>().0 {
            digest.update([u8::from_str_radix(std::str::from_utf8(byte)?, 16)?]);
        }
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn child_cpu_seconds() -> Result<f64> {
    let mut usage = MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the supplied struct on success.
    if unsafe { libc::getrusage(libc::RUSAGE_CHILDREN, usage.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: the call above succeeded.
    let usage = unsafe { usage.assume_init() };
    Ok((usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) as f64
        + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1e6)
}

fn load_average() -> Result<[f64; 3]> {
    let mut load = [0.0; 3];
    // SAFETY: load has space for the three requested values.
    if unsafe { libc::getloadavg(load.as_mut_ptr(), 3) } != 3 {
        return Err("could not read all three load averages".into());
    }
    Ok(load)
}

fn timestamp() -> Result<String> {
    Ok(
        command_output(Command::new("date").args(["-u", "+%Y-%m-%dT%H:%M:%SZ"]))?
            .trim()
            .to_owned(),
    )
}

fn measure(command: &mut Command, timeout: Duration) -> Result<Value> {
    let before = child_cpu_seconds()?;
    let output = output_with_timeout(command, timeout);
    let cpu_seconds = child_cpu_seconds()? - before;
    let mut measurement = match output {
        Ok(output) if output.status.success() => {
            match serde_json::from_slice::<Value>(&output.stdout) {
                Ok(value)
                    if value.is_object()
                        && value["status"].is_string()
                        && [
                            "elapsed_us",
                            "first_batch_us",
                            "rows_per_second",
                            "process_peak_rss_bytes",
                        ]
                        .iter()
                        .all(|metric| {
                            value[*metric].is_null()
                                || value[*metric].as_f64().is_some_and(f64::is_finite)
                        }) =>
                {
                    value
                }
                _ => {
                    json!({"status": "invalid_output", "stdout": String::from_utf8_lossy(&output.stdout),
                "stderr": String::from_utf8_lossy(&output.stderr)})
                }
            }
        }
        Ok(output) => json!({"status": "child_error",
            "exit_code": output.status.code().or_else(|| output.status.signal().map(|s| -s)),
            "stderr": String::from_utf8_lossy(&output.stderr)}),
        Err(crate::error::Error::CommandStart { source, .. })
            if source.kind() == io::ErrorKind::TimedOut =>
        {
            json!({"status": "process_timeout"})
        }
        Err(error) => return Err(error.into()),
    };
    measurement["cpu_seconds"] = json!(cpu_seconds);
    measurement["load_average"] = json!(load_average()?[0]);
    Ok(measurement)
}

fn summarize(samples: &[Value], output: &Path) -> Result<()> {
    let mut groups = BTreeMap::<(&str, &str), Vec<&Value>>::new();
    for sample in samples.iter().filter(|s| s["phase"] == "measured") {
        groups
            .entry((
                sample["case"].as_str().ok_or("missing case")?,
                sample["binary"].as_str().ok_or("missing binary")?,
            ))
            .or_default()
            .push(sample);
    }
    if groups.is_empty() {
        return Ok(());
    }
    let mut writer = csv::Writer::from_path(output)?;
    writer.write_record([
        "case",
        "binary",
        "role",
        "runs",
        "completed",
        "timeouts",
        "other_failures",
        "median_elapsed_us",
        "min_elapsed_us",
        "max_elapsed_us",
        "median_first_batch_us",
        "median_rows_per_second",
        "median_process_peak_rss_bytes",
        "median_cpu_seconds",
    ])?;
    for ((case, binary), measurements) in groups {
        let completed = measurements.iter().filter(|s| s["status"] == "ok").count();
        let timeouts = measurements
            .iter()
            .filter(|s| s["status"] == "timeout")
            .count();
        let mut row = vec![
            case.to_owned(),
            binary.to_owned(),
            measurements[0]["role"]
                .as_str()
                .ok_or("missing role")?
                .to_owned(),
            measurements.len().to_string(),
            completed.to_string(),
            timeouts.to_string(),
            (measurements.len() - completed - timeouts).to_string(),
        ];
        // Any failed run censors the whole group, including otherwise valid measurements.
        for metric in [
            "elapsed_us",
            "first_batch_us",
            "rows_per_second",
            "process_peak_rss_bytes",
            "cpu_seconds",
        ] {
            let mut values: Vec<_> = measurements
                .iter()
                .filter(|_| completed == measurements.len())
                .filter_map(|s| s[metric].as_f64())
                .collect();
            values.sort_by(f64::total_cmp);
            row.push(if values.is_empty() {
                String::new()
            } else {
                ((values[(values.len() - 1) / 2] + values[values.len() / 2]) / 2.0).to_string()
            });
            if metric == "elapsed_us" {
                row.push(values.first().map(ToString::to_string).unwrap_or_default());
                row.push(values.last().map(ToString::to_string).unwrap_or_default());
            }
        }
        writer.write_record(row)?;
    }
    writer.flush()?;
    Ok(())
}

fn argument_error(message: &str) -> ! {
    clap::Error::raw(clap::error::ErrorKind::InvalidValue, message).exit()
}

impl Arguments {
    pub(crate) fn run(mut self) -> Result<()> {
        let selected: Vec<_> = cases()
            .into_iter()
            .filter(|case| {
                self.case_filter.is_match(&case.name())
                    && (self.suite == "all" || self.suite == case.role)
            })
            .collect();
        if selected.is_empty() {
            argument_error("no cases selected");
        }
        let labels: BTreeSet<_> = self.binaries.iter().map(|binary| &binary.label).collect();
        if labels.len() != self.binaries.len() {
            argument_error("each binary needs a unique simple LABEL=PATH");
        }
        let fixture_binary = match &self.fixture_binary {
            Some(label) => self
                .binaries
                .iter()
                .position(|binary| binary.label == *label)
                .unwrap_or_else(|| {
                    argument_error("--fixture-binary must name a supplied --binary label")
                }),
            None if self.binaries.len() == 1 => 0,
            None => argument_error("multiple binaries require --fixture-binary LABEL"),
        };
        let repo = cargo_workspace_root(&env::current_dir()?)?;
        create_directory(&self.output_dir)?;
        let root = fs::canonicalize(&self.output_dir)?;
        fs::create_dir(root.join("bin"))?;
        fs::create_dir(root.join("fixtures"))?;
        for binary in &mut self.binaries {
            let target = root.join("bin").join(&binary.label);
            fs::copy(&binary.path, &target)?;
            binary.path = target;
        }
        let mut sources: Vec<_> = [
            "Cargo.toml",
            "Cargo.lock",
            ".cargo/config.toml",
            "crates/delta-arrow-reader-core/Cargo.toml",
            "crates/delta-arrow-reader-core/benches/scan_scheduling.rs",
            "crates/delta-arrow-reader-core/benches/range_planning/controlled_http.rs",
            "crates/delta-arrow-reader-core/tests/scan_scheduling_benchmark_harness.rs",
            "xtask/Cargo.toml",
            "xtask/tests/scan_scheduling.rs",
        ]
        .into_iter()
        .map(|path| repo.join(path))
        .collect();
        sources.extend(files(&repo.join("xtask/src"))?);
        for source in sources {
            let target = root.join("source").join(source.strip_prefix(&repo)?);
            fs::create_dir_all(target.parent().ok_or("missing source parent")?)?;
            fs::copy(source, target)?;
        }
        let mut binaries = BTreeMap::new();
        for binary in &self.binaries {
            binaries.insert(
                &binary.label,
                json!({"path": binary.path, "sha256": file_sha256(&binary.path)?}),
            );
        }
        #[cfg(target_os = "linux")]
        let affinity = {
            let allowed = rustix::thread::sched_getaffinity(None)?;
            Some(
                (0..rustix::thread::CpuSet::MAX_CPU)
                    .filter(|cpu| allowed.is_set(*cpu))
                    .collect::<Vec<_>>(),
            )
        };
        #[cfg(not(target_os = "linux"))]
        let affinity: Option<Vec<usize>> = None;
        let mut metadata = json!({
            "schema_version": 2, "started_at": timestamp()?,
            "command": env::args().collect::<Vec<_>>(),
            "git_head": command_output(Command::new("git").current_dir(&repo).args(["rev-parse", "HEAD"]))?.trim(),
            "production_source_sha256": tree_hash(&repo.join("crates/delta-arrow-reader-core/src"))?,
            "cargo_lock_sha256": file_sha256(&repo.join("Cargo.lock"))?,
            "harness_sha256": file_sha256(&repo.join("crates/delta-arrow-reader-core/benches/scan_scheduling.rs"))?,
            "controlled_http_sha256": file_sha256(&repo.join("crates/delta-arrow-reader-core/benches/range_planning/controlled_http.rs"))?,
            "runner_sha256": tree_hash(&root.join("source/xtask"))?,
            "runner_executable_sha256": file_sha256(&env::current_exe()?)?,
            "platform": command_output(Command::new("uname").args(["-s", "-r", "-m"]))?.trim(),
            "cpu_affinity": affinity, "load_average_before": load_average()?, "worker_threads": 4,
            "binaries": binaries, "fixture_binary": self.binaries[fixture_binary].label,
            "fixtures": {}, "repetitions": self.repetitions, "probe_repetitions": self.probe_repetitions,
            "timeout_seconds": self.timeout_seconds, "probe_timeout_seconds": self.probe_timeout_seconds,
            "ordering_seed": 122, "ordering_algorithm": "sha256-seed-case-v1",
            "method": "fresh process per run; one warmup per performance case; execution-only timer; ID checksum included; process CPU/RSS include setup and controlled HTTP server",
        });
        for shape in selected
            .iter()
            .map(|case| case.shape)
            .collect::<BTreeSet<_>>()
        {
            let fixture = root.join("fixtures").join(shape);
            let prepared = command_output(
                Command::new(&self.binaries[fixture_binary].path)
                    .arg("prepare")
                    .arg(&fixture)
                    .arg(shape),
            )?;
            let mut manifest: serde_json::Map<String, Value> = serde_json::from_str(&prepared)?;
            manifest.insert("sha256".into(), json!(tree_hash(&fixture)?));
            metadata["fixtures"][shape] = manifest.into();
        }
        let metadata_path = root.join("metadata.json");
        fs::write(
            &metadata_path,
            format!("{}\n", serde_json::to_string_pretty(&metadata)?),
        )?;
        let orders = balanced_orders(self.binaries.len());
        let mut samples = Vec::new();
        let mut raw = File::create(root.join("runs.jsonl"))?;
        for phase in ["warmup", "measured"] {
            let rounds = if phase == "warmup" {
                1
            } else {
                self.repetitions.max(self.probe_repetitions)
            };
            for repetition in 0..rounds {
                for (position, index) in
                    ordered_cases(&selected, repetition).into_iter().enumerate()
                {
                    let case = &selected[index];
                    let progress = case.role == "progress";
                    let count = if progress {
                        self.probe_repetitions
                    } else {
                        self.repetitions
                    };
                    if (phase == "warmup" && progress)
                        || (phase == "measured" && repetition >= count)
                    {
                        continue;
                    }
                    let order = &orders[(repetition as usize + index) % orders.len()];
                    for (binary_position, binary) in
                        order.iter().map(|index| &self.binaries[*index]).enumerate()
                    {
                        let deadline = if progress {
                            self.probe_timeout_seconds
                        } else {
                            self.timeout_seconds
                        };
                        let mut measurement = measure(
                            Command::new(&binary.path)
                                .arg("run")
                                .arg(root.join("fixtures").join(case.shape))
                                .args([
                                    case.transport.to_owned(),
                                    case.backend.to_owned(),
                                    case.partitions.to_string(),
                                    optional(case.cap),
                                    case.prefetch.to_string(),
                                    case.delay_us.to_string(),
                                    optional(case.limit),
                                    deadline.to_string(),
                                ]),
                            Duration::from_secs(u64::from(deadline) + 30),
                        )?;
                        let fields = json!({
                            "case": case.name(), "role": case.role, "binary": binary.label, "phase": phase,
                            "repetition": repetition + 1, "order_position": position,
                            "binary_order_position": binary_position,
                        });
                        for (key, value) in fields.as_object().ok_or("invalid run fields")? {
                            measurement[key] = value.clone();
                        }
                        writeln!(raw, "{measurement}")?;
                        raw.flush()?;
                        let failed = measurement["status"] != "ok"
                            && !(progress && measurement["status"] == "timeout");
                        eprintln!(
                            "{phase} {} {} {}: {} {} us",
                            repetition + 1,
                            binary.label,
                            case.name(),
                            measurement["status"],
                            measurement.get("elapsed_us").unwrap_or(&Value::Null)
                        );
                        samples.push(measurement);
                        summarize(&samples, &root.join("summary.csv"))?;
                        if failed {
                            return Err(format!(
                                "benchmark control failed; see {}",
                                root.join("runs.jsonl").display()
                            )
                            .into());
                        }
                    }
                }
            }
        }
        metadata["finished_at"] = json!(timestamp()?);
        metadata["load_average_after"] = json!(load_average()?);
        fs::write(
            metadata_path,
            format!("{}\n", serde_json::to_string_pretty(&metadata)?),
        )?;
        println!("{}", root.join("summary.csv").display());
        Ok(())
    }
}

#[cfg(test)]
mod tests;
