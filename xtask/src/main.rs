mod acceptance;
mod artifacts;
mod binary;
mod build;
mod command;
mod error;
#[cfg(target_os = "linux")]
mod http_fixture;
mod notices;
mod platform;
mod process;
mod python_release;
mod python_upload;
mod python_wheel_reuse;
mod python_wheels;
mod release_plz;
mod repository;
mod scan;

use std::{
    env,
    error::Error,
    fs, io,
    path::PathBuf,
    process::{Command, ExitCode, Stdio, Termination},
};

use clap::{Parser, Subcommand};
use serde::Deserialize;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

/// Repository maintenance tasks.
#[derive(Parser)]
#[command(name = "xtask", bin_name = "cargo xtask")]
struct Arguments {
    #[command(subcommand)]
    command: Task,
}

#[derive(Subcommand)]
enum Task {
    /// Build and validate CLI distribution artifacts.
    Cli {
        #[command(subcommand)]
        command: CliTask,
    },
    /// Check sccache write failures against its error log.
    CheckCacheWrites,
    /// Keep the two most recently used Windows core CI caches.
    PruneCiCaches,
    /// Prepare release-plz configuration for the current shared release baseline.
    PrepareReleasePrConfig { destination: PathBuf },
    /// Validate a Python release event against the checked-out commit.
    CheckPythonReleaseRequest,
    /// Retrieve validated wheels from the merged release PR.
    ReusePythonWheels {
        destination: PathBuf,
        #[arg(long)]
        expected_version: String,
    },
    /// Download and verify a release PR wheel artifact by ID and SHA256.
    DownloadPythonWheels {
        destination: PathBuf,
        #[arg(long)]
        expected_version: String,
        #[arg(long)]
        artifact_id: u64,
        #[arg(long)]
        sha256: String,
    },
    /// Validate one Python wheel's filename and metadata.
    CheckPythonWheel {
        path: PathBuf,
        #[arg(long)]
        expected_version: String,
        #[arg(long)]
        platform_tag: String,
    },
    /// Validate the complete set of Python wheel artifacts.
    CheckPythonArtifacts {
        directory: PathBuf,
        #[arg(long)]
        expected_version: String,
    },
    /// Stage wheels missing from the selected Python package index.
    StagePythonWheels {
        artifacts: PathBuf,
        #[arg(long)]
        expected_version: String,
        #[arg(long)]
        index: String,
        #[arg(long)]
        destination: PathBuf,
    },
}

#[derive(Subcommand)]
enum CliTask {
    /// Validate the source checkout and resolve its installed build toolchain.
    Prepare(build::PrepareArgs),
    /// Build and check one CLI architecture inside Debian 12.
    Build(build::BuildArgs),
    /// Build, check, collect notices, and package one CLI architecture.
    BuildArchive(build::BuildArchiveArgs),
    /// Inspect and execute a binary on a compatible Linux runtime.
    CheckBinary(binary::CheckBinaryArgs),
    /// Run runtime acceptance tests against an extracted CLI archive.
    TestArchive(acceptance::TestArchiveArgs),
    /// Check CLI arguments, inspection, output failures, and process lifecycle on Linux.
    TestProcess(process::TestProcessArgs),
    /// Check Arrow IPC output and scan process lifecycle on Linux.
    TestScan(scan::TestScanArgs),
    /// Collect dependency redistribution text for one CLI target.
    Notices(notices::NoticeArgs),
    /// Package, extract, assemble, or verify CLI binary archives.
    Artifacts {
        #[command(subcommand)]
        command: artifacts::ArtifactTask,
    },
}

fn main() -> ExitCode {
    match run_task(Arguments::parse().command) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => snafu::Report::from_error(error.as_ref()).report(),
    }
}

fn run_task(task: Task) -> Result<()> {
    match task {
        Task::Cli { command } => command.run().map_err(Into::into),
        Task::CheckCacheWrites => check_cache_writes(),
        Task::PruneCiCaches => prune_ci_caches(),
        Task::PrepareReleasePrConfig { destination } => release_plz::prepare_config(&destination),
        Task::CheckPythonReleaseRequest => python_release::validate_release_request(),
        Task::ReusePythonWheels {
            destination,
            expected_version,
        } => python_wheel_reuse::reuse_release_wheels(&destination, &expected_version),
        Task::DownloadPythonWheels {
            destination,
            expected_version,
            artifact_id,
            sha256,
        } => python_wheel_reuse::download_wheel_artifact(
            &destination,
            &expected_version,
            artifact_id,
            &sha256,
        ),
        Task::CheckPythonWheel {
            path,
            expected_version,
            platform_tag,
        } => python_wheels::check_wheel_metadata(&path, &expected_version, &platform_tag),
        Task::CheckPythonArtifacts {
            directory,
            expected_version,
        } => python_wheels::check_wheel_artifacts(&directory, &expected_version).map(|_| ()),
        Task::StagePythonWheels {
            artifacts,
            expected_version,
            index,
            destination,
        } => {
            python_upload::stage_missing_wheels(&artifacts, &expected_version, &index, &destination)
        }
    }
}

impl CliTask {
    fn run(self) -> error::Result<()> {
        match self {
            Self::Prepare(args) => args.run(),
            Self::Build(args) => args.run(),
            Self::BuildArchive(args) => args.run(),
            Self::CheckBinary(args) => args.run(),
            Self::TestArchive(args) => args.run(),
            Self::TestProcess(args) => args.run(),
            Self::TestScan(args) => args.run(),
            Self::Notices(args) => notices::run(args),
            Self::Artifacts { command } => command.run(),
        }
    }
}

const CACHE_PREFIX: &str = "v0-rust-core-ci-targets-Windows_NT-x64-";
const CACHE_REF: &str = "refs/heads/main";

#[derive(Deserialize)]
struct SccacheStats {
    stats: WriteStats,
}

#[derive(Deserialize)]
struct WriteStats {
    cache_write_errors: usize,
}

#[derive(Deserialize)]
struct CachePage {
    actions_caches: Vec<Cache>,
}

#[derive(Deserialize)]
struct Cache {
    id: u64,
    key: String,
    r#ref: String,
    last_accessed_at: String,
}

fn run(program: &str, args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new(program)
        .args(args)
        .stderr(Stdio::inherit())
        .output()?;
    if !output.status.success() {
        return Err(format!("{program} failed: {}", output.status).into());
    }
    Ok(output.stdout)
}

fn check_writes(stats: &WriteStats, log: &str) -> Result<()> {
    let count = stats.cache_write_errors;
    if count == 0 {
        return Ok(());
    }
    let errors: Vec<_> = log
        .lines()
        .rev()
        .filter(|line| line.contains("Error executing cache write:"))
        .take(count)
        .collect();
    // Concurrent builds can upload the same immutable entry before either sees it.
    if errors.len() != count || errors.iter().any(|line| !line.contains("AlreadyExists")) {
        let startup_errors = log
            .lines()
            .filter(|line| line.contains("storage write check failed:"))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(format!(
            "sccache reported {count} cache write errors\n{startup_errors}\n{}",
            errors.join("\n")
        )
        .into());
    }
    println!("Ignoring {count} cache entries that already exist");
    Ok(())
}

fn check_cache_writes() -> Result<()> {
    let stats: SccacheStats = serde_json::from_slice(&run(
        "sccache",
        &["--show-stats", "--stats-format", "json"],
    )?)?;
    let log =
        match fs::read(env::var_os("SCCACHE_ERROR_LOG").ok_or("SCCACHE_ERROR_LOG is required")?) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
    check_writes(&stats.stats, &String::from_utf8_lossy(&log))
}

fn stale_caches(pages: Vec<CachePage>) -> Vec<Cache> {
    let mut caches: Vec<_> = pages
        .into_iter()
        .flat_map(|page| page.actions_caches)
        .filter(|cache| cache.r#ref == CACHE_REF && cache.key.starts_with(CACHE_PREFIX))
        .collect();
    caches.sort_by(|a, b| b.last_accessed_at.cmp(&a.last_accessed_at));
    caches.into_iter().skip(2).collect()
}

fn prune_ci_caches() -> Result<()> {
    let endpoint = format!("repos/{}/actions/caches", env::var("GH_REPO")?);
    let pages = serde_json::from_slice(&run(
        "gh",
        &[
            "api",
            "--paginate",
            "--slurp",
            &format!("{endpoint}?key={CACHE_PREFIX}&ref={CACHE_REF}&per_page=100"),
        ],
    )?)?;
    for cache in stale_caches(pages) {
        println!("Deleting superseded Windows target cache: {}", cache.key);
        run(
            "gh",
            &[
                "api",
                "--method",
                "DELETE",
                &format!("{endpoint}/{}", cache.id),
            ],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    fn cache(id: u64) -> Cache {
        Cache {
            id,
            key: format!("{CACHE_PREFIX}{id}"),
            r#ref: CACHE_REF.into(),
            last_accessed_at: format!("2026-10-{id:02}T00:00:00Z"),
        }
    }

    #[test]
    fn retains_two_recently_used_caches_across_pages_and_preserves_other_families_and_refs() {
        let mut other_family = cache(5);
        other_family.key = "sccache-other".into();
        let mut other_ref = cache(6);
        other_ref.r#ref = "refs/pull/1/merge".into();
        let pages = vec![
            CachePage {
                actions_caches: vec![cache(3), cache(1), other_family],
            },
            CachePage {
                actions_caches: vec![cache(4), cache(2), other_ref],
            },
        ];
        assert_eq!(
            stale_caches(pages)
                .iter()
                .map(|cache| cache.id)
                .collect::<Vec<_>>(),
            [2, 1]
        );
        for count in 0..=2 {
            assert!(
                stale_caches(vec![CachePage {
                    actions_caches: (1..=count).map(cache).collect()
                }])
                .is_empty()
            );
        }
    }

    #[test]
    fn retains_an_old_cache_reused_after_a_dependency_revert() {
        let mut reused = cache(1);
        reused.last_accessed_at = "2026-10-05T00:00:00Z".into();
        let pages = vec![CachePage {
            actions_caches: vec![reused, cache(2), cache(3), cache(4)],
        }];
        assert_eq!(
            stale_caches(pages)
                .iter()
                .map(|cache| cache.id)
                .collect::<Vec<_>>(),
            [3, 2]
        );
    }

    #[test]
    fn accepts_success_and_existing_entries() -> Result<()> {
        check_writes(
            &WriteStats {
                cache_write_errors: 0,
            },
            "",
        )?;
        check_writes(
            &WriteStats {
                cache_write_errors: 1,
            },
            "Error executing cache write: AlreadyExists: immutable entry",
        )
    }

    #[test]
    fn rejects_backend_errors_and_missing_diagnostics() {
        for log in [
            "",
            "Error executing cache write: RateLimited",
            "Error executing cache write: AlreadyExists\nError executing cache write: PermissionDenied",
        ] {
            assert!(
                check_writes(
                    &WriteStats {
                        cache_write_errors: 1
                    },
                    log
                )
                .is_err()
            );
        }
        for stats in [
            r#"{"stats": {}}"#,
            r#"{"stats": {"cache_write_errors": "0"}}"#,
        ] {
            assert!(serde_json::from_str::<SccacheStats>(stats).is_err());
        }
    }

    #[test]
    fn includes_the_reason_sccache_fell_back_to_read_only() {
        let error = check_writes(
            &WriteStats {
                cache_write_errors: 1,
            },
            "storage write check failed: RateLimited\nError executing cache write: Cannot write to read-only storage",
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("storage write check failed: RateLimited")
        );
    }
}
