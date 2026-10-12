mod acceptance;
mod artifacts;
mod binary;
mod build;
mod command;
mod error;
#[cfg(target_os = "linux")]
mod http_fixture;
mod image;
mod image_artifacts;
mod image_runtime;
mod notices;
mod platform;
mod process;
mod python_cli;
mod python_cli_audit;
mod python_release;
mod python_sdist;
mod python_upload;
mod python_wheel_build;
mod python_wheel_reuse;
mod python_wheels;
mod release_assets;
mod release_images;
mod release_notes;
mod release_plan;
mod release_plz;
mod release_request;
mod release_selection;
mod repository;
mod scan;
mod validation_record;

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
    /// Summarize sccache read failures without printing request details.
    ReportCacheReads,
    /// Keep the two most recently used core CI target caches per platform.
    PruneCiCaches,
    /// Prepare release-plz configuration for the current shared release baseline.
    PrepareReleasePrConfig { destination: PathBuf },
    /// Validate a Python release event against the checked-out commit.
    CheckPythonReleaseRequest,
    /// Build and stage the native executable for Python wheel packaging.
    StagePythonCli(python_cli::StagePythonCliArgs),
    /// Prepare the native CLI and notices, then assemble the combined Python wheel.
    BuildPythonWheel(python_wheel_build::BuildPythonWheelArgs),
    /// Include the native build tools and original workspace in a Maturin sdist.
    CompletePythonSdist {
        source: PathBuf,
        destination: PathBuf,
    },
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
    /// Validate one Python wheel's metadata, installation layout, and file hashes.
    CheckPythonWheel {
        path: PathBuf,
        #[arg(long)]
        expected_version: String,
        #[arg(long)]
        platform_tag: String,
    },
    /// Audit an installed wheel CLI's native dependencies and platform baseline.
    AuditPythonCli(python_cli_audit::AuditPythonCliArgs),
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
    /// Build a local container image from the complete validated binary artifact set.
    BuildImage(image::BuildImageArgs),
    /// Inspect and execute a binary on a compatible Linux runtime.
    CheckBinary(binary::CheckBinaryArgs),
    /// Run runtime acceptance tests against an extracted CLI archive.
    TestArchive(acceptance::TestArchiveArgs),
    /// Check a local container's read-only execution, TLS, IPC, and process lifecycle.
    TestImage(image_runtime::TestImageArgs),
    /// Record an approved release PR's validated binary and image artifact identities.
    RecordValidation(validation_record::RecordValidationArgs),
    /// Select release PR artifacts using GitHub provenance, without publishing.
    SelectReleaseArtifacts(release_selection::SelectReleaseArtifactsArgs),
    /// Download and verify release artifacts and write a local publication plan.
    PlanRelease(release_plan::PlanReleaseArgs),
    /// Validate the Actions release event and select its original validation record.
    CheckReleaseRequest,
    /// Upload or reuse the exact GitHub release assets from a verified publication plan.
    UploadReleaseAssets(release_assets::UploadReleaseAssetsArgs),
    /// Require all four GitHub assets and verify their downloaded bytes against the plan.
    VerifyReleaseAssets(release_assets::VerifyReleaseAssetsArgs),
    /// Publish or reuse the verified CLI image platforms and version index on GHCR.
    UploadReleaseImages(release_images::UploadReleaseImagesArgs),
    /// Download published CLI images anonymously and verify their binary contents.
    VerifyReleaseImages(release_images::VerifyReleaseImagesArgs),
    /// Update only the CLI release section and record the selected validation identity.
    UpdateReleaseNotes(release_notes::UpdateReleaseNotesArgs),
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
    /// Export, assemble, or verify CLI images for publication without rebuilding.
    Images {
        #[command(subcommand)]
        command: image_artifacts::ImageArtifactTask,
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
        Task::ReportCacheReads => report_cache_reads(),
        Task::PruneCiCaches => prune_ci_caches(),
        Task::PrepareReleasePrConfig { destination } => release_plz::prepare_config(&destination),
        Task::CheckPythonReleaseRequest => python_release::validate_release_request(),
        Task::StagePythonCli(args) => args.run().map_err(Into::into),
        Task::BuildPythonWheel(args) => args.run().map_err(Into::into),
        Task::CompletePythonSdist {
            source,
            destination,
        } => python_sdist::complete(&source, &destination).map_err(Into::into),
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
        } => python_wheels::check_wheel(&path, &expected_version, &platform_tag),
        Task::AuditPythonCli(args) => args.run().map_err(Into::into),
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
            Self::BuildImage(args) => args.run(),
            Self::CheckBinary(args) => args.run(),
            Self::TestArchive(args) => args.run(),
            Self::TestImage(args) => args.run(),
            Self::RecordValidation(args) => args.run(),
            Self::SelectReleaseArtifacts(args) => args.run(),
            Self::PlanRelease(args) => args.run(),
            Self::CheckReleaseRequest => release_request::check_release_request(),
            Self::UploadReleaseAssets(args) => args.run(),
            Self::VerifyReleaseAssets(args) => args.run(),
            Self::UploadReleaseImages(args) => args.run(),
            Self::VerifyReleaseImages(args) => args.run(),
            Self::UpdateReleaseNotes(args) => args.run(),
            Self::TestProcess(args) => args.run(),
            Self::TestScan(args) => args.run(),
            Self::Notices(args) => notices::run(args),
            Self::Artifacts { command } => command.run(),
            Self::Images { command } => command.run(),
        }
    }
}

const TARGET_CACHE_PREFIXES: [&str; 3] = [
    "v0-rust-core-ci-targets-Linux-x64-",
    "v0-rust-core-ci-targets-Darwin-arm64-",
    "v0-rust-core-ci-targets-Windows_NT-x64-",
];
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

fn report_cache_reads() -> Result<()> {
    let log = fs::read(env::var_os("SCCACHE_ERROR_LOG").ok_or("SCCACHE_ERROR_LOG is required")?)?;
    let mut failures = 0;
    let mut rate_limits = 0;
    // shortcut: sccache 0.18 counts these failures as misses; update this parser if its log format changes.
    for line in String::from_utf8_lossy(&log).lines() {
        let Some((_, error)) = line.split_once("sccache::cache::cache] Got unexpected error: ")
        else {
            continue;
        };
        failures += 1;
        if error.split_whitespace().next() == Some("RateLimited") {
            rate_limits += 1;
        }
    }
    println!(
        "Remote cache read failures recorded: {failures}\n  RateLimited: {rate_limits}\n  Other: {}",
        failures - rate_limits
    );
    Ok(())
}

fn stale_caches(pages: Vec<CachePage>, prefix: &str) -> Vec<Cache> {
    let mut caches: Vec<_> = pages
        .into_iter()
        .flat_map(|page| page.actions_caches)
        .filter(|cache| cache.r#ref == CACHE_REF && cache.key.starts_with(prefix))
        .collect();
    caches.sort_by(|a, b| b.last_accessed_at.cmp(&a.last_accessed_at));
    caches.into_iter().skip(2).collect()
}

fn prune_ci_caches() -> Result<()> {
    let endpoint = format!("repos/{}/actions/caches", env::var("GH_REPO")?);
    for prefix in TARGET_CACHE_PREFIXES {
        let pages = serde_json::from_slice(&run(
            "gh",
            &[
                "api",
                "--paginate",
                "--slurp",
                &format!("{endpoint}?key={prefix}&ref={CACHE_REF}&per_page=100"),
            ],
        )?)?;
        for cache in stale_caches(pages, prefix) {
            println!("Deleting superseded CI target cache: {}", cache.key);
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
    }
    Ok(())
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    const WINDOWS_CACHE_PREFIX: &str = "v0-rust-core-ci-targets-Windows_NT-x64-";

    fn cache(id: u64) -> Cache {
        Cache {
            id,
            key: format!("{WINDOWS_CACHE_PREFIX}{id}"),
            r#ref: CACHE_REF.into(),
            last_accessed_at: format!("2026-10-{id:02}T00:00:00Z"),
        }
    }

    #[test]
    fn retains_two_recently_used_caches_across_pages_and_preserves_other_families_and_refs() {
        let mut other_family = cache(5);
        other_family.key = "sccache-other".into();
        let other_platform = Cache {
            key: "v0-rust-core-ci-targets-Linux-x64-7".into(),
            ..cache(7)
        };
        let mut other_ref = cache(6);
        other_ref.r#ref = "refs/pull/1/merge".into();
        let pages = vec![
            CachePage {
                actions_caches: vec![cache(3), cache(1), other_family],
            },
            CachePage {
                actions_caches: vec![cache(4), cache(2), other_ref, other_platform],
            },
        ];
        assert_eq!(
            stale_caches(pages, WINDOWS_CACHE_PREFIX)
                .iter()
                .map(|cache| cache.id)
                .collect::<Vec<_>>(),
            [2, 1]
        );
        for count in 0..=2 {
            assert!(
                stale_caches(
                    vec![CachePage {
                        actions_caches: (1..=count).map(cache).collect()
                    }],
                    WINDOWS_CACHE_PREFIX
                )
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
            stale_caches(pages, WINDOWS_CACHE_PREFIX)
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
