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
mod repository;
mod scan;

use clap::{Parser, Subcommand};
use error::Result;

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
}

#[derive(Subcommand)]
enum CliTask {
    /// Validate the source checkout and resolve its installed build toolchain.
    Prepare(build::PrepareArgs),
    /// Build and check one CLI architecture inside Debian 12.
    Build(build::BuildArgs),
    /// Build, check, collect notices, and package one CLI architecture.
    BuildArchive(build::BuildArchiveArgs),
    /// Inspect and execute a binary on its supported Debian 12 platform.
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

#[snafu::report]
fn main() -> Result<()> {
    match Arguments::parse().command {
        Task::Cli {
            command: CliTask::Prepare(args),
        } => args.run(),
        Task::Cli {
            command: CliTask::Build(args),
        } => args.run(),
        Task::Cli {
            command: CliTask::BuildArchive(args),
        } => args.run(),
        Task::Cli {
            command: CliTask::CheckBinary(args),
        } => args.run(),
        Task::Cli {
            command: CliTask::TestArchive(args),
        } => args.run(),
        Task::Cli {
            command: CliTask::TestProcess(args),
        } => args.run(),
        Task::Cli {
            command: CliTask::TestScan(args),
        } => args.run(),
        Task::Cli {
            command: CliTask::Notices(args),
        } => notices::run(args),
        Task::Cli {
            command: CliTask::Artifacts { command },
        } => command.run(),
    }
}
