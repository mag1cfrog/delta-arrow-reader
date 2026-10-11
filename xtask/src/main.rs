mod artifacts;
mod binary;
mod build;
mod command;
mod error;
mod notices;
mod platform;
mod repository;

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
            command: CliTask::Notices(args),
        } => notices::run(args),
        Task::Cli {
            command: CliTask::Artifacts { command },
        } => command.run(),
    }
}
