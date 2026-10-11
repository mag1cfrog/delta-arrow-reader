mod artifacts;
mod error;
mod notices;
mod platform;

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
            command: CliTask::Notices(args),
        } => notices::run(args),
        Task::Cli {
            command: CliTask::Artifacts { command },
        } => command.run(),
    }
}
