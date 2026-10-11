mod error;
mod notices;

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
}

#[snafu::report]
fn main() -> Result<()> {
    match Arguments::parse().command {
        Task::Cli {
            command: CliTask::Notices(args),
        } => notices::run(args),
    }
}
