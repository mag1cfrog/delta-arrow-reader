use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use clap::Args;
use snafu::{ResultExt, ensure};

use crate::{
    command::run_with_timeout,
    error::{FileSnafu, InvalidInputSnafu, Result},
};

#[cfg(target_os = "linux")]
mod linux;

#[derive(Args)]
pub(crate) struct TestProcessArgs {
    /// CLI executable to test; the path is resolved before changing directories.
    binary: PathBuf,
    /// Exact version expected from dar --version.
    #[arg(long)]
    expected_version: String,
    /// Source checkout containing the process tests, fault library, and fixtures.
    #[arg(long)]
    repository: PathBuf,
}

impl TestProcessArgs {
    pub(crate) fn run(self) -> Result<()> {
        run_process_tests(&self.binary, &self.expected_version, &self.repository)?;
        println!("CLI process acceptance passed");
        Ok(())
    }
}

pub(crate) fn run_process_tests(binary: &Path, version: &str, repository: &Path) -> Result<()> {
    ensure!(
        cfg!(target_os = "linux"),
        InvalidInputSnafu {
            message: format!("CLI process tests for dar {version} require Linux")
        }
    );
    let binary = fs::canonicalize(binary).context(FileSnafu { path: binary })?;
    let repository = fs::canonicalize(repository).context(FileSnafu { path: repository })?;
    #[cfg(target_os = "linux")]
    linux::run(&binary, version, &repository)?;

    let working_directory = tempfile::tempdir().context(FileSnafu {
        path: std::env::temp_dir(),
    })?;
    run_with_timeout(
        Command::new("python3")
            .arg(repository.join("crates/delta-arrow-reader-cli/tests/process.py"))
            .arg(binary)
            .current_dir(working_directory.path())
            .stdin(Stdio::null()),
        Duration::from_secs(600),
    )
}
