use std::path::{Path, PathBuf};

use clap::Args;

use crate::error::Result;

#[cfg(target_os = "linux")]
mod ipc;
#[cfg(target_os = "linux")]
mod linux;

#[derive(Args)]
pub(crate) struct TestScanArgs {
    /// CLI executable to test; the path is resolved before changing directories.
    binary: PathBuf,
    /// Source checkout containing the scan checks and external-writer fixtures.
    #[arg(long)]
    repository: PathBuf,
}

impl TestScanArgs {
    pub(crate) fn run(self) -> Result<()> {
        run_scan_tests(&self.binary, &self.repository)?;
        println!("CLI scan acceptance passed");
        Ok(())
    }
}

pub(crate) fn run_scan_tests(binary: &Path, repository: &Path) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::{
            process::{Command, Stdio},
            time::Duration,
        };

        use snafu::ResultExt;

        use crate::{command::run_with_timeout, error::FileSnafu};

        let binary = std::fs::canonicalize(binary).context(FileSnafu { path: binary })?;
        let repository =
            std::fs::canonicalize(repository).context(FileSnafu { path: repository })?;
        linux::run(&binary, &repository)?;
        // shortcut: Python runs the remaining scan groups until their Rust migration is complete.
        run_with_timeout(
            Command::new("python3")
                .arg(repository.join("crates/delta-arrow-reader-cli/tests/scan.py"))
                .arg(&binary)
                .stdin(Stdio::null()),
            Duration::from_secs(600),
        )
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (binary, repository);
        crate::error::InvalidInputSnafu {
            message: "CLI scan tests require Linux",
        }
        .fail()
    }
}
