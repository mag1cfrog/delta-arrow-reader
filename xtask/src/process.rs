use std::path::{Path, PathBuf};

use clap::Args;

use crate::error::Result;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(crate) use linux::{
    check_diagnostic, check_error, check_inspection, check_no_child_processes,
    compile_fault_library, copy_directory,
};

#[derive(Args)]
pub(crate) struct TestProcessArgs {
    /// CLI executable to test; the path is resolved before changing directories.
    binary: PathBuf,
    /// Exact version expected from dar --version.
    #[arg(long)]
    expected_version: String,
    /// Source checkout containing the fault library and external-writer fixtures.
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
    #[cfg(target_os = "linux")]
    {
        use snafu::ResultExt;

        use crate::error::FileSnafu;

        let binary = std::fs::canonicalize(binary).context(FileSnafu { path: binary })?;
        let repository =
            std::fs::canonicalize(repository).context(FileSnafu { path: repository })?;
        linux::run(&binary, version, &repository)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (binary, repository);
        crate::error::InvalidInputSnafu {
            message: format!("CLI process tests for dar {version} require Linux"),
        }
        .fail()
    }
}
