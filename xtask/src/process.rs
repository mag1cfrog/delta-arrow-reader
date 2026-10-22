use std::path::{Path, PathBuf};

use clap::Args;
use snafu::ensure;

use crate::error::{InvalidInputSnafu, Result};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(crate) use linux::{
    check_error, check_inspection, check_no_child_processes, compile_fault_library, copy_directory,
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

pub(crate) fn check_diagnostic<'a>(stderr: &'a [u8], phase: &str, code: &str) -> Result<&'a str> {
    let text = std::str::from_utf8(stderr).map_err(|_| {
        InvalidInputSnafu {
            message: "CLI diagnostic was not UTF-8",
        }
        .build()
    })?;
    let mut lines = text.lines();
    let failure = lines.next().unwrap_or_default();
    let correction = lines.next();
    ensure!(
        text.ends_with('\n')
            && lines.next().is_none()
            && (failure.starts_with("ERROR delta_arrow_reader: ")
                || failure.starts_with("ERROR delta_arrow_reader_cli: "))
            && !text.contains("secret")
            && !text.contains('\x1b'),
        InvalidInputSnafu {
            message: format!(
                "expected one redacted failure and optional input correction: {text:?}"
            )
        }
    );
    for field in [
        format!("error_phase=\"{phase}\""),
        format!("error_code=\"{code}\""),
        "outcome=\"failed\"".into(),
    ] {
        ensure!(
            failure.split_whitespace().any(|token| token == field),
            InvalidInputSnafu {
                message: format!("expected {field}: {text}")
            }
        );
    }
    let reason = failure.split_whitespace().find(|token| {
        token.starts_with("error_reason=\"")
            && token.ends_with('"')
            && token.len() > "error_reason=\"\"".len()
    });
    let reason = reason.ok_or_else(|| {
        InvalidInputSnafu {
            message: format!("missing failure reason: {text}"),
        }
        .build()
    })?;
    if let Some(correction) = correction {
        // Core owns the failure; CLI guidance may follow with the same classification.
        ensure!(
            failure.starts_with("ERROR delta_arrow_reader: ")
                && correction.starts_with("ERROR delta_arrow_reader_cli: ")
                && !correction
                    .split_whitespace()
                    .any(|token| token.starts_with("outcome=")),
            InvalidInputSnafu {
                message: format!("unexpected input correction after the failure: {text}")
            }
        );
        for field in [
            format!("error_phase=\"{phase}\""),
            format!("error_code=\"{code}\""),
            reason.to_owned(),
            "event=\"cli.input_correction\"".to_owned(),
        ] {
            ensure!(
                correction.split_whitespace().any(|token| token == field),
                InvalidInputSnafu {
                    message: format!("input correction does not match {field}: {text}")
                }
            );
        }
        for field in ["argument=\"", "expected=\"", "hint=\""] {
            ensure!(
                correction
                    .split_whitespace()
                    .any(|token| token.starts_with(field)),
                InvalidInputSnafu {
                    message: format!("input correction is missing {field}: {text}")
                }
            );
        }
    }
    Ok(text)
}
