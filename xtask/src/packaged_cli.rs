//! Run the same agent workflow against an extracted archive, installed wheel, or image.
//!
//! The caller supplies the installed command and a writable directory visible to it.
//! Each invocation keeps its original streams beside a JSONL ledger, including failed
//! requests. Discovery traversal is separate from the bounded read/repair workflow.

mod authorization;
mod discovery;
mod workflow;

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{Duration, Instant},
};

use clap::Args;
use serde_json::{Value, json};

use crate::{Result, command::status_with_timeout, scan::ipc::IpcTable};

#[derive(Args)]
pub(crate) struct TestAgentWorkflowArgs {
    /// Installed executable, outside the build directory.
    binary: PathBuf,
    #[arg(long)]
    expected_version: String,
    /// New directory to retain fixtures, results, and invocation records.
    #[arg(long)]
    output_directory: PathBuf,
}

impl TestAgentWorkflowArgs {
    pub(crate) fn run(self) -> crate::error::Result<()> {
        (|| -> Result<()> {
            let binary = self.binary.canonicalize()?;
            fs::create_dir(&self.output_directory)?;
            let directory = self.output_directory.canonicalize()?;
            let name = if binary.file_stem().and_then(|name| name.to_str())
                == Some("delta-arrow-reader")
            {
                "delta-arrow-reader"
            } else {
                "dar"
            };
            let command = || {
                let mut command = Command::new(&binary);
                command.env_clear();
                #[cfg(windows)]
                if let Some(root) = std::env::var_os("SystemRoot") {
                    command.env("SystemRoot", root);
                }
                Ok(command)
            };
            check_agent_workflow(&command, &command, &directory, name, &self.expected_version)
        })()
        .map_err(|error| {
            crate::error::InvalidInputSnafu {
                message: format!("Packaged CLI acceptance failed: {error}"),
            }
            .build()
        })
    }
}

struct PackagedCli<'a> {
    command: &'a dyn Fn() -> Result<Command>,
    directory: &'a Path,
    records: PathBuf,
    expected_table: IpcTable,
    executable_name: &'a str,
    version: &'a str,
    calls: usize,
    stdout_bytes: usize,
    stderr_bytes: usize,
    phase: &'static str,
}

/// The command factory owns platform runtime setup and container mounts.
pub(crate) fn check_agent_workflow(
    offline_command: &dyn Fn() -> Result<Command>,
    storage_command: &dyn Fn() -> Result<Command>,
    directory: &Path,
    executable_name: &str,
    version: &str,
) -> Result<()> {
    let records = match std::env::var_os("DELTA_ARROW_READER_ACCEPTANCE_RECORDS") {
        Some(root) => {
            fs::create_dir_all(&root)?;
            tempfile::Builder::new()
                .prefix(&format!("{executable_name}-"))
                .tempdir_in(root)?
                .keep()
        }
        None => {
            let records = directory.join("records");
            fs::create_dir(&records)?;
            records
        }
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Container runs may write as root; the host must be able to upload their records.
        fs::set_permissions(&records, fs::Permissions::from_mode(0o755))?;
    }
    eprintln!("Packaged CLI invocation records: {}", records.display());
    let expected_table = crate::agent_fixtures::write_wide_table(&directory.join("table"), None)?;
    fs::write(
        directory.join("query.sql"),
        "SELECT count(*) AS row_count FROM t",
    )?;
    let mut cli = PackagedCli {
        command: offline_command,
        directory,
        records,
        expected_table,
        executable_name,
        version,
        calls: 0,
        stdout_bytes: 0,
        stderr_bytes: 0,
        phase: "discovery",
    };
    cli.check_discovery()?;
    cli.phase = "workflow";
    cli.check_read_and_repair()?;
    cli.phase = "boundaries";
    cli.check_output_boundaries()?;
    cli.command = storage_command;
    cli.phase = "authorization";
    cli.check_authorization()?;
    for entry in fs::read_dir(directory)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        assert!(
            !name.starts_with(".dar-result-") && !name.starts_with(".dar-manifest-"),
            "unpublished artifact remains: {name}"
        );
    }
    println!(
        "Packaged agent checks passed: {executable_name} {version}, {}/{}, {} invocations, stdout={} bytes, stderr={} bytes; records: {}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        cli.calls,
        cli.stdout_bytes,
        cli.stderr_bytes,
        cli.records.display(),
    );
    Ok(())
}

impl PackagedCli<'_> {
    fn run_and_record(
        &mut self,
        arguments: &[&str],
        timeout: Duration,
        stdout_limit: usize,
    ) -> Result<Output> {
        let mut command = (self.command)()?;
        command
            .current_dir(self.directory)
            .stdin(Stdio::null())
            .args(["--diagnostics=json"])
            .args(arguments);
        self.calls += 1;
        let stem = format!("{:04}", self.calls);
        let stdout_path = self.records.join(format!("{stem}.stdout"));
        let stderr_path = self.records.join(format!("{stem}.stderr"));
        command
            .stdout(File::create(&stdout_path)?)
            .stderr(File::create(&stderr_path)?);
        let started = Instant::now();
        let status = status_with_timeout(&mut command, timeout);
        let stdout = fs::read(stdout_path)?;
        let stderr = fs::read(stderr_path)?;
        self.stdout_bytes += stdout.len();
        self.stderr_bytes += stderr.len();
        writeln!(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.records.join("invocations.jsonl"))?,
            "{}",
            json!({"step": stem, "phase": self.phase, "executable": self.executable_name,
                "arguments": arguments, "exit": status.as_ref().ok().and_then(|status| status.code()),
                "runner_error": status.as_ref().err().map(ToString::to_string),
                "stdout_bytes": stdout.len(), "stderr_bytes": stderr.len(),
                "stdout_limit": stdout_limit, "elapsed_ms": started.elapsed().as_millis(),
                "cumulative_stdout_bytes": self.stdout_bytes, "cumulative_stderr_bytes": self.stderr_bytes}),
        )?;
        Ok(Output {
            status: status?,
            stdout,
            stderr,
        })
    }

    fn invoke(&mut self, arguments: &[&str], exit: i32, stdout_limit: usize) -> Result<Output> {
        let output = self.run_and_record(arguments, Duration::from_secs(45), stdout_limit)?;
        assert_eq!(
            output.status.code(),
            Some(exit),
            "{arguments:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stdout.len() <= stdout_limit,
            "{arguments:?}: stdout ceiling"
        );
        assert!(
            output.stderr.len() <= 8192,
            "{arguments:?}: diagnostic ceiling"
        );
        let terminal: Value = serde_json::from_slice(&output.stderr)?;
        assert_eq!(
            output.stderr.iter().filter(|&&byte| byte == b'\n').count(),
            1
        );
        assert_eq!(
            terminal["kind"],
            if exit == 0 {
                "completion"
            } else {
                "diagnostic"
            }
        );
        if exit == 0 {
            assert_eq!(terminal["command"], arguments[0]);
            assert_eq!(terminal["output_bytes"], output.stdout.len().to_string());
        } else {
            assert!(
                output.stdout.is_empty(),
                "failed atomic output: {arguments:?}"
            );
            assert_eq!(terminal["terminal"], true);
        }
        for bytes in [&output.stdout, &output.stderr] {
            assert!(!String::from_utf8_lossy(bytes).contains("credential-canary"));
        }
        Ok(output)
    }

    fn read_json(&mut self, arguments: &[&str], limit: usize) -> Result<Value> {
        Ok(serde_json::from_slice(
            &self.invoke(arguments, 0, limit)?.stdout,
        )?)
    }

    fn check_failure(&mut self, arguments: &[&str], exit: i32, code: &str) -> Result<Value> {
        let output = self.invoke(arguments, exit, 0)?;
        let diagnostic: Value = serde_json::from_slice(&output.stderr)?;
        assert_eq!(diagnostic["code"], code, "{arguments:?}: {diagnostic}");
        assert_eq!(diagnostic["format_version"], 2);
        Ok(diagnostic)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn timed_out_invocation_retains_both_streams_and_the_ledger() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let command = || {
            let mut command = Command::new("sh");
            command.args([
                "-c",
                "printf partial-output; printf partial-error >&2; exec sleep 5",
            ]);
            Ok(command)
        };
        let mut cli = PackagedCli {
            command: &command,
            directory: directory.path(),
            records: directory.path().to_owned(),
            expected_table: IpcTable {
                schema: arrow_schema::Schema::empty().into(),
                batches: vec![],
            },
            executable_name: "dar",
            version: "test",
            calls: 0,
            stdout_bytes: 0,
            stderr_bytes: 0,
            phase: "timeout",
        };
        assert!(
            cli.run_and_record(&["query"], Duration::from_millis(100), 8192)
                .is_err()
        );
        assert_eq!(
            fs::read(directory.path().join("0001.stdout"))?,
            b"partial-output"
        );
        assert_eq!(
            fs::read(directory.path().join("0001.stderr"))?,
            b"partial-error"
        );
        let ledger: Value =
            serde_json::from_slice(&fs::read(directory.path().join("invocations.jsonl"))?)?;
        assert!(
            ledger["runner_error"]
                .as_str()
                .unwrap()
                .contains("timed out")
        );
        assert!(ledger["exit"].is_null());
        assert_eq!(ledger["cumulative_stdout_bytes"], 14);
        assert_eq!(ledger["cumulative_stderr_bytes"], 13);
        Ok(())
    }
}
