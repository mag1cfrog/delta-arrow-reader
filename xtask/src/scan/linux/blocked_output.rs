use std::{
    fs::{self, File},
    io::{self, PipeReader, Read, Seek},
    os::unix::process::ExitStatusExt,
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use rustix::{
    pipe::fcntl_setpipe_size,
    process::{Pid, Signal, kill_process},
};
use snafu::{ResultExt, ensure};

use super::{ScanFixture, fixtures::create_repeated_table};
use crate::{
    command::{spawn_on_single_cpu, wait_with_timeout},
    error::{FileSnafu, InvalidInputSnafu, IoSnafu, Result},
    http_fixture::{HttpTestServer, table::HttpTable},
    process::{check_diagnostic, check_no_child_processes},
};

impl ScanFixture<'_> {
    pub(super) fn check_closed_output_pipe(&self) -> Result<()> {
        let (server, mut scan) = self.start_blocked_scan()?;
        drop(scan.stdout.take());
        let (status, stderr) = scan.wait_for_exit(Duration::from_secs(15))?;
        ensure!(
            status.code() == Some(3),
            InvalidInputSnafu {
                message: format!("expected exit 3 after closing stdout, got {status}"),
            }
        );
        check_diagnostic(&stderr, "execution", "output_write")?;
        server.take_requests()?;
        Ok(())
    }

    pub(super) fn check_signals_during_blocked_output(&self) -> Result<()> {
        for signal in [Signal::INT, Signal::TERM] {
            eprintln!("  signal={signal:?}");
            let (server, mut scan) = self.start_blocked_scan()?;
            kill_process(Pid::from_child(&scan.child), signal)
                .map_err(io::Error::from)
                .context(IoSnafu {
                    operation: "signal the CLI while stdout is blocked",
                })?;
            let (status, stderr) = scan.wait_for_exit(Duration::from_secs(15))?;
            ensure!(
                status.signal() == Some(signal.as_raw()) && stderr.is_empty(),
                InvalidInputSnafu {
                    message: format!(
                        "expected termination from {signal:?} without stderr, got {status}; stderr={}",
                        String::from_utf8_lossy(&stderr)
                    ),
                }
            );
            server.take_requests()?;
        }
        Ok(())
    }

    fn start_blocked_scan(&self) -> Result<(HttpTestServer, ScanProcess)> {
        let directory = self.directory.path();
        let table_path = directory.join("repeated-table");
        create_repeated_table(&self.corpus.join("partitioned/table"), &table_path, 32)?;
        let table = HttpTable::from_directory(&table_path)?;
        let server = HttpTestServer::start(move |request| table.respond(request).map(Some))?;
        let options = directory.join("storage.json");
        fs::write(&options, br#"{"allow_http":"true"}"#).context(FileSnafu { path: &options })?;
        let mut command = self.command();
        command
            .arg("--storage-options-file")
            .arg(options)
            .arg(format!("{}/", server.origin));
        let mut scan = ScanProcess::start(command)?;
        scan.wait_for_blocked_output(Duration::from_secs(15))?;
        check_no_child_processes(&scan.child)?;
        Ok((server, scan))
    }
}

struct ScanProcess {
    child: Child,
    stdout: Option<PipeReader>,
    stderr: File,
}

impl ScanProcess {
    fn start(mut command: Command) -> Result<Self> {
        let (reader, writer) = io::pipe().context(IoSnafu {
            operation: "create the scan output pipe",
        })?;
        fcntl_setpipe_size(&reader, 4096)
            .map_err(io::Error::from)
            .context(IoSnafu {
                operation: "set the scan output pipe capacity",
            })?;
        let stderr = tempfile::tempfile().context(FileSnafu {
            path: std::env::temp_dir(),
        })?;
        command
            .stdin(Stdio::null())
            .stdout(writer)
            .stderr(stderr.try_clone().context(IoSnafu {
                operation: "capture scan stderr",
            })?);
        let child = spawn_on_single_cpu(&mut command)?;
        // Owning the command closes its copy of the pipe writer before the caller reads stdout.
        Ok(Self {
            child,
            stdout: Some(reader),
            stderr,
        })
    }

    fn wait_for_blocked_output(&mut self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let tasks = PathBuf::from(format!("/proc/{}/task", self.child.id()));
        loop {
            let status = self.child.try_wait().context(IoSnafu {
                operation: "inspect the scan process status",
            })?;
            ensure!(
                status.is_none(),
                InvalidInputSnafu {
                    message: format!("scan exited before blocking on stdout: {status:?}"),
                }
            );
            for task in fs::read_dir(&tasks).context(FileSnafu { path: &tasks })? {
                let path = task
                    .context(FileSnafu { path: &tasks })?
                    .path()
                    .join("wchan");
                let state = match fs::read_to_string(&path) {
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    result => result.context(FileSnafu { path })?,
                };
                if state.contains("pipe") && state.contains("writ") {
                    return Ok(());
                }
            }
            ensure!(
                Instant::now() < deadline,
                InvalidInputSnafu {
                    message: "scan did not block writing stdout before the deadline",
                }
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> Result<(ExitStatus, Vec<u8>)> {
        let status = wait_with_timeout(&mut self.child, timeout).context(IoSnafu {
            operation: "wait for the scan process to exit",
        })?;
        self.stderr.rewind().context(IoSnafu {
            operation: "rewind scan stderr",
        })?;
        let mut stderr = Vec::new();
        self.stderr.read_to_end(&mut stderr).context(IoSnafu {
            operation: "read scan stderr",
        })?;
        Ok((status, stderr))
    }
}

impl Drop for ScanProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(test)]
mod tests;
