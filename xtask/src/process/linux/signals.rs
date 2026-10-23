use std::{
    fs,
    io::{self, Read},
    os::unix::process::ExitStatusExt,
    process::Child,
    time::Duration,
};

use rustix::process::{Pid, Signal, kill_process};
use snafu::{ResultExt, ensure};

use super::ProcessFixture;
use crate::{
    command::{KillOnDrop, wait_with_timeout},
    error::{CommandStartSnafu, FileSnafu, InvalidInputSnafu, IoSnafu, Result},
    http_fixture::HttpTestServer,
};

impl ProcessFixture<'_> {
    pub(super) fn check_signals(&self) -> Result<()> {
        for action in ["inspect", "scan", "preview", "query"] {
            for signal in [
                Some(Signal::INT),
                Some(Signal::TERM),
                Some(Signal::KILL),
                None,
            ] {
                let server = HttpTestServer::start(|_| Ok(None))?;
                let options = self.write_input_file(br#"{"allow_http":"true"}"#)?;
                let stdout = tempfile::tempfile().context(FileSnafu {
                    path: std::env::temp_dir(),
                })?;
                let mut stderr = tempfile::tempfile().context(FileSnafu {
                    path: std::env::temp_dir(),
                })?;
                let mut command = self.command();
                command.arg(action).arg("--diagnostics=json");
                if action == "query" {
                    let sql = self.directory.path().join("query.sql");
                    fs::write(&sql, "SELECT * FROM t").context(FileSnafu { path: &sql })?;
                    command.arg("--sql-file").arg(sql);
                }
                if signal.is_none() {
                    command.args(["--deadline-ms", "1000"]);
                }
                let mut process = KillOnDrop {
                    child: command
                        .arg("--storage-options-file")
                        .arg(options)
                        .arg(format!("{}/", server.origin))
                        .stdout(stdout.try_clone().context(IoSnafu {
                            operation: "capture signal test stdout",
                        })?)
                        .stderr(stderr.try_clone().context(IoSnafu {
                            operation: "capture signal test stderr",
                        })?)
                        .spawn()
                        .context(CommandStartSnafu {
                            program: self.binary.display().to_string(),
                        })?,
                };
                let request = server.wait_for_request(Duration::from_secs(10))?;
                ensure!(
                    matches!(request.method.as_str(), "GET" | "HEAD" | "PROPFIND"),
                    InvalidInputSnafu {
                        message: format!("unexpected blocked table request: {request:?}")
                    }
                );
                check_no_child_processes(&process.child)?;
                if let Some(signal) = signal {
                    kill_process(Pid::from_child(&process.child), signal)
                        .map_err(io::Error::from)
                        .context(IoSnafu {
                            operation: "signal the CLI during blocked table loading",
                        })?;
                }
                let status = wait_with_timeout(&mut process.child, Duration::from_secs(3))
                    .context(CommandStartSnafu {
                        program: self.binary.display().to_string(),
                    })?;
                ensure!(
                    match signal {
                        Some(Signal::KILL) => status.signal() == Some(Signal::KILL.as_raw()),
                        Some(signal) => status.code() == Some(128 + signal.as_raw()),
                        None => status.code() == Some(1),
                    },
                    InvalidInputSnafu {
                        message: format!("{action} did not terminate from {signal:?}: {status}")
                    }
                );
                let mut diagnostic = String::new();
                use std::io::{Seek, SeekFrom};
                stderr.seek(SeekFrom::Start(0)).context(IoSnafu {
                    operation: "rewind signal diagnostic",
                })?;
                stderr.read_to_string(&mut diagnostic).context(IoSnafu {
                    operation: "read signal diagnostic",
                })?;
                if signal != Some(Signal::KILL) {
                    let record: serde_json::Value =
                        serde_json::from_str(&diagnostic).map_err(|error| {
                            InvalidInputSnafu {
                                message: format!("invalid cancellation diagnostic: {error}"),
                            }
                            .build()
                        })?;
                    ensure!(
                        record["kind"] == "diagnostic"
                            && record["code"]
                                == if signal.is_some() {
                                    "command_cancelled"
                                } else {
                                    "deadline_exceeded"
                                },
                        InvalidInputSnafu {
                            message: format!(
                                "{action} emitted an unexpected terminal record: {record}"
                            )
                        }
                    );
                } else {
                    ensure!(
                        diagnostic.is_empty(),
                        InvalidInputSnafu {
                            message: "SIGKILL emitted a diagnostic"
                        }
                    );
                }
                ensure!(
                    stdout
                        .metadata()
                        .context(IoSnafu {
                            operation: "inspect signal test output"
                        })?
                        .len()
                        == 0,
                    InvalidInputSnafu {
                        message: format!("{action} wrote to stdout while terminated by {signal:?}")
                    }
                );
                server.wait_for_disconnect(request.connection_id, Duration::from_secs(5))?;
            }
        }
        Ok(())
    }
}

pub(crate) fn check_no_child_processes(child: &Child) -> Result<()> {
    let tasks = std::path::PathBuf::from(format!("/proc/{}/task", child.id()));
    for task in fs::read_dir(&tasks).context(FileSnafu { path: &tasks })? {
        let path = task
            .context(FileSnafu { path: &tasks })?
            .path()
            .join("children");
        let children = fs::read_to_string(&path).context(FileSnafu { path: &path })?;
        ensure!(
            children.trim().is_empty(),
            InvalidInputSnafu {
                message: format!(
                    "CLI task {} spawned child processes: {children}",
                    path.display()
                )
            }
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests;
