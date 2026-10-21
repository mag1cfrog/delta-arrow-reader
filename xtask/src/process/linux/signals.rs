use std::{fs, io, os::unix::process::ExitStatusExt, process::Child, time::Duration};

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
        for action in ["inspect", "scan"] {
            for signal in [Signal::INT, Signal::TERM, Signal::KILL] {
                let server = HttpTestServer::start(|_| Ok(None))?;
                let options = self.write_input_file(br#"{"allow_http":"true"}"#)?;
                let stdout = tempfile::tempfile().context(FileSnafu {
                    path: std::env::temp_dir(),
                })?;
                let stderr = tempfile::tempfile().context(FileSnafu {
                    path: std::env::temp_dir(),
                })?;
                let mut process = KillOnDrop {
                    child: self
                        .command()
                        .arg(action)
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
                kill_process(Pid::from_child(&process.child), signal)
                    .map_err(io::Error::from)
                    .context(IoSnafu {
                        operation: "signal the CLI during blocked table loading",
                    })?;
                let status = wait_with_timeout(&mut process.child, Duration::from_secs(10))
                    .context(CommandStartSnafu {
                        program: self.binary.display().to_string(),
                    })?;
                ensure!(
                    status.signal() == Some(signal.as_raw()),
                    InvalidInputSnafu {
                        message: format!("{action} did not terminate from {signal:?}: {status}")
                    }
                );
                for (name, stream) in [("stdout", stdout), ("stderr", stderr)] {
                    ensure!(
                        stream
                            .metadata()
                            .context(IoSnafu {
                                operation: "inspect signal test output"
                            })?
                            .len()
                            == 0,
                        InvalidInputSnafu {
                            message: format!(
                                "{action} wrote to {name} while terminated by {signal:?}"
                            )
                        }
                    );
                }
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
