use std::{
    fs::{self, File},
    io::{self, PipeReader, Read, Seek},
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use arrow_ipc::reader::StreamReader;
use arrow_schema::SchemaRef;
use rustix::{
    event::{PollFd, PollFlags, Timespec, poll},
    pipe::fcntl_setpipe_size,
};
use snafu::{ResultExt, ensure};

use crate::{
    command::{spawn_on_single_cpu, wait_with_timeout},
    error::{ArrowSnafu, FileSnafu, InvalidInputSnafu, IoSnafu, Result},
};

pub(super) struct ScanProcess {
    pub child: Child,
    pub stdout: Option<PipeReader>,
    stderr: File,
}

impl ScanProcess {
    pub fn start(mut command: Command) -> Result<Self> {
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

    pub fn read_schema(&mut self, timeout: Duration) -> Result<SchemaRef> {
        let pipe = self.stdout.as_mut().ok_or_else(|| {
            InvalidInputSnafu {
                message: "scan stdout is already closed",
            }
            .build()
        })?;
        let reader = PipeReaderWithDeadline {
            pipe,
            deadline: Instant::now() + timeout,
        };
        Ok(StreamReader::try_new(reader, None)
            .context(ArrowSnafu {
                path: "scan stdout",
            })?
            .schema())
    }

    pub fn wait_for_blocked_output(&mut self, timeout: Duration) -> Result<()> {
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

    pub fn wait_for_exit(&mut self, timeout: Duration) -> Result<(ExitStatus, Vec<u8>)> {
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

struct PipeReaderWithDeadline<'a> {
    pipe: &'a mut PipeReader,
    deadline: Instant,
}

impl Read for PipeReaderWithDeadline<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            let remaining = self
                .deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::TimedOut, "timed out reading scan stdout")
                })?;
            let timeout = Timespec::try_from(remaining)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            let mut descriptors = [PollFd::new(&*self.pipe, PollFlags::IN)];
            match poll(&mut descriptors, Some(&timeout)) {
                Ok(0) | Err(rustix::io::Errno::INTR) => continue,
                Ok(_) => return self.pipe.read(buffer),
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests;
