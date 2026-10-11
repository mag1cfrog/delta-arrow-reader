use std::{
    io::{self, Read, Seek},
    process::{Child, Command, ExitStatus, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use snafu::{ResultExt, ensure};

use crate::error::{CommandFailedSnafu, CommandStartSnafu, Result, Utf8Snafu};

pub(crate) fn command_output(command: &mut Command) -> Result<String> {
    let program = command.get_program().to_string_lossy().into_owned();
    let output = command
        .stderr(Stdio::inherit())
        .output()
        .context(CommandStartSnafu { program: &program })?;
    ensure!(
        output.status.success(),
        CommandFailedSnafu {
            program: &program,
            status: output.status
        }
    );
    String::from_utf8(output.stdout).context(Utf8Snafu { subject: program })
}

pub(crate) fn run_command(command: &mut Command) -> Result<()> {
    let program = command.get_program().to_string_lossy().into_owned();
    let status = command
        .stdout(Stdio::from(io::stderr()))
        .stderr(Stdio::inherit())
        .status()
        .context(CommandStartSnafu { program: &program })?;
    ensure!(status.success(), CommandFailedSnafu { program, status });
    Ok(())
}

pub(crate) fn output_with_timeout(command: &mut Command, timeout: Duration) -> Result<Output> {
    capture_with_timeout(command, timeout).context(CommandStartSnafu {
        program: command.get_program().to_string_lossy(),
    })
}

pub(crate) fn run_with_timeout(command: &mut Command, timeout: Duration) -> Result<()> {
    let program = command.get_program().to_string_lossy().into_owned();
    let mut child = command
        .stdout(Stdio::from(io::stderr()))
        .stderr(Stdio::inherit())
        .spawn()
        .context(CommandStartSnafu { program: &program })?;
    let status =
        wait_with_timeout(&mut child, timeout).context(CommandStartSnafu { program: &program })?;
    ensure!(status.success(), CommandFailedSnafu { program, status });
    Ok(())
}

fn capture_with_timeout(command: &mut Command, timeout: Duration) -> io::Result<Output> {
    // Files keep noisy children from filling a pipe while their deadline is checked.
    let mut stdout = tempfile::tempfile()?;
    let mut stderr = tempfile::tempfile()?;
    let mut child = command
        .stdout(stdout.try_clone()?)
        .stderr(stderr.try_clone()?)
        .spawn()?;
    let status = wait_with_timeout(&mut child, timeout)?;
    stdout.rewind()?;
    stderr.rewind()?;
    let mut output = Output {
        status,
        stdout: Vec::new(),
        stderr: Vec::new(),
    };
    stdout.read_to_end(&mut output.stdout)?;
    stderr.read_to_end(&mut output.stderr)?;
    Ok(output)
}

fn wait_with_timeout(child: &mut Child, timeout: Duration) -> io::Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Err(error) => break Err(error),
            Ok(None) if Instant::now() >= deadline => {
                break Err(io::Error::new(io::ErrorKind::TimedOut, "command timed out"));
            }
            Ok(None) => thread::sleep(Duration::from_millis(10)),
        }
    };
    if status.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    status
}

#[cfg(test)]
mod tests;
