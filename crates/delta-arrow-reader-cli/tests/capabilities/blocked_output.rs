//! Discovery must exit when a consumer stops draining stdout or stderr.

use std::{
    io::{self, Read, Write},
    os::fd::AsRawFd,
    os::unix::process::CommandExt,
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use crate::{diagnostics::assert_single_terminal_record, run_with_stdout};

fn filled_pipe() -> io::Result<(io::PipeReader, io::PipeWriter)> {
    let (reader, mut writer) = io::pipe()?;
    // Fill the pipe independently of the size of the next discovery response.
    // SAFETY: fcntl only queries the capacity of this live pipe descriptor.
    let capacity = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETPIPE_SZ) };
    assert!(capacity > 0);
    writer.write_all(&vec![0; capacity as usize])?;
    Ok((reader, writer))
}

fn wait_for_exit(child: &mut Child, timeout: Duration) -> io::Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "discovery did not exit",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_response(child: &mut Child, stdout: &std::fs::File) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(3);
    while stdout.metadata()?.len() == 0 {
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "no discovery response",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

#[test]
fn blocked_stdout_respects_the_deadline() -> Result<(), Box<dyn std::error::Error>> {
    let (reader, writer) = filled_pipe()?;
    let output = run_with_stdout(
        Command::new(env!("CARGO_BIN_EXE_dar")).args(["capabilities", "--diagnostics=json"]),
        Stdio::from(writer),
    )?;
    drop(reader);
    assert_eq!(output.status.code(), Some(1));
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["reason"], "discovery_deadline_exceeded");
    assert_eq!(record["source_kind"], "timeout");
    Ok(())
}

#[test]
fn blocked_stderr_respects_the_deadline_for_completion_and_log_setup() -> io::Result<()> {
    for args in [
        vec!["capabilities", "--diagnostics=json"],
        vec![
            "capabilities",
            "--schema",
            "predicate",
            "--diagnostics=json",
        ],
        vec!["capabilities"],
    ] {
        let (_reader, writer) = filled_pipe()?;
        let mut child = Command::new(env!("CARGO_BIN_EXE_dar"))
            .args(args)
            .env("RUST_LOG", "secret=invalid")
            .stdout(Stdio::null())
            .stderr(Stdio::from(writer))
            .spawn()?;
        let status = wait_for_exit(&mut child, Duration::from_millis(6500))?;
        assert_eq!(status.code(), Some(1));
    }
    Ok(())
}

#[test]
fn a_closed_stdout_keeps_status_three_when_stderr_is_blocked() -> io::Result<()> {
    for machine in [false, true] {
        let (stdout_reader, stdout_writer) = io::pipe()?;
        drop(stdout_reader);
        let (_stderr_reader, stderr_writer) = filled_pipe()?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command.arg("capabilities").env_remove("RUST_LOG");
        if machine {
            command.arg("--diagnostics=json");
        }
        let mut child = command
            .stdout(Stdio::from(stdout_writer))
            .stderr(Stdio::from(stderr_writer))
            .spawn()?;
        assert_eq!(
            wait_for_exit(&mut child, Duration::from_secs(3))?.code(),
            Some(3)
        );
    }
    Ok(())
}

#[test]
fn cancellation_with_blocked_stderr_keeps_the_signal_status() -> io::Result<()> {
    for (signal, expected_status) in [(libc::SIGINT, 130), (libc::SIGTERM, 143)] {
        let (_reader, writer) = filled_pipe()?;
        let stdout = tempfile::tempfile()?;
        let mut child = Command::new(env!("CARGO_BIN_EXE_dar"))
            .args(["capabilities", "--diagnostics=json"])
            .stdout(stdout.try_clone()?)
            .stderr(Stdio::from(writer))
            .spawn()?;
        wait_for_response(&mut child, &stdout)?;
        // SAFETY: this is the live subprocess started above.
        assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        assert_eq!(
            wait_for_exit(&mut child, Duration::from_secs(3))?.code(),
            Some(expected_status)
        );
    }
    Ok(())
}

#[test]
fn startup_and_argument_errors_exit_when_stderr_is_blocked() -> io::Result<()> {
    for runtime_failure in [false, true] {
        for machine in [false, true] {
            let (_reader, writer) = filled_pipe()?;
            let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
            command.arg("capabilities").env_remove("RUST_LOG");
            if machine {
                command.arg("--diagnostics=json");
            }
            if runtime_failure {
                // SAFETY: only the child changes its descriptor limit; setrlimit is signal-safe.
                unsafe {
                    command.pre_exec(|| {
                        let limit = libc::rlimit {
                            rlim_cur: 4,
                            rlim_max: 4,
                        };
                        if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) == 0 {
                            Ok(())
                        } else {
                            Err(io::Error::last_os_error())
                        }
                    });
                }
            } else {
                command.args(["--schema", "unknown"]);
            }
            let mut child = command
                .stdout(Stdio::null())
                .stderr(Stdio::from(writer))
                .spawn()?;
            assert_eq!(
                wait_for_exit(&mut child, Duration::from_secs(3))?.code(),
                Some(if runtime_failure { 1 } else { 2 }),
            );
        }
    }
    Ok(())
}

#[test]
fn draining_stderr_after_failure_never_publishes_completion() -> io::Result<()> {
    for signal in [None, Some(libc::SIGINT), Some(libc::SIGTERM)] {
        let (mut reader, writer) = filled_pipe()?;
        // SAFETY: fcntl queries this live pipe without changing it.
        let capacity = unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_GETPIPE_SZ) };
        assert!(capacity > 0);
        let stdout = tempfile::tempfile()?;
        let mut child = Command::new(env!("CARGO_BIN_EXE_dar"))
            .args(["capabilities", "--diagnostics=json"])
            .stdout(stdout.try_clone()?)
            .stderr(Stdio::from(writer))
            .spawn()?;
        wait_for_response(&mut child, &stdout)?;
        if let Some(signal) = signal {
            // SAFETY: this is the live subprocess started above.
            assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
            thread::sleep(Duration::from_millis(150));
        } else {
            thread::sleep(Duration::from_millis(5200));
        }
        reader.read_exact(&mut vec![0; capacity as usize])?;
        let status = wait_for_exit(&mut child, Duration::from_secs(2))?;
        let mut stderr = Vec::new();
        reader.read_to_end(&mut stderr)?;
        assert_eq!(status.code(), Some(signal.map_or(1, |signal| 128 + signal)));
        let record = assert_single_terminal_record(&stderr, "diagnostic");
        assert_eq!(
            record["reason"],
            if signal.is_some() {
                "command_cancelled"
            } else {
                "discovery_deadline_exceeded"
            }
        );
    }
    Ok(())
}

#[test]
fn draining_stderr_before_deadline_finishes_without_changing_pipe_flags() -> io::Result<()> {
    let (mut reader, writer) = filled_pipe()?;
    let retained_writer = writer.try_clone()?;
    // SAFETY: fcntl only queries these live descriptors.
    let original_flags = unsafe { libc::fcntl(retained_writer.as_raw_fd(), libc::F_GETFL) };
    let capacity = unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_GETPIPE_SZ) };
    let stdout = tempfile::tempfile()?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_dar"))
        .args(["capabilities", "--diagnostics=json"])
        .stdout(stdout.try_clone()?)
        .stderr(Stdio::from(writer))
        .spawn()?;
    wait_for_response(&mut child, &stdout)?;
    thread::sleep(Duration::from_millis(50));
    // SAFETY: the writer stays open for the duration of the query.
    assert_eq!(
        unsafe { libc::fcntl(retained_writer.as_raw_fd(), libc::F_GETFL) },
        original_flags
    );
    reader.read_exact(&mut vec![0; capacity as usize])?;
    assert!(wait_for_exit(&mut child, Duration::from_secs(2))?.success());
    drop(retained_writer);
    let mut stderr = Vec::new();
    reader.read_to_end(&mut stderr)?;
    assert_single_terminal_record(&stderr, "completion");
    Ok(())
}

#[test]
fn closed_stderr_cannot_finish_machine_discovery_successfully() -> io::Result<()> {
    let (reader, writer) = io::pipe()?;
    drop(reader);
    let mut child = Command::new(env!("CARGO_BIN_EXE_dar"))
        .args(["capabilities", "--diagnostics=json"])
        .stdout(Stdio::null())
        .stderr(Stdio::from(writer))
        .spawn()?;
    assert_eq!(
        wait_for_exit(&mut child, Duration::from_secs(2))?.code(),
        Some(3)
    );
    Ok(())
}
