//! Blocked inputs and consumers must not extend table commands past their deadline.

use std::{
    fs,
    io::{self, Read, Write},
    path::Path,
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use crate::{
    create_full_pipe, diagnostics::assert_single_terminal_record, run_with_stdout,
    run_with_timeout, wait_for_exit_or_kill,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;
const TABLE_COMMANDS: [&str; 5] = ["inspect", "summary", "scan", "preview", "query"];

fn table_command(name: &str, directory: &Path, table: &Path) -> TestResult<Command> {
    let executable = std::env::var_os("DAR_TEST_RUNTIME_BINARY")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_dar").into());
    let mut cmd = Command::new(executable);
    cmd.env_clear().arg("--diagnostics=json");
    if name == "summary" {
        cmd.args(["inspect", "--view", "summary"]);
    } else {
        cmd.arg(name);
    }
    if name == "query" {
        let sql = directory.join("query.sql");
        fs::write(&sql, "SELECT COUNT(*) AS n FROM t")?;
        cmd.arg("--sql-file").arg(sql);
    }
    cmd.arg(table);
    Ok(cmd)
}

fn local_command(name: &str, directory: &Path) -> TestResult<Command> {
    table_command(name, directory, &Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table"))
}

fn assert_deadline_failure(output: &Output) {
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let error = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(error["reason"], "command_deadline_exceeded");
    assert_eq!(error["recovery"], "adjust_budget");
    assert_eq!(error["retryable"], false);
    let hint = error["hint"].as_str().unwrap();
    for required in [
        "Discard stdout",
        "pinned request",
        "authorization",
        "limits",
    ] {
        assert!(hint.contains(required), "{error}");
    }
    assert!(output.stderr.len() < 1024);
    for unrelated in ["secret", "schema", "plan", "trace", "provider"] {
        assert!(!String::from_utf8_lossy(&output.stderr).contains(unrelated));
    }
}

#[test]
fn deadline_includes_blocked_inputs_and_synchronous_credential_reads() -> TestResult {
    let directory = tempfile::tempdir()?;
    let fifo = directory.path().join("secret-input");
    assert!(Command::new("mkfifo").arg(&fifo).status()?.success());
    for name in TABLE_COMMANDS {
        for option in [
            "--storage-options-file",
            "--env-file",
            "--predicate-file",
            "--execution-options-file",
            "gcs-adc",
            "aws-token",
            "azure-token",
        ] {
            if matches!(name, "inspect" | "summary" | "query")
                && matches!(option, "--predicate-file" | "--execution-options-file")
            {
                continue;
            }
            let mut cmd = if option.starts_with("--") {
                let mut cmd = if option == "--env-file" {
                    table_command(name, directory.path(), Path::new("s3://bucket/table"))?
                } else {
                    local_command(name, directory.path())?
                };
                cmd.arg(option).arg(&fifo);
                cmd
            } else {
                let table = match option {
                    "gcs-adc" => "gs://bucket/table",
                    "aws-token" => "s3://bucket/table",
                    "azure-token" => "az://container/table",
                    _ => unreachable!(),
                };
                let mut cmd = table_command(name, directory.path(), Path::new(table))?;
                match option {
                    "gcs-adc" => {
                        cmd.args(["--auth-source", "env"])
                            .env("GOOGLE_APPLICATION_CREDENTIALS", &fifo);
                    }
                    "aws-token" => {
                        cmd.args(["--auth-source", "aws-web-identity"])
                            .env("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/secret")
                            .env("AWS_WEB_IDENTITY_TOKEN_FILE", &fifo)
                            .env("AWS_REGION", "us-east-1");
                    }
                    "azure-token" => {
                        cmd.args(["--auth-source", "azure-workload-identity"])
                            .env("AZURE_STORAGE_ACCOUNT_NAME", "account")
                            .env("AZURE_CLIENT_ID", "11111111-1111-1111-1111-111111111111")
                            .env("AZURE_TENANT_ID", "22222222-2222-2222-2222-222222222222")
                            .env("AZURE_FEDERATED_TOKEN_FILE", &fifo);
                    }
                    _ => unreachable!(),
                }
                cmd
            };
            let started = Instant::now();
            let output = run_with_timeout(cmd.args(["--deadline-ms", "150"]))?;
            assert!(started.elapsed() < Duration::from_secs(3), "{option}");
            assert_deadline_failure(&output);
            assert!(output.stdout.is_empty());
        }
    }
    Ok(())
}

#[test]
fn query_deadline_includes_sql_and_manifest_reads() -> TestResult {
    let directory = tempfile::tempdir()?;
    let fifo = directory.path().join("secret-input");
    assert!(Command::new("mkfifo").arg(&fifo).status()?.success());
    for option in ["--sql-file", "--tables-file"] {
        let mut cmd = if option == "--sql-file" {
            let mut cmd = Command::new(env!("CARGO_BIN_EXE_dar"));
            cmd.args(["query", "unused-table"]);
            cmd
        } else {
            fs::write(directory.path().join("query.sql"), "SELECT 1")?;
            let mut cmd = Command::new(env!("CARGO_BIN_EXE_dar"));
            cmd.args(["query", "--sql-file"])
                .arg(directory.path().join("query.sql"));
            cmd
        };
        let started = Instant::now();
        let output = run_with_timeout(cmd.arg(option).arg(&fifo).args([
            "--diagnostics=json",
            "--deadline-ms",
            "150",
        ]))?;
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(output.stdout.is_empty());
        assert_deadline_failure(&output);
    }
    Ok(())
}

#[test]
fn signals_interrupt_synchronous_input_reads() -> TestResult {
    use std::os::unix::fs::OpenOptionsExt;

    let directory = tempfile::tempdir()?;
    let fifo = directory.path().join("secret-options");
    assert!(Command::new("mkfifo").arg(&fifo).status()?.success());
    for name in TABLE_COMMANDS {
        for (signal, status) in [(libc::SIGINT, 130), (libc::SIGTERM, 143)] {
            let mut child = local_command(name, directory.path())?
                .arg("--storage-options-file")
                .arg(&fifo)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            let started = Instant::now();
            let _writer = loop {
                match fs::OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(&fifo)
                {
                    Ok(writer) => break writer,
                    Err(error) if error.raw_os_error() == Some(libc::ENXIO) => {}
                    Err(error) => return Err(error.into()),
                }
                if started.elapsed() > Duration::from_secs(3) {
                    child.kill()?;
                    child.wait()?;
                    return Err(format!("{name} never opened the input").into());
                }
                thread::sleep(Duration::from_millis(5));
            };
            // SAFETY: this is the live CLI process whose input reader is now blocked.
            assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
            assert_eq!(
                wait_for_exit_or_kill(&mut child, Duration::from_secs(3))?.code(),
                Some(status)
            );
            let output = child.wait_with_output()?;
            assert!(output.stdout.is_empty());
            let record = assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(record["code"], "command_cancelled");
            assert!(
                record["hint"]
                    .as_str()
                    .unwrap()
                    .contains("Retry only if intended")
            );
        }
    }
    Ok(())
}

#[test]
fn deadlines_close_stalled_metadata_and_parquet_reads() -> TestResult {
    use crate::mock_cloud_storage::{
        CloudStorageProtocol, MockCloudStorageServer, partitioned_objects,
    };
    use std::sync::mpsc;

    let directory = tempfile::tempdir()?;
    for (name, file_output) in TABLE_COMMANDS
        .into_iter()
        .map(|name| (name, false))
        .chain([("scan", true), ("query", true)])
    {
        for parquet in [false, true] {
            if parquet && matches!(name, "inspect" | "summary") {
                continue;
            }
            let (reached, observed) = mpsc::channel();
            let (release, held) = mpsc::channel::<()>();
            let mut stalled = false;
            let server = MockCloudStorageServer::with_objects(
                CloudStorageProtocol::S3,
                partitioned_objects()?,
                move |request, _| {
                    if !stalled && request.contains(".parquet") == parquet {
                        stalled = true;
                        let _ = reached.send(());
                        let _ = held.recv_timeout(Duration::from_secs(5));
                    }
                },
            )?;
            let options = directory.path().join("connection.json");
            fs::write(
                &options,
                serde_json::to_vec(&serde_json::json!({
                    "aws_endpoint": server.endpoint, "aws_allow_http": "true",
                    "aws_access_key_id": "test-key", "aws_secret_access_key": "test-secret", "aws_region": "us-east-1"
                }))?,
            )?;
            let mut command =
                table_command(name, directory.path(), Path::new("s3://bucket/table"))?;
            fs::write(directory.path().join("query.sql"), "SELECT id FROM t")?;
            let destination = directory.path().join("saved.arrow");
            if file_output {
                command.arg("--output").arg(&destination);
            }
            let mut child = command
                .arg("--storage-options-file")
                .arg(options)
                .args(["--deadline-ms", "1000", "--table-version", "0"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            observed.recv_timeout(Duration::from_secs(3))?;
            assert_eq!(
                wait_for_exit_or_kill(&mut child, Duration::from_secs(3))?.code(),
                Some(1)
            );
            let output = child.wait_with_output()?;
            drop(release);
            assert_deadline_failure(&output);
            if file_output {
                assert!(output.stdout.is_empty());
                assert!(!destination.exists());
                assert!(!directory.path().join("saved.arrow.manifest.json").exists());
                assert!(fs::read_dir(directory.path())?.all(|entry| {
                    !entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".dar-")
                }));
            }
            assert!(!output.stdout.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
            if matches!(name, "inspect" | "summary" | "preview") {
                assert!(output.stdout.is_empty());
            }
        }
    }
    Ok(())
}

#[test]
fn blocked_stdout_and_stderr_remain_within_deadline_and_cleanup_grace() -> TestResult {
    let directory = tempfile::tempdir()?;
    for name in TABLE_COMMANDS {
        let (_reader, writer) = create_full_pipe()?;
        let started = Instant::now();
        let output = run_with_stdout(
            local_command(name, directory.path())?.args(["--deadline-ms", "200"]),
            Stdio::from(writer),
        )?;
        assert!(started.elapsed() < Duration::from_secs(3));
        assert_deadline_failure(&output);
        for invalid_logging in [false, true] {
            let (_reader, writer) = create_full_pipe()?;
            let mut cmd = if invalid_logging {
                let machine_command = local_command(name, directory.path())?;
                let mut cmd = Command::new(env!("CARGO_BIN_EXE_dar"));
                cmd.env_clear()
                    .args(machine_command.get_args().skip(1))
                    .env("RUST_LOG", "secret=invalid");
                cmd
            } else {
                local_command(name, directory.path())?
            };
            let mut child = cmd
                .args(["--deadline-ms", "200"])
                .stdout(Stdio::null())
                .stderr(Stdio::from(writer))
                .spawn()?;
            assert_eq!(
                wait_for_exit_or_kill(&mut child, Duration::from_secs(3))?.code(),
                Some(1)
            );
        }
        let (reader, writer) = io::pipe()?;
        drop(reader);
        let output = run_with_stdout(
            local_command(name, directory.path())?.args(["--deadline-ms", "2000"]),
            Stdio::from(writer),
        )?;
        assert_eq!(output.status.code(), Some(3));
        let error = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(error["source_kind"], "broken_pipe");
        assert!(
            error["hint"]
                .as_str()
                .unwrap()
                .contains("Restore the output consumer")
        );
    }
    Ok(())
}

#[test]
fn blocked_completion_can_be_cancelled_without_a_success_record() -> TestResult {
    let directory = tempfile::tempdir()?;
    for name in TABLE_COMMANDS {
        for (signal, status) in [(libc::SIGINT, 130), (libc::SIGTERM, 143)] {
            let (mut reader, writer) = create_full_pipe()?;
            let stdout = tempfile::tempfile()?;
            let mut child = local_command(name, directory.path())?
                .args(["--deadline-ms", "5000"])
                .stdout(stdout.try_clone()?)
                .stderr(Stdio::from(writer))
                .spawn()?;
            let started = Instant::now();
            while stdout.metadata()?.len() == 0 {
                if started.elapsed() > Duration::from_secs(3) {
                    child.kill()?;
                    child.wait()?;
                    return Err(format!("{name} never started stdout").into());
                }
                thread::sleep(Duration::from_millis(10));
            }
            // SAFETY: this process was started above and has not been reaped.
            assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
            assert_eq!(
                wait_for_exit_or_kill(&mut child, Duration::from_secs(3))?.code(),
                Some(status)
            );
            let mut stderr = Vec::new();
            reader.read_to_end(&mut stderr)?;
            assert!(
                stderr.iter().all(|byte| *byte == 0),
                "queued completion after cancellation"
            );
        }
    }
    Ok(())
}

fn wait_for_pipe_state(mut ready: impl FnMut() -> io::Result<bool>) -> io::Result<()> {
    let started = Instant::now();
    while !ready()? {
        if started.elapsed() > Duration::from_secs(3) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "pipe state not reached",
            ));
        }
        thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

fn thread_is_blocked_on_pipe(pid: u32, name: &str) -> io::Result<bool> {
    for task in fs::read_dir(format!("/proc/{pid}/task"))? {
        let task = task?.path();
        // Threads can exit between enumerating the directory and reading their state.
        if fs::read_to_string(task.join("comm")).is_ok_and(|comm| comm.trim() == name)
            && fs::read_to_string(task.join("wchan")).is_ok_and(|wait| wait.contains("pipe"))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[test]
fn draining_stdout_after_cancellation_does_not_finish_an_arrow_stream() -> TestResult {
    use std::os::fd::AsRawFd;

    let directory = tempfile::tempdir()?;
    let normal = run_with_timeout(local_command("scan", directory.path())?.args(["--limit", "0"]))?;
    let end_marker = [255, 255, 255, 255, 0, 0, 0, 0];
    assert!(normal.status.success());
    assert!(normal.stdout.ends_with(&end_marker));
    let header_bytes = normal.stdout.len() - end_marker.len();
    for header_already_written in [false, true] {
        for (signal, expected_exit) in [
            (Some(libc::SIGINT), 130),
            (Some(libc::SIGTERM), 143),
            (None, 1),
        ] {
            let (mut reader, mut writer) = io::pipe()?;
            // SAFETY: fcntl only reads this live pipe's capacity.
            let capacity = unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_GETPIPE_SZ) };
            assert!(capacity as usize > header_bytes);
            let prefilled = capacity as usize
                - if header_already_written {
                    header_bytes
                } else {
                    0
                };
            writer.write_all(&vec![0; prefilled])?;
            let (mut stderr, stderr_writer) = create_full_pipe()?;
            let mut child = local_command("scan", directory.path())?
                .args([
                    "--limit",
                    "0",
                    "--deadline-ms",
                    if signal.is_some() { "5000" } else { "1000" },
                ])
                .stdout(Stdio::from(writer))
                .stderr(Stdio::from(stderr_writer))
                .spawn()?;
            let pid = child.id();
            let setup = (|| -> io::Result<()> {
                wait_for_pipe_state(|| {
                    if !header_already_written {
                        return thread_is_blocked_on_pipe(pid, "dar-output");
                    }
                    let mut available = 0;
                    // SAFETY: ioctl writes an integer byte count for this live pipe.
                    if unsafe { libc::ioctl(reader.as_raw_fd(), libc::FIONREAD, &mut available) }
                        < 0
                    {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(available == capacity)
                })?;
                if let Some(signal) = signal {
                    // SAFETY: this is the unreaped child started above.
                    if unsafe { libc::kill(pid as i32, signal) } < 0 {
                        return Err(io::Error::last_os_error());
                    }
                }
                // Failure reporting proves cancellation and cleanup finished before draining.
                wait_for_pipe_state(|| thread_is_blocked_on_pipe(pid, "dar-diagnostic"))
            })();
            if setup.is_err() {
                let _ = child.kill();
                let _ = child.wait();
            }
            setup?;
            let stdout = thread::spawn(move || {
                let mut bytes = Vec::new();
                reader.read_to_end(&mut bytes).map(|_| bytes)
            });
            let status = wait_for_exit_or_kill(&mut child, Duration::from_secs(3));
            if status.is_err() {
                let _ = child.kill();
                let _ = child.wait();
            }
            let stdout = stdout.join().map_err(|_| "stdout reader panicked")??;
            assert_eq!(status?.code(), Some(expected_exit));
            assert!(
                !stdout[prefilled..].ends_with(&end_marker),
                "completed IPC after cancellation: header={header_already_written}, signal={signal:?}"
            );
            let mut stderr_bytes = Vec::new();
            stderr.read_to_end(&mut stderr_bytes)?;
            assert!(
                stderr_bytes.iter().all(|byte| *byte == 0),
                "queued a terminal record"
            );
        }
    }
    Ok(())
}
