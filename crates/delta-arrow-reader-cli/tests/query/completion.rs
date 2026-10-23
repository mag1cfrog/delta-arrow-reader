//! A complete SQL result is not success until its completion record can be delivered.

use std::{
    io::{self, Read},
    path::Path,
    process::{Child, Stdio},
    thread,
    time::{Duration, Instant},
};

use super::*;
use crate::{create_full_pipe, wait_for_exit_or_kill};

fn wait_for_complete_stdout(child: &mut Child, path: &Path, format: &str) -> TestResult<Vec<u8>> {
    let started = Instant::now();
    loop {
        let bytes = fs::read(path)?;
        let complete = if format == "arrow" {
            bytes.ends_with(IPC_END)
        } else {
            serde_json::from_slice::<serde_json::Value>(&bytes).is_ok()
        };
        if complete {
            return Ok(bytes);
        }
        if let Some(status) = child.try_wait()? {
            return Err(format!("query exited before finishing stdout: {status}").into());
        }
        if started.elapsed() > Duration::from_secs(20) {
            child.kill()?;
            child.wait()?;
            return Err("query did not finish stdout".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_count_result(bytes: &[u8], format: &str) -> TestResult {
    let expected = read_expected_rows("partitioned")?.num_rows();
    if format == "arrow" {
        assert!(bytes.ends_with(IPC_END));
        let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>()?;
        let result = concat_batches(&schema, &batches)?;
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            expected as i64
        );
    } else {
        let result: serde_json::Value = serde_json::from_slice(bytes)?;
        assert_eq!(result["rows"], serde_json::json!([[expected.to_string()]]));
    }
    Ok(())
}

#[test]
fn blocked_completion_remains_cancellable_after_stdout_finishes() -> TestResult {
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT COUNT(*) AS n FROM t",
    )?;
    for format in ["arrow", "json"] {
        for (signal, expected_status) in [("-INT", 130), ("-TERM", 143)] {
            let stdout = fixture.directory.path().join("stdout");
            let (mut reader, writer) = create_full_pipe()?;
            let mut child = fixture
                .command()
                .args(["--format", format])
                .stdout(fs::File::create(&stdout)?)
                .stderr(Stdio::from(writer))
                .spawn()?;
            let bytes = wait_for_complete_stdout(&mut child, &stdout, format)?;
            assert!(
                Command::new("kill")
                    .args([signal, &child.id().to_string()])
                    .status()?
                    .success()
            );
            assert_eq!(
                wait_for_exit_or_kill(&mut child, Duration::from_secs(3))?.code(),
                Some(expected_status)
            );
            assert_count_result(&bytes, format)?;
            let mut stderr = Vec::new();
            reader.read_to_end(&mut stderr)?;
            assert!(
                stderr.iter().all(|byte| *byte == 0),
                "completion was queued after cancellation"
            );
        }
    }
    Ok(())
}

#[test]
fn closed_stderr_rejects_an_otherwise_complete_query() -> TestResult {
    let fixture = SqlCommandFixture::new(
        fixture_path("partitioned").join("table"),
        "SELECT COUNT(*) AS n FROM t",
    )?;
    for format in ["arrow", "json"] {
        let (reader, writer) = io::pipe()?;
        drop(reader);
        let stdout = fixture.directory.path().join("stdout");
        let mut child = fixture
            .command()
            .args(["--format", format])
            .stdout(fs::File::create(&stdout)?)
            .stderr(Stdio::from(writer))
            .spawn()?;
        assert_eq!(
            wait_for_exit_or_kill(&mut child, Duration::from_secs(20))?.code(),
            Some(3)
        );
        assert_count_result(&fs::read(stdout)?, format)?;
    }
    Ok(())
}

#[test]
fn partial_multitable_completion_does_not_block_cancellation() -> TestResult {
    use std::os::fd::AsRawFd;

    let directory = tempfile::tempdir()?;
    let tables: Vec<_> = (0..64)
        .map(|index| {
            serde_json::json!({
                "alias": format!("table_{index:057}"),
                "location": fixture_path("partitioned").join("table"),
            })
        })
        .collect();
    fs::write(
        directory.path().join("tables.json"),
        serde_json::to_vec(&serde_json::json!({"format_version": 1, "tables": tables}))?,
    )?;
    fs::write(directory.path().join("query.sql"), "SELECT 1")?;
    let (mut reader, writer) = io::pipe()?;
    // SAFETY: this live pipe is owned by the test; reducing capacity forces a partial record.
    assert_eq!(
        unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_SETPIPE_SZ, 4096) },
        4096
    );
    let stdout = directory.path().join("stdout");
    let mut child = Command::new(env!("CARGO_BIN_EXE_dar"))
        .current_dir(directory.path())
        .args([
            "query",
            "--tables-file",
            "tables.json",
            "--sql-file",
            "query.sql",
            "--diagnostics=json",
        ])
        .stdout(fs::File::create(&stdout)?)
        .stderr(Stdio::from(writer))
        .spawn()?;
    wait_for_complete_stdout(&mut child, &stdout, "arrow")?;
    let started = Instant::now();
    loop {
        let mut available = 0;
        // SAFETY: ioctl writes an integer byte count for this live pipe descriptor.
        assert_eq!(
            unsafe { libc::ioctl(reader.as_raw_fd(), libc::FIONREAD, &mut available) },
            0
        );
        if available > 0 {
            break;
        }
        if started.elapsed() > Duration::from_secs(3) {
            child.kill()?;
            child.wait()?;
            return Err("completion did not start".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()?
            .success()
    );
    assert_eq!(
        wait_for_exit_or_kill(&mut child, Duration::from_secs(3))?.code(),
        Some(143)
    );
    let mut stderr = Vec::new();
    reader.read_to_end(&mut stderr)?;
    assert!(!stderr.is_empty());
    assert!(
        !stderr.contains(&b'\n'),
        "partial completion must not be followed by another record"
    );
    assert!(serde_json::from_slice::<serde_json::Value>(&stderr).is_err());
    Ok(())
}
