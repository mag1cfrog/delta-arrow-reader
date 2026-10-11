use std::io::Write;

use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use rustix::process::Pid;

use crate::process::check_no_child_processes;

use super::*;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn writing_process() -> Result<ScanProcess> {
    let mut command = Command::new("head");
    command.args(["-c", "1048576", "/dev/zero"]);
    ScanProcess::start(command)
}

#[test]
fn closing_stdout_unblocks_a_writer() -> TestResult {
    let mut process = writing_process()?;
    process.wait_for_blocked_output(Duration::from_secs(5))?;
    check_no_child_processes(&process.child)?;
    drop(process.stdout.take());
    let (status, _) = process.wait_for_exit(Duration::from_secs(5))?;
    assert!(!status.success());
    Ok(())
}

#[test]
fn timeout_kills_and_reaps_a_blocked_writer() -> TestResult {
    let mut process = writing_process()?;
    process.wait_for_blocked_output(Duration::from_secs(5))?;
    let pid = Pid::from_child(&process.child);
    let start = Instant::now();
    let result = process.wait_for_exit(Duration::from_millis(50));
    assert!(
        matches!(result, Err(crate::error::Error::Io { source, .. }) if source.kind() == io::ErrorKind::TimedOut)
    );
    assert!(start.elapsed() < Duration::from_secs(5));
    assert_eq!(
        rustix::process::test_kill_process(pid),
        Err(rustix::io::Errno::SRCH)
    );
    Ok(())
}

#[test]
fn dropping_the_scan_kills_and_reaps_a_blocked_writer() -> TestResult {
    let mut process = writing_process()?;
    process.wait_for_blocked_output(Duration::from_secs(5))?;
    let pid = Pid::from_child(&process.child);
    drop(process);
    assert_eq!(
        rustix::process::test_kill_process(pid),
        Err(rustix::io::Errno::SRCH)
    );
    Ok(())
}

#[test]
fn completed_child_preserves_stderr_and_closes_the_output_pipe() -> TestResult {
    let mut command = Command::new("sh");
    command.args(["-c", "printf stdout; printf stderr >&2"]);
    let mut process = ScanProcess::start(command)?;
    let (status, stderr) = process.wait_for_exit(Duration::from_secs(5))?;
    assert!(status.success());
    assert_eq!(stderr, b"stderr");
    let result = process.wait_for_blocked_output(Duration::from_secs(5));
    assert!(
        matches!(result, Err(crate::error::Error::InvalidInput { message }) if message.contains("exited before blocking"))
    );
    let mut stdout = process.stdout.take().unwrap();
    // Check for closed writers before a blocking read, so a retained descriptor fails instead of hanging.
    let mut descriptors = [PollFd::new(&stdout, PollFlags::IN)];
    assert_eq!(poll(&mut descriptors, Some(&Timespec::default()))?, 1);
    assert!(descriptors[0].revents().contains(PollFlags::HUP));
    let mut bytes = Vec::new();
    stdout.read_to_end(&mut bytes)?;
    assert_eq!(bytes, b"stdout");
    Ok(())
}

#[test]
fn waiting_for_blocked_output_times_out_for_an_idle_child() -> TestResult {
    let mut command = Command::new("sleep");
    command.arg("30");
    let mut process = ScanProcess::start(command)?;
    let start = Instant::now();
    let result = process.wait_for_blocked_output(Duration::from_millis(50));
    assert!(
        matches!(result, Err(crate::error::Error::InvalidInput { message }) if message.contains("before the deadline"))
    );
    assert!(start.elapsed() < Duration::from_secs(5));
    Ok(())
}

#[test]
fn reads_schema_before_the_child_writes_any_batches_or_exits() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("schema.arrow");
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    drop(StreamWriter::try_new(File::create(&path)?, &schema)?);
    let mut command = Command::new("sh");
    command
        .args(["-c", "cat \"$1\"; exec sleep 30", "schema-fixture"])
        .arg(path);
    let mut process = ScanProcess::start(command)?;
    assert_eq!(*process.read_schema(Duration::from_secs(5))?, schema);
    assert!(process.child.try_wait()?.is_none());
    Ok(())
}

#[test]
fn schema_reads_time_out_when_a_writer_stalls() -> TestResult {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    let mut encoded = Vec::new();
    drop(StreamWriter::try_new(&mut encoded, &schema)?);
    for prefix in [&encoded[..0], &encoded[..4], &encoded[..8]] {
        let (mut pipe, mut writer) = io::pipe()?;
        writer.write_all(prefix)?;
        let start = Instant::now();
        let reader = PipeReaderWithDeadline {
            pipe: &mut pipe,
            deadline: start + Duration::from_millis(50),
        };
        let error =
            StreamReader::try_new(reader, None).expect_err("incomplete schema must time out");
        assert!(
            matches!(error, ArrowError::IoError(_, source) if source.kind() == io::ErrorKind::TimedOut)
        );
        assert!(start.elapsed() < Duration::from_secs(5));
    }
    Ok(())
}

#[test]
fn schema_reads_report_early_eof_without_waiting_for_the_deadline() -> TestResult {
    for prefix in [b"".as_slice(), b"\xff\xff\xff\xff"] {
        let (mut pipe, mut writer) = io::pipe()?;
        writer.write_all(prefix)?;
        drop(writer);
        let reader = PipeReaderWithDeadline {
            pipe: &mut pipe,
            deadline: Instant::now() + Duration::from_secs(5),
        };
        let error = StreamReader::try_new(reader, None).expect_err("missing schema must fail");
        assert!(
            !matches!(error, ArrowError::IoError(_, source) if source.kind() == io::ErrorKind::TimedOut)
        );
    }
    Ok(())
}

#[test]
fn pipe_reads_share_one_deadline_even_when_more_bytes_are_ready() -> TestResult {
    let (mut pipe, mut writer) = io::pipe()?;
    writer.write_all(b"abc")?;
    let mut reader = PipeReaderWithDeadline {
        pipe: &mut pipe,
        deadline: Instant::now() + Duration::from_millis(200),
    };
    let mut byte = [0];
    reader.read_exact(&mut byte)?;
    assert_eq!(&byte, b"a");
    thread::sleep(Duration::from_millis(250));
    assert_eq!(
        reader.read(&mut byte).unwrap_err().kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(reader.read(&mut [])?, 0);
    Ok(())
}
