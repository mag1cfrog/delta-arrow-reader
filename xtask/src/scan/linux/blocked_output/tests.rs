use rustix::event::{PollFd, PollFlags, Timespec, poll};

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
