#![cfg(unix)]

use super::*;

#[test]
#[cfg(target_os = "linux")]
fn single_cpu_child_preserves_caller_affinity_on_success_and_failure()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use rustix::thread::{CpuSet, sched_getaffinity};

    let original = sched_getaffinity(None)?;
    let selected = (0..CpuSet::MAX_CPU)
        .find(|cpu| original.is_set(*cpu))
        .unwrap();
    let output = output_on_single_cpu(
        Command::new("cat").arg("/proc/self/status"),
        Duration::from_secs(5),
    )?;
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let stdout = String::from_utf8(output.stdout)?;
    let cpus = stdout
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .unwrap();
    assert_eq!(cpus.trim().parse::<usize>()?, selected);
    assert_eq!(sched_getaffinity(None)?, original);

    let temporary = tempfile::tempdir()?;
    let missing = output_on_single_cpu(
        &mut Command::new(temporary.path().join("missing")),
        Duration::from_secs(5),
    );
    assert!(
        matches!(missing, Err(crate::error::Error::CommandStart { source, .. })
        if source.kind() == io::ErrorKind::NotFound)
    );
    assert_eq!(sched_getaffinity(None)?, original);

    let start = Instant::now();
    let timeout = output_on_single_cpu(Command::new("sleep").arg("30"), Duration::from_millis(50));
    assert!(
        matches!(timeout, Err(crate::error::Error::CommandStart { source, .. })
        if source.kind() == io::ErrorKind::TimedOut)
    );
    assert!(start.elapsed() < Duration::from_secs(5));
    assert_eq!(sched_getaffinity(None)?, original);
    Ok(())
}

#[test]
fn streamed_commands_require_a_successful_exit() -> Result<()> {
    run_with_timeout(
        Command::new("sh").args(["-c", "exit 0"]),
        Duration::from_secs(5),
    )?;
    let error = run_with_timeout(
        Command::new("sh").args(["-c", "exit 17"]),
        Duration::from_secs(5),
    )
    .expect_err("nonzero exit must fail");
    assert!(matches!(
        error,
        crate::error::Error::CommandFailed { status, .. } if status.code() == Some(17)
    ));
    Ok(())
}

#[test]
fn captures_both_streams_without_pipe_backpressure() -> Result<()> {
    let output = output_with_timeout(
        Command::new("sh").args([
            "-c",
            "head -c 131072 /dev/zero; head -c 131072 /dev/zero >&2; exit 17",
        ]),
        Duration::from_secs(5),
    )?;
    assert_eq!(output.status.code(), Some(17));
    assert_eq!(output.stdout, vec![0; 131072]);
    assert_eq!(output.stderr, vec![0; 131072]);
    Ok(())
}

#[test]
fn timeout_kills_and_reaps_the_child() -> std::result::Result<(), Box<dyn std::error::Error>> {
    for capture_output in [true, false] {
        let temporary = tempfile::tempdir()?;
        let pid_file = temporary.path().join("pid");
        let mut command = Command::new("sh");
        command
            .args(["-c", "echo $$ > \"$1\"; exec sleep 30", "fixture"])
            .arg(&pid_file);
        let timeout = Duration::from_millis(200);
        let start = Instant::now();
        let result = if capture_output {
            output_with_timeout(&mut command, timeout).map(|_| ())
        } else {
            status_with_timeout(command.stdout(Stdio::null()).stderr(Stdio::null()), timeout)
                .map(|_| ())
        };
        assert!(
            matches!(result, Err(crate::error::Error::CommandStart { source, .. }) if source.kind() == io::ErrorKind::TimedOut)
        );
        assert!(start.elapsed() < Duration::from_secs(5));
        let pid = std::fs::read_to_string(pid_file)?;
        assert!(
            !Command::new("sh")
                .args(["-c", "kill -0 \"$1\"", "fixture", pid.trim()])
                .output()?
                .status
                .success()
        );
    }
    Ok(())
}

#[test]
fn exit_status_preserves_redirected_streams() -> std::result::Result<(), Box<dyn std::error::Error>>
{
    let mut stdout = tempfile::tempfile()?;
    let mut stderr = tempfile::tempfile()?;
    let status = status_with_timeout(
        Command::new("sh")
            .args(["-c", "printf stdout; printf stderr >&2; exit 17"])
            .stdout(stdout.try_clone()?)
            .stderr(stderr.try_clone()?),
        Duration::from_secs(5),
    )?;
    assert_eq!(status.code(), Some(17));
    for (stream, expected) in [(&mut stdout, "stdout"), (&mut stderr, "stderr")] {
        stream.rewind()?;
        let mut actual = String::new();
        stream.read_to_string(&mut actual)?;
        assert_eq!(actual, expected);
    }
    Ok(())
}

#[test]
#[cfg(target_os = "linux")]
fn redirected_stdout_is_preserved_and_stderr_is_captured()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut destination = tempfile::tempfile()?;
    let output = output_with_redirected_stdout(
        Command::new("sh").args([
            "-c",
            "printf destination; head -c 131072 /dev/zero >&2; exit 17",
        ]),
        Stdio::from(destination.try_clone()?),
        Duration::from_secs(5),
    )?;
    assert_eq!(output.status.code(), Some(17));
    assert!(output.stdout.is_empty());
    assert_eq!(output.stderr, vec![0; 131072]);
    destination.rewind()?;
    let mut actual = String::new();
    destination.read_to_string(&mut actual)?;
    assert_eq!(actual, "destination");
    Ok(())
}
