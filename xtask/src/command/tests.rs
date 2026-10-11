#![cfg(unix)]

use super::*;

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
    let temporary = tempfile::tempdir()?;
    let pid_file = temporary.path().join("pid");
    let start = Instant::now();
    let result = capture_with_timeout(
        Command::new("sh")
            .args(["-c", "echo $$ > \"$1\"; exec sleep 30", "fixture"])
            .arg(&pid_file),
        Duration::from_millis(200),
        None,
    );
    assert_eq!(
        result.expect_err("sleep must time out").kind(),
        io::ErrorKind::TimedOut
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
