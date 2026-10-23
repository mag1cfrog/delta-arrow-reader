//! A blocked input, credential provider, or consumer cannot extend the preview indefinitely.

use std::{
    io,
    process::Stdio,
    thread,
    time::{Duration, Instant},
};

use super::*;
use crate::{create_full_pipe, run_with_stdout, wait_for_exit_or_kill};

#[test]
fn deadline_includes_blocked_inputs_and_synchronous_credential_reads() -> TestResult {
    let directory = tempfile::tempdir()?;
    let fifo = directory.path().join("secret-input");
    assert!(Command::new("mkfifo").arg(&fifo).status()?.success());
    for option in [
        "--storage-options-file",
        "--predicate-file",
        "--execution-options-file",
        "gcs-adc",
        "aws-token",
        "azure-token",
    ] {
        let mut cmd = if option.starts_with("--") {
            let mut cmd = preview_command();
            cmd.arg(option).arg(&fifo);
            cmd
        } else {
            let mut cmd = Command::new(env!("CARGO_BIN_EXE_dar"));
            cmd.env_clear().args(["preview", "--diagnostics=json"]);
            match option {
                "gcs-adc" => {
                    cmd.args(["gs://bucket/table", "--auth-source", "env"])
                        .env("GOOGLE_APPLICATION_CREDENTIALS", &fifo);
                }
                "aws-token" => {
                    cmd.args(["s3://bucket/table", "--auth-source", "aws-web-identity"])
                        .env("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/secret")
                        .env("AWS_WEB_IDENTITY_TOKEN_FILE", &fifo)
                        .env("AWS_REGION", "us-east-1");
                }
                "azure-token" => {
                    cmd.args([
                        "az://container/table",
                        "--auth-source",
                        "azure-workload-identity",
                    ])
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
        assert_eq!(output.status.code(), Some(1), "{option}: {output:?}");
        assert!(output.stdout.is_empty());
        let error = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(
            error["reason"], "command_deadline_exceeded",
            "{option}: {error}"
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    Ok(())
}

#[test]
fn blocked_stdout_and_stderr_remain_within_deadline_and_cleanup_grace() -> TestResult {
    let (_reader, writer) = create_full_pipe()?;
    let started = Instant::now();
    let output = run_with_stdout(
        preview_command().args(["--deadline-ms", "200"]),
        Stdio::from(writer),
    )?;
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "diagnostic")["reason"],
        "command_deadline_exceeded"
    );
    for invalid_logging in [false, true] {
        let (_reader, writer) = create_full_pipe()?;
        let mut cmd = if invalid_logging {
            let mut cmd = Command::new(env!("CARGO_BIN_EXE_dar"));
            cmd.env_clear()
                .arg("preview")
                .arg(fixture_table_path("partitioned"))
                .env("RUST_LOG", "secret=invalid");
            cmd
        } else {
            preview_command()
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
        preview_command().args(["--deadline-ms", "2000"]),
        Stdio::from(writer),
    )?;
    assert_eq!(output.status.code(), Some(3));
    assert_single_terminal_record(&output.stderr, "diagnostic");
    Ok(())
}

#[test]
fn blocked_completion_can_be_cancelled_without_a_success_record() -> TestResult {
    for (signal, status) in [(libc::SIGINT, 130), (libc::SIGTERM, 143)] {
        let (_reader, writer) = create_full_pipe()?;
        let stdout = tempfile::tempfile()?;
        let mut child = preview_command()
            .args(["--deadline-ms", "5000"])
            .stdout(stdout.try_clone()?)
            .stderr(Stdio::from(writer))
            .spawn()?;
        let started = Instant::now();
        while stdout.metadata()?.len() == 0 {
            if started.elapsed() > Duration::from_secs(3) {
                child.kill()?;
                child.wait()?;
                return Err("preview never wrote its sample".into());
            }
            thread::sleep(Duration::from_millis(10));
        }
        // SAFETY: this process was started above and has not been reaped.
        assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        assert_eq!(
            wait_for_exit_or_kill(&mut child, Duration::from_secs(3))?.code(),
            Some(status)
        );
    }
    Ok(())
}
