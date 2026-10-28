use super::*;

#[cfg(target_os = "linux")]
#[tokio::test]
async fn dropping_the_request_waiter_keeps_the_child_owned_until_cleanup() {
    use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};

    let Some(pid_file) = std::env::var_os("DAR_TEST_REQUEST_LIFETIME_PID") else {
        // The process-global cancellation latch must not affect other unit tests.
        let directory = tempfile::tempdir().unwrap();
        let executable = directory.path().join("databricks");
        fs::write(
            &executable,
            "#!/bin/sh\nprintf '%s' $$ > \"$DAR_TEST_REQUEST_LIFETIME_PID\"\nexec /bin/sleep 60\n",
        )
        .unwrap();
        fs::set_permissions(executable, fs::Permissions::from_mode(0o755)).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "table_access::unity_catalog::databricks_cli::tests::dropping_the_request_waiter_keeps_the_child_owned_until_cleanup", "--nocapture"])
                .env("PATH", directory.path())
                .env("DAR_TEST_REQUEST_LIFETIME_PID", directory.path().join("pid"))
                .output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            directory.path().join("pid").is_file(),
            "child lifetime test did not run"
        );
        return;
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        let request = tokio::spawn(run_databricks_cli(&[]));
        let pid_file = PathBuf::from(pid_file);
        let pid = loop {
            if let Some(pid) = fs::read_to_string(&pid_file)
                .ok()
                .and_then(|pid| pid.parse::<i32>().ok())
            {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        // Give an incorrect kill_on_drop owner time to deliver its kill signal.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let child_alive = fs::read_to_string(format!("/proc/{pid}/stat"))
            .is_ok_and(|stat| stat.split_whitespace().nth(2) != Some("Z"));
        cancel_pending_requests().await;
        assert!(child_alive, "dropping the waiter discarded its child owner");
        assert!(
            !PathBuf::from(format!("/proc/{pid}")).exists(),
            "cleanup did not reap the child"
        );
    })
    .await
    .expect("child lifetime test exceeded its deadline");
}

#[tokio::test]
async fn subprocess_output_is_bounded_and_timeout_reaps_child() {
    let directory = tempfile::tempdir().unwrap();
    let pid_file = directory.path().join("pid");
    let result = run_command_with_timeout(
        Command::new("/bin/sh")
            .args(["-c", "printf '%s' $$ > \"$1\"; exec /bin/sleep 60", "sh"])
            .arg(&pid_file),
        Duration::from_millis(200),
    )
    .await;
    assert_eq!(result, Err(Error::DatabricksCommandTimedOut));
    let pid = std::fs::read_to_string(pid_file).unwrap();
    let status = std::process::Command::new("/bin/kill")
        .args(["-0", &pid])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "timed-out child was not reaped");
    let bytes = vec![b'x'; MAX_COMMAND_OUTPUT_BYTES as usize + 1];
    assert_eq!(
        read_bounded_output(bytes.as_slice()).await,
        Err(Error::InvalidDatabricksResponse)
    );
    assert_eq!(
        run_command_with_timeout(
            Command::new("/bin/sh").args(["-c", "printf secret-sentinel >&2; exit 1"]),
            Duration::from_secs(2)
        )
        .await,
        Err(Error::DatabricksCommandFailed)
    );
}
