//! Preview uses UC grants and terminates pending grant requests at its deadline.

use super::*;
use crate::{diagnostics::assert_single_terminal_record, mock_cloud_storage::partitioned_objects};

#[test]
fn preview_reads_with_each_unity_catalog_credential_kind() -> TestResult {
    for kind in [
        StorageCredentialKind::S3,
        StorageCredentialKind::R2,
        StorageCredentialKind::AzureBlobSas,
        StorageCredentialKind::AzureBlobBearer,
        StorageCredentialKind::Gcs,
    ] {
        let fixture = DatabricksCliFixture::new(kind)?;
        let server = MockCloudStorageServer::with_objects(
            kind.storage_protocol(),
            partitioned_objects()?,
            |_, _| {},
        )?;
        fixture.use_storage_endpoint(kind, &server.endpoint)?;
        let output = run_with_timeout(fixture.table_command("preview").args([
            "--column",
            "id",
            "--limit",
            "3",
            "--table-version",
            "0",
            "--diagnostics=json",
        ]))?;
        assert!(
            output.status.success(),
            "{kind:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let sample: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(sample["completion"], "sample");
        assert_eq!(sample["row_count"], "3");
        assert_eq!(sample["stop_reason"], "row_limit");
        assert_eq!(sample["bindings"][0]["table_version"], "0");
        assert_eq!(sample["schema"]["fields"].as_array().unwrap().len(), 1);
        let receipt = assert_single_terminal_record(&output.stderr, "completion");
        assert_eq!(receipt["command"], "preview");
        assert_eq!(receipt["output_bytes"], output.stdout.len().to_string());
        for sensitive in ["secret", PROFILE, TABLE_NAME, TABLE_ID] {
            assert!(!String::from_utf8_lossy(&output.stdout).contains(sensitive));
            assert!(!String::from_utf8_lossy(&output.stderr).contains(sensitive));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn preview_deadline_reaps_initial_and_renewal_databricks_processes() -> TestResult {
    for during_refresh in [false, true] {
        let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
        let _server = if during_refresh {
            Some(fixture.storage_with_credential_expiry_and_invalid_metadata(
                StorageCredentialKind::S3,
                |_| {},
            )?)
        } else {
            None
        };
        let started = Instant::now();
        let output = run_with_timeout(
            fixture
                .table_command("preview")
                .args([
                    "--diagnostics=json",
                    "--deadline-ms",
                    if during_refresh { "4500" } else { "500" },
                ])
                .env("DAR_TEST_HANG", if during_refresh { "0" } else { "1" })
                .env("DAR_TEST_REFRESH", if during_refresh { "hang" } else { "" }),
        )?;
        assert!(started.elapsed() < Duration::from_secs(if during_refresh { 7 } else { 3 }));
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["reason"],
            "command_deadline_exceeded"
        );
        let pid = fs::read_to_string(fixture.directory.path().join("pid"))?.parse::<i32>()?;
        let stopped = || !std::path::Path::new(&format!("/proc/{pid}/stat")).exists();
        if !stopped() {
            // SAFETY: this PID belongs to the test's still-running mock authorization child.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
            panic!(
                "Databricks process was not reaped after preview deadline, refresh={during_refresh}"
            );
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn preview_cancellation_reaps_the_child_and_keeps_the_signal_status() -> TestResult {
    for (during_refresh, signal) in [(false, libc::SIGINT), (true, libc::SIGTERM)] {
        let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
        let _server = if during_refresh {
            Some(fixture.storage_with_credential_expiry_and_invalid_metadata(
                StorageCredentialKind::S3,
                |_| {},
            )?)
        } else {
            None
        };
        let mut child = fixture
            .table_command("preview")
            .arg("--diagnostics=json")
            .env("DAR_TEST_HANG", if during_refresh { "0" } else { "1" })
            .env("DAR_TEST_REFRESH", if during_refresh { "hang" } else { "" })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let pid_file = fixture.directory.path().join("pid");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pid_file.exists() && Instant::now() < deadline && child.try_wait()?.is_none() {
            thread::sleep(Duration::from_millis(10));
        }
        if !pid_file.exists() {
            child.kill()?;
            child.wait()?;
            return Err("preview never started the pending grant request".into());
        }
        let pid = fs::read_to_string(pid_file)?.parse::<i32>()?;
        // SAFETY: this is the live CLI process started above.
        assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        let deadline = Instant::now() + Duration::from_secs(3);
        while child.try_wait()?.is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if child.try_wait()?.is_none() {
            child.kill()?;
            child.wait()?;
            return Err("preview cancellation did not exit within its grace".into());
        }
        let output = child.wait_with_output()?;
        assert_eq!(output.status.code(), Some(128 + signal));
        assert!(output.stdout.is_empty());
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
            "command_cancelled"
        );
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}/stat")).exists(),
            "child was not reaped"
        );
    }
    Ok(())
}
