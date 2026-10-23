//! Deadline and signal cleanup must reap pending credential commands.

use super::*;
use crate::diagnostics::assert_single_terminal_record;

#[test]
fn broken_pipe_reaps_a_credential_refresh_after_the_reader_is_dropped() -> TestResult {
    use crate::mock_cloud_storage::partitioned_objects;
    use std::{io, os::fd::AsRawFd};

    // A disconnect can also wake the supervisor; require an actual write failure in this test.
    for _ in 0..3 {
        let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
        let expires_at = fixture.prepare_credential_renewal(|_| {})?;
        let mut objects = partitioned_objects()?;
        let log_path = "table/_delta_log/00000000000000000000.json";
        let actions: Vec<Value> = std::str::from_utf8(&objects[log_path])?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?;
        let west = actions
            .iter()
            .find(|action| action["add"]["partitionValues"]["region"] == "west")
            .unwrap();
        let parquet = objects[&format!("table/{}", west["add"]["path"].as_str().unwrap())].clone();
        let mut log = String::new();
        for action in actions.iter().filter(|action| action.get("add").is_none()) {
            log.push_str(&format!("{action}\n"));
        }
        // Enough batches to block stdout while the reader starts another credential request.
        for index in 0..100 {
            let mut action = west.clone();
            let path = format!("region=west/copy{index:03}.parquet");
            action["add"]["path"] = json!(path);
            objects.insert(format!("table/{path}"), parquet.clone());
            log.push_str(&format!("{action}\n"));
        }
        objects.insert(log_path.into(), log.into_bytes());
        let mut parquet_requests = 0;
        let server = MockCloudStorageServer::with_objects(
            CloudStorageProtocol::S3,
            objects,
            move |request, _| {
                if request
                    .split_whitespace()
                    .nth(1)
                    .is_some_and(|path| path.ends_with(".parquet"))
                {
                    parquet_requests += 1;
                    if parquet_requests == 12 {
                        thread::sleep(
                            expires_at
                                .duration_since(SystemTime::now())
                                .unwrap_or_default()
                                + Duration::from_millis(20),
                        );
                    }
                }
            },
        )?;
        fixture.use_storage_endpoint(StorageCredentialKind::S3, &server.endpoint)?;
        let execution = fixture.directory.path().join("execution.json");
        fs::write(
            &execution,
            json!({
                "max_concurrent_file_reads_per_scan": 2,
                "max_concurrent_file_reads_per_partition": 2,
                "output_buffer_batches_per_partition": 16,
                "prefetch_files_per_partition": 0,
                "parquet_full_file_read_threshold_bytes": null,
                "parquet_metadata_size_hint_bytes": 8
            })
            .to_string(),
        )?;
        let (reader, writer) = io::pipe()?;
        // SAFETY: this live pipe belongs to the fixture; reducing its capacity forces backpressure.
        assert_eq!(
            unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_SETPIPE_SZ, 4096) },
            4096
        );
        let mut child = fixture
            .table_command("scan")
            .args([
                "--diagnostics=json",
                "--target-partitions",
                "1",
                "--deadline-ms",
                "6000",
            ])
            .arg("--execution-options-file")
            .arg(execution)
            .env("DAR_TEST_REFRESH", "hang")
            .stdout(writer)
            .stderr(Stdio::piped())
            .spawn()?;
        let pid_file = fixture.directory.path().join("pid");
        let waiting = Instant::now();
        while !pid_file.exists()
            && waiting.elapsed() < Duration::from_secs(5)
            && child.try_wait()?.is_none()
        {
            thread::sleep(Duration::from_millis(5));
        }
        if !pid_file.exists() {
            let _ = child.kill();
            let output = child.wait_with_output()?;
            panic!(
                "scan never reached renewal: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let pid = fs::read_to_string(pid_file)?.parse::<i32>()?;
        // Keep the grant pending until cleanup kills it; it cannot race to a normal exit.
        // SAFETY: this PID was published by the fixture's live authorization child.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGSTOP) }, 0);
        drop(reader);
        let exit = crate::wait_for_exit_or_kill(&mut child, Duration::from_secs(3));
        let child_was_reaped = !std::path::Path::new(&format!("/proc/{pid}/stat")).exists();
        if !child_was_reaped {
            // SAFETY: clean up this fixture's stopped child without hiding the failed assertion.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
        exit?;
        let output = child.wait_with_output()?;
        assert_eq!(output.status.code(), Some(3));
        let error = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(error["code"], "output_write");
        assert_eq!(error["source_kind"], "broken_pipe");
        assert!(child_was_reaped, "refresh child was not reaped");
        if error["os_error"] == libc::EPIPE {
            return Ok(());
        }
    }
    Err("stdout monitor always won; worker error cleanup was not exercised".into())
}

#[cfg(target_os = "linux")]
#[test]
fn deadlines_reap_initial_and_renewal_databricks_processes() -> TestResult {
    for operation in ["inspect", "scan", "preview", "query"] {
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
            let mut command = if operation == "query" {
                fixture.query("SELECT * FROM t")?
            } else {
                fixture.table_command(operation)
            };
            let started = Instant::now();
            let output = run_with_timeout(
                command
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
                    "Databricks process was not reaped after {operation} deadline, refresh={during_refresh}"
                );
            }
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
