//! UC scan results, renewal during file reads, and interrupted Arrow output.

use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::Cursor,
    path::PathBuf,
    process::Command,
    thread,
    time::{Duration, SystemTime},
};

use arrow::{
    array::{BooleanArray, Int32Array, StringArray},
    compute::{concat_batches, filter_record_batch},
    ipc::reader::{FileReader, StreamReader},
    record_batch::RecordBatch,
};
use serde_json::{Value, json};

use super::{
    DatabricksCliFixture, MockCloudStorageServer, StorageCredentialKind, TestResult,
    run_with_timeout,
};

const IPC_END: &[u8] = &[255, 255, 255, 255, 0, 0, 0, 0];

fn requested_path(request: &str) -> String {
    let target = request.split_whitespace().nth(1).unwrap();
    let url = url::Url::parse(&format!("http://localhost{target}")).unwrap();
    object_store::path::Path::from_url_path(url.path())
        .unwrap()
        .to_string()
}

fn partitioned_corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned")
}

fn partitioned_objects() -> Result<BTreeMap<String, Vec<u8>>, Box<dyn std::error::Error>> {
    let table = partitioned_corpus().join("table");
    let log = fs::read(table.join("_delta_log/00000000000000000000.json"))?;
    let mut objects = BTreeMap::new();
    for line in log
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let action: Value = serde_json::from_slice(line)?;
        if let Some(path) = action["add"]["path"].as_str() {
            objects.insert(format!("table/{path}"), fs::read(table.join(path))?);
        }
    }
    objects.insert("table/_delta_log/00000000000000000000.json".into(), log);
    Ok(objects)
}

/// A second snapshot drops one partition so a version-selection regression changes the rows.
fn remove_west_partition(objects: &mut BTreeMap<String, Vec<u8>>) {
    let path = objects
        .keys()
        .find(|key| key.starts_with("table/region=west/"))
        .unwrap();
    let action = json!({"remove": {
        "path": path.strip_prefix("table/").unwrap(), "deletionTimestamp": 1, "dataChange": true
    }});
    objects.insert(
        "table/_delta_log/00000000000000000001.json".into(),
        format!("{action}\n").into_bytes(),
    );
}

fn expected_partitioned_rows() -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let reader = FileReader::try_new(
        fs::File::open(partitioned_corpus().join("expected.arrow"))?,
        None,
    )?;
    Ok(concat_batches(
        &reader.schema(),
        &reader.collect::<Result<Vec<_>, _>>()?,
    )?)
}

fn decode_stream(bytes: &[u8]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    Ok(concat_batches(
        &reader.schema(),
        &reader.collect::<Result<Vec<_>, _>>()?,
    )?)
}

/// A limit may select any row order; every returned row must be distinct and belong to the expected set.
fn assert_scan_rows(actual: &RecordBatch, expected: &RecordBatch, limit: usize) {
    assert_eq!(actual.schema(), expected.schema());
    assert_eq!(actual.num_rows(), expected.num_rows().min(limit));
    if actual.num_columns() == 0 {
        return;
    }
    let ids = actual
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let expected_ids = expected
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let mut seen = HashSet::new();
    for (row, id) in ids.values().iter().enumerate() {
        assert!(seen.insert(id), "duplicate output row {id}");
        let expected_row = expected_ids
            .values()
            .iter()
            .position(|expected| expected == id)
            .unwrap();
        assert_eq!(actual.slice(row, 1), expected.slice(expected_row, 1));
    }
}

#[test]
fn scans_preserve_values_options_and_snapshot_selection_for_every_cloud() -> TestResult {
    let all_rows = expected_partitioned_rows()?;
    let mut objects = partitioned_objects()?;
    remove_west_partition(&mut objects);
    for kind in [
        StorageCredentialKind::S3,
        StorageCredentialKind::R2,
        StorageCredentialKind::AzureSas,
        StorageCredentialKind::AzureBearer,
        StorageCredentialKind::Gcs,
    ] {
        let fixture = DatabricksCliFixture::new(kind)?;
        let server = MockCloudStorageServer::with_objects(kind, objects.clone(), |_, _| {})?;
        fixture.use_storage_endpoint(kind, &server.endpoint)?;
        let predicate_file = fixture.directory.path().join("predicate.json");
        fs::write(
            &predicate_file,
            serde_json::to_vec(&json!({
                "op": "lt", "column": "id", "value": {"type": "int32", "value": "20"}
            }))?,
        )?;
        for backend in ["direct", "delta_kernel"] {
            let execution_file = fixture.directory.path().join("execution.json");
            fs::write(
                &execution_file,
                serde_json::to_vec(&json!({
                    "parquet_backend": backend, "parquet_full_file_read_threshold_bytes": null,
                    "max_concurrent_file_reads_per_scan": 2, "prefetch_files_per_partition": 0
                }))?,
            )?;
            for historical in [false, true] {
                for case in [
                    "full",
                    "filtered_projection",
                    "empty_projection",
                    "zero_limit",
                ] {
                    let context = format!("{kind:?} {backend} historical={historical} {case}");
                    let mut command = fixture.scan();
                    command
                        .arg("--execution-options-file")
                        .arg(&execution_file)
                        .args(["--target-partitions", "2", "--diagnostics=json"])
                        .env("AWS_ACCESS_KEY_ID", "ambient-secret-key")
                        .env("AZURE_STORAGE_TOKEN", "ambient-secret-token")
                        .env(
                            "GOOGLE_APPLICATION_CREDENTIALS",
                            "ambient-secret-missing.json",
                        );
                    let mut expected = all_rows.clone();
                    if historical {
                        command.args(["--table-version", "0"]);
                    } else {
                        let regions = expected
                            .column_by_name("region")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        let mask = BooleanArray::from_iter(
                            regions.iter().map(|region| region != Some("west")),
                        );
                        expected = filter_record_batch(&expected, &mask)?;
                    }
                    let limit = match case {
                        "filtered_projection" => {
                            command
                                .args(["--column", "value", "--column", "id", "--limit", "7"])
                                .arg("--predicate-file")
                                .arg(&predicate_file);
                            let ids = expected
                                .column_by_name("id")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap();
                            expected = filter_record_batch(
                                &expected,
                                &BooleanArray::from_iter(ids.values().iter().map(|id| *id < 20)),
                            )?
                            .project(&[1, 0])?;
                            7
                        }
                        "empty_projection" => {
                            command.args(["--no-columns", "--limit", "3"]);
                            expected = expected.project(&[])?;
                            3
                        }
                        "zero_limit" => {
                            command.args(["--limit", "0"]);
                            0
                        }
                        _ => usize::MAX,
                    };
                    let output = run_with_timeout(&mut command)?;
                    assert!(
                        output.status.success(),
                        "{context}: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                    assert!(
                        output.stdout.ends_with(IPC_END),
                        "{context}: missing IPC completion"
                    );
                    let actual = decode_stream(&output.stdout)?;
                    assert_scan_rows(&actual, &expected, limit);
                    let record = super::super::diagnostics::assert_single_terminal_record(
                        &output.stderr,
                        "completion",
                    );
                    assert_eq!(
                        record["bindings"][0]["table_version"],
                        if historical { "0" } else { "1" }
                    );
                }
            }
        }
        assert!(
            server
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|request| requested_path(request).ends_with(".parquet"))
        );
        assert_eq!(
            fixture
                .calls()
                .iter()
                .filter(|arg| *arg == "temporary-table-credentials")
                .count(),
            16
        );
    }
    Ok(())
}

/// Expire credentials after one data file, with reads serialized so output has already started.
fn scan_with_renewal(
    fixture: &DatabricksCliFixture,
    kind: StorageCredentialKind,
    backend: &str,
    update_replacement: impl FnOnce(&mut Value),
) -> Result<(MockCloudStorageServer, Command), Box<dyn std::error::Error>> {
    let expires_at = fixture.prepare_credential_renewal(update_replacement)?;
    let metadata_path = fixture.directory.path().join("metadata.json");
    let mut first_file = None;
    let mut delayed = false;
    let server = MockCloudStorageServer::with_objects(
        kind,
        partitioned_objects()?,
        move |request, objects| {
            let path = requested_path(request);
            if !path.ends_with(".parquet") {
                return;
            }
            if let Some(first_file) = &first_file {
                if path != *first_file && !delayed {
                    delayed = true;
                    fs::write(&metadata_path, "secret-table-was-replaced").unwrap();
                    remove_west_partition(objects);
                    thread::sleep(
                        expires_at
                            .duration_since(SystemTime::now())
                            .unwrap_or_default()
                            + Duration::from_millis(20),
                    );
                }
            } else {
                first_file = Some(path);
            }
        },
    )?;
    fixture.use_storage_endpoint(kind, &server.endpoint)?;
    let options = fixture.directory.path().join("execution.json");
    fs::write(
        &options,
        serde_json::to_vec(&json!({
            "parquet_backend": backend,
            "max_concurrent_file_reads_per_scan": 1,
            "max_concurrent_file_reads_per_partition": 1,
            "output_buffer_batches_per_partition": 1,
            "prefetch_files_per_partition": 0,
            "parquet_full_file_read_threshold_bytes": null,
            "parquet_metadata_size_hint_bytes": 8
        }))?,
    )?;
    let mut command = fixture.scan();
    command
        .args(["--target-partitions", "1", "--diagnostics=json"])
        .arg("--execution-options-file")
        .arg(options)
        .env("DAR_TEST_REFRESH", "rotate");
    Ok((server, command))
}

#[test]
fn renewal_during_scans_keeps_the_original_snapshot_and_emits_each_row_once() -> TestResult {
    let expected = expected_partitioned_rows()?;
    for kind in [
        StorageCredentialKind::S3,
        StorageCredentialKind::R2,
        StorageCredentialKind::AzureSas,
        StorageCredentialKind::AzureBearer,
        StorageCredentialKind::Gcs,
    ] {
        for backend in ["direct", "delta_kernel"] {
            let fixture = DatabricksCliFixture::new(kind)?;
            let (server, mut command) =
                scan_with_renewal(&fixture, kind, backend, |replacement| {
                    kind.rotate_credentials(replacement)
                })?;
            let output = run_with_timeout(&mut command)?;
            assert!(
                output.status.success(),
                "{kind:?} {backend}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(output.stdout.ends_with(IPC_END));
            assert_scan_rows(&decode_stream(&output.stdout)?, &expected, usize::MAX);
            let record = super::super::diagnostics::assert_single_terminal_record(
                &output.stderr,
                "completion",
            );
            assert_eq!(record["bindings"][0]["table_version"], "0");
            let calls = fixture.calls();
            assert_eq!(calls.iter().filter(|arg| *arg == "tables").count(), 1);
            assert_eq!(
                calls
                    .iter()
                    .filter(|arg| *arg == "temporary-table-credentials")
                    .count(),
                2
            );
            for pair in calls.windows(2).filter(|pair| pair[0] == "--table-id") {
                assert_eq!(pair[1], super::TABLE_ID);
            }
            let requests = server.requests.lock().unwrap();
            assert!(
                requests
                    .iter()
                    .filter(|request| requested_path(request).ends_with(".parquet"))
                    .any(|request| request
                        .to_ascii_lowercase()
                        .contains(kind.renewed_authorization()))
            );
            assert!(!output.stdout.windows(7).any(|bytes| bytes == b"secret-"));
        }
    }
    Ok(())
}

#[test]
fn authorization_failures_before_streaming_leave_stdout_empty() -> TestResult {
    for during_refresh in [false, true] {
        let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
        let _server = if during_refresh {
            Some(fixture.storage_with_expiring_credentials(StorageCredentialKind::S3, |_| {})?)
        } else {
            None
        };
        let output = run_with_timeout(
            fixture
                .scan()
                .arg("--diagnostics=json")
                .env("DAR_TEST_DENIED", if during_refresh { "0" } else { "1" })
                .env("DAR_TEST_REFRESH", "deny"),
        )?;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let record =
            super::super::diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "uc_credential_request_failed");
    }
    Ok(())
}

#[test]
fn failed_renewal_after_rows_never_finishes_the_arrow_stream() -> TestResult {
    let expected = expected_partitioned_rows()?;
    for (kind, backend, failure, code) in [
        (
            StorageCredentialKind::S3,
            "direct",
            "deny",
            "uc_credential_request_failed",
        ),
        (
            StorageCredentialKind::S3,
            "delta_kernel",
            "deny",
            "uc_credential_request_failed",
        ),
        (
            StorageCredentialKind::S3,
            "direct",
            "missing",
            "databricks_cli_missing",
        ),
        (
            StorageCredentialKind::Gcs,
            "direct",
            "root",
            "invalid_databricks_response",
        ),
        (
            StorageCredentialKind::Gcs,
            "direct",
            "expired",
            "uc_credentials_expired",
        ),
        (
            StorageCredentialKind::AzureSas,
            "direct",
            "kind",
            "unsupported_uc_table",
        ),
    ] {
        let fixture = DatabricksCliFixture::new(kind)?;
        let (_server, mut command) =
            scan_with_renewal(&fixture, kind, backend, |replacement| match failure {
                "root" => replacement["url"] = json!("gs://other/secret-root"),
                "expired" => replacement["expiration_time"] = json!(0),
                "kind" => {
                    replacement
                        .as_object_mut()
                        .unwrap()
                        .remove("azure_user_delegation_sas");
                    replacement["azure_aad"] = json!({"aad_token":"secret-token"});
                }
                _ => {}
            })?;
        let output = run_with_timeout(
            command
                .env("DAR_TEST_REFRESH", failure)
                .env("RUST_LOG", "trace"),
        )?;
        assert_eq!(
            output.status.code(),
            Some(if failure == "missing" { 2 } else { 1 }),
            "{backend} {failure}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let record =
            super::super::diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], code);
        assert!(!output.stdout.ends_with(IPC_END));
        let actual = decode_stream(&output.stdout)?;
        assert!(actual.num_rows() > 0 && actual.num_rows() < expected.num_rows());
        assert_scan_rows(&actual, &expected, actual.num_rows());
        assert_eq!(
            fixture
                .calls()
                .iter()
                .filter(|arg| *arg == "temporary-table-credentials")
                .count(),
            // An unavailable executable cannot record a second invocation.
            if failure == "missing" { 1 } else { 2 }
        );
        assert!(!output.stdout.windows(7).any(|bytes| bytes == b"secret-"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn cancelling_or_closing_stdout_terminates_a_pending_scan_refresh() -> TestResult {
    use std::{process::Stdio, time::Instant};

    for signal in [Some(libc::SIGINT), Some(libc::SIGTERM), None] {
        let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
        let (_server, mut command) =
            scan_with_renewal(&fixture, StorageCredentialKind::S3, "direct", |_| {})?;
        let mut child = command
            .env("DAR_TEST_REFRESH", "hang")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let pid_file = fixture.directory.path().join("pid");
        let deadline = Instant::now() + Duration::from_secs(8);
        while !pid_file.exists() && Instant::now() < deadline && child.try_wait()?.is_none() {
            thread::sleep(Duration::from_millis(10));
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
        if let Some(signal) = signal {
            // SAFETY: this is the live CLI child created above.
            assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        } else {
            drop(child.stdout.take());
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while child.try_wait()?.is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if child.try_wait()?.is_none() {
            // SAFETY: the fixture recorded its own pending credential process.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
            child.kill()?;
            child.wait()?;
            panic!("scan did not stop a pending refresh: signal={signal:?}");
        }
        let output = child.wait_with_output()?;
        assert_eq!(
            output.status.code(),
            Some(signal.map_or(3, |signal| 128 + signal))
        );
        let record =
            super::super::diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(
            record["code"],
            if signal.is_some() {
                "uc_cancelled"
            } else {
                "output_write"
            }
        );
        if signal.is_some() {
            assert!(!output.stdout.ends_with(IPC_END));
            assert!(decode_stream(&output.stdout)?.num_rows() > 0);
        } else {
            assert_eq!(record["source_kind"], "broken_pipe");
        }
        let stopped = || {
            fs::read_to_string(format!("/proc/{pid}/stat"))
                .map_or(true, |stat| stat.split_whitespace().nth(2) == Some("Z"))
        };
        while !stopped() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(stopped(), "credential process survived its CLI invocation");
    }
    Ok(())
}
