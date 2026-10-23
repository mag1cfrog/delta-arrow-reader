//! Local SQL over UC snapshots, with the existing Databricks and cloud fixtures.

use std::{
    fs,
    process::{Command, Output},
    thread,
    time::{Duration, SystemTime},
};

use arrow::{
    array::{BooleanArray, Int32Array, StringArray},
    compute::{filter_record_batch, sort_to_indices, take_record_batch},
};
use serde_json::{Value, json};

use super::{
    CredentialRefreshFailure, DatabricksCliFixture, MockCloudStorageServer, PROFILE,
    StorageCredentialKind, TestResult, run_with_timeout,
};
use crate::{
    diagnostics::assert_single_terminal_record,
    mock_cloud_storage::{
        append_west_partition_removal_commit, expected_partitioned_rows, partitioned_objects,
        partitioned_objects_with_large_parquet_footers, requested_object_path,
    },
    query::{
        assert_query_success_and_decode_arrow, fixture_path,
        json::assert_query_success_and_decode_json,
    },
};

#[test]
fn json_queries_preserve_uc_and_local_bindings_with_complete_receipts() -> TestResult {
    let kind = StorageCredentialKind::S3;
    let fixture = DatabricksCliFixture::new(kind)?;
    let server = MockCloudStorageServer::with_objects(
        kind.storage_protocol(),
        partitioned_objects()?,
        |_, _| {},
    )?;
    fixture.use_storage_endpoint(kind, &server.endpoint)?;
    for (tables, sql, expected_bindings, expected_rows) in [
        (
            None,
            "SELECT id FROM t WHERE id <= 2 ORDER BY id",
            json!([{"alias":"t", "table_version":"0"}]),
            json!([["1"], ["2"]]),
        ),
        (
            Some(json!([
                {"alias":"orders", "location":"uc://main.sales.orders", "table_version":"0",
                 "authorization":{"source":"databricks", "databricks_profile":PROFILE,
                                  "storage_options_file":"../options.json"}},
                {"alias":"local_rows", "location":fixture_path("partitioned").join("table"), "table_version":"0"}
            ])),
            "SELECT o.id, l.id FROM orders o JOIN local_rows l ON o.id = l.id WHERE o.id <= 2 ORDER BY o.id",
            json!([{"alias":"orders", "table_version":"0"}, {"alias":"local_rows", "table_version":"0"}]),
            json!([["1", "1"], ["2", "2"]]),
        ),
    ] {
        let mut command = match tables {
            Some(tables) => fixture.manifest_query(tables, sql)?,
            None => fixture.query(sql)?,
        };
        let output = run_with_timeout(command.args(["--format", "json", "--diagnostics=json"]))?;
        let result = assert_query_success_and_decode_json(&output)?;
        assert_eq!(result["bindings"], expected_bindings);
        assert_eq!(result["rows"], expected_rows);
        for field in result["schema"]["fields"].as_array().unwrap() {
            assert_eq!(field["name"], "id");
            assert_eq!(field["data_type"], "Int32");
        }
        assert!(!String::from_utf8_lossy(&output.stdout).contains("secret"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    Ok(())
}

#[test]
fn single_table_queries_preserve_values_versions_and_vended_credentials() -> TestResult {
    let expected = expected_partitioned_rows()?;
    let mut objects = partitioned_objects()?;
    append_west_partition_removal_commit(&mut objects);
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
            objects.clone(),
            |_, _| {},
        )?;
        fixture.use_storage_endpoint(kind, &server.endpoint)?;
        for historical in [false, true] {
            let mut command = fixture.query("SELECT value, id FROM t WHERE id < 20 ORDER BY id")?;
            command
                .arg("--diagnostics=json")
                .env("AWS_ACCESS_KEY_ID", "ambient-secret-key")
                .env("AZURE_STORAGE_TOKEN", "ambient-secret-token")
                .env(
                    "GOOGLE_APPLICATION_CREDENTIALS",
                    "ambient-secret-missing.json",
                );
            if historical {
                command.args(["--table-version", "0"]);
            }
            let output = run_with_timeout(&mut command)?;
            let actual = assert_query_success_and_decode_arrow(&output)?;
            let ids = expected
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let regions = expected
                .column_by_name("region")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let selected = filter_record_batch(
                &expected,
                &BooleanArray::from_iter(ids.iter().zip(regions.iter()).map(|(id, region)| {
                    id.is_some_and(|id| id < 20) && (historical || region != Some("west"))
                })),
            )?;
            let selected = take_record_batch(
                &selected,
                &sort_to_indices(selected.column_by_name("id").unwrap(), None, None)?,
            )?
            .project(&[1, 0])?;
            assert_eq!(
                actual.columns(),
                selected.columns(),
                "{kind:?} historical={historical}"
            );
            assert_eq!(actual.schema().field(0).name(), "value");
            assert_eq!(actual.schema().field(1).name(), "id");
            let record = assert_single_terminal_record(&output.stderr, "completion");
            assert_eq!(
                record["bindings"],
                json!([{"alias":"t", "table_version": if historical { "0" } else { "1" }}])
            );
        }
        let recorded = fixture.recorded_arguments();
        assert_eq!(recorded.iter().filter(|arg| *arg == "tables").count(), 2);
        assert_eq!(
            recorded
                .iter()
                .filter(|arg| *arg == "temporary-table-credentials")
                .count(),
            2
        );
        assert!(
            !recorded
                .iter()
                .any(|arg| matches!(arg.as_str(), "sql" | "warehouses" | "clusters"))
        );
        let authentication = match kind {
            StorageCredentialKind::S3 => "/eu-west-1/s3/aws4_request",
            StorageCredentialKind::R2 => "/auto/s3/aws4_request",
            StorageCredentialKind::AzureBlobSas => "sig=test%2bsignature",
            StorageCredentialKind::AzureBlobBearer => "authorization: bearer secret-bearer-token",
            StorageCredentialKind::Gcs => "authorization: bearer secret-gcp-token",
        };
        let requests = server.requests.lock().unwrap();
        assert!(!requests.is_empty());
        for request in requests.iter() {
            assert!(
                request.to_ascii_lowercase().contains(authentication),
                "{kind:?}: missing selected credentials"
            );
            assert!(!request.contains("ambient-secret"));
        }
    }
    Ok(())
}

#[test]
fn query_input_failures_precede_databricks_and_planning_errors_stay_sql_errors() -> TestResult {
    let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
    for sql in ["DROP TABLE t", "SELECT secret("] {
        let output = run_with_timeout(fixture.query(sql)?.arg("--diagnostics=json"))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
            "invalid_sql_input"
        );
        assert!(fixture.recorded_arguments().is_empty());
    }
    let output = run_with_timeout(fixture.query("SELECT * FROM t")?.args([
        "--auth-source",
        "env",
        "--diagnostics=json",
    ]))?;
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "diagnostic")["reason"],
        "auth_source_conflicts_with_unity_catalog"
    );
    assert!(fixture.recorded_arguments().is_empty());

    let server = MockCloudStorageServer::with_objects(
        StorageCredentialKind::S3.storage_protocol(),
        partitioned_objects()?,
        |_, _| {},
    )?;
    fixture.use_storage_endpoint(StorageCredentialKind::S3, &server.endpoint)?;
    let output = run_with_timeout(
        fixture
            .query("SELECT secret_missing_column FROM t")?
            .arg("--diagnostics=json"),
    )?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
        "sql_planning"
    );
    assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    Ok(())
}

/// Expires the grant during Parquet reads while replacing metadata and publishing a new snapshot.
pub(super) fn query_with_credential_expiry_and_table_changes(
    fixture: &DatabricksCliFixture,
    kind: StorageCredentialKind,
    update_replacement: impl FnOnce(&mut Value),
) -> Result<(MockCloudStorageServer, Command), Box<dyn std::error::Error>> {
    let expires_at = fixture.prepare_credential_renewal(update_replacement)?;
    let metadata = fixture.directory.path().join("metadata.json");
    let mut delayed = false;
    let server = MockCloudStorageServer::with_objects(
        kind.storage_protocol(),
        partitioned_objects_with_large_parquet_footers()?,
        move |request, objects| {
            if !delayed && requested_object_path(request).ends_with(".parquet") {
                delayed = true;
                fs::write(&metadata, "secret-table-was-replaced").unwrap();
                append_west_partition_removal_commit(objects);
                thread::sleep(
                    expires_at
                        .duration_since(SystemTime::now())
                        .unwrap_or_default()
                        + Duration::from_millis(20),
                );
            }
        },
    )?;
    fixture.use_storage_endpoint(kind, &server.endpoint)?;
    let mut command = fixture.query("SELECT id, value FROM t ORDER BY id")?;
    command
        .arg("--diagnostics=json")
        .env("DAR_TEST_REFRESH", "rotate")
        .env("AWS_ACCESS_KEY_ID", "ambient-secret-key")
        .env("AZURE_STORAGE_TOKEN", "ambient-secret-token")
        .env(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "ambient-secret-missing.json",
        );
    Ok((server, command))
}

fn assert_query_failure(output: &Output, status: i32, code: &str) {
    assert_eq!(
        output.status.code(),
        Some(status),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
        code
    );
    assert!(!output.stdout.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    assert!(!output.stdout.windows(7).any(|bytes| bytes == b"secret-"));
}

#[test]
fn concurrent_sql_reads_renew_once_and_keep_the_original_snapshot_for_every_cloud() -> TestResult {
    let expected = crate::sort_by_id(&expected_partitioned_rows()?.project(&[0, 1])?)?;
    for kind in [
        StorageCredentialKind::S3,
        StorageCredentialKind::R2,
        StorageCredentialKind::AzureBlobSas,
        StorageCredentialKind::AzureBlobBearer,
        StorageCredentialKind::Gcs,
    ] {
        let fixture = DatabricksCliFixture::new(kind)?;
        let (server, mut command) =
            query_with_credential_expiry_and_table_changes(&fixture, kind, |replacement| {
                kind.rotate_credentials(replacement)
            })?;
        // Hold the refresh open so concurrent file reads must share its result.
        let output = run_with_timeout(command.env("DAR_TEST_REFRESH_DELAY", "1"))?;
        let actual = assert_query_success_and_decode_arrow(&output)?;
        assert_eq!(actual.columns(), expected.columns(), "{kind:?}");
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "completion")["bindings"],
            json!([{"alias":"t", "table_version":"0"}])
        );
        let recorded = fixture.recorded_arguments();
        assert_eq!(recorded.iter().filter(|arg| *arg == "tables").count(), 1);
        assert_eq!(
            recorded
                .iter()
                .filter(|arg| *arg == "temporary-table-credentials")
                .count(),
            2,
            "{kind:?}"
        );
        for pair in recorded.windows(2).filter(|pair| pair[0] == "--table-id") {
            assert_eq!(pair[1], super::TABLE_ID);
        }
        let profiles: Vec<_> = recorded
            .iter()
            .filter(|arg| arg.starts_with("--profile="))
            .collect();
        assert_eq!(profiles, vec![&format!("--profile={}", super::PROFILE); 3]);
        let requests = server.requests.lock().unwrap();
        assert!(requests.iter().any(|request| {
            request
                .to_ascii_lowercase()
                .contains(kind.expected_initial_auth_fragment())
        }));
        assert!(requests.iter().any(|request| {
            requested_object_path(request).ends_with(".parquet")
                && request
                    .to_ascii_lowercase()
                    .contains(kind.expected_renewed_auth_fragment())
        }));
        for request in requests.iter() {
            assert!(!request.contains("ambient-secret"));
            if kind == StorageCredentialKind::S3 {
                assert!(request.contains("/eu-west-1/s3/aws4_request"));
            }
        }
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
        assert!(!output.stdout.windows(7).any(|bytes| bytes == b"secret-"));
    }
    Ok(())
}

#[test]
fn failed_sql_renewals_keep_uc_errors_and_never_complete_the_result() -> TestResult {
    for (kind, failure, format) in [
        (
            StorageCredentialKind::S3,
            CredentialRefreshFailure::PermissionDenied,
            "json",
        ),
        (
            StorageCredentialKind::S3,
            CredentialRefreshFailure::PermissionDenied,
            "arrow",
        ),
        (
            StorageCredentialKind::S3,
            CredentialRefreshFailure::MissingDatabricksCli,
            "arrow",
        ),
        (
            StorageCredentialKind::R2,
            CredentialRefreshFailure::ChangedStorageRoot,
            "arrow",
        ),
        (
            StorageCredentialKind::Gcs,
            CredentialRefreshFailure::ExpiredCredentials,
            "arrow",
        ),
        (
            StorageCredentialKind::Gcs,
            CredentialRefreshFailure::InvalidBearerToken,
            "arrow",
        ),
        (
            StorageCredentialKind::AzureBlobSas,
            CredentialRefreshFailure::ChangedCredentialKind,
            "arrow",
        ),
        (
            StorageCredentialKind::AzureBlobBearer,
            CredentialRefreshFailure::PermissionDenied,
            "arrow",
        ),
    ] {
        let fixture = DatabricksCliFixture::new(kind)?;
        let (server, mut command) =
            query_with_credential_expiry_and_table_changes(&fixture, kind, |replacement| {
                failure.apply_to_response(replacement)
            })?;
        let output = run_with_timeout(
            command
                .args(["--format", format])
                .env("DAR_TEST_REFRESH", failure.cli_refresh_behavior())
                .env("RUST_LOG", "trace"),
        )?;
        if format == "json" {
            assert!(output.stdout.is_empty());
        }
        let cli_missing = matches!(failure, CredentialRefreshFailure::MissingDatabricksCli);
        assert_query_failure(
            &output,
            if cli_missing { 2 } else { 1 },
            failure.expected_error_code(),
        );
        let recorded = fixture.recorded_arguments();
        assert_eq!(recorded.iter().filter(|arg| *arg == "tables").count(), 1);
        assert_eq!(
            recorded
                .iter()
                .filter(|arg| *arg == "temporary-table-credentials")
                .count(),
            if cli_missing { 1 } else { 2 }
        );
        assert!(
            server
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|request| !request.contains("ambient-secret"))
        );
    }
    Ok(())
}

#[test]
fn invalid_uc_grants_or_mismatched_metadata_stop_sql_before_storage_access() -> TestResult {
    for (kind, failure, code) in [
        (
            StorageCredentialKind::S3,
            "denied",
            "uc_credential_request_failed",
        ),
        (
            StorageCredentialKind::Gcs,
            "expired",
            "uc_credentials_expired",
        ),
        (
            StorageCredentialKind::AzureBlobBearer,
            "malformed",
            "invalid_databricks_response",
        ),
        (
            StorageCredentialKind::S3,
            "table_changed",
            "invalid_databricks_response",
        ),
        (
            StorageCredentialKind::R2,
            "root_changed",
            "invalid_databricks_response",
        ),
    ] {
        let fixture = DatabricksCliFixture::new(kind)?;
        let server = MockCloudStorageServer::with_objects(
            kind.storage_protocol(),
            partitioned_objects()?,
            |_, _| {},
        )?;
        fixture.use_storage_endpoint(kind, &server.endpoint)?;
        let path = fixture.directory.path().join("credentials.json");
        let mut credentials: Value = serde_json::from_slice(&fs::read(&path)?)?;
        match failure {
            "expired" => credentials["expiration_time"] = json!(0),
            "root_changed" => {
                credentials["url"] =
                    json!("r2://other@account.r2.cloudflarestorage.com/secret-root/")
            }
            "table_changed" => {
                let path = fixture.directory.path().join("metadata.json");
                let mut metadata: Value = serde_json::from_slice(&fs::read(&path)?)?;
                metadata["full_name"] = json!("main.sales.secret_other_table");
                fs::write(path, metadata.to_string())?;
            }
            _ => {}
        }
        fs::write(
            path,
            if failure == "malformed" {
                "secret-invalid-json".to_owned()
            } else {
                credentials.to_string()
            },
        )?;
        let output = run_with_timeout(
            fixture
                .query("SELECT * FROM t")?
                .arg("--diagnostics=json")
                .env("RUST_LOG", "trace")
                .env(
                    "DAR_TEST_DENIED",
                    if failure == "denied" { "1" } else { "0" },
                ),
        )?;
        assert_query_failure(&output, 1, code);
        assert!(output.stdout.is_empty());
        assert!(server.requests.lock().unwrap().is_empty());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn sql_renewal_timeout_keeps_its_uc_error_and_reaps_the_databricks_child() -> TestResult {
    use std::{process::Stdio, time::Instant};

    let kind = StorageCredentialKind::S3;
    let fixture = DatabricksCliFixture::new(kind)?;
    let (_server, mut command) =
        query_with_credential_expiry_and_table_changes(&fixture, kind, |_| {})?;
    let mut child = command
        .env("DAR_TEST_REFRESH", "hang")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    // Exercise the production 30-second deadline; the ordinary fixture helper allows 15 seconds.
    let deadline = Instant::now() + Duration::from_secs(40);
    while child.try_wait()?.is_none() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    if child.try_wait()?.is_none() {
        child.kill()?;
        child.wait()?;
        return Err("SQL renewal did not respect the Databricks timeout".into());
    }
    let output = child.wait_with_output()?;
    assert_query_failure(&output, 1, "databricks_cli_timeout");
    let pid = fs::read_to_string(fixture.directory.path().join("pid"))?.parse::<u32>()?;
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "Databricks child survived its timeout"
    );
    Ok(())
}
