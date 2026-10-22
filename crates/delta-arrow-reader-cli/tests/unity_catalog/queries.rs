//! Local SQL over UC snapshots, with the existing Databricks and cloud fixtures.

use std::{
    fs, thread,
    time::{Duration, SystemTime},
};

use arrow::{
    array::{BooleanArray, Int32Array, StringArray},
    compute::{filter_record_batch, sort_to_indices, take_record_batch},
};
use serde_json::json;

use super::{
    DatabricksCliFixture, MockCloudStorageServer, StorageCredentialKind, TestResult,
    run_with_timeout,
};
use crate::{
    diagnostics::assert_single_terminal_record,
    mock_cloud_storage::{
        append_west_partition_removal_commit, expected_partitioned_rows, partitioned_objects,
    },
    query::decode_query_result,
};

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
            let actual = decode_query_result(&output)?;
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

#[test]
fn renewal_during_sql_keeps_the_snapshot_and_preserves_authorization_failures() -> TestResult {
    for denied in [false, true] {
        let kind = StorageCredentialKind::S3;
        let fixture = DatabricksCliFixture::new(kind)?;
        let expires_at = fixture
            .prepare_credential_renewal(|replacement| kind.rotate_credentials(replacement))?;
        let metadata = fixture.directory.path().join("metadata.json");
        let mut delayed = false;
        let server = MockCloudStorageServer::with_objects(
            kind.storage_protocol(),
            partitioned_objects()?,
            move |request, objects| {
                if !delayed && request.lines().next().unwrap().contains(".parquet") {
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
        let output = run_with_timeout(
            fixture
                .query("SELECT id, value FROM t ORDER BY id")?
                .arg("--diagnostics=json")
                .env("DAR_TEST_REFRESH", if denied { "deny" } else { "rotate" }),
        )?;
        if denied {
            assert_eq!(output.status.code(), Some(1));
            assert_eq!(
                assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
                "uc_credential_request_failed"
            );
            assert!(!output.stdout.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
        } else {
            let actual = decode_query_result(&output)?;
            let expected = expected_partitioned_rows()?.project(&[0, 1])?;
            let expected =
                take_record_batch(&expected, &sort_to_indices(expected.column(0), None, None)?)?;
            assert_eq!(actual.columns(), expected.columns());
            assert_eq!(
                assert_single_terminal_record(&output.stderr, "completion")["bindings"],
                json!([{"alias":"t", "table_version":"0"}])
            );
            assert!(server.requests.lock().unwrap().iter().any(|request| {
                request
                    .to_ascii_lowercase()
                    .contains(kind.expected_renewed_auth_fragment())
            }));
        }
        let recorded = fixture.recorded_arguments();
        assert_eq!(recorded.iter().filter(|arg| *arg == "tables").count(), 1);
        assert_eq!(
            recorded
                .iter()
                .filter(|arg| *arg == "temporary-table-credentials")
                .count(),
            2
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    Ok(())
}
