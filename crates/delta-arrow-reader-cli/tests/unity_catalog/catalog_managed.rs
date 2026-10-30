//! CLI reads use catalog-confirmed commits and the existing cloud credential providers.

use super::*;
use crate::{
    diagnostics::assert_single_terminal_record,
    mock_cloud_storage::{expected_partitioned_rows, partitioned_objects},
    query::json::assert_query_success_and_decode_json,
};

const STAGED_NAME: &str = "00000000000000000001.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json";

fn catalog_fixture(
    kind: StorageCredentialKind,
) -> TestResult<(DatabricksCliFixture, BTreeMap<String, Vec<u8>>)> {
    let fixture = DatabricksCliFixture::new(kind)?;
    let path = fixture.directory.path().join("metadata.json");
    let mut metadata: Value = serde_json::from_slice(&fs::read(&path)?)?;
    metadata["table_type"] = json!("MANAGED");
    metadata["properties"] = json!({"delta.feature.catalogManaged": "supported"});
    fs::write(path, serde_json::to_vec(&metadata)?)?;
    fs::write(
        fixture.directory.path().join("delta-api-config.json"),
        json!({
            "protocol-version": "1.0",
            "endpoints": ["GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}"]
        })
        .to_string(),
    )?;
    let mut objects = partitioned_objects()?;
    let initial_key = "table/_delta_log/00000000000000000000.json";
    let original = std::str::from_utf8(&objects[initial_key])?;
    let mut creation = String::new();
    let mut insertion = String::new();
    let mut removal = String::new();
    for line in original.lines() {
        let mut action: Value = serde_json::from_str(line)?;
        if action.get("add").is_some() {
            insertion.push_str(&format!("{action}\n"));
            removal.push_str(&format!(
                "{}\n",
                json!({"remove": {
                    "path": action["add"]["path"], "deletionTimestamp": 2, "dataChange": true,
                }})
            ));
            continue;
        }
        if action.get("protocol").is_some() {
            action = json!({"protocol": {
                "minReaderVersion": 3, "minWriterVersion": 7,
                "readerFeatures": ["catalogManaged", "vacuumProtocolCheck"],
                "writerFeatures": ["catalogManaged", "vacuumProtocolCheck", "inCommitTimestamp"]
            }});
        }
        if let Some(metadata) = action.get_mut("metaData") {
            metadata["configuration"]["delta.enableInCommitTimestamps"] = json!("true");
        }
        creation.push_str(&format!("{action}\n"));
    }
    fs::write(fixture.directory.path().join("catalog-snapshot.json"), json!({
        "metadata": {"table-uuid": TABLE_ID, "location": metadata["storage_location"],
                     "table-type": "MANAGED"},
        "latest-table-version": 1,
        "commits": [{"version": 1, "file-name": STAGED_NAME, "file-size": insertion.len(), "file-modification-timestamp": 1}]
    }).to_string())?;
    objects.insert(initial_key.into(), creation.into_bytes());
    objects.insert(
        format!("table/_delta_log/_staged_commits/{STAGED_NAME}"),
        insertion.into_bytes(),
    );
    objects.insert(
        "table/_delta_log/00000000000000000001.json".into(),
        removal.clone().into_bytes(),
    );
    objects.insert(
        "table/_delta_log/00000000000000000002.json".into(),
        removal.clone().into_bytes(),
    );
    objects.insert("table/_delta_log/_staged_commits/00000000000000000001.bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb.json".into(), removal.into_bytes());
    Ok((fixture, objects))
}

#[test]
fn catalog_managed_cli_reads_latest_and_pinned_snapshots() -> TestResult {
    for kind in [
        StorageCredentialKind::S3,
        StorageCredentialKind::AzureBlobSas,
        StorageCredentialKind::Gcs,
    ] {
        let (fixture, objects) = catalog_fixture(kind)?;
        let server =
            MockCloudStorageServer::with_objects(kind.storage_protocol(), objects, |_, _| {})?;
        fixture.use_storage_endpoint(kind, &server.endpoint)?;
        let output =
            run_with_timeout(
                fixture
                    .inspect()
                    .args(["--view", "summary", "--diagnostics=json"]),
            )?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let summary: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(summary["table_version"], "1");
        assert_eq!(summary["checks"]["scan_protocol"], "passed");
        for (version, expected) in [("0", json!([])), ("1", json!([["1"], ["2"]]))] {
            let output = run_with_timeout(
                fixture
                    .query("SELECT id FROM t WHERE id <= 2 ORDER BY id")?
                    .args([
                        "--format",
                        "json",
                        "--table-version",
                        version,
                        "--diagnostics=json",
                    ]),
            )?;
            let result = assert_query_success_and_decode_json(&output)?;
            assert_eq!(result["rows"], expected);
            assert_eq!(result["bindings"][0]["table_version"], version);
        }
        let output = run_with_timeout(fixture.table_command("preview").args([
            "--column",
            "id",
            "--limit",
            "3",
            "--diagnostics=json",
        ]))?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(result["row_count"], "3");
        assert_eq!(result["bindings"][0]["table_version"], "1");
        let output = run_with_timeout(fixture.query("SELECT COUNT(*) AS n FROM t")?.args([
            "--format",
            "json",
            "--diagnostics=json",
        ]))?;
        let result = assert_query_success_and_decode_json(&output)?;
        assert_eq!(
            result["rows"],
            json!([[expected_partitioned_rows()?.num_rows().to_string()]])
        );
        for text in [output.stdout, output.stderr] {
            assert!(!String::from_utf8_lossy(&text).contains("secret"));
        }
        let arguments = fixture.recorded_arguments();
        assert!(arguments.iter().any(|argument| argument
            == "/api/2.1/unity-catalog/delta/v1/config?catalog=main&protocol-versions=1.0"));
        assert!(arguments.iter().any(|argument| argument
            == "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/sales/tables/orders"));
    }
    Ok(())
}

#[test]
fn catalog_identity_and_protocol_failures_precede_storage_reads() -> TestResult {
    for (filename, pointer, invalid, expected_code) in [
        (
            "catalog-snapshot.json",
            "/metadata/table-uuid",
            json!("another-table"),
            "invalid_databricks_response",
        ),
        (
            "catalog-snapshot.json",
            "/metadata/location",
            json!("s3://other/secret-root"),
            "invalid_databricks_response",
        ),
        (
            "catalog-snapshot.json",
            "/metadata/table-type",
            json!("EXTERNAL"),
            "invalid_databricks_response",
        ),
        (
            "delta-api-config.json",
            "/protocol-version",
            json!("2.0"),
            "unsupported_uc_catalog_protocol",
        ),
        (
            "delta-api-config.json",
            "/endpoints",
            json!([]),
            "unsupported_uc_catalog_protocol",
        ),
        (
            "metadata.json",
            "/row_filter",
            json!({}),
            "unsupported_uc_table",
        ),
    ] {
        let (fixture, objects) = catalog_fixture(StorageCredentialKind::S3)?;
        let path = fixture.directory.path().join(filename);
        let mut response: Value = serde_json::from_slice(&fs::read(&path)?)?;
        if pointer == "/row_filter" {
            response["row_filter"] = invalid;
        } else {
            *response.pointer_mut(pointer).unwrap() = invalid;
        }
        fs::write(path, response.to_string())?;
        let server =
            MockCloudStorageServer::with_objects(CloudStorageProtocol::S3, objects, |_, _| {})?;
        fixture.use_storage_endpoint(StorageCredentialKind::S3, &server.endpoint)?;
        let output = run_with_timeout(fixture.inspect().arg("--diagnostics=json"))?;
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let error = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(error["code"], expected_code);
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
        assert!(server.requests.lock().unwrap().is_empty());
        assert!(
            !fixture
                .recorded_arguments()
                .iter()
                .any(|argument| argument == "temporary-table-credentials")
        );
    }
    Ok(())
}

#[test]
fn catalog_request_failures_preserve_filtered_databricks_diagnostics() -> TestResult {
    let (fixture, _) = catalog_fixture(StorageCredentialKind::S3)?;
    fs::write(
        fixture.directory.path().join("failure.txt"),
        super::failure_diagnostics::permission_error(),
    )?;
    let output = run_with_timeout(
        fixture
            .inspect()
            .arg("--diagnostics=json")
            .env("DAR_TEST_FAILURE_STAGE", "api"),
    )?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["code"], "uc_table_request_failed");
    assert_eq!(record["reason"], "uc_permission_denied");
    assert!(!String::from_utf8_lossy(&output.stderr).contains(PROFILE));
    Ok(())
}

#[test]
fn catalog_snapshot_stays_fixed_when_credentials_renew() -> TestResult {
    let kind = StorageCredentialKind::S3;
    let (fixture, objects) = catalog_fixture(kind)?;
    let expires_at =
        fixture.prepare_credential_renewal(|response| kind.rotate_credentials(response))?;
    let metadata_path = fixture.directory.path().join("metadata.json");
    let catalog_path = fixture.directory.path().join("catalog-snapshot.json");
    let mut first_request = true;
    let server =
        MockCloudStorageServer::with_objects(kind.storage_protocol(), objects, move |_, _| {
            if first_request {
                first_request = false;
                // A replacement table name must not change an already authorized snapshot.
                fs::write(&metadata_path, "table was replaced").unwrap();
                fs::write(&catalog_path, "catalog state changed").unwrap();
                thread::sleep(
                    expires_at
                        .duration_since(SystemTime::now())
                        .unwrap_or_default()
                        + Duration::from_millis(20),
                );
            }
        })?;
    fixture.use_storage_endpoint(kind, &server.endpoint)?;
    let output = run_with_timeout(
        fixture
            .query("SELECT COUNT(*) AS n FROM t")?
            .args(["--format", "json", "--diagnostics=json"])
            .env("DAR_TEST_REFRESH", "rotate"),
    )?;
    let result = assert_query_success_and_decode_json(&output)?;
    assert_eq!(result["bindings"][0]["table_version"], "1");
    assert_eq!(
        result["rows"],
        json!([[expected_partitioned_rows()?.num_rows().to_string()]])
    );
    assert_eq!(
        fixture
            .recorded_arguments()
            .iter()
            .filter(|arg| arg.as_str() == "api")
            .count(),
        2
    );
    assert!(server.requests.lock().unwrap().iter().any(|request| {
        request
            .to_ascii_lowercase()
            .contains(kind.expected_renewed_auth_fragment())
    }));
    Ok(())
}

#[test]
fn catalog_log_discovery_stops_at_the_command_deadline() -> TestResult {
    let (fixture, objects) = catalog_fixture(StorageCredentialKind::S3)?;
    let path = fixture.directory.path().join("catalog-snapshot.json");
    let mut response: Value = serde_json::from_slice(&fs::read(&path)?)?;
    response["latest-table-version"] = json!(1_000_000);
    response["commits"] = json!([]);
    fs::write(path, response.to_string())?;
    let server =
        MockCloudStorageServer::with_objects(CloudStorageProtocol::S3, objects, |_, _| {
            thread::sleep(Duration::from_millis(20));
        })?;
    fixture.use_storage_endpoint(StorageCredentialKind::S3, &server.endpoint)?;
    let output =
        run_with_timeout(
            fixture
                .inspect()
                .args(["--deadline-ms", "500", "--diagnostics=json"]),
        )?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["reason"], "command_deadline_exceeded");
    let request_count = server.requests.lock().unwrap().len();
    assert!(
        request_count > 0,
        "the deadline must interrupt storage discovery"
    );
    thread::sleep(Duration::from_millis(100));
    assert_eq!(server.requests.lock().unwrap().len(), request_count);
    Ok(())
}
