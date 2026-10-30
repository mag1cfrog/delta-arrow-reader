//! Published pipeline snapshots use their catalog location and retain the original grant scope.

use super::*;
use crate::{
    diagnostics::assert_single_terminal_record,
    mock_cloud_storage::{expected_partitioned_rows, partitioned_objects, requested_object_path},
    query::{fixture_path, json::assert_query_success_and_decode_json},
};

fn assert_json_failure(stderr: &[u8], phase: &str, code: &str) {
    let record = assert_single_terminal_record(stderr, "diagnostic");
    assert_eq!(record["phase"], phase);
    assert_eq!(record["code"], code);
}

fn pipeline_fixture(
    table_type: &str,
    credentials: StorageCredentialKind,
) -> TestResult<(DatabricksCliFixture, BTreeMap<String, Vec<u8>>)> {
    let fixture = DatabricksCliFixture::new(credentials)?;
    let path = fixture.directory.path().join("metadata.json");
    let mut metadata: Value = serde_json::from_slice(&fs::read(&path)?)?;
    let data_root = metadata["storage_location"].as_str().unwrap().to_owned();
    let file_root = if credentials == StorageCredentialKind::R2 {
        "https://account.r2.cloudflarestorage.com/bucket/table/"
    } else {
        &data_root
    };
    metadata["table_type"] = json!(table_type);
    metadata["data_source_format"] = Value::Null;
    metadata["storage_location"] = Value::Null;
    metadata["pipeline_id"] = json!("pipeline-id");
    fs::write(path, metadata.to_string())?;
    fs::write(
        fixture.directory.path().join("external-delta.json"),
        json!({
            "table_id": TABLE_ID, "catalog_name": "main", "schema_name": "sales", "name": "orders",
            "table_type": "MANAGED", "data_source_format": "DELTA",
            "storage_location": format!("{data_root}_external_metadata/"),
        })
        .to_string(),
    )?;
    let mut objects = partitioned_objects()?;
    let log_key = "table/_delta_log/00000000000000000000.json";
    let mut published_log = String::new();
    for line in std::str::from_utf8(&objects[log_key])?.lines() {
        let mut action: Value = serde_json::from_str(line)?;
        if let Some(add) = action.get_mut("add") {
            add["path"] = json!(format!("{file_root}{}", add["path"].as_str().unwrap()));
        }
        published_log.push_str(&format!("{action}\n"));
    }
    objects.insert(
        "table/_external_metadata/_delta_log/00000000000000000000.json".into(),
        published_log.into_bytes(),
    );
    // Using the credential root as the snapshot root must fail, even when credentials work.
    objects.insert(
        log_key.into(),
        b"internal backing metadata must not be read\n".to_vec(),
    );
    Ok((fixture, objects))
}

#[test]
fn pipeline_datasets_read_published_snapshots_with_each_credential_kind() -> TestResult {
    for table_type in ["MATERIALIZED_VIEW", "STREAMING_TABLE"] {
        for kind in [
            StorageCredentialKind::S3,
            StorageCredentialKind::R2,
            StorageCredentialKind::AzureBlobSas,
            StorageCredentialKind::AzureBlobBearer,
            StorageCredentialKind::Gcs,
        ] {
            let (fixture, objects) = pipeline_fixture(table_type, kind)?;
            let server =
                MockCloudStorageServer::with_objects(kind.storage_protocol(), objects, |_, _| {})?;
            fixture.use_storage_endpoint(kind, &server.endpoint)?;
            for args in [
                vec!["--view", "summary"],
                vec![],
                vec!["--view", "summary", "--column", "id"],
            ] {
                let output =
                    run_with_timeout(fixture.inspect().args(args).arg("--diagnostics=json"))?;
                assert!(output.status.success(), "{table_type} {kind:?}: {output:?}");
                let result: Value = serde_json::from_slice(&output.stdout)?;
                assert_eq!(result["time_travel"], false);
                assert_eq!(result["table_version"], "0");
            }
            for backend in ["direct", "delta_kernel"] {
                let options = fixture.directory.path().join("execution.json");
                fs::write(&options, json!({"parquet_backend":backend}).to_string())?;
                let output = run_with_timeout(
                    fixture
                        .table_command("preview")
                        .args([
                            "--column",
                            "id",
                            "--limit",
                            "1000",
                            "--diagnostics=json",
                            "--execution-options-file",
                        ])
                        .arg(&options),
                )?;
                assert!(
                    output.status.success(),
                    "{table_type} {kind:?} {backend}: {output:?}"
                );
                let result: Value = serde_json::from_slice(&output.stdout)?;
                assert_eq!(
                    result["bindings"],
                    json!([{"alias":"t", "table_version":"0", "time_travel":false}])
                );
                let expected = expected_partitioned_rows()?;
                let mut expected_ids = expected
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec();
                let mut actual_ids = result["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|row| row[0].as_str().unwrap().parse::<i32>().unwrap())
                    .collect::<Vec<_>>();
                expected_ids.sort_unstable();
                actual_ids.sort_unstable();
                assert_eq!(actual_ids, expected_ids);
                assert_eq!(result["row_count"], expected.num_rows().to_string());
                assert_eq!(result["stop_reason"], "end_of_input");
                assert_eq!(result["has_more"], false);
            }
            let output = run_with_timeout(
                fixture
                    .query("SELECT id FROM t WHERE id <= 2 ORDER BY id")?
                    .args(["--format", "json", "--diagnostics=json"]),
            )?;
            let result = assert_query_success_and_decode_json(&output)?;
            assert_eq!(result["rows"], json!([["1"], ["2"]]));
            assert_eq!(result["bindings"][0]["time_travel"], false);
            for stream in [&output.stdout, &output.stderr] {
                assert!(!String::from_utf8_lossy(stream).contains("secret"));
            }
            let args = fixture.recorded_arguments();
            assert!(args.iter().any(|arg| arg == "/api/2.1/unity-catalog/tables/main.sales.orders?read_materialized_view_as_managed=true&read_streaming_table_as_managed=true"));
            assert!(server.requests.lock().unwrap().iter().any(|r| {
                r.to_ascii_lowercase()
                    .contains(kind.expected_initial_auth_fragment())
            }));
        }
    }
    Ok(())
}

#[test]
fn pipeline_datasets_support_exports_plans_and_mixed_table_bindings() -> TestResult {
    for table_type in ["MATERIALIZED_VIEW", "STREAMING_TABLE"] {
        let kind = StorageCredentialKind::S3;
        let (fixture, objects) = pipeline_fixture(table_type, kind)?;
        let server =
            MockCloudStorageServer::with_objects(kind.storage_protocol(), objects, |_, _| {})?;
        fixture.use_storage_endpoint(kind, &server.endpoint)?;
        let scan_path = fixture.directory.path().join("rows.arrow");
        let output = run_with_timeout(
            fixture
                .scan()
                .arg("--output")
                .arg(&scan_path)
                .arg("--diagnostics=json"),
        )?;
        assert!(output.status.success(), "{output:?}");
        let reader = arrow::ipc::reader::StreamReader::try_new(fs::File::open(&scan_path)?, None)?;
        let rows = reader
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(rows, expected_partitioned_rows()?.num_rows());
        let query_path = fixture.directory.path().join("rows.json");
        let output = run_with_timeout(
            fixture
                .query("SELECT id FROM t WHERE id <= 2 ORDER BY id")?
                .args(["--format", "json", "--diagnostics=json", "--output"])
                .arg(&query_path),
        )?;
        assert!(output.status.success(), "{output:?}");
        let saved: Value = serde_json::from_slice(&fs::read(query_path)?)?;
        assert_eq!(saved["rows"], json!([["1"], ["2"]]));
        assert_eq!(saved["bindings"][0]["time_travel"], false);
        let output = run_with_timeout(
            fixture
                .table_command("explain")
                .arg("--sql-file")
                .arg(fixture.directory.path().join("query.sql"))
                .arg("--diagnostics=json"),
        )?;
        assert!(output.status.success(), "{output:?}");
        let plan: Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(plan["bindings"][0]["time_travel"], false);
        let tables = json!([
            {"alias":"orders", "location":"uc://main.sales.orders", "authorization":{"source":"databricks", "databricks_profile":PROFILE, "storage_options_file":"../options.json"}},
            {"alias":"local_rows", "location":fixture_path("partitioned").join("table"), "table_version":"0"}
        ]);
        let output = run_with_timeout(fixture.manifest_query(tables, "SELECT o.id, l.id FROM orders o JOIN local_rows l ON o.id = l.id WHERE o.id <= 2 ORDER BY o.id")?.args(["--format", "json", "--diagnostics=json"]))?;
        let result = assert_query_success_and_decode_json(&output)?;
        assert_eq!(result["rows"], json!([["1", "1"], ["2", "2"]]));
        assert_eq!(
            result["bindings"],
            json!([{"alias":"orders", "table_version":"0", "time_travel":false}, {"alias":"local_rows", "table_version":"0"}])
        );
    }
    Ok(())
}

#[test]
fn pipeline_datasets_reject_versions_before_credentials_and_replay_before_publication() -> TestResult
{
    for table_type in ["MATERIALIZED_VIEW", "STREAMING_TABLE"] {
        let kind = StorageCredentialKind::S3;
        let (fixture, objects) = pipeline_fixture(table_type, kind)?;
        let server =
            MockCloudStorageServer::with_objects(kind.storage_protocol(), objects, |_, _| {})?;
        fixture.use_storage_endpoint(kind, &server.endpoint)?;
        for command in ["inspect", "preview", "scan", "query", "explain"] {
            fs::write(
                fixture.directory.path().join("query.sql"),
                "SELECT * FROM t",
            )?;
            let mut invocation = fixture.table_command(command);
            invocation.args(["--table-version", "0", "--diagnostics=json"]);
            if matches!(command, "query" | "explain") {
                invocation
                    .arg("--sql-file")
                    .arg(fixture.directory.path().join("query.sql"));
            }
            let output = run_with_timeout(&mut invocation)?;
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
            assert_json_failure(
                &output.stderr,
                "configuration",
                "uc_pipeline_time_travel_unsupported",
            );
        }
        let tables = json!([{ "alias":"orders", "location":"uc://main.sales.orders", "table_version":"0", "authorization":{"source":"databricks", "databricks_profile":PROFILE, "storage_options_file":"../options.json"}}]);
        let output = run_with_timeout(
            fixture
                .manifest_query(tables, "SELECT * FROM orders")?
                .arg("--diagnostics=json"),
        )?;
        assert_json_failure(
            &output.stderr,
            "configuration",
            "uc_pipeline_time_travel_unsupported",
        );
        assert!(server.requests.lock().unwrap().is_empty());
        assert!(
            !fixture
                .recorded_arguments()
                .iter()
                .any(|a| a == "temporary-table-credentials" || a == "api")
        );
        for command in ["scan", "query"] {
            let destination = fixture.directory.path().join(format!("{command}.arrow"));
            let mut invocation = fixture.table_command(command);
            invocation
                .args(["--diagnostics=json", "--manifest", "--output"])
                .arg(&destination);
            if command == "query" {
                invocation
                    .arg("--sql-file")
                    .arg(fixture.directory.path().join("query.sql"));
            }
            let output = run_with_timeout(&mut invocation)?;
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
            assert_json_failure(
                &output.stderr,
                "configuration",
                "uc_pipeline_replay_unsupported",
            );
            assert!(!destination.exists());
            assert!(!destination.with_extension("arrow.manifest.json").exists());
        }
        assert!(
            server
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|r| !requested_object_path(r).ends_with(".parquet"))
        );
        assert!(!fs::read_dir(fixture.directory.path())?.any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".dar-")
        }));
    }
    Ok(())
}

#[test]
fn pipeline_admission_failures_do_not_read_storage_or_expose_private_responses() -> TestResult {
    for (file, field, value, expected, grants_allowed) in [
        (
            "metadata.json",
            "row_filter",
            json!({"name":"secret-filter"}),
            "unsupported_uc_table",
            false,
        ),
        (
            "metadata.json",
            "columns",
            json!([{"mask":{"name":"secret-mask"}}]),
            "unsupported_uc_table",
            false,
        ),
        (
            "metadata.json",
            "properties",
            json!({"delta.feature.catalogManaged":"supported"}),
            "unsupported_uc_table",
            false,
        ),
        (
            "external-delta.json",
            "table_id",
            json!("12345678-1234-1234-1234-123456789abd"),
            "invalid_databricks_response",
            false,
        ),
        (
            "external-delta.json",
            "storage_location",
            Value::Null,
            "uc_pipeline_external_access_unavailable",
            false,
        ),
        (
            "credentials.json",
            "url",
            json!("s3://bucket/table-other/secret/"),
            "invalid_databricks_response",
            true,
        ),
    ] {
        let (fixture, objects) = pipeline_fixture("MATERIALIZED_VIEW", StorageCredentialKind::S3)?;
        let path = fixture.directory.path().join(file);
        let mut document: Value = serde_json::from_slice(&fs::read(&path)?)?;
        document[field] = value;
        fs::write(path, document.to_string())?;
        let server =
            MockCloudStorageServer::with_objects(CloudStorageProtocol::S3, objects, |_, _| {})?;
        fixture.use_storage_endpoint(StorageCredentialKind::S3, &server.endpoint)?;
        let output = run_with_timeout(fixture.inspect().arg("--diagnostics=json"))?;
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert_json_failure(&output.stderr, "execution", expected);
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
        assert!(server.requests.lock().unwrap().is_empty());
        assert_eq!(
            fixture
                .recorded_arguments()
                .iter()
                .any(|a| a == "temporary-table-credentials"),
            grants_allowed
        );
    }
    Ok(())
}

#[test]
fn published_snapshots_retain_protocol_admission_checks() -> TestResult {
    for table_type in ["MATERIALIZED_VIEW", "STREAMING_TABLE"] {
        for feature in ["madeUpFeature", "catalogManaged"] {
            let (fixture, mut objects) = pipeline_fixture(table_type, StorageCredentialKind::S3)?;
            let log = objects
                .get_mut("table/_external_metadata/_delta_log/00000000000000000000.json")
                .unwrap();
            let rewritten = std::str::from_utf8(log)?
                .lines()
                .map(|line| -> TestResult<String> {
                    let mut action: Value = serde_json::from_str(line)?;
                    if action.get("protocol").is_some() {
                        action["protocol"] = json!({"minReaderVersion":3, "minWriterVersion":7,
                        "readerFeatures":[feature], "writerFeatures":[feature]});
                    }
                    Ok(format!("{action}\n"))
                })
                .collect::<TestResult<Vec<_>>>()?
                .concat();
            *log = rewritten.into_bytes();
            let server =
                MockCloudStorageServer::with_objects(CloudStorageProtocol::S3, objects, |_, _| {})?;
            fixture.use_storage_endpoint(StorageCredentialKind::S3, &server.endpoint)?;
            let output = run_with_timeout(fixture.inspect().arg("--diagnostics=json"))?;
            assert!(!output.status.success(), "{table_type} {feature}");
            assert!(output.stdout.is_empty());
            assert_single_terminal_record(&output.stderr, "diagnostic");
            assert!(
                server
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|r| !requested_object_path(r).ends_with(".parquet"))
            );
        }
    }
    Ok(())
}

#[test]
fn pipeline_credential_renewal_keeps_the_grant_root_without_resolving_the_name_again() -> TestResult
{
    for change_scope in [false, true] {
        let kind = StorageCredentialKind::S3;
        let (fixture, objects) = pipeline_fixture("STREAMING_TABLE", kind)?;
        let expires_at = fixture.prepare_credential_renewal(|replacement| {
            kind.rotate_credentials(replacement);
            if change_scope {
                replacement["url"] = json!("s3://bucket/");
            }
        })?;
        let mut first = true;
        let metadata = fixture.directory.path().join("metadata.json");
        let published = fixture.directory.path().join("external-delta.json");
        let server =
            MockCloudStorageServer::with_objects(kind.storage_protocol(), objects, move |_, _| {
                if first {
                    first = false;
                    fs::write(&metadata, "secret-replaced-object").unwrap();
                    fs::write(&published, "secret-replaced-object").unwrap();
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
        if change_scope {
            assert!(!output.status.success());
            assert!(output.stdout.is_empty());
            assert_json_failure(&output.stderr, "execution", "invalid_databricks_response");
        } else {
            let result = assert_query_success_and_decode_json(&output)?;
            assert_eq!(
                result["rows"],
                json!([[expected_partitioned_rows()?.num_rows().to_string()]])
            );
            assert!(server.requests.lock().unwrap().iter().any(|r| {
                r.to_ascii_lowercase()
                    .contains(kind.expected_renewed_auth_fragment())
            }));
        }
        assert_eq!(
            fixture
                .recorded_arguments()
                .iter()
                .filter(|a| *a == "tables")
                .count(),
            1
        );
        assert_eq!(
            fixture
                .recorded_arguments()
                .iter()
                .filter(|a| *a == "api")
                .count(),
            1
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    Ok(())
}
