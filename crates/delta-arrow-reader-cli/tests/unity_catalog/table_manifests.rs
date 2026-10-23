//! Mixed SQL bindings keep their own profiles, credentials, and snapshot versions.

use std::{
    fs, thread,
    time::{Duration, SystemTime},
};

use arrow::{
    array::{BooleanArray, StringArray},
    compute::filter_record_batch,
};
use serde_json::{Value, json};

use super::{
    DatabricksCliFixture, MockCloudStorageServer, PROFILE, StorageCredentialKind, TABLE_ID,
    TestResult, run_with_timeout,
};
use crate::{
    diagnostics::assert_single_terminal_record,
    mock_cloud_storage::{
        CloudStorageProtocol, append_west_partition_removal_commit, expected_partitioned_rows,
        partitioned_objects, partitioned_objects_with_large_parquet_footers,
    },
    query::assert_query_success_and_decode_arrow,
    sort_by_id,
};

const SECOND_PROFILE: &str = "secret second profile $(do-not-run)";
const SECOND_TABLE_ID: &str = "87654321-4321-4321-4321-cba987654321";

#[test]
fn mixed_joins_isolate_profiles_and_capture_options_before_any_table_loads() -> TestResult {
    for kind in [
        StorageCredentialKind::S3,
        StorageCredentialKind::R2,
        StorageCredentialKind::AzureBlobSas,
        StorageCredentialKind::AzureBlobBearer,
        StorageCredentialKind::Gcs,
    ] {
        let orders = DatabricksCliFixture::new(kind)?;
        let customers = DatabricksCliFixture::new(kind)?;
        let metadata_path = customers.directory.path().join("metadata.json");
        let mut metadata: Value = serde_json::from_slice(&fs::read(&metadata_path)?)?;
        metadata["table_id"] = json!(SECOND_TABLE_ID);
        fs::write(metadata_path, metadata.to_string())?;
        let credentials_path = customers.directory.path().join("credentials.json");
        let mut credentials: Value = serde_json::from_slice(&fs::read(&credentials_path)?)?;
        kind.rotate_credentials(&mut credentials);
        fs::write(credentials_path, credentials.to_string())?;

        let objects = partitioned_objects()?;
        for (path, bytes) in &objects {
            let path = orders.directory.path().join(path);
            fs::create_dir_all(path.parent().unwrap())?;
            fs::write(path, bytes)?;
        }
        let replaced_files = [
            orders.directory.path().join("options.json"),
            customers.directory.path().join("options.json"),
            orders.directory.path().join("direct.json"),
        ];
        let orders_server = MockCloudStorageServer::with_objects(
            kind.storage_protocol(),
            objects.clone(),
            move |_, _| {
                for path in &replaced_files {
                    fs::write(path, "secret-replaced-options").unwrap();
                }
            },
        )?;
        let mut customers_objects = objects.clone();
        append_west_partition_removal_commit(&mut customers_objects);
        let customers_server = MockCloudStorageServer::with_objects(
            kind.storage_protocol(),
            customers_objects,
            |_, _| {},
        )?;
        let direct_server =
            MockCloudStorageServer::with_objects(CloudStorageProtocol::S3, objects, |_, _| {})?;
        orders.use_storage_endpoint(kind, &orders_server.endpoint)?;
        customers.use_storage_endpoint(kind, &customers_server.endpoint)?;
        fs::write(orders.directory.path().join("direct.json"), json!({
            "aws_endpoint":direct_server.endpoint, "aws_allow_http":"true", "aws_region":"eu-west-1",
            "aws_access_key_id":"secret-direct-key", "aws_secret_access_key":"secret-direct-password",
            "aws_session_token":"secret-direct-session"
        }).to_string())?;
        let tables = json!([
            {"alias":"orders", "location":"uc://main.sales.orders", "table_version":"0",
             "authorization":{"source":"databricks", "databricks_profile":PROFILE, "storage_options_file":"../options.json"}},
            {"alias":"customers", "location":"uc://main.sales.orders",
             "authorization":{"source":"databricks", "databricks_profile":SECOND_PROFILE,
                              "storage_options_file":customers.directory.path().join("options.json")}},
            {"alias":"local_rows", "location":"../table"},
            {"alias":"direct_rows", "location":"s3://bucket/table",
             "authorization":{"source":"json", "storage_options_file":"../direct.json"}}
        ]);
        let output = run_with_timeout(
            orders
                .manifest_query(
                    tables,
                    "SELECT o.id, o.value, c.value, l.value, d.value FROM orders o
             JOIN customers c ON o.id = c.id JOIN local_rows l ON o.id = l.id
             JOIN direct_rows d ON o.id = d.id ORDER BY o.id",
                )?
                .arg("--diagnostics=json")
                .env("DAR_TEST_SECOND_PROFILE", SECOND_PROFILE)
                .env("DAR_TEST_SECOND_PROFILE_DIR", customers.directory.path())
                .env("AWS_ACCESS_KEY_ID", "ambient-secret-key")
                .env("AZURE_STORAGE_TOKEN", "ambient-secret-token")
                .env(
                    "GOOGLE_APPLICATION_CREDENTIALS",
                    "ambient-secret-missing.json",
                ),
        )?;
        let actual = assert_query_success_and_decode_arrow(&output)?;
        let expected = expected_partitioned_rows()?;
        let regions = expected
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let expected = sort_by_id(&filter_record_batch(
            &expected,
            &BooleanArray::from_iter(regions.iter().map(|region| region != Some("west"))),
        )?)?;
        assert_eq!(actual.num_columns(), 5);
        assert_eq!(
            actual.column(0),
            expected.column_by_name("id").unwrap(),
            "{kind:?}"
        );
        for column in &actual.columns()[1..] {
            assert_eq!(
                column,
                expected.column_by_name("value").unwrap(),
                "{kind:?}"
            );
        }
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "completion")["bindings"],
            json!([
                {"alias":"orders", "table_version":"0"}, {"alias":"customers", "table_version":"1"},
                {"alias":"local_rows", "table_version":"0"}, {"alias":"direct_rows", "table_version":"0"}
            ])
        );
        for (fixture, profile, id) in [
            (&orders, PROFILE, TABLE_ID),
            (&customers, SECOND_PROFILE, SECOND_TABLE_ID),
        ] {
            let arguments = fixture.recorded_arguments();
            for command in ["tables", "temporary-table-credentials"] {
                assert_eq!(
                    arguments.iter().filter(|arg| *arg == command).count(),
                    1,
                    "{kind:?}: {command}"
                );
            }
            assert_eq!(
                arguments
                    .iter()
                    .filter(|arg| arg.starts_with("--profile="))
                    .collect::<Vec<_>>(),
                vec![&format!("--profile={profile}"); 2]
            );
            for (option, expected) in [("--table-id", id), ("--operation", "READ")] {
                assert_eq!(
                    arguments
                        .windows(2)
                        .filter(|pair| pair[0] == option)
                        .map(|pair| pair[1].as_str())
                        .collect::<Vec<_>>(),
                    vec![expected],
                    "{kind:?}: {option}"
                );
            }
        }
        for (server, auth) in [
            (&orders_server, kind.expected_initial_auth_fragment()),
            (&customers_server, kind.expected_renewed_auth_fragment()),
            (&direct_server, "credential=secret-direct-key/"),
        ] {
            let requests = server.requests.lock().unwrap();
            assert!(!requests.is_empty());
            for request in requests.iter() {
                assert!(
                    request.to_ascii_lowercase().contains(auth),
                    "{kind:?}: selected identity missing"
                );
                assert!(!request.contains("ambient-secret"));
            }
        }
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    }
    Ok(())
}

#[test]
fn invalid_later_uc_binding_prevents_databricks_and_storage_access() -> TestResult {
    let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
    let server = MockCloudStorageServer::with_objects(
        CloudStorageProtocol::S3,
        partitioned_objects()?,
        |_, _| {},
    )?;
    fixture.use_storage_endpoint(StorageCredentialKind::S3, &server.endpoint)?;
    fs::write(
        fixture.directory.path().join("override.json"),
        json!({"aws_access_key_id":"secret-override"}).to_string(),
    )?;
    fs::write(
        fixture.directory.path().join("invalid.json"),
        "secret-invalid-json",
    )?;
    for (location, authorization, code) in [
        (
            "uc://main.sales.orders",
            json!({"source":"databricks"}),
            "invalid_input_json",
        ),
        (
            "uc://main.sales.orders",
            json!({"source":"databricks","databricks_profile":null}),
            "invalid_input_json",
        ),
        (
            "uc://main.sales.orders",
            json!({"source":"databricks","databricks_profile":" "}),
            "invalid_uc_reference",
        ),
        (
            "uc://main.sales.orders",
            json!({"source":"databricks","databricks_profile":"secret\nprofile"}),
            "invalid_uc_reference",
        ),
        (
            "uc://main.sales.orders",
            json!({"source":"env","databricks_profile":PROFILE}),
            "invalid_input_json",
        ),
        (
            "uc://main.sales.orders",
            json!({"source":"env"}),
            "invalid_uc_reference",
        ),
        (
            "uc://secret-invalid-name",
            json!({"source":"databricks","databricks_profile":PROFILE}),
            "invalid_uc_reference",
        ),
        (
            "s3://bucket/table",
            json!({"source":"databricks","databricks_profile":PROFILE}),
            "invalid_uc_reference",
        ),
        (
            "uc://main.sales.orders",
            json!({"source":"databricks","databricks_profile":PROFILE,"storage_options_file":"../override.json"}),
            "invalid_uc_storage_options",
        ),
        (
            "uc://main.sales.orders",
            json!({"source":"databricks","databricks_profile":PROFILE,"storage_options_file":"../invalid.json"}),
            "invalid_input_json",
        ),
    ] {
        let tables = json!([
            {"alias":"orders", "location":"uc://main.sales.orders",
             "authorization":{"source":"databricks","databricks_profile":PROFILE,"storage_options_file":"../options.json"}},
            {"alias":"direct_rows", "location":"s3://bucket/table"},
            {"alias":"customers", "location":location, "authorization":authorization}
        ]);
        let output = run_with_timeout(
            fixture
                .manifest_query(tables, "SELECT * FROM orders")?
                .arg("--diagnostics=json"),
        )?;
        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert!(output.stdout.is_empty());
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
            code
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
        assert!(fixture.recorded_arguments().is_empty());
        assert!(server.requests.lock().unwrap().is_empty());
    }
    Ok(())
}

#[test]
fn renewal_failure_in_the_second_uc_binding_keeps_its_diagnostic() -> TestResult {
    let kind = StorageCredentialKind::S3;
    let orders = DatabricksCliFixture::new(kind)?;
    let customers = DatabricksCliFixture::new(kind)?;
    let expires_at = customers.prepare_credential_renewal(|_| {})?;
    let mut delayed = false;
    let customers_server = MockCloudStorageServer::with_objects(
        kind.storage_protocol(),
        partitioned_objects_with_large_parquet_footers()?,
        move |request, _| {
            if !delayed && request.lines().next().unwrap().contains(".parquet") {
                delayed = true;
                thread::sleep(
                    expires_at
                        .duration_since(SystemTime::now())
                        .unwrap_or_default()
                        + Duration::from_millis(20),
                );
            }
        },
    )?;
    let orders_server = MockCloudStorageServer::with_objects(
        kind.storage_protocol(),
        partitioned_objects()?,
        |_, _| {},
    )?;
    orders.use_storage_endpoint(kind, &orders_server.endpoint)?;
    customers.use_storage_endpoint(kind, &customers_server.endpoint)?;
    let tables = json!([
        {"alias":"orders", "location":"uc://main.sales.orders",
         "authorization":{"source":"databricks","databricks_profile":PROFILE,"storage_options_file":"../options.json"}},
        {"alias":"customers", "location":"uc://main.sales.orders",
         "authorization":{"source":"databricks","databricks_profile":SECOND_PROFILE,
                          "storage_options_file":customers.directory.path().join("options.json")}}
    ]);
    let output = run_with_timeout(
        orders
            .manifest_query(
                tables,
                "SELECT o.id, c.value FROM orders o JOIN customers c ON o.id = c.id ORDER BY o.id",
            )?
            .arg("--diagnostics=json")
            .env("DAR_TEST_SECOND_PROFILE", SECOND_PROFILE)
            .env("DAR_TEST_SECOND_PROFILE_DIR", customers.directory.path())
            .env("DAR_TEST_REFRESH", "deny"),
    )?;
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
        "uc_credential_request_failed"
    );
    assert!(!output.stdout.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
    for (fixture, requests) in [(&orders, 1), (&customers, 2)] {
        let arguments = fixture.recorded_arguments();
        assert_eq!(arguments.iter().filter(|arg| *arg == "tables").count(), 1);
        assert_eq!(
            arguments
                .iter()
                .filter(|arg| *arg == "temporary-table-credentials")
                .count(),
            requests
        );
    }
    assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    Ok(())
}
