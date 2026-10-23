//! Named snapshots retain separate authorization and fixed versions throughout a query.

use std::{fs, path::Path, process::Command};

use arrow::{
    array::{BooleanArray, StringArray},
    compute::filter_record_batch,
};
use serde_json::{Value, json};

use super::{
    CloudStorageProtocol, DirectStorageFixture, MockCloudStorageServer, TestResult,
    assert_failure_without_stdout, assert_read_result, assert_single_terminal_record,
    mock_credential_proxy::{CredentialService, MockCredentialProxy},
    run_with_timeout,
    s3::verify_signed_session_generations,
};
use crate::{
    mock_cloud_storage::{
        append_west_partition_removal_commit, expected_partitioned_rows, partitioned_objects,
    },
    query::assert_query_success_and_decode_arrow,
    sort_by_id,
};

fn manifest_command(
    directory: &Path,
    tables: Value,
) -> Result<Command, Box<dyn std::error::Error>> {
    fs::create_dir_all(directory.join("manifests"))?;
    fs::write(
        directory.join("manifests/tables.json"),
        json!({"format_version":1,"tables":tables}).to_string(),
    )?;
    fs::write(
        directory.join("query.sql"),
        "SELECT o.id FROM orders o JOIN customers c ON o.id = c.id ORDER BY o.id",
    )?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
    command
        .env_clear()
        .env("RUST_LOG", "trace")
        .current_dir(directory)
        .args([
            "query",
            "--tables-file",
            "manifests/tables.json",
            "--sql-file",
            "query.sql",
        ]);
    Ok(command)
}

#[test]
fn each_manifest_table_uses_its_selected_identity_even_after_reference_files_change() -> TestResult
{
    for (orders_source, customers_source) in [
        ("json", "json"),
        ("json", "env"),
        ("json", "env-file"),
        ("env-file", "env-file"),
    ] {
        let directory = tempfile::tempdir()?;
        fs::create_dir(directory.path().join("authorization"))?;
        let orders_options = directory.path().join("authorization/orders.json");
        let customers_options = directory.path().join("authorization/customers.json");
        let orders_env_file = directory.path().join("authorization/orders.env");
        let customers_env_file = directory.path().join("authorization/customers.env");
        for (path, identity) in [
            (&orders_env_file, "orders"),
            (&customers_env_file, "customers"),
        ] {
            fs::write(
                path,
                format!(
                    "AWS_ACCESS_KEY_ID=secret-{identity}-key\nAWS_SECRET_ACCESS_KEY=secret-{identity}-password\nAWS_SESSION_TOKEN=secret-{identity}-session"
                ),
            )?;
        }
        let mut orders = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
        let customers = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
        let replaced_files = [
            orders_options.clone(),
            customers_options.clone(),
            orders_env_file,
            customers_env_file,
        ];
        orders.restart_storage_with_request_hook(move |_| {
            for path in &replaced_files {
                fs::write(path, "secret-replaced-options").unwrap();
            }
        })?;
        orders.write_options(if orders_source == "json" {
            json!({
                "access_key_id":"secret-orders-key", "secret_access_key":"secret-orders-password",
                "session_token":"secret-orders-session", "region":"eu-west-1"
            })
        } else {
            json!({"region":"eu-west-1"})
        })?;
        customers.write_options(if customers_source == "json" { json!({
            "access_key_id":"secret-customers-key", "secret_access_key":"secret-customers-password",
            "session_token":"secret-customers-session", "region":"eu-west-1"
        }) } else { json!({"region":"eu-west-1"}) })?;
        fs::copy(
            orders.directory.path().join("options.json"),
            &orders_options,
        )?;
        fs::copy(
            customers.directory.path().join("options.json"),
            &customers_options,
        )?;
        let mut tables = json!([
            {"alias":"orders","location":"s3://bucket/table","authorization":{"source":orders_source,"storage_options_file":"../authorization/orders.json"}},
            {"alias":"customers","location":"s3://bucket/table","authorization":{"source":customers_source,"storage_options_file":"../authorization/customers.json"}}
        ]);
        for (index, source, alias) in [
            (0, orders_source, "orders"),
            (1, customers_source, "customers"),
        ] {
            if source == "env-file" {
                tables[index]["authorization"]["env_file"] =
                    json!(format!("../authorization/{alias}.env"));
            }
        }
        let inherited_identity = if customers_source == "env" {
            "customers"
        } else {
            "unused"
        };
        let output = run_with_timeout(
            manifest_command(directory.path(), tables)?
                .arg("--diagnostics=json")
                .env(
                    "AWS_ACCESS_KEY_ID",
                    format!("secret-{inherited_identity}-key"),
                )
                .env(
                    "AWS_SECRET_ACCESS_KEY",
                    format!("secret-{inherited_identity}-password"),
                )
                .env(
                    "AWS_SESSION_TOKEN",
                    format!("secret-{inherited_identity}-session"),
                ),
        )?;
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "completion")["bindings"],
            json!([
                {"alias":"orders","table_version":"0"}, {"alias":"customers","table_version":"0"}
            ])
        );
        assert_read_result(output, "query")?;
        orders.assert_signed_s3_requests(
            "secret-orders-key",
            "secret-orders-password",
            Some("secret-orders-session"),
            "eu-west-1",
        )?;
        customers.assert_signed_s3_requests(
            "secret-customers-key",
            "secret-customers-password",
            Some("secret-customers-session"),
            "eu-west-1",
        )?;
    }
    Ok(())
}

#[test]
fn joins_keep_loaded_snapshots_when_tables_change_and_credentials_refresh() -> TestResult {
    let query_directory = tempfile::tempdir()?;
    let sts_proxy = MockCredentialProxy::new(CredentialService::AwsSts, "refresh")?;
    let mut orders = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    let mut customers = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    for fixture in [&mut orders, &mut customers] {
        let mut new_version_published = false;
        fixture.server = MockCloudStorageServer::with_objects(
            CloudStorageProtocol::S3,
            partitioned_objects()?,
            move |request, objects| {
                // Parquet reads start after both snapshots have loaded, without timing assumptions.
                if !new_version_published && request.lines().next().unwrap().contains(".parquet ") {
                    append_west_partition_removal_commit(objects);
                    new_version_published = true;
                }
            },
        )?;
    }
    fs::write(
        orders.directory.path().join("assertion.jwt"),
        "secret-assertion",
    )?;
    let role_arn = "arn:aws:iam::123456789012:role/secret-orders-role";
    orders.write_options(sts_proxy.add_proxy_options(json!({
        "role_arn":role_arn, "web_identity_token_file":"assertion.jwt", "region":"eu-west-1"
    })))?;
    customers.write_options(json!({
        "access_key_id":"secret-customers-key", "secret_access_key":"secret-customers-password",
        "session_token":"secret-customers-session", "region":"eu-west-1"
    }))?;
    let tables = json!([
        {"alias":"orders","location":"s3://bucket/table","authorization":{"source":"aws-web-identity","storage_options_file":orders.directory.path().join("options.json")}},
        {"alias":"customers","location":"s3://bucket/table","authorization":{"source":"json","storage_options_file":customers.directory.path().join("options.json")}}
    ]);
    let mut command = manifest_command(query_directory.path(), tables)?;
    command.arg("--diagnostics=json");
    let output = run_with_timeout(&mut command)?;
    assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "completion")["bindings"],
        json!([
            {"alias":"orders","table_version":"0"}, {"alias":"customers","table_version":"0"}
        ])
    );
    assert_read_result(output, "query")?;

    let credential_generations = verify_signed_session_generations(&orders, "sts")?;
    let storage_requests = orders.server.requests.lock().unwrap();
    let first_parquet_request_index = storage_requests
        .iter()
        .position(|request| request.lines().next().unwrap().contains(".parquet "))
        .unwrap();
    assert!(
        credential_generations[first_parquet_request_index + 1..]
            .iter()
            .any(|generation| *generation > credential_generations[first_parquet_request_index])
    );
    drop(storage_requests);
    for request in sts_proxy.requests()? {
        assert_eq!(request["parameters"]["RoleArn"], json!([role_arn]));
        assert_eq!(
            request["parameters"]["WebIdentityToken"],
            json!(["secret-assertion"])
        );
    }

    // A fresh query must see both new versions, proving the updates were visible and changed data.
    let output = run_with_timeout(&mut command)?;
    let actual = assert_query_success_and_decode_arrow(&output)?;
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "completion")["bindings"],
        json!([
            {"alias":"orders","table_version":"1"}, {"alias":"customers","table_version":"1"}
        ])
    );
    let original_rows = expected_partitioned_rows()?;
    let regions = original_rows
        .column_by_name("region")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let mask = BooleanArray::from_iter(regions.iter().map(|region| region != Some("west")));
    let expected_current_rows = sort_by_id(&filter_record_batch(&original_rows, &mask)?)?;
    assert!(expected_current_rows.num_rows() < original_rows.num_rows());
    assert_eq!(actual.num_columns(), 1);
    assert_eq!(
        actual.column(0),
        expected_current_rows.column_by_name("id").unwrap()
    );
    customers.assert_signed_s3_requests(
        "secret-customers-key",
        "secret-customers-password",
        Some("secret-customers-session"),
        "eu-west-1",
    )?;
    Ok(())
}

#[test]
fn invalid_later_authorization_prevents_all_storage_reads() -> TestResult {
    let orders = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    orders.write_options(json!({"access_key_id":"secret-key","secret_access_key":"secret-password","region":"eu-west-1"}))?;
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("incomplete.json"), "{}")?;
    fs::write(directory.path().join("duplicate.json"), json!({
        "access_key_id":"secret-one", "AWS_ACCESS_KEY_ID":"secret-two", "secret_access_key":"secret-password"
    }).to_string())?;
    fs::write(
        directory.path().join("incomplete.env"),
        "AWS_ACCESS_KEY_ID=secret-key",
    )?;
    fs::write(
        directory.path().join("malformed.env"),
        "# comment\nAWS_ACCESS_KEY_ID='secret-key' trailing",
    )?;
    fs::write(
        directory.path().join("malformed.json"),
        "{\n\"secret-key\":",
    )?;
    for (authorization, code, reason, field_path) in [
        (
            json!({"source":"json"}),
            "invalid_storage_authorization",
            "auth_source_requires_storage_options_file",
            "/tables/1/authorization",
        ),
        (
            json!({"source":"json","storage_options_file":"../secret-missing.json"}),
            "input_file_io",
            "input_file_read_failed",
            "/tables/1/authorization/storage_options_file",
        ),
        (
            json!({"source":"env-file","env_file":"../incomplete.env","storage_options_file":"../malformed.json"}),
            "invalid_input_json",
            "input_json_decode_failed",
            "/tables/1/authorization/storage_options_file",
        ),
        (
            json!({"source":"json","storage_options_file":"../incomplete.json"}),
            "invalid_storage_authorization",
            "auth_source_incomplete_credentials",
            "/tables/1/authorization",
        ),
        (
            json!({"source":"json","storage_options_file":"../duplicate.json"}),
            "invalid_storage_authorization",
            "auth_source_duplicate_option",
            "/tables/1/authorization",
        ),
        (
            json!({"source":"env"}),
            "invalid_storage_authorization",
            "auth_source_incomplete_credentials",
            "/tables/1/authorization",
        ),
        (
            json!({"source":"env-file"}),
            "invalid_input_json",
            "missing_env_file",
            "/tables/1/authorization/env_file",
        ),
        (
            json!({"source":"env-file","env_file":"../secret-missing.env"}),
            "input_file_io",
            "input_file_read_failed",
            "/tables/1/authorization/env_file",
        ),
        (
            json!({"source":"env-file","env_file":"../incomplete.env"}),
            "invalid_storage_authorization",
            "auth_source_incomplete_credentials",
            "/tables/1/authorization",
        ),
        (
            json!({"source":"env-file","env_file":"../malformed.env"}),
            "invalid_input_env",
            "env_file_invalid_record",
            "/tables/1/authorization/env_file",
        ),
        (
            json!({"source":"env-file","env_file":"../incomplete.env", "storage_options_file":orders.directory.path().join("options.json")}),
            "invalid_storage_authorization",
            "auth_source_conflicting_credentials",
            "/tables/1/authorization",
        ),
    ] {
        let tables = json!([
            {"alias":"orders","location":"s3://bucket/table","authorization":{"source":"json","storage_options_file":orders.directory.path().join("options.json")}},
            {"alias":"customers","location":"s3://bucket/table","authorization":authorization}
        ]);
        for machine in [false, true] {
            let mut command = manifest_command(directory.path(), tables.clone())?;
            if machine {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut command)?;
            assert_failure_without_stdout(&output, 2);
            let text = String::from_utf8_lossy(&output.stderr);
            if machine {
                let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                assert_eq!(record["code"], code);
                assert_eq!(record["reason"], reason);
                assert_eq!(record["argument"], "--tables-file");
                assert_eq!(record["field_path"], field_path);
                if code == "invalid_storage_authorization" {
                    assert!(record.get("expected").is_none());
                    assert!(record.get("hint").is_none());
                }
                if matches!(
                    reason,
                    "env_file_invalid_record" | "input_json_decode_failed"
                ) {
                    assert_eq!(record["line"], 2);
                }
                if code == "input_file_io" {
                    assert_eq!(record["io_kind"], "NotFound");
                    assert_eq!(record["source_kind"], "not_found");
                }
                if reason == "input_json_decode_failed" {
                    assert_eq!(record["json_kind"], "Eof");
                    assert_eq!(record["column"], 13);
                }
            } else {
                assert!(text.contains(&format!("error_code=\"{code}\"")), "{text}");
                assert!(
                    text.contains(&format!("error_reason=\"{reason}\"")),
                    "{text}"
                );
                assert!(text.contains("argument=\"--tables-file\""), "{text}");
                assert!(
                    text.contains(&format!("field_path=\"{field_path}\"")),
                    "{text}"
                );
                if reason == "env_file_invalid_record" {
                    assert!(text.contains("env_file_line=2"), "{text}");
                }
                if reason == "input_json_decode_failed" {
                    assert!(text.contains("json_line=2"), "{text}");
                }
                if code == "input_file_io" {
                    assert!(text.contains("io_kind=NotFound"), "{text}");
                }
            }
            if code == "input_file_io" {
                assert!(text.contains("manifest directory"), "{text}");
                assert!(!text.contains("working directory"), "{text}");
            }
            assert!(orders.server.requests.lock().unwrap().is_empty());
        }
    }
    Ok(())
}
