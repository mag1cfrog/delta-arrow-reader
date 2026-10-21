//! Multiple table declarations retain separate authorization and load references before storage IO.

use std::{fs, path::Path, process::Command};

use serde_json::{Value, json};

use super::{
    CloudStorageProtocol, DirectStorageFixture, TestResult, assert_failure_without_stdout,
    assert_read_result, assert_single_terminal_record, run_with_timeout,
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
            "--diagnostics=json",
        ]);
    Ok(command)
}

#[test]
fn each_manifest_table_uses_its_selected_identity_even_after_reference_files_change() -> TestResult
{
    for customers_source in ["json", "env"] {
        let directory = tempfile::tempdir()?;
        fs::create_dir(directory.path().join("authorization"))?;
        let orders_options = directory.path().join("authorization/orders.json");
        let customers_options = directory.path().join("authorization/customers.json");
        let mut orders = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
        let customers = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
        let replaced_files = [orders_options.clone(), customers_options.clone()];
        orders.restart_storage_with_request_hook(move |_| {
            for path in &replaced_files {
                fs::write(path, "secret-replaced-options").unwrap();
            }
        })?;
        orders.write_options(json!({
            "access_key_id":"secret-orders-key", "secret_access_key":"secret-orders-password",
            "session_token":"secret-orders-session", "region":"eu-west-1"
        }))?;
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
        let tables = json!([
            {"alias":"orders","location":"s3://bucket/table","authorization":{"source":"json","storage_options_file":"../authorization/orders.json"}},
            {"alias":"customers","location":"s3://bucket/table","authorization":{"source":customers_source,"storage_options_file":"../authorization/customers.json"}}
        ]);
        let output = run_with_timeout(
            manifest_command(directory.path(), tables)?
                .env("AWS_ACCESS_KEY_ID", "secret-customers-key")
                .env("AWS_SECRET_ACCESS_KEY", "secret-customers-password")
                .env("AWS_SESSION_TOKEN", "secret-customers-session"),
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
fn invalid_later_authorization_prevents_all_storage_reads() -> TestResult {
    let orders = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    orders.write_options(json!({"access_key_id":"secret-key","secret_access_key":"secret-password","region":"eu-west-1"}))?;
    let directory = tempfile::tempdir()?;
    fs::write(directory.path().join("incomplete.json"), "{}")?;
    fs::write(directory.path().join("duplicate.json"), json!({
        "access_key_id":"secret-one", "AWS_ACCESS_KEY_ID":"secret-two", "secret_access_key":"secret-password"
    }).to_string())?;
    for (authorization, code, reason) in [
        (
            json!({"source":"json"}),
            "invalid_storage_authorization",
            "auth_source_requires_storage_options_file",
        ),
        (
            json!({"source":"json","storage_options_file":"../secret-missing.json"}),
            "input_file_io",
            "input_file_read_failed",
        ),
        (
            json!({"source":"json","storage_options_file":"../incomplete.json"}),
            "invalid_storage_authorization",
            "auth_source_incomplete_credentials",
        ),
        (
            json!({"source":"json","storage_options_file":"../duplicate.json"}),
            "invalid_storage_authorization",
            "auth_source_duplicate_option",
        ),
        (
            json!({"source":"env"}),
            "invalid_storage_authorization",
            "auth_source_incomplete_credentials",
        ),
    ] {
        let tables = json!([
            {"alias":"orders","location":"s3://bucket/table","authorization":{"source":"json","storage_options_file":orders.directory.path().join("options.json")}},
            {"alias":"customers","location":"s3://bucket/table","authorization":authorization}
        ]);
        let output = run_with_timeout(&mut manifest_command(directory.path(), tables)?)?;
        assert_failure_without_stdout(&output, 2);
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], code);
        assert_eq!(record["reason"], reason);
        assert!(orders.server.requests.lock().unwrap().is_empty());
    }
    Ok(())
}
