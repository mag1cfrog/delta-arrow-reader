//! Workload identity fixes the service principal while rereading rotated assertions.

use std::{
    collections::{HashMap, HashSet},
    fs,
    io::Cursor,
    process::Command,
};

use arrow::{array::Int32Array, compute::concat_batches, ipc::reader::StreamReader};
use serde_json::{Value, json};
use url::form_urlencoded;

use super::{
    ACCOUNT_KEY, BEARER_TOKEN, CloudStorageProtocol, DirectStorageFixture, TestResult,
    assert_authorization_input_error, assert_failure_without_stdout, assert_read_result,
    assert_single_terminal_record, mock_token_server::MockAzureTokenServer, run_with_timeout,
};
use crate::mock_cloud_storage::expected_partitioned_rows;

const INITIAL_ASSERTION: &str = "secret-initial-assertion+&=";
const ROTATED_ASSERTION: &str = "secret-rotated-assertion+&=";

fn workload_identity_options(
    fixture: &DirectStorageFixture,
    authority: &MockAzureTokenServer,
) -> Value {
    json!({
        "CLIENT_ID":"secret-client-id",
        "authority_id":"secret-tenant-id",
        "federated_token_file":fixture.directory.path().join("assertion.jwt"),
        "authority_host":authority.endpoint,
        "account_name":"account"
    })
}

fn workload_identity_command(
    fixture: &DirectStorageFixture,
    authority: &MockAzureTokenServer,
    operation: &str,
) -> Command {
    let mut command = fixture.command(operation);
    command
        .args(["--auth-source", "azure-workload-identity"])
        .env("AZURE_CLIENT_ID", "secret-client-id")
        .env("AZURE_TENANT_ID", "secret-tenant-id")
        .env(
            "AZURE_FEDERATED_TOKEN_FILE",
            fixture.directory.path().join("assertion.jwt"),
        )
        .env("AZURE_AUTHORITY_HOST", &authority.endpoint)
        .env("AZURE_STORAGE_ACCOUNT_NAME", "account")
        .env("AZURE_STORAGE_TOKEN", BEARER_TOKEN)
        .env("AZURE_STORAGE_ACCOUNT_KEY", ACCOUNT_KEY)
        .env("AZURE_CLIENT_SECRET", "secret-unused-client-password")
        .env("AZURE_USE_AZURE_CLI", "true")
        .env(
            "AZURE_MSI_ENDPOINT",
            format!("{}/secret-unused-provider", fixture.server.endpoint),
        );
    command
}

fn assert_workload_requests(requests: &[String]) -> Vec<String> {
    assert!(!requests.is_empty());
    requests
        .iter()
        .map(|request| {
            assert!(
                request.starts_with("POST /secret-tenant-id/oauth2/v2.0/token HTTP/1.1\r\n"),
                "{request}"
            );
            let (_, body) = request.split_once("\r\n\r\n").unwrap();
            let mut fields: HashMap<String, String> = form_urlencoded::parse(body.as_bytes())
                .into_owned()
                .collect();
            let assertion = fields.remove("client_assertion").unwrap();
            assert_eq!(
                fields,
                HashMap::from([
                    ("client_id".into(), "secret-client-id".into()),
                    (
                        "client_assertion_type".into(),
                        "urn:ietf:params:oauth:client-assertion-type:jwt-bearer".into()
                    ),
                    ("scope".into(), "https://storage.azure.com/.default".into()),
                    ("grant_type".into(), "client_credentials".into()),
                ])
            );
            assertion
        })
        .collect()
}

#[test]
fn all_reads_use_the_selected_principal_and_rotate_the_assertion() -> TestResult {
    for references in ["json", "env", "mixed"] {
        for operation in ["inspect", "scan", "preview", "query"] {
            let authority = MockAzureTokenServer::new(usize::MAX)?;
            let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
            let assertion_path = fixture.directory.path().join("assertion.jwt");
            fs::write(&assertion_path, INITIAL_ASSERTION)?;
            let options_path = fixture.directory.path().join("options.json");
            fixture.restart_storage_with_request_hook(move |_| {
                // Replace the projected token atomically, as a workload issuer would.
                let replacement = assertion_path.with_extension("replacement");
                fs::write(&replacement, ROTATED_ASSERTION).unwrap();
                fs::rename(replacement, &assertion_path).unwrap();
                // Only the assertion is reread; the selected principal and path stay fixed.
                fs::write(&options_path, "secret-replaced-options").unwrap();
            })?;
            let options = match references {
                "json" => workload_identity_options(&fixture, &authority),
                "mixed" => json!({"CLIENT_ID":"secret-client-id"}),
                _ => json!({}),
            };
            fixture.write_options(options)?;
            let mut command = workload_identity_command(&fixture, &authority, operation);
            command.arg("--diagnostics=json");
            if references != "env" {
                command.env("AZURE_CLIENT_ID", "secret-unused-client");
            }
            if references == "json" {
                command
                    .env("AZURE_TENANT_ID", "secret-unused-tenant")
                    .env("AZURE_FEDERATED_TOKEN_FILE", "secret-unused-file")
                    .env("AZURE_AUTHORITY_HOST", "http://secret-unused-authority")
                    .env("AZURE_STORAGE_ACCOUNT_NAME", "secret-unused-account");
            }
            let output = run_with_timeout(&mut command)?;
            assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
            assert_read_result(output, operation)?;
            let tokens = fixture.storage_bearer_tokens();
            assert_eq!(tokens[0], "secret-oauth-1");
            assert!(
                tokens
                    .iter()
                    .all(|token| token.starts_with("secret-oauth-"))
            );
            assert!(tokens.iter().any(|token| token != "secret-oauth-1"));
            let assertions = assert_workload_requests(&authority.requests.lock().unwrap());
            assert_eq!(assertions[0], INITIAL_ASSERTION);
            assert!(
                assertions[1..]
                    .iter()
                    .all(|assertion| assertion == ROTATED_ASSERTION)
            );
        }
    }
    Ok(())
}

#[test]
fn missing_or_conflicting_references_fail_before_io() -> TestResult {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let authority = MockAzureTokenServer::new(usize::MAX)?;
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
    fixture.write_options(json!({}))?;
    for variable in [
        "AZURE_CLIENT_ID",
        "AZURE_TENANT_ID",
        "AZURE_FEDERATED_TOKEN_FILE",
    ] {
        for (value, reason) in [
            (None, "auth_source_incomplete_credentials"),
            (Some(OsString::new()), "auth_source_invalid_value"),
            (
                Some(OsString::from_vec(vec![0xff])),
                "auth_source_non_utf8_environment",
            ),
        ] {
            let mut command = workload_identity_command(&fixture, &authority, "inspect");
            command.arg("--diagnostics=json").env_remove(variable);
            if let Some(value) = value {
                command.env(variable, value);
            }
            let output = run_with_timeout(&mut command)?;
            assert_authorization_input_error(&output, reason);
        }
    }
    for (extra, reason) in [
        (
            json!({"account_key":ACCOUNT_KEY}),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({"sas_key":"sig=secret-sas"}),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({"token":BEARER_TOKEN}),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({"client_secret":"secret-password"}),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({"azure_client_id":"secret-other-client"}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"azure_federated_token_file":"secret-other-file"}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"msi_endpoint":"http://secret-unused-provider"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"use_azure_cli":"true"}),
            "auth_source_unsupported_option",
        ),
        (json!({"CLIENT_ID":""}), "auth_source_invalid_value"),
        (
            json!({"federated_token_file":"secret-\npath"}),
            "auth_source_invalid_value",
        ),
    ] {
        let mut options = workload_identity_options(&fixture, &authority);
        options
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        fixture.write_options(options)?;
        let output = run_with_timeout(
            workload_identity_command(&fixture, &authority, "inspect").arg("--diagnostics=json"),
        )?;
        assert_authorization_input_error(&output, reason);
        assert!(!String::from_utf8_lossy(&output.stderr).contains(ACCOUNT_KEY));
    }
    assert!(authority.requests.lock().unwrap().is_empty());
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn acquisition_refresh_and_storage_failures_never_select_ambient_credentials() -> TestResult {
    for successful_tokens in [0, 1, usize::MAX] {
        for machine in [false, true] {
            let authority = MockAzureTokenServer::new(successful_tokens)?;
            let fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
            fixture.write_options(json!({}))?;
            fs::write(
                fixture.directory.path().join("assertion.jwt"),
                INITIAL_ASSERTION,
            )?;
            if successful_tokens == usize::MAX {
                fixture.server.deny_all_requests();
            }
            let mut command = workload_identity_command(&fixture, &authority, "inspect");
            if machine {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut command)?;
            assert_failure_without_stdout(&output, 1);
            if machine {
                let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                if successful_tokens == usize::MAX {
                    assert_eq!(record["source_kind"], "permission_denied");
                }
            }
            let requests = authority.requests.lock().unwrap();
            assert!(
                assert_workload_requests(&requests)
                    .iter()
                    .all(|assertion| assertion == INITIAL_ASSERTION)
            );
            let tokens = fixture.storage_bearer_tokens();
            match successful_tokens {
                0 => {
                    assert_eq!(requests.len(), 1);
                    assert!(tokens.is_empty());
                }
                1 => {
                    assert_eq!(requests.len(), 2);
                    assert_eq!(tokens, ["secret-oauth-1"]);
                }
                _ => {
                    assert!(!tokens.is_empty());
                    assert!(
                        tokens
                            .iter()
                            .all(|token| token.starts_with("secret-oauth-"))
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn failed_refresh_after_rows_leaves_an_unfinished_stream_without_duplicates() -> TestResult {
    let expected = expected_partitioned_rows()?;
    let expected_ids = expected
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    for parquet_backend in ["direct", "delta_kernel"] {
        let authority = MockAzureTokenServer::new(usize::MAX)?;
        let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
        let assertion_path = fixture.directory.path().join("assertion.jwt");
        fs::write(&assertion_path, INITIAL_ASSERTION)?;
        let mut first_file = None;
        let mut assertion_removed = false;
        fixture.restart_storage_with_request_hook(move |request| {
            let target = request.split_whitespace().nth(1).unwrap();
            if !target.ends_with(".parquet") {
                return;
            }
            if let Some(first_file) = &first_file {
                if target != first_file && !assertion_removed {
                    fs::remove_file(&assertion_path).unwrap();
                    assertion_removed = true;
                }
            } else {
                first_file = Some(target.to_owned());
            }
        })?;
        fixture.write_options(workload_identity_options(&fixture, &authority))?;
        let execution_options = fixture.directory.path().join("execution.json");
        // Serialize file reads so the first file emits rows before the second loses its credentials.
        fs::write(
            &execution_options,
            serde_json::to_vec(&json!({
                "parquet_backend": parquet_backend,
                "max_concurrent_file_reads_per_scan": 1,
                "max_concurrent_file_reads_per_partition": 1,
                "output_buffer_batches_per_partition": 1,
                "prefetch_files_per_partition": 0,
                "parquet_full_file_read_threshold_bytes": null,
                "parquet_metadata_size_hint_bytes": 8
            }))?,
        )?;
        let output = run_with_timeout(
            workload_identity_command(&fixture, &authority, "scan")
                .args(["--target-partitions", "1", "--diagnostics=json"])
                .arg("--execution-options-file")
                .arg(execution_options),
        )?;
        assert_eq!(output.status.code(), Some(1), "{parquet_backend}");
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "data_file_read");
        assert!(!output.stdout.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
        assert!(!String::from_utf8_lossy(&output.stdout).contains("secret"));
        let reader = StreamReader::try_new(Cursor::new(output.stdout), None)?;
        let schema = reader.schema();
        let actual = concat_batches(&schema, &reader.collect::<Result<Vec<_>, _>>()?)?;
        assert!(actual.num_rows() > 0 && actual.num_rows() < expected.num_rows());
        let actual_ids = actual
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut seen = HashSet::new();
        for id in actual_ids.values() {
            assert!(expected_ids.values().contains(id));
            assert!(seen.insert(id), "duplicate output row {id}");
        }
        assert!(!fixture.directory.path().join("assertion.jwt").exists());
        assert!(
            assert_workload_requests(&authority.requests.lock().unwrap())
                .iter()
                .all(|assertion| assertion == INITIAL_ASSERTION)
        );
        assert!(
            fixture
                .storage_bearer_tokens()
                .iter()
                .all(|token| token.starts_with("secret-oauth-"))
        );
    }
    Ok(())
}

#[test]
fn missing_assertion_at_startup_or_refresh_stops_the_read() -> TestResult {
    for remove_after_first_read in [false, true] {
        for machine in [false, true] {
            let authority = MockAzureTokenServer::new(usize::MAX)?;
            let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
            if remove_after_first_read {
                let assertion_path = fixture.directory.path().join("assertion.jwt");
                fs::write(&assertion_path, INITIAL_ASSERTION)?;
                fixture.restart_storage_with_request_hook(move |_| {
                    let _ = fs::remove_file(&assertion_path);
                })?;
            }
            fixture.write_options(json!({}))?;
            let mut command = workload_identity_command(&fixture, &authority, "inspect");
            if machine {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut command)?;
            assert_failure_without_stdout(&output, 1);
            if machine {
                assert_single_terminal_record(&output.stderr, "diagnostic");
            }
            let requests = authority.requests.lock().unwrap();
            let tokens = fixture.storage_bearer_tokens();
            if remove_after_first_read {
                assert_eq!(assert_workload_requests(&requests), [INITIAL_ASSERTION]);
                assert_eq!(tokens, ["secret-oauth-1"]);
            } else {
                assert!(requests.is_empty());
                assert!(tokens.is_empty());
            }
        }
    }
    Ok(())
}

#[test]
fn workload_source_requires_azure_blob_and_explicit_selection() -> TestResult {
    for table in [
        "s3://bucket/table",
        "gs://bucket/table",
        "secret-local-table",
    ] {
        let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).env_clear().args([
            "inspect",
            table,
            "--auth-source",
            "azure-workload-identity",
            "--diagnostics=json",
        ]))?;
        assert_authorization_input_error(&output, "auth_source_unsupported_storage");
    }
    let authority = MockAzureTokenServer::new(usize::MAX)?;
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
    fixture.write_options(workload_identity_options(&fixture, &authority))?;
    for source in ["json", "env"] {
        let output = run_with_timeout(fixture.command("inspect").args([
            "--auth-source",
            source,
            "--diagnostics=json",
        ]))?;
        assert_authorization_input_error(
            &output,
            if source == "env" {
                "auth_source_conflicting_credentials"
            } else {
                "auth_source_unsupported_option"
            },
        );
    }
    assert!(authority.requests.lock().unwrap().is_empty());
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}
