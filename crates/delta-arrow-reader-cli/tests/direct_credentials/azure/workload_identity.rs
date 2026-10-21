//! Workload identity fixes the service principal while rereading rotated assertions.

use std::{collections::HashMap, fs, process::Command};

use serde_json::{Value, json};
use url::form_urlencoded;

use super::{
    ACCOUNT_KEY, BEARER_TOKEN, CloudStorageProtocol, DirectStorageFixture, TestResult,
    assert_read_result, assert_single_terminal_record, run_with_timeout, storage_bearer_tokens,
    token_server::AzureTokenServer,
};
use crate::cloud_storage::{MockCloudStorageServer, partitioned_objects};

const INITIAL_ASSERTION: &str = "secret-initial-assertion+&=";
const ROTATED_ASSERTION: &str = "secret-rotated-assertion+&=";

fn workload_identity_options(
    fixture: &DirectStorageFixture,
    authority: &AzureTokenServer,
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
    authority: &AzureTokenServer,
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
    if operation == "scan" {
        command.args(["--column", "id"]);
    } else if operation == "query" {
        command
            .arg("--sql-file")
            .arg(fixture.directory.path().join("query.sql"));
    }
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
        for operation in ["inspect", "scan", "query"] {
            let authority = AzureTokenServer::new(usize::MAX)?;
            let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::Azure)?;
            let assertion_path = fixture.directory.path().join("assertion.jwt");
            fs::write(&assertion_path, INITIAL_ASSERTION)?;
            let options_path = fixture.directory.path().join("options.json");
            fixture.server = MockCloudStorageServer::with_objects(
                CloudStorageProtocol::Azure,
                partitioned_objects()?,
                move |_, _| {
                    // Replace the projected token atomically, as a workload issuer would.
                    let replacement = assertion_path.with_extension("replacement");
                    fs::write(&replacement, ROTATED_ASSERTION).unwrap();
                    fs::rename(replacement, &assertion_path).unwrap();
                    // Only the assertion is reread; the selected principal and path stay fixed.
                    fs::write(&options_path, "secret-replaced-options").unwrap();
                },
            )?;
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
            let tokens = storage_bearer_tokens(&fixture);
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
    let authority = AzureTokenServer::new(usize::MAX)?;
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::Azure)?;
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
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
            let record = assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(record["reason"], reason);
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
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains(ACCOUNT_KEY));
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["reason"], reason);
    }
    assert!(authority.requests.lock().unwrap().is_empty());
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn acquisition_refresh_and_storage_failures_never_select_ambient_credentials() -> TestResult {
    for successful_tokens in [0, 1, usize::MAX] {
        for machine in [false, true] {
            let authority = AzureTokenServer::new(successful_tokens)?;
            let fixture = DirectStorageFixture::new(CloudStorageProtocol::Azure)?;
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
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
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
            let tokens = storage_bearer_tokens(&fixture);
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
fn missing_assertion_at_startup_or_refresh_stops_the_read() -> TestResult {
    for remove_after_first_read in [false, true] {
        for machine in [false, true] {
            let authority = AzureTokenServer::new(usize::MAX)?;
            let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::Azure)?;
            if remove_after_first_read {
                let assertion_path = fixture.directory.path().join("assertion.jwt");
                fs::write(&assertion_path, INITIAL_ASSERTION)?;
                fixture.server = MockCloudStorageServer::with_objects(
                    CloudStorageProtocol::Azure,
                    partitioned_objects()?,
                    move |_, _| {
                        let _ = fs::remove_file(&assertion_path);
                    },
                )?;
            }
            fixture.write_options(json!({}))?;
            let mut command = workload_identity_command(&fixture, &authority, "inspect");
            if machine {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut command)?;
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
            if machine {
                assert_single_terminal_record(&output.stderr, "diagnostic");
            }
            let requests = authority.requests.lock().unwrap();
            let tokens = storage_bearer_tokens(&fixture);
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
fn workload_source_is_azure_only_and_requires_explicit_selection() -> TestResult {
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
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["reason"], "auth_source_unsupported_storage");
    }
    let authority = AzureTokenServer::new(usize::MAX)?;
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::Azure)?;
    fixture.write_options(workload_identity_options(&fixture, &authority))?;
    for source in ["json", "env"] {
        let output = run_with_timeout(fixture.command("inspect").args([
            "--auth-source",
            source,
            "--diagnostics=json",
        ]))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(
            record["reason"],
            if source == "env" {
                "auth_source_conflicting_credentials"
            } else {
                "auth_source_unsupported_option"
            }
        );
    }
    assert!(authority.requests.lock().unwrap().is_empty());
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}
