//! Azure authentication with a client secret keeps one identity across reads and token refreshes.

use std::{collections::HashMap, fs, process::Command};

use serde_json::{Value, json};
use url::form_urlencoded;

use super::{
    ACCOUNT_KEY, BEARER_TOKEN, CREDENTIALS, CloudStorageProtocol, DirectStorageFixture, TestResult,
    assert_authorization_input_error, assert_failure_without_stdout, assert_read_result,
    assert_single_terminal_record, mock_token_server::MockAzureTokenServer, run_with_timeout,
};

// JSON aliases and the only environment variables advertised for this identity.
const CLIENT_SECRET_FIELDS: [(&str, &str, &str); 3] = [
    ("CLIENT_ID", "AZURE_CLIENT_ID", "secret-client-id"),
    (
        "azure_client_secret",
        "AZURE_CLIENT_SECRET",
        "secret-client-password+&=",
    ),
    ("authority_id", "AZURE_TENANT_ID", "secret-tenant-id"),
];

fn client_secret_json_fields() -> Value {
    CLIENT_SECRET_FIELDS
        .into_iter()
        .map(|(key, _, value)| (key.to_owned(), json!(value)))
        .collect()
}

fn write_client_secret_options(
    fixture: &DirectStorageFixture,
    authority: &MockAzureTokenServer,
    source: &str,
) -> TestResult {
    let mut options = if source == "json" {
        client_secret_json_fields()
    } else {
        json!({})
    };
    options["account_name"] = json!("account");
    options["authority_host"] = json!(authority.endpoint);
    fixture.write_options(options)
}

fn client_secret_command(fixture: &DirectStorageFixture, source: &str, operation: &str) -> Command {
    let mut command = fixture.command(operation);
    command
        .args(["--auth-source", source])
        .env("AZURE_USE_AZURE_CLI", "true")
        .env("AZURE_FEDERATED_TOKEN_FILE", "secret-unused-file")
        .env("AZURE_AUTHORITY_HOST", "http://secret-unused-authority")
        .env(
            "AZURE_MSI_ENDPOINT",
            format!("{}/secret-unused-provider", fixture.server.endpoint),
        );
    for (_, variable, value) in CLIENT_SECRET_FIELDS {
        command.env(
            variable,
            if source == "env" {
                value
            } else {
                "secret-unused"
            },
        );
    }
    if source == "json" {
        command.env("AZURE_STORAGE_TOKEN", BEARER_TOKEN);
    }
    command
}

fn assert_client_secret_requests(requests: &[String]) {
    assert!(!requests.is_empty());
    for request in requests {
        assert!(
            request.starts_with("POST /secret-tenant-id/oauth2/v2.0/token HTTP/1.1\r\n"),
            "{request}"
        );
        let (_, body) = request.split_once("\r\n\r\n").unwrap();
        let fields: HashMap<_, _> = form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        assert_eq!(
            fields,
            HashMap::from([
                ("client_id".into(), "secret-client-id".into()),
                ("client_secret".into(), "secret-client-password+&=".into()),
                ("scope".into(), "https://storage.azure.com/.default".into()),
                ("grant_type".into(), "client_credentials".into()),
            ])
        );
    }
}

#[test]
fn tokens_authorize_all_reads_and_refresh_the_loaded_identity() -> TestResult {
    for source in ["json", "env"] {
        for operation in ["inspect", "scan", "preview", "query"] {
            let authority = MockAzureTokenServer::new(usize::MAX)?;
            let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
            let options_path = fixture.directory.path().join("options.json");
            fixture.restart_storage_with_request_hook(move |_| {
                // Refresh must use the loaded identity, even if its source file changes.
                fs::write(&options_path, "secret-replaced-options").unwrap();
            })?;
            write_client_secret_options(&fixture, &authority, source)?;
            let output = run_with_timeout(
                client_secret_command(&fixture, source, operation).arg("--diagnostics=json"),
            )?;
            assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
            assert_read_result(output, operation)?;
            let tokens = fixture.storage_bearer_tokens();
            assert!(tokens.len() > 1);
            assert_eq!(tokens[0], "secret-oauth-1");
            assert!(
                tokens
                    .iter()
                    .all(|token| token.starts_with("secret-oauth-"))
            );
            assert!(tokens.iter().any(|token| token != "secret-oauth-1"));
            assert_client_secret_requests(&authority.requests.lock().unwrap());
        }
    }
    Ok(())
}

#[test]
fn acquisition_refresh_and_permission_failures_keep_the_selected_identity() -> TestResult {
    for source in ["json", "env"] {
        // Zero or one token exercises acquisition or refresh failure; unlimited tests storage denial.
        for successful_tokens in [0, 1, usize::MAX] {
            for machine in [false, true] {
                let authority = MockAzureTokenServer::new(successful_tokens)?;
                let fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
                write_client_secret_options(&fixture, &authority, source)?;
                if successful_tokens == usize::MAX {
                    fixture.server.deny_all_requests();
                }
                let mut command = client_secret_command(&fixture, source, "inspect");
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
                assert_client_secret_requests(&requests);
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
    }
    Ok(())
}

#[test]
fn partial_or_mixed_client_secret_families_fail_before_io() -> TestResult {
    let authority = MockAzureTokenServer::new(usize::MAX)?;
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
    for source in ["json", "env"] {
        for present_field_indices in [
            vec![0],
            vec![1],
            vec![2],
            vec![0, 1],
            vec![0, 2],
            vec![1, 2],
            vec![0, 1, 2],
        ] {
            for static_credential in [
                None,
                Some(CREDENTIALS[0]),
                Some(CREDENTIALS[1]),
                Some(CREDENTIALS[2]),
            ] {
                if present_field_indices.len() == 3 && static_credential.is_none() {
                    continue;
                }
                let mut options =
                    json!({"account_name":"account", "authority_host":authority.endpoint});
                let mut command = fixture.command("inspect");
                command.args(["--auth-source", source, "--diagnostics=json"]);
                for &index in &present_field_indices {
                    let (key, variable, value) = CLIENT_SECRET_FIELDS[index];
                    if source == "json" {
                        options[key] = json!(value);
                    } else {
                        command.env(variable, value);
                    }
                }
                if let Some((key, variable, value)) = static_credential {
                    if source == "json" {
                        options[key] = json!(value);
                    } else {
                        command.env(variable, value);
                    }
                }
                fixture.write_options(options)?;
                let output = run_with_timeout(&mut command)?;
                assert!(!String::from_utf8_lossy(&output.stderr).contains(ACCOUNT_KEY));
                assert_authorization_input_error(
                    &output,
                    if static_credential.is_some() {
                        "auth_source_conflicting_credentials"
                    } else {
                        "auth_source_incomplete_credentials"
                    },
                );
            }
        }
    }
    assert!(authority.requests.lock().unwrap().is_empty());
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn duplicate_aliases_and_cross_source_fields_are_rejected() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
    for (source, extra, reason) in [
        (
            "json",
            json!({"client_id":"secret-other-client"}),
            "auth_source_duplicate_option",
        ),
        (
            "json",
            json!({"tenant_id":"secret-other-tenant"}),
            "auth_source_duplicate_option",
        ),
        (
            "json",
            json!({"client_secret":"secret-other-password"}),
            "auth_source_duplicate_option",
        ),
        ("env", json!({}), "auth_source_conflicting_credentials"),
    ] {
        let mut options = client_secret_json_fields();
        options
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        fixture.write_options(options)?;
        let output = run_with_timeout(
            client_secret_command(&fixture, source, "inspect").arg("--diagnostics=json"),
        )?;
        assert_authorization_input_error(&output, reason);
    }
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}
