//! Managed identity requests keep one explicit selector across reads and renewals.

use std::{collections::HashMap, fs, process::Command};

use serde_json::json;
use url::Url;

use super::{
    ACCOUNT_KEY, BEARER_TOKEN, CloudStorageProtocol, DirectStorageFixture, TestResult,
    assert_authorization_input_error, assert_failure_without_stdout, assert_read_result,
    assert_single_terminal_record, mock_token_server::MockAzureTokenServer, run_with_timeout,
};

// JSON alias, advertised environment variable, metadata query key, and synthetic identity.
const IDENTITY_SELECTORS: [(&str, &str, &str, &str); 3] = [
    (
        "CLIENT_ID",
        "AZURE_CLIENT_ID",
        "client_id",
        "secret-client-id",
    ),
    (
        "object_id",
        "AZURE_OBJECT_ID",
        "object_id",
        "secret-object-id",
    ),
    (
        "msi_resource_id",
        "AZURE_MSI_RESOURCE_ID",
        "msi_res_id",
        "/subscriptions/secret-subscription/resourceGroups/secret-group/providers/Microsoft.ManagedIdentity/userAssignedIdentities/secret-identity",
    ),
];
const IDENTITY_HEADER: &str = "secret-identity-header";

fn managed_identity_command(
    fixture: &DirectStorageFixture,
    metadata: &MockAzureTokenServer,
    operation: &str,
) -> Command {
    let mut command = fixture.command(operation);
    command
        .args(["--auth-source", "azure-managed-identity"])
        .env(
            "IDENTITY_ENDPOINT",
            format!("{}/metadata/identity/oauth2/token", metadata.endpoint),
        )
        .env("IDENTITY_HEADER", IDENTITY_HEADER)
        .env("AZURE_STORAGE_ACCOUNT_NAME", "account")
        .env("AZURE_STORAGE_TOKEN", BEARER_TOKEN)
        .env("AZURE_STORAGE_ACCOUNT_KEY", ACCOUNT_KEY)
        .env("AZURE_STORAGE_CLIENT_ID", "secret-unused-client")
        .env("AZURE_CLIENT_SECRET", "secret-unused-password")
        .env("AZURE_TENANT_ID", "secret-unused-tenant")
        .env("AZURE_FEDERATED_TOKEN_FILE", "secret-unused-file")
        .env("AZURE_AUTHORITY_HOST", "http://secret-unused-authority")
        .env("AZURE_USE_AZURE_CLI", "true")
        .env("AZURE_MSI_ENDPOINT", "http://secret-unused-endpoint");
    command
}

fn assert_metadata_requests(
    metadata: &MockAzureTokenServer,
    query_key: &str,
    identity: &str,
    identity_header: Option<&str>,
) {
    let requests = metadata.requests.lock().unwrap();
    assert!(!requests.is_empty());
    for request in requests.iter() {
        assert!(request.starts_with("GET "), "{request}");
        let target = request.split_whitespace().nth(1).unwrap();
        let url = Url::parse(&format!("http://localhost{target}")).unwrap();
        assert_eq!(url.path(), "/metadata/identity/oauth2/token");
        let parameters: HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(
            parameters,
            HashMap::from([
                ("api-version".into(), "2019-08-01".into()),
                ("resource".into(), "https://storage.azure.com".into()),
                (query_key.to_owned(), identity.to_owned()),
            ])
        );
        let headers: HashMap<_, _> = request
            .lines()
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                Some((name.to_ascii_lowercase(), value.trim()))
            })
            .collect();
        assert_eq!(headers.get("metadata").copied(), Some("true"));
        assert_eq!(headers.get("x-identity-header").copied(), identity_header);
        assert!(!request.contains("secret-unused"), "{request}");
    }
}

#[test]
fn all_reads_refresh_only_the_selected_managed_identity() -> TestResult {
    for references in ["json", "env"] {
        for (option, variable, query_key, identity) in IDENTITY_SELECTORS {
            for operation in ["inspect", "scan", "query"] {
                let metadata = MockAzureTokenServer::new(usize::MAX)?;
                let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
                let options_path = fixture.directory.path().join("options.json");
                fixture.restart_storage_with_request_hook(move |_| {
                    // Renewal must keep the loaded identity and metadata endpoint.
                    fs::write(&options_path, "secret-replaced-options").unwrap();
                })?;
                let mut options = json!({});
                if references == "json" {
                    options[option] = json!(identity);
                    options["identity_endpoint"] = json!(format!(
                        "{}/metadata/identity/oauth2/token",
                        metadata.endpoint
                    ));
                    options["account_name"] = json!("account");
                }
                fixture.write_options(options)?;
                let mut command = managed_identity_command(&fixture, &metadata, operation);
                command.arg("--diagnostics=json");
                if references == "json" {
                    // An explicit selector overrides all environment selectors, including other kinds.
                    for (_, name, _, _) in IDENTITY_SELECTORS {
                        command.env(name, "secret-unused-identity");
                    }
                    command
                        .env("IDENTITY_ENDPOINT", "http://secret-unused-endpoint")
                        .env("AZURE_STORAGE_ACCOUNT_NAME", "secret-unused-account");
                } else {
                    command
                        .env(variable, identity)
                        .env_remove("IDENTITY_HEADER");
                }
                let output = run_with_timeout(&mut command)?;
                assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
                assert_read_result(output, operation)?;
                assert_metadata_requests(
                    &metadata,
                    query_key,
                    identity,
                    if references == "json" {
                        Some(IDENTITY_HEADER)
                    } else {
                        None
                    },
                );
                let tokens = fixture.storage_bearer_tokens();
                assert_eq!(tokens[0], "secret-oauth-1");
                assert!(
                    tokens
                        .iter()
                        .all(|token| token.starts_with("secret-oauth-"))
                );
                assert!(tokens.iter().any(|token| token != "secret-oauth-1"));
                assert!(
                    fixture
                        .server
                        .requests
                        .lock()
                        .unwrap()
                        .iter()
                        .all(|request| !request.to_ascii_lowercase().contains("x-identity-header"))
                );
            }
        }
    }
    Ok(())
}

#[test]
fn zero_or_multiple_identity_selectors_fail_before_io() -> TestResult {
    let metadata = MockAzureTokenServer::new(usize::MAX)?;
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
    for references in ["json", "env"] {
        for selector_indices in [vec![], vec![0, 1], vec![0, 2], vec![1, 2]] {
            let mut options = json!({});
            let mut command = managed_identity_command(&fixture, &metadata, "inspect");
            command.arg("--diagnostics=json");
            for &index in &selector_indices {
                let (option, variable, _, identity) = IDENTITY_SELECTORS[index];
                if references == "json" {
                    options[option] = json!(identity);
                } else {
                    command.env(variable, identity);
                }
            }
            fixture.write_options(options)?;
            let output = run_with_timeout(&mut command)?;
            assert_authorization_input_error(
                &output,
                if selector_indices.is_empty() {
                    "auth_source_incomplete_credentials"
                } else {
                    "auth_source_conflicting_credentials"
                },
            );
        }
    }
    assert!(metadata.requests.lock().unwrap().is_empty());
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn malformed_environment_values_and_conflicting_json_are_rejected() -> TestResult {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let metadata = MockAzureTokenServer::new(usize::MAX)?;
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
    fixture.write_options(json!({}))?;
    for variable in [
        "AZURE_CLIENT_ID",
        "AZURE_OBJECT_ID",
        "AZURE_MSI_RESOURCE_ID",
        "IDENTITY_ENDPOINT",
        "IDENTITY_HEADER",
    ] {
        for (value, reason) in [
            (OsString::new(), "auth_source_invalid_value"),
            (
                OsString::from("secret-invalid\nvalue"),
                "auth_source_invalid_value",
            ),
            (
                OsString::from_vec(vec![0xff]),
                "auth_source_non_utf8_environment",
            ),
        ] {
            let output = run_with_timeout(
                managed_identity_command(&fixture, &metadata, "inspect")
                    .env("AZURE_CLIENT_ID", "secret-client-id")
                    .env(variable, value)
                    .arg("--diagnostics=json"),
            )?;
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
            json!({"tenant_id":"secret-tenant"}),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({"authority_host":"http://secret-authority"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"federated_token_file":"secret-file"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"use_azure_cli":"true"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"azure_client_id":"secret-other-id"}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"identity_endpoint":"http://secret-other-endpoint"}),
            "auth_source_duplicate_option",
        ),
        (json!({"client_id":""}), "auth_source_invalid_value"),
        (
            json!({"msi_endpoint":"secret-\nendpoint"}),
            "auth_source_invalid_value",
        ),
    ] {
        let mut options = json!({"client_id":"secret-client-id", "msi_endpoint":metadata.endpoint});
        options
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        fixture.write_options(options)?;
        let output = run_with_timeout(
            managed_identity_command(&fixture, &metadata, "inspect").arg("--diagnostics=json"),
        )?;
        assert_authorization_input_error(&output, reason);
        assert!(!String::from_utf8_lossy(&output.stderr).contains(ACCOUNT_KEY));
    }
    assert!(metadata.requests.lock().unwrap().is_empty());
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn acquisition_refresh_and_storage_failures_never_select_another_identity() -> TestResult {
    for successful_tokens in [0, 1, usize::MAX] {
        for machine in [false, true] {
            let metadata = MockAzureTokenServer::new(successful_tokens)?;
            let fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
            fixture.write_options(json!({}))?;
            if successful_tokens == usize::MAX {
                fixture.server.deny_all_requests();
            }
            let mut command = managed_identity_command(&fixture, &metadata, "inspect");
            command.env("AZURE_CLIENT_ID", "secret-client-id");
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
            assert_metadata_requests(
                &metadata,
                "client_id",
                "secret-client-id",
                Some(IDENTITY_HEADER),
            );
            let requests = metadata.requests.lock().unwrap();
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
fn managed_identity_requires_azure_blob_and_its_explicit_source() -> TestResult {
    for table in [
        "s3://bucket/table",
        "gs://bucket/table",
        "secret-local-table",
    ] {
        let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).env_clear().args([
            "inspect",
            table,
            "--auth-source",
            "azure-managed-identity",
            "--diagnostics=json",
        ]))?;
        assert_authorization_input_error(&output, "auth_source_unsupported_storage");
    }
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
    fixture.write_options(json!({"object_id":"secret-object-id"}))?;
    for source in ["json", "env", "azure-workload-identity"] {
        let output = run_with_timeout(fixture.command("inspect").args([
            "--auth-source",
            source,
            "--diagnostics=json",
        ]))?;
        assert_authorization_input_error(&output, "auth_source_unsupported_option");
    }
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}
