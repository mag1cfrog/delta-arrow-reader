//! ECS task and EKS Pod Identity credentials authorize reads without selecting ambient identities.

use std::{fs, process::Command};

use serde_json::{Value, json};
use url::Url;

use super::{
    CloudStorageProtocol, DirectStorageFixture, TestResult, assert_authorization_input_error,
    assert_failure_without_stdout, assert_read_result, assert_single_terminal_record,
    run_with_timeout, verify_signed_session_generations,
};
use crate::direct_credentials::mock_credential_proxy::{CredentialService, MockCredentialProxy};

const ECS_RELATIVE_URI: &str = "/v2/credentials/secret-task?scope=secret-selected";
const EKS_HTTP_URI: &str = "http://169.254.170.23/v1/credentials?scope=secret-selected";
const EKS_HTTPS_URI: &str =
    "https://credentials.secret.invalid/v1/credentials?scope=secret-selected";
const INITIAL_TOKEN: &str = "secret-initial-pod-token";
const ROTATED_TOKEN: &str = "secret-rotated-pod-token";

fn container_credential_reference_options(fixture: &DirectStorageFixture, endpoint: &str) -> Value {
    if endpoint.starts_with('/') {
        json!({"container_credentials_relative_uri":endpoint})
    } else {
        json!({
            "container_credentials_full_uri":endpoint,
            "container_authorization_token_file":fixture.directory.path().join("pod-token")
        })
    }
}

fn container_credential_command(fixture: &DirectStorageFixture, operation: &str) -> Command {
    let mut command = fixture.command(operation);
    command
        .args(["--auth-source", "aws-container"])
        .env("AWS_REGION", "eu-west-1")
        .env("AWS_DEFAULT_REGION", "secret-unused-region")
        .env("AWS_ACCESS_KEY_ID", "secret-unused-key")
        .env("AWS_SECRET_ACCESS_KEY", "secret-unused-password")
        .env("AWS_SESSION_TOKEN", "secret-unused-token")
        .env("AWS_PROFILE", "secret-unused-profile")
        .env("AWS_ROLE_ARN", "secret-unused-role")
        .env("AWS_WEB_IDENTITY_TOKEN_FILE", "secret-unused-file")
        .env("AWS_SKIP_SIGNATURE", "true")
        .env(
            "AWS_CONTAINER_AUTHORIZATION_TOKEN",
            "secret-unused-inline-token",
        )
        .env(
            "AWS_METADATA_ENDPOINT",
            format!("{}/secret-unused-metadata", fixture.server.endpoint),
        );
    command
}

fn set_container_credential_environment(command: &mut Command, options: &Value) {
    for (key, value) in options.as_object().unwrap() {
        command.env(
            format!("AWS_{}", key.to_ascii_uppercase()),
            value.as_str().unwrap(),
        );
    }
}

fn assert_container_credential_requests(requests: &[Value], endpoint: &str) -> Vec<Option<String>> {
    assert!(!requests.is_empty());
    let url = Url::parse(&if endpoint.starts_with('/') {
        format!("http://169.254.170.2{endpoint}")
    } else {
        endpoint.to_owned()
    })
    .unwrap();
    requests
        .iter()
        .map(|request| {
            assert_eq!(request["host"], url.host_str().unwrap());
            assert_eq!(request["path"], url.path());
            assert_eq!(request["parameters"], json!({"scope":["secret-selected"]}));
            request["authorization"].as_str().map(str::to_owned)
        })
        .collect()
}

#[test]
fn all_reads_refresh_the_selected_container_reference() -> TestResult {
    for endpoint in [ECS_RELATIVE_URI, EKS_HTTP_URI, EKS_HTTPS_URI] {
        for references in ["json", "env"] {
            for operation in ["inspect", "scan", "preview", "query"] {
                let proxy = MockCredentialProxy::new(CredentialService::AwsContainer, "refresh")?;
                let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
                let token_path = fixture.directory.path().join("pod-token");
                fs::write(&token_path, INITIAL_TOKEN)?;
                let options_path = fixture.directory.path().join("options.json");
                fixture.restart_storage_with_request_hook(move |_| {
                    let replacement = token_path.with_extension("replacement");
                    fs::write(&replacement, ROTATED_TOKEN).unwrap();
                    fs::rename(replacement, &token_path).unwrap();
                    // Refresh keeps the loaded endpoint and path even if the options file changes.
                    fs::write(&options_path, "secret-replaced-options").unwrap();
                })?;
                let reference = container_credential_reference_options(&fixture, endpoint);
                let mut options = if references == "json" {
                    reference.clone()
                } else {
                    json!({})
                };
                let mut command = container_credential_command(&fixture, operation);
                command.arg("--diagnostics=json");
                if references == "json" {
                    options["default_region"] = json!("eu-west-1");
                    command
                        .env("AWS_REGION", "secret-unused-region")
                        .env(
                            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                            "secret-invalid-relative-uri",
                        )
                        .env(
                            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                            "secret-invalid-full-uri",
                        )
                        .env(
                            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
                            "secret-unused-file",
                        );
                } else {
                    set_container_credential_environment(&mut command, &reference);
                }
                fixture.write_options(proxy.add_proxy_options(options))?;
                let output = run_with_timeout(&mut command)?;
                assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
                assert_read_result(output, operation)?;
                let generations = verify_signed_session_generations(&fixture, "container")?;
                assert_eq!(generations[0], 1);
                assert!(generations.iter().any(|generation| *generation > 1));
                let tokens = assert_container_credential_requests(&proxy.requests()?, endpoint);
                if endpoint == ECS_RELATIVE_URI {
                    assert!(tokens.iter().all(Option::is_none));
                } else {
                    assert_eq!(tokens[0].as_deref(), Some(INITIAL_TOKEN));
                    assert!(
                        tokens[1..]
                            .iter()
                            .all(|token| token.as_deref() == Some(ROTATED_TOKEN))
                    );
                }
                assert!(
                    fixture
                        .server
                        .requests
                        .lock()
                        .unwrap()
                        .iter()
                        .all(|request| !request.contains("pod-token"))
                );
            }
        }
    }
    Ok(())
}

#[test]
fn partial_and_mixed_references_fail_before_io() -> TestResult {
    let proxy = MockCredentialProxy::new(CredentialService::AwsContainer, "refresh")?;
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    for references in ["json", "env"] {
        for (options, reason) in [
            (json!({}), "auth_source_incomplete_credentials"),
            (
                json!({"container_credentials_full_uri":EKS_HTTP_URI}),
                "auth_source_incomplete_credentials",
            ),
            (
                json!({"container_authorization_token_file":"secret-file"}),
                "auth_source_incomplete_credentials",
            ),
            (
                json!({"container_credentials_relative_uri":ECS_RELATIVE_URI, "container_credentials_full_uri":EKS_HTTP_URI}),
                "auth_source_conflicting_credentials",
            ),
            (
                json!({"container_credentials_relative_uri":ECS_RELATIVE_URI, "container_authorization_token_file":"secret-file"}),
                "auth_source_conflicting_credentials",
            ),
        ] {
            let mut command = container_credential_command(&fixture, "inspect");
            command.arg("--diagnostics=json");
            if references == "json" {
                // JSON must be complete; even a valid ambient ECS reference cannot fill missing fields.
                if !options.as_object().unwrap().is_empty() {
                    set_container_credential_environment(
                        &mut command,
                        &container_credential_reference_options(&fixture, ECS_RELATIVE_URI),
                    );
                }
                fixture.write_options(proxy.add_proxy_options(options))?;
            } else {
                fixture.write_options(proxy.add_proxy_options(json!({})))?;
                set_container_credential_environment(&mut command, &options);
            }
            let output = run_with_timeout(&mut command)?;
            assert_authorization_input_error(&output, reason);
        }
    }
    assert!(proxy.requests()?.is_empty());
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn malformed_references_and_conflicting_sources_fail_before_io() -> TestResult {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let proxy = MockCredentialProxy::new(CredentialService::AwsContainer, "refresh")?;
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    fixture.write_options(proxy.add_proxy_options(json!({})))?;
    for variable in [
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
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
                container_credential_command(&fixture, "inspect")
                    .arg("--diagnostics=json")
                    .env(variable, value),
            )?;
            assert_authorization_input_error(&output, reason);
        }
    }
    let mut invalid_options = vec![
        (
            json!({"container_credentials_relative_uri":ECS_RELATIVE_URI, "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI":ECS_RELATIVE_URI}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"container_credentials_full_uri":EKS_HTTP_URI, "AWS_CONTAINER_CREDENTIALS_FULL_URI":EKS_HTTP_URI}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"container_authorization_token_file":"secret-file", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE":"secret-file"}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"container_authorization_token_file":""}),
            "auth_source_invalid_value",
        ),
        (
            json!({"container_credentials_relative_uri":"\nsecret-path"}),
            "auth_source_invalid_value",
        ),
        (
            json!({"access_key_id":"secret-key", "secret_access_key":"secret-password"}),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({"session_token":"secret-token"}),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({"role_arn":"secret-role"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"metadata_endpoint":"http://secret-metadata"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"container_authorization_token":"secret-inline-token"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"skip_signature":"true"}),
            "auth_source_unsupported_option",
        ),
    ];
    for uri in [
        "secret-path",
        "@secret.invalid/path",
        ".secret.invalid/path",
        "//secret.invalid/path",
        "/\\secret.invalid/path",
        "/path#secret-fragment",
    ] {
        invalid_options.push((
            json!({"container_credentials_relative_uri":uri}),
            "auth_source_invalid_value",
        ));
    }
    for uri in [
        "secret-invalid-uri",
        "file:///secret-file",
        "ftp://secret.invalid/path",
        "http://user:secret@localhost/path",
        "http://localhost/path#secret-fragment",
    ] {
        invalid_options.push((json!({"container_credentials_full_uri":uri, "container_authorization_token_file":"secret-file"}), "auth_source_invalid_value"));
    }
    for (options, reason) in invalid_options {
        fixture.write_options(proxy.add_proxy_options(options))?;
        let mut command = container_credential_command(&fixture, "inspect");
        command.arg("--diagnostics=json");
        set_container_credential_environment(
            &mut command,
            &container_credential_reference_options(&fixture, ECS_RELATIVE_URI),
        );
        let output = run_with_timeout(&mut command)?;
        assert_authorization_input_error(&output, reason);
    }
    assert!(proxy.requests()?.is_empty());
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn acquisition_refresh_and_storage_failures_never_select_ambient_credentials() -> TestResult {
    for endpoint in [ECS_RELATIVE_URI, EKS_HTTP_URI] {
        for mode in ["reject", "reject_refresh", "malformed", "storage_denied"] {
            for machine in [false, true] {
                let proxy = MockCredentialProxy::new(CredentialService::AwsContainer, mode)?;
                let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
                fs::write(fixture.directory.path().join("pod-token"), INITIAL_TOKEN)?;
                fixture.write_options(proxy.add_proxy_options(
                    container_credential_reference_options(&fixture, endpoint),
                ))?;
                if mode == "storage_denied" {
                    fixture.server.deny_all_requests();
                }
                let mut command = container_credential_command(&fixture, "inspect");
                if machine {
                    command.arg("--diagnostics=json");
                }
                let output = run_with_timeout(&mut command)?;
                assert_failure_without_stdout(&output, 1);
                if machine {
                    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                    if mode == "storage_denied" {
                        assert_eq!(record["source_kind"], "permission_denied");
                    }
                }
                let requests = proxy.requests()?;
                let tokens = assert_container_credential_requests(&requests, endpoint);
                let expected_token = if endpoint == ECS_RELATIVE_URI {
                    None
                } else {
                    Some(INITIAL_TOKEN)
                };
                assert!(
                    tokens
                        .iter()
                        .all(|token| token.as_deref() == expected_token)
                );
                let generations = verify_signed_session_generations(&fixture, "container")?;
                match mode {
                    "reject" | "malformed" => {
                        assert_eq!(requests.len(), 1);
                        assert!(generations.is_empty());
                    }
                    "reject_refresh" => {
                        assert_eq!(requests.len(), 2);
                        assert_eq!(generations, [1]);
                    }
                    _ => assert!(!generations.is_empty()),
                }
            }
        }
    }
    Ok(())
}

#[test]
fn missing_or_invalid_pod_tokens_stop_reads_without_fallback() -> TestResult {
    for failure in ["missing", "removed_after_read", "invalid_header"] {
        for machine in [false, true] {
            let proxy = MockCredentialProxy::new(CredentialService::AwsContainer, "refresh")?;
            let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
            let token_path = fixture.directory.path().join("pod-token");
            if failure == "removed_after_read" {
                fs::write(&token_path, INITIAL_TOKEN)?;
                fixture.restart_storage_with_request_hook(move |_| {
                    let _ = fs::remove_file(&token_path);
                })?;
            } else if failure == "invalid_header" {
                fs::write(&token_path, "secret-invalid\nheader")?;
            }
            fixture.write_options(proxy.add_proxy_options(
                container_credential_reference_options(&fixture, EKS_HTTP_URI),
            ))?;
            let mut command = container_credential_command(&fixture, "inspect");
            if machine {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut command)?;
            assert_failure_without_stdout(&output, 1);
            if machine {
                assert_single_terminal_record(&output.stderr, "diagnostic");
            }
            let requests = proxy.requests()?;
            let generations = verify_signed_session_generations(&fixture, "container")?;
            if failure == "removed_after_read" {
                assert_eq!(
                    assert_container_credential_requests(&requests, EKS_HTTP_URI),
                    [Some(INITIAL_TOKEN.to_owned())]
                );
                assert_eq!(generations, [1]);
            } else {
                assert!(requests.is_empty());
                assert!(generations.is_empty());
            }
        }
    }
    Ok(())
}

#[test]
fn container_credentials_require_s3_and_explicit_selection() -> TestResult {
    for table in [
        "az://container/table",
        "gs://bucket/table",
        "secret-local-table",
    ] {
        let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).env_clear().args([
            "inspect",
            table,
            "--auth-source",
            "aws-container",
            "--diagnostics=json",
        ]))?;
        assert_authorization_input_error(&output, "auth_source_unsupported_storage");
    }
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    fixture.write_options(container_credential_reference_options(
        &fixture,
        ECS_RELATIVE_URI,
    ))?;
    for source in ["json", "env", "aws-web-identity"] {
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
