//! Web identity credentials sign real reads and refresh only the configured role.

use std::{fs, process::Command};

use serde_json::{Value, json};

use super::{
    CloudStorageProtocol, DirectStorageFixture, TestResult, assert_authorization_input_error,
    assert_failure_without_stdout, assert_read_result, assert_single_terminal_record,
    run_with_timeout, signed_session_generations,
};
use crate::direct_credentials::mock_credential_proxy::{CredentialService, MockCredentialProxy};

const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/secret-selected-role";
const INITIAL_ASSERTION: &str = "secret-initial-assertion+&=";
const ROTATED_ASSERTION: &str = "secret-rotated-assertion+&=";
const CUSTOM_STS_ENDPOINT: &str = "https://sts.secret-override.invalid/";

fn web_identity_command(fixture: &DirectStorageFixture, operation: &str) -> Command {
    let mut command = fixture.command(operation);
    command
        .args(["--auth-source", "aws-web-identity"])
        .env("AWS_ROLE_ARN", ROLE_ARN)
        .env(
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            fixture.directory.path().join("assertion.jwt"),
        )
        .env("AWS_REGION", "eu-west-1")
        .env("AWS_DEFAULT_REGION", "secret-unused-region")
        .env("AWS_ACCESS_KEY_ID", "secret-unused-key")
        .env("AWS_SECRET_ACCESS_KEY", "secret-unused-password")
        .env("AWS_SESSION_TOKEN", "secret-unused-token")
        .env("AWS_PROFILE", "secret-unused-profile")
        .env("AWS_SKIP_SIGNATURE", "true")
        .env(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            format!("{}/secret-unused-provider", fixture.server.endpoint),
        )
        .env(
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
            "secret-unused-file",
        )
        .env(
            "AWS_METADATA_ENDPOINT",
            format!("{}/secret-unused-metadata", fixture.server.endpoint),
        );
    command
}

fn assert_sts_requests(requests: &[Value], host: &str, session_name: &str) -> Vec<String> {
    assert!(!requests.is_empty());
    requests
        .iter()
        .map(|request| {
            assert_eq!(request["host"], host);
            assert_eq!(request["path"], "/");
            let mut parameters = request["parameters"].clone();
            let assertion = parameters
                .as_object_mut()
                .unwrap()
                .remove("WebIdentityToken")
                .unwrap();
            assert_eq!(
                parameters,
                json!({
                    "Action":["AssumeRoleWithWebIdentity"], "DurationSeconds":["3600"],
                    "RoleArn":[ROLE_ARN], "RoleSessionName":[session_name], "Version":["2011-06-15"]
                })
            );
            let assertions = assertion.as_array().unwrap();
            assert_eq!(assertions.len(), 1);
            assertions[0].as_str().unwrap().to_owned()
        })
        .collect()
}

#[test]
fn all_reads_refresh_the_fixed_role_with_rotated_assertions() -> TestResult {
    for references in ["json", "env", "mixed"] {
        for operation in ["inspect", "scan", "preview", "query"] {
            let proxy = MockCredentialProxy::new(CredentialService::AwsSts, "refresh")?;
            let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
            let assertion_path = fixture.directory.path().join("assertion.jwt");
            fs::write(&assertion_path, INITIAL_ASSERTION)?;
            let options_path = fixture.directory.path().join("options.json");
            fixture.restart_storage_with_request_hook(move |_| {
                let replacement = assertion_path.with_extension("replacement");
                fs::write(&replacement, ROTATED_ASSERTION).unwrap();
                fs::rename(replacement, &assertion_path).unwrap();
                // Only the assertion rotates; the loaded role and file path stay fixed.
                fs::write(&options_path, "secret-replaced-options").unwrap();
            })?;
            let options = match references {
                "json" => json!({
                    "ROLE_ARN":ROLE_ARN, "web_identity_token_file":fixture.directory.path().join("assertion.jwt"),
                    "role_session_name":"secret-selected-session", "endpoint_url_sts":CUSTOM_STS_ENDPOINT,
                    "default_region":"eu-west-1"
                }),
                "mixed" => json!({"role_arn":ROLE_ARN, "region":"eu-west-1"}),
                _ => json!({}),
            };
            fixture.write_options(proxy.add_proxy_options(options))?;
            let mut command = web_identity_command(&fixture, operation);
            command.arg("--diagnostics=json");
            if references != "env" {
                command
                    .env("AWS_ROLE_ARN", "secret-unused-role")
                    .env("AWS_REGION", "secret-unused-region");
            }
            if references == "json" {
                command
                    .env("AWS_WEB_IDENTITY_TOKEN_FILE", "secret-unused-file")
                    .env("AWS_ROLE_SESSION_NAME", "secret-unused-session")
                    .env(
                        "AWS_ENDPOINT_URL_STS",
                        "https://secret-unused-endpoint.invalid",
                    );
            } else if references == "mixed" {
                command
                    .env("AWS_ROLE_SESSION_NAME", "secret-selected-session")
                    .env("AWS_ENDPOINT_URL_STS", CUSTOM_STS_ENDPOINT);
            }
            let output = run_with_timeout(&mut command)?;
            assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
            assert_read_result(output, operation)?;
            let generations = signed_session_generations(&fixture, "sts")?;
            assert_eq!(generations[0], 1);
            assert!(generations.iter().any(|generation| *generation > 1));
            let assertions = assert_sts_requests(
                &proxy.requests()?,
                if references == "env" {
                    "sts.eu-west-1.amazonaws.com"
                } else {
                    "sts.secret-override.invalid"
                },
                if references == "env" {
                    "WebIdentitySession"
                } else {
                    "secret-selected-session"
                },
            );
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
fn missing_malformed_and_conflicting_references_fail_before_io() -> TestResult {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let proxy = MockCredentialProxy::new(CredentialService::AwsSts, "refresh")?;
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    fixture.write_options(proxy.add_proxy_options(json!({})))?;
    for variable in ["AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE"] {
        for (value, reason) in [
            (None, "auth_source_incomplete_credentials"),
            (Some(OsString::new()), "auth_source_invalid_value"),
            (
                Some(OsString::from("secret-invalid\nvalue")),
                "auth_source_invalid_value",
            ),
            (
                Some(OsString::from_vec(vec![0xff])),
                "auth_source_non_utf8_environment",
            ),
        ] {
            let mut command = web_identity_command(&fixture, "inspect");
            command.arg("--diagnostics=json").env_remove(variable);
            if let Some(value) = value {
                command.env(variable, value);
            }
            let output = run_with_timeout(&mut command)?;
            assert_authorization_input_error(&output, reason);
        }
    }
    for (options, reason) in [
        (
            json!({"access_key_id":"secret-key", "secret_access_key":"secret-password"}),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({"session_token":"secret-token"}),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({"role_arn":ROLE_ARN, "AWS_ROLE_ARN":ROLE_ARN}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"web_identity_token_file":"secret-file", "AWS_WEB_IDENTITY_TOKEN_FILE":"secret-file"}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"endpoint_url_sts":CUSTOM_STS_ENDPOINT, "AWS_ENDPOINT_URL_STS":CUSTOM_STS_ENDPOINT}),
            "auth_source_duplicate_option",
        ),
        (json!({"role_session_name":""}), "auth_source_invalid_value"),
        (json!({"role_arn":""}), "auth_source_invalid_value"),
        (
            json!({"web_identity_token_file":"secret-\nfile"}),
            "auth_source_invalid_value",
        ),
        (
            json!({"container_credentials_relative_uri":"/secret-container"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"metadata_endpoint":"http://secret-metadata"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"skip_signature":"true"}),
            "auth_source_unsupported_option",
        ),
    ] {
        fixture.write_options(proxy.add_proxy_options(options))?;
        let output =
            run_with_timeout(web_identity_command(&fixture, "inspect").arg("--diagnostics=json"))?;
        assert_authorization_input_error(&output, reason);
    }
    assert!(proxy.requests()?.is_empty());
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn sts_and_storage_failures_never_use_ambient_credentials() -> TestResult {
    for mode in ["reject", "reject_refresh", "storage_denied"] {
        for machine in [false, true] {
            let proxy = MockCredentialProxy::new(CredentialService::AwsSts, mode)?;
            let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
            fs::write(
                fixture.directory.path().join("assertion.jwt"),
                INITIAL_ASSERTION,
            )?;
            fixture.write_options(proxy.add_proxy_options(json!({})))?;
            if mode == "storage_denied" {
                fixture.server.deny_all_requests();
            }
            let mut command = web_identity_command(&fixture, "inspect");
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
            assert!(
                assert_sts_requests(
                    &requests,
                    "sts.eu-west-1.amazonaws.com",
                    "WebIdentitySession"
                )
                .iter()
                .all(|assertion| assertion == INITIAL_ASSERTION)
            );
            let generations = signed_session_generations(&fixture, "sts")?;
            match mode {
                "reject" => {
                    assert_eq!(requests.len(), 1);
                    assert!(generations.is_empty());
                }
                "reject_refresh" => {
                    assert_eq!(requests.len(), 2);
                    assert_eq!(generations, [1]);
                }
                _ => {
                    assert!(!generations.is_empty());
                }
            }
        }
    }
    Ok(())
}

#[test]
fn unreadable_assertions_at_startup_or_refresh_stop_the_read() -> TestResult {
    for remove_after_first_read in [false, true] {
        for machine in [false, true] {
            let proxy = MockCredentialProxy::new(CredentialService::AwsSts, "refresh")?;
            let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
            if remove_after_first_read {
                let assertion_path = fixture.directory.path().join("assertion.jwt");
                fs::write(&assertion_path, INITIAL_ASSERTION)?;
                fixture.restart_storage_with_request_hook(move |_| {
                    let _ = fs::remove_file(&assertion_path);
                })?;
            }
            fixture.write_options(proxy.add_proxy_options(json!({})))?;
            let mut command = web_identity_command(&fixture, "inspect");
            if machine {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut command)?;
            assert_failure_without_stdout(&output, 1);
            if machine {
                assert_single_terminal_record(&output.stderr, "diagnostic");
            }
            let requests = proxy.requests()?;
            let generations = signed_session_generations(&fixture, "sts")?;
            if remove_after_first_read {
                assert_eq!(
                    assert_sts_requests(
                        &requests,
                        "sts.eu-west-1.amazonaws.com",
                        "WebIdentitySession"
                    ),
                    [INITIAL_ASSERTION]
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
fn allowing_http_storage_does_not_allow_plain_http_sts() -> TestResult {
    let proxy = MockCredentialProxy::new(CredentialService::AwsSts, "refresh")?;
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    fs::write(
        fixture.directory.path().join("assertion.jwt"),
        INITIAL_ASSERTION,
    )?;
    fixture.write_options(proxy.add_proxy_options(json!({
        "endpoint_url_sts":format!("{}/secret-sts", fixture.server.endpoint)
    })))?;
    let output =
        run_with_timeout(web_identity_command(&fixture, "inspect").arg("--diagnostics=json"))?;
    assert_failure_without_stdout(&output, 1);
    assert_single_terminal_record(&output.stderr, "diagnostic");
    assert!(proxy.requests()?.is_empty());
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn web_identity_requires_s3_and_its_explicit_source() -> TestResult {
    for table in [
        "az://container/table",
        "gs://bucket/table",
        "secret-local-table",
    ] {
        let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).env_clear().args([
            "inspect",
            table,
            "--auth-source",
            "aws-web-identity",
            "--diagnostics=json",
        ]))?;
        assert_authorization_input_error(&output, "auth_source_unsupported_storage");
    }
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    fixture.write_options(json!({"role_arn":ROLE_ARN, "web_identity_token_file":"secret-file"}))?;
    for source in ["json", "env"] {
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
