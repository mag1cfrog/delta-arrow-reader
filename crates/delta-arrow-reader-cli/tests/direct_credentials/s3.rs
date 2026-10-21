//! S3 source selection reaches signed requests without credential fallback or leaks.

mod container_credentials;
mod web_identity;

use serde_json::{Map, Value, json};
use std::process::Command;

use super::{
    CloudStorageProtocol, DirectStorageFixture, TestResult, assert_authorization_input_error,
    assert_failure_without_stdout, assert_read_result, assert_single_terminal_record,
    run_with_timeout,
};

impl DirectStorageFixture {
    fn assert_signed_s3_requests(
        &self,
        access_key_id: &str,
        secret_access_key: &str,
        token: Option<&str>,
        region: &str,
    ) -> TestResult {
        let requests = self.server.requests.lock().unwrap();
        assert!(!requests.is_empty());
        for request in requests.iter() {
            assert!(
                request.contains(&format!("Credential={access_key_id}/")),
                "{request}"
            );
            assert!(
                request.contains(&format!("/{region}/s3/aws4_request")),
                "{request}"
            );
            assert!(request.contains("Signature="), "{request}");
            let sent_token = request.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("x-amz-security-token")
                    .then(|| value.trim())
            });
            assert_eq!(sent_token, token, "{request}");
            assert!(!request.contains("secret-unused"));
        }
        drop(requests);
        self.verify_request_signatures(json!({access_key_id: secret_access_key}))
    }
}

fn signed_session_generations(
    fixture: &DirectStorageFixture,
    source: &str,
) -> Result<Vec<usize>, Box<dyn std::error::Error>> {
    let generations: Vec<usize> = fixture
        .server
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|request| {
            let (_, credential) = request
                .split_once(&format!("Credential=secret-{source}-key-"))
                .unwrap();
            let generation: usize = credential.split_once('/').unwrap().0.parse().unwrap();
            assert!(request.contains("/eu-west-1/s3/aws4_request"), "{request}");
            assert!(request.contains("Signature="), "{request}");
            let token = request.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("x-amz-security-token")
                    .then(|| value.trim())
            });
            assert_eq!(
                token,
                Some(format!("secret-{source}-session-{generation}").as_str())
            );
            for forbidden in ["secret-unused", "password", "assertion"] {
                assert!(!request.contains(forbidden), "{request}");
            }
            generation
        })
        .collect();
    if !generations.is_empty() {
        let signing_keys: Map<String, Value> = generations
            .iter()
            .map(|generation| {
                (
                    format!("secret-{source}-key-{generation}"),
                    json!(format!("secret-{source}-password-{generation}")),
                )
            })
            .collect();
        fixture.verify_request_signatures(json!(signing_keys))?;
    }
    Ok(generations)
}

#[test]
fn selected_credentials_sign_inspection_scan_and_sql_requests() -> TestResult {
    for (source, options, environment_region) in [
        (
            "json",
            json!({
                "ACCESS_KEY_ID": "secret-json-key", "secret_access_key": "secret-json-password",
                "AWS_TOKEN": "secret-json-session", "region": "eu-west-1"
            }),
            "secret-unused-region",
        ),
        (
            "env",
            json!({"aws_default_region": "eu-west-1"}),
            "secret-unused-region",
        ),
        ("env", json!({}), "eu-west-1"),
    ] {
        let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
        fixture.write_options(options)?;
        for operation in ["inspect", "scan", "query"] {
            let mut command = fixture.command(operation);
            command
                .args(["--auth-source", source, "--diagnostics=json"])
                .env("AWS_ACCESS_KEY_ID", "secret-env-key")
                .env("AWS_SECRET_ACCESS_KEY", "secret-env-password")
                .env("AWS_SESSION_TOKEN", "secret-env-session")
                .env("AWS_REGION", environment_region)
                .env("AWS_DEFAULT_REGION", "secret-unused-default-region")
                .env("AWS_SKIP_SIGNATURE", "true")
                .env("AWS_PROFILE", "secret-unused-profile")
                .env("AWS_WEB_IDENTITY_TOKEN_FILE", "secret-unused-file");
            let output = run_with_timeout(&mut command)?;
            assert_read_result(output, operation)?;
        }
        let (key, secret_key, token) = if source == "json" {
            (
                "secret-json-key",
                "secret-json-password",
                "secret-json-session",
            )
        } else {
            (
                "secret-env-key",
                "secret-env-password",
                "secret-env-session",
            )
        };
        fixture.assert_signed_s3_requests(key, secret_key, Some(token), "eu-west-1")?;
    }
    Ok(())
}

#[test]
fn invalid_sources_fail_before_storage_io_even_with_usable_ambient_credentials() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    for (options, reason) in [
        (json!({}), "auth_source_incomplete_credentials"),
        (
            json!({"AWS_TOKEN": "secret-token"}),
            "auth_source_incomplete_credentials",
        ),
        (
            json!({"access_key_id": "secret-key"}),
            "auth_source_incomplete_credentials",
        ),
        (
            json!({"secret_access_key": "secret-password"}),
            "auth_source_incomplete_credentials",
        ),
        (json!({"access_key_id": ""}), "auth_source_invalid_value"),
        (
            json!({"access_key_id": "secret-key\n"}),
            "auth_source_invalid_value",
        ),
        (
            json!({"access_key_id": "secret-key", "AWS_ACCESS_KEY_ID": "secret-key"}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"aws_token": "secret-token", "session_token": "secret-token"}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"allow_http": "true", "AWS_ALLOW_HTTP": "true"}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"aws_skip_signature": "false"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"aws_role_arn": "secret-role"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"secret-unknown": "secret-value"}),
            "auth_source_unsupported_option",
        ),
    ] {
        fixture.write_options(options)?;
        let output = run_with_timeout(
            fixture
                .command("inspect")
                .args(["--auth-source", "json", "--diagnostics=json"])
                .env("AWS_ACCESS_KEY_ID", "secret-ambient-key")
                .env("AWS_SECRET_ACCESS_KEY", "secret-ambient-password"),
        )?;
        assert_authorization_input_error(&output, reason);
    }
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn invalid_region_options_fail_before_credentials_or_storage() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    for source in ["json", "env", "aws-web-identity", "aws-container"] {
        for (key, value) in [
            ("region", ""),
            ("AWS_REGION", "eu-west-1\nsecret-region"),
            ("default_region", "   "),
            ("AWS_DEFAULT_REGION", "eu-west-1\rsecret-region"),
        ] {
            let mut options = json!({});
            options[key] = json!(value);
            if source == "json" {
                options["access_key_id"] = json!("secret-key");
                options["secret_access_key"] = json!("secret-password");
            }
            fixture.write_options(options)?;
            let output = run_with_timeout(
                fixture
                    .command("inspect")
                    .args(["--auth-source", source, "--diagnostics=json"])
                    .env("AWS_ACCESS_KEY_ID", "secret-key")
                    .env("AWS_SECRET_ACCESS_KEY", "secret-password")
                    .env("AWS_REGION", "eu-west-1"),
            )?;
            assert_authorization_input_error(&output, "auth_source_invalid_value");
        }
    }
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn environment_requires_one_complete_family_and_rejects_json_credentials() -> TestResult {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    for (options, key, password, reason) in [
        (json!({}), None, None, "auth_source_incomplete_credentials"),
        (
            json!({}),
            Some(OsString::from("secret-key")),
            None,
            "auth_source_incomplete_credentials",
        ),
        (
            json!({}),
            Some(OsString::new()),
            Some("secret-password"),
            "auth_source_invalid_value",
        ),
        (
            json!({}),
            Some(OsString::from_vec(vec![0xff])),
            Some("secret-password"),
            "auth_source_non_utf8_environment",
        ),
        (
            json!({"access_key_id":"secret-file-key", "secret_access_key":"secret-file-password"}),
            Some(OsString::from("secret-key")),
            Some("secret-password"),
            "auth_source_conflicting_credentials",
        ),
    ] {
        fixture.write_options(options)?;
        let mut command = fixture.command("inspect");
        command.args(["--auth-source", "env", "--diagnostics=json"]);
        if let Some(key) = key {
            command.env("AWS_ACCESS_KEY_ID", key);
        }
        if let Some(password) = password {
            command.env("AWS_SECRET_ACCESS_KEY", password);
        }
        let output = run_with_timeout(&mut command)?;
        assert_authorization_input_error(&output, reason);
    }
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn selected_credentials_never_fall_back_after_permission_denial() -> TestResult {
    for source in ["json", "env"] {
        let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
        fixture.server.deny_all_requests();
        fixture.write_options(if source == "json" {
            json!({"access_key_id":"secret-selected-key", "secret_access_key":"secret-selected-password", "region":"eu-west-1"})
        } else { json!({"region":"eu-west-1"}) })?;
        for machine in [false, true] {
            let mut command = fixture.command("inspect");
            command
                .args(["--auth-source", source])
                .env(
                    "AWS_ACCESS_KEY_ID",
                    if source == "json" {
                        "secret-unused-key"
                    } else {
                        "secret-selected-key"
                    },
                )
                .env("AWS_SECRET_ACCESS_KEY", "secret-environment-password")
                .env(
                    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                    format!("{}/secret-unused-provider", fixture.server.endpoint),
                )
                .env(
                    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
                    "secret-unused-file",
                );
            if machine {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut command)?;
            assert_failure_without_stdout(&output, 1);
            if machine {
                let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                assert_eq!(record["source_kind"], "permission_denied");
            }
        }
        let secret_key = if source == "json" {
            "secret-selected-password"
        } else {
            "secret-environment-password"
        };
        fixture.assert_signed_s3_requests("secret-selected-key", secret_key, None, "eu-west-1")?;
    }
    Ok(())
}

#[test]
fn source_selection_requires_supported_targets_and_explicit_files() -> TestResult {
    for (args, reason) in [
        (
            vec!["inspect", "s3://bucket/table", "--auth-source", "json"],
            "auth_source_requires_storage_options_file",
        ),
        (
            vec!["inspect", "secret-local-path", "--auth-source", "env"],
            "auth_source_unsupported_storage",
        ),
        (
            vec!["inspect", "memory://bucket/table", "--auth-source", "env"],
            "auth_source_unsupported_storage",
        ),
        (
            vec![
                "inspect",
                "s3://secret-key:secret-password@bucket/table",
                "--auth-source",
                "env",
            ],
            "auth_source_invalid_table_url",
        ),
        (
            vec![
                "inspect",
                "uc://main.sales.orders",
                "--databricks-profile",
                "secret-profile",
                "--auth-source",
                "env",
            ],
            "auth_source_conflicts_with_unity_catalog",
        ),
    ] {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .env_clear()
                .args(args)
                .arg("--diagnostics=json"),
        )?;
        assert_authorization_input_error(&output, reason);
    }
    Ok(())
}

#[test]
fn omitting_source_preserves_existing_storage_options() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    fixture.write_options(
        json!({"aws_skip_signature": "true", "unrecognized_option": "secret-ignored"}),
    )?;
    let output = run_with_timeout(fixture.command("inspect").arg("--diagnostics=json"))?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_single_terminal_record(&output.stderr, "completion");
    let requests = fixture.server.requests.lock().unwrap();
    assert!(!requests.is_empty());
    assert!(
        requests
            .iter()
            .all(|request| !request.to_ascii_lowercase().contains("authorization:"))
    );
    Ok(())
}
