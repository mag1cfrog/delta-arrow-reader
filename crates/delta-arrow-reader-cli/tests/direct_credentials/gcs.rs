//! Explicit ADC files reach GCS reads and retain the selected identity during refresh.

mod oauth_proxy;

use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
};

use serde_json::{Value, json};

use super::{
    CloudStorageProtocol, DirectStorageFixture, MockCloudStorageServer, TestResult,
    assert_read_result, assert_single_terminal_record, partitioned_objects, run_with_timeout,
};
use oauth_proxy::OAuthProxyFixture;

// Public test material generated for these fixtures; it must never grant real cloud access.
const TEST_KEY: &str = include_str!("gcs/test-key.pem");

fn service_account_adc() -> Value {
    json!({
        "type": "service_account", "private_key": TEST_KEY,
        "private_key_id": "secret-key-id", "client_email": "secret-selected@example.test",
        // ADC must not interpret these legacy service-account options as anonymous access.
        "disable_oauth": true, "gcs_base_url": "http://secret-unused-endpoint"
    })
}

fn authorized_user_adc() -> Value {
    json!({
        "type": "authorized_user", "client_id": "secret-client-id",
        "client_secret": "secret-client-password", "refresh_token": "secret-refresh-token"
    })
}

impl DirectStorageFixture {
    fn adc_path(&self) -> PathBuf {
        self.directory.path().join("secret-adc.json")
    }

    fn write_adc(&self, contents: Value) -> TestResult {
        fs::write(self.adc_path(), serde_json::to_vec(&contents)?)?;
        Ok(())
    }

    fn configure_gcs_source(&self, source: &str, mut options: Value) -> TestResult {
        let mut unused_credentials = service_account_adc();
        unused_credentials["private_key_id"] = json!("secret-unused-key-id");
        unused_credentials["client_email"] = json!("secret-unused@example.test");
        fs::write(
            self.directory.path().join("secret-unused-adc.json"),
            serde_json::to_vec(&unused_credentials)?,
        )?;
        if source == "json" {
            options["APPLICATION_CREDENTIALS"] = json!(self.adc_path());
        }
        self.write_options(options)
    }

    fn gcs_command(&self, operation: &str, source: &str) -> Command {
        let mut command = self.command(operation);
        command.args(["--auth-source", source]);
        command.env(
            "GOOGLE_APPLICATION_CREDENTIALS",
            if source == "env" {
                self.adc_path()
            } else {
                self.directory.path().join("secret-unused-adc.json")
            },
        );
        // A metadata fallback would reach the storage fixture and be recorded as an unsigned request.
        let host = self.server.endpoint.strip_prefix("http://").unwrap();
        command
            .env("GCE_METADATA_HOST", host)
            .env("GCE_METADATA_IP", host)
            .env(
                "GOOGLE_SERVICE_ACCOUNT",
                self.directory.path().join("secret-unused-adc.json"),
            )
            .env("GOOGLE_SKIP_SIGNATURE", "true");
        command
    }

    fn bearer_tokens(&self) -> Vec<String> {
        let requests = self.server.requests.lock().unwrap();
        assert!(!requests.is_empty());
        requests
            .iter()
            .map(|request| {
                assert!(!request.contains("secret-unused"), "{request}");
                request
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("authorization")
                            .then(|| value.trim().strip_prefix("Bearer "))
                            .flatten()
                    })
                    .unwrap()
                    .to_owned()
            })
            .collect()
    }

    fn assert_no_gcs_credential_leaks(&self, output: &Output) {
        for bytes in [&output.stdout, &output.stderr] {
            let text = String::from_utf8_lossy(bytes);
            assert!(!text.contains("secret"), "{text}");
            assert!(!text.contains(TEST_KEY.lines().nth(1).unwrap()), "{text}");
            for token in self.bearer_tokens() {
                assert!(!text.contains(&token), "{text}");
            }
        }
    }

    fn verify_service_account_jwts(&self) -> TestResult {
        let tokens = self.directory.path().join("tokens.json");
        fs::write(&tokens, serde_json::to_vec(&self.bearer_tokens())?)?;
        let output = run_with_timeout(
            Command::new("python3")
                .arg(
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("tests/direct_credentials/gcs/verify_jwts.py"),
                )
                .arg(tokens),
        )?;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }
}

#[test]
fn service_account_files_sign_inspection_scan_and_sql_requests() -> TestResult {
    for source in ["json", "env"] {
        let fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
        fixture.write_adc(service_account_adc())?;
        fixture.configure_gcs_source(source, json!({}))?;
        for operation in ["inspect", "scan", "query"] {
            let mut command = fixture.gcs_command(operation, source);
            command.arg("--diagnostics=json");
            if operation == "scan" {
                command.args(["--column", "id"]);
            }
            if operation == "query" {
                command
                    .arg("--sql-file")
                    .arg(fixture.directory.path().join("query.sql"));
            }
            let output = run_with_timeout(&mut command)?;
            fixture.assert_no_gcs_credential_leaks(&output);
            assert_read_result(output, operation)?;
        }
        fixture.verify_service_account_jwts()?;
    }
    Ok(())
}

#[test]
fn invalid_source_configuration_never_reaches_storage_or_metadata() -> TestResult {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
    fixture.write_adc(service_account_adc())?;
    for (source, options, environment_path, reason) in [
        (
            "json",
            json!({}),
            Some(OsString::from(fixture.adc_path())),
            "auth_source_incomplete_credentials",
        ),
        ("env", json!({}), None, "auth_source_incomplete_credentials"),
        (
            "env",
            json!({}),
            Some(OsString::new()),
            "auth_source_invalid_value",
        ),
        (
            "env",
            json!({}),
            Some(OsString::from_vec(vec![0xff])),
            "auth_source_non_utf8_environment",
        ),
        (
            "json",
            json!({"application_credentials":""}),
            None,
            "auth_source_invalid_value",
        ),
        (
            "json",
            json!({"application_credentials":fixture.adc_path(), "GOOGLE_APPLICATION_CREDENTIALS":fixture.adc_path()}),
            None,
            "auth_source_duplicate_option",
        ),
        (
            "env",
            json!({"application_credentials":fixture.adc_path()}),
            Some(OsString::from(fixture.adc_path())),
            "auth_source_conflicting_credentials",
        ),
        (
            "json",
            json!({"service_account":"secret-file"}),
            None,
            "auth_source_unsupported_option",
        ),
        (
            "json",
            json!({"service_account_key":"secret-key"}),
            None,
            "auth_source_unsupported_option",
        ),
        (
            "json",
            json!({"skip_signature":"false"}),
            None,
            "auth_source_unsupported_option",
        ),
        (
            "json",
            json!({"bucket":"secret-bucket"}),
            None,
            "auth_source_unsupported_option",
        ),
        (
            "json",
            json!({"base_url":"secret-endpoint", "GOOGLE_BASE_URL":"secret-endpoint"}),
            None,
            "auth_source_duplicate_option",
        ),
    ] {
        fixture.write_options(options)?;
        let mut command = fixture.command("inspect");
        command.args(["--auth-source", source, "--diagnostics=json"]);
        if let Some(path) = environment_path {
            command.env("GOOGLE_APPLICATION_CREDENTIALS", path);
        }
        let output = run_with_timeout(&mut command)?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "invalid_storage_authorization");
        assert_eq!(record["reason"], reason);
    }
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn missing_malformed_and_unsupported_adc_files_fail_without_fallback() -> TestResult {
    for source in ["json", "env"] {
        let fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
        fixture.configure_gcs_source(source, json!({}))?;
        for contents in [
            None,
            Some(b"secret-not-json".to_vec()),
            Some(br#"{"type":"external_account","audience":"secret-audience"}"#.to_vec()),
            Some(br#"{"type":"authorized_user","client_id":"secret-client"}"#.to_vec()),
            Some(br#"{"type":"service_account","private_key":"secret-invalid-key","private_key_id":"secret-id","client_email":"secret-email"}"#.to_vec()),
        ] {
            if let Some(contents) = contents { fs::write(fixture.adc_path(), contents)?; }
            for machine in [false, true] {
                let mut command = fixture.gcs_command("inspect", source);
                if machine { command.arg("--diagnostics=json"); }
                let output = run_with_timeout(&mut command)?;
                assert_eq!(output.status.code(), Some(2));
                assert!(output.stdout.is_empty());
                assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
                if machine {
                    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                    assert_eq!(record["reason"], "auth_source_invalid_value");
                }
            }
        }
        assert!(fixture.server.requests.lock().unwrap().is_empty());
    }
    Ok(())
}

#[test]
fn service_account_permission_denial_keeps_the_selected_identity() -> TestResult {
    for source in ["json", "env"] {
        let fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
        fixture.write_adc(service_account_adc())?;
        fixture.configure_gcs_source(source, json!({}))?;
        fixture.server.deny_all_requests();
        for machine in [false, true] {
            let mut command = fixture.gcs_command("inspect", source);
            if machine {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut command)?;
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            fixture.assert_no_gcs_credential_leaks(&output);
            if machine {
                let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                assert_eq!(record["source_kind"], "permission_denied");
            }
        }
        fixture.verify_service_account_jwts()?;
    }
    Ok(())
}

#[test]
fn authorized_user_tokens_reach_all_reads_and_refresh_the_loaded_identity() -> TestResult {
    for source in ["json", "env"] {
        let proxy = OAuthProxyFixture::new("refresh")?;
        let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
        let adc_path = fixture.adc_path();
        fixture.server = MockCloudStorageServer::with_objects(
            CloudStorageProtocol::Gcs,
            partitioned_objects()?,
            move |_, _| {
                // Every token refresh must keep the credentials loaded at command startup.
                fs::write(&adc_path, "secret-file-was-replaced").unwrap();
            },
        )?;
        fixture.configure_gcs_source(
            source,
            json!({
                "proxy_url":proxy.endpoint, "proxy_excludes":"127.0.0.1",
                "allow_invalid_certificates":"true"
            }),
        )?;
        for operation in ["inspect", "scan", "query"] {
            fixture.write_adc(authorized_user_adc())?;
            let mut command = fixture.gcs_command(operation, source);
            command.arg("--diagnostics=json");
            if operation == "scan" {
                command.args(["--column", "id"]);
            }
            if operation == "query" {
                command
                    .arg("--sql-file")
                    .arg(fixture.directory.path().join("query.sql"));
            }
            let output = run_with_timeout(&mut command)?;
            fixture.assert_no_gcs_credential_leaks(&output);
            assert_read_result(output, operation)?;
        }
        assert!(
            fixture
                .bearer_tokens()
                .iter()
                .all(|token| token.starts_with("secret-oauth-"))
        );
        let requests = proxy.requests()?;
        assert!(
            requests.len() > 3,
            "tokens did not refresh within the commands"
        );
        for request in requests {
            assert_eq!(
                request,
                json!({
                    "grant_type":["refresh_token"], "client_id":["secret-client-id"],
                    "client_secret":["secret-client-password"], "refresh_token":["secret-refresh-token"]
                })
            );
        }
    }
    Ok(())
}

#[test]
fn authorized_user_fetch_and_refresh_failures_do_not_select_another_source() -> TestResult {
    for source in ["json", "env"] {
        for mode in ["reject", "reject_refresh"] {
            let proxy = OAuthProxyFixture::new(mode)?;
            let fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
            fixture.write_adc(authorized_user_adc())?;
            fixture.configure_gcs_source(
                source,
                json!({
                    "proxy_url":proxy.endpoint, "proxy_excludes":"127.0.0.1",
                    "allow_invalid_certificates":"true"
                }),
            )?;
            let output = run_with_timeout(
                fixture
                    .gcs_command("inspect", source)
                    .arg("--diagnostics=json"),
            )?;
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            assert_single_terminal_record(&output.stderr, "diagnostic");
            let requests = proxy.requests()?;
            assert_eq!(requests.len(), if mode == "reject" { 1 } else { 2 });
            for request in requests {
                assert_eq!(request["client_id"], json!(["secret-client-id"]));
            }
            if mode == "reject" {
                assert!(fixture.server.requests.lock().unwrap().is_empty());
            } else {
                assert!(
                    fixture
                        .bearer_tokens()
                        .iter()
                        .all(|token| token == "secret-oauth-1")
                );
            }
        }
    }
    Ok(())
}
