//! Explicit ADC files reach GCS reads and retain the selected identity during refresh.

use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
};

use serde_json::{Value, json};

use super::{
    CloudStorageProtocol, DirectStorageFixture, TestResult, assert_authorization_input_error,
    assert_failure_without_stdout, assert_read_result, assert_single_terminal_record,
    mock_credential_proxy::{CredentialService, MockCredentialProxy},
    run_python_verifier, run_with_timeout,
};

// Public test material generated for these fixtures; it must never grant real cloud access.
const TEST_PRIVATE_KEY_PEM: &str = include_str!("test-key.pem");

fn service_account_adc() -> Value {
    json!({
        "type": "service_account", "private_key": TEST_PRIVATE_KEY_PEM,
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

    fn assert_no_gcs_credential_leaks(&self, output: &Output) {
        let tokens = self.storage_bearer_tokens();
        assert!(!tokens.is_empty());
        for bytes in [&output.stdout, &output.stderr] {
            let text = String::from_utf8_lossy(bytes);
            assert!(!text.contains("secret"), "{text}");
            assert!(
                !text.contains(TEST_PRIVATE_KEY_PEM.lines().nth(1).unwrap()),
                "{text}"
            );
            for token in &tokens {
                assert!(!text.contains(token), "{text}");
            }
        }
    }

    fn verify_service_account_jwts(&self) -> TestResult {
        let tokens = self.directory.path().join("tokens.json");
        fs::write(&tokens, serde_json::to_vec(&self.storage_bearer_tokens())?)?;
        run_python_verifier("gcs/verify_jwts.py", &tokens)
    }
}

#[test]
fn service_account_files_sign_all_read_commands() -> TestResult {
    for source in ["json", "env"] {
        let fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
        fixture.write_adc(service_account_adc())?;
        fixture.configure_gcs_source(source, json!({}))?;
        for operation in ["inspect", "scan", "preview", "query"] {
            let mut command = fixture.gcs_command(operation, source);
            command.arg("--diagnostics=json");
            let output = run_with_timeout(&mut command)?;
            fixture.assert_no_gcs_credential_leaks(&output);
            assert_read_result(output, operation)?;
        }
        fixture.verify_service_account_jwts()?;
    }
    Ok(())
}

#[test]
fn env_file_adc_reference_belongs_to_the_env_files_directory() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
    fixture.write_adc(service_account_adc())?;
    fixture.write_options(json!({}))?;
    let directory = fixture.directory.path();
    fs::create_dir(directory.join("config"))?;
    fs::rename(fixture.adc_path(), directory.join("config/secret-adc.json"))?;
    fs::write(
        directory.join("config/selected.env"),
        "GOOGLE_APPLICATION_CREDENTIALS=secret-adc.json",
    )?;
    let output = run_with_timeout(
        fixture
            .command("query")
            .current_dir(directory)
            .args(["--env-file", "config/selected.env", "--diagnostics=json"])
            .env("GOOGLE_APPLICATION_CREDENTIALS", "secret-unused-path"),
    )?;
    fixture.assert_no_gcs_credential_leaks(&output);
    assert_read_result(output, "query")?;
    fixture.verify_service_account_jwts()?;
    Ok(())
}

#[test]
fn manifest_adc_references_use_the_containing_file_and_environment_keeps_the_working_directory()
-> TestResult {
    for source in ["json", "env", "env-file"] {
        let fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
        let directory = fixture.directory.path();
        fs::create_dir_all(directory.join("config/identities"))?;
        fs::create_dir(directory.join("manifests"))?;
        fixture.write_adc(service_account_adc())?;
        fs::rename(
            fixture.adc_path(),
            directory.join("config/identities/secret-adc.json"),
        )?;
        fixture.write_options(if source == "json" {
            json!({"APPLICATION_CREDENTIALS":"identities/secret-adc.json"})
        } else {
            json!({})
        })?;
        fs::rename(
            directory.join("options.json"),
            directory.join("config/storage.json"),
        )?;
        let mut authorization =
            json!({"source":source,"storage_options_file":"../config/storage.json"});
        if source == "env-file" {
            fs::create_dir_all(directory.join("env/nested"))?;
            fs::write(
                directory.join("env/nested/selected.env"),
                "GOOGLE_APPLICATION_CREDENTIALS=../../config/identities/secret-adc.json",
            )?;
            authorization["env_file"] = json!("../env/nested/selected.env");
        }
        fs::write(
            directory.join("manifests/tables.json"),
            json!({"format_version":1,"tables":[{
                "alias":"orders", "location":"gs://bucket/table",
                "authorization":authorization
            }]})
            .to_string(),
        )?;
        fs::write(
            directory.join("query.sql"),
            "SELECT id FROM orders ORDER BY id",
        )?;
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .env_clear()
                .env("RUST_LOG", "trace")
                .current_dir(directory)
                .env(
                    "GOOGLE_APPLICATION_CREDENTIALS",
                    if source == "env" {
                        "config/identities/secret-adc.json"
                    } else {
                        "secret-unused-file"
                    },
                )
                .args([
                    "query",
                    "--tables-file",
                    "manifests/tables.json",
                    "--sql-file",
                    "query.sql",
                    "--diagnostics=json",
                ]),
        )?;
        fixture.assert_no_gcs_credential_leaks(&output);
        assert_read_result(output, "query")?;
        fixture.verify_service_account_jwts()?;
    }
    Ok(())
}

#[test]
fn jwt_verifier_rejects_a_valid_signature_from_the_wrong_principal() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
    let mut credentials = service_account_adc();
    credentials["client_email"] = json!("secret-other@example.test");
    fixture.write_adc(credentials)?;
    fixture.configure_gcs_source("json", json!({}))?;
    let output = run_with_timeout(
        fixture
            .gcs_command("inspect", "json")
            .arg("--diagnostics=json"),
    )?;
    assert_read_result(output, "inspect")?;
    let error = fixture
        .verify_service_account_jwts()
        .expect_err("a valid signature must not bypass the principal check");
    assert!(error.to_string().contains("AssertionError"), "{error}");
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
        assert_authorization_input_error(&output, reason);
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
                assert_failure_without_stdout(&output, 2);
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
            assert_failure_without_stdout(&output, 1);
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
        let proxy = MockCredentialProxy::new(CredentialService::GoogleOAuth, "refresh")?;
        let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
        let adc_path = fixture.adc_path();
        fixture.restart_storage_with_request_hook(move |_| {
            // Every token refresh must keep the credentials loaded at command startup.
            fs::write(&adc_path, "secret-file-was-replaced").unwrap();
        })?;
        fixture.configure_gcs_source(source, proxy.add_proxy_options(json!({})))?;
        let mut oauth_request_count = 0;
        let mut storage_request_count = 0;
        for operation in ["inspect", "scan", "preview", "query"] {
            fixture.write_adc(authorized_user_adc())?;
            let mut command = fixture.gcs_command(operation, source);
            command.arg("--diagnostics=json");
            let output = run_with_timeout(&mut command)?;
            fixture.assert_no_gcs_credential_leaks(&output);
            assert_read_result(output, operation)?;
            let requests = proxy.requests()?;
            let current_requests = &requests[oauth_request_count..];
            assert!(
                current_requests.len() > 1,
                "{source} {operation}: token was not refreshed"
            );
            let tokens = fixture.storage_bearer_tokens();
            let current_tokens: std::collections::HashSet<_> =
                tokens[storage_request_count..].iter().collect();
            assert!(
                current_tokens.len() > 1,
                "{source} {operation}: storage kept using one token"
            );
            let issued_tokens: std::collections::HashSet<_> = (oauth_request_count + 1
                ..=requests.len())
                .map(|index| format!("secret-oauth-{index}"))
                .collect();
            assert!(
                current_tokens
                    .iter()
                    .all(|token| issued_tokens.contains(*token))
            );
            eprintln!(
                "GCS refresh source={source} command={operation} oauth_requests={} distinct_storage_tokens={}",
                current_requests.len(),
                current_tokens.len()
            );
            for request in current_requests {
                assert_eq!(
                    *request,
                    json!({
                        "grant_type":["refresh_token"], "client_id":["secret-client-id"],
                        "client_secret":["secret-client-password"], "refresh_token":["secret-refresh-token"]
                    })
                );
            }
            oauth_request_count = requests.len();
            storage_request_count = tokens.len();
        }
    }
    Ok(())
}

#[test]
fn authorized_user_fetch_and_refresh_failures_do_not_select_another_source() -> TestResult {
    for source in ["json", "env"] {
        for mode in ["reject", "reject_refresh"] {
            let proxy = MockCredentialProxy::new(CredentialService::GoogleOAuth, mode)?;
            let fixture = DirectStorageFixture::new(CloudStorageProtocol::Gcs)?;
            fixture.write_adc(authorized_user_adc())?;
            fixture.configure_gcs_source(source, proxy.add_proxy_options(json!({})))?;
            let output = run_with_timeout(
                fixture
                    .gcs_command("inspect", source)
                    .arg("--diagnostics=json"),
            )?;
            assert_failure_without_stdout(&output, 1);
            assert_single_terminal_record(&output.stderr, "diagnostic");
            let requests = proxy.requests()?;
            assert_eq!(requests.len(), if mode == "reject" { 1 } else { 2 });
            for request in requests {
                assert_eq!(request["client_id"], json!(["secret-client-id"]));
            }
            if mode == "reject" {
                assert!(fixture.server.requests.lock().unwrap().is_empty());
            } else {
                let tokens = fixture.storage_bearer_tokens();
                assert!(!tokens.is_empty());
                assert!(tokens.iter().all(|token| token == "secret-oauth-1"));
            }
        }
    }
    Ok(())
}
