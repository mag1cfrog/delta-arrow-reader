use std::{
    collections::HashMap,
    fs,
    io::{Read, Write},
    net::TcpListener,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use arrow::ipc::reader::FileReader;
use serde_json::{Value, json};
use url::Url;

use super::{check_failure, run_with_timeout};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const TABLE_ID: &str = "12345678-1234-1234-1234-123456789abc";
const TABLE_NAME: &str = "main.sales.orders";
const PROFILE: &str = "secret profile $(do-not-run)";

/// Credential forms returned by the fake Databricks CLI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StorageCredentialKind {
    S3,
    R2,
    AzureSas,
    AzureBearer,
    Gcs,
}

/// A fake Databricks executable with controllable table metadata and credential responses.
struct DatabricksCliFixture {
    directory: tempfile::TempDir,
}

impl DatabricksCliFixture {
    fn new(credential_kind: StorageCredentialKind) -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let location = match credential_kind {
            StorageCredentialKind::S3 => "s3://bucket/table/",
            StorageCredentialKind::R2 => "r2://bucket@account.r2.cloudflarestorage.com/table/",
            StorageCredentialKind::AzureSas | StorageCredentialKind::AzureBearer => {
                "abfss://container@account.dfs.core.windows.net/table/"
            }
            StorageCredentialKind::Gcs => "gs://bucket/table/",
        };
        fs::write(
            directory.path().join("metadata.json"),
            serde_json::to_vec(&json!({
                "table_id": TABLE_ID,
                "full_name": TABLE_NAME,
                "table_type": "EXTERNAL",
                "data_source_format": "DELTA",
                "storage_location": location,
                "securable_kind_manifest": {
                    "securable_type": "TABLE",
                    "securable_kind": "TABLE_DELTA_EXTERNAL",
                    "capabilities": []
                },
                "columns": [{"name": "not_the_delta_schema", "type_name": "STRING"}]
            }))?,
        )?;
        let expiration_time = (SystemTime::now().duration_since(UNIX_EPOCH)?
            + Duration::from_secs(300))
        .as_millis() as u64;
        let mut credentials = json!({
            "url": location,
            "expiration_time": expiration_time
        });
        match credential_kind {
            StorageCredentialKind::S3 | StorageCredentialKind::R2 => {
                let credential_field = if credential_kind == StorageCredentialKind::R2 {
                    // Exercise equivalent catalog and native R2 URL forms across the two responses.
                    credentials["url"] =
                        json!("https://account.r2.cloudflarestorage.com/bucket/table/");
                    "r2_temp_credentials"
                } else {
                    "aws_temp_credentials"
                };
                credentials[credential_field] = json!({
                    "access_key_id": "test-key",
                    "secret_access_key": "secret-sentinel",
                    "session_token": "test-session"
                });
            }
            StorageCredentialKind::AzureSas => {
                credentials["azure_user_delegation_sas"] =
                    json!({"sas_token":"sv=test&sp=rl&sig=test%2Bsignature"});
            }
            StorageCredentialKind::AzureBearer => {
                credentials["azure_aad"] = json!({"aad_token":"secret-bearer-token"});
            }
            StorageCredentialKind::Gcs => {
                credentials["gcp_oauth_token"] = json!({"oauth_token":"secret-gcp-token"});
            }
        }
        fs::write(
            directory.path().join("credentials.json"),
            serde_json::to_vec(&credentials)?,
        )?;
        let executable = directory.path().join("databricks");
        fs::write(
            &executable,
            r#"#!/bin/sh
set -eu
printf '%s\000' "$@" >> "$DAR_TEST_DIR/calls"
if IFS= read -r unused; then exit 98; fi
if [ -n "${DAR_TEST_PID1_SIGNAL:-}" ]; then
  printf '%s' "$PPID" > "$DAR_TEST_DIR/parent_pid"
  [ "$PPID" = 1 ] || exit 96
  kill -"$DAR_TEST_PID1_SIGNAL" "$PPID"
  exec /bin/sleep 60
fi
if [ "${DAR_TEST_HANG:-}" = 1 ]; then
  printf '%s' "$$" > "$DAR_TEST_DIR/pid"
  exec /bin/sleep 60
fi
case "$1" in
  tables) /bin/cat "$DAR_TEST_DIR/metadata.json" ;;
  temporary-table-credentials)
    if [ "${DAR_TEST_DENIED:-}" = 1 ]; then printf 'secret-sentinel\n' >&2; exit 1; fi
    if [ -n "${DAR_TEST_REFRESH:-}" ]; then
      if [ -e "$DAR_TEST_DIR/initial_grant_issued" ]; then
        case "$DAR_TEST_REFRESH" in
          deny) printf 'secret-sentinel\n' >&2; exit 1 ;;
          hang) printf '%s' "$$" > "$DAR_TEST_DIR/pid"; exec /bin/sleep 60 ;;
        esac
        /bin/cat "$DAR_TEST_DIR/credentials.next.json"
        exit 0
      fi
      : > "$DAR_TEST_DIR/initial_grant_issued"
    fi
    /bin/cat "$DAR_TEST_DIR/credentials.json"
    ;;
  *) exit 97 ;;
esac
"#,
        )?;
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755))?;
        let options = if credential_kind == StorageCredentialKind::S3 {
            json!({"aws_region":"eu-west-1"})
        } else {
            json!({})
        };
        fs::write(
            directory.path().join("options.json"),
            serde_json::to_vec(&options)?,
        )?;
        Ok(Self { directory })
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command
            .args(args)
            .env_remove("RUST_LOG")
            .env("PATH", self.directory.path())
            .env("DAR_TEST_DIR", self.directory.path());
        command
    }

    fn inspect(&self) -> Command {
        let mut command = self.command(&[
            "inspect",
            "uc://main.sales.orders",
            "--databricks-profile",
            PROFILE,
        ]);
        command
            .arg("--storage-options-file")
            .arg(self.directory.path().join("options.json"));
        command
    }

    fn use_storage_endpoint(&self, kind: StorageCredentialKind, endpoint: &str) -> TestResult {
        let options = match kind {
            StorageCredentialKind::S3 => json!({
                "aws_endpoint": endpoint, "aws_allow_http": "true", "aws_region": "eu-west-1"
            }),
            StorageCredentialKind::R2 => json!({
                "aws_endpoint_url_s3": endpoint, "aws_allow_http": "true"
            }),
            StorageCredentialKind::AzureSas | StorageCredentialKind::AzureBearer => json!({
                "azure_endpoint": endpoint, "azure_allow_http": "true"
            }),
            StorageCredentialKind::Gcs => json!({
                "google_base_url": endpoint, "google_allow_http": "true"
            }),
        };
        fs::write(
            self.directory.path().join("options.json"),
            serde_json::to_vec(&options)?,
        )?;
        Ok(())
    }

    /// Let the initial grant expire while the first storage response is pending.
    fn storage_with_expiring_credentials(
        &self,
        kind: StorageCredentialKind,
        update_replacement: impl FnOnce(&mut Value),
    ) -> Result<MockCloudStorageServer, Box<dyn std::error::Error>> {
        let credentials_path = self.directory.path().join("credentials.json");
        let mut initial: Value = serde_json::from_slice(&fs::read(&credentials_path)?)?;
        let mut replacement = initial.clone();
        update_replacement(&mut replacement);
        fs::write(
            self.directory.path().join("credentials.next.json"),
            serde_json::to_vec(&replacement)?,
        )?;
        let expires_at = SystemTime::now() + Duration::from_secs(3);
        initial["expiration_time"] =
            json!(expires_at.duration_since(UNIX_EPOCH)?.as_millis() as u64);
        fs::write(credentials_path, serde_json::to_vec(&initial)?)?;
        let log = fs::read(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
            "../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table/_delta_log/00000000000000000000.json",
        ))?;
        let mut first_request = true;
        let metadata_path = self.directory.path().join("metadata.json");
        let server = MockCloudStorageServer::with_request_hook(kind, log, move |_| {
            if first_request {
                first_request = false;
                // Re-resolving the table name after authorization must not be part of renewal.
                fs::write(&metadata_path, "secret-table-was-replaced").unwrap();
                thread::sleep(
                    expires_at
                        .duration_since(SystemTime::now())
                        .unwrap_or_default()
                        + Duration::from_millis(20),
                );
            }
        })?;
        self.use_storage_endpoint(kind, &server.endpoint)?;
        Ok(server)
    }

    fn calls(&self) -> Vec<String> {
        fs::read(self.directory.path().join("calls"))
            .unwrap_or_default()
            .split(|b| *b == 0)
            .filter(|arg| !arg.is_empty())
            .map(|arg| String::from_utf8(arg.to_vec()).unwrap())
            .collect()
    }
}

#[test]
fn inspection_uses_vended_cloud_credentials_and_actual_delta_schema() -> TestResult {
    let corpus = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned",
    );
    let schema =
        FileReader::try_new(fs::File::open(corpus.join("expected.arrow"))?, None)?.schema();
    let log = fs::read(corpus.join("table/_delta_log/00000000000000000000.json"))?;
    for credential_kind in [
        StorageCredentialKind::S3,
        StorageCredentialKind::R2,
        StorageCredentialKind::AzureSas,
        StorageCredentialKind::AzureBearer,
        StorageCredentialKind::Gcs,
    ] {
        let fixture = DatabricksCliFixture::new(credential_kind)?;
        let server = MockCloudStorageServer::new(credential_kind, log.clone())?;
        fixture.use_storage_endpoint(credential_kind, &server.endpoint)?;
        for (historical, verbosity, machine) in [
            (false, "", false),
            (true, "", false),
            (false, "trace", false),
            (false, "off", true),
            (true, "trace", true),
        ] {
            if credential_kind == StorageCredentialKind::AzureSas && historical {
                // Raw '+' and percent-encoded '+' must produce the same SAS signature.
                let path = fixture.directory.path().join("credentials.json");
                let mut credentials: Value = serde_json::from_slice(&fs::read(&path)?)?;
                credentials["azure_user_delegation_sas"] =
                    json!({"sas_token":"?sv=test&sp=rl&sig=test+signature"});
                fs::write(path, serde_json::to_vec(&credentials)?)?;
            }
            let mut command = fixture.inspect();
            command
                .env("AWS_ACCESS_KEY_ID", "ambient-key")
                .env("AZURE_STORAGE_ACCOUNT_KEY", "invalid-ambient-secret")
                .env("AZURE_STORAGE_TOKEN", "ambient-token")
                .env("AZURE_STORAGE_SAS_KEY", "sig=ambient-signature")
                .env("GOOGLE_APPLICATION_CREDENTIALS", "secret-missing-adc.json")
                .env(
                    "GOOGLE_SERVICE_ACCOUNT",
                    "secret-missing-service-account.json",
                )
                .env("GOOGLE_SKIP_SIGNATURE", "true");
            if !verbosity.is_empty() {
                command.env("RUST_LOG", verbosity);
            }
            if historical {
                command.args(["--table-version", "0"]);
            }
            if machine {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut command)?;
            assert!(
                output.status.success(),
                "{credential_kind:?}: {}, requests={:?}",
                String::from_utf8_lossy(&output.stderr),
                server
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|r| r.lines().next())
                    .collect::<Vec<_>>()
            );
            let diagnostic = std::str::from_utf8(&output.stderr)?;
            if machine {
                let record =
                    super::diagnostics::assert_single_terminal_record(&output.stderr, "completion");
                assert_eq!(record["command"], "inspect");
                assert_eq!(record["bindings"][0]["table_version"], "0");
                assert_eq!(record["output_bytes"], output.stdout.len().to_string());
            } else if verbosity.is_empty() {
                assert!(diagnostic.is_empty());
            } else {
                assert!(diagnostic.contains("event=\"table_load.completed\""));
            }
            if machine || !verbosity.is_empty() {
                for sentinel in [
                    PROFILE,
                    TABLE_NAME,
                    TABLE_ID,
                    "secret",
                    "test-key",
                    "test-session",
                    "test%2Bsignature",
                    "secret-bearer-token",
                    "bucket/table",
                    "account.dfs",
                    "account.r2",
                ] {
                    assert!(!diagnostic.contains(sentinel), "{diagnostic}");
                }
            }
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stdout)?,
                json!({
                    "format_version": 1,
                    "table_version": "0",
                    "schema": schema
                })
            );
        }
        let profile = format!("--profile={PROFILE}");
        let expected = [
            "tables",
            "get",
            "--include-manifest-capabilities",
            &profile,
            "--output",
            "json",
            "--",
            TABLE_NAME,
            "temporary-table-credentials",
            "generate-temporary-table-credentials",
            "--table-id",
            TABLE_ID,
            "--operation",
            "READ",
            &profile,
            "--output",
            "json",
        ];
        assert_eq!(fixture.calls(), expected.repeat(5));
        let requests = server.requests.lock().unwrap();
        assert!(!requests.is_empty());
        for request in requests.iter() {
            let first = request.lines().next().unwrap();
            let request_url = Url::parse(&format!(
                "http://localhost{}",
                request
                    .split_whitespace()
                    .nth(1)
                    .ok_or("missing request target")?
            ))?;
            let request_path = object_store::path::Path::from_url_path(request_url.path())?;
            assert!(
                !request_path.as_ref().ends_with(".parquet")
                    && !request_path.as_ref().ends_with(".bin"),
                "{first}"
            );
            match credential_kind {
                StorageCredentialKind::Gcs => {
                    assert!(
                        request
                            .to_ascii_lowercase()
                            .contains("authorization: bearer secret-gcp-token\r\n"),
                        "missing UC GCS token"
                    );
                    assert!(!first.contains("sig="), "unexpected SAS credential");
                }
                StorageCredentialKind::AzureBearer => {
                    assert!(
                        request
                            .to_ascii_lowercase()
                            .contains("authorization: bearer secret-bearer-token\r\n"),
                        "missing UC bearer token"
                    );
                    assert!(!first.contains("sig="), "unexpected SAS credential");
                }
                StorageCredentialKind::AzureSas => {
                    assert!(first.contains("sig=test%2Bsignature"), "{first}");
                }
                StorageCredentialKind::S3 | StorageCredentialKind::R2 => {
                    let region = if credential_kind == StorageCredentialKind::R2 {
                        "auto"
                    } else {
                        "eu-west-1"
                    };
                    assert!(
                        request.contains("Credential=test-key/")
                            && request.contains(&format!("/{region}/s3/aws4_request")),
                        "missing UC signing credential or region"
                    );
                    assert!(
                        request
                            .to_ascii_lowercase()
                            .contains("x-amz-security-token: test-session")
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn help_malformed_input_and_direct_paths_do_not_invoke_databricks() -> TestResult {
    let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
    for args in [
        vec!["--help"],
        vec!["--version"],
        vec!["inspect", "--help"],
        vec!["inspect", "uc://a.b.c"],
        vec!["inspect", "uc://a.b", "--databricks-profile", "work"],
        vec!["scan", "uc://a.b.c", "--databricks-profile", "work"],
        vec!["inspect", "local", "--databricks-profile", "work"],
    ] {
        let output = run_with_timeout(&mut fixture.command(&args))?;
        if !args
            .iter()
            .any(|arg| matches!(*arg, "--help" | "--version"))
        {
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
        }
    }
    for key in [
        "AWS_TOKEN",
        "azure_storage_sas_key",
        "bucket",
        "account_name",
    ] {
        fs::write(
            fixture.directory.path().join("options.json"),
            serde_json::to_vec(&json!({key:"secret-sentinel"}))?,
        )?;
        let output = run_with_timeout(fixture.inspect().env("RUST_LOG", "trace"))?;
        check_failure(
            &output.stderr,
            "configuration",
            "invalid_uc_storage_options",
        );
    }
    assert!(fixture.calls().is_empty());
    let local = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(
        "../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table",
    );
    let output = run_with_timeout(fixture.command(&["inspect"]).arg(local).env("PATH", ""))?;
    assert!(output.status.success());
    let output = run_with_timeout(
        fixture
            .command(&["inspect", "uc://a.b.c", "--databricks-profile", "work"])
            .env("PATH", ""),
    )?;
    assert_eq!(output.status.code(), Some(2));
    let diagnostic = check_failure(&output.stderr, "configuration", "databricks_cli_missing");
    assert!(
        diagnostic.contains("Install the Databricks CLI"),
        "{diagnostic}"
    );
    Ok(())
}

#[test]
fn inspection_renews_expired_grants_without_resolving_the_table_name_again() -> TestResult {
    for kind in [
        StorageCredentialKind::S3,
        StorageCredentialKind::R2,
        StorageCredentialKind::AzureSas,
        StorageCredentialKind::AzureBearer,
        StorageCredentialKind::Gcs,
    ] {
        let fixture = DatabricksCliFixture::new(kind)?;
        let server = fixture.storage_with_expiring_credentials(kind, |replacement| match kind {
            StorageCredentialKind::S3 | StorageCredentialKind::R2 => {
                let field = if kind == StorageCredentialKind::S3 {
                    "aws_temp_credentials"
                } else {
                    "r2_temp_credentials"
                };
                replacement[field] = json!({
                    "access_key_id": "renewed-key",
                    "secret_access_key": "renewed-secret",
                    "session_token": "renewed-session"
                });
            }
            StorageCredentialKind::AzureSas => {
                replacement["azure_user_delegation_sas"] =
                    json!({"sas_token":"sig=renewed%2Bsignature"});
            }
            StorageCredentialKind::AzureBearer => {
                replacement["azure_aad"] = json!({"aad_token":"renewed-azure-token"});
            }
            StorageCredentialKind::Gcs => {
                replacement["gcp_oauth_token"] = json!({"oauth_token":"renewed-gcp-token"});
            }
        })?;
        let output = run_with_timeout(
            fixture
                .inspect()
                .env("DAR_TEST_REFRESH", "rotate")
                .env("RUST_LOG", "trace"),
        )?;
        assert!(
            output.status.success(),
            "{kind:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout)?["table_version"],
            "0"
        );
        let calls = fixture.calls();
        assert_eq!(calls.iter().filter(|arg| *arg == "tables").count(), 1);
        assert_eq!(
            calls
                .iter()
                .filter(|arg| *arg == "temporary-table-credentials")
                .count(),
            2
        );
        assert_eq!(calls.iter().filter(|arg| *arg == "READ").count(), 2);
        for pair in calls.windows(2).filter(|pair| pair[0] == "--table-id") {
            assert_eq!(pair[1], TABLE_ID);
        }
        assert_eq!(
            calls
                .iter()
                .filter(|arg| **arg == format!("--profile={PROFILE}"))
                .count(),
            3
        );
        let requests = server.requests.lock().unwrap();
        let renewed_auth = match kind {
            StorageCredentialKind::S3 | StorageCredentialKind::R2 => "credential=renewed-key/",
            StorageCredentialKind::AzureSas => "sig=renewed%2bsignature",
            StorageCredentialKind::AzureBearer => "authorization: bearer renewed-azure-token",
            StorageCredentialKind::Gcs => "authorization: bearer renewed-gcp-token",
        };
        assert!(
            requests
                .iter()
                .any(|request| request.to_ascii_lowercase().contains(renewed_auth)),
            "{kind:?}: no request used renewed credentials"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        for sensitive in ["secret", "renewed-", TABLE_NAME, TABLE_ID, PROFILE] {
            assert!(!stderr.contains(sensitive), "{stderr}");
        }
    }
    Ok(())
}

#[test]
fn refresh_failures_preserve_redacted_uc_categories_and_stop_inspection() -> TestResult {
    for (kind, failure, code) in [
        (
            StorageCredentialKind::S3,
            "deny",
            "uc_credential_request_failed",
        ),
        (
            StorageCredentialKind::S3,
            "expired",
            "uc_credentials_expired",
        ),
        (
            StorageCredentialKind::Gcs,
            "root",
            "invalid_databricks_response",
        ),
        (
            StorageCredentialKind::Gcs,
            "invalid",
            "invalid_databricks_response",
        ),
        (
            StorageCredentialKind::AzureSas,
            "kind",
            "unsupported_uc_table",
        ),
    ] {
        let fixture = DatabricksCliFixture::new(kind)?;
        let _server =
            fixture.storage_with_expiring_credentials(kind, |replacement| match failure {
                "expired" => replacement["expiration_time"] = json!(0),
                "root" => replacement["url"] = json!("gs://other/secret-root"),
                "invalid" => {
                    replacement["gcp_oauth_token"] = json!({"oauth_token":"secret invalid token"})
                }
                "kind" => {
                    replacement
                        .as_object_mut()
                        .unwrap()
                        .remove("azure_user_delegation_sas");
                    replacement["azure_aad"] = json!({"aad_token":"secret-new-kind"});
                }
                _ => {}
            })?;
        let output = run_with_timeout(
            fixture
                .inspect()
                .arg("--diagnostics=json")
                .env("DAR_TEST_REFRESH", failure)
                .env("RUST_LOG", "trace"),
        )?;
        assert_eq!(
            output.status.code(),
            Some(1),
            "{failure}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty());
        let record =
            super::diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], code);
        assert_eq!(
            fixture
                .calls()
                .iter()
                .filter(|arg| *arg == "temporary-table-credentials")
                .count(),
            2
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        for sensitive in ["secret", TABLE_NAME, TABLE_ID, PROFILE] {
            assert!(!stderr.contains(sensitive), "{stderr}");
        }
    }
    Ok(())
}

#[test]
fn invalid_responses_and_denied_credentials_are_redacted() -> TestResult {
    for (response, expected) in [
        ("secret-sentinel".into(), "invalid_databricks_response"),
        ("x".repeat(1024 * 1024 + 1), "invalid_databricks_response"),
        (
            r#"{"table_id":"secret-sentinel"}"#.into(),
            "invalid_databricks_response",
        ),
    ] {
        let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
        fs::write(fixture.directory.path().join("metadata.json"), response)?;
        let output = run_with_timeout(&mut fixture.inspect())?;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        check_failure(&output.stderr, "execution", expected);
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
        let output = run_with_timeout(
            fixture
                .inspect()
                .arg("--diagnostics=json")
                .env("RUST_LOG", "off"),
        )?;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let record =
            super::diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], expected);
        assert_eq!(fixture.calls().first().map(String::as_str), Some("tables"));
        assert!(
            !fixture
                .calls()
                .iter()
                .any(|a| a == "temporary-table-credentials")
        );
    }
    let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
    let output = run_with_timeout(
        fixture
            .inspect()
            .env("RUST_LOG", "trace")
            .env("DAR_TEST_DENIED", "1"),
    )?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    check_failure(&output.stderr, "execution", "uc_credential_request_failed");
    assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
    let output = run_with_timeout(
        fixture
            .inspect()
            .arg("--diagnostics=json")
            .env("RUST_LOG", "trace")
            .env("DAR_TEST_DENIED", "1"),
    )?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let record = super::diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["code"], "uc_credential_request_failed");
    assert_eq!(record["recovery"], "unknown");
    Ok(())
}

#[test]
fn uc_admission_preserves_direct_path_inspection_of_unsupported_protocols() -> TestResult {
    for feature in ["madeUpFeature", "catalogManaged"] {
        let directory = tempfile::tempdir()?;
        fs::create_dir(directory.path().join("_delta_log"))?;
        let log = format!(
            "{}\n{}\n",
            json!({
                "protocol": {
                    "minReaderVersion": 3,
                    "minWriterVersion": 7,
                    "readerFeatures": [feature],
                    "writerFeatures": [feature]
                }
            }),
            json!({
                "metaData": {
                    "id": "test",
                    "format": {"provider": "parquet", "options": {}},
                    "schemaString": r#"{"type":"struct","fields":[{"name":"id","type":"integer","nullable":true,"metadata":{}}]}"#,
                    "partitionColumns": [],
                    "configuration": {}
                }
            })
        );
        fs::write(
            directory
                .path()
                .join("_delta_log/00000000000000000000.json"),
            &log,
        )?;
        let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
        let server = MockCloudStorageServer::new(StorageCredentialKind::S3, log.into_bytes())?;
        fs::write(
            fixture.directory.path().join("options.json"),
            serde_json::to_vec(&json!({
                "aws_region": "eu-west-1",
                "aws_endpoint": server.endpoint,
                "aws_allow_http": "true"
            }))?,
        )?;
        let output = run_with_timeout(&mut fixture.inspect())?;
        assert_eq!(output.status.code(), Some(1));
        assert!(
            output.stdout.is_empty(),
            "UC inspection must not report an unsupported snapshot"
        );
        if feature == "madeUpFeature" {
            check_failure(&output.stderr, "execution", "unsupported_uc_table");
        } else {
            check_failure(&output.stderr, "snapshot", "snapshot_load");
        }
        if feature == "madeUpFeature" {
            let output = run_with_timeout(fixture.command(&["inspect"]).arg(directory.path()))?;
            assert!(
                output.status.success(),
                "direct-path protocol inspection changed"
            );
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn uc_cancellation_remains_responsive_with_blocked_stdout() -> TestResult {
    use std::os::fd::AsRawFd;
    let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
    let schema = json!({
        "type": "struct",
        "fields": [{
            "name": "id",
            "type": "integer",
            "nullable": true,
            "metadata": {"large": "x".repeat(256 * 1024)}
        }]
    });
    let log = format!(
        "{}\n{}\n",
        json!({
            "protocol": {
                "minReaderVersion": 1,
                "minWriterVersion": 2
            }
        }),
        json!({
            "metaData": {
                "id": "test",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": schema.to_string(),
                "partitionColumns": [],
                "configuration": {}
            }
        })
    );
    let server = MockCloudStorageServer::new(StorageCredentialKind::S3, log.into_bytes())?;
    fs::write(
        fixture.directory.path().join("options.json"),
        serde_json::to_vec(&json!({
            "aws_region": "eu-west-1",
            "aws_endpoint": server.endpoint,
            "aws_allow_http": "true"
        }))?,
    )?;
    let mut child = fixture
        .inspect()
        .arg("--diagnostics=json")
        .env("RUST_LOG", "off")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut ready = libc::pollfd {
        fd: child.stdout.as_ref().unwrap().as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: the poll descriptor points to this child's live stdout pipe.
    let polled = unsafe { libc::poll(&mut ready, 1, 5000) };
    if polled != 1 || ready.revents & libc::POLLIN == 0 {
        child.kill()?;
        let output = child.wait_with_output()?;
        panic!(
            "inspection did not produce output: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    // SAFETY: this is the live subprocess created above.
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait()?.is_none() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    if child.try_wait()?.is_none() {
        child.kill()?;
        child.wait()?;
        panic!("stdout blocked cancellation");
    }
    let output = child.wait_with_output()?;
    assert_eq!(output.status.code(), Some(143));
    assert!(!output.stdout.is_empty());
    let record = super::diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["code"], "uc_cancelled");
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn cancellation_terminates_pending_databricks_process() -> TestResult {
    for (signal, machine, during_refresh) in [
        (libc::SIGINT, false, false),
        (libc::SIGTERM, false, false),
        (libc::SIGINT, true, false),
        (libc::SIGTERM, true, false),
        (libc::SIGINT, false, true),
        (libc::SIGTERM, true, true),
    ] {
        let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
        let _server = if during_refresh {
            Some(fixture.storage_with_expiring_credentials(StorageCredentialKind::S3, |_| {})?)
        } else {
            None
        };
        let mut command = fixture.inspect();
        if machine {
            command.arg("--diagnostics=json");
        }
        let mut child = command
            .env("RUST_LOG", if machine { "off" } else { "info" })
            .env("DAR_TEST_HANG", if during_refresh { "0" } else { "1" })
            .env("DAR_TEST_REFRESH", if during_refresh { "hang" } else { "" })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let pid_file = fixture.directory.path().join("pid");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pid_file.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        let pid = fs::read_to_string(pid_file)?.parse::<i32>()?;
        // SAFETY: both process IDs belong to this test's subprocess tree.
        assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        while child.try_wait()?.is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if child.try_wait()?.is_none() {
            child.kill()?;
            panic!("cancelled CLI did not exit");
        }
        let output = child.wait_with_output()?;
        assert_eq!(output.status.code(), Some(128 + signal));
        assert!(output.stdout.is_empty());
        if machine {
            let record =
                super::diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(record["code"], "uc_cancelled");
        } else {
            check_cancellation(&output.stderr, 128 + signal);
        }
        let status_path = format!("/proc/{pid}/stat");
        let stopped = || {
            fs::read_to_string(&status_path)
                .map_or(true, |s| s.split_whitespace().nth(2) == Some("Z"))
        };
        while !stopped() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(stopped(), "Databricks process survived cancellation");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn check_cancellation(stderr: &[u8], status: i32) {
    let text = std::str::from_utf8(stderr).unwrap();
    let records = text
        .lines()
        .filter(|line| line.contains("event=\"cli.cancelled\""))
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 1, "{text}");
    let record = records[0];
    assert!(record.contains("INFO delta_arrow_reader_cli"), "{text}");
    assert!(record.contains("outcome=\"cancelled\""), "{text}");
    assert!(record.contains("error_code=\"uc_cancelled\""), "{text}");
    assert!(record.contains(&format!("exit_status={status}")), "{text}");
    assert!(!text.contains("ERROR"), "{text}");
    assert!(!text.contains("secret"), "{text}");
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires unshare and permission to create user/PID namespaces"]
fn pid_one_cancellation_preserves_uc_diagnostic() -> TestResult {
    for (signal, status) in [("INT", 130), ("TERM", 143)] {
        let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
        let inspect = fixture.inspect();
        // Keep unshare on the host PATH; env then execs DAR as namespace PID 1.
        // --kill-child also cleans up the namespace if the test times out.
        let output = run_with_timeout(
            Command::new("unshare")
                .args([
                    "--user",
                    "--map-root-user",
                    "--pid",
                    "--fork",
                    "--kill-child",
                ])
                .arg("/usr/bin/env")
                .arg(format!("PATH={}", fixture.directory.path().display()))
                .arg(inspect.get_program())
                .args(inspect.get_args())
                .env("DAR_TEST_DIR", fixture.directory.path())
                .env("RUST_LOG", "info")
                .env("DAR_TEST_PID1_SIGNAL", signal),
        )?;
        assert_eq!(
            fs::read_to_string(fixture.directory.path().join("parent_pid"))?,
            "1"
        );
        assert_eq!(output.status.code(), Some(status));
        assert!(output.stdout.is_empty());
        check_cancellation(&output.stderr, status);
    }
    Ok(())
}

/// A local server that serves Delta logs as S3/Azure objects and records requests.
///
/// Only GET/HEAD and log listing are needed, following the core cloud-location fixture.
struct MockCloudStorageServer {
    endpoint: String,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl MockCloudStorageServer {
    fn new(credential_kind: StorageCredentialKind, log: Vec<u8>) -> std::io::Result<Self> {
        Self::with_request_hook(credential_kind, log, |_| {})
    }

    fn with_request_hook(
        credential_kind: StorageCredentialKind,
        log: Vec<u8>,
        mut on_request: impl FnMut(&str) + Send + 'static,
    ) -> std::io::Result<Self> {
        let is_azure = matches!(
            credential_kind,
            StorageCredentialKind::AzureSas | StorageCredentialKind::AzureBearer
        );
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                let Ok((mut socket, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut byte = [0];
                while request.len() < 16 * 1024 && !request.ends_with(b"\r\n\r\n") {
                    if socket.read_exact(&mut byte).is_err() {
                        break;
                    }
                    request.push(byte[0]);
                }
                let request = String::from_utf8(request).unwrap();
                recorded.lock().unwrap().push(request.clone());
                on_request(&request);
                let Some(target) = request.split_whitespace().nth(1) else {
                    continue;
                };
                let url = Url::parse(&format!("http://localhost{target}")).unwrap();
                // GCS encodes slashes in object keys, unlike the S3 and Azure clients.
                let request_path = object_store::path::Path::from_url_path(url.path()).unwrap();
                let query = url.query_pairs().collect::<HashMap<_, _>>();
                let listing = query.contains_key("list-type")
                    || query.get("comp").is_some_and(|v| v == "list");
                let key = "table/_delta_log/00000000000000000000.json";
                let (status, mut body) = if listing {
                    let include = key.starts_with(query.get("prefix").map_or("", |v| v.as_ref()))
                        && key > query.get("start-after").map_or("", |v| v.as_ref());
                    let entry = if !include {
                        String::new()
                    } else if is_azure {
                        format!(
                            "<Blob><Name>{key}</Name><Properties><Last-Modified>Wed, 01 Jan 2020 00:00:00 GMT</Last-Modified><Etag>test</Etag><Content-Length>{}</Content-Length><Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType></Properties></Blob>",
                            log.len()
                        )
                    } else {
                        format!(
                            "<Contents><Key>{key}</Key><LastModified>2020-01-01T00:00:00Z</LastModified><Size>{}</Size></Contents>",
                            log.len()
                        )
                    };
                    let xml = if is_azure {
                        format!(
                            "<EnumerationResults><Blobs>{entry}</Blobs><NextMarker /></EnumerationResults>"
                        )
                    } else {
                        format!(
                            "<ListBucketResult><IsTruncated>false</IsTruncated>{entry}</ListBucketResult>"
                        )
                    };
                    (200, xml.into_bytes())
                } else if request_path.as_ref()
                    == format!("{}/{key}", if is_azure { "container" } else { "bucket" })
                {
                    (200, log.clone())
                } else {
                    (404, Vec::new())
                };
                let length = body.len();
                if request.starts_with("HEAD ") {
                    body.clear();
                }
                let headers = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Length: {length}\r\nContent-Type: {}\r\nLast-Modified: Wed, 01 Jan 2020 00:00:00 GMT\r\nETag: test\r\nConnection: close\r\n\r\n",
                    if listing {
                        "application/xml"
                    } else {
                        "application/octet-stream"
                    }
                );
                let _ = socket
                    .write_all(headers.as_bytes())
                    .and_then(|()| socket.write_all(&body));
            }
        });
        Ok(Self {
            endpoint,
            requests,
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for MockCloudStorageServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}
