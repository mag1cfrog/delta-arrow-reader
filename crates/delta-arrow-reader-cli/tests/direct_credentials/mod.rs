//! Direct S3 source selection reaches signed requests without credential fallback or leaks.

use std::{fs, io::Cursor, process::Command};

use arrow::{compute::concat_batches, ipc::reader::StreamReader};
use serde_json::{Value, json};

use super::{
    cloud_storage::{
        CloudStorageProtocol, MockCloudStorageServer, expected_partitioned_rows,
        partitioned_objects,
    },
    diagnostics::assert_single_terminal_record,
    run_with_timeout, sort_by_id,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Owns the selected options file and the storage endpoint that records the resulting requests.
struct S3AuthorizationFixture {
    directory: tempfile::TempDir,
    server: MockCloudStorageServer,
}

impl S3AuthorizationFixture {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        fs::write(
            directory.path().join("query.sql"),
            "SELECT id FROM t ORDER BY id",
        )?;
        Ok(Self {
            directory,
            server: MockCloudStorageServer::with_objects(
                CloudStorageProtocol::S3,
                partitioned_objects()?,
                |_, _| {},
            )?,
        })
    }

    fn write_options(&self, mut options: Value) -> TestResult {
        options["aws_endpoint"] = json!(self.server.endpoint);
        options["allow_http"] = json!("true");
        fs::write(
            self.directory.path().join("options.json"),
            serde_json::to_vec(&options)?,
        )?;
        Ok(())
    }

    fn command(&self, operation: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command
            .env_clear()
            .env("RUST_LOG", "trace")
            .args([operation, "s3://bucket/table", "--storage-options-file"])
            .arg(self.directory.path().join("options.json"));
        command
    }

    fn assert_request_identity(&self, key: &str, token: Option<&str>, region: &str) {
        let requests = self.server.requests.lock().unwrap();
        assert!(!requests.is_empty());
        for request in requests.iter() {
            assert!(request.contains(&format!("Credential={key}/")), "{request}");
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
    }
}

#[test]
fn selected_credentials_sign_inspection_scan_and_sql_requests() -> TestResult {
    let expected = expected_partitioned_rows()?;
    let expected_ids = sort_by_id(&expected)?.column_by_name("id").unwrap().clone();
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
        let fixture = S3AuthorizationFixture::new()?;
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
            if operation == "scan" {
                command.args(["--column", "id"]);
            }
            if operation == "query" {
                command
                    .arg("--sql-file")
                    .arg(fixture.directory.path().join("query.sql"));
            }
            let output = run_with_timeout(&mut command)?;
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(!String::from_utf8_lossy(&output.stdout).contains("secret"));
            assert_single_terminal_record(&output.stderr, "completion");
            if operation == "inspect" {
                let result: Value = serde_json::from_slice(&output.stdout)?;
                assert_eq!(result["table_version"], "0");
                assert_eq!(
                    result["schema"]["fields"].as_array().unwrap().len(),
                    expected.num_columns()
                );
            } else {
                assert!(output.stdout.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]));
                let reader = StreamReader::try_new(Cursor::new(output.stdout), None)?;
                let schema = reader.schema();
                let actual = concat_batches(&schema, &reader.collect::<Result<Vec<_>, _>>()?)?;
                assert_eq!(sort_by_id(&actual)?.column(0), &expected_ids);
            }
        }
        let (key, token) = if source == "json" {
            ("secret-json-key", "secret-json-session")
        } else {
            ("secret-env-key", "secret-env-session")
        };
        fixture.assert_request_identity(key, Some(token), "eu-west-1");
    }
    Ok(())
}

#[test]
fn invalid_sources_fail_before_storage_io_even_with_usable_ambient_credentials() -> TestResult {
    let fixture = S3AuthorizationFixture::new()?;
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
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "invalid_storage_authorization");
        assert_eq!(record["reason"], reason);
        assert_eq!(record["recovery"], "fix_input");
    }
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn environment_requires_one_complete_family_and_rejects_json_credentials() -> TestResult {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let fixture = S3AuthorizationFixture::new()?;
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
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["reason"], reason);
    }
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn selected_credentials_never_fall_back_after_permission_denial() -> TestResult {
    for source in ["json", "env"] {
        let fixture = S3AuthorizationFixture::new()?;
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
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            assert!(!String::from_utf8_lossy(&output.stderr).contains("secret"));
            if machine {
                let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                assert_eq!(record["source_kind"], "permission_denied");
            }
        }
        fixture.assert_request_identity("secret-selected-key", None, "eu-west-1");
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
            "auth_source_requires_s3",
        ),
        (
            vec!["inspect", "gs://bucket/table", "--auth-source", "env"],
            "auth_source_requires_s3",
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
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["reason"], reason);
    }
    Ok(())
}

#[test]
fn omitting_source_preserves_existing_storage_options() -> TestResult {
    let fixture = S3AuthorizationFixture::new()?;
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
