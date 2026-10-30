//! External Hive/Glue Delta reads use the same UC commands with path-scoped authorization.

use super::*;
use crate::diagnostics::assert_single_terminal_record;

const LOCATION_DENIED: &str =
    "User does not have EXTERNAL USE LOCATION on External Location 'test_location'.";
const LOCATION_HINT: &str =
    "Ask the owner of the named external location to grant EXTERNAL USE LOCATION, then retry.";

fn write_location_denial(fixture: &DatabricksCliFixture) -> TestResult {
    // Captured CLI v1.6.0 403 format; profile, host, and location identifiers are replaced.
    fs::write(
        fixture.directory.path().join("failure.txt"),
        format!(
            "Error: {LOCATION_DENIED}\n\nProfile:   {PROFILE}\nHost:      https://workspace.invalid\nAuth type: OAuth (databricks-cli)\n\nNext steps:\n  - Verify you have the required permissions for this operation\n  - Check your identity: databricks auth describe --profile {PROFILE}\n"
        ),
    )?;
    Ok(())
}

fn assert_path_requests(fixture: &DatabricksCliFixture, count: usize) {
    let recorded = fixture.recorded_arguments();
    assert_eq!(
        recorded
            .iter()
            .filter(|arg| *arg == "temporary-path-credentials")
            .count(),
        count
    );
    assert_eq!(
        recorded
            .windows(2)
            .filter(|pair| pair[0] == "s3://bucket/table/" && pair[1] == "PATH_READ")
            .count(),
        count
    );
    assert!(!recorded.iter().any(|arg| matches!(
        arg.as_str(),
        "temporary-table-credentials"
            | "PATH_READ_WRITE"
            | "READ"
            | "grants"
            | "sql"
            | "warehouses"
    )));
}

#[test]
fn missing_external_location_privilege_is_actionable_in_both_diagnostic_modes() -> TestResult {
    let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
    fixture.use_foreign_delta_metadata()?;
    write_location_denial(&fixture)?;
    for machine in [false, true] {
        for mut command in [
            fixture.inspect(),
            fixture.table_command("preview"),
            fixture.scan(),
            fixture.query("SELECT * FROM t")?,
            fixture.manifest_query(
                json!([{"alias":"orders", "location":"uc://main.sales.orders",
                    "authorization":{"source":"databricks", "databricks_profile":PROFILE,
                    "storage_options_file":"../options.json"}}]),
                "SELECT * FROM orders",
            )?,
        ] {
            if machine {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(
                command.env("DAR_TEST_FAILURE_STAGE", "temporary-path-credentials"),
            )?;
            assert_eq!(output.status.code(), Some(1), "{output:?}");
            assert!(output.stdout.is_empty());
            let stderr = std::str::from_utf8(&output.stderr)?;
            if machine {
                let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                assert_eq!(record["code"], "uc_credential_request_failed");
                assert_eq!(record["reason"], "uc_permission_denied");
                assert_eq!(record["source_kind"], "permission_denied");
                assert_eq!(record["recovery"], "request_access");
                assert_eq!(record["retryable"], false);
                assert_eq!(record["upstream_message"], LOCATION_DENIED);
                assert_eq!(record["hint"], LOCATION_HINT);
            } else {
                assert!(stderr.contains(LOCATION_DENIED), "{stderr}");
                assert!(stderr.contains(LOCATION_HINT), "{stderr}");
            }
            for private in [
                PROFILE,
                "workspace.invalid",
                "Auth type:",
                "Next steps:",
                "secret-sentinel",
            ] {
                assert!(!stderr.contains(private), "{stderr}");
            }
        }
    }
    assert_path_requests(&fixture, 10);
    Ok(())
}

#[test]
fn path_grant_renewal_keeps_the_original_root_and_reports_revoked_access() -> TestResult {
    let kind = StorageCredentialKind::S3;
    for denied in [false, true] {
        let fixture = DatabricksCliFixture::new(kind)?;
        fixture.use_foreign_delta_metadata()?;
        write_location_denial(&fixture)?;
        let server =
            fixture.storage_with_credential_expiry_and_invalid_metadata(kind, |replacement| {
                kind.rotate_credentials(replacement);
            })?;
        let output = run_with_timeout(
            fixture
                .inspect()
                .arg("--diagnostics=json")
                .env("DAR_TEST_REFRESH", if denied { "deny" } else { "rotate" }),
        )?;
        if denied {
            assert_eq!(output.status.code(), Some(1), "{output:?}");
            assert!(output.stdout.is_empty());
            let record = assert_single_terminal_record(&output.stderr, "diagnostic");
            assert_eq!(record["reason"], "uc_permission_denied");
            assert_eq!(record["upstream_message"], LOCATION_DENIED);
            assert_eq!(record["hint"], LOCATION_HINT);
        } else {
            assert!(output.status.success(), "{output:?}");
            assert_eq!(
                serde_json::from_slice::<Value>(&output.stdout)?["table_version"],
                "0"
            );
        }
        assert_path_requests(&fixture, 2);
        let recorded = fixture.recorded_arguments();
        assert_eq!(recorded.iter().filter(|arg| *arg == "tables").count(), 1);
        assert_eq!(
            recorded
                .iter()
                .filter(|arg| **arg == format!("--profile={PROFILE}"))
                .count(),
            3
        );
        assert_eq!(
            server.requests.lock().unwrap().iter().any(|request| request
                .to_ascii_lowercase()
                .contains(kind.expected_renewed_auth_fragment())),
            !denied
        );
    }
    Ok(())
}

#[test]
fn path_credentials_cannot_authorize_a_different_storage_root() -> TestResult {
    for root in [
        "s3://bucket/",
        "s3://bucket/table-sibling/",
        "s3://other/table/",
    ] {
        let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
        fixture.use_foreign_delta_metadata()?;
        let path = fixture.directory.path().join("credentials.json");
        let mut credentials: Value = serde_json::from_slice(&fs::read(&path)?)?;
        credentials["url"] = json!(root);
        fs::write(path, serde_json::to_vec(&credentials)?)?;
        let output = run_with_timeout(fixture.inspect().arg("--diagnostics=json"))?;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["code"],
            "invalid_databricks_response"
        );
        assert_path_requests(&fixture, 1);
    }
    Ok(())
}

#[test]
fn native_table_permission_failures_do_not_try_path_credentials() -> TestResult {
    let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
    write_location_denial(&fixture)?;
    let output = run_with_timeout(
        fixture
            .inspect()
            .arg("--diagnostics=json")
            .env("DAR_TEST_FAILURE_STAGE", "temporary-table-credentials"),
    )?;
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        assert_single_terminal_record(&output.stderr, "diagnostic")["reason"],
        "uc_permission_denied"
    );
    let recorded = fixture.recorded_arguments();
    assert_eq!(
        recorded
            .iter()
            .filter(|arg| *arg == "temporary-table-credentials")
            .count(),
        1
    );
    assert!(
        !recorded
            .iter()
            .any(|arg| arg == "temporary-path-credentials")
    );
    Ok(())
}
