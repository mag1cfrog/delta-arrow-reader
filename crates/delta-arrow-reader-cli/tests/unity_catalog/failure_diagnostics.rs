//! Exercise the subprocess-to-terminal path with synthetic official CLI error formats.

use super::*;

pub(super) fn permission_error() -> String {
    auth_error(
        "Read access was denied.",
        "Verify you have the required permissions for this operation",
    )
}

fn auth_error(message: &str, step: &str) -> String {
    // The footer is from CLI v1.15.0 libs/auth/error.go; service wording is synthetic.
    format!(
        "Error: {message}\n\nProfile:   {PROFILE}\nHost:      https://workspace.invalid\nAuth type: Personal Access Token (pat)\n\nNext steps:\n  - {step}\n  - Check your identity: databricks auth describe --profile {PROFILE}\n"
    )
}

#[test]
fn request_failures_retain_explanations_and_recovery_in_both_diagnostic_modes() -> TestResult {
    let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
    for (stderr, message, reason, kind, recovery) in [
        (
            permission_error(),
            "Read access was denied.",
            Some("uc_permission_denied"),
            Some("permission_denied"),
            "request_access",
        ),
        (
            auth_error(
                "Login expired.",
                &format!(
                    "Regenerate your access token or run: databricks auth login --profile {PROFILE}"
                ),
            ),
            "Login expired.",
            Some("uc_authentication_failed"),
            Some("unauthenticated"),
            "authorize",
        ),
        (
            "Error: Unsupported fixture option: sample_mode.\nAllowed values are A and B.\n".into(),
            "Unsupported fixture option: sample_mode.\nAllowed values are A and B.",
            None,
            None,
            "unknown",
        ),
    ] {
        fs::write(fixture.directory.path().join("failure.txt"), stderr)?;
        for (stage, code) in [
            ("tables", "uc_table_request_failed"),
            (
                "temporary-table-credentials",
                "uc_credential_request_failed",
            ),
        ] {
            for json_diagnostics in [false, true] {
                let mut command = fixture.inspect();
                if json_diagnostics {
                    command.arg("--diagnostics=json");
                }
                let output = run_with_timeout(
                    command
                        .env("DAR_TEST_FAILURE_STAGE", stage)
                        .env("RUST_LOG", "trace"),
                )?;
                assert_eq!(output.status.code(), Some(1));
                assert!(output.stdout.is_empty());
                let text = std::str::from_utf8(&output.stderr)?;
                for sensitive in [
                    PROFILE,
                    TABLE_ID,
                    "workspace.invalid",
                    "Auth type:",
                    "Next steps:",
                ] {
                    assert!(!text.contains(sensitive), "{text}");
                }
                if json_diagnostics {
                    let record = super::super::diagnostics::assert_single_terminal_record(
                        &output.stderr,
                        "diagnostic",
                    );
                    assert_eq!(record["code"], code);
                    assert_eq!(record["reason"], reason.unwrap_or(code));
                    assert_eq!(record["source_kind"], json!(kind));
                    assert_eq!(record["recovery"], recovery);
                    assert_eq!(
                        record["retryable"],
                        if kind.is_some() {
                            json!(false)
                        } else {
                            Value::Null
                        }
                    );
                    assert_eq!(record["upstream_message"], message);
                    assert_eq!(record.get("hint").is_some(), kind.is_some());
                    assert!(record.get("upstream_message_truncated").is_none());
                } else {
                    assert!(text.contains(message), "{text}");
                    assert!(text.contains(reason.unwrap_or(code)), "{text}");
                }
            }
        }
    }
    Ok(())
}

#[test]
fn sdk_request_dumps_and_credentials_do_not_reach_terminal_diagnostics() -> TestResult {
    let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
    fs::write(
        fixture.directory.path().join("failure.txt"),
        concat!(
            "DEBUG response secret-debug\n",
            "Error: Failed to decode response at https://name:secret-password@workspace.invalid/table?sig=secret-signature. ",
            "This is likely a bug in the Databricks SDK for Go or the underlying REST API. Request log:\n```\n",
            "< {\"secret_access_key\":\"secret-aws\",\"sas_token\":\"secret-sas\",\"aad_token\":\"secret-aad\",\"oauth_token\":\"secret-gcp\"}\n```\n",
        ),
    )?;
    for json_diagnostics in [false, true] {
        let mut command = fixture.inspect();
        if json_diagnostics {
            command.arg("--diagnostics=json");
        }
        let output = run_with_timeout(
            command
                .env("DAR_TEST_FAILURE_STAGE", "temporary-table-credentials")
                .env("RUST_LOG", "trace"),
        )?;
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let text = std::str::from_utf8(&output.stderr)?;
        assert!(text.contains("Failed to decode response"), "{text}");
        assert!(text.contains("[URL details removed]"), "{text}");
        for forbidden in [
            "secret",
            "Request log:",
            "DEBUG response",
            "underlying REST API",
        ] {
            assert!(!text.contains(forbidden), "{text}");
        }
        if json_diagnostics {
            super::super::diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
        }
    }
    Ok(())
}

#[test]
fn long_unicode_explanations_remain_one_bounded_terminal_record() -> TestResult {
    let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
    fs::write(
        fixture.directory.path().join("failure.txt"),
        format!("Error: {}", "界".repeat(2000)),
    )?;
    let output = run_with_timeout(
        fixture
            .inspect()
            .arg("--diagnostics=json")
            .env("DAR_TEST_FAILURE_STAGE", "tables"),
    )?;
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let record =
        super::super::diagnostics::assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["code"], "uc_table_request_failed");
    assert_eq!(record["upstream_message_truncated"], true);
    assert!(record["upstream_message"].as_str().unwrap().len() <= 1024);
    Ok(())
}

#[test]
fn local_command_failures_keep_io_and_exit_details_without_service_access_advice() -> TestResult {
    let fixture = DatabricksCliFixture::new(StorageCredentialKind::S3)?;
    let executable = fixture.directory.path().join("databricks");
    for (script, executable_mode, reason, kind, exit_code, signal) in [
        (
            "#!/bin/sh\nexit 0\n",
            0o644,
            "databricks_cli_spawn_failed",
            Some("permission_denied"),
            None,
            None,
        ),
        (
            "#!/bin/sh\nexit 42\n",
            0o755,
            "uc_table_request_failed",
            None,
            Some(42),
            None,
        ),
        (
            "#!/bin/sh\nkill -TERM $$\n",
            0o755,
            "uc_table_request_failed",
            None,
            None,
            Some(15),
        ),
    ] {
        fs::write(&executable, script)?;
        fs::set_permissions(&executable, fs::Permissions::from_mode(executable_mode))?;
        for json_diagnostics in [false, true] {
            let mut command = fixture.inspect();
            if json_diagnostics {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut command)?;
            assert_eq!(output.status.code(), Some(1));
            assert!(output.stdout.is_empty());
            let text = std::str::from_utf8(&output.stderr)?;
            assert!(!text.contains(PROFILE), "{text}");
            assert!(
                !text.contains(fixture.directory.path().to_str().unwrap()),
                "{text}"
            );
            assert!(!text.contains("uc_permission_denied"), "{text}");
            assert!(!text.contains("administrator"), "{text}");
            if json_diagnostics {
                let record = super::super::diagnostics::assert_single_terminal_record(
                    &output.stderr,
                    "diagnostic",
                );
                assert_eq!(record["code"], "uc_table_request_failed");
                assert_eq!(record["reason"], reason);
                assert_eq!(record["source_kind"], json!(kind));
                assert_eq!(record["command_exit_code"], json!(exit_code));
                assert_eq!(record["command_signal"], json!(signal));
                if kind.is_some() {
                    assert_eq!(record["command_operation"], "spawn");
                    assert_eq!(record["io_kind"], "PermissionDenied");
                    assert!(record["os_error"].as_i64().is_some());
                    assert_eq!(record["recovery"], "fix_input");
                    assert!(
                        record["hint"]
                            .as_str()
                            .unwrap()
                            .contains("executable is runnable")
                    );
                } else {
                    assert_eq!(record["command_operation"], "exit");
                    assert!(record.get("io_kind").is_none());
                }
            } else {
                assert!(text.contains(reason), "{text}");
                if let Some(code) = exit_code {
                    assert!(
                        text.contains(&format!("command_exit_code={code}")),
                        "{text}"
                    );
                }
                if let Some(signal) = signal {
                    assert!(text.contains(&format!("command_signal={signal}")), "{text}");
                }
                if kind.is_some() {
                    assert!(text.contains("io_kind=PermissionDenied"), "{text}");
                }
            }
        }
    }
    Ok(())
}
