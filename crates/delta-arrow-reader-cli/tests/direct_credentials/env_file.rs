//! A selected file supplies credentials without shell execution or ambient fallback.

use std::{fs, process::Command};

use serde_json::json;

use super::{
    CloudStorageProtocol, DirectStorageFixture, TestResult, assert_authorization_input_error,
    assert_failure_without_stdout, assert_read_result, assert_single_terminal_record,
    run_with_timeout,
};

#[test]
fn s3_env_file_reaches_every_read_with_literal_values_and_json_configuration_precedence()
-> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    let path = fixture.directory.path().join("selected.env");
    let marker = fixture.directory.path().join("must-not-exist");
    let password = format!(
        "secret-$(touch {})-`touch {}`",
        marker.display(),
        marker.display()
    );
    fs::write(
        &path,
        format!(
            "# selected credentials\r\n AWS_ACCESS_KEY_ID = 'secret-file-key'\r\nAWS_SECRET_ACCESS_KEY=\"{password}\"\r\nAWS_SESSION_TOKEN=secret-${{HOME}}#literal\r\nAWS_REGION=unused-file-region\r\n"
        ),
    )?;
    for (operation, region_option) in [
        ("inspect", "region"),
        ("scan", "default_region"),
        ("preview", "aws_region"),
        ("query", "aws_default_region"),
    ] {
        fixture.write_options(json!({region_option: "eu-west-1"}))?;
        let discovery =
            run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).env_clear().args([
                "capabilities",
                "--command",
                operation,
            ]))?;
        assert!(discovery.status.success());
        let guide: serde_json::Value = serde_json::from_slice(&discovery.stdout)?;
        assert!(!String::from_utf8_lossy(&discovery.stdout).contains("AWS_ACCESS_KEY_ID"));
        let file_arguments = guide["authorization"]["env_file"]["arguments"]
            .as_array()
            .unwrap();
        let mut command = fixture.command(operation);
        for argument in file_arguments {
            if argument == "PATH" {
                command.arg(&path);
            } else {
                command.arg(argument.as_str().unwrap());
            }
        }
        command
            .arg("--diagnostics=json")
            .env("AWS_ACCESS_KEY_ID", "secret-unused-key")
            .env("AWS_SECRET_ACCESS_KEY", "secret-unused-password")
            .env("AWS_SESSION_TOKEN", "secret-unused-session")
            .env("AWS_REGION", "secret-unused-region");
        for argument in command.get_args() {
            let argument = argument.to_string_lossy();
            for value in [
                "secret-file-key",
                password.as_str(),
                "secret-${HOME}#literal",
            ] {
                assert!(!argument.contains(value));
            }
        }
        assert_read_result(run_with_timeout(&mut command)?, operation)?;
    }
    assert!(!marker.exists());
    fixture.assert_signed_s3_requests(
        "secret-file-key",
        &password,
        Some("secret-${HOME}#literal"),
        "eu-west-1",
    )?;
    Ok(())
}

#[test]
fn saved_results_record_credential_references_without_values_or_hashes() -> TestResult {
    use sha2::{Digest, Sha256};

    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    fixture.write_options(json!({"region": "eu-west-1"}))?;
    let credentials =
        b"AWS_ACCESS_KEY_ID=secret-artifact-key\nAWS_SECRET_ACCESS_KEY=secret-artifact-password\n";
    let path = fixture.directory.path().join("credentials.env");
    fs::write(&path, credentials)?;
    let credential_hash = format!("{:x}", Sha256::digest(credentials));
    let options_hash = format!(
        "{:x}",
        Sha256::digest(fs::read(fixture.directory.path().join("options.json"))?)
    );
    for operation in ["scan", "query"] {
        let destination = fixture.directory.path().join(operation);
        let output = run_with_timeout(
            fixture
                .command(operation)
                .arg("--env-file")
                .arg(&path)
                .args(["--manifest", "--output"])
                .arg(&destination)
                .arg("--diagnostics=json"),
        )?;
        assert!(output.status.success(), "{output:?}");
        let receipt: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        let manifest = fs::read_to_string(receipt["manifest"].as_str().unwrap())?;
        for text in [
            &manifest,
            &String::from_utf8(output.stdout)?,
            &String::from_utf8(output.stderr)?,
        ] {
            for secret in ["secret-artifact", &credential_hash, &options_hash] {
                assert!(!text.contains(secret));
            }
        }
        let manifest: serde_json::Value = serde_json::from_str(&manifest)?;
        assert_eq!(
            manifest["replay"]["options"]["env-file"],
            path.to_str().unwrap()
        );
        assert!(
            manifest["replay"]["inputs"]
                .as_array()
                .unwrap()
                .iter()
                .all(|input| input["argument"] == "--sql-file")
        );
    }
    fixture.assert_signed_s3_requests(
        "secret-artifact-key",
        "secret-artifact-password",
        None,
        "eu-west-1",
    )?;
    Ok(())
}

#[test]
fn malformed_and_incomplete_files_never_use_ambient_credentials() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    fixture.write_options(json!({"region":"eu-west-1"}))?;
    let path = fixture.directory.path().join("secret-selected.env");
    for (bytes, reason) in [
        (b"".to_vec(), "env_file_empty"),
        (
            b"# comment\nAWS_ACCESS_KEY_ID='secret-value' trailing".to_vec(),
            "env_file_invalid_record",
        ),
        (
            b"AWS_ACCESS_KEY_ID=secret-one\nAWS_ACCESS_KEY_ID=secret-two".to_vec(),
            "env_file_duplicate_name",
        ),
        (b"AWS_ACCESS_KEY_ID=\xff".to_vec(), "env_file_not_utf8"),
        (vec![b'x'; 1024 * 1024 + 1], "input_file_too_large"),
        (
            b"PATH=secret-program\nAWS_ACCESS_KEY_ID=secret-key".to_vec(),
            "env_file_unsupported_variable",
        ),
        (
            b"AWS_ACCESS_KEY_ID=secret-key".to_vec(),
            "auth_source_incomplete_credentials",
        ),
    ] {
        fs::write(&path, &bytes)?;
        for machine in [false, true] {
            let mut command = fixture.command("inspect");
            command
                .arg("--env-file")
                .arg(&path)
                .env("AWS_ACCESS_KEY_ID", "secret-unused-key")
                .env("AWS_SECRET_ACCESS_KEY", "secret-unused-password");
            if machine {
                command.arg("--diagnostics=json");
            }
            let output = run_with_timeout(&mut command)?;
            assert_failure_without_stdout(&output, 2);
            if !machine && reason == "env_file_invalid_record" {
                assert!(String::from_utf8_lossy(&output.stderr).contains("env_file_line=2"));
            }
            if machine {
                let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                assert_eq!(record["reason"], reason);
                if reason == "env_file_invalid_record" {
                    assert_eq!(record["line"], 2);
                    assert_eq!(record["argument"], "--env-file");
                }
                if matches!(
                    reason,
                    "env_file_unsupported_variable" | "auth_source_incomplete_credentials"
                ) {
                    assert_eq!(record["argument"], "--env-file");
                    assert!(
                        record["hint"]
                            .as_str()
                            .unwrap()
                            .contains("capabilities --authorization s3 --detail env-file")
                    );
                }
            }
        }
    }
    fs::remove_file(&path)?;
    let output = run_with_timeout(
        fixture
            .command("inspect")
            .arg("--env-file")
            .arg(&path)
            .arg("--diagnostics=json"),
    )?;
    assert_failure_without_stdout(&output, 2);
    let diagnostic = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(diagnostic["reason"], "input_file_read_failed");
    assert_eq!(diagnostic["argument"], "--env-file");
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn unknown_env_file_variables_offer_only_the_selected_service_guide() -> TestResult {
    for (protocol, service) in [
        (CloudStorageProtocol::S3, "s3"),
        (CloudStorageProtocol::AzureBlob, "azure-blob"),
        (CloudStorageProtocol::Gcs, "gcs"),
    ] {
        let fixture = DirectStorageFixture::new(protocol)?;
        fixture.write_options(json!({}))?;
        let path = fixture.directory.path().join("secret-credentials.env");
        fs::write(&path, "SECRET_UNKNOWN_NAME=secret-value")?;
        let output = run_with_timeout(
            fixture
                .command("inspect")
                .arg("--env-file")
                .arg(path)
                .arg("--diagnostics=json"),
        )?;
        assert_failure_without_stdout(&output, 2);
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["argument"], "--env-file");
        let hint = record["hint"].as_str().unwrap();
        assert!(hint.contains(&format!(
            "capabilities --authorization {service} --detail env-file"
        )));
        assert!(!hint.contains("--full"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("SECRET_UNKNOWN_NAME"));
        assert!(fixture.server.requests.lock().unwrap().is_empty());
        let guide = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).args([
            "capabilities",
            "--authorization",
            service,
            "--detail",
            "env-file",
        ]))?;
        assert!(guide.status.success());
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&guide.stdout)?["name"],
            service
        );
    }
    Ok(())
}

#[test]
fn env_file_rejects_json_credentials_and_other_explicit_sources() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    let path = fixture.directory.path().join("selected.env");
    fs::write(
        &path,
        "AWS_ACCESS_KEY_ID=secret-file-key\nAWS_SECRET_ACCESS_KEY=secret-file-password",
    )?;
    for options in [
        json!({"access_key_id":"secret-json-key"}),
        json!({"AWS_SECRET_ACCESS_KEY":"secret-json-password"}),
        json!({"session_token":"secret-json-session"}),
    ] {
        fixture.write_options(options)?;
        let output = run_with_timeout(
            fixture
                .command("inspect")
                .arg("--env-file")
                .arg(&path)
                .arg("--diagnostics=json"),
        )?;
        assert_authorization_input_error(&output, "auth_source_conflicting_credentials");
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["argument"], "--storage-options-file");
        assert!(
            record["expected"]
                .as_str()
                .unwrap()
                .contains("without credentials")
        );
    }
    for args in [
        vec!["inspect", "s3://bucket/table", "--auth-source", "env"],
        vec![
            "inspect",
            "uc://main.sales.orders",
            "--databricks-profile",
            "secret-profile",
        ],
        vec![
            "query",
            "--tables-file",
            "secret-manifest.json",
            "--sql-file",
            "secret-query.sql",
        ],
    ] {
        let output = run_with_timeout(
            Command::new(env!("CARGO_BIN_EXE_dar"))
                .env_clear()
                .args(args)
                .arg("--env-file")
                .arg(&path)
                .arg("--diagnostics=json"),
        )?;
        assert_failure_without_stdout(&output, 2);
        assert_eq!(
            assert_single_terminal_record(&output.stderr, "diagnostic")["argument_kind"],
            "ArgumentConflict"
        );
    }
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn a_working_directory_env_file_does_not_select_credentials() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
    fixture.write_options(json!({"region": "eu-west-1"}))?;
    fs::write(
        fixture.directory.path().join(".env"),
        "AWS_ACCESS_KEY_ID=secret-file-key\nAWS_SECRET_ACCESS_KEY=secret-file-password",
    )?;
    let output = run_with_timeout(
        fixture
            .command("inspect")
            .current_dir(fixture.directory.path())
            .args(["--auth-source", "env", "--diagnostics=json"]),
    )?;
    assert_authorization_input_error(&output, "auth_source_incomplete_credentials");
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn selected_file_is_captured_once_and_permission_failures_keep_the_same_identity() -> TestResult {
    for denied in [false, true] {
        let mut fixture = DirectStorageFixture::new(CloudStorageProtocol::S3)?;
        let path = fixture.directory.path().join("selected.env");
        fs::write(
            &path,
            "AWS_ACCESS_KEY_ID=secret-file-key\nAWS_SECRET_ACCESS_KEY=secret-file-password\nAWS_REGION=eu-west-1",
        )?;
        let replaced = path.clone();
        fixture.restart_storage_with_request_hook(move |_| {
            fs::write(&replaced, "AWS_ACCESS_KEY_ID=secret-replaced-key").unwrap();
        })?;
        fixture.write_options(json!({}))?;
        if denied {
            fixture.server.deny_all_requests();
        }
        let output = run_with_timeout(
            fixture
                .command("scan")
                .arg("--env-file")
                .arg(&path)
                .arg("--diagnostics=json")
                .env("AWS_ACCESS_KEY_ID", "secret-unused-key")
                .env("AWS_SECRET_ACCESS_KEY", "secret-unused-password"),
        )?;
        if denied {
            assert_failure_without_stdout(&output, 1);
        } else {
            assert_read_result(output, "scan")?;
        }
        fixture.assert_signed_s3_requests(
            "secret-file-key",
            "secret-file-password",
            None,
            "eu-west-1",
        )?;
    }
    Ok(())
}

#[test]
fn azure_env_file_keeps_the_selected_token_and_locates_conflicting_files() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::AzureBlob)?;
    fixture.write_options(json!({"azure_storage_account_name":"account"}))?;
    let path = fixture.directory.path().join("selected.env");
    fs::write(
        &path,
        "AZURE_STORAGE_TOKEN=secret-file-token\nAZURE_STORAGE_ACCOUNT_NAME=unused-account",
    )?;
    let output = run_with_timeout(
        fixture
            .command("preview")
            .arg("--env-file")
            .arg(&path)
            .arg("--diagnostics=json")
            .env("AZURE_STORAGE_TOKEN", "secret-unused-token"),
    )?;
    assert_read_result(output, "preview")?;
    assert!(
        fixture
            .storage_bearer_tokens()
            .iter()
            .all(|token| token == "secret-file-token")
    );
    fixture.server.requests.lock().unwrap().clear();
    fs::write(
        fixture.directory.path().join("query.sql"),
        "SELECT id FROM orders",
    )?;
    for (options, env_contents, argument, hint) in [
        (
            json!({"azure_storage_account_name":"account", "client_id":"secret-json-client"}),
            "AZURE_STORAGE_TOKEN=secret-file-token",
            "--storage-options-file",
            "Remove credential fields from --storage-options-file",
        ),
        (
            json!({"azure_storage_account_name":"account"}),
            "AZURE_STORAGE_TOKEN=secret-file-token\nAZURE_CLIENT_ID=secret-partial-family",
            "--env-file",
            "capabilities --authorization azure-blob --detail env-file",
        ),
    ] {
        fixture.write_options(options)?;
        fs::write(&path, env_contents)?;
        for manifest in [false, true] {
            let mut command = if manifest {
                let tables_path = fixture.directory.path().join("tables.json");
                fs::write(&tables_path, json!({
                    "format_version": 1,
                    "tables": [{"alias":"orders", "location":"az://container/table", "authorization": {
                        "source":"env-file", "env_file":path,
                        "storage_options_file":fixture.directory.path().join("options.json")
                    }}]
                }).to_string())?;
                let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
                command
                    .env_clear()
                    .env("RUST_LOG", "trace")
                    .arg("query")
                    .arg("--tables-file")
                    .arg(tables_path)
                    .arg("--sql-file")
                    .arg(fixture.directory.path().join("query.sql"));
                command
            } else {
                let mut command = fixture.command("inspect");
                command.arg("--env-file").arg(&path);
                command
            };
            let output = run_with_timeout(command.arg("--diagnostics=json"))?;
            assert_authorization_input_error(&output, "auth_source_conflicting_credentials");
            let record = assert_single_terminal_record(&output.stderr, "diagnostic");
            assert!(record["hint"].as_str().unwrap().contains(hint));
            if manifest {
                assert_eq!(record["argument"], "--tables-file");
                let field = if argument == "--env-file" {
                    "env_file"
                } else {
                    "storage_options_file"
                };
                assert_eq!(
                    record["field_path"],
                    format!("/tables/0/authorization/{field}")
                );
            } else {
                assert_eq!(record["argument"], argument);
            }
            assert!(fixture.server.requests.lock().unwrap().is_empty());
        }
    }
    Ok(())
}
