//! Azure credentials are selected once and reach every storage request.

use std::{collections::HashMap, process::Command};

use serde_json::json;
use url::Url;

use super::{
    CloudStorageProtocol, DirectStorageFixture, TestResult, assert_read_result,
    assert_single_terminal_record, run_with_timeout,
};

const ACCOUNT_KEY: &str = "c2VjcmV0LXNlbGVjdGVkLWtleQ==";
const SAS: &str = "?sv=2025-01-05&sp=rl&sig=secret-sas%2Bsignature%2F%3D";
const BEARER_TOKEN: &str = "secret-bearer-token";
// JSON alias, advertised environment variable, and a synthetic credential.
const CREDENTIALS: [(&str, &str, &str); 3] = [
    ("ACCESS_KEY", "AZURE_STORAGE_ACCOUNT_KEY", ACCOUNT_KEY),
    ("sas_token", "AZURE_STORAGE_SAS_KEY", SAS),
    ("bearer_token", "AZURE_STORAGE_TOKEN", BEARER_TOKEN),
];

fn assert_request_credentials(fixture: &DirectStorageFixture, option: &str) {
    let requests = fixture.server.requests.lock().unwrap();
    assert!(!requests.is_empty());
    for request in requests.iter() {
        let authorization = request.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("authorization")
                .then(|| value.trim())
        });
        let target = request.split_whitespace().nth(1).unwrap();
        let url = Url::parse(&format!("http://localhost{target}")).unwrap();
        let parameters: HashMap<_, _> = url.query_pairs().collect();
        match option {
            "ACCESS_KEY" => {
                assert!(
                    authorization.unwrap().starts_with("SharedKey account:"),
                    "{request}"
                );
                assert!(!parameters.contains_key("sig"), "{request}");
            }
            "sas_token" => {
                assert_eq!(authorization, None, "{request}");
                assert_eq!(
                    parameters.get("sig").map(|value| value.as_ref()),
                    Some("secret-sas+signature/=")
                );
                assert_eq!(parameters.get("sp").map(|value| value.as_ref()), Some("rl"));
            }
            "bearer_token" => {
                assert_eq!(
                    authorization,
                    Some("Bearer secret-bearer-token"),
                    "{request}"
                );
                assert!(!parameters.contains_key("sig"), "{request}");
            }
            _ => panic!("unexpected test credential"),
        }
        assert!(!request.contains("secret-unused"), "{request}");
    }
}

#[test]
fn selected_credentials_authorize_inspection_scan_and_sql() -> TestResult {
    for source in ["json", "env"] {
        for (option, variable, value) in CREDENTIALS {
            let fixture = DirectStorageFixture::new(CloudStorageProtocol::Azure)?;
            let mut options = json!({"account_name":"account"});
            if source == "json" {
                options[option] = json!(value);
            }
            fixture.write_options(options)?;
            for operation in ["inspect", "scan", "query"] {
                let mut command = fixture.command(operation);
                command
                    .args(["--auth-source", source, "--diagnostics=json"])
                    .env("AZURE_STORAGE_ACCOUNT_NAME", "secret-unused-account")
                    .env("AZURE_USE_AZURE_CLI", "true")
                    .env("AZURE_STORAGE_USE_EMULATOR", "true")
                    .env("AZURE_SKIP_SIGNATURE", "true")
                    .env("AZURE_CLIENT_ID", "secret-unused-client")
                    .env("AZURE_FEDERATED_TOKEN_FILE", "secret-unused-file");
                if source == "env" {
                    command.env(variable, value);
                } else {
                    command.env("AZURE_STORAGE_TOKEN", "secret-unused-token");
                }
                if operation == "scan" {
                    command.args(["--column", "id"]);
                } else if operation == "query" {
                    command
                        .arg("--sql-file")
                        .arg(fixture.directory.path().join("query.sql"));
                }
                let output = run_with_timeout(&mut command)?;
                assert!(!String::from_utf8_lossy(&output.stdout).contains(ACCOUNT_KEY));
                assert!(!String::from_utf8_lossy(&output.stderr).contains(ACCOUNT_KEY));
                assert_read_result(output, operation)?;
            }
            assert_request_credentials(&fixture, option);
        }
    }
    Ok(())
}

#[test]
fn account_names_come_from_json_environment_or_native_urls() -> TestResult {
    for (table, source, file_account, environment_account) in [
        (
            "az://container/table",
            "json",
            Some("account"),
            Some("secret-unused-account"),
        ),
        ("az://container/table", "env", None, Some("account")),
        (
            "abfss://container@account.dfs.core.windows.net/table",
            "env",
            None,
            None,
        ),
        (
            "abfss://container@account.dfs.core.windows.net/table",
            "env",
            Some("secret-unused-file-account"),
            Some("secret-unused-env-account"),
        ),
        (
            "https://account.blob.core.windows.net/container/table",
            "json",
            None,
            None,
        ),
    ] {
        let fixture = DirectStorageFixture::new(CloudStorageProtocol::Azure)?;
        let mut options = json!({});
        if source == "json" {
            options["account_key"] = json!(ACCOUNT_KEY);
        }
        if let Some(account) = file_account {
            options["account_name"] = json!(account);
        }
        fixture.write_options(options)?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
        command
            .env_clear()
            .args([
                "inspect",
                table,
                "--auth-source",
                source,
                "--diagnostics=json",
                "--storage-options-file",
            ])
            .arg(fixture.directory.path().join("options.json"));
        if source == "env" {
            command.env("AZURE_STORAGE_ACCOUNT_KEY", ACCOUNT_KEY);
        }
        if let Some(account) = environment_account {
            command.env("AZURE_STORAGE_ACCOUNT_NAME", account);
        }
        assert_read_result(run_with_timeout(&mut command)?, "inspect")?;
        assert_request_credentials(&fixture, "ACCESS_KEY");
    }

    let fixture = DirectStorageFixture::new(CloudStorageProtocol::Azure)?;
    fixture.write_options(json!({"account_key":ACCOUNT_KEY}))?;
    let output = run_with_timeout(
        fixture
            .command("inspect")
            .args(["--auth-source", "json", "--diagnostics=json"])
            .env("AZURE_STORAGE_ACCOUNT_NAME", "secret-unused-account"),
    )?;
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
    assert_eq!(record["reason"], "auth_source_invalid_value");
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn invalid_json_sources_fail_before_io_without_using_ambient_credentials() -> TestResult {
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::Azure)?;
    for (options, reason) in [
        (json!({}), "auth_source_incomplete_credentials"),
        (
            json!({"account_key":"secret-not-base64"}),
            "auth_source_invalid_value",
        ),
        (json!({"token":""}), "auth_source_invalid_value"),
        (json!({"token":"secret token"}), "auth_source_invalid_value"),
        (
            json!({"token":"secret-\u{fffd}"}),
            "auth_source_invalid_value",
        ),
        (json!({"sas_key":"sp=rl"}), "auth_source_invalid_value"),
        (
            json!({"sas_key":"sig=secret&malformed"}),
            "auth_source_invalid_value",
        ),
        (json!({"sas_key":"sig=%FF"}), "auth_source_invalid_value"),
        (
            json!({"token":BEARER_TOKEN, "account_key":ACCOUNT_KEY}),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({"sas_key":SAS, "token":BEARER_TOKEN}),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({"account_key":ACCOUNT_KEY, "access_key":ACCOUNT_KEY}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"sas_key":SAS, "AZURE_STORAGE_SAS_TOKEN":SAS}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"allow_http":"true", "azure_allow_http":"true"}),
            "auth_source_duplicate_option",
        ),
        (
            json!({"use_emulator":"false"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"skip_signature":"false"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"client_id":"secret-client"}),
            "auth_source_unsupported_option",
        ),
        (
            json!({"container_name":"secret-container"}),
            "auth_source_unsupported_option",
        ),
    ] {
        let mut options = options;
        options["account_name"] = json!("account");
        fixture.write_options(options)?;
        let output = run_with_timeout(
            fixture
                .command("inspect")
                .args(["--auth-source", "json", "--diagnostics=json"])
                .env("AZURE_STORAGE_TOKEN", BEARER_TOKEN),
        )?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert!(!String::from_utf8_lossy(&output.stderr).contains(ACCOUNT_KEY));
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["code"], "invalid_storage_authorization");
        assert_eq!(record["reason"], reason);
    }
    assert!(fixture.server.requests.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn environment_requires_exactly_one_family_and_rejects_json_credentials() -> TestResult {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let fixture = DirectStorageFixture::new(CloudStorageProtocol::Azure)?;
    for (options, token, key, reason) in [
        (json!({}), None, None, "auth_source_incomplete_credentials"),
        (
            json!({}),
            Some(OsString::from(BEARER_TOKEN)),
            Some(ACCOUNT_KEY),
            "auth_source_conflicting_credentials",
        ),
        (
            json!({}),
            Some(OsString::new()),
            None,
            "auth_source_invalid_value",
        ),
        (
            json!({}),
            Some(OsString::from_vec(vec![0xff])),
            None,
            "auth_source_non_utf8_environment",
        ),
        (
            json!({"token":BEARER_TOKEN}),
            Some(OsString::from(BEARER_TOKEN)),
            None,
            "auth_source_conflicting_credentials",
        ),
    ] {
        let mut options = options;
        options["account_name"] = json!("account");
        fixture.write_options(options)?;
        let mut command = fixture.command("inspect");
        command.args(["--auth-source", "env", "--diagnostics=json"]);
        if let Some(token) = token {
            command.env("AZURE_STORAGE_TOKEN", token);
        }
        if let Some(key) = key {
            command.env("AZURE_STORAGE_ACCOUNT_KEY", key);
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
fn permission_denial_keeps_the_selected_identity_and_redacts_credentials() -> TestResult {
    for source in ["json", "env"] {
        for (option, variable, value) in CREDENTIALS {
            let fixture = DirectStorageFixture::new(CloudStorageProtocol::Azure)?;
            fixture.server.deny_all_requests();
            let mut options = json!({"account_name":"account"});
            if source == "json" {
                options[option] = json!(value);
            }
            fixture.write_options(options)?;
            for machine in [false, true] {
                let mut command = fixture.command("inspect");
                command.args(["--auth-source", source]).env(
                    "AZURE_MSI_ENDPOINT",
                    format!("{}/secret-unused-provider", fixture.server.endpoint),
                );
                if source == "env" {
                    command.env(variable, value);
                } else {
                    command.env("AZURE_STORAGE_TOKEN", "secret-unused-token");
                }
                if machine {
                    command.arg("--diagnostics=json");
                }
                let output = run_with_timeout(&mut command)?;
                assert_eq!(output.status.code(), Some(1));
                assert!(output.stdout.is_empty());
                let stderr = String::from_utf8_lossy(&output.stderr);
                assert!(!stderr.contains("secret"), "{stderr}");
                assert!(!stderr.contains(ACCOUNT_KEY), "{stderr}");
                if machine {
                    let record = assert_single_terminal_record(&output.stderr, "diagnostic");
                    assert_eq!(record["source_kind"], "permission_denied");
                }
            }
            assert_request_credentials(&fixture, option);
        }
    }
    Ok(())
}

#[test]
fn table_urls_reject_embedded_credentials() -> TestResult {
    for table in [
        "az://container/table?sig=secret",
        "abfss://container:secret@account.dfs.core.windows.net/table",
        "https://secret@account.blob.core.windows.net/container/table",
    ] {
        let output = run_with_timeout(Command::new(env!("CARGO_BIN_EXE_dar")).env_clear().args([
            "inspect",
            table,
            "--auth-source",
            "env",
            "--diagnostics=json",
        ]))?;
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let record = assert_single_terminal_record(&output.stderr, "diagnostic");
        assert_eq!(record["reason"], "auth_source_invalid_table_url");
    }
    Ok(())
}
