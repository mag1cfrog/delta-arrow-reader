//! Reference material for provisioning or correcting a credential env file.
//!
//! Passing an existing file needs only `--env-file PATH`. These views describe
//! its contents when needed: `--authorization s3 --detail env-file` lists S3
//! variables, while `--detail env-file-grammar` describes the shared syntax.

use clap::error::ErrorKind;
use serde_json::{Value, json};

use super::read_support::AuthorizationGuide;
use crate::{Error, diagnostics::InputCorrection, input};

pub(super) fn document(service: AuthorizationGuide, detail: &str) -> Result<Value, Error> {
    if matches!(service, AuthorizationGuide::UnityCatalog)
        || !matches!(detail, "env-file" | "env-file-grammar")
    {
        return Err(Error::Argument {
            kind: ErrorKind::InvalidValue,
            correction: Some(InputCorrection {
                argument: "--detail".into(),
                field_path: None,
                expected: "env-file or env-file-grammar for s3, azure-blob, or gcs".into(),
                supplied: None,
                hint: "Select a direct-storage authorization guide.",
            }),
        });
    }
    let mut document = if detail == "env-file-grammar" {
        json!({
            "max_file_bytes": input::MAX_INPUT_BYTES,
            "min_records": 1,
            "max_records": input::MAX_ENV_FILE_RECORDS,
            "syntax": [
                "UTF-8 with LF or CRLF. Ignore blank lines and whole-line # comments.",
                "One NAME=VALUE record per line. NAME matches [A-Za-z_][A-Za-z0-9_]*. Trim spaces/tabs outside names and values.",
                "Values are unquoted text without quotes, or enclosed in matching single/double quotes. The enclosing quote cannot occur inside; trailing text is invalid.",
                "Quoted whitespace is preserved. Reject empty or whitespace-only values, duplicate names and control characters including NUL.",
                "No escapes, multiline values, inline comments, export, interpolation or execution. Dollar signs, backticks, # and backslashes inside values are literal.",
            ],
            "variables_command": [crate::command_name(), "capabilities", "--authorization", service, "--detail", "env-file"],
        })
    } else {
        let mut variables = match service {
            AuthorizationGuide::S3 => json!({
                "required": ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
                "optional": ["AWS_SESSION_TOKEN", "AWS_REGION", "AWS_DEFAULT_REGION"],
                "notes": "AWS_REGION overrides AWS_DEFAULT_REGION; JSON region settings override both. Set the bucket's region; the native default is us-east-1.",
            }),
            AuthorizationGuide::AzureBlob => json!({
                "one_credential_family": [
                    ["AZURE_STORAGE_ACCOUNT_KEY"],
                    ["AZURE_STORAGE_SAS_KEY"],
                    ["AZURE_STORAGE_TOKEN"],
                    ["AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID"],
                ],
                "configuration": ["AZURE_STORAGE_ACCOUNT_NAME"],
                "notes": "Use exactly one complete family. Supply the account name if absent from the URL; JSON account settings override the env file. Client-secret tokens renew for the loaded identity.",
            }),
            AuthorizationGuide::Gcs => json!({
                "required": ["GOOGLE_APPLICATION_CREDENTIALS"],
                "notes": "Reference a service-account or authorized-user ADC file. Relative paths use the env file's directory. No default ADC lookup; tokens renew for the loaded identity.",
            }),
            AuthorizationGuide::UnityCatalog => {
                unreachable!("rejected before constructing the guide")
            }
        };
        variables["names"] = json!(
            "Only these case-sensitive names are accepted; JSON key aliases are not env-file names."
        );
        json!({
            "variables": variables,
            "grammar_command": [crate::command_name(), "capabilities", "--authorization", service, "--detail", "env-file-grammar"],
        })
    };
    document["format_version"] = json!(1);
    document["kind"] = json!("authorization_detail");
    document["name"] = json!(service);
    document["detail"] = json!(detail);
    Ok(document)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use delta_arrow_reader::{DeltaStorageOptions, DeltaTableBuilder};

    use super::*;

    #[test]
    fn published_variables_configure_each_supported_credential_family()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let adc = directory.path().join("adc.json");
        std::fs::write(
            &adc,
            json!({
                "type":"authorized_user", "client_id":"secret-client",
                "client_secret":"secret-password", "refresh_token":"secret-refresh",
            })
            .to_string(),
        )?;
        let path = directory.path().join("selected.env");
        for (service, location) in [
            (AuthorizationGuide::S3, "s3://bucket/table"),
            (AuthorizationGuide::AzureBlob, "az://container/table"),
            (AuthorizationGuide::Gcs, "gs://bucket/table"),
        ] {
            let guide = document(service, "env-file")?;
            let variables = &guide["variables"];
            let families = variables["one_credential_family"]
                .as_array()
                .cloned()
                .unwrap_or_else(|| vec![variables["required"].clone()]);
            for family in families {
                let mut records = String::new();
                for name in family
                    .as_array()
                    .unwrap()
                    .iter()
                    .chain(variables["optional"].as_array().into_iter().flatten())
                    .chain(variables["configuration"].as_array().into_iter().flatten())
                {
                    let name = name.as_str().unwrap();
                    let value = match name {
                        "AZURE_STORAGE_ACCOUNT_KEY" => "c2VjcmV0",
                        "AZURE_STORAGE_ACCOUNT_NAME" => "account",
                        "AZURE_STORAGE_SAS_KEY" => "sig=secret&sp=rl",
                        "AWS_REGION" | "AWS_DEFAULT_REGION" => "eu-west-1",
                        "GOOGLE_APPLICATION_CREDENTIALS" => "adc.json",
                        _ => "secret-value",
                    };
                    records.push_str(&format!("{name}={value}\n"));
                }
                std::fs::write(&path, records)?;
                let _ = crate::direct_credentials::env_file::configure_table_credentials(
                    DeltaTableBuilder::new(location),
                    location,
                    DeltaStorageOptions::new(),
                    path.to_str().unwrap(),
                )?;
            }
        }
        Ok(())
    }
}
