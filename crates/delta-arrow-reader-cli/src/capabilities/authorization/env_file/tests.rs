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
        (AuthorizationService::S3, "s3://bucket/table"),
        (AuthorizationService::AzureBlob, "az://container/table"),
        (AuthorizationService::Gcs, "gs://bucket/table"),
    ] {
        let guide = build_env_file_guide(service, "env-file")?;
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
            let _ = crate::table_access::direct_credentials::env_file::configure_credentials_from_env_file(
                    DeltaTableBuilder::new(location),
                    location,
                    DeltaStorageOptions::new(),
                    path.to_str().unwrap(),
                )?;
        }
    }
    Ok(())
}
