//! Credentials from `--env-file`, combined with optional JSON connection settings.
//!
//! The file supplies the complete identity. JSON can override configuration such
//! as the S3 region, but cannot supply any part of that identity. A relative ADC
//! reference belongs to the env file's directory, not the working directory.

use std::path::Path;

use delta_arrow_reader::{DeltaStorageOptions, DeltaTableBuilder};
use object_store::{ObjectStoreScheme, azure::AzureConfigKey, gcp::GoogleConfigKey};
use snafu::ensure;

use super::{CredentialSource, azure_blob, gcs, normalize_storage_options, s3, storage_scheme};
use crate::{Error, StorageAuthorizationSnafu, input};

pub(crate) fn configure_table_credentials(
    builder: DeltaTableBuilder,
    location: &str,
    json_options: DeltaStorageOptions,
    path: &str,
) -> Result<DeltaTableBuilder, Error> {
    let scheme = storage_scheme(location)?;
    // Apply the existing env source's restrictions before combining either file.
    let configuration = match scheme {
        ObjectStoreScheme::AmazonS3 => normalize_storage_options(json_options, |key, value| {
            s3::validate_storage_option(key, value, CredentialSource::Env)
        }),
        ObjectStoreScheme::MicrosoftAzure => {
            normalize_storage_options(json_options, |key, value| {
                azure_blob::validate_storage_option(key, value, CredentialSource::Env)
            })
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            normalize_storage_options(json_options, |key, value| {
                gcs::validate_storage_option(key, value, CredentialSource::Env)
            })
        }
        _ => unreachable!("storage_scheme accepts only supported cloud backends"),
    }?;
    let mut options = DeltaStorageOptions::new();
    for (name, value) in input::read_env_file(path)? {
        let key = storage_option_key(&name, &scheme).ok_or(Error::StorageAuthorization {
            reason: "env_file_unsupported_variable",
        })?;
        ensure!(
            options.insert(key, value).is_none(),
            StorageAuthorizationSnafu {
                reason: "auth_source_duplicate_option"
            }
        );
    }
    // A JSON region setting replaces both env-file spellings of the same setting.
    if matches!(scheme, ObjectStoreScheme::AmazonS3)
        && (configuration.contains_key("aws_region")
            || configuration.contains_key("aws_default_region"))
    {
        options.remove("aws_region");
        options.remove("aws_default_region");
    }
    options.extend(configuration);
    // JSON credential handling does not fill missing fields from inherited credentials.
    CredentialSource::Json.configure_table_credentials(
        builder,
        location,
        options,
        Some(Path::new(path).parent().unwrap_or(Path::new("."))),
    )
}

fn storage_option_key(name: &str, scheme: &ObjectStoreScheme) -> Option<String> {
    match scheme {
        ObjectStoreScheme::AmazonS3 => s3::CREDENTIAL_ENVIRONMENT_VARIABLES
            .iter()
            .chain(s3::REGION_ENVIRONMENT_VARIABLES)
            .find_map(|(variable, key)| (*variable == name).then(|| key.as_ref())),
        ObjectStoreScheme::MicrosoftAzure
            if name == azure_blob::ACCOUNT_NAME_ENVIRONMENT_VARIABLE =>
        {
            Some(AzureConfigKey::AccountName.as_ref())
        }
        ObjectStoreScheme::MicrosoftAzure => azure_blob::CREDENTIAL_ENVIRONMENT_VARIABLES
            .iter()
            .find_map(|(variable, key)| (*variable == name).then(|| key.as_ref())),
        ObjectStoreScheme::GoogleCloudStorage if name == gcs::ADC_ENVIRONMENT_VARIABLE => {
            Some(GoogleConfigKey::ApplicationCredentials.as_ref())
        }
        _ => None,
    }
    .map(str::to_owned)
}
