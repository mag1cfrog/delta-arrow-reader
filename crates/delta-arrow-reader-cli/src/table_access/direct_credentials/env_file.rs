//! Credentials from `--env-file`, combined with optional JSON connection settings.
//!
//! The file supplies the complete identity. JSON can override configuration such
//! as the S3 region, but cannot supply any part of that identity. A relative ADC
//! reference belongs to the env file's directory, not the working directory.

use std::path::Path;

use delta_arrow_reader::{DeltaStorageOptions, DeltaTableBuilder};
use object_store::{ObjectStoreScheme, azure::AzureConfigKey, gcp::GoogleConfigKey};

use super::{
    CredentialSource, azure_blob, gcs, normalize_storage_options, parse_supported_storage_scheme,
    s3,
};
use crate::{
    diagnostics::{Error, InputCorrection},
    input,
};

pub(crate) fn configure_credentials_from_env_file(
    builder: DeltaTableBuilder,
    location: &str,
    json_options: DeltaStorageOptions,
    path: &str,
) -> Result<DeltaTableBuilder, Error> {
    let scheme = parse_supported_storage_scheme(location)?;
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
        _ => unreachable!("parse_supported_storage_scheme accepts only supported cloud backends"),
    }
    .map_err(|source| {
        let hint = if matches!(source, Error::StorageAuthorization { reason: "auth_source_conflicting_credentials" }) {
            "Remove credential fields from --storage-options-file; --env-file supplies the complete identity."
        } else {
            match scheme {
                ObjectStoreScheme::AmazonS3 => "Check connection settings with capabilities --authorization s3 --detail connection; keep credentials only in --env-file.",
                ObjectStoreScheme::MicrosoftAzure => "Check connection settings with capabilities --authorization azure-blob --detail connection; keep credentials only in --env-file.",
                ObjectStoreScheme::GoogleCloudStorage => "Check connection settings with capabilities --authorization gcs --detail connection; keep credentials only in --env-file.",
                _ => unreachable!("validated cloud backend"),
            }
        };
        Error::CredentialFileValidation {
            source: Box::new(source),
            correction: InputCorrection {
                argument: "--storage-options-file".into(),
                field_path: None,
                expected: "supported connection settings without credentials".into(),
                supplied: None,
                hint,
            },
        }
    })?;
    let env_file_validation_error = |source| Error::CredentialFileValidation {
        source: Box::new(source),
        correction: InputCorrection {
            argument: "--env-file".into(),
            field_path: None,
            expected: "supported variables for one complete credential family".into(),
            supplied: None,
            hint: match scheme {
                ObjectStoreScheme::AmazonS3 => {
                    "Check variables with capabilities --authorization s3 --detail env-file; keep the identity in the selected file."
                }
                ObjectStoreScheme::MicrosoftAzure => {
                    "Check variables with capabilities --authorization azure-blob --detail env-file; keep the identity in the selected file."
                }
                ObjectStoreScheme::GoogleCloudStorage => {
                    "Check variables with capabilities --authorization gcs --detail env-file; keep the identity in the selected file."
                }
                _ => unreachable!("validated cloud backend"),
            },
        },
    };
    let mut options = DeltaStorageOptions::new();
    for (name, value) in input::read_env_file(path)? {
        let key = storage_key_for_env_variable(&name, &scheme).ok_or_else(|| {
            env_file_validation_error(Error::StorageAuthorization {
                reason: "env_file_unsupported_variable",
            })
        })?;
        if options.insert(key, value).is_some() {
            return Err(env_file_validation_error(Error::StorageAuthorization {
                reason: "auth_source_duplicate_option",
            }));
        }
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
    ).map_err(|source| match source {
        Error::StorageAuthorization { reason: "auth_source_incomplete_credentials" | "auth_source_conflicting_credentials" } => env_file_validation_error(source),
        _ => source,
    })
}

fn storage_key_for_env_variable(variable_name: &str, scheme: &ObjectStoreScheme) -> Option<String> {
    match scheme {
        ObjectStoreScheme::AmazonS3 => s3::CREDENTIAL_ENVIRONMENT_VARIABLES
            .iter()
            .chain(s3::REGION_ENVIRONMENT_VARIABLES)
            .find_map(|(variable, key)| (*variable == variable_name).then(|| key.as_ref())),
        ObjectStoreScheme::MicrosoftAzure
            if variable_name == azure_blob::ACCOUNT_NAME_ENVIRONMENT_VARIABLE =>
        {
            Some(AzureConfigKey::AccountName.as_ref())
        }
        ObjectStoreScheme::MicrosoftAzure => azure_blob::CREDENTIAL_ENVIRONMENT_VARIABLES
            .iter()
            .find_map(|(variable, key)| (*variable == variable_name).then(|| key.as_ref())),
        ObjectStoreScheme::GoogleCloudStorage if variable_name == gcs::ADC_ENVIRONMENT_VARIABLE => {
            Some(GoogleConfigKey::ApplicationCredentials.as_ref())
        }
        _ => None,
    }
    .map(str::to_owned)
}
