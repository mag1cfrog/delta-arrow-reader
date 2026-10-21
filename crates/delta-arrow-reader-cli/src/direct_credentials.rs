//! Explicit credential selection for direct cloud reads.

mod azure_blob;
mod credential_file_paths;
mod gcs;
mod s3;

use std::{env, path::Path, str::FromStr};

use clap::ValueEnum;
use delta_arrow_reader::{DeltaStorageOptions, DeltaTableBuilder};
use object_store::ObjectStoreScheme;
use snafu::ensure;
use url::Url;

use crate::{Error, StorageAuthorizationSnafu};

/// Selects one source; missing or rejected credentials never select another identity.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum CredentialSource {
    /// Read credentials from the explicitly supplied storage-options file.
    Json,
    /// Read the advertised credential variables from the inherited environment.
    Env,
    /// Assume an AWS role using an explicitly configured web identity token file.
    AwsWebIdentity,
    /// Use explicitly configured ECS task or EKS pod credentials.
    AwsContainer,
    /// Use Azure workload identity with an explicitly configured token file.
    AzureWorkloadIdentity,
    /// Use Azure managed identity with one explicitly selected identity ID.
    AzureManagedIdentity,
}

impl CredentialSource {
    /// Pins the selected identity and retains noncredential storage options.
    pub(crate) fn configure_table_credentials(
        self,
        builder: DeltaTableBuilder,
        location: &str,
        mut options: DeltaStorageOptions,
        credential_file_directory: Option<&Path>,
    ) -> Result<DeltaTableBuilder, Error> {
        let unsupported_storage = || Error::StorageAuthorization {
            reason: "auth_source_unsupported_storage",
        };
        let url = Url::parse(location).map_err(|_| unsupported_storage())?;
        let (scheme, _) = ObjectStoreScheme::parse(&url).map_err(|_| unsupported_storage())?;
        ensure!(
            matches!(
                scheme,
                ObjectStoreScheme::AmazonS3
                    | ObjectStoreScheme::MicrosoftAzure
                    | ObjectStoreScheme::GoogleCloudStorage
            ),
            StorageAuthorizationSnafu {
                reason: "auth_source_unsupported_storage"
            }
        );
        validate_table_url(&url, &scheme)?;
        if let Some(directory) = credential_file_directory {
            credential_file_paths::resolve_relative_paths(&mut options, &scheme, directory)?;
        }
        match (scheme, self) {
            (
                ObjectStoreScheme::AmazonS3,
                Self::Json | Self::Env | Self::AwsWebIdentity | Self::AwsContainer,
            ) => s3::configure_table_credentials(builder, location, options, self),
            (
                ObjectStoreScheme::MicrosoftAzure,
                Self::Json | Self::Env | Self::AzureWorkloadIdentity | Self::AzureManagedIdentity,
            ) => azure_blob::configure_table_credentials(builder, location, options, self),
            (ObjectStoreScheme::GoogleCloudStorage, Self::Json | Self::Env) => {
                gcs::configure_table_credentials(builder, location, options, self)
            }
            _ => Err(unsupported_storage()),
        }
    }
}

/// Azure Blob URLs may use container@account; passwords and embedded tokens remain invalid.
pub(crate) fn validate_table_url(url: &Url, scheme: &ObjectStoreScheme) -> Result<(), Error> {
    ensure!(
        (url.username().is_empty()
            || (*scheme == ObjectStoreScheme::MicrosoftAzure
                && matches!(url.scheme(), "az" | "abfs" | "abfss")))
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        StorageAuthorizationSnafu {
            reason: "auth_source_invalid_table_url"
        }
    );
    Ok(())
}

/// Uses native aliases and rejects duplicates after the provider validates each option.
fn normalize_storage_options<ConfigKey: FromStr + AsRef<str>>(
    options: DeltaStorageOptions,
    validate_option: impl Fn(&ConfigKey, &str) -> Result<(), Error>,
) -> Result<DeltaStorageOptions, Error> {
    let mut normalized = DeltaStorageOptions::new();
    for (name, value) in options {
        let key = name
            .to_ascii_lowercase()
            .parse::<ConfigKey>()
            .map_err(|_| Error::StorageAuthorization {
                reason: "auth_source_unsupported_option",
            })?;
        validate_option(&key, &value)?;
        ensure!(
            normalized.insert(key.as_ref().to_owned(), value).is_none(),
            StorageAuthorizationSnafu {
                reason: "auth_source_duplicate_option"
            }
        );
    }
    Ok(normalized)
}

fn read_environment_value(name: &str) -> Result<Option<String>, Error> {
    match env::var(name) {
        Ok(value) => {
            validate_auth_source_value(&value)?;
            Ok(Some(value))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => StorageAuthorizationSnafu {
            reason: "auth_source_non_utf8_environment",
        }
        .fail(),
    }
}

fn validate_auth_source_value(value: &str) -> Result<(), Error> {
    ensure!(
        !value.trim().is_empty() && !value.chars().any(char::is_control),
        StorageAuthorizationSnafu {
            reason: "auth_source_invalid_value"
        }
    );
    Ok(())
}
