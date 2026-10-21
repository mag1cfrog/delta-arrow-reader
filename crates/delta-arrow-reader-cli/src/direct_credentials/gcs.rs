//! Explicit ADC files with native GCS token generation and renewal.

use std::sync::Arc;

use delta_arrow_reader::{DeltaStorageCredentialProvider, DeltaStorageOptions, DeltaTableBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use snafu::{OptionExt, ensure};

use super::{
    CredentialSource, normalize_storage_options, read_environment_value, validate_auth_source_value,
};
use crate::{Error, StorageAuthorizationSnafu};

pub(super) fn configure_table_credentials(
    builder: DeltaTableBuilder,
    location: &str,
    options: DeltaStorageOptions,
    source: CredentialSource,
) -> Result<DeltaTableBuilder, Error> {
    let mut options = normalize_storage_options(options, |key, value| {
        validate_storage_option(key, value, source)
    })?;
    let credentials_path = match source {
        CredentialSource::Json => options.remove("google_application_credentials"),
        CredentialSource::Env => read_environment_value("GOOGLE_APPLICATION_CREDENTIALS")?,
        CredentialSource::AwsWebIdentity
        | CredentialSource::AwsContainer
        | CredentialSource::AzureWorkloadIdentity
        | CredentialSource::AzureManagedIdentity => {
            return StorageAuthorizationSnafu {
                reason: "auth_source_unsupported_storage",
            }
            .fail();
        }
    }
    .context(StorageAuthorizationSnafu {
        reason: "auth_source_incomplete_credentials",
    })?;

    // An explicit ADC file prevents native home-directory lookup and metadata fallback.
    let mut storage_builder = GoogleCloudStorageBuilder::new()
        .with_url(location)
        .with_application_credentials(credentials_path);
    for (name, value) in &options {
        let key = name
            .parse::<GoogleConfigKey>()
            .map_err(|_| Error::StorageAuthorization {
                reason: "auth_source_unsupported_option",
            })?;
        storage_builder = storage_builder.with_config(key, value);
    }
    let storage = storage_builder
        .build()
        .map_err(|_| Error::StorageAuthorization {
            reason: "auth_source_invalid_value",
        })?;
    // The provider owns the loaded identity and refreshes tokens without reopening the file.
    Ok(builder
        .with_storage_options(options)
        .with_credential_provider(DeltaStorageCredentialProvider::Gcp(Arc::clone(
            storage.credentials(),
        ))))
}

fn validate_storage_option(
    key: &GoogleConfigKey,
    value: &str,
    source: CredentialSource,
) -> Result<(), Error> {
    match key {
        GoogleConfigKey::ApplicationCredentials => {
            ensure!(
                matches!(source, CredentialSource::Json),
                StorageAuthorizationSnafu {
                    reason: "auth_source_conflicting_credentials"
                }
            );
            validate_auth_source_value(value)?;
        }
        GoogleConfigKey::BaseUrl | GoogleConfigKey::Client(_) => {}
        _ => {
            return StorageAuthorizationSnafu {
                reason: "auth_source_unsupported_option",
            }
            .fail();
        }
    }
    Ok(())
}
