//! Explicit ADC files with native GCS token generation and renewal.

use std::sync::Arc;

use delta_arrow_reader::{DeltaStorageCredentialProvider, DeltaStorageOptions, DeltaTableBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
use snafu::{OptionExt, ensure};

use super::{CredentialSource, read_environment_value, validate_auth_source_value};
use crate::{Error, StorageAuthorizationSnafu};

pub(super) fn configure_table_credentials(
    builder: DeltaTableBuilder,
    location: &str,
    options: DeltaStorageOptions,
    source: CredentialSource,
) -> Result<DeltaTableBuilder, Error> {
    let mut options = normalize_gcs_options(options, source)?;
    let credentials_path = match source {
        CredentialSource::Json => options.remove("google_application_credentials"),
        CredentialSource::Env => read_environment_value("GOOGLE_APPLICATION_CREDENTIALS")?,
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

fn normalize_gcs_options(
    options: DeltaStorageOptions,
    source: CredentialSource,
) -> Result<DeltaStorageOptions, Error> {
    let mut normalized = DeltaStorageOptions::new();
    for (name, value) in options {
        let key = name
            .to_ascii_lowercase()
            .parse::<GoogleConfigKey>()
            .map_err(|_| Error::StorageAuthorization {
                reason: "auth_source_unsupported_option",
            })?;
        match key {
            GoogleConfigKey::ApplicationCredentials => {
                ensure!(
                    matches!(source, CredentialSource::Json),
                    StorageAuthorizationSnafu {
                        reason: "auth_source_conflicting_credentials"
                    }
                );
                validate_auth_source_value(&value)?;
            }
            GoogleConfigKey::BaseUrl | GoogleConfigKey::Client(_) => {}
            _ => {
                return StorageAuthorizationSnafu {
                    reason: "auth_source_unsupported_option",
                }
                .fail();
            }
        }
        ensure!(
            normalized.insert(key.as_ref().to_owned(), value).is_none(),
            StorageAuthorizationSnafu {
                reason: "auth_source_duplicate_option"
            }
        );
    }
    Ok(normalized)
}
