//! Explicit credential selection for direct cloud reads.

mod azure;
mod gcs;
mod s3;

use std::env;

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
}

impl CredentialSource {
    /// Pins the selected credentials and retains noncredential storage options.
    pub(crate) fn configure_table_credentials(
        self,
        builder: DeltaTableBuilder,
        location: &str,
        options: DeltaStorageOptions,
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
        // Azure URLs may use container@account; passwords and embedded tokens remain invalid.
        ensure!(
            (url.username().is_empty()
                || (scheme == ObjectStoreScheme::MicrosoftAzure
                    && matches!(url.scheme(), "az" | "abfs" | "abfss")))
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            StorageAuthorizationSnafu {
                reason: "auth_source_invalid_table_url"
            }
        );
        match scheme {
            ObjectStoreScheme::AmazonS3 => s3::configure_table_credentials(builder, options, self),
            ObjectStoreScheme::MicrosoftAzure => {
                azure::configure_table_credentials(builder, location, options, self)
            }
            ObjectStoreScheme::GoogleCloudStorage => {
                gcs::configure_table_credentials(builder, location, options, self)
            }
            _ => Err(unsupported_storage()),
        }
    }
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
