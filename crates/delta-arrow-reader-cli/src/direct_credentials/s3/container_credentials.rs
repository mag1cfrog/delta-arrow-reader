//! ECS task and EKS Pod Identity credential configuration.

use delta_arrow_reader::DeltaStorageOptions;
use object_store::aws::AmazonS3ConfigKey;
use snafu::ensure;
use url::Url;

use crate::{Error, StorageAuthorizationSnafu, direct_credentials::read_environment_value};

const CONTAINER_REFERENCE_KEYS: [AmazonS3ConfigKey; 3] = [
    AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
    AmazonS3ConfigKey::ContainerCredentialsFullUri,
    AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
];

pub(super) fn load_environment_options(options: &mut DeltaStorageOptions) -> Result<(), Error> {
    // JSON selects the entire reference; never pair its endpoint with an ambient token file.
    if !CONTAINER_REFERENCE_KEYS
        .iter()
        .any(|key| options.contains_key(key.as_ref()))
    {
        for key in CONTAINER_REFERENCE_KEYS {
            if let Some(value) = read_environment_value(&key.as_ref().to_ascii_uppercase())? {
                options.insert(key.as_ref().to_owned(), value);
            }
        }
    }
    Ok(())
}

pub(super) fn validate_credential_family(options: &DeltaStorageOptions) -> Result<(), Error> {
    let relative_uri = options.get(AmazonS3ConfigKey::ContainerCredentialsRelativeUri.as_ref());
    let full_uri = options.get(AmazonS3ConfigKey::ContainerCredentialsFullUri.as_ref());
    let token_file = options.get(AmazonS3ConfigKey::ContainerAuthorizationTokenFile.as_ref());
    ensure!(
        relative_uri.is_none() || (full_uri.is_none() && token_file.is_none()),
        StorageAuthorizationSnafu {
            reason: "auth_source_conflicting_credentials"
        }
    );
    let endpoint = if let Some(uri) = relative_uri {
        // The native provider appends this value to the ECS host without inserting a slash.
        ensure!(
            uri.starts_with('/') && !uri.starts_with("//") && !uri.contains('\\'),
            StorageAuthorizationSnafu {
                reason: "auth_source_invalid_value"
            }
        );
        format!("http://169.254.170.2{uri}")
    } else {
        let (Some(uri), Some(_)) = (full_uri, token_file) else {
            return StorageAuthorizationSnafu {
                reason: "auth_source_incomplete_credentials",
            }
            .fail();
        };
        uri.clone()
    };
    let url = Url::parse(&endpoint).map_err(|_| Error::StorageAuthorization {
        reason: "auth_source_invalid_value",
    })?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.has_host()
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none(),
        StorageAuthorizationSnafu {
            reason: "auth_source_invalid_value"
        }
    );
    Ok(())
}
