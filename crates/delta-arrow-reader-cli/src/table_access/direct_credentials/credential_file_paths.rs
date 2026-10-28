//! Credential references in env files and manifest-selected JSON use their containing directory.

use std::path::Path;

use delta_arrow_reader::DeltaStorageOptions;
use object_store::{
    ObjectStoreScheme, aws::AmazonS3ConfigKey, azure::AzureConfigKey, gcp::GoogleConfigKey,
};

use crate::diagnostics::Error;

/// Resolves only native file-reference options, before providers add inherited environment values.
pub(super) fn resolve_relative_paths(
    options: &mut DeltaStorageOptions,
    scheme: &ObjectStoreScheme,
    directory: &Path,
) -> Result<(), Error> {
    for (name, value) in options.iter_mut() {
        let key = name.to_ascii_lowercase();
        let is_file_reference = match scheme {
            ObjectStoreScheme::AmazonS3 => matches!(
                key.parse::<AmazonS3ConfigKey>(),
                Ok(AmazonS3ConfigKey::WebIdentityTokenFile
                    | AmazonS3ConfigKey::ContainerAuthorizationTokenFile)
            ),
            ObjectStoreScheme::MicrosoftAzure => matches!(
                key.parse::<AzureConfigKey>(),
                Ok(AzureConfigKey::FederatedTokenFile)
            ),
            ObjectStoreScheme::GoogleCloudStorage => matches!(
                key.parse::<GoogleConfigKey>(),
                Ok(GoogleConfigKey::ApplicationCredentials)
            ),
            _ => false,
        };
        if is_file_reference {
            // Validate before joining: an empty reference must not turn into a valid directory path.
            super::validate_auth_source_value(value)?;
            *value = directory
                .join(value.as_str())
                .into_os_string()
                .into_string()
                .map_err(|_| Error::StorageAuthorization {
                    reason: "auth_source_invalid_value",
                })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
