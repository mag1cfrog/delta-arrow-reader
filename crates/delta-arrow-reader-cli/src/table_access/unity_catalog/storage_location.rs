//! Canonical cloud table locations and allowed transport/region options.

use delta_arrow_reader::DeltaStorageOptions;
use object_store::{
    ObjectStoreScheme, aws::AmazonS3ConfigKey, azure::AzureConfigKey, gcp::GoogleConfigKey,
};
use url::Url;

use super::Error;

/// Storage backends this adapter supports with temporary Unity Catalog credentials.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum StorageBackend {
    S3,
    R2,
    /// Azure Blob Storage, including ADLS Gen2 locations.
    AzureBlob,
    Gcs,
}

/// A validated table storage URL, normalized for comparing metadata and credential responses.
pub(super) struct TableStorageLocation {
    url: Url,
    pub(super) backend: StorageBackend,
}

impl TableStorageLocation {
    pub(super) fn as_url(&self) -> &Url {
        &self.url
    }

    /// Normalizes supported cloud URLs and adds a trailing slash to the table root.
    pub(super) fn parse(raw: &str) -> Result<Self, Error> {
        let mut url = Url::parse(raw).map_err(|_| Error::InvalidDatabricksResponse)?;
        if url.query().is_some()
            || url.fragment().is_some()
            || url.password().is_some()
            || url.port().is_some()
            || url.path().starts_with("//")
            || raw.chars().any(char::is_control)
        {
            return Err(Error::InvalidDatabricksResponse);
        }
        let is_r2_endpoint = url.host_str().is_some_and(|host| {
            host.strip_suffix(".r2.cloudflarestorage.com")
                .is_some_and(|account| !account.is_empty() && !account.contains('.'))
        });
        let backend = match url.scheme() {
            "s3" | "s3a" if url.username().is_empty() && url.host_str().is_some() => {
                url.set_scheme("s3")
                    .map_err(|_| Error::InvalidDatabricksResponse)?;
                StorageBackend::S3
            }
            "gs" if url.username().is_empty() && url.host_str().is_some() => StorageBackend::Gcs,
            "r2" if is_r2_endpoint && !url.username().is_empty() => {
                if url.username().contains('%') || matches!(url.username(), "." | "..") {
                    return Err(Error::InvalidDatabricksResponse);
                }
                // The native S3 client accepts R2 HTTPS URLs; keep the account in the canonical root.
                url = Url::parse(&format!(
                    "https://{}/{}{}",
                    url.host_str().ok_or(Error::InvalidDatabricksResponse)?,
                    url.username(),
                    url.path()
                ))
                .map_err(|_| Error::InvalidDatabricksResponse)?;
                StorageBackend::R2
            }
            "https" if is_r2_endpoint && url.username().is_empty() => {
                let bucket = url.path().split('/').nth(1).unwrap_or_default();
                if bucket.is_empty() || bucket.contains('%') {
                    return Err(Error::InvalidDatabricksResponse);
                }
                StorageBackend::R2
            }
            "abfss"
                if !url.username().is_empty()
                    && url
                        .host_str()
                        .is_some_and(|host| host.ends_with(".dfs.core.windows.net")) =>
            {
                StorageBackend::AzureBlob
            }
            "https" if url.username().is_empty() => {
                let account = url
                    .host_str()
                    .and_then(|host| {
                        host.strip_suffix(".dfs.core.windows.net")
                            .or_else(|| host.strip_suffix(".blob.core.windows.net"))
                    })
                    .filter(|account| !account.is_empty())
                    .ok_or(Error::UnsupportedDirectRead)?;
                let path = url.path().trim_start_matches('/');
                let (container, path) = path.split_once('/').unwrap_or((path, ""));
                if container.is_empty() {
                    return Err(Error::InvalidDatabricksResponse);
                }
                url = Url::parse(&format!(
                    "abfss://{container}@{account}.dfs.core.windows.net/{path}"
                ))
                .map_err(|_| Error::InvalidDatabricksResponse)?;
                StorageBackend::AzureBlob
            }
            _ => return Err(Error::UnsupportedDirectRead),
        };
        ObjectStoreScheme::parse(&url).map_err(|_| Error::InvalidDatabricksResponse)?;
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        Ok(Self { url, backend })
    }
}

/// Allows transport and region settings without letting callers replace UC credentials or identity.
///
/// Before the metadata request, `None` accepts settings for any supported backend.
/// Once the table URL is known, settings must match that backend and S3 requires a region.
pub(super) fn validate_storage_options(
    options: &DeltaStorageOptions,
    backend: Option<StorageBackend>,
) -> Result<(), Error> {
    let mut has_region = false;
    for (key, value) in options {
        let key = key.to_ascii_lowercase();
        let aws_config_key = key.parse::<AmazonS3ConfigKey>();
        let azure_config_key = key.parse::<AzureConfigKey>();
        let gcp_config_key = key.parse::<GoogleConfigKey>();
        let aws_option_allowed = matches!(
            aws_config_key,
            Ok(AmazonS3ConfigKey::Region
                | AmazonS3ConfigKey::DefaultRegion
                | AmazonS3ConfigKey::Endpoint
                | AmazonS3ConfigKey::S3Endpoint
                | AmazonS3ConfigKey::VirtualHostedStyleRequest
                | AmazonS3ConfigKey::UnsignedPayload
                | AmazonS3ConfigKey::Checksum
                | AmazonS3ConfigKey::Client(_))
        );
        let azure_option_allowed = matches!(
            azure_config_key,
            Ok(AzureConfigKey::Endpoint | AzureConfigKey::Client(_))
        );
        let gcp_option_allowed = matches!(
            gcp_config_key,
            Ok(GoogleConfigKey::BaseUrl | GoogleConfigKey::Client(_))
        );
        // An option alias recognized by multiple backends must be safe in all of them.
        if (aws_config_key.is_ok() && !aws_option_allowed)
            || (azure_config_key.is_ok() && !azure_option_allowed)
            || (gcp_config_key.is_ok() && !gcp_option_allowed)
        {
            return Err(Error::InvalidStorageOptions);
        }
        let allowed_for_backend = match backend {
            None => aws_option_allowed || azure_option_allowed || gcp_option_allowed,
            Some(StorageBackend::S3) => aws_option_allowed,
            // R2 URLs supply the endpoint; only the service-specific override takes precedence.
            Some(StorageBackend::R2) => {
                aws_option_allowed && !matches!(aws_config_key, Ok(AmazonS3ConfigKey::Endpoint))
            }
            Some(StorageBackend::AzureBlob) => azure_option_allowed,
            Some(StorageBackend::Gcs) => gcp_option_allowed,
        };
        if !allowed_for_backend {
            return Err(Error::InvalidStorageOptions);
        }
        if matches!(
            aws_config_key,
            Ok(AmazonS3ConfigKey::Region | AmazonS3ConfigKey::DefaultRegion)
        ) {
            if has_region || value.trim().is_empty() {
                return Err(Error::InvalidStorageOptions);
            }
            if backend == Some(StorageBackend::R2) && value != "auto" {
                return Err(Error::InvalidStorageOptions);
            }
            has_region = true;
        }
    }
    if backend == Some(StorageBackend::S3) && !has_region {
        return Err(Error::MissingS3Region);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
