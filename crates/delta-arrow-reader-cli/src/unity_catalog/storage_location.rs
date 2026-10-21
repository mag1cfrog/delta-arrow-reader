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
    Azure,
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
                StorageBackend::Azure
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
                StorageBackend::Azure
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
            Some(StorageBackend::Azure) => azure_option_allowed,
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
mod tests {
    use super::*;

    #[test]
    fn options_keep_transport_and_region_but_never_override_identity() {
        for key in [
            "AWS_ACCESS_KEY_ID",
            "access_key_id",
            "AWS_TOKEN",
            "token",
            "AWS_BUCKET_NAME",
            "bucket",
            "azure_storage_master_key",
            "access_key",
            "AZURE_STORAGE_SAS_TOKEN",
            "sas_key",
            "container_name",
            "account_name",
            "azure_use_azure_cli",
            "skip_signature",
            "aws_role_arn",
            "federated_token_file",
            "aws_metadata_endpoint",
            "GOOGLE_SERVICE_ACCOUNT",
            "service_account_key",
            "google_application_credentials",
            "google_bucket",
            "google_skip_signature",
            "unknown_option",
        ] {
            let options = DeltaStorageOptions::from([(key.into(), "secret-sentinel".into())]);
            assert_eq!(
                validate_storage_options(&options, None),
                Err(Error::InvalidStorageOptions),
                "{key}"
            );
        }
        let mut options = DeltaStorageOptions::from([
            ("AWS_REGION".into(), "eu-west-1".into()),
            ("aws_allow_http".into(), "true".into()),
            ("aws_endpoint".into(), "http://127.0.0.1:1234".into()),
        ]);
        validate_storage_options(&options, Some(StorageBackend::S3)).unwrap();
        options.insert("default_region".into(), "us-east-1".into());
        assert_eq!(
            validate_storage_options(&options, Some(StorageBackend::S3)),
            Err(Error::InvalidStorageOptions)
        );
        assert_eq!(
            validate_storage_options(&DeltaStorageOptions::new(), Some(StorageBackend::S3)),
            Err(Error::MissingS3Region)
        );
        validate_storage_options(&DeltaStorageOptions::new(), Some(StorageBackend::Azure)).unwrap();
        let gcs_options = DeltaStorageOptions::from([
            ("GOOGLE_BASE_URL".into(), "http://127.0.0.1:1234".into()),
            ("google_allow_http".into(), "true".into()),
        ]);
        validate_storage_options(&gcs_options, None).unwrap();
        validate_storage_options(&gcs_options, Some(StorageBackend::Gcs)).unwrap();
        assert_eq!(
            validate_storage_options(&gcs_options, Some(StorageBackend::S3)),
            Err(Error::InvalidStorageOptions)
        );
        assert_eq!(
            validate_storage_options(
                &DeltaStorageOptions::from([("aws_region".into(), "eu-west-1".into())]),
                Some(StorageBackend::Gcs),
            ),
            Err(Error::InvalidStorageOptions)
        );
    }

    #[test]
    fn azure_container_root_locations_are_equivalent() {
        let expected = "abfss://container@account.dfs.core.windows.net/";
        for location in [
            "https://account.blob.core.windows.net/container",
            "https://account.blob.core.windows.net/container/",
            "https://account.dfs.core.windows.net/container",
            "https://account.dfs.core.windows.net/container/",
            expected,
        ] {
            assert_eq!(
                TableStorageLocation::parse(location).unwrap().url.as_str(),
                expected
            );
        }
        for location in [
            "https://account.blob.core.windows.net",
            "https://account.dfs.core.windows.net/",
        ] {
            assert!(TableStorageLocation::parse(location).is_err(), "{location}");
        }
    }

    #[test]
    fn r2_locations_preserve_account_bucket_and_encoded_path() {
        for (location, expected) in [
            (
                "r2://bucket@account.r2.cloudflarestorage.com",
                "https://account.r2.cloudflarestorage.com/bucket/",
            ),
            (
                "r2://bucket@account.r2.cloudflarestorage.com/",
                "https://account.r2.cloudflarestorage.com/bucket/",
            ),
            (
                "https://account.r2.cloudflarestorage.com/bucket",
                "https://account.r2.cloudflarestorage.com/bucket/",
            ),
            (
                "r2://bucket@account.r2.cloudflarestorage.com/a%20b/%252F",
                "https://account.r2.cloudflarestorage.com/bucket/a%20b/%252F/",
            ),
            (
                "https://account.r2.cloudflarestorage.com/bucket/a%20b/%252F/",
                "https://account.r2.cloudflarestorage.com/bucket/a%20b/%252F/",
            ),
        ] {
            let parsed = TableStorageLocation::parse(location).unwrap();
            assert_eq!(parsed.url.as_str(), expected);
            assert!(parsed.backend == StorageBackend::R2);
        }
        for location in [
            "r2://account.r2.cloudflarestorage.com/bucket/table",
            "r2://bucket@unrecognized.example/table",
            "r2://bucket@account.r2.cloudflarestorage.com.example/table",
            "r2://bucket@sub.account.r2.cloudflarestorage.com/table",
            "r2://bucket@account.r2.cloudflarestorage.com:1234/table",
            "r2://bucket:secret@account.r2.cloudflarestorage.com/table",
            "r2://buck%65t@account.r2.cloudflarestorage.com/table",
            "r2://..@account.r2.cloudflarestorage.com/table",
            "https://account.r2.cloudflarestorage.com/",
            "https://account.r2.cloudflarestorage.com/buck%2Fet/table",
            "https://account.r2.cloudflarestorage.com/bucket/table?secret",
            "https://account.r2.cloudflarestorage.com/bucket/table#fragment",
            "http://account.r2.cloudflarestorage.com/bucket/table",
        ] {
            assert!(TableStorageLocation::parse(location).is_err(), "{location}");
        }
    }

    #[test]
    fn r2_options_use_auto_region_and_the_service_endpoint_override() {
        validate_storage_options(&DeltaStorageOptions::new(), Some(StorageBackend::R2)).unwrap();
        let options = DeltaStorageOptions::from([
            ("AWS_DEFAULT_REGION".into(), "auto".into()),
            ("aws_endpoint_url_s3".into(), "http://127.0.0.1:1234".into()),
            ("aws_allow_http".into(), "true".into()),
        ]);
        validate_storage_options(&options, Some(StorageBackend::R2)).unwrap();
        for (key, value) in [
            ("region", "eu-west-1"),
            ("default_region", "us-east-1"),
            ("aws_endpoint", "http://127.0.0.1:1234"),
            ("endpoint_url", "http://127.0.0.1:1234"),
            ("azure_endpoint", "http://127.0.0.1:1234"),
            ("aws_bucket", "other"),
            ("aws_session_token", "secret"),
        ] {
            let options = DeltaStorageOptions::from([(key.into(), value.into())]);
            assert_eq!(
                validate_storage_options(&options, Some(StorageBackend::R2)),
                Err(Error::InvalidStorageOptions),
                "{key}"
            );
        }
    }
}
