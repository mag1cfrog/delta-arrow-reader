//! Caller-owned credentials for native cloud storage clients.

use std::fmt;

use object_store::{
    ObjectStoreScheme,
    aws::{AmazonS3ConfigKey, AwsCredentialProvider},
    azure::{AzureConfigKey, AzureCredentialProvider},
    gcp::{GcpCredentialProvider, GoogleConfigKey},
};
use snafu::ensure;
use url::Url;

use crate::{DeltaReaderError, DeltaStorageOptions, error::InvalidConfigurationSnafu};

/// Supplies credentials to the native storage client throughout a table's lifetime.
///
/// The provider owns caching and renewal. Its `get_credential` method may run
/// concurrently on the caller's runtime or Delta Kernel's runtime. It must return
/// usable credentials or an error; the reader never falls back to another identity.
/// Clones, scans, and refreshed tables retain the same provider.
///
/// See [`crate::DeltaTableBuilder::with_credential_provider`] for configuration
/// rules and an example.
#[derive(Clone)]
#[non_exhaustive]
pub enum DeltaStorageCredentialProvider {
    /// AWS keys and optional session tokens, for S3 or Cloudflare R2.
    Aws(AwsCredentialProvider),
    /// Azure credentials, including renewable SAS or bearer tokens.
    Azure(AzureCredentialProvider),
    /// GCS OAuth bearer tokens.
    Gcp(GcpCredentialProvider),
}

impl fmt::Debug for DeltaStorageCredentialProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A caller's provider Debug implementation may contain credentials.
        formatter.write_str(match self {
            Self::Aws(_) => "Aws(<redacted>)",
            Self::Azure(_) => "Azure(<redacted>)",
            Self::Gcp(_) => "Gcp(<redacted>)",
        })
    }
}

impl DeltaStorageCredentialProvider {
    /// Rejects a mismatched store or options incompatible with the provider before reads.
    pub(crate) fn validate_storage_configuration(
        &self,
        table_url: &Url,
        storage_options: &DeltaStorageOptions,
    ) -> Result<(), DeltaReaderError> {
        let expected_store = match self {
            Self::Aws(_) => ObjectStoreScheme::AmazonS3,
            Self::Azure(_) => ObjectStoreScheme::MicrosoftAzure,
            Self::Gcp(_) => ObjectStoreScheme::GoogleCloudStorage,
        };
        // Native parsing also recognizes HTTPS storage URLs, including R2 and Azure.
        let store_matches_provider = ObjectStoreScheme::parse(table_url)
            .is_ok_and(|(table_store, _)| table_store == expected_store);
        ensure!(
            store_matches_provider,
            InvalidConfigurationSnafu {
                reason: "credential_provider_store_mismatch",
            }
        );

        // Native with_credentials handles ordinary credential precedence.
        ensure!(
            !storage_options
                .keys()
                .any(|option_name| self.is_incompatible_option(option_name)),
            InvalidConfigurationSnafu {
                reason: "credential_provider_conflicting_options",
            }
        );
        Ok(())
    }

    fn is_incompatible_option(&self, option_name: &str) -> bool {
        // Native parsers keep aliases and ignored unknown keys consistent with store construction.
        let normalized_name = option_name.to_ascii_lowercase();
        match self {
            // Unsigned requests bypass the provider.
            Self::Aws(_) => matches!(
                normalized_name.parse(),
                Ok(AmazonS3ConfigKey::SkipSignature)
            ),
            // Emulator mode also bypasses the provider.
            Self::Azure(_) => matches!(
                normalized_name.parse(),
                Ok(AzureConfigKey::SkipSignature | AzureConfigKey::UseEmulator)
            ),
            // GCS parses service-account data before selecting the provider and can use its endpoint.
            Self::Gcp(_) => matches!(
                normalized_name.parse(),
                Ok(GoogleConfigKey::SkipSignature
                    | GoogleConfigKey::ServiceAccount
                    | GoogleConfigKey::ServiceAccountKey)
            ),
        }
    }
}
