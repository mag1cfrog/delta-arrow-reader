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
    pub(crate) fn validate(
        &self,
        table_url: &Url,
        options: &DeltaStorageOptions,
    ) -> Result<(), DeltaReaderError> {
        let scheme = ObjectStoreScheme::parse(table_url).map(|(scheme, _)| scheme);
        ensure!(
            matches!(
                (self, scheme),
                (Self::Aws(_), Ok(ObjectStoreScheme::AmazonS3))
                    | (Self::Azure(_), Ok(ObjectStoreScheme::MicrosoftAzure))
                    | (Self::Gcp(_), Ok(ObjectStoreScheme::GoogleCloudStorage))
            ),
            InvalidConfigurationSnafu {
                reason: "credential_provider_store_mismatch",
            }
        );
        ensure!(
            !options.keys().any(|key| self.conflicts_with_option(key)),
            InvalidConfigurationSnafu {
                reason: "credential_provider_conflicting_options",
            }
        );
        Ok(())
    }

    fn conflicts_with_option(&self, key: &str) -> bool {
        let key = key.to_ascii_lowercase();
        match self {
            Self::Aws(_) => matches!(
                key.parse::<AmazonS3ConfigKey>(),
                Ok(AmazonS3ConfigKey::AccessKeyId
                    | AmazonS3ConfigKey::SecretAccessKey
                    | AmazonS3ConfigKey::Token
                    | AmazonS3ConfigKey::ImdsV1Fallback
                    | AmazonS3ConfigKey::MetadataEndpoint
                    | AmazonS3ConfigKey::ContainerCredentialsRelativeUri
                    | AmazonS3ConfigKey::ContainerCredentialsFullUri
                    | AmazonS3ConfigKey::ContainerAuthorizationTokenFile
                    | AmazonS3ConfigKey::WebIdentityTokenFile
                    | AmazonS3ConfigKey::RoleArn
                    | AmazonS3ConfigKey::RoleSessionName
                    | AmazonS3ConfigKey::StsEndpoint
                    | AmazonS3ConfigKey::SkipSignature)
            ),
            Self::Azure(_) => matches!(
                key.parse::<AzureConfigKey>(),
                Ok(AzureConfigKey::AccessKey
                    | AzureConfigKey::ClientId
                    | AzureConfigKey::ClientSecret
                    | AzureConfigKey::AuthorityId
                    | AzureConfigKey::AuthorityHost
                    | AzureConfigKey::SasKey
                    | AzureConfigKey::Token
                    | AzureConfigKey::UseEmulator
                    | AzureConfigKey::MsiEndpoint
                    | AzureConfigKey::ObjectId
                    | AzureConfigKey::MsiResourceId
                    | AzureConfigKey::FederatedTokenFile
                    | AzureConfigKey::UseAzureCli
                    | AzureConfigKey::SkipSignature
                    | AzureConfigKey::FabricTokenServiceUrl
                    | AzureConfigKey::FabricWorkloadHost
                    | AzureConfigKey::FabricSessionToken
                    | AzureConfigKey::FabricClusterIdentifier)
            ),
            Self::Gcp(_) => matches!(
                key.parse::<GoogleConfigKey>(),
                Ok(GoogleConfigKey::ServiceAccount
                    | GoogleConfigKey::ServiceAccountKey
                    | GoogleConfigKey::ApplicationCredentials
                    | GoogleConfigKey::SkipSignature)
            ),
        }
    }
}
