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
    /// Rejects a mismatched store or competing authentication settings before reads.
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

        // Presence alone conflicts, even when an authentication flag is set to false.
        ensure!(
            !storage_options
                .keys()
                .any(|option_name| self.is_authentication_option(option_name)),
            InvalidConfigurationSnafu {
                reason: "credential_provider_conflicting_options",
            }
        );
        Ok(())
    }

    fn is_authentication_option(&self, option_name: &str) -> bool {
        // Native parsers keep aliases and ignored unknown keys consistent with store construction.
        let normalized_name = option_name.to_ascii_lowercase();
        match self {
            Self::Aws(_) => normalized_name
                .parse::<AmazonS3ConfigKey>()
                .is_ok_and(|option| AWS_AUTHENTICATION_OPTIONS.contains(&option)),
            Self::Azure(_) => normalized_name
                .parse::<AzureConfigKey>()
                .is_ok_and(|option| AZURE_AUTHENTICATION_OPTIONS.contains(&option)),
            Self::Gcp(_) => normalized_name
                .parse::<GoogleConfigKey>()
                .is_ok_and(|option| GCP_AUTHENTICATION_OPTIONS.contains(&option)),
        }
    }
}

const AWS_AUTHENTICATION_OPTIONS: &[AmazonS3ConfigKey] = &[
    // These values would supply a second credential set.
    AmazonS3ConfigKey::AccessKeyId,
    AmazonS3ConfigKey::SecretAccessKey,
    AmazonS3ConfigKey::Token,
    // These settings configure the SDK's instance, container, or web-identity providers.
    AmazonS3ConfigKey::ImdsV1Fallback,
    AmazonS3ConfigKey::MetadataEndpoint,
    AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
    AmazonS3ConfigKey::ContainerCredentialsFullUri,
    AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
    AmazonS3ConfigKey::WebIdentityTokenFile,
    AmazonS3ConfigKey::RoleArn,
    AmazonS3ConfigKey::RoleSessionName,
    AmazonS3ConfigKey::StsEndpoint,
    // Unsigned requests bypass the supplied provider.
    AmazonS3ConfigKey::SkipSignature,
];

const AZURE_AUTHENTICATION_OPTIONS: &[AzureConfigKey] = &[
    // These values would supply a second credential set.
    AzureConfigKey::AccessKey,
    AzureConfigKey::SasKey,
    AzureConfigKey::Token,
    // These settings configure service-principal, managed-identity, or CLI authentication.
    AzureConfigKey::ClientId,
    AzureConfigKey::ClientSecret,
    AzureConfigKey::AuthorityId,
    AzureConfigKey::AuthorityHost,
    AzureConfigKey::MsiEndpoint,
    AzureConfigKey::ObjectId,
    AzureConfigKey::MsiResourceId,
    AzureConfigKey::FederatedTokenFile,
    AzureConfigKey::UseAzureCli,
    // Fabric's token service would select another credential provider.
    AzureConfigKey::FabricTokenServiceUrl,
    AzureConfigKey::FabricWorkloadHost,
    AzureConfigKey::FabricSessionToken,
    AzureConfigKey::FabricClusterIdentifier,
    // Emulator mode and unsigned requests bypass the supplied provider.
    AzureConfigKey::UseEmulator,
    AzureConfigKey::SkipSignature,
];

const GCP_AUTHENTICATION_OPTIONS: &[GoogleConfigKey] = &[
    // Credential files or inline service-account keys would supply another identity.
    GoogleConfigKey::ServiceAccount,
    GoogleConfigKey::ServiceAccountKey,
    GoogleConfigKey::ApplicationCredentials,
    // Unsigned requests bypass the supplied provider.
    GoogleConfigKey::SkipSignature,
];
