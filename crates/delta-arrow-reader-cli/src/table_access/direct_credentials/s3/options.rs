//! Allowed S3 options for each explicit credential source.

use object_store::aws::AmazonS3ConfigKey;
use snafu::ensure;

use crate::{
    diagnostics::{Error, StorageAuthorizationSnafu},
    table_access::direct_credentials::{CredentialSource, validate_auth_source_value},
};

pub(in crate::table_access::direct_credentials) fn validate_storage_option(
    key: &AmazonS3ConfigKey,
    value: &str,
    source: CredentialSource,
) -> Result<(), Error> {
    match key {
        AmazonS3ConfigKey::AccessKeyId
        | AmazonS3ConfigKey::SecretAccessKey
        | AmazonS3ConfigKey::Token => {
            ensure!(
                matches!(source, CredentialSource::Json),
                StorageAuthorizationSnafu {
                    reason: "auth_source_conflicting_credentials"
                }
            );
            validate_auth_source_value(value)?;
        }
        AmazonS3ConfigKey::RoleArn
        | AmazonS3ConfigKey::WebIdentityTokenFile
        | AmazonS3ConfigKey::RoleSessionName
        | AmazonS3ConfigKey::StsEndpoint => {
            ensure!(
                matches!(source, CredentialSource::AwsWebIdentity),
                StorageAuthorizationSnafu {
                    reason: "auth_source_unsupported_option"
                }
            );
            validate_auth_source_value(value)?;
        }
        AmazonS3ConfigKey::ContainerCredentialsRelativeUri
        | AmazonS3ConfigKey::ContainerCredentialsFullUri
        | AmazonS3ConfigKey::ContainerAuthorizationTokenFile => {
            ensure!(
                matches!(source, CredentialSource::AwsContainer),
                StorageAuthorizationSnafu {
                    reason: "auth_source_unsupported_option"
                }
            );
            validate_auth_source_value(value)?;
        }
        AmazonS3ConfigKey::Region | AmazonS3ConfigKey::DefaultRegion => {
            // The native signer embeds the region in an HTTP header without validation.
            validate_auth_source_value(value)?;
        }
        AmazonS3ConfigKey::Endpoint
        | AmazonS3ConfigKey::S3Endpoint
        | AmazonS3ConfigKey::VirtualHostedStyleRequest
        | AmazonS3ConfigKey::UnsignedPayload
        | AmazonS3ConfigKey::Checksum
        | AmazonS3ConfigKey::RequestPayer
        | AmazonS3ConfigKey::Client(_) => {}
        _ => {
            return StorageAuthorizationSnafu {
                reason: "auth_source_unsupported_option",
            }
            .fail();
        }
    }
    Ok(())
}
