//! Explicit S3 access keys and identity providers without ambient fallback.

mod container_credentials;

use std::sync::Arc;

use delta_arrow_reader::{DeltaStorageCredentialProvider, DeltaStorageOptions, DeltaTableBuilder};
use object_store::{
    aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential},
    client::StaticCredentialProvider,
};
use snafu::{OptionExt, ensure};

use super::{
    CredentialSource, normalize_storage_options, read_environment_value, validate_auth_source_value,
};
use crate::{Error, StorageAuthorizationSnafu};

pub(super) const CREDENTIAL_ENVIRONMENT_VARIABLES: &[(&str, AmazonS3ConfigKey)] = &[
    ("AWS_ACCESS_KEY_ID", AmazonS3ConfigKey::AccessKeyId),
    ("AWS_SECRET_ACCESS_KEY", AmazonS3ConfigKey::SecretAccessKey),
    ("AWS_SESSION_TOKEN", AmazonS3ConfigKey::Token),
];
pub(super) const REGION_ENVIRONMENT_VARIABLES: &[(&str, AmazonS3ConfigKey)] = &[
    ("AWS_REGION", AmazonS3ConfigKey::Region),
    ("AWS_DEFAULT_REGION", AmazonS3ConfigKey::DefaultRegion),
];

pub(super) fn configure_table_credentials(
    builder: DeltaTableBuilder,
    location: &str,
    options: DeltaStorageOptions,
    source: CredentialSource,
) -> Result<DeltaTableBuilder, Error> {
    let mut options = normalize_storage_options(options, |key, value| {
        validate_storage_option(key, value, source)
    })?;
    if matches!(source, CredentialSource::Env) {
        // Read only the advertised variables. Provider and profile variables are not selected.
        for (name, key) in CREDENTIAL_ENVIRONMENT_VARIABLES {
            if let Some(value) = read_environment_value(name)? {
                options.insert(key.as_ref().to_owned(), value);
            }
        }
    } else if matches!(source, CredentialSource::AwsWebIdentity) {
        for name in [
            "AWS_ROLE_ARN",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_ROLE_SESSION_NAME",
            "AWS_ENDPOINT_URL_STS",
        ] {
            let key = name.to_ascii_lowercase();
            if !options.contains_key(&key)
                && let Some(value) = read_environment_value(name)?
            {
                options.insert(key, value);
            }
        }
    } else if matches!(source, CredentialSource::AwsContainer) {
        container_credentials::load_environment_options(&mut options)?;
    }
    if !matches!(source, CredentialSource::Json)
        && !options.contains_key("aws_region")
        && !options.contains_key("aws_default_region")
    {
        for (name, key) in REGION_ENVIRONMENT_VARIABLES {
            if let Some(value) = read_environment_value(name)? {
                options.insert(key.as_ref().to_owned(), value);
            }
        }
    }
    if matches!(source, CredentialSource::AwsWebIdentity) {
        ensure!(
            options.contains_key("aws_role_arn")
                && options.contains_key("aws_web_identity_token_file"),
            StorageAuthorizationSnafu {
                reason: "auth_source_incomplete_credentials"
            }
        );
        return configure_native_credentials(builder, location, options);
    }
    if matches!(source, CredentialSource::AwsContainer) {
        container_credentials::validate_credential_family(&options)?;
        return configure_native_credentials(builder, location, options);
    }
    let credentials = AwsCredential {
        key_id: take_required_credential(&mut options, "aws_access_key_id")?,
        secret_key: take_required_credential(&mut options, "aws_secret_access_key")?,
        token: options.remove("aws_session_token"),
    };
    // with_credentials bypasses the native provider chain, including after a failed request.
    Ok(builder
        .with_storage_options(options)
        .with_credential_provider(DeltaStorageCredentialProvider::Aws(Arc::new(
            StaticCredentialProvider::new(credentials),
        ))))
}

fn configure_native_credentials(
    builder: DeltaTableBuilder,
    location: &str,
    mut options: DeltaStorageOptions,
) -> Result<DeltaTableBuilder, Error> {
    // Source-specific validation leaves exactly one complete native provider configuration.
    let mut storage_builder = AmazonS3Builder::new().with_url(location);
    for (name, value) in &options {
        let key = name
            .parse::<AmazonS3ConfigKey>()
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
    // The provider owns its references; do not retain a second source in storage options.
    for key in [
        AmazonS3ConfigKey::RoleArn,
        AmazonS3ConfigKey::WebIdentityTokenFile,
        AmazonS3ConfigKey::RoleSessionName,
        AmazonS3ConfigKey::StsEndpoint,
        AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
        AmazonS3ConfigKey::ContainerCredentialsFullUri,
        AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
    ] {
        options.remove(key.as_ref());
    }
    Ok(builder
        .with_storage_options(options)
        .with_credential_provider(DeltaStorageCredentialProvider::Aws(Arc::clone(
            storage.credentials(),
        ))))
}

pub(super) fn validate_storage_option(
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

fn take_required_credential(options: &mut DeltaStorageOptions, key: &str) -> Result<String, Error> {
    options.remove(key).context(StorageAuthorizationSnafu {
        reason: "auth_source_incomplete_credentials",
    })
}
