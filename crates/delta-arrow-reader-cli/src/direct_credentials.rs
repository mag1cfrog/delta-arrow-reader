//! Explicit credential selection for direct cloud reads.

use std::{env, sync::Arc};

use clap::ValueEnum;
use delta_arrow_reader::{DeltaStorageCredentialProvider, DeltaStorageOptions, DeltaTableBuilder};
use object_store::{
    ObjectStoreScheme,
    aws::{AmazonS3ConfigKey, AwsCredential},
    client::StaticCredentialProvider,
};
use snafu::{OptionExt, ensure};
use url::Url;

use crate::{Error, StorageAuthorizationSnafu};

/// Selects one source; missing or rejected credentials never select another identity.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum CredentialSource {
    /// Read credentials from the explicitly supplied storage-options file.
    Json,
    /// Read the advertised AWS credential variables from the inherited environment.
    Env,
}

impl CredentialSource {
    /// Pins S3 credentials in the native provider and retains noncredential storage options.
    pub(crate) fn configure_table_credentials(
        self,
        builder: DeltaTableBuilder,
        location: &str,
        options: DeltaStorageOptions,
    ) -> Result<DeltaTableBuilder, Error> {
        let url = Url::parse(location).map_err(|_| Error::StorageAuthorization {
            reason: "auth_source_requires_s3",
        })?;
        ensure!(
            ObjectStoreScheme::parse(&url)
                .is_ok_and(|(scheme, _)| scheme == ObjectStoreScheme::AmazonS3),
            StorageAuthorizationSnafu {
                reason: "auth_source_requires_s3"
            }
        );
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            StorageAuthorizationSnafu {
                reason: "auth_source_invalid_table_url"
            }
        );
        let mut options = normalize_s3_options(options, self)?;
        if matches!(self, Self::Env) {
            // Read only the advertised variables. Provider and profile variables are not selected.
            for name in [
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
            ] {
                if let Some(value) = read_environment_value(name)? {
                    options.insert(name.to_ascii_lowercase(), value);
                }
            }
            if !options.contains_key("aws_region") && !options.contains_key("aws_default_region") {
                for name in ["AWS_REGION", "AWS_DEFAULT_REGION"] {
                    if let Some(value) = read_environment_value(name)? {
                        options.insert(name.to_ascii_lowercase(), value);
                    }
                }
            }
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
}

fn normalize_s3_options(
    options: DeltaStorageOptions,
    source: CredentialSource,
) -> Result<DeltaStorageOptions, Error> {
    let mut normalized = DeltaStorageOptions::new();
    for (name, value) in options {
        let key = name
            .to_ascii_lowercase()
            .parse::<AmazonS3ConfigKey>()
            .map_err(|_| Error::StorageAuthorization {
                reason: "auth_source_unsupported_option",
            })?;
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
                validate_auth_source_value(&value)?;
            }
            AmazonS3ConfigKey::Region
            | AmazonS3ConfigKey::DefaultRegion
            | AmazonS3ConfigKey::Endpoint
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
        // Native parsing recognizes aliases; the canonical key makes duplicate aliases unambiguous.
        ensure!(
            normalized.insert(key.as_ref().to_owned(), value).is_none(),
            StorageAuthorizationSnafu {
                reason: "auth_source_duplicate_option"
            }
        );
    }
    Ok(normalized)
}

fn take_required_credential(options: &mut DeltaStorageOptions, key: &str) -> Result<String, Error> {
    options.remove(key).context(StorageAuthorizationSnafu {
        reason: "auth_source_incomplete_credentials",
    })
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
