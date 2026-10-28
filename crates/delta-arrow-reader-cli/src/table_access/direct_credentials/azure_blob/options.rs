//! Accepted Azure options and required credential families for each selected source.

use delta_arrow_reader::DeltaStorageOptions;
use object_store::azure::AzureConfigKey;
use snafu::ensure;

use super::MANAGED_IDENTITY_KEYS;
use crate::{
    diagnostics::{Error, StorageAuthorizationSnafu},
    table_access::direct_credentials::{CredentialSource, validate_auth_source_value},
};

pub(super) fn validate_credential_family(
    options: &DeltaStorageOptions,
    source: CredentialSource,
) -> Result<(), Error> {
    if matches!(source, CredentialSource::AzureManagedIdentity) {
        let identity_count = MANAGED_IDENTITY_KEYS
            .iter()
            .filter(|key| options.contains_key(key.as_ref()))
            .count();
        ensure!(
            identity_count > 0,
            StorageAuthorizationSnafu {
                reason: "auth_source_incomplete_credentials"
            }
        );
        ensure!(
            identity_count == 1,
            StorageAuthorizationSnafu {
                reason: "auth_source_conflicting_credentials"
            }
        );
        return Ok(());
    }
    if matches!(source, CredentialSource::AzureWorkloadIdentity) {
        ensure!(
            [
                AzureConfigKey::ClientId,
                AzureConfigKey::AuthorityId,
                AzureConfigKey::FederatedTokenFile,
            ]
            .iter()
            .all(|key| options.contains_key(key.as_ref())),
            StorageAuthorizationSnafu {
                reason: "auth_source_incomplete_credentials"
            }
        );
        return Ok(());
    }
    let static_credential_keys = [
        AzureConfigKey::AccessKey,
        AzureConfigKey::SasKey,
        AzureConfigKey::Token,
    ];
    let client_secret_keys = [
        AzureConfigKey::ClientId,
        AzureConfigKey::ClientSecret,
        AzureConfigKey::AuthorityId,
    ];
    let static_credential_count = static_credential_keys
        .iter()
        .filter(|key| options.contains_key(key.as_ref()))
        .count();
    let client_secret_field_count = client_secret_keys
        .iter()
        .filter(|key| options.contains_key(key.as_ref()))
        .count();
    // Even an incomplete client-secret family conflicts with a static credential.
    let credential_family_count =
        static_credential_count + usize::from(client_secret_field_count > 0);
    ensure!(
        credential_family_count <= 1,
        StorageAuthorizationSnafu {
            reason: "auth_source_conflicting_credentials"
        }
    );
    ensure!(
        static_credential_count == 1 || client_secret_field_count == client_secret_keys.len(),
        StorageAuthorizationSnafu {
            reason: "auth_source_incomplete_credentials"
        }
    );
    if let Some(token) = options.get("azure_storage_token") {
        ensure!(
            token.bytes().all(|byte| byte.is_ascii_graphic()),
            StorageAuthorizationSnafu {
                reason: "auth_source_invalid_value"
            }
        );
    }

    Ok(())
}

pub(in crate::table_access::direct_credentials) fn validate_storage_option(
    key: &AzureConfigKey,
    value: &str,
    source: CredentialSource,
) -> Result<(), Error> {
    match key {
        AzureConfigKey::AccessKey
        | AzureConfigKey::SasKey
        | AzureConfigKey::Token
        | AzureConfigKey::ClientSecret => {
            ensure!(
                matches!(source, CredentialSource::Json),
                StorageAuthorizationSnafu {
                    reason: "auth_source_conflicting_credentials"
                }
            );
            validate_auth_source_value(value)?;
        }
        AzureConfigKey::ClientId => {
            ensure!(
                !matches!(source, CredentialSource::Env),
                StorageAuthorizationSnafu {
                    reason: "auth_source_conflicting_credentials"
                }
            );
            validate_auth_source_value(value)?;
        }
        AzureConfigKey::AuthorityId => {
            ensure!(
                matches!(
                    source,
                    CredentialSource::Json | CredentialSource::AzureWorkloadIdentity
                ),
                StorageAuthorizationSnafu {
                    reason: "auth_source_conflicting_credentials"
                }
            );
            validate_auth_source_value(value)?;
        }
        AzureConfigKey::ObjectId | AzureConfigKey::MsiResourceId | AzureConfigKey::MsiEndpoint => {
            ensure!(
                matches!(source, CredentialSource::AzureManagedIdentity),
                StorageAuthorizationSnafu {
                    reason: "auth_source_unsupported_option"
                }
            );
            validate_auth_source_value(value)?;
        }
        AzureConfigKey::FederatedTokenFile => {
            ensure!(
                matches!(source, CredentialSource::AzureWorkloadIdentity),
                StorageAuthorizationSnafu {
                    reason: "auth_source_unsupported_option"
                }
            );
            validate_auth_source_value(value)?;
        }
        AzureConfigKey::AuthorityHost => {
            ensure!(
                !matches!(source, CredentialSource::AzureManagedIdentity),
                StorageAuthorizationSnafu {
                    reason: "auth_source_unsupported_option"
                }
            );
            validate_auth_source_value(value)?;
        }
        AzureConfigKey::AccountName => {
            validate_auth_source_value(value)?;
        }
        AzureConfigKey::Endpoint
        | AzureConfigKey::UseFabricEndpoint
        | AzureConfigKey::Client(_) => {}
        _ => {
            return StorageAuthorizationSnafu {
                reason: "auth_source_unsupported_option",
            }
            .fail();
        }
    }
    Ok(())
}
