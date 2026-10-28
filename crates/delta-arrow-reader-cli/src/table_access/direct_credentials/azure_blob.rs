//! Explicit credentials and identity providers for Azure Blob Storage without ambient fallback.

use std::sync::Arc;

use delta_arrow_reader::{DeltaStorageCredentialProvider, DeltaStorageOptions, DeltaTableBuilder};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use snafu::{OptionExt, ensure};

use super::super::azure_sas;
use super::{
    CredentialSource, normalize_storage_options, read_environment_value, validate_auth_source_value,
};
use crate::diagnostics::{Error, StorageAuthorizationSnafu};

pub(super) const CREDENTIAL_ENVIRONMENT_VARIABLES: &[(&str, AzureConfigKey)] = &[
    ("AZURE_STORAGE_ACCOUNT_KEY", AzureConfigKey::AccessKey),
    ("AZURE_STORAGE_SAS_KEY", AzureConfigKey::SasKey),
    ("AZURE_STORAGE_TOKEN", AzureConfigKey::Token),
    ("AZURE_CLIENT_ID", AzureConfigKey::ClientId),
    ("AZURE_CLIENT_SECRET", AzureConfigKey::ClientSecret),
    ("AZURE_TENANT_ID", AzureConfigKey::AuthorityId),
];
pub(super) const ACCOUNT_NAME_ENVIRONMENT_VARIABLE: &str = "AZURE_STORAGE_ACCOUNT_NAME";

const MANAGED_IDENTITY_KEYS: [AzureConfigKey; 3] = [
    AzureConfigKey::ClientId,
    AzureConfigKey::ObjectId,
    AzureConfigKey::MsiResourceId,
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
    load_environment_options(&mut options, source)?;
    validate_credential_family(&options, source)?;

    // The native builder validates account keys and URLs. Exactly one selected family can win.
    let mut storage_builder = MicrosoftAzureBuilder::new().with_url(location);
    for (name, value) in &options {
        let key = name
            .parse::<AzureConfigKey>()
            .map_err(|_| Error::StorageAuthorization {
                reason: "auth_source_unsupported_option",
            })?;
        storage_builder = if key == AzureConfigKey::SasKey {
            let pairs = azure_sas::parse_query_pairs(value).context(StorageAuthorizationSnafu {
                reason: "auth_source_invalid_value",
            })?;
            storage_builder.with_sas_authorization(pairs)
        } else {
            storage_builder.with_config(key, value)
        };
    }
    let storage = storage_builder
        .build()
        .map_err(|_| Error::StorageAuthorization {
            reason: "auth_source_invalid_value",
        })?;
    // Keep the native provider for every read, without retaining a second credential source.
    for key in [
        AzureConfigKey::AccessKey,
        AzureConfigKey::SasKey,
        AzureConfigKey::Token,
        AzureConfigKey::ClientId,
        AzureConfigKey::ClientSecret,
        AzureConfigKey::AuthorityId,
        AzureConfigKey::FederatedTokenFile,
        AzureConfigKey::ObjectId,
        AzureConfigKey::MsiResourceId,
        AzureConfigKey::MsiEndpoint,
    ] {
        options.remove(key.as_ref());
    }
    Ok(builder
        .with_storage_options(options)
        .with_credential_provider(DeltaStorageCredentialProvider::Azure(Arc::clone(
            storage.credentials(),
        ))))
}

fn load_environment_options(
    options: &mut DeltaStorageOptions,
    source: CredentialSource,
) -> Result<(), Error> {
    if matches!(source, CredentialSource::Json) {
        return Ok(());
    }
    if matches!(source, CredentialSource::AzureManagedIdentity) {
        load_managed_identity_environment(options)?;
    } else if matches!(source, CredentialSource::AzureWorkloadIdentity) {
        for (name, key) in [
            ("AZURE_CLIENT_ID", AzureConfigKey::ClientId),
            ("AZURE_TENANT_ID", AzureConfigKey::AuthorityId),
            (
                "AZURE_FEDERATED_TOKEN_FILE",
                AzureConfigKey::FederatedTokenFile,
            ),
            ("AZURE_AUTHORITY_HOST", AzureConfigKey::AuthorityHost),
        ] {
            // Provider references in JSON take precedence over injected environment settings.
            if !options.contains_key(key.as_ref())
                && let Some(value) = read_environment_value(name)?
            {
                options.insert(key.as_ref().to_owned(), value);
            }
        }
    } else {
        for (name, key) in CREDENTIAL_ENVIRONMENT_VARIABLES {
            if let Some(value) = read_environment_value(name)? {
                options.insert(key.as_ref().to_owned(), value);
            }
        }
    }
    if !options.contains_key("azure_storage_account_name")
        && let Some(account) = read_environment_value(ACCOUNT_NAME_ENVIRONMENT_VARIABLE)?
    {
        options.insert("azure_storage_account_name".into(), account);
    }
    Ok(())
}

fn load_managed_identity_environment(options: &mut DeltaStorageOptions) -> Result<(), Error> {
    // One explicit JSON selector replaces the environment's entire identity selection.
    if !MANAGED_IDENTITY_KEYS
        .iter()
        .any(|key| options.contains_key(key.as_ref()))
    {
        for (name, key) in [
            ("AZURE_CLIENT_ID", AzureConfigKey::ClientId),
            ("AZURE_OBJECT_ID", AzureConfigKey::ObjectId),
            ("AZURE_MSI_RESOURCE_ID", AzureConfigKey::MsiResourceId),
        ] {
            if let Some(value) = read_environment_value(name)? {
                options.insert(key.as_ref().to_owned(), value);
            }
        }
    }
    if !options.contains_key("azure_msi_endpoint")
        && let Some(endpoint) = read_environment_value("IDENTITY_ENDPOINT")?
    {
        options.insert("azure_msi_endpoint".into(), endpoint);
    }
    // The native provider reads this header; reject malformed inherited values before any IO.
    read_environment_value("IDENTITY_HEADER")?;
    Ok(())
}

fn validate_credential_family(
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

pub(super) fn validate_storage_option(
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
