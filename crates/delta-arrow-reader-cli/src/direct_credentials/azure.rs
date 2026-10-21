//! Explicit Azure static credentials and client-secret authorization.

use std::sync::Arc;

use delta_arrow_reader::{DeltaStorageCredentialProvider, DeltaStorageOptions, DeltaTableBuilder};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use snafu::{OptionExt, ensure};

use super::{CredentialSource, read_environment_value, validate_auth_source_value};
use crate::{Error, StorageAuthorizationSnafu, azure_sas};

pub(super) fn configure_table_credentials(
    builder: DeltaTableBuilder,
    location: &str,
    options: DeltaStorageOptions,
    source: CredentialSource,
) -> Result<DeltaTableBuilder, Error> {
    let mut options = normalize_azure_options(options, source)?;
    if matches!(source, CredentialSource::Env) {
        for (name, key) in [
            ("AZURE_STORAGE_ACCOUNT_KEY", AzureConfigKey::AccessKey),
            ("AZURE_STORAGE_SAS_KEY", AzureConfigKey::SasKey),
            ("AZURE_STORAGE_TOKEN", AzureConfigKey::Token),
            ("AZURE_CLIENT_ID", AzureConfigKey::ClientId),
            ("AZURE_CLIENT_SECRET", AzureConfigKey::ClientSecret),
            ("AZURE_TENANT_ID", AzureConfigKey::AuthorityId),
        ] {
            if let Some(value) = read_environment_value(name)? {
                options.insert(key.as_ref().to_owned(), value);
            }
        }
        if !options.contains_key("azure_storage_account_name")
            && let Some(account) = read_environment_value("AZURE_STORAGE_ACCOUNT_NAME")?
        {
            options.insert("azure_storage_account_name".into(), account);
        }
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
    for key in static_credential_keys.into_iter().chain(client_secret_keys) {
        options.remove(key.as_ref());
    }
    Ok(builder
        .with_storage_options(options)
        .with_credential_provider(DeltaStorageCredentialProvider::Azure(Arc::clone(
            storage.credentials(),
        ))))
}

fn normalize_azure_options(
    options: DeltaStorageOptions,
    source: CredentialSource,
) -> Result<DeltaStorageOptions, Error> {
    let mut normalized = DeltaStorageOptions::new();
    for (name, value) in options {
        let key = name
            .to_ascii_lowercase()
            .parse::<AzureConfigKey>()
            .map_err(|_| Error::StorageAuthorization {
                reason: "auth_source_unsupported_option",
            })?;
        match key {
            AzureConfigKey::AccessKey
            | AzureConfigKey::SasKey
            | AzureConfigKey::Token
            | AzureConfigKey::ClientId
            | AzureConfigKey::ClientSecret
            | AzureConfigKey::AuthorityId => {
                ensure!(
                    matches!(source, CredentialSource::Json),
                    StorageAuthorizationSnafu {
                        reason: "auth_source_conflicting_credentials"
                    }
                );
                validate_auth_source_value(&value)?;
            }
            AzureConfigKey::AccountName | AzureConfigKey::AuthorityHost => {
                validate_auth_source_value(&value)?;
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
        ensure!(
            normalized.insert(key.as_ref().to_owned(), value).is_none(),
            StorageAuthorizationSnafu {
                reason: "auth_source_duplicate_option"
            }
        );
    }
    Ok(normalized)
}
