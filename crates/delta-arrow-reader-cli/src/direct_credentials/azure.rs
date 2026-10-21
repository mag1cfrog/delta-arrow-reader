//! Explicit Azure account-key, SAS, and bearer-token sources.

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
        for name in [
            "AZURE_STORAGE_ACCOUNT_KEY",
            "AZURE_STORAGE_SAS_KEY",
            "AZURE_STORAGE_TOKEN",
        ] {
            if let Some(value) = read_environment_value(name)? {
                options.insert(name.to_ascii_lowercase(), value);
            }
        }
        if !options.contains_key("azure_storage_account_name")
            && let Some(account) = read_environment_value("AZURE_STORAGE_ACCOUNT_NAME")?
        {
            options.insert("azure_storage_account_name".into(), account);
        }
    }

    let credential_keys = [
        AzureConfigKey::AccessKey,
        AzureConfigKey::SasKey,
        AzureConfigKey::Token,
    ];
    let credential_count = credential_keys
        .iter()
        .filter(|key| options.contains_key(key.as_ref()))
        .count();
    ensure!(
        credential_count > 0,
        StorageAuthorizationSnafu {
            reason: "auth_source_incomplete_credentials"
        }
    );
    ensure!(
        credential_count == 1,
        StorageAuthorizationSnafu {
            reason: "auth_source_conflicting_credentials"
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
    let mut storage = MicrosoftAzureBuilder::new().with_url(location);
    for (name, value) in &options {
        let key = name
            .parse::<AzureConfigKey>()
            .map_err(|_| Error::StorageAuthorization {
                reason: "auth_source_unsupported_option",
            })?;
        storage = if key == AzureConfigKey::SasKey {
            let pairs = azure_sas::parse_query_pairs(value).context(StorageAuthorizationSnafu {
                reason: "auth_source_invalid_value",
            })?;
            storage.with_sas_authorization(pairs)
        } else {
            storage.with_config(key, value)
        };
    }
    let storage = storage.build().map_err(|_| Error::StorageAuthorization {
        reason: "auth_source_invalid_value",
    })?;
    // Keep the native provider for every read, without retaining a second credential source.
    for key in credential_keys {
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
            AzureConfigKey::AccessKey | AzureConfigKey::SasKey | AzureConfigKey::Token => {
                ensure!(
                    matches!(source, CredentialSource::Json),
                    StorageAuthorizationSnafu {
                        reason: "auth_source_conflicting_credentials"
                    }
                );
                validate_auth_source_value(&value)?;
            }
            AzureConfigKey::AccountName => validate_auth_source_value(&value)?,
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
