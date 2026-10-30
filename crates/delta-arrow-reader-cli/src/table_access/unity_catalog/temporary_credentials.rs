//! Decode UC credential responses, validate their scope, and produce native cloud credentials.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use object_store::{aws::AwsCredential, azure::AzureCredential, gcp::GcpCredential};
use serde::Deserialize;

use crate::input::JsonObject;

use super::super::azure_sas;
use super::{
    Error,
    storage_location::{StorageBackend, TableStorageLocation},
};

/// Temporary access keys and session token for an AWS S3 or Cloudflare R2 table.
#[derive(Deserialize)]
struct S3SessionCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    /// S3 access points need a storage mapping this adapter does not implement.
    access_point: Option<String>,
}

/// An Azure user delegation SAS token returned for the table's storage location.
#[derive(Deserialize)]
struct AzureSasCredentials {
    sas_token: String,
}

/// An Azure AD bearer token for the table's storage location.
#[derive(Deserialize)]
struct AzureBearerCredentials {
    aad_token: String,
}

/// A GCS OAuth bearer token for the table's storage location.
#[derive(Deserialize)]
struct GcsOAuthCredentials {
    oauth_token: String,
}

/// Raw response from the Databricks temporary-table-credentials command.
///
/// Separate fields allow rejection of mixed credential families.
#[derive(Deserialize)]
struct TemporaryCredentialsResponse {
    url: String,
    /// Credential expiry as milliseconds since the Unix epoch.
    expiration_time: u64,
    aws_temp_credentials: Option<JsonObject<S3SessionCredentials>>,
    r2_temp_credentials: Option<JsonObject<S3SessionCredentials>>,
    azure_user_delegation_sas: Option<JsonObject<AzureSasCredentials>>,
    azure_aad: Option<JsonObject<AzureBearerCredentials>>,
    gcp_oauth_token: Option<JsonObject<GcsOAuthCredentials>>,
}

/// Native credentials from exactly one supported Unity Catalog credential family.
#[derive(Clone)]
pub(super) enum CloudCredentials {
    Aws(Arc<AwsCredential>),
    Azure(Arc<AzureCredential>),
    Gcp(Arc<GcpCredential>),
}

impl CloudCredentials {
    /// Matches the native credential family; Azure SAS and bearer tokens are distinct.
    pub(super) fn same_kind(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Aws(_), Self::Aws(_)) | (Self::Gcp(_), Self::Gcp(_)) => true,
            (Self::Azure(left), Self::Azure(right)) => {
                std::mem::discriminant(left.as_ref()) == std::mem::discriminant(right.as_ref())
            }
            _ => false,
        }
    }
}

/// Validated native cloud credentials and their expiry.
pub(super) struct TemporaryTableCredentials {
    pub(super) credentials: CloudCredentials,
    pub(super) expires_at: SystemTime,
}

/// Validates every grant against the original table root and supported credential families.
pub(super) fn parse_temporary_credentials(
    credentials_json: &[u8],
    storage_location: &TableStorageLocation,
) -> Result<TemporaryTableCredentials, Error> {
    let response =
        serde_json::from_slice::<JsonObject<TemporaryCredentialsResponse>>(credentials_json)
            .map_err(|_| Error::InvalidDatabricksResponse)?
            .0;
    credentials_for_location(response, storage_location)
}

/// Published pipeline metadata can be below the granted data root; renewals keep that exact root.
pub(super) fn parse_pipeline_credentials(
    credentials_json: &[u8],
    read_location: &TableStorageLocation,
) -> Result<(TemporaryTableCredentials, TableStorageLocation), Error> {
    let response =
        serde_json::from_slice::<JsonObject<TemporaryCredentialsResponse>>(credentials_json)
            .map_err(|_| Error::InvalidDatabricksResponse)?
            .0;
    let scope = TableStorageLocation::parse(&response.url)?;
    let scope_url = scope.as_url();
    let read_url = read_location.as_url();
    if scope_url[..url::Position::BeforePath] != read_url[..url::Position::BeforePath]
        || !read_url.path().starts_with(scope_url.path())
    {
        return Err(Error::InvalidDatabricksResponse);
    }
    let credentials = credentials_for_location(response, &scope)?;
    Ok((credentials, scope))
}

fn credentials_for_location(
    response: TemporaryCredentialsResponse,
    storage_location: &TableStorageLocation,
) -> Result<TemporaryTableCredentials, Error> {
    if TableStorageLocation::parse(&response.url)?.as_url() != storage_location.as_url() {
        return Err(Error::InvalidDatabricksResponse);
    }
    let credentials_expire_at = UNIX_EPOCH
        .checked_add(Duration::from_millis(response.expiration_time))
        .ok_or(Error::InvalidDatabricksResponse)?;
    if credentials_expire_at <= SystemTime::now() {
        return Err(Error::CredentialsExpired);
    }
    // Accept exactly one credential type, matching the table URL's storage backend.
    let credentials = match (storage_location.backend, response) {
        (
            StorageBackend::S3,
            TemporaryCredentialsResponse {
                aws_temp_credentials: Some(JsonObject(credentials)),
                r2_temp_credentials: None,
                azure_user_delegation_sas: None,
                azure_aad: None,
                gcp_oauth_token: None,
                ..
            },
        )
        | (
            StorageBackend::R2,
            TemporaryCredentialsResponse {
                aws_temp_credentials: None,
                r2_temp_credentials: Some(JsonObject(credentials)),
                azure_user_delegation_sas: None,
                azure_aad: None,
                gcp_oauth_token: None,
                ..
            },
        ) => {
            if credentials.access_point.is_some() {
                return Err(Error::UnsupportedDirectRead);
            }
            for value in [
                &credentials.access_key_id,
                &credentials.secret_access_key,
                &credentials.session_token,
            ] {
                if value.trim().is_empty() || value.chars().any(char::is_control) {
                    return Err(Error::InvalidDatabricksResponse);
                }
            }
            CloudCredentials::Aws(Arc::new(AwsCredential {
                key_id: credentials.access_key_id,
                secret_key: credentials.secret_access_key,
                token: Some(credentials.session_token),
            }))
        }
        (
            StorageBackend::AzureBlob,
            TemporaryCredentialsResponse {
                aws_temp_credentials: None,
                r2_temp_credentials: None,
                azure_user_delegation_sas: Some(JsonObject(sas_credentials)),
                azure_aad: None,
                gcp_oauth_token: None,
                ..
            },
        ) => {
            let query_pairs = azure_sas::parse_query_pairs(&sas_credentials.sas_token)
                .ok_or(Error::InvalidDatabricksResponse)?;
            CloudCredentials::Azure(Arc::new(AzureCredential::SASToken(query_pairs)))
        }
        (
            StorageBackend::AzureBlob,
            TemporaryCredentialsResponse {
                aws_temp_credentials: None,
                r2_temp_credentials: None,
                azure_user_delegation_sas: None,
                azure_aad: Some(JsonObject(bearer_credentials)),
                gcp_oauth_token: None,
                ..
            },
        ) => {
            validate_bearer_token(&bearer_credentials.aad_token)?;
            CloudCredentials::Azure(Arc::new(AzureCredential::BearerToken(
                bearer_credentials.aad_token,
            )))
        }
        (
            StorageBackend::Gcs,
            TemporaryCredentialsResponse {
                aws_temp_credentials: None,
                r2_temp_credentials: None,
                azure_user_delegation_sas: None,
                azure_aad: None,
                gcp_oauth_token: Some(JsonObject(credentials)),
                ..
            },
        ) => {
            validate_bearer_token(&credentials.oauth_token)?;
            CloudCredentials::Gcp(Arc::new(GcpCredential {
                bearer: credentials.oauth_token,
            }))
        }
        _ => return Err(Error::UnsupportedDirectRead),
    };
    Ok(TemporaryTableCredentials {
        credentials,
        expires_at: credentials_expire_at,
    })
}

fn validate_bearer_token(token: &str) -> Result<(), Error> {
    if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::InvalidDatabricksResponse);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
