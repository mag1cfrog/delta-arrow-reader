//! Decode UC credential responses, validate their scope, and produce native cloud credentials.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use object_store::{aws::AwsCredential, azure::AzureCredential, gcp::GcpCredential};
use serde::Deserialize;

use crate::input::JsonObject;

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
            let query_pairs = crate::azure_sas::parse_query_pairs(&sas_credentials.sas_token)
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
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn aws_credentials_response() -> Value {
        let expiration_time = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
            + Duration::from_secs(300))
        .as_millis() as u64;
        json!({
            "url": "s3://bucket/table/",
            "expiration_time": expiration_time,
            "aws_temp_credentials": {
                "access_key_id": "test-key",
                "secret_access_key": "test-secret",
                "session_token": "test-token"
            }
        })
    }

    #[test]
    fn r2_credentials_require_the_same_root_and_exactly_one_credential_kind() {
        let mut response = aws_credentials_response();
        let credentials = response
            .as_object_mut()
            .unwrap()
            .remove("aws_temp_credentials")
            .unwrap();
        response["r2_temp_credentials"] = credentials;
        response["url"] = json!("https://account.r2.cloudflarestorage.com/bucket/table/");
        let parse_grant = |value: &Value| {
            parse_temporary_credentials(
                &serde_json::to_vec(value).unwrap(),
                &TableStorageLocation::parse("r2://bucket@account.r2.cloudflarestorage.com/table")
                    .unwrap(),
            )
        };
        let grant = parse_grant(&response).unwrap();
        let CloudCredentials::Aws(credentials) = grant.credentials else {
            panic!("R2 must use AWS credentials");
        };
        assert_eq!(credentials.key_id, "test-key");
        assert_eq!(credentials.secret_key, "test-secret");
        assert_eq!(credentials.token.as_deref(), Some("test-token"));
        for (field, invalid) in [
            (
                "url",
                json!("https://other.r2.cloudflarestorage.com/bucket/table/"),
            ),
            (
                "url",
                json!("https://account.r2.cloudflarestorage.com/other/table/"),
            ),
            (
                "url",
                json!("https://account.r2.cloudflarestorage.com/bucket/other/"),
            ),
            (
                "aws_temp_credentials",
                response["r2_temp_credentials"].clone(),
            ),
            (
                "azure_user_delegation_sas",
                json!({"sas_token":"sig=secret"}),
            ),
            ("azure_aad", json!({"aad_token":"secret"})),
            ("gcp_oauth_token", json!({"oauth_token":"secret"})),
            ("r2_temp_credentials", json!([])),
            ("r2_temp_credentials", json!({"access_key_id":"key"})),
            (
                "r2_temp_credentials",
                json!({
                    "access_key_id": "key",
                    "secret_access_key": "",
                    "session_token": "token"
                }),
            ),
        ] {
            let mut value = response.clone();
            value[field] = invalid;
            assert!(parse_grant(&value).is_err(), "{field}");
        }
        let mut wrong_kind = response.clone();
        wrong_kind["aws_temp_credentials"] = wrong_kind
            .as_object_mut()
            .unwrap()
            .remove("r2_temp_credentials")
            .unwrap();
        assert!(matches!(
            parse_grant(&wrong_kind),
            Err(Error::UnsupportedDirectRead)
        ));
        response["url"] = json!("s3://bucket/table/");
        assert!(matches!(
            parse_temporary_credentials(
                &serde_json::to_vec(&response).unwrap(),
                &TableStorageLocation::parse("s3://bucket/table/").unwrap(),
            ),
            Err(Error::UnsupportedDirectRead)
        ));
    }

    #[test]
    fn credentials_validate_expiry_location_and_cloud_before_mapping() {
        let value = aws_credentials_response();
        let parse_grant = |value: &Value| {
            parse_temporary_credentials(
                &serde_json::to_vec(value).unwrap(),
                &TableStorageLocation::parse("s3a://bucket/table").unwrap(),
            )
        };
        let CloudCredentials::Aws(credentials) = parse_grant(&value).unwrap().credentials else {
            panic!("S3 must use AWS credentials");
        };
        assert_eq!(credentials.token.as_deref(), Some("test-token"));
        for (field, invalid) in [
            ("expiration_time", json!(0)),
            ("expiration_time", json!("tomorrow")),
            ("url", json!("s3://other/table")),
            ("url", json!("s3://bucket/table2")),
            ("url", json!("s3://bucket/table?secret-sentinel")),
            (
                "aws_temp_credentials",
                json!({
                    "access_key_id": "test-key",
                    "secret_access_key": "",
                    "session_token": "test-token"
                }),
            ),
            ("aws_temp_credentials", json!({"access_key_id":"test-key"})),
            ("r2_temp_credentials", value["aws_temp_credentials"].clone()),
            (
                "azure_user_delegation_sas",
                json!({"sas_token":"sig=secret"}),
            ),
            ("azure_aad", json!({"aad_token":"secret"})),
            ("gcp_oauth_token", json!({"oauth_token":"secret"})),
        ] {
            let mut invalid_value = value.clone();
            invalid_value[field] = invalid;
            assert!(parse_grant(&invalid_value).is_err(), "{field}");
        }
        let mut azure_blob_response = value;
        azure_blob_response
            .as_object_mut()
            .unwrap()
            .remove("aws_temp_credentials");
        azure_blob_response["url"] =
            json!("https://account.blob.core.windows.net/container/a%20b/%252F");
        azure_blob_response["azure_user_delegation_sas"] =
            json!({"sas_token": "?sv=test&sig=test%2Bsignature&sp=rl"});
        let result = parse_temporary_credentials(
            &serde_json::to_vec(&azure_blob_response).unwrap(),
            &TableStorageLocation::parse(
                "abfss://container@account.dfs.core.windows.net/a%20b/%252F/",
            )
            .unwrap(),
        )
        .unwrap();
        let CloudCredentials::Azure(credentials) = result.credentials else {
            panic!("Azure Blob must use Azure credentials");
        };
        let AzureCredential::SASToken(query_pairs) = credentials.as_ref() else {
            panic!("expected SAS credentials");
        };
        assert_eq!(
            query_pairs,
            &vec![
                ("sv".into(), "test".into()),
                ("sig".into(), "test+signature".into()),
                ("sp".into(), "rl".into()),
            ]
        );
        for sas in [
            "",
            "sig=",
            "sig=secret&missing-value",
            "sig=secret#fragment",
            "sig=secret\nvalue",
            "sig=%FF",
        ] {
            let mut response = azure_blob_response.clone();
            response["azure_user_delegation_sas"] = json!({"sas_token": sas});
            assert!(matches!(
                parse_temporary_credentials(
                    &serde_json::to_vec(&response).unwrap(),
                    &TableStorageLocation::parse(response["url"].as_str().unwrap()).unwrap(),
                ),
                Err(Error::InvalidDatabricksResponse)
            ));
        }
        for (field, conflicting) in [
            (
                "aws_temp_credentials",
                aws_credentials_response()["aws_temp_credentials"].clone(),
            ),
            (
                "r2_temp_credentials",
                aws_credentials_response()["aws_temp_credentials"].clone(),
            ),
            ("azure_aad", json!({"aad_token": "secret-bearer-token"})),
        ] {
            let mut response = azure_blob_response.clone();
            response[field] = conflicting;
            assert!(matches!(
                parse_temporary_credentials(
                    &serde_json::to_vec(&response).unwrap(),
                    &TableStorageLocation::parse(response["url"].as_str().unwrap()).unwrap(),
                ),
                Err(Error::UnsupportedDirectRead)
            ));
        }
        for location in [
            "file:///tmp/table",
            "https://unknown/table",
            "abfs://container@account.dfs.core.windows.net/table",
            "abfss://account.dfs.core.windows.net/table",
            "s3://user:secret@bucket/table",
            "s3://bucket//table",
            "s3://bucket/table#fragment",
        ] {
            assert!(TableStorageLocation::parse(location).is_err(), "{location}");
        }
    }

    #[test]
    fn gcs_credentials_require_one_token_for_the_same_bucket_and_root() {
        let mut response = aws_credentials_response();
        response
            .as_object_mut()
            .unwrap()
            .remove("aws_temp_credentials");
        response["url"] = json!("gs://bucket/table/");
        response["gcp_oauth_token"] = json!({"oauth_token": "secret-gcp-token"});
        let parse_grant = |value: &Value| {
            parse_temporary_credentials(
                &serde_json::to_vec(value).unwrap(),
                &TableStorageLocation::parse("gs://bucket/table").unwrap(),
            )
        };
        let grant = parse_grant(&response).unwrap();
        let CloudCredentials::Gcp(credentials) = grant.credentials else {
            panic!("GCS must use GCP credentials");
        };
        assert_eq!(credentials.bearer, "secret-gcp-token");
        for invalid in [
            json!("secret-gcp-token"),
            json!([]),
            json!({}),
            json!({"oauth_token": null}),
            json!({"oauth_token": ""}),
            json!({"oauth_token": " "}),
            json!({"oauth_token": "secret\ngcp-token"}),
            json!({"oauth_token": "secret gcp-token"}),
        ] {
            let mut value = response.clone();
            value["gcp_oauth_token"] = invalid;
            assert!(matches!(
                parse_grant(&value),
                Err(Error::InvalidDatabricksResponse)
            ));
        }
        for (field, credentials) in [
            (
                "aws_temp_credentials",
                aws_credentials_response()["aws_temp_credentials"].clone(),
            ),
            (
                "r2_temp_credentials",
                aws_credentials_response()["aws_temp_credentials"].clone(),
            ),
            (
                "azure_user_delegation_sas",
                json!({"sas_token": "sig=secret"}),
            ),
            ("azure_aad", json!({"aad_token": "secret-bearer-token"})),
        ] {
            let mut value = response.clone();
            value[field] = credentials;
            assert!(matches!(
                parse_grant(&value),
                Err(Error::UnsupportedDirectRead)
            ));
        }
        for url in [
            "gs://other/table/",
            "gs://bucket/other/",
            "gs://bucket/table?secret",
            "gs://user@bucket/table",
            "gs://bucket:1234/table",
            "gs://bucket//table",
            "gs://bucket/table#fragment",
            "s3://bucket/table/",
        ] {
            let mut value = response.clone();
            value["url"] = json!(url);
            assert!(parse_grant(&value).is_err(), "{url}");
        }
        response["expiration_time"] = json!(0);
        assert!(matches!(
            parse_grant(&response),
            Err(Error::CredentialsExpired)
        ));
    }

    #[test]
    fn azure_bearer_credentials_require_one_valid_token_for_azure_blob_storage() {
        let mut response = aws_credentials_response();
        response
            .as_object_mut()
            .unwrap()
            .remove("aws_temp_credentials");
        response["url"] = json!("abfss://container@account.dfs.core.windows.net/table/");
        response["azure_aad"] = json!({"aad_token":"secret-bearer-token"});
        let parse_grant = |value: &Value| {
            parse_temporary_credentials(
                &serde_json::to_vec(value).unwrap(),
                &TableStorageLocation::parse(value["url"].as_str().unwrap()).unwrap(),
            )
        };
        let grant = parse_grant(&response).unwrap();
        let CloudCredentials::Azure(credentials) = grant.credentials else {
            panic!("Azure Blob must use Azure credentials");
        };
        assert!(matches!(
            credentials.as_ref(),
            AzureCredential::BearerToken(token) if token == "secret-bearer-token"
        ));
        for invalid in [
            json!("secret-bearer-token"),
            json!([]),
            json!({}),
            json!({"aad_token":null}),
            json!({"aad_token":""}),
            json!({"aad_token":" "}),
            json!({"aad_token":"secret\nbearer-token"}),
            json!({"aad_token":"secret bearer-token"}),
        ] {
            let mut value = response.clone();
            value["azure_aad"] = invalid;
            assert!(matches!(
                parse_grant(&value),
                Err(Error::InvalidDatabricksResponse)
            ));
        }
        for (field, conflicting) in [
            (
                "azure_user_delegation_sas",
                json!({"sas_token":"sig=secret"}),
            ),
            (
                "aws_temp_credentials",
                aws_credentials_response()["aws_temp_credentials"].clone(),
            ),
            ("gcp_oauth_token", json!({"oauth_token":"secret"})),
            (
                "r2_temp_credentials",
                json!({
                    "access_key_id": "key",
                    "secret_access_key": "secret",
                    "session_token": "token"
                }),
            ),
            ("url", json!("s3://bucket/table/")),
        ] {
            let mut value = response.clone();
            value[field] = conflicting;
            assert!(matches!(
                parse_grant(&value),
                Err(Error::UnsupportedDirectRead)
            ));
        }
    }
}
