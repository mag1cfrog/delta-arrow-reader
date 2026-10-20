//! Validated table locations, credential responses, and local snapshot loading.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use delta_arrow_reader::{
    DeltaReaderError, DeltaSnapshotSelection, DeltaStorageOptions, DeltaTable, DeltaTableBuilder,
    WarmupMode,
};
use object_store::{
    ObjectStoreScheme,
    aws::{AmazonS3ConfigKey, AwsCredential},
    azure::{AzureConfigKey, AzureCredential},
    gcp::{GcpCredential, GoogleConfigKey},
};
use serde::Deserialize;
use url::Url;

use crate::input::JsonObject;

use super::{Error, credential_cache::TableCredentialCache};

/// Storage backends this adapter supports with temporary Unity Catalog credentials.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum StorageBackend {
    S3,
    R2,
    Azure,
    Gcs,
}

/// A validated table storage URL, normalized for comparing metadata and credential responses.
pub(super) struct TableStorageLocation {
    url: Url,
    pub(super) backend: StorageBackend,
}

impl TableStorageLocation {
    /// Normalizes supported cloud URLs and adds a trailing slash to the table root.
    pub(super) fn parse(raw: &str) -> Result<Self, Error> {
        let mut url = Url::parse(raw).map_err(|_| Error::InvalidDatabricksResponse)?;
        if url.query().is_some()
            || url.fragment().is_some()
            || url.password().is_some()
            || url.port().is_some()
            || url.path().starts_with("//")
            || raw.chars().any(char::is_control)
        {
            return Err(Error::InvalidDatabricksResponse);
        }
        let is_r2_endpoint = url.host_str().is_some_and(|host| {
            host.strip_suffix(".r2.cloudflarestorage.com")
                .is_some_and(|account| !account.is_empty() && !account.contains('.'))
        });
        let backend = match url.scheme() {
            "s3" | "s3a" if url.username().is_empty() && url.host_str().is_some() => {
                url.set_scheme("s3")
                    .map_err(|_| Error::InvalidDatabricksResponse)?;
                StorageBackend::S3
            }
            "gs" if url.username().is_empty() && url.host_str().is_some() => StorageBackend::Gcs,
            "r2" if is_r2_endpoint && !url.username().is_empty() => {
                if url.username().contains('%') || matches!(url.username(), "." | "..") {
                    return Err(Error::InvalidDatabricksResponse);
                }
                // The native S3 client accepts R2 HTTPS URLs; keep the account in the canonical root.
                url = Url::parse(&format!(
                    "https://{}/{}{}",
                    url.host_str().ok_or(Error::InvalidDatabricksResponse)?,
                    url.username(),
                    url.path()
                ))
                .map_err(|_| Error::InvalidDatabricksResponse)?;
                StorageBackend::R2
            }
            "https" if is_r2_endpoint && url.username().is_empty() => {
                let bucket = url.path().split('/').nth(1).unwrap_or_default();
                if bucket.is_empty() || bucket.contains('%') {
                    return Err(Error::InvalidDatabricksResponse);
                }
                StorageBackend::R2
            }
            "abfss"
                if !url.username().is_empty()
                    && url
                        .host_str()
                        .is_some_and(|host| host.ends_with(".dfs.core.windows.net")) =>
            {
                StorageBackend::Azure
            }
            "https" if url.username().is_empty() => {
                let account = url
                    .host_str()
                    .and_then(|host| {
                        host.strip_suffix(".dfs.core.windows.net")
                            .or_else(|| host.strip_suffix(".blob.core.windows.net"))
                    })
                    .filter(|account| !account.is_empty())
                    .ok_or(Error::UnsupportedDirectRead)?;
                let path = url.path().trim_start_matches('/');
                let (container, path) = path.split_once('/').unwrap_or((path, ""));
                if container.is_empty() {
                    return Err(Error::InvalidDatabricksResponse);
                }
                url = Url::parse(&format!(
                    "abfss://{container}@{account}.dfs.core.windows.net/{path}"
                ))
                .map_err(|_| Error::InvalidDatabricksResponse)?;
                StorageBackend::Azure
            }
            _ => return Err(Error::UnsupportedDirectRead),
        };
        ObjectStoreScheme::parse(&url).map_err(|_| Error::InvalidDatabricksResponse)?;
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        Ok(Self { url, backend })
    }
}

/// Allows transport and region settings without letting callers replace UC credentials or identity.
///
/// Before the metadata request, `None` accepts settings for any supported backend.
/// Once the table URL is known, settings must match that backend and S3 requires a region.
pub(super) fn validate_storage_options(
    options: &DeltaStorageOptions,
    backend: Option<StorageBackend>,
) -> Result<(), Error> {
    let mut has_region = false;
    for (key, value) in options {
        let key = key.to_ascii_lowercase();
        let aws_config_key = key.parse::<AmazonS3ConfigKey>();
        let azure_config_key = key.parse::<AzureConfigKey>();
        let gcp_config_key = key.parse::<GoogleConfigKey>();
        let aws_option_allowed = matches!(
            aws_config_key,
            Ok(AmazonS3ConfigKey::Region
                | AmazonS3ConfigKey::DefaultRegion
                | AmazonS3ConfigKey::Endpoint
                | AmazonS3ConfigKey::S3Endpoint
                | AmazonS3ConfigKey::VirtualHostedStyleRequest
                | AmazonS3ConfigKey::UnsignedPayload
                | AmazonS3ConfigKey::Checksum
                | AmazonS3ConfigKey::Client(_))
        );
        let azure_option_allowed = matches!(
            azure_config_key,
            Ok(AzureConfigKey::Endpoint | AzureConfigKey::Client(_))
        );
        let gcp_option_allowed = matches!(
            gcp_config_key,
            Ok(GoogleConfigKey::BaseUrl | GoogleConfigKey::Client(_))
        );
        // An option alias recognized by multiple backends must be safe in all of them.
        if (aws_config_key.is_ok() && !aws_option_allowed)
            || (azure_config_key.is_ok() && !azure_option_allowed)
            || (gcp_config_key.is_ok() && !gcp_option_allowed)
        {
            return Err(Error::InvalidStorageOptions);
        }
        let allowed_for_backend = match backend {
            None => aws_option_allowed || azure_option_allowed || gcp_option_allowed,
            Some(StorageBackend::S3) => aws_option_allowed,
            // R2 URLs supply the endpoint; only the service-specific override takes precedence.
            Some(StorageBackend::R2) => {
                aws_option_allowed && !matches!(aws_config_key, Ok(AmazonS3ConfigKey::Endpoint))
            }
            Some(StorageBackend::Azure) => azure_option_allowed,
            Some(StorageBackend::Gcs) => gcp_option_allowed,
        };
        if !allowed_for_backend {
            return Err(Error::InvalidStorageOptions);
        }
        if matches!(
            aws_config_key,
            Ok(AmazonS3ConfigKey::Region | AmazonS3ConfigKey::DefaultRegion)
        ) {
            if has_region || value.trim().is_empty() {
                return Err(Error::InvalidStorageOptions);
            }
            if backend == Some(StorageBackend::R2) && value != "auto" {
                return Err(Error::InvalidStorageOptions);
            }
            has_region = true;
        }
    }
    if backend == Some(StorageBackend::S3) && !has_region {
        return Err(Error::MissingS3Region);
    }
    Ok(())
}

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
    /// Azure SAS and bearer credentials must not switch families during renewal.
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

/// One validated credential response, before it enters the invocation's cache.
pub(super) struct TemporaryTableCredentials {
    pub(super) credentials: CloudCredentials,
    pub(super) expires_at: SystemTime,
}

/// Validated storage access for one UC invocation, including its renewable credentials.
///
/// The location and credentials must never be logged.
pub(crate) struct AuthorizedTableAccess {
    /// Table root shared by the metadata and credential responses.
    storage_url: Url,
    /// The caller's permitted transport and region settings.
    storage_options: DeltaStorageOptions,
    /// Retains the redacted refresh failure even if a dependency flattens its error source.
    credential_cache: Arc<TableCredentialCache>,
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
    if TableStorageLocation::parse(&response.url)?.url != storage_location.url {
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
            StorageBackend::Azure,
            TemporaryCredentialsResponse {
                aws_temp_credentials: None,
                r2_temp_credentials: None,
                azure_user_delegation_sas: Some(JsonObject(sas_credentials)),
                azure_aad: None,
                gcp_oauth_token: None,
                ..
            },
        ) => {
            let sas = sas_credentials
                .sas_token
                .strip_prefix('?')
                .unwrap_or(&sas_credentials.sas_token);
            // SAS signatures use literal '+', which form decoding would otherwise turn into a space.
            let query_pairs: Vec<_> =
                url::form_urlencoded::parse(sas.replace('+', "%2B").as_bytes())
                    .into_owned()
                    .collect();
            // Form decoding is lossy for invalid UTF-8, so reject replacement characters too.
            if sas.contains('#')
                || sas.chars().any(char::is_control)
                || sas
                    .split('&')
                    .any(|part| !part.trim().is_empty() && !part.contains('='))
                || query_pairs
                    .iter()
                    .any(|(key, value)| key.contains('\u{fffd}') || value.contains('\u{fffd}'))
                || !query_pairs
                    .iter()
                    .any(|(key, value)| key == "sig" && !value.is_empty())
            {
                return Err(Error::InvalidDatabricksResponse);
            }
            CloudCredentials::Azure(Arc::new(AzureCredential::SASToken(query_pairs)))
        }
        (
            StorageBackend::Azure,
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

impl AuthorizedTableAccess {
    /// Binds renewal to the selected profile, immutable table ID, and canonical root.
    pub(super) fn new(
        storage_location: TableStorageLocation,
        storage_options: DeltaStorageOptions,
        profile: String,
        table_id: String,
        credentials: TemporaryTableCredentials,
    ) -> Result<Self, Error> {
        let storage_url = storage_location.url.clone();
        let credential_cache =
            TableCredentialCache::new(profile, table_id, storage_location, credentials)?;
        Ok(Self {
            storage_url,
            storage_options,
            credential_cache,
        })
    }

    /// Loads the requested snapshot with credentials renewed on demand by the storage clients.
    ///
    /// Both UC commands require a read protocol supported by the local reader.
    pub(crate) async fn load_table(
        &self,
        snapshot_selection: DeltaSnapshotSelection,
    ) -> Result<DeltaTable, crate::Error> {
        let builder = DeltaTableBuilder::new(self.storage_url.as_str())
            .with_warmup(WarmupMode::None)
            .with_snapshot_selection(snapshot_selection)
            .with_credential_provider(self.credential_cache.provider())
            .with_storage_options(self.storage_options.clone());
        let table = match builder.load_table().await {
            Ok(table) => table,
            Err(source) => return Err(self.map_reader_error(source).await),
        };
        // The Delta log can declare unsupported features even when UC metadata passed validation.
        let protocol = table.protocol();
        if !matches!(protocol.min_reader_version(), 1..=3)
            || protocol.first_unsupported_reader_feature().is_some()
            || protocol
                .writer_features()
                .iter()
                .any(|feature| feature == "catalogManaged")
        {
            return Err(crate::Error::UnityCatalog {
                source: Error::UnsupportedDirectRead,
            });
        }
        Ok(table)
    }

    /// Preserves UC authorization failures through snapshot, planning, and streaming errors.
    pub(crate) async fn map_reader_error(&self, source: DeltaReaderError) -> crate::Error {
        self.credential_cache
            .refresh_failure()
            .await
            .map_or(crate::Error::Reader { source }, |source| {
                crate::Error::UnityCatalog { source }
            })
    }
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
    fn options_keep_transport_and_region_but_never_override_identity() {
        for key in [
            "AWS_ACCESS_KEY_ID",
            "access_key_id",
            "AWS_TOKEN",
            "token",
            "AWS_BUCKET_NAME",
            "bucket",
            "azure_storage_master_key",
            "access_key",
            "AZURE_STORAGE_SAS_TOKEN",
            "sas_key",
            "container_name",
            "account_name",
            "azure_use_azure_cli",
            "skip_signature",
            "aws_role_arn",
            "federated_token_file",
            "aws_metadata_endpoint",
            "GOOGLE_SERVICE_ACCOUNT",
            "service_account_key",
            "google_application_credentials",
            "google_bucket",
            "google_skip_signature",
            "unknown_option",
        ] {
            let options = DeltaStorageOptions::from([(key.into(), "secret-sentinel".into())]);
            assert_eq!(
                validate_storage_options(&options, None),
                Err(Error::InvalidStorageOptions),
                "{key}"
            );
        }
        let mut options = DeltaStorageOptions::from([
            ("AWS_REGION".into(), "eu-west-1".into()),
            ("aws_allow_http".into(), "true".into()),
            ("aws_endpoint".into(), "http://127.0.0.1:1234".into()),
        ]);
        validate_storage_options(&options, Some(StorageBackend::S3)).unwrap();
        options.insert("default_region".into(), "us-east-1".into());
        assert_eq!(
            validate_storage_options(&options, Some(StorageBackend::S3)),
            Err(Error::InvalidStorageOptions)
        );
        assert_eq!(
            validate_storage_options(&DeltaStorageOptions::new(), Some(StorageBackend::S3)),
            Err(Error::MissingS3Region)
        );
        validate_storage_options(&DeltaStorageOptions::new(), Some(StorageBackend::Azure)).unwrap();
        let gcs_options = DeltaStorageOptions::from([
            ("GOOGLE_BASE_URL".into(), "http://127.0.0.1:1234".into()),
            ("google_allow_http".into(), "true".into()),
        ]);
        validate_storage_options(&gcs_options, None).unwrap();
        validate_storage_options(&gcs_options, Some(StorageBackend::Gcs)).unwrap();
        assert_eq!(
            validate_storage_options(&gcs_options, Some(StorageBackend::S3)),
            Err(Error::InvalidStorageOptions)
        );
        assert_eq!(
            validate_storage_options(
                &DeltaStorageOptions::from([("aws_region".into(), "eu-west-1".into())]),
                Some(StorageBackend::Gcs),
            ),
            Err(Error::InvalidStorageOptions)
        );
    }

    #[test]
    fn azure_container_root_locations_are_equivalent() {
        let expected = "abfss://container@account.dfs.core.windows.net/";
        for location in [
            "https://account.blob.core.windows.net/container",
            "https://account.blob.core.windows.net/container/",
            "https://account.dfs.core.windows.net/container",
            "https://account.dfs.core.windows.net/container/",
            expected,
        ] {
            assert_eq!(
                TableStorageLocation::parse(location).unwrap().url.as_str(),
                expected
            );
        }
        for location in [
            "https://account.blob.core.windows.net",
            "https://account.dfs.core.windows.net/",
        ] {
            assert!(TableStorageLocation::parse(location).is_err(), "{location}");
        }
    }

    #[test]
    fn r2_locations_preserve_account_bucket_and_encoded_path() {
        for (location, expected) in [
            (
                "r2://bucket@account.r2.cloudflarestorage.com",
                "https://account.r2.cloudflarestorage.com/bucket/",
            ),
            (
                "r2://bucket@account.r2.cloudflarestorage.com/",
                "https://account.r2.cloudflarestorage.com/bucket/",
            ),
            (
                "https://account.r2.cloudflarestorage.com/bucket",
                "https://account.r2.cloudflarestorage.com/bucket/",
            ),
            (
                "r2://bucket@account.r2.cloudflarestorage.com/a%20b/%252F",
                "https://account.r2.cloudflarestorage.com/bucket/a%20b/%252F/",
            ),
            (
                "https://account.r2.cloudflarestorage.com/bucket/a%20b/%252F/",
                "https://account.r2.cloudflarestorage.com/bucket/a%20b/%252F/",
            ),
        ] {
            let parsed = TableStorageLocation::parse(location).unwrap();
            assert_eq!(parsed.url.as_str(), expected);
            assert!(parsed.backend == StorageBackend::R2);
        }
        for location in [
            "r2://account.r2.cloudflarestorage.com/bucket/table",
            "r2://bucket@unrecognized.example/table",
            "r2://bucket@account.r2.cloudflarestorage.com.example/table",
            "r2://bucket@sub.account.r2.cloudflarestorage.com/table",
            "r2://bucket@account.r2.cloudflarestorage.com:1234/table",
            "r2://bucket:secret@account.r2.cloudflarestorage.com/table",
            "r2://buck%65t@account.r2.cloudflarestorage.com/table",
            "r2://..@account.r2.cloudflarestorage.com/table",
            "https://account.r2.cloudflarestorage.com/",
            "https://account.r2.cloudflarestorage.com/buck%2Fet/table",
            "https://account.r2.cloudflarestorage.com/bucket/table?secret",
            "https://account.r2.cloudflarestorage.com/bucket/table#fragment",
            "http://account.r2.cloudflarestorage.com/bucket/table",
        ] {
            assert!(TableStorageLocation::parse(location).is_err(), "{location}");
        }
    }

    #[test]
    fn r2_options_use_auto_region_and_the_service_endpoint_override() {
        validate_storage_options(&DeltaStorageOptions::new(), Some(StorageBackend::R2)).unwrap();
        let options = DeltaStorageOptions::from([
            ("AWS_DEFAULT_REGION".into(), "auto".into()),
            ("aws_endpoint_url_s3".into(), "http://127.0.0.1:1234".into()),
            ("aws_allow_http".into(), "true".into()),
        ]);
        validate_storage_options(&options, Some(StorageBackend::R2)).unwrap();
        for (key, value) in [
            ("region", "eu-west-1"),
            ("default_region", "us-east-1"),
            ("aws_endpoint", "http://127.0.0.1:1234"),
            ("endpoint_url", "http://127.0.0.1:1234"),
            ("azure_endpoint", "http://127.0.0.1:1234"),
            ("aws_bucket", "other"),
            ("aws_session_token", "secret"),
        ] {
            let options = DeltaStorageOptions::from([(key.into(), value.into())]);
            assert_eq!(
                validate_storage_options(&options, Some(StorageBackend::R2)),
                Err(Error::InvalidStorageOptions),
                "{key}"
            );
        }
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
        let resolve = |value: &Value| {
            parse_temporary_credentials(
                &serde_json::to_vec(value).unwrap(),
                &TableStorageLocation::parse("r2://bucket@account.r2.cloudflarestorage.com/table")
                    .unwrap(),
            )
        };
        let access = resolve(&response).unwrap();
        let CloudCredentials::Aws(credentials) = access.credentials else {
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
            assert!(resolve(&value).is_err(), "{field}");
        }
        let mut wrong_kind = response.clone();
        wrong_kind["aws_temp_credentials"] = wrong_kind
            .as_object_mut()
            .unwrap()
            .remove("r2_temp_credentials")
            .unwrap();
        assert!(matches!(
            resolve(&wrong_kind),
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
        let resolve = |value: &Value| {
            parse_temporary_credentials(
                &serde_json::to_vec(value).unwrap(),
                &TableStorageLocation::parse("s3a://bucket/table").unwrap(),
            )
        };
        let CloudCredentials::Aws(credentials) = resolve(&value).unwrap().credentials else {
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
            assert!(resolve(&invalid_value).is_err(), "{field}");
        }
        let mut azure = value;
        azure
            .as_object_mut()
            .unwrap()
            .remove("aws_temp_credentials");
        azure["url"] = json!("https://account.blob.core.windows.net/container/a%20b/%252F");
        azure["azure_user_delegation_sas"] =
            json!({"sas_token": "?sv=test&sig=test%2Bsignature&sp=rl"});
        let result = parse_temporary_credentials(
            &serde_json::to_vec(&azure).unwrap(),
            &TableStorageLocation::parse(
                "abfss://container@account.dfs.core.windows.net/a%20b/%252F/",
            )
            .unwrap(),
        )
        .unwrap();
        let CloudCredentials::Azure(credentials) = result.credentials else {
            panic!("Azure must use Azure credentials");
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
            let mut response = azure.clone();
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
            let mut response = azure.clone();
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
        let resolve = |value: &Value| {
            parse_temporary_credentials(
                &serde_json::to_vec(value).unwrap(),
                &TableStorageLocation::parse("gs://bucket/table").unwrap(),
            )
        };
        let access = resolve(&response).unwrap();
        let CloudCredentials::Gcp(credentials) = access.credentials else {
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
                resolve(&value),
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
            assert!(matches!(resolve(&value), Err(Error::UnsupportedDirectRead)));
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
            assert!(resolve(&value).is_err(), "{url}");
        }
        response["expiration_time"] = json!(0);
        assert!(matches!(resolve(&response), Err(Error::CredentialsExpired)));
    }

    #[test]
    fn azure_bearer_credentials_require_one_valid_token_for_azure_storage() {
        let mut response = aws_credentials_response();
        response
            .as_object_mut()
            .unwrap()
            .remove("aws_temp_credentials");
        response["url"] = json!("abfss://container@account.dfs.core.windows.net/table/");
        response["azure_aad"] = json!({"aad_token":"secret-bearer-token"});
        let resolve = |value: &Value| {
            parse_temporary_credentials(
                &serde_json::to_vec(value).unwrap(),
                &TableStorageLocation::parse(value["url"].as_str().unwrap()).unwrap(),
            )
        };
        let access = resolve(&response).unwrap();
        let CloudCredentials::Azure(credentials) = access.credentials else {
            panic!("Azure must use Azure credentials");
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
                resolve(&value),
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
            assert!(matches!(resolve(&value), Err(Error::UnsupportedDirectRead)));
        }
    }
}
