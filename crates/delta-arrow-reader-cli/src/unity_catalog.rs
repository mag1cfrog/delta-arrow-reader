//! Databricks CLI authorization for local Delta metadata reads.
//!
//! The CLI supplies table metadata and temporary storage credentials. Core then reads
//! the Delta log directly. Table identifiers, credentials, and raw command output
//! must stay out of diagnostics; the types carrying them deliberately omit `Debug`.

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    io,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use delta_arrow_reader::{DeltaStorageOptions, DeltaTable, DeltaTableBuilder};
use object_store::{ObjectStoreScheme, aws::AmazonS3ConfigKey, azure::AzureConfigKey};
use serde::Deserialize;
use serde_json::Value;
use snafu::{ResultExt, Snafu};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
};
use url::Url;

use crate::input::JsonObject;

const MAX_COMMAND_OUTPUT_BYTES: u64 = 1024 * 1024;
const DATABRICKS_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

/// Failures that the CLI can report without exposing Databricks responses or credentials.
#[derive(Clone, Copy, PartialEq, Eq, Snafu)]
pub(crate) enum Error {
    #[snafu(display(
        "Use uc://catalog.schema.table with an explicit Databricks profile; profile flags are invalid for other targets."
    ))]
    InvalidTableTarget,
    #[snafu(display("Unity Catalog targets currently support inspection only."))]
    UnsupportedScan,
    #[snafu(display(
        "Unity Catalog storage options must contain only supported transport or region settings, without credential or namespace overrides."
    ))]
    InvalidStorageOptions,
    #[snafu(display(
        "Supply an explicit S3 region in the storage options file for this Unity Catalog table."
    ))]
    MissingS3Region,
    #[snafu(display("Install the Databricks CLI and make it available on PATH."))]
    DatabricksCliNotFound,
    #[snafu(display("Could not run the Databricks CLI."))]
    DatabricksCommandFailed,
    #[snafu(display("The Databricks CLI request timed out."))]
    DatabricksCommandTimedOut,
    #[snafu(display("The Databricks CLI returned an invalid or oversized response."))]
    InvalidDatabricksResponse,
    #[snafu(display(
        "Could not fetch Unity Catalog metadata. Check the CLI version, profile login, and table permissions."
    ))]
    TableMetadataRequestFailed,
    #[snafu(display(
        "Could not obtain read credentials. Check the profile login, external access setting, and EXTERNAL USE SCHEMA grant."
    ))]
    ReadCredentialsRequestFailed,
    #[snafu(display(
        "This Unity Catalog table, storage location, or credential type is not supported for direct reads."
    ))]
    UnsupportedDirectRead,
    #[snafu(display(
        "Unity Catalog credentials expired. Run inspection again to obtain fresh credentials."
    ))]
    CredentialsExpired,
    #[snafu(display("Unity Catalog inspection was cancelled."))]
    Cancelled { status: u8 },
}

impl Error {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::InvalidTableTarget => "invalid_uc_reference",
            Self::UnsupportedScan => "unsupported_uc_scan",
            Self::InvalidStorageOptions => "invalid_uc_storage_options",
            Self::MissingS3Region => "uc_s3_region_required",
            Self::DatabricksCliNotFound => "databricks_cli_missing",
            Self::DatabricksCommandFailed => "databricks_cli_failed",
            Self::DatabricksCommandTimedOut => "databricks_cli_timeout",
            Self::InvalidDatabricksResponse => "invalid_databricks_response",
            Self::TableMetadataRequestFailed => "uc_table_request_failed",
            Self::ReadCredentialsRequestFailed => "uc_credential_request_failed",
            Self::UnsupportedDirectRead => "unsupported_uc_table",
            Self::CredentialsExpired => "uc_credentials_expired",
            Self::Cancelled { .. } => "uc_cancelled",
        }
    }

    pub(crate) fn exit_status(&self) -> u8 {
        match self {
            Self::InvalidTableTarget
            | Self::UnsupportedScan
            | Self::InvalidStorageOptions
            | Self::MissingS3Region
            | Self::DatabricksCliNotFound => 2,
            Self::Cancelled { status } => *status,
            _ => 1,
        }
    }

    pub(crate) fn phase(&self) -> &'static str {
        if self.exit_status() == 2 {
            "configuration"
        } else {
            "execution"
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// A validated `uc://catalog.schema.table` target and the profile used to request access.
#[derive(Clone, Copy)]
pub(crate) struct UnityCatalogTableTarget<'a> {
    /// The `catalog.schema.table` name without the `uc://` prefix.
    full_table_name: &'a str,
    /// Name of the Databricks CLI profile to use for authorization.
    databricks_profile: &'a str,
}

impl<'a> UnityCatalogTableTarget<'a> {
    /// Checks the target and profile before any subprocess or storage access.
    ///
    /// Direct file/storage paths without a Databricks profile return `None`.
    /// UC targets require a profile and currently support inspection only.
    pub(crate) fn parse(
        target: &'a str,
        databricks_profile: Option<&'a str>,
        is_scan: bool,
    ) -> Result<Option<Self>, Error> {
        let Some(full_table_name) = target.strip_prefix("uc://") else {
            return if databricks_profile.is_some() || target.to_ascii_lowercase().starts_with("uc:")
            {
                Err(Error::InvalidTableTarget)
            } else {
                Ok(None)
            };
        };
        let databricks_profile = databricks_profile
            .filter(|profile| !profile.trim().is_empty() && !profile.chars().any(char::is_control))
            .ok_or(Error::InvalidTableTarget)?;
        let name_parts = full_table_name.split('.').collect::<Vec<_>>();
        if name_parts.len() != 3
            || name_parts.iter().any(|part| {
                part.is_empty()
                    || part.starts_with('-')
                    || !part
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
            })
        {
            return Err(Error::InvalidTableTarget);
        }
        if is_scan {
            return Err(Error::UnsupportedScan);
        }
        Ok(Some(Self {
            full_table_name,
            databricks_profile,
        }))
    }

    /// Requests temporary READ credentials after fetching and validating the table metadata.
    pub(crate) async fn request_read_access(
        self,
        storage_options: DeltaStorageOptions,
    ) -> Result<AuthorizedTableAccess, Error> {
        // Reject every option that could select another identity before starting a subprocess.
        validate_storage_options(&storage_options, None)?;
        let profile_arg = format!("--profile={}", self.databricks_profile);
        let metadata_json = run_databricks_cli(
            &[
                "tables",
                "get",
                "--include-manifest-capabilities",
                &profile_arg,
                "--output",
                "json",
                "--",
                self.full_table_name,
            ],
            DATABRICKS_COMMAND_TIMEOUT,
        )
        .await
        .map_err(|error| {
            if error == Error::DatabricksCommandFailed {
                Error::TableMetadataRequestFailed
            } else {
                error
            }
        })?;
        let table_metadata =
            serde_json::from_slice::<JsonObject<TableMetadataResponse>>(&metadata_json)
                .map_err(|_| Error::InvalidDatabricksResponse)?
                .0;
        table_metadata.validate_direct_read(self.full_table_name)?;
        let storage_location = TableStorageLocation::parse(&table_metadata.storage_location)?;
        validate_storage_options(&storage_options, Some(storage_location.backend))?;
        let credentials_json = run_databricks_cli(
            &[
                "temporary-table-credentials",
                "generate-temporary-table-credentials",
                "--table-id",
                &table_metadata.table_id,
                "--operation",
                "READ",
                &profile_arg,
                "--output",
                "json",
            ],
            DATABRICKS_COMMAND_TIMEOUT,
        )
        .await
        .map_err(|error| {
            if error == Error::DatabricksCommandFailed {
                Error::ReadCredentialsRequestFailed
            } else {
                error
            }
        })?;
        validate_read_credentials(&credentials_json, storage_location, storage_options)
    }
}

/// Object kind and capability names returned in `securable_kind_manifest`.
#[derive(Deserialize)]
struct TableKindManifest {
    securable_type: String,
    securable_kind: String,
    #[serde(default)]
    capabilities: Vec<String>,
}

/// The column policy field needed to reject reads that would bypass a mask.
///
/// Arrow column types come from the Delta log, so the remaining UC column fields are ignored.
#[derive(Deserialize)]
struct ColumnMaskMetadata {
    mask: Option<Value>,
}

/// Fields from `databricks tables get` needed to check and authorize a direct Delta read.
#[derive(Deserialize)]
struct TableMetadataResponse {
    table_id: String,
    full_name: String,
    table_type: String,
    data_source_format: String,
    storage_location: String,
    securable_kind_manifest: JsonObject<TableKindManifest>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
    #[serde(default)]
    columns: Vec<ColumnMaskMetadata>,
    row_filter: Option<Value>,
    pipeline_id: Option<String>,
    #[serde(default)]
    browse_only: bool,
}

impl TableMetadataResponse {
    /// Checks table identity and rejects policies or table kinds the local reader cannot honor.
    fn validate_direct_read(&self, requested_table_name: &str) -> Result<(), Error> {
        if !self.full_name.eq_ignore_ascii_case(requested_table_name)
            || self.table_id.len() != 36
            || !self.table_id.bytes().enumerate().all(|(index, byte)| {
                if [8, 13, 18, 23].contains(&index) {
                    byte == b'-'
                } else {
                    byte.is_ascii_hexdigit()
                }
            })
        {
            return Err(Error::InvalidDatabricksResponse);
        }
        // This reader cannot apply UC row filters or column masks, or use a commit coordinator.
        if !matches!(self.table_type.as_str(), "MANAGED" | "EXTERNAL")
            || self.data_source_format != "DELTA"
            || self.securable_kind_manifest.0.securable_type != "TABLE"
            || !matches!(
                self.securable_kind_manifest.0.securable_kind.as_str(),
                "TABLE_DELTA" | "TABLE_DELTA_EXTERNAL"
            )
            || self
                .securable_kind_manifest
                .0
                .capabilities
                .iter()
                .any(|capability| capability.is_empty())
            || self.row_filter.is_some()
            || self.columns.iter().any(|column| column.mask.is_some())
            || self.pipeline_id.is_some()
            || self.browse_only
            || self.properties.keys().any(|key| {
                key == "delta.feature.catalogManaged"
                    || key.starts_with("delta.coordinatedCommits.")
            })
        {
            return Err(Error::UnsupportedDirectRead);
        }
        Ok(())
    }
}

/// Storage backends that can use temporary credentials from Unity Catalog.
#[derive(Clone, Copy, PartialEq, Eq)]
enum StorageBackend {
    S3,
    Azure,
}

/// A validated table storage URL, normalized for comparing metadata and credential responses.
struct TableStorageLocation {
    url: Url,
    backend: StorageBackend,
}

impl TableStorageLocation {
    /// Normalizes S3 aliases and Azure HTTPS URLs, and adds a trailing slash to the table root.
    fn parse(raw: &str) -> Result<Self, Error> {
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
        let backend = match url.scheme() {
            "s3" | "s3a" if url.username().is_empty() && url.host_str().is_some() => {
                url.set_scheme("s3")
                    .map_err(|_| Error::InvalidDatabricksResponse)?;
                StorageBackend::S3
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
/// Before the metadata request, `None` accepts settings for either supported backend.
/// Once the table URL is known, settings must match that backend and S3 requires a region.
fn validate_storage_options(
    options: &DeltaStorageOptions,
    backend: Option<StorageBackend>,
) -> Result<(), Error> {
    let mut has_region = false;
    for (key, value) in options {
        let key = key.to_ascii_lowercase();
        let aws = key.parse::<AmazonS3ConfigKey>();
        let azure = key.parse::<AzureConfigKey>();
        let aws_allowed = matches!(
            aws,
            Ok(AmazonS3ConfigKey::Region
                | AmazonS3ConfigKey::DefaultRegion
                | AmazonS3ConfigKey::Endpoint
                | AmazonS3ConfigKey::S3Endpoint
                | AmazonS3ConfigKey::VirtualHostedStyleRequest
                | AmazonS3ConfigKey::UnsignedPayload
                | AmazonS3ConfigKey::Checksum
                | AmazonS3ConfigKey::Client(_))
        );
        let azure_allowed = matches!(
            azure,
            Ok(AzureConfigKey::Endpoint | AzureConfigKey::Client(_))
        );
        // An option alias recognized by both backends must be safe in both.
        if (aws.is_ok() && !aws_allowed)
            || (azure.is_ok() && !azure_allowed)
            || !match backend {
                None => aws_allowed || azure_allowed,
                Some(StorageBackend::S3) => aws_allowed,
                Some(StorageBackend::Azure) => azure_allowed,
            }
        {
            return Err(Error::InvalidStorageOptions);
        }
        if matches!(
            aws,
            Ok(AmazonS3ConfigKey::Region | AmazonS3ConfigKey::DefaultRegion)
        ) {
            if has_region || value.trim().is_empty() {
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

/// Temporary AWS session credentials returned for an S3 table.
#[derive(Deserialize)]
struct AwsTemporaryCredentials {
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

/// Raw response from the Databricks temporary-table-credentials command.
///
/// Unsupported credential fields are retained so their presence causes a rejection.
#[derive(Deserialize)]
struct TemporaryCredentialsResponse {
    url: String,
    /// Credential expiry as milliseconds since the Unix epoch.
    expiration_time: u64,
    aws_temp_credentials: Option<JsonObject<AwsTemporaryCredentials>>,
    azure_user_delegation_sas: Option<JsonObject<AzureSasCredentials>>,
    gcp_oauth_token: Option<Value>,
    azure_aad: Option<Value>,
    r2_temp_credentials: Option<Value>,
}

/// Validated storage access for one UC inspection, including its credential deadline.
///
/// The storage options contain secrets and must never be logged.
pub(crate) struct AuthorizedTableAccess {
    /// Table root shared by the metadata and credential responses.
    pub(crate) storage_url: Url,
    /// Granted credentials combined with the caller's permitted transport settings.
    pub(crate) storage_options: DeltaStorageOptions,
    credentials_expire_at: SystemTime,
}

/// Validates the grant's location, lifetime, and backend before building Core storage options.
fn validate_read_credentials(
    credentials_json: &[u8],
    storage_location: TableStorageLocation,
    mut storage_options: DeltaStorageOptions,
) -> Result<AuthorizedTableAccess, Error> {
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
    if response.gcp_oauth_token.is_some()
        || response.azure_aad.is_some()
        || response.r2_temp_credentials.is_some()
    {
        return Err(Error::UnsupportedDirectRead);
    }
    // Accept exactly one credential type, matching the table URL's storage backend.
    match (
        storage_location.backend,
        response.aws_temp_credentials.map(|value| value.0),
        response.azure_user_delegation_sas.map(|value| value.0),
    ) {
        (StorageBackend::S3, Some(aws), None) => {
            if aws.access_point.is_some() {
                return Err(Error::UnsupportedDirectRead);
            }
            for (key, value) in [
                ("aws_access_key_id", aws.access_key_id),
                ("aws_secret_access_key", aws.secret_access_key),
                ("aws_session_token", aws.session_token),
            ] {
                if value.trim().is_empty() || value.chars().any(char::is_control) {
                    return Err(Error::InvalidDatabricksResponse);
                }
                storage_options.insert(key.into(), value);
            }
        }
        (StorageBackend::Azure, None, Some(azure)) => {
            let sas = azure
                .sas_token
                .strip_prefix('?')
                .unwrap_or(&azure.sas_token);
            if sas.contains(['#', '\n', '\r'])
                || !url::form_urlencoded::parse(sas.as_bytes())
                    .any(|(key, value)| key == "sig" && !value.is_empty())
            {
                return Err(Error::InvalidDatabricksResponse);
            }
            storage_options.insert("azure_storage_sas_key".into(), sas.into());
        }
        _ => return Err(Error::UnsupportedDirectRead),
    }
    Ok(AuthorizedTableAccess {
        storage_url: storage_location.url,
        storage_options,
        credentials_expire_at,
    })
}

impl AuthorizedTableAccess {
    /// Loads the Delta snapshot with Core, checking credential expiry and supported reader protocols.
    pub(crate) async fn load_table(
        &self,
        builder: DeltaTableBuilder,
    ) -> Result<DeltaTable, crate::Error> {
        // shortcut: inspection uses one credential grant; replace with renewable providers when UC scanning is enabled.
        let remaining = self
            .credentials_expire_at
            .duration_since(SystemTime::now())
            .map_err(|_| Error::CredentialsExpired)
            .context(crate::UnityCatalogSnafu)?;
        let deadline = tokio::time::Instant::now()
            .checked_add(remaining)
            .ok_or(Error::InvalidDatabricksResponse)
            .context(crate::UnityCatalogSnafu)?;
        let table = tokio::time::timeout_at(deadline, builder.load_table())
            .await
            .map_err(|_| Error::CredentialsExpired)
            .context(crate::UnityCatalogSnafu)?
            .context(crate::ReaderSnafu)?;
        if self.credentials_expire_at <= SystemTime::now() {
            return Err(crate::Error::UnityCatalog {
                source: Error::CredentialsExpired,
            });
        }
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
}

/// Caps each captured stream; the extra byte distinguishes an oversized response from EOF.
async fn read_bounded_output(reader: impl AsyncRead + Unpin) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_COMMAND_OUTPUT_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| Error::DatabricksCommandFailed)?;
    if bytes.len() as u64 > MAX_COMMAND_OUTPUT_BYTES {
        return Err(Error::InvalidDatabricksResponse);
    }
    Ok(bytes)
}

async fn run_databricks_cli(args: &[&str], timeout: Duration) -> Result<Vec<u8>, Error> {
    run_command_with_timeout(Command::new("databricks").args(args), timeout).await
}

/// Captures stdout, drains stderr, and stops the child on timeout or read failure.
///
/// stderr can contain secrets, so callers receive only a redacted error on failure.
async fn run_command_with_timeout(
    command: &mut Command,
    timeout: Duration,
) -> Result<Vec<u8>, Error> {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Signal cancellation drops this future before the explicit cleanup below.
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Error::DatabricksCliNotFound
            } else {
                Error::DatabricksCommandFailed
            }
        })?;
    let stdout = child.stdout.take().ok_or(Error::DatabricksCommandFailed)?;
    let stderr = child.stderr.take().ok_or(Error::DatabricksCommandFailed)?;
    let result = tokio::time::timeout(timeout, async {
        // Drain both pipes while waiting so a full stderr pipe cannot block the child.
        let (stdout, _, status) = tokio::try_join!(
            read_bounded_output(stdout),
            read_bounded_output(stderr),
            async {
                child
                    .wait()
                    .await
                    .map_err(|_| Error::DatabricksCommandFailed)
            }
        )?;
        if !status.success() {
            return Err(Error::DatabricksCommandFailed);
        }
        Ok(stdout)
    })
    .await
    .unwrap_or(Err(Error::DatabricksCommandTimedOut));
    if result.is_err() {
        let _ = child.kill().await;
    }
    result
}

/// Cancels UC inspection on Ctrl-C or Unix SIGTERM, preserving exit status 130 or 143.
///
/// Signal handlers are installed before polling the work; dropping that work also
/// drops any Databricks child process protected by `kill_on_drop`.
pub(crate) async fn cancel_on_signal<T>(
    work: impl Future<Output = Result<T, crate::Error>>,
) -> Result<T, crate::Error> {
    #[cfg(unix)]
    let cancellation_signal = {
        use tokio::signal::unix::{SignalKind, signal};
        let mut interrupt = signal(SignalKind::interrupt()).context(crate::RuntimeSnafu {
            reason: "interrupt_handler_install_failed",
        })?;
        let mut terminate = signal(SignalKind::terminate()).context(crate::RuntimeSnafu {
            reason: "terminate_handler_install_failed",
        })?;
        #[cfg(target_os = "linux")]
        crate::signals::defer_to_async_cancellation();
        async move {
            Ok::<u8, io::Error>(
                tokio::select! { _ = interrupt.recv() => 130, _ = terminate.recv() => 143 },
            )
        }
    };
    #[cfg(not(unix))]
    let cancellation_signal = async { tokio::signal::ctrl_c().await.map(|()| 130) };
    tokio::select! {
        biased;
        exit_status = cancellation_signal => {
            let exit_status = exit_status.context(crate::RuntimeSnafu {
                reason: "signal_wait_failed",
            })?;
            Err(crate::Error::UnityCatalog {
                source: Error::Cancelled { status: exit_status },
            })
        },
        result = work => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table_metadata_response() -> Value {
        json!({
            "table_id": "12345678-1234-1234-1234-123456789abc",
            "full_name": "main.sales.orders", "table_type": "MANAGED", "data_source_format": "DELTA",
            "storage_location": "s3://bucket/table", "securable_kind_manifest": {
                "securable_type": "TABLE", "securable_kind": "TABLE_DELTA", "capabilities": []
            }
        })
    }

    fn aws_credentials_response() -> Value {
        json!({
            "url": "s3://bucket/table/",
            "expiration_time": (SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + Duration::from_secs(300)).as_millis() as u64,
            "aws_temp_credentials": { "access_key_id": "test-key", "secret_access_key": "test-secret", "session_token": "test-token" }
        })
    }

    #[test]
    fn table_target_requires_valid_name_and_explicit_profile() {
        for target in ["uc://main.sales.orders", "uc://catalog-1.schema_2.table3"] {
            assert!(
                UnityCatalogTableTarget::parse(target, Some("work profile"), false)
                    .unwrap()
                    .is_some()
            );
            assert!(matches!(
                UnityCatalogTableTarget::parse(target, Some("work profile"), true),
                Err(Error::UnsupportedScan)
            ));
        }
        for target in [
            "uc://",
            "uc://main.sales",
            "uc://a.b.c.d",
            "uc://a.b.c/",
            "uc://a.b.c?x",
            "uc://a.b.c#x",
            "uc://a.b.%63",
            "uc://a.b.`c`",
            "uc://-a.b.c",
            "UC://a.b.c",
            "uc:a.b.c",
        ] {
            assert!(
                matches!(
                    UnityCatalogTableTarget::parse(target, Some("work"), false),
                    Err(Error::InvalidTableTarget)
                ),
                "{target}"
            );
        }
        for profile in [None, Some(""), Some("  "), Some("work\nprofile")] {
            assert!(matches!(
                UnityCatalogTableTarget::parse("uc://a.b.c", profile, false),
                Err(Error::InvalidTableTarget)
            ));
        }
        assert!(
            UnityCatalogTableTarget::parse("s3://bucket/table", None, true)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            UnityCatalogTableTarget::parse("/local/table", Some("work"), false),
            Err(Error::InvalidTableTarget)
        ));
    }

    #[test]
    fn admission_rejects_unsupported_and_mismatched_tables() {
        for kind in ["MANAGED", "EXTERNAL"] {
            let mut value = table_metadata_response();
            value["table_type"] = json!(kind);
            serde_json::from_value::<JsonObject<TableMetadataResponse>>(value)
                .unwrap()
                .0
                .validate_direct_read("MAIN.sales.orders")
                .unwrap();
        }
        for (field, invalid) in [
            ("table_id", json!("not-a-uuid")),
            ("full_name", json!("other.schema.table")),
            ("table_type", json!("VIEW")),
            ("table_type", json!("MANAGED_SHALLOW_CLONE")),
            ("data_source_format", json!("ICEBERG")),
            ("row_filter", json!({})),
            ("columns", json!([{"mask": {}}])),
            ("browse_only", json!(true)),
            ("pipeline_id", json!("pipeline")),
            (
                "properties",
                json!({"delta.feature.catalogManaged": "supported"}),
            ),
            (
                "properties",
                json!({"delta.coordinatedCommits.commitCoordinator-preview": "unity-catalog"}),
            ),
            (
                "securable_kind_manifest",
                json!({"securable_type": "TABLE", "securable_kind": "TABLE_VIEW"}),
            ),
        ] {
            let mut value = table_metadata_response();
            value[field] = invalid;
            assert!(
                serde_json::from_value::<JsonObject<TableMetadataResponse>>(value)
                    .unwrap()
                    .0
                    .validate_direct_read("main.sales.orders")
                    .is_err(),
                "{field}"
            );
        }
        for field in ["table_id", "storage_location", "securable_kind_manifest"] {
            let mut value = table_metadata_response();
            value.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<JsonObject<TableMetadataResponse>>(value).is_err());
        }
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
    fn credentials_validate_expiry_location_and_cloud_before_mapping() {
        let value = aws_credentials_response();
        let resolve = |value: &Value| {
            validate_read_credentials(
                &serde_json::to_vec(value).unwrap(),
                TableStorageLocation::parse("s3a://bucket/table").unwrap(),
                DeltaStorageOptions::new(),
            )
        };
        assert_eq!(
            resolve(&value).unwrap().storage_options["aws_session_token"],
            "test-token"
        );
        for (field, invalid) in [
            ("expiration_time", json!(0)),
            ("expiration_time", json!("tomorrow")),
            ("url", json!("s3://other/table")),
            ("url", json!("s3://bucket/table2")),
            ("url", json!("s3://bucket/table?secret-sentinel")),
            (
                "aws_temp_credentials",
                json!({"access_key_id":"test-key", "secret_access_key":"", "session_token":"test-token"}),
            ),
            ("aws_temp_credentials", json!({"access_key_id":"test-key"})),
            (
                "azure_user_delegation_sas",
                json!({"sas_token":"sig=secret"}),
            ),
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
        let result = validate_read_credentials(
            &serde_json::to_vec(&azure).unwrap(),
            TableStorageLocation::parse(
                "abfss://container@account.dfs.core.windows.net/a%20b/%252F/",
            )
            .unwrap(),
            DeltaStorageOptions::new(),
        )
        .unwrap();
        assert_eq!(
            result.storage_options["azure_storage_sas_key"],
            "sv=test&sig=test%2Bsignature&sp=rl"
        );
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

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_output_is_bounded_and_timeout_reaps_child() {
        let directory = tempfile::tempdir().unwrap();
        let pid_file = directory.path().join("pid");
        let result = run_command_with_timeout(
            Command::new("/bin/sh")
                .args(["-c", "printf '%s' $$ > \"$1\"; exec /bin/sleep 60", "sh"])
                .arg(&pid_file),
            Duration::from_millis(200),
        )
        .await;
        assert_eq!(result, Err(Error::DatabricksCommandTimedOut));
        let pid = std::fs::read_to_string(pid_file).unwrap();
        let status = std::process::Command::new("/bin/kill")
            .args(["-0", &pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success(), "timed-out child was not reaped");
        let bytes = vec![b'x'; MAX_COMMAND_OUTPUT_BYTES as usize + 1];
        assert_eq!(
            read_bounded_output(bytes.as_slice()).await,
            Err(Error::InvalidDatabricksResponse)
        );
        assert_eq!(
            run_command_with_timeout(
                Command::new("/bin/sh").args(["-c", "printf secret-sentinel >&2; exit 1"]),
                Duration::from_secs(2)
            )
            .await,
            Err(Error::DatabricksCommandFailed)
        );
    }
}
