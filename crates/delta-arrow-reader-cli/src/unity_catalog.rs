//! Databricks CLI authorization for local Delta metadata reads.

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
use snafu::ResultExt;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
};
use url::Url;

use crate::input::JsonObject;

const MAX_RESPONSE_BYTES: u64 = 1024 * 1024;
const COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    Reference,
    Scan,
    Options,
    Region,
    MissingCli,
    Process,
    Timeout,
    Response,
    Table,
    Credentials,
    Unsupported,
    Expired,
    Cancelled(u8),
}

impl Error {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Reference => "invalid_uc_reference",
            Self::Scan => "unsupported_uc_scan",
            Self::Options => "invalid_uc_storage_options",
            Self::Region => "uc_s3_region_required",
            Self::MissingCli => "databricks_cli_missing",
            Self::Process => "databricks_cli_failed",
            Self::Timeout => "databricks_cli_timeout",
            Self::Response => "invalid_databricks_response",
            Self::Table => "uc_table_request_failed",
            Self::Credentials => "uc_credential_request_failed",
            Self::Unsupported => "unsupported_uc_table",
            Self::Expired => "uc_credentials_expired",
            Self::Cancelled(_) => "uc_cancelled",
        }
    }

    pub(crate) fn exit_status(&self) -> u8 {
        match self {
            Self::Reference | Self::Scan | Self::Options | Self::Region | Self::MissingCli => 2,
            Self::Cancelled(status) => *status,
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Reference => "Use uc://catalog.schema.table with an explicit Databricks profile; profile flags are invalid for other targets.",
            Self::Scan => "Unity Catalog targets currently support inspection only.",
            Self::Options => "Unity Catalog storage options must contain only supported transport or region settings, without credential or namespace overrides.",
            Self::Region => "Supply an explicit S3 region in the storage options file for this Unity Catalog table.",
            Self::MissingCli => "Install the Databricks CLI and make it available on PATH.",
            Self::Process => "Could not run the Databricks CLI.",
            Self::Timeout => "The Databricks CLI request timed out.",
            Self::Response => "The Databricks CLI returned an invalid or oversized response.",
            Self::Table => "Could not fetch Unity Catalog metadata. Check the CLI version, profile login, and table permissions.",
            Self::Credentials => "Could not obtain read credentials. Check the profile login, external access setting, and EXTERNAL USE SCHEMA grant.",
            Self::Unsupported => "This Unity Catalog table, storage location, or credential type is not supported for direct reads.",
            Self::Expired => "Unity Catalog credentials expired. Run inspection again to obtain fresh credentials.",
            Self::Cancelled(_) => "Unity Catalog inspection was cancelled.",
        })
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Copy)]
pub(crate) struct Reference<'a> {
    name: &'a str,
    profile: &'a str,
}

impl<'a> Reference<'a> {
    pub(crate) fn parse(
        target: &'a str,
        profile: Option<&'a str>,
        scan: bool,
    ) -> Result<Option<Self>, Error> {
        let Some(name) = target.strip_prefix("uc://") else {
            return if profile.is_some() || target.to_ascii_lowercase().starts_with("uc:") {
                Err(Error::Reference)
            } else {
                Ok(None)
            };
        };
        let profile = profile
            .filter(|p| !p.trim().is_empty() && !p.chars().any(char::is_control))
            .ok_or(Error::Reference)?;
        let parts = name.split('.').collect::<Vec<_>>();
        if parts.len() != 3
            || parts.iter().any(|part| {
                part.is_empty()
                    || part.starts_with('-')
                    || !part
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            })
        {
            return Err(Error::Reference);
        }
        if scan {
            return Err(Error::Scan);
        }
        Ok(Some(Self { name, profile }))
    }

    pub(crate) async fn resolve(
        self,
        options: DeltaStorageOptions,
    ) -> Result<ResolvedTable, Error> {
        // Reject every option that could select another identity before starting a subprocess.
        validate_options(&options, None)?;
        let profile = format!("--profile={}", self.profile);
        let bytes = run_cli(
            &[
                "tables",
                "get",
                "--include-manifest-capabilities",
                &profile,
                "--output",
                "json",
                "--",
                self.name,
            ],
            COMMAND_TIMEOUT,
        )
        .await
        .map_err(|error| {
            if error == Error::Process {
                Error::Table
            } else {
                error
            }
        })?;
        let table = serde_json::from_slice::<JsonObject<TableInfo>>(&bytes)
            .map_err(|_| Error::Response)?
            .0;
        table.validate(self.name)?;
        let location = Location::parse(&table.storage_location)?;
        validate_options(&options, Some(location.cloud))?;
        let bytes = run_cli(
            &[
                "temporary-table-credentials",
                "generate-temporary-table-credentials",
                "--table-id",
                &table.table_id,
                "--operation",
                "READ",
                &profile,
                "--output",
                "json",
            ],
            COMMAND_TIMEOUT,
        )
        .await
        .map_err(|error| {
            if error == Error::Process {
                Error::Credentials
            } else {
                error
            }
        })?;
        credentials(&bytes, location, options)
    }
}

#[derive(Deserialize)]
struct Manifest {
    securable_type: String,
    securable_kind: String,
    #[serde(default)]
    capabilities: Vec<String>,
}

#[derive(Deserialize)]
struct Column {
    mask: Option<Value>,
}

#[derive(Deserialize)]
struct TableInfo {
    table_id: String,
    full_name: String,
    table_type: String,
    data_source_format: String,
    storage_location: String,
    securable_kind_manifest: JsonObject<Manifest>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
    #[serde(default)]
    columns: Vec<Column>,
    row_filter: Option<Value>,
    pipeline_id: Option<String>,
    #[serde(default)]
    browse_only: bool,
}

impl TableInfo {
    fn validate(&self, requested: &str) -> Result<(), Error> {
        if !self.full_name.eq_ignore_ascii_case(requested)
            || self.table_id.len() != 36
            || !self.table_id.bytes().enumerate().all(|(i, b)| {
                if [8, 13, 18, 23].contains(&i) {
                    b == b'-'
                } else {
                    b.is_ascii_hexdigit()
                }
            })
        {
            return Err(Error::Response);
        }
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
                .any(|s| s.is_empty())
            || self.row_filter.is_some()
            || self.columns.iter().any(|c| c.mask.is_some())
            || self.pipeline_id.is_some()
            || self.browse_only
            || self.properties.keys().any(|key| {
                key == "delta.feature.catalogManaged"
                    || key.starts_with("delta.coordinatedCommits.")
            })
        {
            return Err(Error::Unsupported);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Cloud {
    S3,
    Azure,
}

struct Location {
    url: Url,
    cloud: Cloud,
}

impl Location {
    fn parse(raw: &str) -> Result<Self, Error> {
        let mut url = Url::parse(raw).map_err(|_| Error::Response)?;
        if url.query().is_some()
            || url.fragment().is_some()
            || url.password().is_some()
            || url.port().is_some()
            || url.path().starts_with("//")
            || raw.chars().any(char::is_control)
        {
            return Err(Error::Response);
        }
        let cloud = match url.scheme() {
            "s3" | "s3a" if url.username().is_empty() && url.host_str().is_some() => {
                url.set_scheme("s3").map_err(|_| Error::Response)?;
                Cloud::S3
            }
            "abfss"
                if !url.username().is_empty()
                    && url
                        .host_str()
                        .is_some_and(|h| h.ends_with(".dfs.core.windows.net")) =>
            {
                Cloud::Azure
            }
            "https" if url.username().is_empty() => {
                let account = url
                    .host_str()
                    .and_then(|h| {
                        h.strip_suffix(".dfs.core.windows.net")
                            .or_else(|| h.strip_suffix(".blob.core.windows.net"))
                    })
                    .filter(|s| !s.is_empty())
                    .ok_or(Error::Unsupported)?;
                let path = url.path().trim_start_matches('/');
                let (container, path) = path.split_once('/').unwrap_or((path, ""));
                if container.is_empty() {
                    return Err(Error::Response);
                }
                url = Url::parse(&format!(
                    "abfss://{container}@{account}.dfs.core.windows.net/{path}"
                ))
                .map_err(|_| Error::Response)?;
                Cloud::Azure
            }
            _ => return Err(Error::Unsupported),
        };
        ObjectStoreScheme::parse(&url).map_err(|_| Error::Response)?;
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        Ok(Self { url, cloud })
    }
}

fn validate_options(options: &DeltaStorageOptions, cloud: Option<Cloud>) -> Result<(), Error> {
    let mut region = false;
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
        if (aws.is_ok() && !aws_allowed)
            || (azure.is_ok() && !azure_allowed)
            || !match cloud {
                None => aws_allowed || azure_allowed,
                Some(Cloud::S3) => aws_allowed,
                Some(Cloud::Azure) => azure_allowed,
            }
        {
            return Err(Error::Options);
        }
        if matches!(
            aws,
            Ok(AmazonS3ConfigKey::Region | AmazonS3ConfigKey::DefaultRegion)
        ) {
            if region || value.trim().is_empty() {
                return Err(Error::Options);
            }
            region = true;
        }
    }
    if cloud == Some(Cloud::S3) && !region {
        return Err(Error::Region);
    }
    Ok(())
}

#[derive(Deserialize)]
struct AwsCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    access_point: Option<String>,
}
#[derive(Deserialize)]
struct AzureCredentials {
    sas_token: String,
}
#[derive(Deserialize)]
struct Credentials {
    url: String,
    expiration_time: u64,
    aws_temp_credentials: Option<JsonObject<AwsCredentials>>,
    azure_user_delegation_sas: Option<JsonObject<AzureCredentials>>,
    gcp_oauth_token: Option<Value>,
    azure_aad: Option<Value>,
    r2_temp_credentials: Option<Value>,
}

pub(crate) struct ResolvedTable {
    pub(crate) location: Url,
    pub(crate) storage_options: DeltaStorageOptions,
    expires: SystemTime,
}

fn credentials(
    bytes: &[u8],
    location: Location,
    mut options: DeltaStorageOptions,
) -> Result<ResolvedTable, Error> {
    let response = serde_json::from_slice::<JsonObject<Credentials>>(bytes)
        .map_err(|_| Error::Response)?
        .0;
    if Location::parse(&response.url)?.url != location.url {
        return Err(Error::Response);
    }
    let expires = UNIX_EPOCH
        .checked_add(Duration::from_millis(response.expiration_time))
        .ok_or(Error::Response)?;
    if expires <= SystemTime::now() {
        return Err(Error::Expired);
    }
    if response.gcp_oauth_token.is_some()
        || response.azure_aad.is_some()
        || response.r2_temp_credentials.is_some()
    {
        return Err(Error::Unsupported);
    }
    match (
        location.cloud,
        response.aws_temp_credentials.map(|value| value.0),
        response.azure_user_delegation_sas.map(|value| value.0),
    ) {
        (Cloud::S3, Some(aws), None) => {
            if aws.access_point.is_some() {
                return Err(Error::Unsupported);
            }
            for (key, value) in [
                ("aws_access_key_id", aws.access_key_id),
                ("aws_secret_access_key", aws.secret_access_key),
                ("aws_session_token", aws.session_token),
            ] {
                if value.trim().is_empty() || value.chars().any(char::is_control) {
                    return Err(Error::Response);
                }
                options.insert(key.into(), value);
            }
        }
        (Cloud::Azure, None, Some(azure)) => {
            let sas = azure
                .sas_token
                .strip_prefix('?')
                .unwrap_or(&azure.sas_token);
            if sas.contains(['#', '\n', '\r'])
                || !url::form_urlencoded::parse(sas.as_bytes())
                    .any(|(key, value)| key == "sig" && !value.is_empty())
            {
                return Err(Error::Response);
            }
            options.insert("azure_storage_sas_key".into(), sas.into());
        }
        _ => return Err(Error::Unsupported),
    }
    Ok(ResolvedTable {
        location: location.url,
        storage_options: options,
        expires,
    })
}

impl ResolvedTable {
    pub(crate) async fn load(
        &self,
        builder: DeltaTableBuilder,
    ) -> Result<DeltaTable, crate::Error> {
        // shortcut: inspection uses one credential grant; replace with renewable providers when UC scanning is enabled.
        let remaining = self
            .expires
            .duration_since(SystemTime::now())
            .map_err(|_| Error::Expired)
            .context(crate::UnityCatalogSnafu)?;
        let deadline = tokio::time::Instant::now()
            .checked_add(remaining)
            .ok_or(Error::Response)
            .context(crate::UnityCatalogSnafu)?;
        let table = tokio::time::timeout_at(deadline, builder.load_table())
            .await
            .map_err(|_| Error::Expired)
            .context(crate::UnityCatalogSnafu)?
            .context(crate::ReaderSnafu)?;
        if self.expires <= SystemTime::now() {
            return Err(crate::Error::UnityCatalog {
                source: Error::Expired,
            });
        }
        let protocol = table.protocol();
        if !matches!(protocol.min_reader_version(), 1..=3)
            || protocol.first_unsupported_reader_feature().is_some()
            || protocol
                .writer_features()
                .iter()
                .any(|f| f == "catalogManaged")
        {
            return Err(crate::Error::UnityCatalog {
                source: Error::Unsupported,
            });
        }
        Ok(table)
    }
}

async fn bounded_output(reader: impl AsyncRead + Unpin) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_RESPONSE_BYTES + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| Error::Process)?;
    if bytes.len() as u64 > MAX_RESPONSE_BYTES {
        return Err(Error::Response);
    }
    Ok(bytes)
}

async fn run_cli(args: &[&str], timeout: Duration) -> Result<Vec<u8>, Error> {
    run_command(Command::new("databricks").args(args), timeout).await
}

async fn run_command(command: &mut Command, timeout: Duration) -> Result<Vec<u8>, Error> {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Error::MissingCli
            } else {
                Error::Process
            }
        })?;
    let stdout = child.stdout.take().ok_or(Error::Process)?;
    let stderr = child.stderr.take().ok_or(Error::Process)?;
    let result = tokio::time::timeout(timeout, async {
        let (stdout, _, status) =
            tokio::try_join!(bounded_output(stdout), bounded_output(stderr), async {
                child.wait().await.map_err(|_| Error::Process)
            })?;
        if !status.success() {
            return Err(Error::Process);
        }
        Ok(stdout)
    })
    .await
    .unwrap_or(Err(Error::Timeout));
    if result.is_err() {
        let _ = child.kill().await;
    }
    result
}

pub(crate) async fn cancellable<T>(
    work: impl Future<Output = Result<T, crate::Error>>,
) -> Result<T, crate::Error> {
    #[cfg(unix)]
    let stop = {
        use tokio::signal::unix::{SignalKind, signal};
        let mut interrupt = signal(SignalKind::interrupt()).context(crate::RuntimeSnafu {
            reason: "interrupt_handler_install_failed",
        })?;
        let mut terminate = signal(SignalKind::terminate()).context(crate::RuntimeSnafu {
            reason: "terminate_handler_install_failed",
        })?;
        async move {
            Ok::<u8, io::Error>(
                tokio::select! { _ = interrupt.recv() => 130, _ = terminate.recv() => 143 },
            )
        }
    };
    #[cfg(not(unix))]
    let stop = async { tokio::signal::ctrl_c().await.map(|()| 130) };
    tokio::select! {
        biased;
        status = stop => Err(crate::Error::UnityCatalog { source: Error::Cancelled(status.context(crate::RuntimeSnafu { reason: "signal_wait_failed" })?) }),
        result = work => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table() -> Value {
        json!({
            "table_id": "12345678-1234-1234-1234-123456789abc",
            "full_name": "main.sales.orders", "table_type": "MANAGED", "data_source_format": "DELTA",
            "storage_location": "s3://bucket/table", "securable_kind_manifest": {
                "securable_type": "TABLE", "securable_kind": "TABLE_DELTA", "capabilities": []
            }
        })
    }

    fn aws_credentials() -> Value {
        json!({
            "url": "s3://bucket/table/",
            "expiration_time": (SystemTime::now().duration_since(UNIX_EPOCH).unwrap() + Duration::from_secs(300)).as_millis() as u64,
            "aws_temp_credentials": { "access_key_id": "test-key", "secret_access_key": "test-secret", "session_token": "test-token" }
        })
    }

    #[test]
    fn reference_and_profile_are_explicit_and_unambiguous() {
        for target in ["uc://main.sales.orders", "uc://catalog-1.schema_2.table3"] {
            assert!(
                Reference::parse(target, Some("work profile"), false)
                    .unwrap()
                    .is_some()
            );
            assert!(matches!(
                Reference::parse(target, Some("work profile"), true),
                Err(Error::Scan)
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
                    Reference::parse(target, Some("work"), false),
                    Err(Error::Reference)
                ),
                "{target}"
            );
        }
        for profile in [None, Some(""), Some("  "), Some("work\nprofile")] {
            assert!(matches!(
                Reference::parse("uc://a.b.c", profile, false),
                Err(Error::Reference)
            ));
        }
        assert!(
            Reference::parse("s3://bucket/table", None, true)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            Reference::parse("/local/table", Some("work"), false),
            Err(Error::Reference)
        ));
    }

    #[test]
    fn admission_rejects_unsupported_and_mismatched_tables() {
        for kind in ["MANAGED", "EXTERNAL"] {
            let mut value = table();
            value["table_type"] = json!(kind);
            serde_json::from_value::<JsonObject<TableInfo>>(value)
                .unwrap()
                .0
                .validate("MAIN.sales.orders")
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
            let mut value = table();
            value[field] = invalid;
            assert!(
                serde_json::from_value::<JsonObject<TableInfo>>(value)
                    .unwrap()
                    .0
                    .validate("main.sales.orders")
                    .is_err(),
                "{field}"
            );
        }
        for field in ["table_id", "storage_location", "securable_kind_manifest"] {
            let mut value = table();
            value.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<JsonObject<TableInfo>>(value).is_err());
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
                validate_options(&options, None),
                Err(Error::Options),
                "{key}"
            );
        }
        let mut options = DeltaStorageOptions::from([
            ("AWS_REGION".into(), "eu-west-1".into()),
            ("aws_allow_http".into(), "true".into()),
            ("aws_endpoint".into(), "http://127.0.0.1:1234".into()),
        ]);
        validate_options(&options, Some(Cloud::S3)).unwrap();
        options.insert("default_region".into(), "us-east-1".into());
        assert_eq!(
            validate_options(&options, Some(Cloud::S3)),
            Err(Error::Options)
        );
        assert_eq!(
            validate_options(&DeltaStorageOptions::new(), Some(Cloud::S3)),
            Err(Error::Region)
        );
        validate_options(&DeltaStorageOptions::new(), Some(Cloud::Azure)).unwrap();
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
            assert_eq!(Location::parse(location).unwrap().url.as_str(), expected);
        }
        for location in [
            "https://account.blob.core.windows.net",
            "https://account.dfs.core.windows.net/",
        ] {
            assert!(Location::parse(location).is_err(), "{location}");
        }
    }

    #[test]
    fn credentials_validate_expiry_location_and_cloud_before_mapping() {
        let value = aws_credentials();
        let resolve = |value: &Value| {
            credentials(
                &serde_json::to_vec(value).unwrap(),
                Location::parse("s3a://bucket/table").unwrap(),
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
        let result = credentials(
            &serde_json::to_vec(&azure).unwrap(),
            Location::parse("abfss://container@account.dfs.core.windows.net/a%20b/%252F/").unwrap(),
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
            assert!(Location::parse(location).is_err(), "{location}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn subprocess_output_is_bounded_and_timeout_reaps_child() {
        let directory = tempfile::tempdir().unwrap();
        let pid_file = directory.path().join("pid");
        let result = run_command(
            Command::new("/bin/sh")
                .args(["-c", "printf '%s' $$ > \"$1\"; exec /bin/sleep 60", "sh"])
                .arg(&pid_file),
            Duration::from_millis(200),
        )
        .await;
        assert_eq!(result, Err(Error::Timeout));
        let pid = std::fs::read_to_string(pid_file).unwrap();
        let status = std::process::Command::new("/bin/kill")
            .args(["-0", &pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert!(!status.success(), "timed-out child was not reaped");
        let bytes = vec![b'x'; MAX_RESPONSE_BYTES as usize + 1];
        assert_eq!(bounded_output(bytes.as_slice()).await, Err(Error::Response));
        assert_eq!(
            run_command(
                Command::new("/bin/sh").args(["-c", "printf secret-sentinel >&2; exit 1"]),
                Duration::from_secs(2)
            )
            .await,
            Err(Error::Process)
        );
    }
}
