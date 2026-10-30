//! Unity Catalog failure categories and filtered Databricks error explanations.

use std::fmt;

use super::databricks_cli::DatabricksErrorDetails;
use snafu::Snafu;

/// Failures from input validation, metadata lookup, and read authorization.
#[derive(Clone, PartialEq, Eq, Snafu)]
pub(crate) enum Error {
    #[snafu(display(
        "Use uc://catalog.schema.table with an explicit Databricks profile; profiles are invalid for other targets."
    ))]
    InvalidTableTarget,
    #[snafu(display(
        "Unity Catalog storage options must contain only supported transport or region settings, without credential or namespace overrides."
    ))]
    InvalidStorageOptions,
    #[snafu(display(
        "Supply an explicit S3 region in the storage options file for this Unity Catalog table."
    ))]
    MissingS3Region,
    #[snafu(display(
        "Install the Databricks CLI and make the databricks executable available on PATH, then retry with the same --databricks-profile or manifest databricks_profile."
    ))]
    DatabricksCliNotFound,
    #[snafu(display("Could not run the Databricks CLI{details}"))]
    DatabricksCommandFailed { details: DatabricksErrorDetails },
    #[snafu(display("The Databricks CLI request timed out."))]
    DatabricksCommandTimedOut,
    #[snafu(display("The Databricks CLI returned an invalid or oversized response."))]
    InvalidDatabricksResponse,
    #[snafu(display("Could not fetch Unity Catalog metadata{details}"))]
    TableMetadataRequestFailed { details: DatabricksErrorDetails },
    #[snafu(display("Could not obtain Unity Catalog read credentials{details}"))]
    ReadCredentialsRequestFailed { details: DatabricksErrorDetails },
    #[snafu(display(
        "This Unity Catalog table, storage location, or credential type is not supported for direct reads."
    ))]
    UnsupportedDirectRead,
    #[snafu(display(
        "This workspace does not advertise Delta API 1.0 table reads required for catalog-managed tables. Additional grants do not add protocol support."
    ))]
    UnsupportedCatalogProtocol,
    #[snafu(display(
        "Unity Catalog did not publish a Delta representation. Enable pipelines.externalMetadata.enabled and refresh the dataset. This external-access path does not support catalog commits."
    ))]
    PipelineExternalAccessUnavailable,
    #[snafu(display(
        "Unity Catalog materialized views and streaming tables support only the latest published snapshot. Omit --table-version or the table_version entry."
    ))]
    PipelineTimeTravelUnsupported,
    #[snafu(display(
        "Unity Catalog materialized views and streaming tables cannot replay pinned snapshots. Save the result with --output without --manifest."
    ))]
    PipelineReplayUnsupported,
    #[snafu(display(
        "Unity Catalog returned expired credentials. Check the profile login and system clock, then retry."
    ))]
    CredentialsExpired,
    #[snafu(display("Unity Catalog command was cancelled."))]
    Cancelled { status: u8 },
}

impl Error {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::InvalidTableTarget => "invalid_uc_reference",
            Self::InvalidStorageOptions => "invalid_uc_storage_options",
            Self::MissingS3Region => "uc_s3_region_required",
            Self::DatabricksCliNotFound => "databricks_cli_missing",
            Self::DatabricksCommandFailed { .. } => "databricks_cli_failed",
            Self::DatabricksCommandTimedOut => "databricks_cli_timeout",
            Self::InvalidDatabricksResponse => "invalid_databricks_response",
            Self::TableMetadataRequestFailed { .. } => "uc_table_request_failed",
            Self::ReadCredentialsRequestFailed { .. } => "uc_credential_request_failed",
            Self::UnsupportedDirectRead => "unsupported_uc_table",
            Self::UnsupportedCatalogProtocol => "unsupported_uc_catalog_protocol",
            Self::PipelineExternalAccessUnavailable => "uc_pipeline_external_access_unavailable",
            Self::PipelineTimeTravelUnsupported => "uc_pipeline_time_travel_unsupported",
            Self::PipelineReplayUnsupported => "uc_pipeline_replay_unsupported",
            Self::CredentialsExpired => "uc_credentials_expired",
            Self::Cancelled { .. } => "uc_cancelled",
        }
    }

    pub(crate) fn command_error_details(&self) -> Option<&DatabricksErrorDetails> {
        match self {
            Self::TableMetadataRequestFailed { details }
            | Self::DatabricksCommandFailed { details }
            | Self::ReadCredentialsRequestFailed { details } => Some(details),
            _ => None,
        }
    }

    pub(crate) fn source_kind(&self) -> Option<&'static str> {
        self.command_error_details()
            .and_then(|details| details.source_kind)
    }

    pub(crate) fn reason(&self) -> &'static str {
        if let Some(details) = self.command_error_details() {
            match details.operation {
                Some("spawn") => return "databricks_cli_spawn_failed",
                Some("read_stdout") => return "databricks_cli_stdout_read_failed",
                Some("read_stderr") => return "databricks_cli_stderr_read_failed",
                Some("wait") => return "databricks_cli_wait_failed",
                Some("worker") => return "databricks_cli_worker_failed",
                _ => {}
            }
        }
        match self.source_kind() {
            Some("unauthenticated") => "uc_authentication_failed",
            Some("permission_denied") => "uc_permission_denied",
            _ => self.code(),
        }
    }

    pub(crate) fn exit_status(&self) -> u8 {
        match self {
            Self::InvalidTableTarget
            | Self::InvalidStorageOptions
            | Self::MissingS3Region
            | Self::PipelineTimeTravelUnsupported
            | Self::PipelineReplayUnsupported
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
