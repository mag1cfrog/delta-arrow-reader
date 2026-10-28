//! Unity Catalog failure codes and messages that omit table identifiers and credentials.

use std::fmt;

use snafu::Snafu;

/// Failures that the CLI can report without exposing Databricks responses or credentials.
#[derive(Clone, Copy, PartialEq, Eq, Snafu)]
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
