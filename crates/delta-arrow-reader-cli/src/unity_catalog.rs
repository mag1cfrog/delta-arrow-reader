//! Databricks CLI authorization for local Delta reads and SQL queries.
//!
//! The CLI supplies table metadata and temporary storage credentials. Core then reads
//! the Delta log and data files directly. Table identifiers, credentials, and raw command output
//! must stay out of diagnostics. Raw responses and credential values omit `Debug`;
//! credential caches and providers expose only redacted `Debug` output.

mod credential_cache;
mod databricks_cli;
mod storage_access;
mod storage_location;
mod table_metadata;
mod temporary_credentials;

pub(crate) use databricks_cli::{
    DATABRICKS_COMMAND_TIMEOUT, MAX_COMMAND_OUTPUT_BYTES, cancel_pending_requests,
};

use std::fmt;

use delta_arrow_reader::DeltaStorageOptions;
use snafu::Snafu;

pub(crate) use storage_access::AuthorizedTableAccess;
use storage_location::TableStorageLocation;
use table_metadata::TableMetadataResponse;
use temporary_credentials::parse_temporary_credentials;

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

/// A validated `uc://catalog.schema.table` target and the profile used to request access.
pub(crate) struct UnityCatalogTableTarget {
    /// The `catalog.schema.table` name without the `uc://` prefix.
    full_table_name: String,
    /// Name of the Databricks CLI profile to use for authorization.
    databricks_profile: String,
}

/// Rejects identity overrides before any table's metadata or credentials are requested.
pub(crate) fn validate_storage_options(options: &DeltaStorageOptions) -> Result<(), Error> {
    storage_location::validate_storage_options(options, None)
}

impl UnityCatalogTableTarget {
    /// Checks the target and profile before any subprocess or storage access.
    ///
    /// Direct file/storage paths without a Databricks profile return `None`.
    /// UC targets require an explicit profile.
    pub(crate) fn parse(
        target: &str,
        databricks_profile: Option<&str>,
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
        Ok(Some(Self {
            full_table_name: full_table_name.to_owned(),
            databricks_profile: databricks_profile.to_owned(),
        }))
    }

    /// Requests temporary READ credentials after fetching and validating the table metadata.
    pub(crate) async fn request_read_access(
        self,
        storage_options: DeltaStorageOptions,
    ) -> Result<AuthorizedTableAccess, Error> {
        // Reject identity overrides before starting a subprocess, even before the backend is known.
        validate_storage_options(&storage_options)?;
        let metadata_json =
            databricks_cli::get_table_metadata(&self.databricks_profile, &self.full_table_name)
                .await?;
        let table_metadata = TableMetadataResponse::parse(&metadata_json, &self.full_table_name)?;
        let storage_location = TableStorageLocation::parse(&table_metadata.storage_location)?;
        storage_location::validate_storage_options(
            &storage_options,
            Some(storage_location.backend),
        )?;
        let credentials_json = databricks_cli::generate_read_credentials(
            &self.databricks_profile,
            &table_metadata.table_id,
        )
        .await?;
        let credentials = parse_temporary_credentials(&credentials_json, &storage_location)?;
        AuthorizedTableAccess::new(
            storage_location,
            storage_options,
            self.databricks_profile,
            table_metadata.table_id,
            credentials,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_target_requires_valid_name_and_explicit_profile() {
        for target in ["uc://main.sales.orders", "uc://catalog-1.schema_2.table3"] {
            assert!(
                UnityCatalogTableTarget::parse(target, Some("work profile"))
                    .unwrap()
                    .is_some()
            );
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
                    UnityCatalogTableTarget::parse(target, Some("work")),
                    Err(Error::InvalidTableTarget)
                ),
                "{target}"
            );
        }
        for profile in [None, Some(""), Some("  "), Some("work\nprofile")] {
            assert!(matches!(
                UnityCatalogTableTarget::parse("uc://a.b.c", profile),
                Err(Error::InvalidTableTarget)
            ));
        }
        assert!(
            UnityCatalogTableTarget::parse("s3://bucket/table", None)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            UnityCatalogTableTarget::parse("/local/table", Some("work")),
            Err(Error::InvalidTableTarget)
        ));
    }
}
