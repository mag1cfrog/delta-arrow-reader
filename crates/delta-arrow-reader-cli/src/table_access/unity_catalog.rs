//! Databricks CLI authorization for local Delta reads and SQL queries.
//!
//! The CLI supplies table metadata and temporary storage credentials. Core then reads
//! the Delta log and data files directly. Failed requests retain a filtered error
//! explanation; successful credential responses never enter diagnostics. Raw responses
//! and credential values omit `Debug`; caches and providers omit their stored credentials.

mod catalog_snapshot;
mod credential_cache;
mod databricks_cli;
mod error;
mod storage_access;
mod storage_location;
mod table_metadata;
mod temporary_credentials;

pub(crate) use databricks_cli::{
    DATABRICKS_COMMAND_TIMEOUT, MAX_COMMAND_OUTPUT_BYTES, cancel_pending_requests,
};

use delta_arrow_reader::{DeltaSnapshotSelection, DeltaStorageOptions};

use databricks_cli::ReadCredentialTarget;
pub(crate) use error::Error;
pub(crate) use storage_access::AuthorizedTableAccess;
use table_metadata::TableMetadataResponse;
use temporary_credentials::{parse_pipeline_credentials, parse_temporary_credentials};

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

    /// Requests read-only credentials after fetching and validating the table metadata.
    pub(crate) async fn request_read_access(
        self,
        storage_options: DeltaStorageOptions,
        snapshot_selection: DeltaSnapshotSelection,
    ) -> Result<AuthorizedTableAccess, Error> {
        // Reject identity overrides before starting a subprocess, even before the backend is known.
        validate_storage_options(&storage_options)?;
        let metadata_json =
            databricks_cli::get_table_metadata(&self.databricks_profile, &self.full_table_name)
                .await?;
        let table_metadata = TableMetadataResponse::parse(&metadata_json, &self.full_table_name)?;
        let latest_only = table_metadata.is_pipeline_dataset();
        if latest_only && matches!(snapshot_selection, DeltaSnapshotSelection::Version(_)) {
            return Err(Error::PipelineTimeTravelUnsupported);
        }
        let storage_location = if latest_only {
            let external = databricks_cli::get_catalog_api_response(
                &self.databricks_profile,
                &format!("/api/2.1/unity-catalog/tables/{}?read_materialized_view_as_managed=true&read_streaming_table_as_managed=true", self.full_table_name),
            ).await?;
            table_metadata.parse_external_delta_location(&external)?
        } else {
            table_metadata.storage_location()?
        };
        storage_location::validate_storage_options(
            &storage_options,
            Some(storage_location.backend),
        )?;
        let catalog_snapshot = if table_metadata.is_catalog_managed() {
            Some(
                catalog_snapshot::load_catalog_snapshot(
                    &self.databricks_profile,
                    &self.full_table_name,
                    &table_metadata,
                    &storage_location,
                )
                .await?,
            )
        } else {
            None
        };
        let credential_target = if table_metadata.uses_path_credentials() {
            ReadCredentialTarget::ExternalPath(storage_location.as_url().clone())
        } else {
            ReadCredentialTarget::TableId(table_metadata.table_id)
        };
        let credentials_json =
            databricks_cli::generate_read_credentials(&self.databricks_profile, &credential_target)
                .await?;
        let (credentials, credential_scope) = if latest_only {
            parse_pipeline_credentials(&credentials_json, &storage_location)?
        } else {
            (
                parse_temporary_credentials(&credentials_json, &storage_location)?,
                storage_location.clone(),
            )
        };
        let credential_cache = credential_cache::TableCredentialCache::new(
            self.databricks_profile,
            credential_target,
            credential_scope,
            credentials,
        )?;
        Ok(AuthorizedTableAccess::new(
            storage_location,
            storage_options,
            credential_cache,
            catalog_snapshot,
            latest_only,
        ))
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
