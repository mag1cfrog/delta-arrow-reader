//! Load an authorized Delta snapshot and preserve UC failures from subsequent reads.

use std::sync::Arc;

use delta_arrow_reader::{
    DeltaCatalogSnapshot, DeltaSnapshotSelection, DeltaStorageOptions, DeltaTable,
    DeltaTableBuilder, WarmupMode,
};
use url::Url;

use super::{
    Error, credential_cache::TableCredentialCache, storage_location::TableStorageLocation,
};
use crate::diagnostics::Error as CliError;

/// Validated storage access for one UC table binding, including its renewable credentials.
///
/// The location and credentials must never be logged.
pub(crate) struct AuthorizedTableAccess {
    /// Delta log location returned by the catalog; pipeline datasets can publish a separate root.
    storage_url: Url,
    /// The caller's permitted transport and region settings.
    storage_options: DeltaStorageOptions,
    /// Retains the redacted refresh failure even if a dependency flattens its error source.
    credential_cache: Arc<TableCredentialCache>,
    catalog_snapshot: Option<DeltaCatalogSnapshot>,
    latest_only: bool,
}

impl AuthorizedTableAccess {
    /// Connects the published snapshot location to credentials for its validated storage scope.
    pub(super) fn new(
        storage_location: TableStorageLocation,
        storage_options: DeltaStorageOptions,
        credential_cache: Arc<TableCredentialCache>,
        catalog_snapshot: Option<DeltaCatalogSnapshot>,
        latest_only: bool,
    ) -> Self {
        let storage_url = storage_location.as_url().clone();
        Self {
            storage_url,
            storage_options,
            credential_cache,
            catalog_snapshot,
            latest_only,
        }
    }

    pub(crate) fn supports_time_travel(&self) -> bool {
        !self.latest_only
    }

    pub(crate) fn validate_versioned_replay(&self) -> Result<(), Error> {
        if self.latest_only {
            return Err(Error::PipelineReplayUnsupported);
        }
        Ok(())
    }

    /// Loads the requested snapshot with credentials renewed on demand by the storage clients.
    ///
    /// UC reads require a protocol supported by the local reader.
    pub(crate) async fn load_table(
        &self,
        snapshot_selection: DeltaSnapshotSelection,
    ) -> Result<DeltaTable, CliError> {
        let mut builder = DeltaTableBuilder::new(self.storage_url.as_str())
            .with_warmup(WarmupMode::None)
            .with_snapshot_selection(snapshot_selection)
            .with_credential_provider(self.credential_cache.credential_provider())
            .with_storage_options(self.storage_options.clone());
        if let Some(snapshot) = &self.catalog_snapshot {
            builder = builder.with_catalog_snapshot(snapshot.clone());
        }
        let table = match builder.load_table().await {
            Ok(table) => table,
            Err(source) => return Err(self.map_read_error(CliError::Reader { source }).await),
        };
        // The Delta log can declare unsupported features even when UC metadata passed validation.
        let protocol = table.protocol();
        if !matches!(protocol.min_reader_version(), 1..=3)
            || protocol.first_unsupported_reader_feature().is_some()
            || (self.catalog_snapshot.is_none()
                && protocol
                    .writer_features()
                    .iter()
                    .any(|feature| feature == "catalogManaged"))
        {
            return Err(CliError::UnityCatalog {
                source: Error::UnsupportedDirectRead,
            });
        }
        Ok(table)
    }

    /// Preserves UC authorization failures through snapshot, planning, and streaming errors.
    pub(crate) async fn map_read_error(&self, error: CliError) -> CliError {
        self.credential_cache
            .refresh_failure()
            .await
            .map_or(error, |source| CliError::UnityCatalog { source })
    }
}
