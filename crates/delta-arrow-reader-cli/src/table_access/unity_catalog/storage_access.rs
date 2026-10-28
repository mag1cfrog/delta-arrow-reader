//! Load an authorized Delta snapshot and preserve UC failures from subsequent reads.

use std::sync::Arc;

use delta_arrow_reader::{
    DeltaSnapshotSelection, DeltaStorageOptions, DeltaTable, DeltaTableBuilder, WarmupMode,
};
use url::Url;

use super::{
    Error, credential_cache::TableCredentialCache, storage_location::TableStorageLocation,
    temporary_credentials::TemporaryTableCredentials,
};
use crate::diagnostics::Error as CliError;

/// Validated storage access for one UC table binding, including its renewable credentials.
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

impl AuthorizedTableAccess {
    /// Binds renewal to the selected profile, immutable table ID, and canonical root.
    pub(super) fn new(
        storage_location: TableStorageLocation,
        storage_options: DeltaStorageOptions,
        profile: String,
        table_id: String,
        credentials: TemporaryTableCredentials,
    ) -> Result<Self, Error> {
        let storage_url = storage_location.as_url().clone();
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
    /// UC reads require a protocol supported by the local reader.
    pub(crate) async fn load_table(
        &self,
        snapshot_selection: DeltaSnapshotSelection,
    ) -> Result<DeltaTable, CliError> {
        let builder = DeltaTableBuilder::new(self.storage_url.as_str())
            .with_warmup(WarmupMode::None)
            .with_snapshot_selection(snapshot_selection)
            .with_credential_provider(self.credential_cache.credential_provider())
            .with_storage_options(self.storage_options.clone());
        let table = match builder.load_table().await {
            Ok(table) => table,
            Err(source) => return Err(self.map_read_error(CliError::Reader { source }).await),
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
