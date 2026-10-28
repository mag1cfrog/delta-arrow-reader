//! Shared snapshot selection and authorization for a single table location.

mod azure_sas;
pub(crate) mod direct_credentials;
pub(crate) mod unity_catalog;

use std::path::Path;

use clap::Args;
use delta_arrow_reader::{
    DeltaSnapshotSelection, DeltaStorageOptions, DeltaTable, DeltaTableBuilder, WarmupMode,
};
use snafu::{ResultExt, ensure};

use direct_credentials::CredentialSource;
use unity_catalog::{AuthorizedTableAccess, UnityCatalogTableTarget};

use crate::{
    Error, ReaderSnafu, StorageAuthorizationSnafu, UnityCatalogSnafu,
    cli::parse_unsigned_decimal,
    input::{self, JsonInputKind},
};

/// Captured input for one table load, validated before any query binding starts table IO.
pub(crate) enum TableLoadRequest {
    DirectStorage(DeltaTableBuilder),
    UnityCatalog {
        target: UnityCatalogTableTarget,
        storage_options: DeltaStorageOptions,
        snapshot_selection: DeltaSnapshotSelection,
    },
}

impl TableLoadRequest {
    pub(crate) async fn load_table(
        self,
    ) -> Result<(DeltaTable, Option<AuthorizedTableAccess>), Error> {
        match self {
            Self::DirectStorage(builder) => {
                Ok((builder.load_table().await.context(ReaderSnafu)?, None))
            }
            Self::UnityCatalog {
                target,
                storage_options,
                snapshot_selection,
            } => {
                let access = target
                    .request_read_access(storage_options)
                    .await
                    .context(UnityCatalogSnafu)?;
                let table = access.load_table(snapshot_selection).await?;
                Ok((table, Some(access)))
            }
        }
    }
}

/// Required location and settings for inspect, scan, and preview.
#[derive(Args, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct TableArgs {
    /// UTF-8 local path or storage URL. Relative paths use the working directory.
    /// Use '--' before a table path beginning with '-'.
    pub(crate) table: String,
    #[command(flatten)]
    #[serde(flatten)]
    pub(crate) options: TableOptions,
}

/// Settings for one table, shared by positional inputs and manifest entries.
#[derive(Args, Default, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct TableOptions {
    /// Unsigned decimal snapshot version; default: latest.
    #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<u64>)]
    pub(crate) table_version: Option<u64>,
    /// Local JSON object of string keys and string values.
    /// At most 1 MiB; '-' is a filename, not stdin.
    #[arg(long, value_name = "PATH")]
    pub(crate) storage_options_file: Option<String>,
    /// Explicit UTF-8 NAME=VALUE credential file, at most 1 MiB. Values are literal.
    #[arg(long, value_name = "PATH", conflicts_with_all = ["auth_source", "databricks_profile"])]
    pub(crate) env_file: Option<String>,
    /// S3, Azure Blob, or GCS credential source. Remote --output requires a source or --env-file;
    /// a storage-options file alone selects json. Stdout keeps backend defaults.
    #[arg(long, value_enum)]
    pub(crate) auth_source: Option<CredentialSource>,
}

impl TableOptions {
    /// File results must be replayable without an implicit provider-chain identity.
    pub(crate) fn select_file_output_credentials(&mut self, location: &str) -> Result<(), Error> {
        if Path::new(location).is_absolute() || location.starts_with("uc://") {
            return Ok(());
        }
        let Ok(url) = url::Url::parse(location) else {
            return Ok(());
        };
        let (scheme, _) = object_store::ObjectStoreScheme::parse(&url).map_err(|_| {
            Error::StorageAuthorization {
                reason: "auth_source_invalid_table_url",
            }
        })?;
        direct_credentials::validate_table_url(&url, &scheme)?;
        if matches!(scheme, object_store::ObjectStoreScheme::Local) {
            return Ok(());
        }
        if self.env_file.is_none() && self.auth_source.is_none() {
            ensure!(
                self.storage_options_file.is_some(),
                StorageAuthorizationSnafu {
                    reason: "file_output_requires_auth_source",
                }
            );
            self.auth_source = Some(CredentialSource::Json);
        }
        Ok(())
    }

    fn snapshot_selection(&self) -> DeltaSnapshotSelection {
        self.table_version.map_or(
            DeltaSnapshotSelection::Latest,
            DeltaSnapshotSelection::Version,
        )
    }

    fn read_storage_options(&self) -> Result<DeltaStorageOptions, Error> {
        ensure!(
            !matches!(self.auth_source, Some(CredentialSource::Json))
                || self.storage_options_file.is_some(),
            StorageAuthorizationSnafu {
                reason: "auth_source_requires_storage_options_file"
            }
        );
        match &self.storage_options_file {
            Some(path) => Ok(input::read_json_file::<input::StorageOptionsInput>(
                path,
                JsonInputKind::StorageOptions,
            )?
            .0),
            None => Ok(Default::default()),
        }
    }

    /// Validates explicit credentials without loading the snapshot or reading data files.
    /// A supplied directory resolves credential paths in the options file; environment paths stay cwd-relative.
    fn direct_table_builder(
        &self,
        location: &str,
        credential_file_directory: Option<&Path>,
    ) -> Result<DeltaTableBuilder, Error> {
        let options = self.read_storage_options()?;
        // Each command plans its own reads after the snapshot is loaded.
        let builder = DeltaTableBuilder::new(location)
            .with_warmup(WarmupMode::None)
            .with_snapshot_selection(self.snapshot_selection());
        if let Some(path) = &self.env_file {
            return direct_credentials::env_file::configure_credentials_from_env_file(
                builder, location, options, path,
            );
        }
        match self.auth_source {
            Some(source) => source.configure_table_credentials(
                builder,
                location,
                options,
                credential_file_directory,
            ),
            None => Ok(builder.with_storage_options(options)),
        }
    }

    /// Captures options and rejects credential overrides before starting any table IO.
    pub(crate) fn prepare_load(
        &self,
        location: &str,
        uc_target: Option<UnityCatalogTableTarget>,
        credential_file_directory: Option<&Path>,
    ) -> Result<TableLoadRequest, Error> {
        ensure!(
            self.env_file.is_none() || self.auth_source.is_none(),
            StorageAuthorizationSnafu {
                reason: "env_file_conflicts_with_auth_source"
            }
        );
        match uc_target {
            Some(target) => {
                ensure!(
                    self.auth_source.is_none(),
                    StorageAuthorizationSnafu {
                        reason: "auth_source_conflicts_with_unity_catalog"
                    }
                );
                ensure!(
                    self.env_file.is_none(),
                    StorageAuthorizationSnafu {
                        reason: "env_file_conflicts_with_unity_catalog"
                    }
                );
                let storage_options = self.read_storage_options()?;
                unity_catalog::validate_storage_options(&storage_options)
                    .context(UnityCatalogSnafu)?;
                Ok(TableLoadRequest::UnityCatalog {
                    target,
                    storage_options,
                    snapshot_selection: self.snapshot_selection(),
                })
            }
            None => Ok(TableLoadRequest::DirectStorage(
                self.direct_table_builder(location, credential_file_directory)?,
            )),
        }
    }
}

impl TableArgs {
    /// Keeps UC authorization alive alongside the pinned snapshot, including during scans.
    pub(crate) async fn load_table(
        &self,
        uc_target: Option<UnityCatalogTableTarget>,
    ) -> Result<(DeltaTable, Option<AuthorizedTableAccess>), Error> {
        self.options
            .prepare_load(&self.table, uc_target, None)?
            .load_table()
            .await
    }
}
