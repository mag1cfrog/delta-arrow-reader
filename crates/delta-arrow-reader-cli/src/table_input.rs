//! Shared snapshot selection and authorization for a single table location.

use std::path::Path;

use clap::Args;
use delta_arrow_reader::{
    DeltaSnapshotSelection, DeltaStorageOptions, DeltaTable, DeltaTableBuilder, WarmupMode,
};
use snafu::{ResultExt, ensure};

use crate::{
    Error, ReaderSnafu, StorageAuthorizationSnafu, UnityCatalogSnafu,
    direct_credentials::CredentialSource,
    input::{self, JsonInputKind},
    parse_unsigned_decimal,
    unity_catalog::{AuthorizedTableAccess, UnityCatalogTableTarget},
};

/// Required location and settings for inspect and scan.
#[derive(Args)]
pub(crate) struct TableArgs {
    /// UTF-8 local path or storage URL. Relative paths use the working directory.
    /// Use '--' before a table path beginning with '-'.
    pub(crate) table: String,
    #[command(flatten)]
    pub(crate) options: TableOptions,
}

/// Settings for one table, shared by positional inputs and manifest entries.
#[derive(Args, Default)]
pub(crate) struct TableOptions {
    /// Unsigned decimal snapshot version; default: latest.
    #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<u64>)]
    pub(crate) table_version: Option<u64>,
    /// Local JSON object of string keys and string values.
    /// At most 1 MiB; '-' is a filename, not stdin.
    #[arg(long, value_name = "PATH")]
    pub(crate) storage_options_file: Option<String>,
    /// Explicit S3, Azure Blob, or GCS credential source. Without it, existing storage behavior applies.
    #[arg(long, value_enum)]
    pub(crate) auth_source: Option<CredentialSource>,
}

impl TableOptions {
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
    pub(crate) fn direct_table_builder(
        &self,
        location: &str,
        credential_file_directory: Option<&Path>,
    ) -> Result<DeltaTableBuilder, Error> {
        let options = self.read_storage_options()?;
        // Each command plans its own reads after the snapshot is loaded.
        let builder = DeltaTableBuilder::new(location)
            .with_warmup(WarmupMode::None)
            .with_snapshot_selection(self.snapshot_selection());
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
}

impl TableArgs {
    /// Keeps UC authorization alive alongside the pinned snapshot, including during scans.
    pub(crate) async fn load_table(
        &self,
        uc_target: Option<UnityCatalogTableTarget<'_>>,
    ) -> Result<(DeltaTable, Option<AuthorizedTableAccess>), Error> {
        match uc_target {
            Some(target) => {
                ensure!(
                    self.options.auth_source.is_none(),
                    StorageAuthorizationSnafu {
                        reason: "auth_source_conflicts_with_unity_catalog"
                    }
                );
                let access = target
                    .request_read_access(self.options.read_storage_options()?)
                    .await
                    .context(UnityCatalogSnafu)?;
                let table = access.load_table(self.options.snapshot_selection()).await?;
                Ok((table, Some(access)))
            }
            None => {
                let table = self
                    .options
                    .direct_table_builder(&self.table, None)?
                    .load_table()
                    .await
                    .context(ReaderSnafu)?;
                Ok((table, None))
            }
        }
    }
}
