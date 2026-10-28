//! SQL and table declarations shared by query execution and plan inspection.

use clap::Args;
use snafu::ResultExt;

use super::ReadOnlySqlQuery;
use crate::{
    diagnostics::{Error, UnityCatalogSnafu},
    output,
    table_access::{TableLoadRequest, TableOptions, unity_catalog::UnityCatalogTableTarget},
};

/// SQL and snapshot declarations shared by query execution and plan inspection.
#[derive(Args, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct QueryInputArgs {
    /// UTF-8 local path or storage URL, registered as t. Relative paths use the working directory.
    /// UC targets use uc://catalog.schema.table and require --databricks-profile.
    /// Use '--' before a table path beginning with '-'.
    #[arg(required_unless_present = "tables_file")]
    pub(crate) table: Option<String>,
    /// Local JSON manifest of 1-64 named tables; at most 1 MiB. Relative locations use its directory.
    /// Conflicts with TABLE, --table-version, --storage-options-file, --env-file, --auth-source, and --databricks-profile.
    #[arg(long, value_name = "PATH", conflicts_with_all = ["table", "table_version", "storage_options_file", "env_file", "auth_source", "databricks_profile"])]
    tables_file: Option<String>,
    #[command(flatten)]
    #[serde(flatten)]
    pub(crate) table_options: TableOptions,
    /// Existing Databricks CLI profile for positional uc:// TABLE; login must already be configured.
    #[arg(long, value_name = "PROFILE")]
    databricks_profile: Option<String>,
    /// UTF-8 file containing one read-only SQL query over the selected aliases; at most 64 KiB.
    #[arg(long, value_name = "PATH")]
    pub(crate) sql_file: String,
}

impl QueryInputArgs {
    /// Validates all bindings and SQL before loading any snapshot.
    pub(crate) fn prepare_inputs(
        &self,
        artifact: &mut Option<output::artifact::StagedResult>,
    ) -> Result<(Vec<(String, TableLoadRequest)>, ReadOnlySqlQuery), Error> {
        let requests = if let Some(path) = &self.tables_file {
            crate::input::read_table_requests(path, artifact)?
        } else {
            let location = self.table.as_deref().ok_or(Error::Argument {
                kind: clap::error::ErrorKind::MissingRequiredArgument,
                correction: None,
            })?;
            let uc_target =
                UnityCatalogTableTarget::parse(location, self.databricks_profile.as_deref())
                    .context(UnityCatalogSnafu)?;
            vec![(
                "t".to_owned(),
                self.table_options.prepare_load(location, uc_target, None)?,
            )]
        };
        let (sql_query, bytes) = ReadOnlySqlQuery::read_file_with_bytes(&self.sql_file)?;
        if let Some(artifact) = artifact {
            artifact.record_replay_input("--sql-file", &self.sql_file, &bytes);
        }
        Ok((requests, sql_query))
    }
}
