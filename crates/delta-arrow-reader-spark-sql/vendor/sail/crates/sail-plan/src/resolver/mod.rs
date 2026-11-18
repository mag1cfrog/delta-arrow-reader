// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;

use crate::config::PlanConfig;

mod data_type;
mod expression;
mod file_metadata;
mod interval_qualifier;
mod literal;
pub mod plan;
mod query;
mod schema;
mod state;
mod tree;

/// A trusted adapter's file-origin slots, before opaque resolver renaming.
pub struct FileMetadataTable {
    pub provider: Arc<dyn TableProvider>,
    pub file_path_column: usize,
    pub file_name_column: usize,
}

pub type FileMetadataAdapter = Arc<
    dyn Fn(Arc<dyn TableProvider>) -> datafusion_common::Result<Option<FileMetadataTable>>
        + Send
        + Sync,
>;

pub struct PlanResolver<'a> {
    ctx: &'a SessionContext,
    config: Arc<PlanConfig>,
    allow_host_functions: bool,
    file_metadata_adapter: Option<FileMetadataAdapter>,
}

impl<'a> PlanResolver<'a> {
    pub fn new(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self {
            ctx,
            config,
            allow_host_functions: true,
            file_metadata_adapter: None,
        }
    }

    /// Resolve metadata from registered providers and frontend built-in functions only.
    pub fn new_for_metadata(ctx: &'a SessionContext, config: Arc<PlanConfig>) -> Self {
        Self {
            ctx,
            config,
            allow_host_functions: false,
            file_metadata_adapter: None,
        }
    }

    pub fn with_file_metadata_adapter(mut self, adapter: FileMetadataAdapter) -> Self {
        self.file_metadata_adapter = Some(adapter);
        self
    }
}
