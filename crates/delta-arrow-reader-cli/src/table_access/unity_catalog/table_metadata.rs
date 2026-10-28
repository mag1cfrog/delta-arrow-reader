//! Unity Catalog table metadata and the policies required for a direct Delta read.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

use crate::input::JsonObject;

use super::Error;

/// Object kind and capability names returned in `securable_kind_manifest`.
#[derive(Deserialize)]
struct TableKindManifest {
    securable_type: Option<String>,
    securable_kind: Option<String>,
    #[serde(default)]
    capabilities: Vec<String>,
}

/// The column policy field needed to reject reads that would bypass a mask.
///
/// Arrow column types come from the Delta log, so the remaining UC column fields are ignored.
#[derive(Deserialize)]
struct ColumnMaskMetadata {
    mask: Option<Value>,
}

/// Fields from `databricks tables get` needed to check and authorize a direct Delta read.
#[derive(Deserialize)]
pub(super) struct TableMetadataResponse {
    pub(super) table_id: String,
    full_name: String,
    table_type: String,
    data_source_format: String,
    pub(super) storage_location: String,
    securable_kind_manifest: JsonObject<TableKindManifest>,
    #[serde(default)]
    properties: BTreeMap<String, String>,
    #[serde(default)]
    columns: Vec<ColumnMaskMetadata>,
    row_filter: Option<Value>,
    pipeline_id: Option<String>,
    #[serde(default)]
    browse_only: bool,
}

impl TableMetadataResponse {
    /// Decodes a response and checks that it describes the requested, directly readable table.
    pub(super) fn parse(metadata_json: &[u8], requested_table_name: &str) -> Result<Self, Error> {
        let metadata = serde_json::from_slice::<JsonObject<Self>>(metadata_json)
            .map_err(|_| Error::InvalidDatabricksResponse)?
            .0;
        metadata.validate_direct_read(requested_table_name)?;
        Ok(metadata)
    }

    /// Checks table identity and rejects policies or table kinds the local reader cannot honor.
    fn validate_direct_read(&self, requested_table_name: &str) -> Result<(), Error> {
        if !self.full_name.eq_ignore_ascii_case(requested_table_name)
            || self.table_id.len() != 36
            || !self.table_id.bytes().enumerate().all(|(index, byte)| {
                if [8, 13, 18, 23].contains(&index) {
                    byte == b'-'
                } else {
                    byte.is_ascii_hexdigit()
                }
            })
        {
            return Err(Error::InvalidDatabricksResponse);
        }
        let manifest = &self.securable_kind_manifest.0;
        // Some workspaces return capabilities without the optional kind fields.
        let has_kind_evidence = manifest.securable_kind.is_some()
            || manifest
                .capabilities
                .iter()
                .any(|capability| capability == "HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT");
        let is_supported_table_kind = matches!(self.table_type.as_str(), "MANAGED" | "EXTERNAL")
            && self.data_source_format == "DELTA"
            && has_kind_evidence
            && manifest
                .securable_type
                .as_deref()
                .is_none_or(|kind| kind == "TABLE")
            && manifest
                .securable_kind
                .as_deref()
                .is_none_or(|kind| matches!(kind, "TABLE_DELTA" | "TABLE_DELTA_EXTERNAL"));
        let has_invalid_capability = manifest
            .capabilities
            .iter()
            .any(|capability| capability.is_empty());
        // The local reader cannot apply UC row filters or column masks.
        let has_read_policy =
            self.row_filter.is_some() || self.columns.iter().any(|column| column.mask.is_some());
        let requires_commit_coordinator = self.properties.keys().any(|key| {
            key == "delta.feature.catalogManaged" || key.starts_with("delta.coordinatedCommits.")
        });
        if !is_supported_table_kind
            || has_invalid_capability
            || has_read_policy
            || self.pipeline_id.is_some()
            || self.browse_only
            || requires_commit_coordinator
        {
            return Err(Error::UnsupportedDirectRead);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
