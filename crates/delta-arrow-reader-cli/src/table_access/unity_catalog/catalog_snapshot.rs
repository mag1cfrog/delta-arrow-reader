//! Resolve the catalog's confirmed Delta version and commits before reading storage.

use delta_arrow_reader::{DeltaCatalogCommit, DeltaCatalogSnapshot};
use serde::Deserialize;

use super::{
    Error, databricks_cli, storage_location::TableStorageLocation,
    table_metadata::TableMetadataResponse,
};
use crate::input::JsonObject;

const DELTA_API: &str = "/api/2.1/unity-catalog/delta/v1";
const GET_TABLE_ENDPOINT: &str = "GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}";

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DeltaApiConfiguration {
    protocol_version: String,
    endpoints: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct CatalogTableIdentity {
    table_uuid: String,
    location: String,
    securable_type: Option<String>,
    table_type: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct CatalogCommitResponse {
    version: i64,
    file_name: String,
    file_size: i64,
    file_modification_timestamp: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct CatalogSnapshotResponse {
    metadata: JsonObject<CatalogTableIdentity>,
    latest_table_version: i64,
    #[serde(default)]
    commits: Vec<JsonObject<CatalogCommitResponse>>,
}

pub(super) async fn load_catalog_snapshot(
    profile: &str,
    full_table_name: &str,
    table_metadata: &TableMetadataResponse,
    storage_location: &TableStorageLocation,
) -> Result<DeltaCatalogSnapshot, Error> {
    let [catalog, schema, table] = full_table_name.split('.').collect::<Vec<_>>()[..] else {
        return Err(Error::InvalidTableTarget);
    };
    let configuration = databricks_cli::get_delta_api_response(
        profile,
        &format!("{DELTA_API}/config?catalog={catalog}&protocol-versions=1.0"),
    )
    .await?;
    let configuration = serde_json::from_slice::<JsonObject<DeltaApiConfiguration>>(&configuration)
        .map_err(|_| Error::InvalidDatabricksResponse)?
        .0;
    if configuration.protocol_version != "1.0"
        || !configuration
            .endpoints
            .iter()
            .any(|endpoint| endpoint == GET_TABLE_ENDPOINT)
    {
        return Err(Error::UnsupportedCatalogProtocol);
    }
    let response = databricks_cli::get_delta_api_response(
        profile,
        &format!("{DELTA_API}/catalogs/{catalog}/schemas/{schema}/tables/{table}"),
    )
    .await?;
    parse_catalog_snapshot(&response, &table_metadata.table_id, storage_location)
}

fn parse_catalog_snapshot(
    response: &[u8],
    table_id: &str,
    storage_location: &TableStorageLocation,
) -> Result<DeltaCatalogSnapshot, Error> {
    let response = serde_json::from_slice::<JsonObject<CatalogSnapshotResponse>>(response)
        .map_err(|_| Error::InvalidDatabricksResponse)?
        .0;
    let identity = response.metadata.0;
    let catalog_location = TableStorageLocation::parse(&identity.location)?;
    if !identity.table_uuid.eq_ignore_ascii_case(table_id)
        || catalog_location.as_url() != storage_location.as_url()
        || identity
            .securable_type
            .as_deref()
            .is_some_and(|kind| kind != "TABLE")
        || identity.table_type != "MANAGED"
    {
        return Err(Error::InvalidDatabricksResponse);
    }
    let mut commits = response
        .commits
        .into_iter()
        .map(|commit| {
            let commit = commit.0;
            Ok(DeltaCatalogCommit {
                version: u64::try_from(commit.version)
                    .map_err(|_| Error::InvalidDatabricksResponse)?,
                file_name: commit.file_name,
                file_size: u64::try_from(commit.file_size)
                    .map_err(|_| Error::InvalidDatabricksResponse)?,
                last_modified_ms: commit.file_modification_timestamp,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    // UC returns newest first; Kernel requires an ascending, contiguous tail.
    commits.sort_unstable_by_key(|commit| commit.version);
    Ok(DeltaCatalogSnapshot {
        max_version: u64::try_from(response.latest_table_version)
            .map_err(|_| Error::InvalidDatabricksResponse)?,
        commits,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn catalog_tail_is_sorted_without_hiding_duplicate_versions() {
        let location = TableStorageLocation::parse("s3://bucket/table").unwrap();
        let mut response = json!({
            "metadata": {"table-uuid":"table-id", "location":"s3://bucket/table/",
                         "table-type":"MANAGED"},
            "latest-table-version": 2,
            "commits": ([2, 1, 1].map(|version| json!({"version":version,
                "file-name":format!("{version:020}.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json"),
                "file-size":100, "file-modification-timestamp":1})))
        });
        let snapshot = parse_catalog_snapshot(
            &serde_json::to_vec(&response).unwrap(),
            "table-id",
            &location,
        )
        .unwrap();
        assert_eq!(
            snapshot
                .commits
                .iter()
                .map(|c| c.version)
                .collect::<Vec<_>>(),
            [1, 1, 2]
        );
        for invalid in [
            json!(-1),
            json!(1.5),
            json!("2"),
            json!(null),
            json!(u64::MAX),
        ] {
            response["latest-table-version"] = invalid;
            assert!(
                parse_catalog_snapshot(
                    &serde_json::to_vec(&response).unwrap(),
                    "table-id",
                    &location
                )
                .is_err()
            );
        }
        response["latest-table-version"] = json!(2);
        response.as_object_mut().unwrap().remove("commits");
        assert!(
            parse_catalog_snapshot(
                &serde_json::to_vec(&response).unwrap(),
                "table-id",
                &location
            )
            .unwrap()
            .commits
            .is_empty()
        );
        response["commits"] = json!([[1, "file", 100, 1]]);
        assert!(
            parse_catalog_snapshot(
                &serde_json::to_vec(&response).unwrap(),
                "table-id",
                &location
            )
            .is_err()
        );
    }
}
