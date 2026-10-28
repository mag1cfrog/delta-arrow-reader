//! Bounded table declarations. Validation and path resolution finish before snapshot loading.

mod json_schema;

pub(crate) use json_schema::tables_manifest_schema;

use std::{collections::HashSet, path::Path};

use clap::ValueEnum;
use datafusion::sql::sqlparser::keywords::ALL_KEYWORDS;
use object_store::ObjectStoreScheme;
use serde::{Deserialize, Deserializer};
use snafu::ResultExt;
use url::Url;

use crate::{
    diagnostics::{Error, InputCorrection, InputIoSnafu, UnityCatalogSnafu},
    input::{self, JsonInputKind, JsonObject},
    table_access::{
        TableLoadRequest, TableOptions,
        direct_credentials::{CredentialSource, validate_table_url},
        unity_catalog::UnityCatalogTableTarget,
    },
};

// Even with 63-byte aliases and full u64 versions, provenance fits one 8 KiB completion record.
pub(crate) const MAX_TABLES: usize = 64;

/// On-disk declarations, never credentials or already loaded snapshots.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TablesManifest {
    format_version: u8,
    tables: Vec<JsonObject<ManifestTable>>,
}

/// One SQL alias and its requested snapshot; relative locations belong to the manifest directory.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestTable {
    alias: String,
    location: String,
    #[serde(default, deserialize_with = "deserialize_table_version")]
    table_version: Option<u64>,
    #[serde(default, deserialize_with = "input::deserialize_present_field")]
    authorization: Option<JsonObject<ManifestAuthorization>>,
}

/// Selects the existing credential provider using references, never credential values in the manifest.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManifestAuthorization {
    source: String,
    #[serde(default, deserialize_with = "input::deserialize_present_field")]
    storage_options_file: Option<String>,
    #[serde(default, deserialize_with = "input::deserialize_present_field")]
    env_file: Option<String>,
    #[serde(default, deserialize_with = "input::deserialize_present_field")]
    databricks_profile: Option<String>,
}

/// Prepares every binding without opening any table, including when a later entry is invalid.
pub(crate) fn read_table_requests(
    path: &str,
    artifact: &mut Option<crate::output::artifact::StagedResult>,
) -> Result<Vec<(String, TableLoadRequest)>, Error> {
    let (JsonObject(manifest), bytes) = input::read_json_file_with_bytes::<
        JsonObject<TablesManifest>,
    >(path, JsonInputKind::Tables)?;
    if let Some(artifact) = artifact {
        artifact.record_replay_input("--tables-file", path, &bytes);
    }
    if manifest.format_version != 1 {
        return Err(manifest_error(
            "/format_version",
            "unsupported_tables_format_version",
            "the JSON integer 1",
        ));
    }
    if manifest.tables.is_empty() {
        return Err(manifest_error(
            "/tables",
            "empty_tables_manifest",
            "a nonempty array of table objects",
        ));
    }
    if manifest.tables.len() > MAX_TABLES {
        return Err(manifest_error(
            "/tables",
            "too_many_tables",
            "at most 64 table objects",
        ));
    }
    let manifest_path = std::path::absolute(path).context(InputIoSnafu {
        argument: JsonInputKind::Tables.argument(),
    })?;
    let directory = manifest_path.parent().unwrap_or(Path::new("."));
    let mut aliases = HashSet::new();
    manifest
        .tables
        .into_iter()
        .enumerate()
        .map(|(index, JsonObject(table))| {
            let alias_field_path = format!("/tables/{index}/alias");
            if table.alias.len() > 63
                || !table
                    .alias
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_lowercase)
                || !table
                    .alias
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            {
                return Err(manifest_error(
                    &alias_field_path,
                    "invalid_table_alias",
                    "an alias matching [a-z][a-z0-9_]{0,62}",
                ));
            }
            if matches!(
                table.alias.as_str(),
                "t" | "datafusion" | "public" | "information_schema" | "pg_catalog" | "system"
            ) || ALL_KEYWORDS
                .binary_search(&table.alias.to_ascii_uppercase().as_str())
                .is_ok()
            {
                return Err(manifest_error(
                    &alias_field_path,
                    "reserved_table_alias",
                    "an alias other than t, a system catalog/schema name, or a SQL keyword",
                ));
            }
            if !aliases.insert(table.alias.clone()) {
                return Err(manifest_error(
                    &alias_field_path,
                    "duplicate_table_alias",
                    "an alias used only once in this manifest",
                ));
            }
            let location = resolve_location(&table.location, directory, index)?;
            let mut options = TableOptions {
                table_version: table.table_version,
                ..Default::default()
            };
            let mut databricks_profile = None;
            if let Some(JsonObject(authorization)) = table.authorization {
                let profile_field_path =
                    format!("/tables/{index}/authorization/databricks_profile");
                let env_file_field_path = format!("/tables/{index}/authorization/env_file");
                if authorization.source != "env-file" && authorization.env_file.is_some() {
                    return Err(manifest_error(
                        &env_file_field_path,
                        "env_file_requires_env_file_source",
                        "env_file only with source env-file",
                    ));
                }
                if authorization.source != "databricks"
                    && authorization.databricks_profile.is_some()
                {
                    return Err(manifest_error(
                        &profile_field_path,
                        "databricks_profile_requires_databricks_source",
                        "databricks_profile only with source databricks",
                    ));
                }
                match authorization.source.as_str() {
                    "databricks" => {
                        databricks_profile =
                            Some(authorization.databricks_profile.ok_or_else(|| {
                                manifest_error(
                                    &profile_field_path,
                                    "missing_databricks_profile",
                                    "an existing Databricks CLI profile name",
                                )
                            })?);
                    }
                    "env-file" => {
                        let path = authorization.env_file.ok_or_else(|| {
                            manifest_error(
                                &env_file_field_path,
                                "missing_env_file",
                                "an env_file path with source env-file",
                            )
                        })?;
                        options.env_file = Some(resolve_authorization_file_path(
                            &path, directory, index, "env_file",
                        )?);
                    }
                    source => {
                        options.auth_source =
                            Some(CredentialSource::from_str(source, false).map_err(|_| {
                                manifest_error(
                                    &format!("/tables/{index}/authorization/source"),
                                    "invalid_authorization_source",
                                    "an installed direct-storage source, env-file, or databricks",
                                )
                            })?);
                    }
                }
                options.storage_options_file = authorization
                    .storage_options_file
                    .map(|path| {
                        resolve_authorization_file_path(
                            &path,
                            directory,
                            index,
                            "storage_options_file",
                        )
                    })
                    .transpose()?;
            }
            let uc_target =
                UnityCatalogTableTarget::parse(&location, databricks_profile.as_deref())
                    .context(UnityCatalogSnafu)?;
            if artifact.is_some() {
                options
                    .select_file_output_credentials(&location)
                    .map_err(|source| Error::ManifestAuthorizationInput {
                        source: Box::new(source),
                        field_path: format!("/tables/{index}/authorization"),
                    })?;
            }
            let credential_file_directory = options
                .storage_options_file
                .as_deref()
                .and_then(|path| Path::new(path).parent());
            let request = options
                .prepare_load(&location, uc_target, credential_file_directory)
                .map_err(|source| {
                    let correction = source.input_correction();
                    let field = match correction.as_ref().map(|value| value.argument.as_str()) {
                        Some("--env-file") => "/env_file",
                        Some("--storage-options-file") => "/storage_options_file",
                        _ => "",
                    };
                    Error::ManifestAuthorizationInput {
                        source: Box::new(source),
                        field_path: format!("/tables/{index}/authorization{field}"),
                    }
                })?;
            Ok((table.alias, request))
        })
        .collect()
}

fn resolve_authorization_file_path(
    path: &str,
    manifest_directory: &Path,
    index: usize,
    field: &str,
) -> Result<String, Error> {
    let invalid_path = || {
        manifest_error(
            &format!("/tables/{index}/authorization/{field}"),
            "invalid_authorization_reference",
            "a nonempty UTF-8 local file path without control characters",
        )
    };
    if path.trim().is_empty()
        || path.chars().any(char::is_control)
        || (!Path::new(path).is_absolute()
            && !matches!(
                Url::parse(path),
                Err(url::ParseError::RelativeUrlWithoutBase)
            ))
    {
        return Err(invalid_path());
    }
    manifest_directory
        .join(path)
        .into_os_string()
        .into_string()
        .map_err(|_| invalid_path())
}

fn resolve_location(location: &str, directory: &Path, index: usize) -> Result<String, Error> {
    let invalid_location = || {
        manifest_error(
            &format!("/tables/{index}/location"),
            "invalid_table_location",
            "a nonempty UTF-8 path or storage URL without embedded credentials or control characters",
        )
    };
    if location.trim().is_empty() || location.chars().any(char::is_control) {
        return Err(invalid_location());
    }
    if location.starts_with("uc://") {
        // UC names and their profile are validated together after authorization is decoded.
        return Ok(location.to_owned());
    }
    if !Path::new(location).is_absolute() {
        match Url::parse(location) {
            Ok(url) => {
                let (scheme, _) = ObjectStoreScheme::parse(&url).map_err(|_| invalid_location())?;
                validate_table_url(&url, &scheme).map_err(|_| invalid_location())?;
                return Ok(url.into());
            }
            Err(url::ParseError::RelativeUrlWithoutBase) => {}
            Err(_) => return Err(invalid_location()),
        }
    }
    directory
        .join(location)
        .to_str()
        .map(str::to_owned)
        .ok_or_else(invalid_location)
}

fn deserialize_table_version<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u64>, D::Error> {
    // Explicit null must not silently select the latest snapshot.
    input::deserialize_integer_string(deserializer).map(Some)
}

fn manifest_error(field_path: &str, reason: &'static str, expected: &'static str) -> Error {
    Error::InputValidation {
        reason,
        correction: InputCorrection {
            argument: JsonInputKind::Tables.argument().to_owned(),
            field_path: Some(field_path.to_owned()),
            supplied: None,
            expected: expected.to_owned(),
            hint: "Correct this manifest field; locations are relative to the manifest directory.",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_resolution_distinguishes_explicit_paths_from_storage_urls()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = std::path::absolute("manifest")?;
        for location in ["../table with spaces", "./name://table", "./-table"] {
            assert_eq!(
                Path::new(&resolve_location(location, &directory, 0)?),
                directory.join(location)
            );
        }
        let absolute_table = directory.join("table");
        let location = absolute_table.to_str().ok_or("non-UTF-8 test directory")?;
        assert_eq!(resolve_location(location, &directory, 0)?, location);
        for location in [
            "file:///tmp/table",
            "s3://bucket/table",
            "gs://bucket/table",
            "abfss://container@account.dfs.core.windows.net/table",
        ] {
            assert_eq!(resolve_location(location, &directory, 0)?, location);
        }
        assert!(resolve_location("https://", &directory, 0).is_err());
        Ok(())
    }

    #[test]
    fn manifest_versions_preserve_the_full_unsigned_range() -> Result<(), Box<dyn std::error::Error>>
    {
        for version in [0, u64::MAX] {
            let json = serde_json::json!({"format_version":1,"tables":[{
                "alias":"orders","location":"./table","table_version":version.to_string()
            }]})
            .to_string();
            let manifest = input::parse_json::<JsonObject<TablesManifest>>(
                json.as_bytes(),
                JsonInputKind::Tables,
            )?
            .0;
            assert_eq!(manifest.tables[0].0.table_version, Some(version));
        }
        Ok(())
    }
}
