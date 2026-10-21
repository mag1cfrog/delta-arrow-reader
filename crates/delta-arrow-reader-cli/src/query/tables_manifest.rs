//! Bounded table declarations. Validation and path resolution finish before snapshot loading.

mod json_schema;

pub(crate) use json_schema::tables_manifest_schema;

use std::{collections::HashSet, path::Path};

use clap::ValueEnum;
use datafusion::sql::sqlparser::keywords::ALL_KEYWORDS;
use delta_arrow_reader::DeltaTableBuilder;
use object_store::ObjectStoreScheme;
use serde::{Deserialize, Deserializer, de};
use snafu::ResultExt;
use url::Url;

use crate::{
    Error, InputIoSnafu,
    diagnostics::InputCorrection,
    direct_credentials::{CredentialSource, validate_table_url},
    input::{self, JsonInputKind, JsonObject},
    table_input::TableOptions,
};

// Even with 63-byte aliases and full u64 versions, provenance fits one 8 KiB completion record.
pub(crate) const MAX_TABLES: usize = 64;

/// On-disk declarations, never credentials or already loaded snapshots.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TablesManifest {
    format_version: u8,
    tables: Vec<JsonObject<ManifestTable>>,
}

/// One SQL alias and its requested snapshot; relative locations belong to the manifest directory.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestTable {
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
struct ManifestAuthorization {
    #[serde(deserialize_with = "deserialize_credential_source")]
    source: CredentialSource,
    #[serde(default, deserialize_with = "input::deserialize_present_field")]
    storage_options_file: Option<String>,
}

/// Prepares every binding without opening any table, including when a later entry is invalid.
pub(crate) fn read_table_builders(path: &str) -> Result<Vec<(String, DeltaTableBuilder)>, Error> {
    let manifest =
        input::read_json_file::<JsonObject<TablesManifest>>(path, JsonInputKind::Tables)?.0;
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
            let alias_path = format!("/tables/{index}/alias");
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
                    &alias_path,
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
                    &alias_path,
                    "reserved_table_alias",
                    "an alias other than t, a system catalog/schema name, or a SQL keyword",
                ));
            }
            if !aliases.insert(table.alias.clone()) {
                return Err(manifest_error(
                    &alias_path,
                    "duplicate_table_alias",
                    "an alias used only once in this manifest",
                ));
            }
            let location = resolve_location(&table.location, directory, index)?;
            let mut options = TableOptions {
                table_version: table.table_version,
                ..Default::default()
            };
            if let Some(JsonObject(authorization)) = table.authorization {
                options.auth_source = Some(authorization.source);
                options.storage_options_file = authorization
                    .storage_options_file
                    .map(|path| resolve_options_file(&path, directory, index))
                    .transpose()?;
            }
            let credential_file_directory = options
                .storage_options_file
                .as_deref()
                .and_then(|path| Path::new(path).parent());
            let builder = options.direct_table_builder(&location, credential_file_directory)?;
            Ok((table.alias, builder))
        })
        .collect()
}

fn resolve_options_file(path: &str, directory: &Path, index: usize) -> Result<String, Error> {
    let invalid_path = || {
        manifest_error(
            &format!("/tables/{index}/authorization/storage_options_file"),
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
    directory
        .join(path)
        .into_os_string()
        .into_string()
        .map_err(|_| invalid_path())
}

fn deserialize_credential_source<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<CredentialSource, D::Error> {
    let source = String::deserialize(deserializer)?;
    CredentialSource::from_str(&source, false)
        .map_err(|_| de::Error::custom("an installed --auth-source name"))
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
    super::reject_unity_catalog_query(location)?;
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
    let text = String::deserialize(deserializer)?;
    // Strings preserve all u64 values across JSON clients; null must not silently select latest.
    text.parse::<u64>()
        .ok()
        .filter(|version| version.to_string() == text)
        .map(Some)
        .ok_or_else(|| {
            de::Error::custom(
                "table_version must be a canonical unsigned decimal string fitting u64",
            )
        })
}

fn manifest_error(field_path: &str, reason: &'static str, expected: &'static str) -> Error {
    Error::InputValidation {
        reason,
        correction: InputCorrection {
            argument: JsonInputKind::Tables.argument().to_owned(),
            field_path: Some(field_path.to_owned()),
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
