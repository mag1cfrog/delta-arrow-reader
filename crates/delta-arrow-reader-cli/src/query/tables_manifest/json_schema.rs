//! Manifest structure for offline discovery; value and reference checks remain in the parser.

use clap::ValueEnum;
use serde_json::{Value, json};

use crate::table_access::direct_credentials::CredentialSource;

pub(crate) fn tables_manifest_schema() -> Value {
    let mut sources: Vec<_> = CredentialSource::value_variants()
        .iter()
        .filter_map(|source| source.to_possible_value())
        .map(|value| value.get_name().to_owned())
        .collect();
    sources.extend(["env-file".to_owned(), "databricks".to_owned()]);
    json!({
        "title": "Named table snapshots",
        "description": "Relative locations use the manifest directory. Omit table_version for latest at load time; updates and credential refresh never retarget a loaded snapshot. Snapshots do not form a cross-table transaction.",
        "type": "object", "additionalProperties": false,
        "required": ["format_version", "tables"],
        "properties": {
            "format_version": {
                "type": "integer", "const": 1,
                "description": "JSON integer 1; fraction and exponent notation are invalid.",
            },
            "tables": {
                "type": "array", "minItems": 1, "maxItems": super::MAX_TABLES,
                "items": {"$ref": "#/$defs/table"},
                "description": "Unique aliases in declaration order. Each input loads one snapshot.",
            },
        },
        "$defs": {
            "table": {
                "type": "object", "additionalProperties": false,
                "required": ["alias", "location"],
                "properties": {
                    "alias": {
                        "type": "string", "pattern": "^[a-z][a-z0-9_]{0,62}$",
                        "not": {"pattern": "[^a-z0-9_]"},
                        "description": "Unique in the manifest. Cannot be t, datafusion, public, information_schema, pg_catalog, system, or a SQL keyword.",
                    },
                    "location": {
                        "type": "string", "minLength": 1,
                        "description": "Local path, direct storage URL, or uc://catalog.schema.table. Relative paths use the manifest directory. No blank values, control characters, embedded credentials, URL queries or fragments.",
                    },
                    "table_version": {
                        "type": "string", "pattern": "^(0|[1-9][0-9]*)$",
                        "not": {"pattern": "[^0-9]"}, "maxLength": 20,
                        "description": "Canonical decimal u64 string, 0 through 18446744073709551615. Omit for latest at load time; null is invalid. Updates and credential refresh never retarget a loaded snapshot.",
                    },
                    "authorization": {"$ref": "#/$defs/authorization"},
                },
            },
            "authorization": {
                "type": "object", "additionalProperties": false, "required": ["source"],
                "description": "References only; no inline credentials. Required for remote --output; stdout allows direct-storage defaults when omitted. UC requires databricks. Null is invalid.",
                "properties": {
                    "source": {
                        "type": "string", "enum": sources,
                        "description": "An --auth-source name or env-file for direct storage, or databricks for UC. No identity fallback.",
                    },
                    "storage_options_file": {
                        "type": "string", "minLength": 1,
                        "description": "Local JSON path relative to the manifest; required for json. With env-file, accepts noncredential configuration only. UC allows transport/region only. Nested credential paths use this file's directory; environment paths use cwd. No blanks, control characters or URLs.",
                    },
                    "env_file": {
                        "type": "string", "minLength": 1,
                        "description": "Credential env-file path relative to the manifest. Required only for source env-file; invalid for other sources. Nested credential references use the env file's directory. No blanks, control characters or URLs.",
                    },
                    "databricks_profile": {
                        "type": "string", "minLength": 1,
                        "description": "Existing Databricks CLI profile. Required only for databricks; invalid for other sources. No blank names or control characters.",
                    },
                },
                "allOf": [{
                    "if": {"properties": {"source": {"const": "json"}}},
                    "then": {"required": ["storage_options_file"]},
                }, {
                    "if": {"properties": {"source": {"const": "env-file"}}},
                    "then": {"required": ["env_file"]},
                    "else": {"not": {"required": ["env_file"]}},
                }, {
                    "if": {"properties": {"source": {"const": "databricks"}}},
                    "then": {"required": ["databricks_profile"]},
                    "else": {"not": {"required": ["databricks_profile"]}},
                }],
            },
        },
        "examples": [{"format_version": 1, "tables": [
            {"alias": "orders", "location": "./orders", "table_version": "0"},
            {"alias": "customers", "location": "./customers"},
        ]}],
    })
}

#[cfg(test)]
mod tests;
