//! Manifest structure for offline discovery; value and reference checks remain in the parser.

use clap::ValueEnum;
use serde_json::{Value, json};

use crate::direct_credentials::CredentialSource;

pub(crate) fn tables_manifest_schema() -> Value {
    let sources: Vec<_> = CredentialSource::value_variants()
        .iter()
        .filter_map(|source| source.to_possible_value())
        .map(|value| value.get_name().to_owned())
        .collect();
    json!({
        "title": "Named table snapshots",
        "description": "Bind each alias once to its own snapshot and authorization. Snapshots do not form a cross-table transaction.",
        "type": "object", "additionalProperties": false,
        "required": ["format_version", "tables"],
        "properties": {
            "format_version": {"type": "integer", "const": 1},
            "tables": {
                "type": "array", "minItems": 1, "maxItems": super::MAX_TABLES,
                "items": {"$ref": "#/$defs/table"},
                "description": "Unique aliases in declaration order; completion reports each alias and actual version in that order.",
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
                        "description": "Local path or direct storage URL. Relative paths use the manifest directory. No blank values, control characters, embedded credentials, URL queries or fragments. UC targets are not supported.",
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
                "description": "References only; no inline credentials or env_file. Omit for backend defaults; null is invalid.",
                "properties": {
                    "source": {
                        "type": "string", "enum": sources,
                        "description": "Same restrictions as --auth-source. Credentials never fall back to another identity.",
                    },
                    "storage_options_file": {
                        "type": "string", "minLength": 1,
                        "description": "Local JSON path relative to the manifest, required for json. Nested credential paths use this file's directory; environment paths use the working directory. No blank values, control characters or URLs.",
                    },
                },
                "allOf": [{
                    "if": {"properties": {"source": {"const": "json"}}},
                    "then": {"required": ["storage_options_file"]},
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
mod tests {
    use super::*;

    #[test]
    fn manifest_schema_matches_parser_shapes_and_table_count_limits()
    -> Result<(), Box<dyn std::error::Error>> {
        let schema = tables_manifest_schema();
        let validator = jsonschema::validator_for(&schema)?;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("tables.json");
        let baseline =
            json!({"format_version": 1, "tables": [{"alias": "orders", "location": "./orders"}]});
        let mut cases = vec![(baseline.clone(), true)];
        for version in ["0", "18446744073709551615"] {
            let mut manifest = baseline.clone();
            manifest["tables"][0]["table_version"] = json!(version);
            cases.push((manifest, true));
        }
        for (field, value) in [
            ("alias", json!("MixedCase")),
            ("alias", json!("orders\n")),
            ("location", json!(null)),
            ("table_version", json!(1)),
            ("table_version", json!(null)),
            ("table_version", json!("01")),
            ("table_version", json!("1\n")),
            ("authorization", json!(null)),
            ("authorization", json!([])),
            ("authorization", json!({"source":"unknown"})),
            ("authorization", json!({"source":"json"})),
            (
                "authorization",
                json!({"source":"env", "access_key_id":"secret"}),
            ),
            (
                "authorization",
                json!({"source":"env", "storage_options_file":null}),
            ),
        ] {
            let mut manifest = baseline.clone();
            manifest["tables"][0][field] = value;
            cases.push((manifest, false));
        }
        for count in [0, super::super::MAX_TABLES, super::super::MAX_TABLES + 1] {
            let tables: Vec<_> = (0..count)
                .map(|index| json!({"alias":format!("table_{index}"), "location":"./table"}))
                .collect();
            cases.push((
                json!({"format_version":1,"tables":tables}),
                count == super::super::MAX_TABLES,
            ));
        }
        for (manifest, accepted) in cases {
            assert_eq!(validator.is_valid(&manifest), accepted, "{manifest}");
            std::fs::write(&path, manifest.to_string())?;
            assert_eq!(
                super::super::read_table_builders(path.to_str().unwrap()).is_ok(),
                accepted,
                "{manifest}",
            );
        }
        Ok(())
    }
}
