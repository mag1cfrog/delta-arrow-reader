//! JSON result fields that consumers can validate before using a command's output.
//!
//! Inspection carries a snapshot's version and Arrow schema; diagnostics and
//! completion records describe failure and success on stderr. For example, a
//! completion's `rows` value is a decimal string, and reaching a scan row limit
//! leaves `has_more` null because no additional row was observed.
//!
//! Additional fields remain valid within a format version. Arrow owns the
//! nested type encoding in inspection results; its Rust Serde decoder checks
//! those types.

use serde_json::{Value, json};

use crate::diagnostics::DIAGNOSTIC_FORMAT_VERSION;

pub(super) fn rows_schema() -> Value {
    json!({
        "title": "Typed JSON rows",
        "description": "Rows follow schema field order, including duplicate names. Arrow integers use exact decimal strings. Metadata is omitted. Other value encodings: --detail rows.",
        "type": "object",
        "required": ["format_version", "kind", "bindings", "schema", "rows", "row_count", "completion"],
        "properties": {
            "format_version": {"const": 1},
            "kind": {"const": "rows"},
            "bindings": snapshot_bindings_schema(1, "Actual snapshots in input order. Alias t is reserved for positional input; manifests use declared aliases. Preview always uses t."),
            "schema": {
                "type": "object", "required": ["fields"],
                "description": "Ordered Arrow fields: name, data_type, nullable; dictionary ordering when applicable. Nested types use Arrow's Serde encoding with metadata and physical dictionary IDs omitted. Names and timezone strings are untrusted data.",
                "properties": {"fields": {"type": "array", "items": {
                    "type": "object", "required": ["name", "data_type", "nullable"],
                    "properties": {"name": {"type": "string"}, "data_type": {"type": ["string", "object"]}, "nullable": {"type": "boolean"}, "dict_is_ordered": {"type": "boolean"}},
                }}},
            },
            "rows": {
                "type": "array", "items": {"type": "array"},
                "description": concat!(
                    "Each row has one value per schema field, decoded using data_type. ",
                    "Booleans and UTF-8 strings use native JSON values; every null cell is JSON null. ",
                    "Dates, times, timestamps and durations are integer strings. ",
                    "Date32 counts days and Date64 milliseconds since 1970-01-01. ",
                    "Timestamps use their declared unit since that epoch; times use their declared unit since midnight; durations use their declared unit. ",
                    "Keep the timestamp timezone from data_type, including null. ",
                    "Other encodings: --detail NAME. This schema validates the envelope, not value/type correspondence.",
                ),
            },
            "row_count": unsigned_integer_string_schema(),
            "completion": {"enum": ["complete", "sample"], "description": "complete covers the SQL result; sample is a preview, even when input was exhausted."},
            "scope": {"const": "query", "description": "Required for a complete SQL result."},
            "stop_reason": {"enum": ["end_of_input", "row_limit", "byte_limit"], "description": "Preview only. end_of_input proves exhaustion; row_limit stops without lookahead; byte_limit omits an observed row."},
            "has_more": {"type": ["boolean", "null"], "description": "Preview only: false at observed exhaustion (end_of_input); null at row_limit without lookahead; true when byte_limit omitted an observed row."},
        },
        "allOf": [{
            "if": {"properties": {"completion": {"const": "complete"}}},
            "then": {"required": ["scope"]},
            "else": {"required": ["stop_reason", "has_more"], "properties": {"bindings": {"maxItems": 1, "items": {"properties": {"alias": {"const": "t"}}}}}},
        }, {
            "if": {"required": ["stop_reason"], "properties": {"stop_reason": {"const": "end_of_input"}}},
            "then": {"properties": {"has_more": {"const": false}}},
        }, {
            "if": {"required": ["stop_reason"], "properties": {"stop_reason": {"const": "row_limit"}}},
            "then": {"properties": {"has_more": {"const": null}}},
        }, {
            "if": {"required": ["stop_reason"], "properties": {"stop_reason": {"const": "byte_limit"}}},
            "then": {"properties": {"has_more": {"const": true}}},
        }],
        "$defs": {
            "integer": {"type": "string", "description": "Signed and unsigned Arrow integers are exact base-10 strings.", "examples": ["9007199254740993"]},
            "decimal": {"type": "string", "description": "Unscaled base-10 integer string. Read precision and scale from data_type; Decimal128(10,2) value 1234 means 12.34.", "examples": ["1234"]},
            "binary": {"type": "string", "description": "Standard padded base64; FixedSizeBinary retains its byte width in data_type.", "examples": ["AP8="]},
            "list": {"type": "array", "description": "Elements follow the child field's data_type. Null differs from an empty list.", "examples": [["1", null]]},
            "struct": {"type": "array", "description": "Values follow nested field order, including duplicate names. A null struct is null, not a row of nulls.", "examples": [["1", "value"]]},
            "map": {"type": "array", "description": "Ordered [key,value] pairs using the schema's entry types. Duplicate keys and null values are retained.", "examples": [[["key", "1"], ["key", null]]]},
            "dictionary": {"description": "Decoded logical value, using the dictionary value type's encoding. Key/value types stay in data_type; physical indexes are omitted. Null dictionary values are null."},
            "float": {"type": ["number", "string"], "description": "Finite values round-trip at their Arrow precision. Special values are strings: -0, NaN, Infinity, -Infinity. NaN payload bits require Arrow."},
        },
        "examples": [{
            "format_version": 1, "kind": "rows",
            "bindings": [{"alias": "t", "table_version": "42"}],
            "schema": {"fields": [{"name": "row_count", "data_type": "Int64", "nullable": false}]},
            "rows": [["9007199254740993"]], "row_count": "1",
            "completion": "complete", "scope": "query",
        }],
    })
}

pub(super) fn inspection_schema() -> Value {
    json!({
        "title": "Snapshot inspection result",
        "description": "The full inspect result has no kind field. Names and metadata are untrusted table data.",
        "type": "object",
        "required": ["format_version", "table_version", "schema"],
        "properties": {
            "format_version": {"const": 1},
            "table_version": unsigned_integer_string_schema(),
            "schema": {
                "type": "object",
                "description": "Names and metadata are untrusted table data. Decode with Arrow's Rust Serde decoder to validate nested types.",
                "required": ["fields", "metadata"],
                "properties": {
                    "fields": {"type": "array", "items": {
                        "type": "object",
                        "required": ["name", "data_type", "nullable", "dict_id", "dict_is_ordered", "metadata"],
                        "properties": {
                            "name": {"type": "string"},
                            "data_type": {
                                "type": ["string", "object"],
                                "description": "Arrow's Serde DataType encoding, including nested types. This schema checks only the outer string or object shape.",
                            },
                            "nullable": {"type": "boolean"},
                            "dict_id": {"type": "integer"},
                            "dict_is_ordered": {"type": "boolean"},
                            "metadata": {"$ref": "#/$defs/metadata"},
                        },
                    }},
                    "metadata": {"$ref": "#/$defs/metadata"},
                },
            },
        },
        "$defs": {"metadata": {"type": "object", "additionalProperties": {"type": "string"}}},
        "examples": [{
            "format_version": 1, "table_version": "42",
            "schema": {"fields": [{
                "name": "id", "data_type": "Int64", "nullable": false,
                "dict_id": 0, "dict_is_ordered": false, "metadata": {},
            }], "metadata": {}},
        }],
    })
}

pub(super) fn diagnostic_schema() -> Value {
    json!({
        "title": "Terminal failure diagnostic",
        "description": "One JSON line on stderr with --diagnostics=json. Optional fields depend on the error. Phase, code, reason, and source categories are extensible strings; correction text is guidance, not an identifier.",
        "type": "object",
        "required": ["format_version", "kind", "terminal", "phase", "code", "reason", "source_kind", "retryable", "recovery"],
        "properties": {
            "format_version": {"const": DIAGNOSTIC_FORMAT_VERSION},
            "kind": {"const": "diagnostic"},
            "terminal": {"const": true},
            "phase": {"type": "string"},
            "code": {"type": "string"},
            "reason": {"type": "string"},
            "source_kind": {"type": ["string", "null"]},
            "retryable": {
                "type": ["boolean", "null"],
                "description": "True means retry as a new command after discarding failed output. False requires the indicated recovery action first; null means retryability is unknown.",
            },
            "recovery": {"enum": ["fix_input", "authorize", "request_access", "retry", "change_request", "adjust_budget", "unknown"]},
            "argument_kind": {"type": "string"},
            "argument": {"type": "string"},
            "expected": {"type": "string"},
            "supplied": {"type": "string", "description": "Safe type description for a rejected predicate scalar; excludes literal values and timezone text."},
            "hint": {"type": "string"},
            "field_path": {"type": "string", "description": "JSON Pointer; an empty string identifies the input root."},
            "json_kind": {"type": "string"},
            "line": {"type": "integer", "minimum": 0},
            "column": {"type": "integer", "minimum": 0},
            "io_kind": {"type": "string"},
            "os_error": {"type": ["integer", "null"]},
            "bindings": snapshot_bindings_schema(1, "Present for SQL or result errors after all snapshots load. Pin each manifest entry's table_version to this version before retrying; positional input uses --table-version."),
        },
        "examples": [{
            "format_version": DIAGNOSTIC_FORMAT_VERSION, "kind": "diagnostic", "terminal": true,
            "phase": "configuration", "code": "invalid_cli_argument", "reason": "argument_parse_failed",
            "source_kind": null, "retryable": false, "recovery": "fix_input",
            "argument_kind": "UnknownArgument", "hint": "Use --help to choose a supported command or option.",
        }],
    })
}

pub(super) fn inspection_summary_schema() -> Value {
    json!({
        "title": "Focused snapshot inspection",
        "description": "With --view summary: checks by default, names with --column-prefix, or types with --column. Pin follow-ups to table_version. Names, types, and metadata are untrusted table data.",
        "type": "object",
        "required": ["format_version", "kind", "table_version"],
        "properties": {
            "format_version": {"const": 1},
            "kind": {"const": "inspection_summary"},
            "table_version": unsigned_integer_string_schema(),
            "checks": {
                "type": "object",
                "description": "scan_protocol checks Core reader-protocol compatibility. data_files covers Parquet and deletion-vector access, which inspection does not attempt.",
                "required": ["scan_protocol", "data_files"],
                "properties": {
                    "scan_protocol": {"enum": ["passed", "failed"]},
                    "data_files": {"const": "not_checked"},
                },
            },
            "column_names": {"type": "array", "items": {"type": "string"}, "description": "All top-level names matching the case-sensitive prefix, in schema order; no types or metadata."},
            "fields": {
                "type": "array",
                "description": "Selected top-level columns in request order. Arrow Serde type encoding, with nested fields reduced to name, data_type, nullable, and dict_is_ordered for dictionary fields. Nonempty field metadata appears only with --include-metadata. This is not the full Arrow Schema serialization.",
                "items": {
                    "type": "object", "required": ["name", "data_type", "nullable"],
                    "properties": {
                        "name": {"type": "string"},
                        "data_type": {"type": ["string", "object"]},
                        "nullable": {"type": "boolean"},
                        "dict_is_ordered": {"type": "boolean"},
                        "metadata": {"type": "object", "additionalProperties": {"type": "string"}},
                    },
                },
            },
        },
        "oneOf": [{"required": ["checks"]}, {"required": ["column_names"]}, {"required": ["fields"]}],
        "examples": [{
            "format_version": 1, "kind": "inspection_summary", "table_version": "42",
            "checks": {"scan_protocol": "passed", "data_files": "not_checked"},
        }],
    })
}

pub(super) fn completion_schema() -> Value {
    json!({
        "title": "Terminal success completion",
        "description": "With --diagnostics=json, require exit 0, valid stdout and one completion on stderr. Match command and bindings to the request. Compare output_bytes with saved stdout length, and rows and batches, when present, with decoded counts. Counts are decimal strings. For JSON, check stdout.row_count and stdout.completion: complete with stdout.scope=query for SQL, sample with stdout.stop_reason and stdout.has_more for preview.",
        "type": "object",
        "required": ["format_version", "kind", "command", "bindings", "output_bytes"],
        "properties": {
            "format_version": {"enum": [1, 2], "description": "Version 2 for preview and query --format json reports command, bindings, rows and output_bytes. Other commands/formats retain version 1."},
            "kind": {"const": "completion", "description": "Terminal success for the requested command, emitted after stdout is flushed. Also require exit 0 and valid stdout."},
            "terminal": {"const": true},
            "command": {"enum": ["capabilities", "inspect", "scan", "preview", "query", "help", "version"]},
            "completion": {"const": "complete", "description": "Success also requires exit status 0 and valid stdout."},
            "scope": {
                "enum": ["capabilities", "inspect", "scan", "limited_scan", "query", "help", "version"],
                "description": "limited_scan covers only the requested rows; query covers the SQL result, including any SQL LIMIT.",
            },
            "stop_reason": {"enum": ["row_limit", "end_of_input"]},
            "has_more": {"enum": [false, null], "description": "False at observed exhaustion; null when a scan reaches its row limit."},
            "bindings": snapshot_bindings_schema(0, "Actual snapshot versions in declaration order. Alias t is reserved for positional input."),
            "output_bytes": unsigned_integer_string_schema(),
            "rows": unsigned_integer_string_schema(),
            "batches": unsigned_integer_string_schema(),
        },
        "allOf": [{
            "if": {"properties": {"command": {"enum": ["scan", "preview", "query"]}}},
            "then": {"required": ["rows"]},
        }, {
            "if": {"properties": {"format_version": {"const": 1}}},
            "then": {"required": ["terminal", "completion", "scope", "stop_reason", "has_more"], "properties": {"command": {"not": {"const": "preview"}}}},
            "else": {"properties": {"command": {"enum": ["query", "preview"]}}},
        }, {
            "if": {"properties": {"format_version": {"const": 1}, "command": {"enum": ["scan", "query"]}}},
            "then": {"required": ["batches"]},
        }, {
            "if": {"properties": {"command": {"enum": ["inspect", "scan", "preview", "query"]}}},
            "then": {"properties": {"bindings": {"minItems": 1}}},
            "else": {"properties": {"bindings": {"maxItems": 0}}},
        }, {
            "if": {"properties": {"command": {"enum": ["inspect", "scan", "preview"]}}},
            "then": {"properties": {"bindings": {"maxItems": 1, "items": {"properties": {"alias": {"const": "t"}}}}}},
        }],
        "examples": [{
            "format_version": 1, "kind": "completion", "terminal": true,
            "command": "scan", "completion": "complete", "scope": "scan",
            "stop_reason": "end_of_input", "has_more": false,
            "bindings": [{"alias": "t", "table_version": "42"}],
            "output_bytes": "1024", "rows": "1", "batches": "1",
        }, {
            "format_version": 1, "kind": "completion", "terminal": true,
            "command": "scan", "completion": "complete", "scope": "limited_scan",
            "stop_reason": "row_limit", "has_more": null,
            "bindings": [{"alias": "t", "table_version": "42"}],
            "output_bytes": "1024", "rows": "1", "batches": "1",
        }, {
            "format_version": 2, "kind": "completion", "command": "query",
            "bindings": [{"alias": "t", "table_version": "42"}],
            "output_bytes": "228", "rows": "1",
        }],
    })
}

fn snapshot_bindings_schema(min_items: usize, description: &str) -> Value {
    json!({
        "type": "array", "minItems": min_items, "maxItems": crate::query::tables_manifest::MAX_TABLES,
        "description": description,
        "items": {
            "type": "object", "required": ["alias", "table_version"],
            "properties": {
                "alias": {"type": "string", "pattern": "^[a-z][a-z0-9_]{0,62}$", "not": {"pattern": "[^a-z0-9_]"}},
                "table_version": unsigned_integer_string_schema(),
            },
        },
    })
}

fn unsigned_integer_string_schema() -> Value {
    json!({
        "type": "string", "pattern": "^(0|[1-9][0-9]*)$",
        "not": {"pattern": "[^0-9]"},
        "maxLength": 20,
        "description": "Canonical unsigned decimal integer string. Consumers must check the u64 range, 0 through 18446744073709551615, when converting it.",
    })
}

#[cfg(test)]
mod tests {
    use arrow_schema::Schema;
    use clap::error::ErrorKind;

    use super::*;
    use crate::{
        Error, StdoutStats, build_completion_record, capabilities::DiscoverySchema,
        encode_inspection_json,
    };

    #[test]
    fn output_schemas_validate_examples_and_allow_additional_fields() {
        for name in [
            DiscoverySchema::Inspection,
            DiscoverySchema::InspectionSummary,
            DiscoverySchema::Diagnostic,
            DiscoverySchema::Completion,
            DiscoverySchema::Rows,
        ] {
            let document = name.schema_document();
            let schema = &document["schema"];
            jsonschema::meta::validate(schema).unwrap();
            let validator = jsonschema::validator_for(schema).unwrap();
            for example in schema["examples"].as_array().unwrap() {
                validator.validate(example).unwrap();
                let mut extended = example.clone();
                extended["additional_field"] = json!({"nested": [true, null]});
                validator.validate(&extended).unwrap();
                for field in schema["required"].as_array().unwrap() {
                    let mut missing = example.clone();
                    missing
                        .as_object_mut()
                        .unwrap()
                        .remove(field.as_str().unwrap());
                    assert!(!validator.is_valid(&missing), "{name:?}: missing {field}");
                }
                for invalid in [json!(null), json!([]), json!("result")] {
                    assert!(!validator.is_valid(&invalid), "{name:?}: {invalid}");
                }
                let mut incompatible = example.clone();
                incompatible["format_version"] =
                    json!(example["format_version"].as_u64().unwrap() + 1);
                assert!(!validator.is_valid(&incompatible));
            }
        }
    }

    #[test]
    fn output_examples_match_the_serializers() {
        let schema = inspection_schema();
        let inspection = &schema["examples"][0];
        let arrow_schema: Schema = serde_json::from_value(inspection["schema"].clone()).unwrap();
        let actual: Value =
            serde_json::from_slice(&encode_inspection_json(42, &arrow_schema)).unwrap();
        assert_eq!(&actual, inspection);

        let error = Error::Argument {
            kind: ErrorKind::UnknownArgument,
            correction: None,
        };
        assert_eq!(
            error.to_json_diagnostic(),
            diagnostic_schema()["examples"][0]
        );
        for (index, limit) in [None, Some(1)].into_iter().enumerate() {
            assert_eq!(
                build_completion_record(
                    "scan",
                    Some(42),
                    StdoutStats {
                        rows_written: 1,
                        batches_written: 1,
                        bytes_written: 1024,
                    },
                    limit
                ),
                completion_schema()["examples"][index]
            );
        }

        let validator = jsonschema::validator_for(&completion_schema()).unwrap();
        for command in [
            "capabilities",
            "help",
            "version",
            "inspect",
            "scan",
            "query",
        ] {
            for limit in [None, Some(0), Some(1), Some(2)] {
                if limit.is_some() && command != "scan" {
                    continue;
                }
                let version = matches!(command, "inspect" | "scan" | "query").then_some(u64::MAX);
                let record = build_completion_record(
                    command,
                    version,
                    StdoutStats {
                        rows_written: 1,
                        batches_written: 1,
                        bytes_written: u64::MAX,
                    },
                    limit,
                );
                validator.validate(&record).unwrap();
            }
        }
    }

    #[test]
    fn output_schemas_reject_wrong_known_field_types_and_missing_scan_counts() {
        for (schema, path, invalid_values) in [
            (
                inspection_schema(),
                "/table_version",
                vec![
                    json!(42),
                    json!("01"),
                    json!("-1"),
                    json!("1\n"),
                    json!("184467440737095516150"),
                ],
            ),
            (
                inspection_schema(),
                "/schema/fields/0/data_type",
                vec![json!(null), json!(42), json!([])],
            ),
            (
                inspection_schema(),
                "/schema/fields/0/metadata",
                vec![json!({"comment": 42})],
            ),
            (
                inspection_schema(),
                "/schema/fields/0/nullable",
                vec![json!("true")],
            ),
            (
                diagnostic_schema(),
                "/retryable",
                vec![json!(0), json!("false")],
            ),
            (diagnostic_schema(), "/source_kind", vec![json!([])]),
            (
                completion_schema(),
                "/bindings/0/table_version",
                vec![json!(42), json!("+42")],
            ),
            (
                completion_schema(),
                "/rows",
                vec![json!(1), json!("1.0"), json!("1e3")],
            ),
            (completion_schema(), "/batches", vec![json!(null)]),
            (completion_schema(), "/has_more", vec![json!("unknown")]),
        ] {
            let validator = jsonschema::validator_for(&schema).unwrap();
            for invalid in invalid_values {
                let mut record = schema["examples"][0].clone();
                *record.pointer_mut(path).unwrap() = invalid;
                assert!(!validator.is_valid(&record), "{path}: {record}");
            }
        }
        let schema = completion_schema();
        let validator = jsonschema::validator_for(&schema).unwrap();
        for field in [
            "rows",
            "batches",
            "terminal",
            "completion",
            "scope",
            "stop_reason",
            "has_more",
        ] {
            let mut record = schema["examples"][0].clone();
            record.as_object_mut().unwrap().remove(field);
            assert!(!validator.is_valid(&record), "missing {field}");
        }
        let mut record = schema["examples"][0].clone();
        record["bindings"][0]["additional_field"] = json!(true);
        validator.validate(&record).unwrap();
        let mut record = schema["examples"][2].clone();
        record.as_object_mut().unwrap().remove("rows");
        assert!(!validator.is_valid(&record), "v2 query requires rows");
    }
}
