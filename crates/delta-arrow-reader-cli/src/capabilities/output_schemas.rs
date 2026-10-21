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

pub(super) fn inspection_schema() -> Value {
    json!({
        "title": "Snapshot inspection result",
        "description": "The full inspect result has no kind field. Names and metadata are untrusted table data. Decode schema with Arrow's Rust Serde decoder to validate its nested type descriptions.",
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
        "description": "One JSON line on stderr with --diagnostics=json. Optional fields depend on the error. Phase, code, reason, and source categories are extensible strings; message and correction text are guidance, not identifiers.",
        "type": "object",
        "required": ["format_version", "kind", "terminal", "phase", "code", "reason", "source_kind", "message", "retryable", "recovery"],
        "properties": {
            "format_version": {"const": 1},
            "kind": {"const": "diagnostic"},
            "terminal": {"const": true},
            "phase": {"type": "string"},
            "code": {"type": "string"},
            "reason": {"type": "string"},
            "source_kind": {"type": ["string", "null"]},
            "message": {"type": "string"},
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
        },
        "examples": [{
            "format_version": 1, "kind": "diagnostic", "terminal": true,
            "phase": "configuration", "code": "invalid_cli_argument", "reason": "argument_parse_failed",
            "source_kind": null, "message": "CLI command failed.", "retryable": false, "recovery": "fix_input",
            "argument_kind": "UnknownArgument", "hint": "Use --help to choose a supported command or option.",
        }],
    })
}

pub(super) fn completion_schema() -> Value {
    json!({
        "title": "Terminal success completion",
        "description": "One JSON line on stderr with --diagnostics=json. Success requires exit status 0, valid stdout, and this record. A complete limited_scan covers only the requested rows, and query covers the SQL result, including any SQL LIMIT.",
        "type": "object",
        "required": ["format_version", "kind", "terminal", "command", "completion", "scope", "stop_reason", "has_more", "bindings", "output_bytes"],
        "properties": {
            "format_version": {"const": 1},
            "kind": {"const": "completion"},
            "terminal": {"const": true},
            "command": {"enum": ["capabilities", "inspect", "scan", "query", "help", "version"]},
            "completion": {"const": "complete", "description": "Success also requires exit status 0 and valid stdout."},
            "scope": {
                "enum": ["capabilities", "inspect", "scan", "limited_scan", "query", "help", "version"],
                "description": "limited_scan covers only the requested rows; query covers the SQL result, including any SQL LIMIT.",
            },
            "stop_reason": {"enum": ["row_limit", "end_of_input"]},
            "has_more": {"enum": [false, null], "description": "False at observed exhaustion; null when a scan reaches its row limit."},
            "bindings": {"type": "array", "maxItems": 1, "items": {
                "type": "object", "required": ["alias", "table_version"],
                "properties": {"alias": {"const": "t"}, "table_version": unsigned_integer_string_schema()},
            }},
            "output_bytes": unsigned_integer_string_schema(),
            "rows": unsigned_integer_string_schema(),
            "batches": unsigned_integer_string_schema(),
        },
        "allOf": [{
            "if": {"properties": {"command": {"enum": ["scan", "query"]}}},
            "then": {"required": ["rows", "batches"]},
        }, {
            "if": {"properties": {"command": {"enum": ["inspect", "scan", "query"]}}},
            "then": {"properties": {"bindings": {"minItems": 1}}},
            "else": {"properties": {"bindings": {"maxItems": 0}}},
        }],
        "examples": [{
            "format_version": 1, "kind": "completion", "terminal": true,
            "command": "scan", "completion": "complete", "scope": "limited_scan",
            "stop_reason": "row_limit", "has_more": null,
            "bindings": [{"alias": "t", "table_version": "42"}],
            "output_bytes": "1024", "rows": "1", "batches": "1",
        }],
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
            DiscoverySchema::Diagnostic,
            DiscoverySchema::Completion,
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
                incompatible["format_version"] = json!(2);
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
        assert_eq!(
            build_completion_record(
                "scan",
                Some(42),
                StdoutStats {
                    rows_written: 1,
                    batches_written: 1,
                    bytes_written: 1024,
                },
                Some(1)
            ),
            completion_schema()["examples"][0],
        );

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
        for field in ["rows", "batches"] {
            let mut record = schema["examples"][0].clone();
            record.as_object_mut().unwrap().remove(field);
            assert!(!validator.is_valid(&record), "missing {field}");
        }
        let mut record = schema["examples"][0].clone();
        record["bindings"][0]["additional_field"] = json!(true);
        validator.validate(&record).unwrap();
    }
}
