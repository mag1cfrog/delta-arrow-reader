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
            "retryable": {"type": ["boolean", "null"]},
            "recovery": {"enum": ["fix_input", "authorize", "request_access", "retry", "change_request", "adjust_budget", "unknown"]},
            "argument_kind": {"type": "string"},
            "argument": {"type": "string"},
            "expected": {"type": "string"},
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
            "completion": {"const": "complete"},
            "scope": {"enum": ["capabilities", "inspect", "scan", "limited_scan", "query", "help", "version"]},
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
mod tests;
