//! JSON schemas for terminal diagnostics and successful command completions.

use serde_json::{Value, json};

use super::{snapshot_bindings_schema, unsigned_integer_string_schema};
use crate::diagnostics::DIAGNOSTIC_FORMAT_VERSION;

pub(in crate::capabilities::schemas) fn diagnostic_schema() -> Value {
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
            "cause": {
                "type": "object",
                "description": "Reader failure inside SQL; present only when a typed reader cause is available. Categories are extensible strings.",
                "required": ["phase", "code", "reason", "source_kind"],
                "properties": {
                    "phase": {"type": "string"},
                    "code": {"type": "string"},
                    "reason": {"type": "string"},
                    "source_kind": {"type": ["string", "null"]},
                },
            },
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
            "upstream_message": {"type": "string", "description": "Filtered Databricks CLI error excerpt, at most 1024 UTF-8 bytes. Guidance only; may contain workspace or table identifiers."},
            "upstream_message_truncated": {"const": true, "description": "The upstream excerpt was shortened."},
            "resource_details_command": {"type": "array", "items": {"type": "string"}, "description": "Optional focused lookup for SQL memory and spill policy."},
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

pub(in crate::capabilities::schemas) fn completion_schema() -> Value {
    json!({
        "title": "Terminal success completion",
        "description": "Require exit 0 and valid stdout. With --diagnostics=json, also require a stderr completion for the requested command. v1: Read completion, scope, stop_reason and has_more from stderr. v2 (JSON preview/query): Read result semantics from stdout: preview is a sample with stop_reason/has_more; query is complete with scope=query. v3 (--output/explain): Read the saved-result path from the receipt, or plan details from query_plan stdout. Counts and output_bytes are decimal strings.",
        "type": "object",
        "required": ["format_version", "kind", "command", "output_bytes"],
        "properties": {
            "format_version": {"enum": [1, 2, 3], "description": "Version 3: scan/query --output or explain; command and stdout byte count only. Version 2: preview/query JSON stdout. Other stdout uses version 1. JSON result counts and bindings are in the result; explain bindings are in stdout."},
            "kind": {"const": "completion", "description": "Terminal success for the requested command, emitted after stdout is flushed. Also require exit 0 and valid stdout."},
            "terminal": {"const": true},
            "command": {"enum": ["capabilities", "inspect", "scan", "preview", "query", "explain", "help", "version"]},
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
            "if": {"properties": {"format_version": {"enum": [1, 2]}, "command": {"enum": ["scan", "preview", "query"]}}},
            "then": {"required": ["rows"]},
        }, {
            "if": {"properties": {"format_version": {"enum": [1, 2]}}},
            "then": {"required": ["bindings"]},
        }, {
            "if": {"properties": {"format_version": {"const": 1}}},
            "then": {"required": ["terminal", "completion", "scope", "stop_reason", "has_more"], "properties": {"command": {"not": {"enum": ["preview", "explain"]}}}},
        }, {
            "if": {"properties": {"format_version": {"const": 2}}},
            "then": {"properties": {"command": {"enum": ["query", "preview"]}}},
        }, {
            "if": {"properties": {"format_version": {"const": 3}}},
            "then": {"properties": {"command": {"enum": ["query", "scan", "explain"]}}},
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
        }, {"format_version": 3, "kind": "completion", "command": "query", "output_bytes": "182"}],
    })
}
