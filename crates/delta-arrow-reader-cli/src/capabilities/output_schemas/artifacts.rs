//! File receipts locate saved results; manifests record completion and replay inputs.

use serde_json::{Value, json};

use super::{snapshot_bindings_schema, unsigned_integer_string_schema};

pub(in crate::capabilities) fn result_artifact_schema() -> Value {
    json!({
        "title": "Saved result receipt", "type": "object",
        "description": "File output only. These references locate the saved result and its completion manifest.",
        "required": ["format_version", "kind", "result", "manifest"],
        "properties": {
            "format_version": {"const": 1}, "kind": {"const": "result_artifact"},
            "result": {"type": "string", "description": "Absolute local result path. Use the manifest's format to decode it."},
            "manifest": {"type": "string", "description": "Absolute local completion-manifest path. Check completion=complete and result.path against the receipt; read other fields as needed for the task."},
        },
        "examples": [{"format_version": 1, "kind": "result_artifact", "result": "/work/rows.arrow", "manifest": "/work/rows.arrow.manifest.json"}],
    })
}

pub(in crate::capabilities) fn result_manifest_schema() -> Value {
    let integer = unsigned_integer_string_schema();
    let digest = json!({"type": "string", "pattern": "^[a-f0-9]{64}$", "description": "SHA-256, lowercase hexadecimal."});
    json!({
        "title": "Result completion manifest", "type": "object",
        "description": "Completion and replay information for a saved result. A result without a matching complete manifest is uncommitted; publication is not a two-file transaction.",
        "required": ["format_version", "kind", "command", "completion", "scope", "stop_reason", "has_more", "bindings", "rows", "batches", "output_bytes", "result", "build", "replay"],
        "properties": {
            "format_version": {"const": 1}, "kind": {"const": "result_manifest"},
            "command": {"enum": ["scan", "query"]}, "completion": {"const": "complete"},
            "scope": {"enum": ["scan", "limited_scan", "query"], "description": "limited_scan covers the selected prefix; query covers the SQL result, including SQL LIMIT."},
            "stop_reason": {"enum": ["end_of_input", "row_limit"]},
            "has_more": {"enum": [false, null], "description": "Null when a scan reaches its row limit without lookahead; false at exhaustion."},
            "bindings": snapshot_bindings_schema(1, "Actual versions in declaration order. Replay must pin every alias to its recorded version; independent tables are not a transaction."),
            "rows": integer, "batches": integer, "output_bytes": integer,
            "result": {
                "type": "object", "required": ["path", "format", "sha256"],
                "properties": {"path": {"type": "string"}, "format": {"enum": ["arrow", "json"]}, "sha256": digest},
                "description": "Absolute path, encoding and hash of the complete file. JSON uses the rows schema; batches count source RecordBatches, which JSON does not retain as boundaries.",
            },
            "build": {
                "type": "object", "required": ["cli", "core", "datafusion"],
                "properties": {"cli": {"type": "string"}, "core": {"type": "string"}, "datafusion": {"type": "string"}},
            },
            "replay": {
                "type": "object", "required": ["working_directory", "options", "inputs"],
                "description": "Reuse the original input files with the recorded command/options in working_directory and choose a new --output. Pin manifest table entries to bindings; credential references must remain provisioned. Ordering and volatile SQL can still vary.",
                "properties": {
                    "working_directory": {"type": "string", "description": "Absolute directory for relative CLI paths; table-manifest references remain relative to that manifest."},
                    "options": replay_options_schema(),
                    "inputs": {
                        "type": "array", "maxItems": 2,
                        "description": "Digests of the exact bytes parsed. Never credential files or credential digests. Paths use working_directory.",
                        "items": {"type": "object", "required": ["argument", "path", "sha256"], "properties": {
                            "argument": {"enum": ["--sql-file", "--tables-file", "--predicate-file", "--execution-options-file"]},
                            "path": {"type": "string"}, "sha256": digest,
                        }},
                    },
                },
            },
        },
    })
}

fn replay_options_schema() -> Value {
    let path = json!({"type": ["string", "null"], "description": "Local file reference; null means omitted. Credential contents are never recorded."});
    let count = json!({"anyOf": [unsigned_integer_string_schema(), {"type": "null"}]});
    json!({
        "type": "object", "additionalProperties": false,
        "description": "Effective CLI options without -- prefixes; table is positional, column repeats. Numbers are decimal strings. Omit null/false values when replaying. Keep limits and authorization; use bindings for actual versions. These are references and settings, never SQL text or credential values.",
        "properties": {
            "table": {"type": ["string", "null"]}, "table-version": unsigned_integer_string_schema(),
            "tables-file": path, "sql-file": path, "predicate-file": path, "execution-options-file": path,
            "storage-options-file": path, "env-file": path,
            "auth-source": {"type": ["string", "null"]}, "databricks-profile": {"type": ["string", "null"]},
            "column": {"type": "array", "items": {"type": "string"}}, "no-columns": {"type": "boolean"},
            "target-partitions": count, "limit": count, "max-rows": count, "max-output-bytes": count,
            "memory-limit-bytes": count, "spill-limit-bytes": count, "spill-dir": path, "deadline-ms": count,
            "format": {"enum": ["arrow", "json"]},
        },
    })
}
