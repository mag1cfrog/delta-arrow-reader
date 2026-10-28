//! Interpret planned work without mistaking it for an executed query.
//!
//! A summary counts selectable operations. For example, node 0 is the physical
//! root; scan 0 is the first logical table scan, not necessarily physical node 0.
//! Statistics describe planner knowledge and selected files, not measured costs.

use serde_json::{Value, json};

use super::{snapshot_bindings_schema, unsigned_integer_string_schema};

pub(in crate::capabilities::schemas) fn query_plan_schema() -> Value {
    let position = json!({"type": "integer", "minimum": 0});
    let precision = json!({
        "type": "object", "required": ["precision"],
        "description": "Planner knowledge, not observed execution. exact/estimated have a decimal-string value; unknown has no value.",
        "properties": {
            "precision": {"enum": ["exact", "estimated", "unknown"]},
            "value": unsigned_integer_string_schema(),
        },
        "if": {"properties": {"precision": {"const": "unknown"}}},
        "then": {"not": {"required": ["value"]}},
        "else": {"required": ["value"]},
    });
    let statistics = json!({
        "type": "object", "required": ["rows", "relation_bytes"],
        "description": "Planner rows and relation bytes, not storage IO, duration or actual result size. selected_files appears only on a Delta scan; estimates are pre-filter and pre-deletion-vector. Unknown means unavailable, not zero.",
        "properties": {
            "rows": precision, "relation_bytes": precision,
            "selected_files": {
                "type": "object", "required": ["files", "input_rows_estimate", "input_bytes_estimate"],
                "description": "Files planned from snapshot metadata, not files read. Input row/byte estimates may be null. They do not account for row filters or deletion vectors.",
                "properties": {
                    "files": unsigned_integer_string_schema(),
                    "input_rows_estimate": {"type": ["string", "null"], "pattern": "^[0-9]+$", "description": "Unsigned decimal integer string; null when unavailable."},
                    "input_bytes_estimate": {"type": ["string", "null"], "pattern": "^[0-9]+$", "description": "Unsigned decimal integer string; null when unavailable."},
                },
            },
        },
    });
    let node = json!({
        "type": "object", "required": ["id"],
        "description": "--node N returns operator/children/column_count. --field N (repeatable) and --statistics can be combined; without --node they select root 0. Field positions refer to this node's output, not the source table.",
        "properties": {
            "id": position,
            "operator": {"type": "string", "description": "Built-in physical operator name; no expressions or raw plan text."},
            "children": {"type": "array", "items": position},
            "column_count": position,
            "fields": {
                "type": "array", "items": {
                    "type": "object", "required": ["index", "data_type", "nullable"],
                    "properties": {
                        "index": position, "data_type": {"type": ["string", "object"]},
                        "nullable": {"type": "boolean"}, "dict_is_ordered": {"type": "boolean"},
                    },
                },
                "description": "Exact Arrow Serde types in selection order. Top-level names and all metadata are omitted. Nested names must be fixed structural names (item/element/entries/key/value/keys/values/run_ends); timezones must be null or UTC. Other type text fails before stdout because it may originate in SQL literals. Increasing the budget cannot fix that; select another field or omit --field.",
            },
            "statistics": statistics,
        },
    });
    json!({
        "title": "Query plan selection",
        "description": "explain returns counts by default, or selected node and/or scan details. No result rows execute; metadata IO and planning do occur. IDs are zero-based preorder positions within separate physical-node and logical-scan lists. Keep SQL/options and pinned bindings for follow-ups. Select nested details with dots, e.g. node.statistics.",
        "type": "object", "required": ["format_version", "kind", "bindings", "execution"],
        "properties": {
            "format_version": {"const": 1}, "kind": {"const": "query_plan"},
            "execution": {"const": "not_started"},
            "bindings": snapshot_bindings_schema(1, "Actual snapshots in declaration order, including unused inputs. Pin every alias before follow-ups. No locations or credentials are returned."),
            "node_count": position, "scan_count": position, "column_count": position,
            "node": node,
            "scan": {
                "type": "object", "required": ["id", "binding", "projection", "source_column_count", "pushed_filters"],
                "description": "--scan N selects a logical table scan, separate from physical nodes. Self-joins may have multiple scans of one binding. No predicate expressions or literals are returned.",
                "properties": {
                    "id": position, "binding": {"type": "string"},
                    "projection": {"type": ["array", "null"], "items": position, "description": "Source column positions; null means all columns and [] means zero columns."},
                    "source_column_count": position,
                    "pushed_filters": {
                        "type": "object", "required": ["exact", "inexact"],
                        "properties": {"exact": position, "inexact": position},
                        "description": "Filters offered to this scan: exact is fully enforced, inexact needs a residual filter. Other filters remain in the physical plan. Counts exclude their expressions and values.",
                    },
                    "fetch": unsigned_integer_string_schema(),
                },
            },
        },
        "oneOf": [
            {"required": ["node_count", "scan_count", "column_count"]},
            {"anyOf": [{"required": ["node"]}, {"required": ["scan"]}]},
        ],
    })
}
