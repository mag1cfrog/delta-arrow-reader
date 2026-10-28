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

pub(super) mod artifacts;
pub(super) mod query_plan;
mod terminal;

use serde_json::{Value, json};

pub(super) use terminal::{completion_schema, diagnostic_schema};

pub(super) fn rows_schema() -> Value {
    json!({
        "title": "Typed JSON rows",
        "description": "Use schema.fields for column types and order; names may repeat. Arrow integers use exact decimal strings. Metadata is omitted.",
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

fn snapshot_bindings_schema(min_items: usize, description: &str) -> Value {
    json!({
        "type": "array", "minItems": min_items, "maxItems": crate::input::MAX_TABLES,
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
mod tests;
