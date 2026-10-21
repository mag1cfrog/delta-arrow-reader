//! The shapes and literal encodings accepted in a predicate file.
//!
//! Comparisons, null checks, and boolean groups have different required fields.
//! For example, `is_null` needs a column but no value; `eq` also needs a typed
//! scalar. The recursive schema describes those alternatives. Tree size, numeric
//! conversion, and table-dependent checks are listed in the discovery response.

use serde_json::{Map, Value, json};

pub(in crate::input) fn predicate_schema() -> Value {
    json!({
        "title": "Row predicate file",
        "$ref": "#/$defs/predicate",
        "$defs": {
            "predicate": {"oneOf": [
                object_schema_with_required_fields(&[
                    ("op", json!({"enum": ["constant"]})),
                    ("value", json!({"type": "boolean"})),
                ]),
                object_schema_with_required_fields(&[
                    ("op", json!({"enum": ["eq", "ne", "lt", "le", "gt", "ge"]})),
                    ("column", json!({"type": "string"})),
                    ("value", json!({"$ref": "#/$defs/scalar"})),
                ]),
                object_schema_with_required_fields(&[
                    ("op", json!({"enum": ["is_null", "is_not_null"]})),
                    ("column", json!({"type": "string"})),
                ]),
                object_schema_with_required_fields(&[
                    ("op", json!({"enum": ["and", "or"]})),
                    ("args", json!({"type": "array", "items": {"$ref": "#/$defs/predicate"}})),
                ]),
                object_schema_with_required_fields(&[
                    ("op", json!({"enum": ["not"]})),
                    ("arg", json!({"$ref": "#/$defs/predicate"})),
                ]),
            ]},
            "scalar": {"oneOf": [
                object_schema_with_required_fields(&[
                    ("type", json!({"enum": ["boolean"]})),
                    ("value", json!({"type": "boolean"})),
                ]),
                object_schema_with_required_fields(&[
                    ("type", json!({"enum": ["int8"]})),
                    ("value", integer_string_schema(i8::MIN, i8::MAX)),
                ]),
                object_schema_with_required_fields(&[
                    ("type", json!({"enum": ["int16"]})),
                    ("value", integer_string_schema(i16::MIN, i16::MAX)),
                ]),
                object_schema_with_required_fields(&[
                    ("type", json!({"enum": ["int32", "date32"]})),
                    ("value", integer_string_schema(i32::MIN, i32::MAX)),
                ]),
                object_schema_with_required_fields(&[
                    ("type", json!({"enum": ["int64"]})),
                    ("value", integer_string_schema(i64::MIN, i64::MAX)),
                ]),
                object_schema_with_required_fields(&[
                    ("type", json!({"enum": ["float32", "float64"]})),
                    ("value", json!({"type": "number", "description": "Must remain finite after conversion to the declared floating-point width."})),
                ]),
                object_schema_with_required_fields(&[
                    ("type", json!({"enum": ["utf8", "large_utf8"]})),
                    ("value", json!({"type": "string"})),
                ]),
                object_schema_with_required_fields(&[
                    ("type", json!({"enum": ["binary", "large_binary"]})),
                    ("value", json!({"$ref": "#/$defs/bytes"})),
                ]),
                object_schema_with_required_fields(&[
                    ("type", json!({"enum": ["fixed_size_binary"]})),
                    ("value", json!({"$ref": "#/$defs/bytes"})),
                    ("size", json!({"type": "integer", "minimum": 1, "maximum": i32::MAX,
                        "description": "Must equal the byte array length."})),
                ]),
                object_schema_with_required_fields(&[
                    ("type", json!({"enum": ["timestamp_us"]})),
                    ("value", integer_string_schema(i64::MIN, i64::MAX)),
                    ("timezone", json!({"type": ["string", "null"], "minLength": 1})),
                ]),
                object_schema_with_required_fields(&[
                    ("type", json!({"enum": ["decimal128"]})),
                    ("value", integer_string_schema(i128::MIN, i128::MAX)),
                    ("precision", json!({"type": "integer", "minimum": 0, "maximum": u8::MAX})),
                    ("scale", json!({"type": "integer", "minimum": i8::MIN, "maximum": i8::MAX})),
                ]),
            ]},
            "bytes": {"type": "array", "items": {"type": "integer", "minimum": 0, "maximum": u8::MAX}},
        },
        "examples": [
            {"op": "ge", "column": "id", "value": {"type": "int32", "value": "10"}},
            {"op": "and", "args": [
                {"op": "is_not_null", "column": "id"},
                {"op": "not", "arg": {"op": "eq", "column": "id", "value": {"type": "int32", "value": "0"}}},
            ]},
        ],
    })
}

fn object_schema_with_required_fields(properties: &[(&str, Value)]) -> Value {
    json!({
        "type": "object",
        "properties": properties.iter().map(|(name, schema)| ((*name).to_owned(), schema.clone())).collect::<Map<_, _>>(),
        "required": properties.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        "additionalProperties": false,
    })
}

fn integer_string_schema(
    minimum: impl std::fmt::Display,
    maximum: impl std::fmt::Display,
) -> Value {
    json!({
        "type": "string",
        "pattern": "^(0|-?[1-9][0-9]*)$",
        // Some regex implementations let '$' match before a final newline.
        "not": {"pattern": "[^0-9-]"},
        "description": format!("Canonical decimal integer string from {minimum} to {maximum}, inclusive. The parser checks this range."),
    })
}
