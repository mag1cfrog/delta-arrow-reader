//! Error details for invalid conditions and typed literals in predicate JSON.
//!
//! A predicate's conditions form a tree. This module associates a schema error
//! with the condition or literal that contains it, using the same input definitions
//! as predicate parsing. Checks against the table, such as whether a referenced
//! column exists, are part of reader validation.
//!
//! For example, the second condition below uses a string as its boolean value:
//!
//! ```json
//! {
//!   "op": "and",
//!   "args": [
//!     {"op": "constant", "value": true},
//!     {"op": "constant", "value": "true"}
//!   ]
//! }
//! ```
//!
//! The correction points to `/args/1/value`, where array indices start at zero.
//! Changing `"true"` to `true` makes this predicate valid.

use serde::de::IgnoredAny;
use serde_json::value::RawValue;

use crate::{
    diagnostics::InputCorrection,
    input::schema_errors::{FieldLocation, ObjectFields},
};

pub(in crate::input) fn diagnose_schema_error(
    raw: &RawValue,
    location: &FieldLocation,
) -> Option<InputCorrection> {
    check_node_schema(raw, location, 1).err()
}

fn check_node_schema(
    raw: &RawValue,
    location: &FieldLocation,
    depth: usize,
) -> Result<(), InputCorrection> {
    // Check child nodes separately; tagged enums can erase their error paths.
    if depth > super::MAX_PREDICATE_DEPTH {
        return Err(location.invalid(
            format!(
                "at most {} predicate levels, including the root",
                super::MAX_PREDICATE_DEPTH
            ),
            "Reduce predicate nesting.",
        ));
    }
    let fields: ObjectFields<&RawValue> = parse_json_at(raw, location, "a JSON predicate object")?;
    use super::{
        ColumnReferenceFields, ComparisonFields, LogicalGroupFields, NegationFields,
        PredicateOperator, ValueFields,
    };
    let op = fields.deserialize_tag::<PredicateOperator>("op", location)?;
    match op {
        PredicateOperator::Constant => {
            fields.check_variant_fields::<ValueFields<bool>>("op", location)?
        }
        PredicateOperator::And | PredicateOperator::Or => {
            fields.check_variant_fields::<LogicalGroupFields<IgnoredAny>>("op", location)?;
            let args = location.field("args");
            let children: Vec<&RawValue> = parse_json_at(
                fields.required_field("args", location)?,
                &args,
                "an array of predicate objects",
            )?;
            for (index, child) in children.into_iter().enumerate() {
                check_node_schema(child, &args.index(index), depth + 1)?;
            }
        }
        PredicateOperator::Not => {
            fields.check_variant_fields::<NegationFields<IgnoredAny>>("op", location)?;
            check_node_schema(
                fields.required_field("arg", location)?,
                &location.field("arg"),
                depth + 1,
            )?;
        }
        PredicateOperator::IsNull | PredicateOperator::IsNotNull => {
            fields.check_variant_fields::<ColumnReferenceFields>("op", location)?
        }
        PredicateOperator::Eq
        | PredicateOperator::Ne
        | PredicateOperator::Lt
        | PredicateOperator::Le
        | PredicateOperator::Gt
        | PredicateOperator::Ge => {
            fields.check_variant_fields::<ComparisonFields<IgnoredAny>>("op", location)?;
            check_scalar_schema(
                fields.required_field("value", location)?,
                &location.field("value"),
            )?;
        }
    }
    Ok(())
}

fn check_scalar_schema(raw: &RawValue, location: &FieldLocation) -> Result<(), InputCorrection> {
    use super::{
        Decimal128Fields, FixedSizeBinaryFields, IntegerStringFields, ScalarType,
        TimestampMicrosecondFields, ValueFields,
    };
    let fields: ObjectFields<&RawValue> =
        parse_json_at(raw, location, "a typed JSON scalar object")?;
    match fields.deserialize_tag::<ScalarType>("type", location)? {
        ScalarType::Boolean => fields.check_variant_fields::<ValueFields<bool>>("type", location),
        ScalarType::Int8 => {
            fields.check_variant_fields::<IntegerStringFields<i8>>("type", location)
        }
        ScalarType::Int16 => {
            fields.check_variant_fields::<IntegerStringFields<i16>>("type", location)
        }
        ScalarType::Int32 | ScalarType::Date32 => {
            fields.check_variant_fields::<IntegerStringFields<i32>>("type", location)
        }
        ScalarType::Int64 => {
            fields.check_variant_fields::<IntegerStringFields<i64>>("type", location)
        }
        ScalarType::Float32 | ScalarType::Float64 => {
            fields.check_variant_fields::<ValueFields<f64>>("type", location)
        }
        ScalarType::Utf8 | ScalarType::LargeUtf8 => {
            fields.check_variant_fields::<ValueFields<String>>("type", location)
        }
        ScalarType::Binary | ScalarType::LargeBinary => {
            fields.check_variant_fields::<ValueFields<Vec<u8>>>("type", location)
        }
        ScalarType::FixedSizeBinary => {
            fields.check_variant_fields::<FixedSizeBinaryFields>("type", location)
        }
        ScalarType::TimestampUs => {
            fields.check_variant_fields::<TimestampMicrosecondFields>("type", location)
        }
        ScalarType::Decimal128 => fields.check_variant_fields::<Decimal128Fields>("type", location),
    }
}

fn parse_json_at<'de, T: serde::Deserialize<'de>>(
    raw: &'de RawValue,
    location: &FieldLocation,
    expected: &str,
) -> Result<T, InputCorrection> {
    serde_json::from_str(raw.get()).map_err(|_| {
        location.invalid(
            expected,
            "Supply a value matching this field's expected format.",
        )
    })
}
