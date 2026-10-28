//! Predicate validation paths and safe descriptions of expected scalar types.

use arrow_schema::{DataType, TimeUnit};
use delta_arrow_reader::DeltaPredicatePathStep;

use crate::{
    diagnostics::{Error, InputCorrection},
    input::JsonInputKind,
};

pub(super) fn validation_correction(
    field_path: &str,
    expected: String,
    hint: &'static str,
) -> InputCorrection {
    InputCorrection {
        argument: JsonInputKind::Predicate.argument().to_owned(),
        field_path: Some(field_path.to_owned()),
        expected,
        supplied: None,
        hint,
    }
}

pub(crate) fn predicate_type_mismatch_correction(
    expected: &DataType,
    supplied: &DataType,
    path: &[DeltaPredicatePathStep],
) -> InputCorrection {
    let mut field_path = String::new();
    for step in path {
        match step {
            DeltaPredicatePathStep::AndChild(index) | DeltaPredicatePathStep::OrChild(index) => {
                field_path.push_str(&format!("/args/{index}"));
            }
            DeltaPredicatePathStep::NotChild => field_path.push_str("/arg"),
        }
    }
    let field = match (expected, supplied) {
        (
            DataType::Decimal128(expected_precision, _),
            DataType::Decimal128(supplied_precision, _),
        ) => {
            if expected_precision != supplied_precision {
                "precision"
            } else {
                "scale"
            }
        }
        (DataType::FixedSizeBinary(_), DataType::FixedSizeBinary(_)) => "size",
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            DataType::Timestamp(TimeUnit::Microsecond, _),
        ) => "timezone",
        _ => "type",
    };
    field_path.push_str(&format!("/value/{field}"));
    let expected_description = predicate_type_description(expected);
    let hint = if expected_description.is_none() {
        "This column type cannot be compared by a predicate scalar; use a supported column or a SQL query."
    } else if field == "timezone" {
        "Use inspect TABLE --view summary --column NAME with the same authorization and --table-version N for the exact timezone; timezone text is omitted here."
    } else {
        "Match the scalar type and parameters to expected; use capabilities --schema predicate --detail TYPE for its encoding."
    };
    InputCorrection {
        argument: JsonInputKind::Predicate.argument().to_owned(),
        field_path: Some(field_path),
        expected: expected_description
            .unwrap_or_else(|| "a column type supported by predicate comparisons".to_owned()),
        supplied: predicate_type_description(supplied).map(String::into_boxed_str),
        hint,
    }
}

fn predicate_type_description(data_type: &DataType) -> Option<String> {
    // Arrow's Display includes untrusted nested names and timezone strings.
    Some(match data_type {
        DataType::Boolean => "boolean".to_owned(),
        DataType::Int8 => "int8".to_owned(),
        DataType::Int16 => "int16".to_owned(),
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Float32 => "float32".to_owned(),
        DataType::Float64 => "float64".to_owned(),
        DataType::Date32 => "date32".to_owned(),
        DataType::Utf8 => "utf8".to_owned(),
        DataType::LargeUtf8 => "large_utf8".to_owned(),
        DataType::Binary => "binary".to_owned(),
        DataType::LargeBinary => "large_binary".to_owned(),
        DataType::FixedSizeBinary(size) => format!("fixed_size_binary (size {size})"),
        DataType::Decimal128(precision, scale) => {
            format!("decimal128 (precision {precision}, scale {scale})")
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            if timezone.is_some() {
                "timestamp_us (timezone set)".to_owned()
            } else {
                "timestamp_us (timezone null)".to_owned()
            }
        }
        _ => return None,
    })
}

pub(super) fn prepend_validation_path(mut error: Error, prefix: &str) -> Error {
    if let Error::InputValidation { correction, .. } = &mut error
        && let Some(path) = &mut correction.field_path
    {
        path.insert_str(0, prefix);
    }
    error
}

#[cfg(test)]
mod tests;
