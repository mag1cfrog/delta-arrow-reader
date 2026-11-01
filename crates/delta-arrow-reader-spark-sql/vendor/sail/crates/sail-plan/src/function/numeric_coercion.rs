//! Conditional values, VALUES columns and Decimal array membership share numeric coercion.
//! For example, DECIMAL(38,20) and BIGINT share DECIMAL(38,18), retaining enough
//! integral digits for either input.

use arrow::datatypes::DataType;
use datafusion_expr::type_coercion::binary::type_union_resolution;

pub(crate) fn common_numeric_type(data_types: &[DataType], ansi_mode: bool) -> Option<DataType> {
    let has_decimal = data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Decimal128(..)));
    let mut integral_digits = 0;
    let mut fractional_digits = 0;
    let mut has_float = false;
    for data_type in data_types {
        let (precision, scale) = match data_type {
            DataType::Decimal128(precision, scale) => (i16::from(*precision), i16::from(*scale)),
            DataType::Int8 => (3, 0),
            DataType::Int16 => (5, 0),
            DataType::Int32 => (10, 0),
            DataType::Int64 => (20, 0),
            DataType::Null => continue,
            DataType::Float32 | DataType::Float64 => {
                has_float = true;
                continue;
            }
            _ => return None,
        };
        integral_digits = integral_digits.max(precision - scale);
        fractional_digits = fractional_digits.max(scale);
    }
    if has_float
        && (has_decimal || (ansi_mode && data_types.iter().any(DataType::is_signed_integer)))
    {
        Some(DataType::Float64)
    } else if has_decimal {
        // Spark preserves integral digits when the common precision exceeds 38.
        let scale = fractional_digits.min(38 - integral_digits);
        Some(DataType::Decimal128(
            (integral_digits + scale) as u8,
            scale as i8,
        ))
    } else {
        type_union_resolution(data_types)
    }
}
