//! Convert Python filter groups to native predicates before scan planning.

use arrow::{
    array::types::{Decimal128Type, DecimalType},
    datatypes::{DataType, TimeUnit},
};
use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTable};
use pyo3::{
    exceptions::{PyException, PyOverflowError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyBytes, PyFloat, PyInt, PyList, PyString, PyTuple},
};

use crate::reader_error;

const FILTER_SHAPE_ERROR: &str =
    "filters must be a list of (column, operator, value) tuples or a list of lists of these tuples";

pub(crate) fn to_predicate(
    table: &DeltaTable,
    filters: &Bound<'_, PyAny>,
) -> PyResult<Option<DeltaPredicate>> {
    let filters = filters
        .cast::<PyList>()
        .map_err(|_| PyTypeError::new_err(FILTER_SHAPE_ERROR))?;
    if filters.is_empty() {
        return Ok(None);
    }
    let predicate = if filters.get_item(0)?.is_instance_of::<PyTuple>() {
        parse_and_group(table, filters)?
    } else {
        let groups = filters
            .iter()
            .map(|group| {
                let group = group
                    .cast::<PyList>()
                    .map_err(|_| PyTypeError::new_err(FILTER_SHAPE_ERROR))?;
                parse_and_group(table, group)
            })
            .collect::<PyResult<_>>()?;
        DeltaPredicate::Or(groups)
    };
    Ok(Some(predicate))
}

fn parse_and_group(table: &DeltaTable, group: &Bound<'_, PyList>) -> PyResult<DeltaPredicate> {
    group
        .iter()
        .map(|condition| parse_condition(table, &condition))
        .collect::<PyResult<_>>()
        .map(DeltaPredicate::And)
}

fn parse_condition(table: &DeltaTable, condition: &Bound<'_, PyAny>) -> PyResult<DeltaPredicate> {
    let condition = condition
        .cast::<PyTuple>()
        .map_err(|_| PyTypeError::new_err(FILTER_SHAPE_ERROR))?;
    if condition.len() != 3 {
        return Err(PyTypeError::new_err(FILTER_SHAPE_ERROR));
    }
    let column = condition
        .get_item(0)?
        .extract::<String>()
        .map_err(|_| PyTypeError::new_err("filter column names must be strings"))?;
    let operator = condition
        .get_item(1)?
        .extract::<String>()
        .map_err(|_| PyTypeError::new_err("filter operators must be strings"))?;
    let value = condition.get_item(2)?;
    let data_type = table.predicate_column_type(&column).map_err(|error| {
        reader_error(
            condition.py(),
            error.to_string(),
            error.phase().as_str(),
            error.code(),
        )
    })?;
    if matches!(operator.as_str(), "is" | "is not") {
        if !value.is_none() {
            return Err(PyValueError::new_err("null tests require None"));
        }
        return Ok(if operator == "is" {
            DeltaPredicate::IsNull { column }
        } else {
            DeltaPredicate::IsNotNull { column }
        });
    }
    let op = match operator.as_str() {
        "==" => DeltaComparison::Eq,
        "!=" => DeltaComparison::NotEq,
        "<" => DeltaComparison::Lt,
        "<=" => DeltaComparison::LtEq,
        ">" => DeltaComparison::Gt,
        ">=" => DeltaComparison::GtEq,
        _ => return Err(PyValueError::new_err("unsupported filter operator")),
    };
    Ok(DeltaPredicate::Compare {
        column,
        op,
        value: to_scalar(&data_type, &value)?,
    })
}

fn to_scalar(data_type: &DataType, value: &Bound<'_, PyAny>) -> PyResult<DeltaScalar> {
    if value.is_none() {
        return Err(PyValueError::new_err(
            "comparisons require a non-null value; use 'is' or 'is not' for null tests",
        ));
    }
    let is_integer = value.is_instance_of::<PyInt>() && !value.is_instance_of::<PyBool>();
    match data_type {
        DataType::Boolean if value.is_instance_of::<PyBool>() => {
            value.extract().map(DeltaScalar::Boolean)
        }
        DataType::Int8 if is_integer => value.extract().map(DeltaScalar::Int8),
        DataType::Int16 if is_integer => value.extract().map(DeltaScalar::Int16),
        DataType::Int32 if is_integer => value.extract().map(DeltaScalar::Int32),
        DataType::Int64 if is_integer => value.extract().map(DeltaScalar::Int64),
        DataType::Float32 | DataType::Float64 if value.is_instance_of::<PyFloat>() => {
            let number = value.extract::<f64>()?;
            if !number.is_finite() {
                return Err(PyValueError::new_err("filter float must be finite"));
            }
            if data_type == &DataType::Float32 {
                let number = number as f32;
                if !number.is_finite() {
                    return Err(PyValueError::new_err("filter float overflows Float32"));
                }
                Ok(DeltaScalar::Float32(number))
            } else {
                Ok(DeltaScalar::Float64(number))
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 if value.is_instance_of::<PyString>() => {
            let text = value
                .extract::<String>()
                .map_err(|_| PyValueError::new_err("filter string must be valid UTF-8"))?;
            Ok(if data_type == &DataType::Utf8 {
                DeltaScalar::Utf8(text)
            } else {
                DeltaScalar::LargeUtf8(text)
            })
        }
        DataType::Binary | DataType::LargeBinary if value.is_instance_of::<PyBytes>() => {
            let bytes = value.cast::<PyBytes>()?.as_bytes().to_vec();
            Ok(if data_type == &DataType::Binary {
                DeltaScalar::Binary(bytes)
            } else {
                DeltaScalar::LargeBinary(bytes)
            })
        }
        DataType::FixedSizeBinary(size) if value.is_instance_of::<PyBytes>() => {
            let bytes = value.cast::<PyBytes>()?.as_bytes();
            if *size <= 0 || usize::try_from(*size).ok() != Some(bytes.len()) {
                return Err(PyValueError::new_err(
                    "filter bytes must match the column's fixed size",
                ));
            }
            Ok(DeltaScalar::FixedSizeBinary {
                size: *size,
                value: bytes.to_vec(),
            })
        }
        DataType::Decimal128(precision, scale) => to_decimal(value, *precision, *scale),
        DataType::Date32 => {
            let datetime = value.py().import("datetime")?;
            let date = datetime.getattr("date")?;
            let value_type = value.get_type();
            if !value_type.is_subclass(&date)?
                || value_type.is_subclass(&datetime.getattr("datetime")?)?
            {
                return Err(PyTypeError::new_err(
                    "date filters require datetime.date, excluding datetime.datetime",
                ));
            }
            let epoch = date.call1((1970, 1, 1))?;
            // Use the base method so subclasses cannot change the stored date.
            let days = date
                .call_method1("__sub__", (value, epoch))?
                .getattr("days")?
                .extract()?;
            Ok(DeltaScalar::Date32(days))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let datetime = value.py().import("datetime")?.getattr("datetime")?;
            if !value.get_type().is_subclass(&datetime)? {
                return Err(PyTypeError::new_err(
                    "timestamp filters require datetime.datetime",
                ));
            }
            let epoch = datetime.call1((1970, 1, 1))?;
            // Use the base method to preserve stored fields and validate awareness.
            let duration = datetime
                .call_method1("__sub__", (value, epoch))
                .map_err(|error| {
                    if error.is_instance_of::<PyException>(value.py()) {
                        PyValueError::new_err(
                            "timestamp filter requires a naive datetime with no UTC offset",
                        )
                    } else {
                        error
                    }
                })?;
            let days = duration.getattr("days")?.extract::<i64>()?;
            let seconds = duration.getattr("seconds")?.extract::<i64>()?;
            let microseconds = duration.getattr("microseconds")?.extract::<i64>()?;
            Ok(DeltaScalar::TimestampMicrosecond {
                value: days * 86_400_000_000 + seconds * 1_000_000 + microseconds,
                timezone: None,
            })
        }
        _ => Err(PyTypeError::new_err(
            "filter value type is not supported for this column",
        )),
    }
    .map_err(|error| {
        if error.is_instance_of::<PyOverflowError>(value.py()) {
            PyOverflowError::new_err("filter value is out of range for the column type")
        } else {
            error
        }
    })
}

fn to_decimal(value: &Bound<'_, PyAny>, precision: u8, scale: i8) -> PyResult<DeltaScalar> {
    let decimal = value.py().import("decimal")?.getattr("Decimal")?;
    if !value.get_type().is_subclass(&decimal)? {
        return Err(PyTypeError::new_err(
            "decimal filters require decimal.Decimal",
        ));
    }
    // Read the stored value without subclass overrides or decimal-context arithmetic.
    let parts = decimal.call_method1("as_tuple", (value,))?;
    let sign = parts.get_item(0)?.extract::<u8>()?;
    let mut digits = parts.get_item(1)?.extract::<Vec<u8>>()?;
    let exponent = parts
        .get_item(2)?
        .extract::<i64>()
        .map_err(|_| PyValueError::new_err("filter decimal must be finite"))?;
    let overflow =
        || PyOverflowError::new_err("filter decimal does not fit a signed 128-bit integer");
    let mut shift = exponent
        .checked_add(i64::from(scale))
        .ok_or_else(overflow)?;
    // Remove trailing zeros before building an i128 coefficient, so an exact
    // value with many redundant decimal places does not overflow prematurely.
    while digits.last() == Some(&0) {
        digits.pop();
        shift = shift.checked_add(1).ok_or_else(overflow)?;
    }
    let unscaled = if digits.is_empty() {
        0
    } else {
        if shift < 0 {
            return Err(PyValueError::new_err(
                "filter decimal is not exact at the column scale",
            ));
        }
        let multiplier = u32::try_from(shift)
            .ok()
            .and_then(|shift| 10_i128.checked_pow(shift))
            .ok_or_else(overflow)?;
        let coefficient = digits.into_iter().try_fold(0_i128, |coefficient, digit| {
            let digit = i128::from(digit);
            let digit = if sign == 0 { digit } else { -digit };
            coefficient
                .checked_mul(10)
                .and_then(|coefficient| coefficient.checked_add(digit))
                .ok_or_else(overflow)
        })?;
        coefficient.checked_mul(multiplier).ok_or_else(overflow)?
    };
    Decimal128Type::validate_decimal_precision(unscaled, precision, scale)
        .map_err(|_| PyValueError::new_err("filter decimal exceeds the column precision"))?;
    Ok(DeltaScalar::Decimal128 {
        value: unscaled,
        precision,
        scale,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_decimals_with_negative_scales() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            let decimal = py.import("decimal")?.getattr("Decimal")?;
            // Delta schemas cannot express negative scales; the native type can.
            for (text, precision, scale, unscaled) in [
                ("12300", 5, -2, 123),
                ("-12300.0000", 5, -2, -123),
                ("9999900", 5, -2, 99999),
                ("-0E-999999999999999999", 5, -2, 0),
                ("1E128", 1, -128, 1),
            ] {
                let value = decimal.call1((text,))?;
                assert_eq!(
                    to_scalar(&DataType::Decimal128(precision, scale), &value)?,
                    DeltaScalar::Decimal128 {
                        value: unscaled,
                        precision,
                        scale
                    },
                );
            }
            for text in ["12301", "-12300.01", "10000000"] {
                let value = decimal.call1((text,))?;
                let error = to_scalar(&DataType::Decimal128(5, -2), &value)
                    .expect_err("inexact or oversized decimal must fail");
                assert!(error.is_instance_of::<PyValueError>(py));
            }
            Ok(())
        })
    }

    #[test]
    fn converts_large_and_fixed_width_scalars() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            // Delta schemas expose Utf8/Binary; test the other native variants here.
            let text = PyString::new(py, "a\0\u{1f642}");
            assert_eq!(
                to_scalar(&DataType::LargeUtf8, text.as_any())?,
                DeltaScalar::LargeUtf8("a\0\u{1f642}".into()),
            );
            let bytes = PyBytes::new(py, b"\0\xff");
            assert_eq!(
                to_scalar(&DataType::LargeBinary, bytes.as_any())?,
                DeltaScalar::LargeBinary(b"\0\xff".to_vec()),
            );
            assert_eq!(
                to_scalar(&DataType::FixedSizeBinary(2), bytes.as_any())?,
                DeltaScalar::FixedSizeBinary {
                    size: 2,
                    value: b"\0\xff".to_vec(),
                },
            );
            for size in [-1, 0, 1, 3] {
                let error = to_scalar(&DataType::FixedSizeBinary(size), bytes.as_any())
                    .expect_err("invalid fixed width must fail");
                assert!(error.is_instance_of::<PyValueError>(py));
            }
            for (data_type, value) in [
                (DataType::LargeUtf8, bytes.as_any()),
                (DataType::LargeBinary, text.as_any()),
                (DataType::FixedSizeBinary(2), text.as_any()),
            ] {
                let error = to_scalar(&data_type, value).expect_err("wrong type must fail");
                assert!(error.is_instance_of::<PyTypeError>(py));
            }
            Ok(())
        })
    }
}
