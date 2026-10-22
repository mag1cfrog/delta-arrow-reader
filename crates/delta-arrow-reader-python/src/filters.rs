//! Convert Python filter groups to native predicates before scan planning.

use arrow::{
    array::types::{Decimal128Type, DecimalType},
    datatypes::{DataType, TimeUnit},
};
use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTable};
use pyo3::{
    exceptions::{PyException, PyOverflowError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple},
};

use crate::{DeltaReaderError, core_error};

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
        crate::diagnostics::validation_error(
            &error,
            "filters",
            core_error::<DeltaReaderError>(condition.py(), &error),
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
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let datetime_module = value.py().import("datetime")?;
            let datetime = datetime_module.getattr("datetime")?;
            // Subclasses can carry finer precision or sentinels outside datetime's fields.
            if !value.get_type().is(&datetime) {
                return Err(PyTypeError::new_err(
                    "timestamp filters require datetime.datetime, excluding subclasses",
                ));
            }
            let invalid_offset = || {
                PyValueError::new_err(if timezone.is_some() {
                    "timestamp filter requires an aware datetime with a valid UTC offset"
                } else {
                    "timestamp filter requires a naive datetime with no UTC offset"
                })
            };
            let offset = value.call_method0("utcoffset").map_err(|error| {
                if error.is_instance_of::<PyException>(value.py()) {
                    invalid_offset()
                } else {
                    error
                }
            })?;
            if offset.is_none() != timezone.is_none() {
                return Err(invalid_offset());
            }
            if !offset.is_none() && !offset.get_type().is(&datetime_module.getattr("timedelta")?) {
                return Err(PyValueError::new_err(
                    "filter UTC offset must be datetime.timedelta, excluding subclasses",
                ));
            }
            // Remove tzinfo so subtraction cannot call the offset callback again.
            let replace_kwargs = PyDict::new(value.py());
            replace_kwargs.set_item("tzinfo", value.py().None())?;
            let naive = value.call_method("replace", (), Some(&replace_kwargs))?;
            let epoch = datetime.call1((1970, 1, 1))?;
            let duration = naive.sub(epoch)?;
            let duration = if offset.is_none() {
                duration
            } else {
                duration.sub(offset)?
            };
            let days = duration.getattr("days")?.extract::<i64>()?;
            let seconds = duration.getattr("seconds")?.extract::<i64>()?;
            let microseconds = duration.getattr("microseconds")?.extract::<i64>()?;
            Ok(DeltaScalar::TimestampMicrosecond {
                value: days * 86_400_000_000 + seconds * 1_000_000 + microseconds,
                timezone: timezone.as_ref().map(ToString::to_string),
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
mod tests;
