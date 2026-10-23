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

use crate::{DeltaReaderError, core_error_to_python_exception};

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
        crate::diagnostics::log_validation_error(
            &error,
            "filters",
            core_error_to_python_exception::<DeltaReaderError>(condition.py(), &error),
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
mod tests {
    use super::*;
    use pyo3::{PyTypeInfo, types::PyByteArray};

    fn assert_error<E: PyTypeInfo>(data_type: &DataType, value: &Bound<'_, PyAny>) {
        let py = value.py();
        let error = to_scalar(data_type, value).unwrap_err();
        assert!(error.get_type(py).is(py.get_type::<E>()), "{error}");
        assert!(!error.to_string().contains("secret"), "{error}");
        assert!(error.cause(py).is_none());
        assert!(error.context(py).is_none());
    }

    #[test]
    fn boolean_and_integer_conversion_checks_type_and_width() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            for value in [false, true] {
                assert_eq!(
                    to_scalar(&DataType::Boolean, PyBool::new(py, value).as_any())?,
                    DeltaScalar::Boolean(value),
                );
            }
            for (data_type, bits) in [
                (DataType::Int8, 8),
                (DataType::Int16, 16),
                (DataType::Int32, 32),
                (DataType::Int64, 64),
            ] {
                let max = (1_i128 << (bits - 1)) - 1;
                for value in [-max - 1, -1, 0, 1, max] {
                    let expected = match data_type {
                        DataType::Int8 => DeltaScalar::Int8(value.try_into().unwrap()),
                        DataType::Int16 => DeltaScalar::Int16(value.try_into().unwrap()),
                        DataType::Int32 => DeltaScalar::Int32(value.try_into().unwrap()),
                        _ => DeltaScalar::Int64(value.try_into().unwrap()),
                    };
                    assert_eq!(
                        to_scalar(&data_type, PyInt::new(py, value).as_any())?,
                        expected
                    );
                }
                for value in [-max - 2, max + 1, -(1_i128 << 100), 1_i128 << 100] {
                    assert_error::<PyOverflowError>(&data_type, PyInt::new(py, value).as_any());
                }
                for value in [false, true] {
                    assert_error::<PyTypeError>(&data_type, PyBool::new(py, value).as_any());
                }
            }
            for value in [-1, 0, 1] {
                assert_error::<PyTypeError>(&DataType::Boolean, PyInt::new(py, value).as_any());
            }
            Ok(())
        })
    }

    #[test]
    fn floats_preserve_rounding_subnormals_and_signed_zero() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            let max32 = f64::from(f32::MAX);
            let min32 = f64::from(f32::from_bits(1));
            for (input, expected) in [
                (-max32, -f32::MAX),
                (max32, f32::MAX),
                (-1.5, -1.5),
                (1.5, 1.5),
                (-0.0, -0.0),
                (0.0, 0.0),
                (0.1, 0.1),
                (min32, f32::from_bits(1)),
                (-max32.next_up(), -f32::MAX),
                (max32.next_up(), f32::MAX),
                (-min32 / 2.0, -0.0),
                (min32 / 2.0, 0.0),
            ] {
                let DeltaScalar::Float32(actual) =
                    to_scalar(&DataType::Float32, PyFloat::new(py, input).as_any())?
                else {
                    panic!("expected Float32");
                };
                assert_eq!(actual.to_bits(), expected.to_bits());
            }
            for input in [
                -f64::MAX,
                f64::MAX,
                -1.5,
                1.5,
                -0.0,
                0.0,
                0.1,
                f64::from_bits(1),
            ] {
                let DeltaScalar::Float64(actual) =
                    to_scalar(&DataType::Float64, PyFloat::new(py, input).as_any())?
                else {
                    panic!("expected Float64");
                };
                assert_eq!(actual.to_bits(), input.to_bits());
            }
            for data_type in [DataType::Float32, DataType::Float64] {
                for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
                    assert_error::<PyValueError>(&data_type, PyFloat::new(py, value).as_any());
                }
                assert_error::<PyTypeError>(&data_type, PyInt::new(py, 1).as_any());
                assert_error::<PyTypeError>(&data_type, PyBool::new(py, true).as_any());
            }
            for value in [2_f64.powi(128), f64::MAX, 2_f64.powi(128) - 2_f64.powi(103)] {
                for input in [value, -value] {
                    assert_error::<PyValueError>(
                        &DataType::Float32,
                        PyFloat::new(py, input).as_any(),
                    );
                }
            }
            Ok(())
        })
    }

    #[test]
    fn text_and_bytes_are_preserved_without_coercion() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            for text in ["", "a\0b", "caf\u{e9}", "cafe\u{301}", "\u{1f642}"] {
                assert_eq!(
                    to_scalar(&DataType::Utf8, PyString::new(py, text).as_any())?,
                    DeltaScalar::Utf8(text.into()),
                );
            }
            for bytes in [b"".as_slice(), b"a\0b", b"\xff", b"\0", b"\xc3\xa9"] {
                assert_eq!(
                    to_scalar(&DataType::Binary, PyBytes::new(py, bytes).as_any())?,
                    DeltaScalar::Binary(bytes.to_vec()),
                );
            }
            for codepoint in [0xd800, 0xdfff] {
                let surrogate = py.import("builtins")?.getattr("chr")?.call1((codepoint,))?;
                let value = surrogate.add("secret")?;
                assert_error::<PyValueError>(&DataType::Utf8, &value);
            }
            let bytes = PyBytes::new(py, b"secret");
            let view = py
                .import("builtins")?
                .getattr("memoryview")?
                .call1((&bytes,))?;
            for data_type in [DataType::Utf8, DataType::Binary] {
                assert_error::<PyTypeError>(&data_type, PyByteArray::new(py, b"secret").as_any());
                assert_error::<PyTypeError>(&data_type, &view);
            }
            assert_error::<PyTypeError>(&DataType::Utf8, bytes.as_any());
            assert_error::<PyTypeError>(&DataType::Binary, PyString::new(py, "secret").as_any());
            Ok(())
        })
    }

    #[test]
    fn decimals_preserve_exact_values_at_precision_and_scale_boundaries() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            let decimal = py.import("decimal")?.getattr("Decimal")?;
            let wide = 10_i128.pow(38) - 1;
            for (text, precision, scale, unscaled) in [
                ("-999.99".into(), 5, 2, -99999),
                ("999.99".into(), 5, 2, 99999),
                ("-99999".into(), 5, 0, -99999),
                ("99999".into(), 5, 0, 99999),
                ("1.2300E2".into(), 5, 0, 123),
                (format!("1.23{}", "0".repeat(200)), 5, 2, 123),
                (wide.to_string(), 38, 0, wide),
                ((-wide).to_string(), 38, 0, -wide),
                (format!("{wide}{}E-200", "0".repeat(200)), 38, 0, wide),
                (format!("0.{wide}"), 38, 38, wide),
                (format!("-0.{wide}"), 38, 38, -wide),
                ("1E-38".into(), 38, 38, 1),
            ] {
                assert_eq!(
                    to_scalar(
                        &DataType::Decimal128(precision, scale),
                        &decimal.call1((text,))?
                    )?,
                    DeltaScalar::Decimal128 {
                        value: unscaled,
                        precision,
                        scale
                    },
                );
            }
            for (precision, scale, invalid) in [
                (5, 2, vec!["1000".into(), "-1000".into(), "1.234".into()]),
                (5, 0, vec!["100000".into(), "1.1".into()]),
                (
                    38,
                    0,
                    vec!["1E38".into(), i128::MIN.to_string(), i128::MAX.to_string()],
                ),
                (38, 38, vec!["1".into(), "1E-39".into()]),
            ] {
                let data_type = DataType::Decimal128(precision, scale);
                for text in ["0", "-0E-999999999999999999", "0E+999999999999999999"] {
                    assert_eq!(
                        to_scalar(&data_type, &decimal.call1((text,))?)?,
                        DeltaScalar::Decimal128 {
                            value: 0,
                            precision,
                            scale
                        }
                    );
                }
                for text in invalid.iter().map(String::as_str).chain([
                    "NaN123",
                    "-sNaN456",
                    "Infinity",
                    "-Infinity",
                    "1E-999999999999999999",
                ]) {
                    assert_error::<PyValueError>(&data_type, &decimal.call1((text,))?);
                }
                for text in [
                    "1E999999999999999999".into(),
                    "-1E999999999999999999".into(),
                    (1_u128 << 127).to_string(),
                    format!("-{}", (1_u128 << 127) + 1),
                    "9".repeat(200),
                ] {
                    assert_error::<PyOverflowError>(&data_type, &decimal.call1((text,))?);
                }
                assert_error::<PyTypeError>(&data_type, PyFloat::new(py, 1.23).as_any());
            }
            Ok(())
        })
    }

    #[test]
    fn dates_preserve_calendar_days_and_exclude_datetimes() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            let datetime = py.import("datetime")?;
            let date = datetime.getattr("date")?;
            for (fields, days) in [
                ((1, 1, 1), -719162),
                ((1900, 3, 1), -25508),
                ((1969, 12, 31), -1),
                ((1970, 1, 1), 0),
                ((2000, 2, 29), 11016),
                ((9999, 12, 31), 2932896),
            ] {
                assert_eq!(
                    to_scalar(&DataType::Date32, &date.call1(fields)?)?,
                    DeltaScalar::Date32(days)
                );
            }
            let naive = datetime.getattr("datetime")?.call1((1970, 1, 1))?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("tzinfo", datetime.getattr("timezone")?.getattr("utc")?)?;
            let aware = naive.call_method("replace", (), Some(&kwargs))?;
            for value in [naive, aware] {
                assert_error::<PyTypeError>(&DataType::Date32, &value);
            }
            Ok(())
        })
    }

    #[test]
    fn timestamps_preserve_microseconds_and_utc_offset_at_calendar_boundaries() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            let module = py.import("datetime")?;
            let datetime = module.getattr("datetime")?;
            let timezone = module.getattr("timezone")?;
            let timedelta = module.getattr("timedelta")?;
            for (fields, micros) in [
                ((1, 1, 1, 0, 0, 0, 0), -62_135_596_800_000_000_i64),
                ((1969, 12, 31, 23, 59, 58, 999999), -1_000_001),
                ((1969, 12, 31, 23, 59, 59, 999999), -1),
                ((1970, 1, 1, 0, 0, 0, 0), 0),
                ((1970, 1, 1, 0, 0, 0, 1), 1),
                ((2000, 2, 29, 12, 34, 56, 123456), 951_827_696_123_456),
                ((9999, 12, 31, 23, 59, 59, 999999), 253_402_300_799_999_999),
            ] {
                for offset in [
                    None,
                    Some(0),
                    Some(1),
                    Some(-1),
                    Some(19_800_000_000_i64),
                    Some(-25_200_000_000),
                    Some(30_000_001),
                ] {
                    let value = datetime.call1(fields)?;
                    let value = if let Some(offset) = offset {
                        let kwargs = PyDict::new(py);
                        kwargs.set_item(
                            "tzinfo",
                            timezone.call1((timedelta.call1((0, 0, offset))?,))?,
                        )?;
                        value.call_method("replace", (), Some(&kwargs))?
                    } else {
                        value
                    };
                    let field_timezone = offset.map(|_| "UTC".into());
                    assert_eq!(
                        to_scalar(
                            &DataType::Timestamp(TimeUnit::Microsecond, field_timezone),
                            &value
                        )?,
                        DeltaScalar::TimestampMicrosecond {
                            value: micros - offset.unwrap_or(0),
                            timezone: offset.map(|_| "UTC".into()),
                        },
                    );
                    let wrong_timezone = if offset.is_some() {
                        None
                    } else {
                        Some("UTC".into())
                    };
                    assert_error::<PyValueError>(
                        &DataType::Timestamp(TimeUnit::Microsecond, wrong_timezone),
                        &value,
                    );
                }
            }
            Ok(())
        })
    }

    #[test]
    fn scalar_conversion_rejects_wrong_types_and_null_comparisons() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            let decimal = py.import("decimal")?.getattr("Decimal")?.call1(("1",))?;
            for data_type in [
                DataType::Boolean,
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::Float32,
                DataType::Float64,
                DataType::Utf8,
                DataType::Binary,
                DataType::Decimal128(5, 2),
                DataType::Date32,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ] {
                assert_error::<PyValueError>(&data_type, py.None().bind(py));
                assert_error::<PyTypeError>(&data_type, PyList::empty(py).as_any());
                assert_error::<PyTypeError>(&data_type, PyDict::new(py).as_any());
                if data_type != DataType::Boolean {
                    assert_error::<PyTypeError>(&data_type, PyBool::new(py, true).as_any());
                }
                if !matches!(
                    data_type,
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                ) {
                    assert_error::<PyTypeError>(&data_type, PyInt::new(py, 1).as_any());
                }
                if !matches!(data_type, DataType::Float32 | DataType::Float64) {
                    assert_error::<PyTypeError>(&data_type, PyFloat::new(py, 1.0).as_any());
                }
                if data_type != DataType::Utf8 {
                    assert_error::<PyTypeError>(&data_type, PyString::new(py, "secret").as_any());
                }
                if data_type != DataType::Binary {
                    assert_error::<PyTypeError>(&data_type, PyBytes::new(py, b"secret").as_any());
                }
                if !matches!(data_type, DataType::Decimal128(..)) {
                    assert_error::<PyTypeError>(&data_type, &decimal);
                }
                if data_type != DataType::Date32 {
                    let date = py
                        .import("datetime")?
                        .getattr("date")?
                        .call1((1970, 1, 1))?;
                    assert_error::<PyTypeError>(&data_type, &date);
                }
            }
            Ok(())
        })
    }

    #[test]
    fn timestamp_conversion_preserves_field_timezone() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            let datetime = py.import("datetime")?;
            let offset = datetime.getattr("timedelta")?.call1((0, 3600))?;
            let timezone = datetime.getattr("timezone")?.call1((offset,))?;
            let value = datetime
                .getattr("datetime")?
                .call1((1970, 1, 1, 1, 0, 0, 0, timezone))?;
            // Delta schemas use UTC; exercise other native field metadata here.
            let field_timezone = "America/New_York";
            assert_eq!(
                to_scalar(
                    &DataType::Timestamp(TimeUnit::Microsecond, Some(field_timezone.into())),
                    &value,
                )?,
                DeltaScalar::TimestampMicrosecond {
                    value: 0,
                    timezone: Some(field_timezone.into()),
                },
            );
            Ok(())
        })
    }

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
