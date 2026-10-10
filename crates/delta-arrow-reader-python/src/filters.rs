//! Convert Python filter groups to native predicates before scan planning.

use arrow::datatypes::DataType;
use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTable};
use pyo3::{
    exceptions::{PyOverflowError, PyTypeError, PyValueError},
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
        _ => Err(PyTypeError::new_err(
            "filter value type is not supported for this column",
        )),
    }
    .map_err(|error| {
        if error.is_instance_of::<PyOverflowError>(value.py()) {
            PyOverflowError::new_err("filter integer is out of range for the column type")
        } else {
            error
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
