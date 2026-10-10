//! Convert Python filter groups to native predicates before scan planning.

use arrow::datatypes::DataType;
use delta_arrow_reader::{DeltaComparison, DeltaPredicate, DeltaScalar, DeltaTable};
use pyo3::{
    exceptions::{PyOverflowError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyInt, PyList, PyTuple},
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
