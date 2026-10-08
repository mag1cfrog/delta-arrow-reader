//! Convert Python filter groups to native predicates before scan planning.

use delta_arrow_reader::{DeltaPredicate, DeltaTable};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyList, PyTuple},
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
    table.predicate_column_type(&column).map_err(|error| {
        reader_error(
            condition.py(),
            error.to_string(),
            error.phase().as_str(),
            error.code(),
        )
    })?;
    match operator.as_str() {
        "is" | "is not" => {
            if !value.is_none() {
                return Err(PyValueError::new_err("null tests require None"));
            }
            Ok(if operator == "is" {
                DeltaPredicate::IsNull { column }
            } else {
                DeltaPredicate::IsNotNull { column }
            })
        }
        _ => Err(PyValueError::new_err("unsupported filter operator")),
    }
}
