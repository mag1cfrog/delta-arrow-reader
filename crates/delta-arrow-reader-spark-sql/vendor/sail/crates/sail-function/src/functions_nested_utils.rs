use std::sync::Arc;

use arrow::array::{NullArray, UInt64Array};
/// [Credit]: <https://github.com/apache/datafusion/blob/94d178ebe9674669b32ecd7896b5597f49e90791/datafusion/functions-nested/src/utils.rs>
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::compute::take_arrays;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_large_list_array, as_list_array};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err};
use datafusion_expr::{ColumnarValue, LambdaArgument};

macro_rules! downcast_arg {
    ($ARG:expr_2021, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            ::datafusion_common::DataFusionError::Internal(format!(
                "could not cast to {}",
                ::std::any::type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

macro_rules! opt_downcast_arg {
    ($ARG:expr_2021, $ARRAY_TYPE:ident) => {{ $ARG.as_any().downcast_ref::<$ARRAY_TYPE>() }};
}

pub(crate) use downcast_arg;
pub(crate) use opt_downcast_arg;

/// Apply an array kernel and restore scalars for scalar-only inputs.
/// List/LargeList scalars retain their complete element fields and child storage.
pub(crate) fn make_scalar_function<F>(
    inner: F,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue>
where
    F: Fn(&[ArrayRef]) -> Result<ArrayRef>,
{
    move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let args = ColumnarValue::values_to_arrays(args)?;

        let result = inner(&args);

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| {
                if matches!(arr.data_type(), DataType::List(_) | DataType::LargeList(_))
                    && arr.is_empty()
                {
                    return Err(exec_datafusion_err!(
                        "cannot restore a scalar from an empty list result"
                    ));
                }
                // The generic scalar converter omits List/LargeList element metadata.
                match arr.data_type() {
                    DataType::List(_) => Ok(ScalarValue::List(Arc::new(
                        as_list_array(arr.as_ref())?.slice(0, 1),
                    ))),
                    DataType::LargeList(_) => Ok(ScalarValue::LargeList(Arc::new(
                        as_large_list_array(arr.as_ref())?.slice(0, 1),
                    ))),
                    _ => ScalarValue::try_from_array(&arr, 0),
                }
            });
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

pub(crate) fn evaluate_lambda_rows(lambda: &LambdaArgument, rows: &[u64]) -> Result<ArrayRef> {
    let indices = UInt64Array::from(rows.to_vec());
    let dummy = || Ok(Arc::new(NullArray::new(rows.len())) as ArrayRef);

    lambda
        .evaluate(&[&dummy], |arrays| Ok(take_arrays(arrays, &indices, None)?))?
        .into_array(rows.len())
}

pub(crate) fn evaluate_lambdas_until_null(
    lambdas: &[&LambdaArgument],
    number_rows: usize,
) -> Result<(Vec<ArrayRef>, Vec<u64>)> {
    let row_count = u64::try_from(number_rows)
        .map_err(|_| exec_datafusion_err!("sequence row count does not fit in u64"))?;
    let mut active_rows = (0..row_count).collect::<Vec<_>>();
    let mut values = Vec::with_capacity(lambdas.len());

    for lambda in lambdas {
        if active_rows.is_empty() {
            break;
        }
        let value = evaluate_lambda_rows(lambda, &active_rows)?;
        let nulls = value.logical_nulls();
        let mut retained_positions = Vec::with_capacity(value.len());
        let mut retained_rows = Vec::with_capacity(value.len());
        for (position, row) in active_rows.iter().copied().enumerate() {
            if nulls.as_ref().is_none_or(|nulls| nulls.is_valid(position)) {
                retained_positions.push(
                    u64::try_from(position).map_err(|_| {
                        exec_datafusion_err!("sequence row index does not fit in u64")
                    })?,
                );
                retained_rows.push(row);
            }
        }

        values.push(value);
        if retained_rows.len() != active_rows.len() {
            let indices = UInt64Array::from(retained_positions);
            values = take_arrays(&values, &indices, None)?;
            active_rows = retained_rows;
        }
    }
    Ok((values, active_rows))
}

pub(crate) fn scatter_active_rows(
    value: ArrayRef,
    active_rows: &[u64],
    number_rows: usize,
) -> Result<ArrayRef> {
    if active_rows.len() == number_rows {
        return Ok(value);
    }

    let mut indices = vec![None::<u64>; number_rows];
    for (compact_index, row) in active_rows.iter().copied().enumerate() {
        let row = usize::try_from(row)
            .map_err(|_| exec_datafusion_err!("sequence row index does not fit in usize"))?;
        let compact_index = u64::try_from(compact_index)
            .map_err(|_| exec_datafusion_err!("sequence row index does not fit in u64"))?;
        let index = indices
            .get_mut(row)
            .ok_or_else(|| exec_datafusion_err!("sequence row index is out of bounds"))?;
        *index = Some(compact_index);
    }
    take_arrays(&[value], &UInt64Array::from(indices), None)?
        .pop()
        .ok_or_else(|| exec_datafusion_err!("sequence take returned no arrays"))
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int32Array, LargeListArray, ListArray, StructArray, new_null_array};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Fields};

    #[test]
    fn list_scalar_adapter_preserves_fields_nulls_and_storage() -> Result<()> {
        let fields: Fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, true)
                .with_metadata([("unit".into(), "count".into())].into()),
        ]
        .into();
        for elements_nullable in [false, true] {
            let values: ArrayRef = Arc::new(StructArray::try_new(
                fields.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![0, 1, 2])),
                    Arc::new(Int32Array::from(vec![Some(900), None, Some(800)])),
                ],
                elements_nullable.then(|| NullBuffer::from(vec![true, false, true])),
            )?);
            let field = Arc::new(
                Field::new("entry", values.data_type().clone(), elements_nullable)
                    .with_metadata([("origin".into(), "element".into())].into()),
            );
            for parent_null in [false, true] {
                let nulls = Some(NullBuffer::from(vec![true, !parent_null, true]));
                for scalar in [
                    ScalarValue::List(Arc::new(
                        ListArray::try_new(
                            field.clone(),
                            OffsetBuffer::from_lengths([1, 1, 1]),
                            values.clone(),
                            nulls.clone(),
                        )?
                        .slice(1, 1),
                    )),
                    ScalarValue::LargeList(Arc::new(
                        LargeListArray::try_new(
                            field.clone(),
                            OffsetBuffer::from_lengths([1, 1, 1]),
                            values.clone(),
                            nulls.clone(),
                        )?
                        .slice(1, 1),
                    )),
                ] {
                    let expected = scalar.to_array_of_size(1)?;
                    let actual = make_scalar_function(|args| Ok(args[0].clone()))(&[
                        ColumnarValue::Scalar(scalar),
                    ])?;
                    assert!(matches!(&actual, ColumnarValue::Scalar(_)));
                    let actual = actual.into_array(1)?;
                    actual.to_data().validate_full()?;
                    assert_eq!(actual.data_type(), expected.data_type());
                    assert_eq!(actual.as_ref(), expected.as_ref());
                    assert!(actual.to_data().ptr_eq(&expected.to_data()));
                    assert_eq!(actual.null_count(), usize::from(parent_null));
                    let children = match actual.data_type() {
                        DataType::List(_) => as_list_array(actual.as_ref())?.value(0),
                        DataType::LargeList(_) => as_large_list_array(actual.as_ref())?.value(0),
                        _ => unreachable!(),
                    };
                    assert_eq!(
                        children.logical_null_count(),
                        usize::from(elements_nullable)
                    );
                    for array in [expected.clone(), expected.slice(0, 0)] {
                        let actual = make_scalar_function(|args| Ok(args[0].clone()))(&[
                            ColumnarValue::Array(array.clone()),
                        ])?;
                        assert!(matches!(&actual, ColumnarValue::Array(_)));
                        assert!(Arc::ptr_eq(&actual.into_array(array.len())?, &array));
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn scalar_adapter_preserves_primitives_arrays_and_errors() -> Result<()> {
        for value in [ScalarValue::Int32(Some(7)), ScalarValue::Int32(None)] {
            let actual =
                make_scalar_function(|args| Ok(args[0].clone()))(&[ColumnarValue::Scalar(
                    value.clone(),
                )])?;
            let ColumnarValue::Scalar(actual) = actual else {
                return Err(exec_datafusion_err!("expected a scalar result"));
            };
            assert_eq!(actual, value);
        }
        for values in [vec![], vec![Some(7), None]] {
            let array: ArrayRef = Arc::new(Int32Array::from(values));
            let actual =
                make_scalar_function(|args| Ok(args[0].clone()))(&[ColumnarValue::Array(
                    array.clone(),
                )])?;
            assert!(matches!(&actual, ColumnarValue::Array(_)));
            assert!(Arc::ptr_eq(&actual.into_array(array.len())?, &array));
        }
        let actual = make_scalar_function(|args| Ok(args[0].clone()))(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(7))),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 2]))),
        ])?;
        assert!(matches!(&actual, ColumnarValue::Array(_)));
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![7, 7]));
        assert_eq!(actual.into_array(2)?.as_ref(), expected.as_ref());
        for argument in [
            ColumnarValue::Scalar(ScalarValue::Int32(Some(7))),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![7]))),
        ] {
            let result =
                make_scalar_function(|_| Err(exec_datafusion_err!("kernel sentinel")))(&[argument]);
            assert!(result.is_err_and(|error| error.to_string().contains("kernel sentinel")));
        }
        let result = make_scalar_function(|args| Ok(args[0].clone()))(&[
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![1]))),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 2]))),
        ]);
        assert!(result.is_err_and(|error| error.to_string().contains("mixed length")));
        Ok(())
    }

    #[test]
    fn list_scalar_adapter_extracts_the_first_row_and_rejects_empty_output() -> Result<()> {
        let field = Arc::new(
            Field::new("entry", DataType::Int32, true)
                .with_metadata([("origin".into(), "element".into())].into()),
        );
        for parent_null in [false, true] {
            let values: ArrayRef = Arc::new(Int32Array::from(vec![11, 22]));
            let nulls = Some(NullBuffer::from(vec![!parent_null, true]));
            for array in [
                Arc::new(ListArray::try_new(
                    field.clone(),
                    OffsetBuffer::from_lengths([1, 1]),
                    values.clone(),
                    nulls.clone(),
                )?) as ArrayRef,
                Arc::new(LargeListArray::try_new(
                    field.clone(),
                    OffsetBuffer::from_lengths([1, 1]),
                    values.clone(),
                    nulls.clone(),
                )?) as ArrayRef,
            ] {
                let actual =
                    make_scalar_function(|_| Ok(array.clone()))(&[ColumnarValue::Scalar(
                        ScalarValue::Int32(Some(7)),
                    )])?;
                assert!(matches!(&actual, ColumnarValue::Scalar(_)));
                let actual = actual.into_array(1)?;
                let expected = array.slice(0, 1);
                actual.to_data().validate_full()?;
                assert_eq!(actual.as_ref(), expected.as_ref());
                assert_eq!(actual.data_type(), array.data_type());
                assert_eq!(actual.len(), 1);
                assert_eq!(actual.null_count(), usize::from(parent_null));
                assert!(actual.to_data().ptr_eq(&expected.to_data()));
                let empty = new_null_array(array.data_type(), 0);
                let result = make_scalar_function(|_| Ok(empty.clone()))(&[ColumnarValue::Scalar(
                    ScalarValue::Int32(Some(7)),
                )]);
                assert!(result.is_err_and(|error| error.to_string().contains("empty list result")));
            }
        }
        Ok(())
    }
}
