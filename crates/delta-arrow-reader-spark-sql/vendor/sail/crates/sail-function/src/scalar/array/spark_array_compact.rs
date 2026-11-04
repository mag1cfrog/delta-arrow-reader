use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, GenericListArray, OffsetSizeTrait, as_large_list_array,
    as_list_array, new_null_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions_nested_utils::make_scalar_function;

/// `array_compact` removes all null values from the array.
/// This is the Spark-compatible implementation.
///
/// Returns null if the input array is null.
/// Returns an empty array if all elements are null.
/// Output elements are non-null; deeper fields retain their input declarations.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkArrayCompact {
    signature: Signature,
}

impl Default for SparkArrayCompact {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkArrayCompact {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkArrayCompact {
    fn name(&self) -> &str {
        "spark_array_compact"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [arg_type] = arg_types else {
            return plan_err!("spark_array_compact needs exactly one argument");
        };
        match arg_type {
            DataType::List(field) => Ok(DataType::List(Arc::new(
                field.as_ref().clone().with_nullable(false),
            ))),
            DataType::LargeList(field) => Ok(DataType::LargeList(Arc::new(
                field.as_ref().clone().with_nullable(false),
            ))),
            DataType::Null => Ok(DataType::new_list(DataType::Null, false)),
            _ => plan_err!(
                "spark_array_compact can only accept List or LargeList, got {:?}",
                arg_type
            ),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return plan_err!("spark_array_compact needs exactly one argument");
        };
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&[field.data_type().clone()])?,
            field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(array_compact_inner)(&args)
    }
}

fn array_compact_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("spark_array_compact needs exactly one argument");
    }
    match args[0].data_type() {
        DataType::Null => Ok(new_null_array(
            &DataType::new_list(DataType::Null, false),
            args[0].len(),
        )),
        DataType::List(_) => {
            let list = as_list_array(&args[0]);
            array_compact_generic::<i32>(list)
        }
        DataType::LargeList(_) => {
            let list = as_large_list_array(&args[0]);
            array_compact_generic::<i64>(list)
        }
        other => exec_err!("spark_array_compact does not support type: {:?}", other),
    }
}

fn array_compact_generic<O: OffsetSizeTrait>(list: &GenericListArray<O>) -> Result<ArrayRef> {
    let offsets = list.offsets();
    let first = offsets[0].as_usize();
    let last = offsets[list.len()].as_usize();
    // A sliced list retains backing values outside its visible offset range.
    let values = list.values().slice(first, last - first);
    let values_nulls = values.logical_nulls();

    // Build a boolean filter mask for the flattened values array.
    // We keep a value if its parent row is not null AND the value itself is not null.
    let mut keep: Vec<bool> = Vec::with_capacity(values.len());
    // New offsets after compaction: one entry per row, counting retained values.
    let mut new_offsets: Vec<O> = Vec::with_capacity(list.len() + 1);
    new_offsets.push(O::usize_as(0));
    let mut total_kept: usize = 0;

    for i in 0..list.len() {
        let start = offsets[i].as_usize() - first;
        let end = offsets[i + 1].as_usize() - first;

        if list.is_null(i) {
            // Null row: mark all values in the range as not kept.
            keep.extend(std::iter::repeat_n(false, end - start));
            // Null row: offset stays the same (empty slice).
            new_offsets.push(O::usize_as(total_kept));
        } else {
            // Non-null row: keep only non-null values.
            for j in start..end {
                let keep_value = values_nulls.as_ref().is_none_or(|nulls| nulls.is_valid(j));
                keep.push(keep_value);
                if keep_value {
                    total_kept += 1;
                }
            }
            new_offsets.push(O::usize_as(total_kept));
        }
    }

    let keep_mask = BooleanArray::from(keep);
    let new_values = compute::filter(values.as_ref(), &keep_mask)?;

    let field = match list.data_type() {
        DataType::List(f) | DataType::LargeList(f) => {
            Arc::new(f.as_ref().clone().with_nullable(false))
        }
        _ => unreachable!(),
    };

    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        OffsetBuffer::new(new_offsets.into()),
        new_values,
        list.nulls().cloned(),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{
        AsArray, DictionaryArray, Int8Array, Int32Array, ListArray, StructArray, new_empty_array,
    };
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::{Fields, Int8Type, Int32Type};
    use datafusion_common::ScalarValue;

    fn invoke_compact(
        value: ColumnarValue,
        parent_nullable: bool,
        rows: usize,
    ) -> Result<(ColumnarValue, FieldRef)> {
        let field = Arc::new(
            Field::new("input", value.data_type(), parent_nullable)
                .with_metadata([("origin".into(), "input".into())].into()),
        );
        let udf = SparkArrayCompact::new();
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&field),
            scalar_arguments: &[None],
        })?;
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![value],
            arg_fields: vec![field],
            number_rows: rows,
            return_field: return_field.clone(),
            config_options: Arc::default(),
        })?;
        Ok((result, return_field))
    }

    #[test]
    fn array_compact_keeps_visible_sliced_values_and_fields() -> Result<()> {
        let field = Arc::new(
            Field::new("entry", DataType::Int32, true)
                .with_metadata([("origin".into(), "compact-slice".into())].into()),
        );
        let nulls = Some(NullBuffer::from(vec![
            true, true, false, true, true, true, true,
        ]));
        let input = ListArray::try_new(
            field.clone(),
            OffsetBuffer::from_lengths([2, 3, 2, 0, 2, 1, 1]),
            Arc::new(Int32Array::from(vec![
                Some(900),
                Some(901),
                Some(3),
                None,
                Some(4),
                Some(700),
                Some(701),
                None,
                None,
                Some(800),
                Some(99),
            ])),
            nulls.clone(),
        )?;
        let required = Arc::new(field.as_ref().clone().with_nullable(false));
        let expected = ListArray::try_new(
            required.clone(),
            OffsetBuffer::from_lengths([2, 2, 0, 0, 0, 1, 1]),
            Arc::new(Int32Array::from(vec![900, 901, 3, 4, 800, 99])),
            nulls,
        )?;
        for (data_type, return_type) in [
            (
                DataType::List(field.clone()),
                DataType::List(required.clone()),
            ),
            (DataType::LargeList(field), DataType::LargeList(required)),
        ] {
            let input = compute::cast(&input, &data_type)?;
            let expected = compute::cast(&expected, &return_type)?;
            for (offset, len) in [
                (0, 7),
                (1, 5),
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 1),
                (5, 1),
                (3, 0),
            ] {
                let argument = input.slice(offset, len);
                let (actual, field) = invoke_compact(ColumnarValue::Array(argument), true, len)?;
                let actual = actual.into_array(len)?;
                actual.to_data().validate_full()?;
                assert_eq!(actual.data_type(), field.data_type());
                assert_eq!(actual.data_type(), &return_type);
                assert!(field.is_nullable());
                assert!(field.metadata().is_empty());
                let (first, last, child_len) = match actual.data_type() {
                    DataType::List(_) => {
                        let list = as_list_array(&actual);
                        (
                            list.offsets()[0] as usize,
                            list.offsets()[len] as usize,
                            list.values().len(),
                        )
                    }
                    DataType::LargeList(_) => {
                        let list = as_large_list_array(&actual);
                        (
                            list.offsets()[0] as usize,
                            list.offsets()[len] as usize,
                            list.values().len(),
                        )
                    }
                    _ => unreachable!(),
                };
                assert_eq!(first, 0);
                assert_eq!(last, child_len);
                assert_eq!(
                    actual.as_ref(),
                    expected.slice(offset, len).as_ref(),
                    "{data_type} offset={offset} len={len}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn array_compact_refines_only_the_immediate_element_field() -> Result<()> {
        let nested = DataType::Struct(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new(
                    "nested",
                    DataType::List(Arc::new(
                        Field::new("number", DataType::Int32, true)
                            .with_metadata([("unit".into(), "count".into())].into()),
                    )),
                    true,
                ),
            ]
            .into(),
        );
        let udf = SparkArrayCompact::new();
        for elements_nullable in [false, true] {
            let element = Arc::new(
                Field::new("entry", nested.clone(), elements_nullable)
                    .with_metadata([("origin".into(), "element".into())].into()),
            );
            let required = Arc::new(element.as_ref().clone().with_nullable(false));
            for (input_type, expected) in [
                (
                    DataType::List(element.clone()),
                    DataType::List(required.clone()),
                ),
                (
                    DataType::LargeList(element.clone()),
                    DataType::LargeList(required.clone()),
                ),
            ] {
                for nullable in [false, true] {
                    let field = Arc::new(
                        Field::new("source", input_type.clone(), nullable)
                            .with_metadata([("origin".into(), "source".into())].into()),
                    );
                    let actual = udf.return_field_from_args(ReturnFieldArgs {
                        arg_fields: &[field],
                        scalar_arguments: &[None],
                    })?;
                    assert_eq!(actual.data_type(), &expected);
                    assert_eq!(actual.is_nullable(), nullable);
                    assert!(actual.metadata().is_empty());
                }
            }
        }
        for types in [vec![], vec![DataType::Int32], vec![DataType::Null; 2]] {
            assert!(udf.return_type(&types).is_err());
            let fields = types
                .iter()
                .map(|data_type| Arc::new(Field::new("input", data_type.clone(), true)))
                .collect::<Vec<_>>();
            assert!(
                udf.return_field_from_args(ReturnFieldArgs {
                    arg_fields: &fields,
                    scalar_arguments: &[]
                })
                .is_err()
            );
        }
        assert!(array_compact_inner(&[]).is_err());
        Ok(())
    }

    #[test]
    fn array_compact_nested_arrays_and_scalars_keep_deeper_nulls() -> Result<()> {
        let nested_field = Arc::new(
            Field::new("number", DataType::Int32, true)
                .with_metadata([("unit".into(), "count".into())].into()),
        );
        let fields: Fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("nested", DataType::List(nested_field.clone()), true)
                .with_metadata([("origin".into(), "nested".into())].into()),
        ]
        .into();
        let nested: ArrayRef = Arc::new(ListArray::try_new(
            nested_field.clone(),
            OffsetBuffer::from_lengths([2, 1, 0]),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(99)])),
            None,
        )?);
        let values: ArrayRef = Arc::new(StructArray::try_new(
            fields.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])), nested],
            Some(NullBuffer::from(vec![true, false, true])),
        )?);
        let expected_nested: ArrayRef = Arc::new(ListArray::try_new(
            nested_field,
            OffsetBuffer::from_lengths([2, 0]),
            Arc::new(Int32Array::from(vec![Some(1), None])),
            None,
        )?);
        let expected_values: ArrayRef = Arc::new(StructArray::try_new(
            fields,
            vec![Arc::new(Int32Array::from(vec![1, 3])), expected_nested],
            None,
        )?);
        let element = Arc::new(
            Field::new("entry", values.data_type().clone(), true)
                .with_metadata([("origin".into(), "element".into())].into()),
        );
        let required = Arc::new(element.as_ref().clone().with_nullable(false));
        let input = ListArray::try_new(
            element.clone(),
            OffsetBuffer::from_lengths([3, 0]),
            values,
            None,
        )?;
        let expected = ListArray::try_new(
            required.clone(),
            OffsetBuffer::from_lengths([2, 0]),
            expected_values,
            None,
        )?;
        for (input_type, expected_type) in [
            (
                DataType::List(element.clone()),
                DataType::List(required.clone()),
            ),
            (DataType::LargeList(element), DataType::LargeList(required)),
        ] {
            let input = compute::cast(&input, &input_type)?;
            let expected = compute::cast(&expected, &expected_type)?;
            for (argument, rows) in [
                (ColumnarValue::Array(input.clone()), 2),
                (ColumnarValue::Array(input.slice(1, 0)), 0),
                (
                    ColumnarValue::Scalar(ScalarValue::try_from_array(&input, 0)?),
                    1,
                ),
            ] {
                let scalar = matches!(&argument, ColumnarValue::Scalar(_));
                let (actual, field) = invoke_compact(argument, false, rows)?;
                assert_eq!(matches!(&actual, ColumnarValue::Scalar(_)), scalar);
                let actual = actual.into_array(rows)?;
                actual.to_data().validate_full()?;
                assert!(!field.is_nullable());
                assert_eq!(actual.data_type(), field.data_type());
                assert_eq!(actual.as_ref(), expected.slice(0, rows).as_ref());
            }
        }
        Ok(())
    }

    #[test]
    fn array_compact_null_inputs_return_typed_null_lists() -> Result<()> {
        for (input_type, expected) in [
            (DataType::Null, DataType::new_list(DataType::Null, false)),
            (
                DataType::new_list(DataType::Int32, true),
                DataType::new_list(DataType::Int32, false),
            ),
            (
                DataType::new_large_list(DataType::Int32, true),
                DataType::new_large_list(DataType::Int32, false),
            ),
            (
                DataType::new_list(DataType::Null, true),
                DataType::new_list(DataType::Null, false),
            ),
        ] {
            for (argument, rows) in [
                (
                    ColumnarValue::Scalar(ScalarValue::try_from(&input_type)?),
                    1,
                ),
                (ColumnarValue::Array(new_null_array(&input_type, 2)), 2),
                (ColumnarValue::Array(new_empty_array(&input_type)), 0),
            ] {
                let scalar = matches!(&argument, ColumnarValue::Scalar(_));
                let (actual, field) = invoke_compact(argument, true, rows)?;
                assert_eq!(matches!(&actual, ColumnarValue::Scalar(_)), scalar);
                let actual = actual.into_array(rows)?;
                actual.to_data().validate_full()?;
                assert_eq!(actual.len(), rows);
                assert_eq!(actual.null_count(), rows);
                assert_eq!(actual.data_type(), &expected);
                assert_eq!(actual.data_type(), field.data_type());
                assert!(field.is_nullable());
            }
        }
        Ok(())
    }

    #[test]
    fn array_compact_removes_logical_dictionary_nulls_without_rewriting_the_pool() -> Result<()> {
        let pool: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(3), Some(4), None]));
        let values: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(2), Some(1), Some(3)]),
            pool.clone(),
        )?);
        let element = Arc::new(Field::new("entry", values.data_type().clone(), true));
        let required = Arc::new(element.as_ref().clone().with_nullable(false));
        let nulls = Some(NullBuffer::from(vec![true, true, false]));
        let input = ListArray::try_new(
            element.clone(),
            OffsetBuffer::from_lengths([2, 3, 1]),
            values,
            nulls.clone(),
        )?;
        let expected = ListArray::try_new(
            required.clone(),
            OffsetBuffer::from_lengths([1, 2, 0]),
            Arc::new(DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![1, 2, 1]),
                pool.clone(),
            )?),
            nulls,
        )?;
        for (input_type, return_type) in [
            (
                DataType::List(element.clone()),
                DataType::List(required.clone()),
            ),
            (DataType::LargeList(element), DataType::LargeList(required)),
        ] {
            let input = compute::cast(&input, &input_type)?;
            let expected = compute::cast(&expected, &return_type)?;
            let (actual, field) = invoke_compact(ColumnarValue::Array(input), true, 3)?;
            let actual = actual.into_array(3)?;
            actual.to_data().validate_full()?;
            assert_eq!(actual.data_type(), field.data_type());
            assert_eq!(actual.as_ref(), expected.as_ref());
            let values = match actual.data_type() {
                DataType::List(_) => as_list_array(&actual).values(),
                DataType::LargeList(_) => as_large_list_array(&actual).values(),
                _ => unreachable!(),
            };
            let dictionary = values.as_dictionary::<Int8Type>();
            assert_eq!(dictionary.logical_null_count(), 0);
            assert!(Arc::ptr_eq(dictionary.values(), &pool));
            assert_eq!(
                compute::cast(values.as_ref(), &DataType::Int32)?.as_primitive::<Int32Type>(),
                &Int32Array::from(vec![3, 4, 3])
            );
        }
        Ok(())
    }
}
