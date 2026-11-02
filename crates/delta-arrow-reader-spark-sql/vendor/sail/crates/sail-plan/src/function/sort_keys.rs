//! Spark nested NULL children sort below values independently of container placement.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, LargeListArray, StructArray, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::row::{RowConverter, SortField};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

use crate::function::float_keys::normalize_nested_float_keys;

pub(crate) fn spark_sort_key(expression: Expr, schema: &DFSchema) -> Result<Expr> {
    if is_nested_key(&expression.get_type(schema)?) {
        Ok(ScalarUDF::from(SparkNestedSortKey).call(vec![expression]))
    } else {
        Ok(expression)
    }
}

fn is_nested_key(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_) => true,
        DataType::Dictionary(_, value) => is_nested_key(value),
        _ => false,
    }
}

fn take_sort_values(array: &ArrayRef, indices: &UInt64Array) -> Result<ArrayRef> {
    let indices = UInt64Array::from_iter(
        indices
            .iter()
            .map(|index| index.filter(|index| array.is_valid(*index as usize))),
    );
    match array.data_type() {
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_any_dictionary();
            let keys = arrow::compute::take(dictionary.keys(), &indices, None)?;
            let keys = arrow::compute::cast(keys.as_ref(), &DataType::UInt64)?;
            take_sort_values(dictionary.values(), keys.as_primitive())
        }
        DataType::Struct(fields) => {
            let columns = array
                .as_struct()
                .columns()
                .iter()
                .map(|column| take_sort_values(column, &indices))
                .collect::<Result<Vec<_>>>()?;
            let fields = fields
                .iter()
                .zip(&columns)
                .map(|(field, column)| {
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(column.data_type().clone())
                            .with_nullable(true),
                    )
                })
                .collect();
            Ok(Arc::new(StructArray::try_new_with_length(
                fields,
                columns,
                indices.nulls().cloned(),
                indices.len(),
            )?))
        }
        DataType::List(field) => {
            let list = array.as_list::<i32>();
            take_sort_list(field, list.values(), &indices, |index| {
                list.value_offsets()[index] as usize..list.value_offsets()[index + 1] as usize
            })
        }
        DataType::LargeList(field) => {
            let list = array.as_list::<i64>();
            take_sort_list(field, list.values(), &indices, |index| {
                list.value_offsets()[index] as usize..list.value_offsets()[index + 1] as usize
            })
        }
        DataType::ListView(field) => {
            let list = array.as_list_view::<i32>();
            take_sort_list(field, list.values(), &indices, |index| {
                let start = list.value_offsets()[index] as usize;
                start..start + list.value_sizes()[index] as usize
            })
        }
        DataType::LargeListView(field) => {
            let list = array.as_list_view::<i64>();
            take_sort_list(field, list.values(), &indices, |index| {
                let start = list.value_offsets()[index] as usize;
                start..start + list.value_sizes()[index] as usize
            })
        }
        DataType::FixedSizeList(field, size) => {
            let list = array.as_fixed_size_list();
            take_sort_list(field, list.values(), &indices, |index| {
                let start = index * *size as usize;
                start..start + *size as usize
            })
        }
        _ => Ok(arrow::compute::take(array.as_ref(), &indices, None)?),
    }
}

fn take_sort_list(
    field: &FieldRef,
    values: &ArrayRef,
    indices: &UInt64Array,
    range: impl Fn(usize) -> std::ops::Range<usize>,
) -> Result<ArrayRef> {
    let mut children = Vec::new();
    let mut offsets = Vec::with_capacity(indices.len() + 1);
    offsets.push(0_i64);
    for index in indices.iter() {
        if let Some(index) = index {
            children.extend(range(index as usize).map(|index| index as u64));
        }
        offsets.push(i64::try_from(children.len()).map_err(|_| {
            datafusion_common::DataFusionError::Execution("nested sort key offset overflow".into())
        })?);
    }
    let values = take_sort_values(values, &UInt64Array::from_iter_values(children))?;
    let field = Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(values.data_type().clone())
            .with_nullable(true),
    );
    Ok(Arc::new(LargeListArray::try_new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        indices.nulls().cloned(),
    )?))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkNestedSortKey;

impl ScalarUDFImpl for SparkNestedSortKey {
    fn name(&self) -> &str {
        "spark_nested_sort_key"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Binary,
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = datafusion_common::utils::take_function_args(self.name(), args.args)?;
        let scalar = matches!(value, ColumnarValue::Scalar(_));
        let array = value.to_array(1)?;
        // Visit only selected rows and live children before any recursive float work.
        let array = take_sort_values(
            &array,
            &UInt64Array::from_iter_values(0..array.len() as u64),
        )?;
        let array = normalize_nested_float_keys(array, &args.config_options)?;
        let converter = RowConverter::new(vec![SortField::new(array.data_type().clone())])?;
        // Encode ascending with NULL children first; the consumer applies direction
        // and container NULL placement to this temporary key independently.
        let rows = converter.convert_columns(std::slice::from_ref(&array))?;
        let (offsets, values, _) = rows.try_into_binary()?.into_parts();
        let key = Arc::new(BinaryArray::try_new(
            offsets,
            values,
            array.logical_nulls(),
        )?);
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(key.as_ref(), 0)?)
        } else {
            ColumnarValue::Array(key)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, AsArray, DictionaryArray, FixedSizeListArray, Int8Array, Int32Array,
        LargeListViewArray, ListArray, ListViewArray, StringArray, StructArray,
    };
    use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices};
    use arrow::datatypes::{Int8Type, Int32Type};

    fn encode(value: ColumnarValue) -> Result<ColumnarValue> {
        let field = Arc::new(Field::new("key", value.data_type(), true));
        SparkNestedSortKey.invoke_with_args(ScalarFunctionArgs {
            args: vec![value],
            arg_fields: vec![field],
            number_rows: 1,
            return_field: Arc::new(Field::new("key", DataType::Binary, true)),
            config_options: Arc::default(),
        })
    }

    #[test]
    fn child_null_order_is_independent_of_container_placement() -> Result<()> {
        let array: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), None]),
            None,
            Some(vec![]),
            Some(vec![None]),
            Some(vec![None, Some(1)]),
            Some(vec![Some(1)]),
            Some(vec![Some(1), None]),
        ]));
        let field = Arc::new(Field::new("child", array.data_type().clone(), true));
        let structure: ArrayRef = Arc::new(StructArray::try_new(
            vec![field].into(),
            vec![array.clone()],
            array.nulls().cloned(),
        )?);
        for input in [array, structure] {
            let key = encode(ColumnarValue::Array(input.clone()))?.into_array(input.len())?;
            assert_eq!(key.logical_nulls(), input.logical_nulls());
            let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(0..input.len() as i32));
            for descending in [false, true] {
                for nulls_first in [false, true] {
                    let indices = lexsort_to_indices(
                        &[
                            SortColumn {
                                values: key.clone(),
                                options: Some(SortOptions {
                                    descending,
                                    nulls_first,
                                }),
                            },
                            SortColumn {
                                values: ids.clone(),
                                options: None,
                            },
                        ],
                        None,
                    )?;
                    let mut expected = if descending {
                        vec![0, 6, 5, 4, 3, 2]
                    } else {
                        vec![2, 3, 4, 5, 0, 6]
                    };
                    expected.insert(if nulls_first { 0 } else { expected.len() }, 1);
                    assert_eq!(indices.values().as_ref(), expected.as_slice());
                }
            }
            let scalar = ScalarValue::try_from_array(&input, 4)?;
            assert_eq!(
                encode(ColumnarValue::Scalar(scalar))?
                    .into_array(1)?
                    .to_data(),
                key.slice(4, 1).to_data()
            );
            let empty = encode(ColumnarValue::Array(input.slice(0, 0)))?.into_array(0)?;
            assert!(empty.is_empty());
        }
        Ok(())
    }

    #[test]
    fn dictionaries_use_logical_nulls_and_batch_independent_keys() -> Result<()> {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None, Some(-1)]));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), Some(2), None]),
            values,
        )?);
        let structure: ArrayRef = Arc::new(StructArray::try_new(
            vec![Field::new("value", dictionary.data_type().clone(), true)].into(),
            vec![dictionary],
            Some(vec![true, true, true, false].into()),
        )?);
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), Some(2), Some(3), None]),
            structure.clone(),
        )?);
        for input in [structure, dictionary] {
            let key = encode(ColumnarValue::Array(input.clone()))?.into_array(input.len())?;
            assert_eq!(key.logical_nulls(), input.logical_nulls());
            assert!(key.as_binary::<i32>().value(1) < key.as_binary::<i32>().value(2));
            assert!(key.as_binary::<i32>().value(2) < key.as_binary::<i32>().value(0));
            for index in 0..input.len() {
                let sliced = encode(ColumnarValue::Array(input.slice(index, 1)))?.into_array(1)?;
                assert_eq!(sliced.to_data(), key.slice(index, 1).to_data());
            }
        }
        Ok(())
    }

    #[test]
    fn sparse_views_pack_only_live_children_including_nested_nulls() -> Result<()> {
        const BACKING_ROWS: usize = 1_000_000;
        let integers: ArrayRef = Arc::new(Int32Array::from_iter_values(0..BACKING_ROWS as i32));
        let strings: ArrayRef = Arc::new(StringArray::from_iter_values((0..BACKING_ROWS).map(
            |index| {
                if index == BACKING_ROWS - 1 {
                    "last"
                } else {
                    "first"
                }
            },
        )));
        for values in [integers, strings] {
            let field = Arc::new(Field::new("item", values.data_type().clone(), true));
            let views: Vec<ArrayRef> = vec![
                Arc::new(ListViewArray::try_new(
                    field.clone(),
                    vec![0, 0, BACKING_ROWS as i32 - 1].into(),
                    vec![1, BACKING_ROWS as i32, 1].into(),
                    values.clone(),
                    None,
                )?),
                Arc::new(LargeListViewArray::try_new(
                    field,
                    vec![0, 0, BACKING_ROWS as i64 - 1].into(),
                    vec![1, BACKING_ROWS as i64, 1].into(),
                    values,
                    None,
                )?),
            ];
            for view in views {
                let nulls = Some(vec![true, false, true].into());
                let nullable_view = arrow::array::make_array(
                    view.to_data().into_builder().nulls(nulls.clone()).build()?,
                );
                let structure: ArrayRef = Arc::new(StructArray::try_new(
                    vec![Field::new("view", view.data_type().clone(), true)].into(),
                    vec![view],
                    nulls,
                )?);
                let list: ArrayRef = Arc::new(ListArray::try_new(
                    Arc::new(Field::new("item", structure.data_type().clone(), true)),
                    OffsetBuffer::from_repeated_length(1, 3),
                    structure.clone(),
                    None,
                )?);
                let fixed: ArrayRef = Arc::new(FixedSizeListArray::try_new_with_length(
                    Arc::new(Field::new("item", structure.data_type().clone(), true)),
                    1,
                    structure.clone(),
                    None,
                    3,
                )?);
                let large_list = arrow::compute::cast(
                    list.as_ref(),
                    &DataType::LargeList(Arc::new(Field::new(
                        "item",
                        structure.data_type().clone(),
                        true,
                    ))),
                )?;
                for (input, live_children) in [
                    (nullable_view, 2),
                    (structure, 2),
                    (list, 2),
                    (large_list, 2),
                    (fixed.slice(1, 2), 1),
                ] {
                    let rows = input.len();
                    let packed =
                        take_sort_values(&input, &UInt64Array::from_iter_values(0..rows as u64))?;
                    assert!(packed.get_array_memory_size() < 4096, "{packed:?}");
                    let mut leaf = &packed;
                    loop {
                        match leaf.data_type() {
                            DataType::Struct(_) => leaf = leaf.as_struct().column(0),
                            DataType::LargeList(_) => leaf = leaf.as_list::<i64>().values(),
                            _ => break,
                        }
                    }
                    assert_eq!(leaf.len(), live_children);
                    let encoded = encode(ColumnarValue::Array(input))?.into_array(rows)?;
                    assert!(encoded.as_binary::<i32>().value_data().len() < 4096);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn sliced_nested_dictionaries_visit_only_selected_values() -> Result<()> {
        use arrow::datatypes::Int32Type;

        const BACKING_ROWS: usize = 1_000_000;
        let values: ArrayRef = Arc::new(Int32Array::from_iter_values(0..BACKING_ROWS as i32));
        let views: ArrayRef = Arc::new(LargeListViewArray::try_new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            (0..BACKING_ROWS as i64).collect::<Vec<_>>().into(),
            (0..BACKING_ROWS)
                .map(|index| {
                    if index == 1 {
                        BACKING_ROWS as i64 - 1
                    } else {
                        1
                    }
                })
                .collect::<Vec<_>>()
                .into(),
            values,
            None,
        )?);
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
            Int32Array::from_iter_values(0..BACKING_ROWS as i32),
            views,
        )?);
        let structures: ArrayRef = Arc::new(StructArray::try_new(
            vec![Field::new(
                "dictionary",
                dictionary.data_type().clone(),
                true,
            )]
            .into(),
            vec![dictionary],
            None,
        )?);
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![
                Some(1),
                Some(0),
                Some(BACKING_ROWS as i32 - 1),
                None,
                Some(1),
            ]),
            structures,
        )?);
        let dictionary = dictionary.slice(1, 3);
        let packed = take_sort_values(&dictionary, &UInt64Array::from_iter_values(0..3))?;
        assert!(packed.get_array_memory_size() < 4096);
        let lists = packed.as_struct().column(0).as_list::<i64>();
        assert_eq!(
            lists.values().as_primitive::<Int32Type>().values().as_ref(),
            &[0, BACKING_ROWS as i32 - 1]
        );
        assert_eq!(lists.value_offsets(), &[0, 1, 2, 2]);
        assert!(packed.is_null(2));
        let encoded = encode(ColumnarValue::Array(dictionary))?.into_array(3)?;
        assert!(encoded.as_binary::<i32>().value_data().len() < 4096);
        assert!(encoded.is_null(2));
        Ok(())
    }

    #[test]
    fn floating_dictionary_keys_normalize_after_selection() -> Result<()> {
        use arrow::array::Float64Array;
        use arrow::datatypes::Float64Type;

        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            -0.0,
            0.0,
            f64::from_bits(0xfff8000000004321),
            f64::from_bits(0x7ff8000000001234),
        ]));
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0, 1, 2, 3]),
            values.clone(),
        )?);
        let structure: ArrayRef = Arc::new(StructArray::try_new(
            vec![Field::new("value", dictionary.data_type().clone(), true)].into(),
            vec![dictionary],
            None,
        )?);
        let encoded = encode(ColumnarValue::Array(structure))?.into_array(4)?;
        let encoded = encoded.as_binary::<i32>();
        assert_eq!(encoded.value(0), encoded.value(1));
        assert_eq!(encoded.value(2), encoded.value(3));
        let values = values.as_primitive::<Float64Type>();
        assert_eq!(values.value(0).to_bits(), (-0.0_f64).to_bits());
        assert_eq!(values.value(2).to_bits(), 0xfff8000000004321);
        Ok(())
    }
}
