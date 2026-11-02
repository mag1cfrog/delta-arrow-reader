//! Spark nested NULL children sort below values independently of container placement.

use std::sync::Arc;

use arrow::array::{Array, BinaryArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::row::{RowConverter, SortField};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

use crate::function::float_keys::normalize_spark_float_key;

pub(crate) fn spark_sort_key(expression: Expr, schema: &DFSchema) -> Result<Expr> {
    if is_nested_key(&expression.get_type(schema)?) {
        let expression = normalize_spark_float_key(expression, schema)?;
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
        ArrayRef, AsArray, DictionaryArray, Int8Array, Int32Array, ListArray, StructArray,
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
}
