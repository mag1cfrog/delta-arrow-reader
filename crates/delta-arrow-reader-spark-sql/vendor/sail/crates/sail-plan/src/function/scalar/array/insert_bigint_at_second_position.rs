// DAR extension to Sail v0.7.1. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, ListArray, MutableArrayData, make_array};
use datafusion::arrow::buffer::{BooleanBuffer, OffsetBuffer};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_int64_array, as_list_array};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct HasSecondPosition {
    signature: Signature,
}

impl Default for HasSecondPosition {
    fn default() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for HasSecondPosition {
    fn name(&self) -> &str {
        "spark_array_has_second_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arguments: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [input] = args.args.as_slice() else {
            return internal_err!("The second-position predicate requires one argument.");
        };
        let (lists, scalar) = match input {
            ColumnarValue::Array(array) => (as_list_array(array.as_ref())?, false),
            ColumnarValue::Scalar(ScalarValue::List(array)) => (array.as_ref(), true),
            _ => return internal_err!("The second-position predicate requires a List."),
        };
        let offsets = lists.value_offsets();
        let values =
            BooleanBuffer::collect_bool(lists.len(), |row| offsets[row + 1] - offsets[row] > 1);
        let predicate = BooleanArray::new(values, lists.nulls().cloned());
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::Boolean(
                predicate.is_valid(0).then(|| predicate.value(0)),
            ))
        } else {
            ColumnarValue::Array(Arc::new(predicate))
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct InsertBigIntAtSecondPosition {
    signature: Signature,
}

impl Default for InsertBigIntAtSecondPosition {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for InsertBigIntAtSecondPosition {
    fn name(&self) -> &str {
        "spark_insert_bigint_at_second_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        Ok(arguments[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        let arguments = ColumnarValue::values_to_arrays(&args.args)?;
        let [lists, element] = arguments.as_slice() else {
            return internal_err!("The second-position array insert requires two arguments.");
        };
        let lists = as_list_array(lists.as_ref())?;
        let element = as_int64_array(element.as_ref())?;
        let DataType::List(field) = lists.data_type() else {
            unreachable!("ListArray always has a List type");
        };

        let offsets = lists.value_offsets();
        let mut lengths = Vec::with_capacity(lists.len());
        let mut total = 0_usize;
        for (row, bounds) in offsets.windows(2).enumerate() {
            let length = if lists.is_null(row) {
                0
            } else {
                ((bounds[1] - bounds[0]) as usize).max(1) + 1
            };
            total = total
                .checked_add(length)
                .filter(|total| *total <= i32::MAX as usize)
                .ok_or_else(|| {
                    exec_datafusion_err!("ARRAY_INSERT result exceeds List offset capacity")
                })?;
            lengths.push(length);
        }

        let old = lists.values().to_data();
        let added = element.to_data();
        let mut values = MutableArrayData::new(vec![&old, &added], true, total);
        for (row, bounds) in offsets.windows(2).enumerate() {
            if lengths[row] == 0 {
                continue;
            }
            let start = bounds[0] as usize;
            let end = bounds[1] as usize;
            let prefix_end = if start == end {
                values.extend_nulls(1);
                start
            } else {
                values.extend(0, start, start + 1);
                start + 1
            };
            values.extend(1, row, row + 1);
            values.extend(0, prefix_end, end);
        }

        let result = Arc::new(ListArray::try_new(
            field.clone(),
            OffsetBuffer::from_lengths(lengths),
            make_array(values.freeze()),
            lists.nulls().cloned(),
        )?);
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::List(result))
        } else {
            ColumnarValue::Array(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{Field, Int64Type};
    use datafusion_common::config::ConfigOptions;

    #[test]
    fn offset_copy_uses_the_sliced_rows_and_preserves_the_child_field() -> Result<()> {
        let source = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(90)]),
            Some(vec![Some(1), None, Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![Some(91)]),
        ]);
        let field = Arc::new(
            Field::new("member", DataType::Int64, true)
                .with_metadata([("unit".to_owned(), "items".to_owned())].into()),
        );
        let lists = ListArray::try_new(
            Arc::clone(&field),
            source.offsets().clone(),
            Arc::clone(source.values()),
            source.nulls().cloned(),
        )?
        .slice(1, 2);
        assert_eq!(lists.value_offsets(), [1, 4, 6]);

        let output_field = Arc::new(Field::new("out", lists.data_type().clone(), true));
        let result = InsertBigIntAtSecondPosition::default()
            .invoke_with_args(ScalarFunctionArgs {
                arg_fields: vec![
                    Arc::clone(&output_field),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ],
                args: vec![
                    ColumnarValue::Array(Arc::new(lists)),
                    ColumnarValue::Array(Arc::new(Int64Array::from(vec![None, Some(-1)]))),
                ],
                number_rows: 2,
                return_field: Arc::clone(&output_field),
                config_options: Arc::new(ConfigOptions::new()),
            })?
            .into_array(2)?;
        let list = as_list_array(result.as_ref())?;
        assert_eq!(list.data_type(), &DataType::List(field));
        assert_eq!(list.value_offsets(), [0, 4, 7]);
        assert_eq!(
            as_int64_array(list.values().as_ref())?
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), None, None, Some(3), Some(4), Some(-1), Some(5)],
        );
        list.to_data().validate_full()?;
        Ok(())
    }
}
