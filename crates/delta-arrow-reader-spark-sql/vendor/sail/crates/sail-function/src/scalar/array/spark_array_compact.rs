use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, GenericListArray, OffsetSizeTrait, as_large_list_array,
    as_list_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::functions_nested_utils::make_scalar_function;

/// `array_compact` removes all null values from the array.
/// This is the Spark-compatible implementation.
///
/// Returns null if the input array is null.
/// Returns an empty array if all elements are null.
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
        match &arg_types[0] {
            DataType::List(_) | DataType::LargeList(_) => Ok(arg_types[0].clone()),
            DataType::Null => Ok(DataType::Null),
            _ => plan_err!(
                "spark_array_compact can only accept List or LargeList, got {:?}",
                arg_types[0]
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        if args[0].data_type() == DataType::Null {
            return Ok(ColumnarValue::Scalar(ScalarValue::Null));
        }
        make_scalar_function(array_compact_inner)(&args)
    }
}

fn array_compact_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("spark_array_compact needs exactly one argument");
    }
    match args[0].data_type() {
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
        DataType::List(f) | DataType::LargeList(f) => f.clone(),
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

    use datafusion::arrow::array::{Int32Array, ListArray};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::datatypes::Field;

    #[test]
    fn array_compact_keeps_visible_sliced_values_and_fields() -> Result<()> {
        let field = Arc::new(Field::new_list_field(DataType::Int32, true)
            .with_metadata([("origin".into(), "compact-slice".into())].into()));
        let nulls = Some(NullBuffer::from(vec![true, true, false, true, true, true, true]));
        let input = ListArray::try_new(
            field.clone(), OffsetBuffer::from_lengths([2, 3, 2, 0, 2, 1, 1]),
            Arc::new(Int32Array::from(vec![Some(900), Some(901), Some(3), None, Some(4),
                Some(700), Some(701), None, None, Some(800), Some(99)])), nulls.clone(),
        )?;
        let expected = ListArray::try_new(
            field.clone(), OffsetBuffer::from_lengths([2, 2, 0, 0, 0, 1, 1]),
            Arc::new(Int32Array::from(vec![900, 901, 3, 4, 800, 99])), nulls,
        )?;
        for data_type in [DataType::List(field.clone()), DataType::LargeList(field)] {
            let input = compute::cast(&input, &data_type)?;
            let expected = compute::cast(&expected, &data_type)?;
            for (offset, len) in [(0, 7), (1, 5), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (3, 0)] {
                let argument = input.slice(offset, len);
                let arg_field = Arc::new(Field::new("a", data_type.clone(), true));
                let actual = SparkArrayCompact::new().invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(argument)],
                    arg_fields: vec![arg_field.clone()],
                    number_rows: len,
                    return_field: arg_field,
                    config_options: Arc::default(),
                })?.into_array(len)?;
                actual.to_data().validate_full()?;
                assert_eq!(actual.data_type(), &data_type);
                assert_eq!(actual.as_ref(), expected.slice(offset, len).as_ref(),
                    "{data_type} offset={offset} len={len}");
            }
        }
        Ok(())
    }
}
