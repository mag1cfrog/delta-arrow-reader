// DAR extension to Sail v0.7.1. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
use std::sync::Arc;

use datafusion::arrow::array::{Array, ListArray, MutableArrayData, make_array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::DataType;
use datafusion_common::cast::{as_int64_array, as_list_array};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

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
