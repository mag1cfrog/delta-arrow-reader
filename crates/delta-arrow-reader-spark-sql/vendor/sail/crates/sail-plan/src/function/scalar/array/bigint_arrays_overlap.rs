// Added to the vendored Sail planner for the Delta reader experiment.
// See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::ops::Range;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanBuilder, GenericListArray, Int64Array, OffsetSizeTrait,
};
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion_common::utils::take_function_args;
use datafusion_common::{HashSet, Result, ScalarValue, internal_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct BigIntArraysOverlap;

impl BigIntArraysOverlap {
    pub(super) const NAME: &'static str = "spark_bigint_arrays_overlap";
}

impl ScalarUDFImpl for BigIntArraysOverlap {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arguments: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args
            .args
            .iter()
            .any(|argument| matches!(argument, ColumnarValue::Scalar(value) if value.is_null()))
        {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }
        make_scalar_function(
            |arrays| {
                let [left, right] = take_function_args(self.name(), arrays)?;
                let result = match (left.data_type(), right.data_type()) {
                    (DataType::List(_), DataType::List(_)) => {
                        overlap_lists(left.as_list::<i32>(), right.as_list::<i32>())
                    }
                    (DataType::LargeList(_), DataType::LargeList(_)) => {
                        overlap_lists(left.as_list::<i64>(), right.as_list::<i64>())
                    }
                    types => {
                        return internal_err!("Unexpected BIGINT array overlap inputs {types:?}");
                    }
                };
                Ok(Arc::new(result) as ArrayRef)
            },
            vec![],
        )(&args.args)
    }
}

fn overlap_lists<O: OffsetSizeTrait>(
    left: &GenericListArray<O>,
    right: &GenericListArray<O>,
) -> datafusion::arrow::array::BooleanArray {
    let mut result = BooleanBuilder::with_capacity(left.len());
    let left_values = left.values().as_primitive::<Int64Type>();
    let right_values = right.values().as_primitive::<Int64Type>();
    for (row, (left_offsets, right_offsets)) in left
        .value_offsets()
        .windows(2)
        .zip(right.value_offsets().windows(2))
        .enumerate()
    {
        if left.is_null(row) || right.is_null(row) {
            result.append_null();
            continue;
        }
        let left_range = left_offsets[0].as_usize()..left_offsets[1].as_usize();
        let right_range = right_offsets[0].as_usize()..right_offsets[1].as_usize();
        result.append_option(overlap_row(
            left_values,
            left_range,
            right_values,
            right_range,
        ));
    }
    result.finish()
}

fn overlap_row(
    left: &Int64Array,
    mut left_range: Range<usize>,
    right: &Int64Array,
    mut right_range: Range<usize>,
) -> Option<bool> {
    if left_range.is_empty() || right_range.is_empty() {
        return Some(false);
    }

    // Small rows avoid the hash table. Larger rows index the smaller input.
    const MAX_DIRECT_COMPARISONS: usize = 256;
    let matches = if left_range.len().saturating_mul(right_range.len()) <= MAX_DIRECT_COMPARISONS {
        nonnull_values(left, left_range.clone())
            .any(|value| nonnull_values(right, right_range.clone()).any(|other| value == other))
    } else if left_range.len() <= right_range.len() {
        let lookup: HashSet<_> = nonnull_values(left, left_range.clone()).collect();
        nonnull_values(right, right_range.clone()).any(|value| lookup.contains(&value))
    } else {
        let lookup: HashSet<_> = nonnull_values(right, right_range.clone()).collect();
        nonnull_values(left, left_range.clone()).any(|value| lookup.contains(&value))
    };

    if matches {
        Some(true)
    } else if left_range.any(|row| left.is_null(row)) || right_range.any(|row| right.is_null(row)) {
        None
    } else {
        Some(false)
    }
}

fn nonnull_values(values: &Int64Array, range: Range<usize>) -> impl Iterator<Item = i64> + '_ {
    range
        .filter(move |&row| values.is_valid(row))
        .map(move |row| values.value(row))
}
