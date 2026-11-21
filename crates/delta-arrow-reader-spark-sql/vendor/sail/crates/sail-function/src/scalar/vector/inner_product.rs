use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Float32Array, GenericListArray, OffsetSizeTrait, as_large_list_array,
    as_list_array,
};
use datafusion::arrow::datatypes::{DataType, Float32Type};
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::functions_nested_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct VectorInnerProduct {
    signature: Signature,
}

impl Default for VectorInnerProduct {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorInnerProduct {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for VectorInnerProduct {
    fn name(&self) -> &str {
        "vector_inner_product"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 || !arg_types.iter().all(is_float_vector) {
            return plan_err!(
                "vector_inner_product expects two ARRAY<FLOAT> arguments, got {arg_types:?}"
            );
        }
        Ok(DataType::Float32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(vector_inner_product_inner)(&args.args)
    }
}

fn is_float_vector(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(field) | DataType::LargeList(field)
            if field.data_type() == &DataType::Float32
    )
}

fn vector_inner_product_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("vector_inner_product needs exactly two arguments");
    }
    match (args[0].data_type(), args[1].data_type()) {
        (DataType::List(_), DataType::List(_)) => {
            compute_inner_product(as_list_array(&args[0]), as_list_array(&args[1]))
        }
        (DataType::LargeList(_), DataType::LargeList(_)) => {
            compute_inner_product(as_large_list_array(&args[0]), as_large_list_array(&args[1]))
        }
        (left, right) => exec_err!(
            "vector_inner_product expects matching array types, got {left:?} and {right:?}"
        ),
    }
}

/// Spark 4.2 `vectorInnerProduct` accumulates products eight at a time:
/// `sum += a0*b0 + ... + a7*b7` then a scalar tail. Sequential f32 folds can
/// differ when large values cancel across an eight-element boundary.
fn dot_product_spark(left: &[f32], right: &[f32]) -> f32 {
    debug_assert_eq!(left.len(), right.len());
    let mut sum = 0.0f32;
    let mut i = 0;
    let simd_limit = (left.len() / 8) * 8;
    while i < simd_limit {
        sum += left[i] * right[i]
            + left[i + 1] * right[i + 1]
            + left[i + 2] * right[i + 2]
            + left[i + 3] * right[i + 3]
            + left[i + 4] * right[i + 4]
            + left[i + 5] * right[i + 5]
            + left[i + 6] * right[i + 6]
            + left[i + 7] * right[i + 7];
        i += 8;
    }
    while i < left.len() {
        sum += left[i] * right[i];
        i += 1;
    }
    sum
}

fn compute_inner_product<O: OffsetSizeTrait>(
    left: &GenericListArray<O>,
    right: &GenericListArray<O>,
) -> Result<ArrayRef> {
    let child_buffers = left
        .values()
        .as_primitive_opt::<Float32Type>()
        .zip(right.values().as_primitive_opt::<Float32Type>())
        .filter(|(left, right)| left.null_count() == 0 && right.null_count() == 0);

    let values = (0..left.len()).map(|row| {
        if left.is_null(row) || right.is_null(row) {
            return Ok(None);
        }
        let left_length = left.value_length(row).as_usize();
        let right_length = right.value_length(row).as_usize();
        if left_length != right_length {
            return exec_err!(
                "vector_inner_product requires vectors with matching dimensions, got {left_length} and {right_length}"
            );
        }
        if let Some((left_values, right_values)) = child_buffers {
            let left_offsets = left.value_offsets();
            let right_offsets = right.value_offsets();
            let left_row = &left_values.values()
                [left_offsets[row].as_usize()..left_offsets[row + 1].as_usize()];
            let right_row = &right_values.values()
                [right_offsets[row].as_usize()..right_offsets[row + 1].as_usize()];
            return Ok(Some(dot_product_spark(left_row, right_row)));
        }
        let left = left.value(row);
        let right = right.value(row);
        let left = left.as_primitive::<Float32Type>();
        let right = right.as_primitive::<Float32Type>();
        if left.null_count() > 0 || right.null_count() > 0 {
            return Ok(None);
        }
        // Match Spark 4.2: accumulate products in groups of eight so floating-point
        // cancellation across an 8-element boundary stays Spark-compatible.
        Ok(Some(dot_product_spark(left.values(), right.values())))
    });
    let values = values.collect::<Result<Vec<Option<f32>>>>()?;
    Ok(Arc::new(Float32Array::from(values)))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{LargeListArray, ListArray};

    use super::*;

    #[test]
    fn computes_spark_compatible_results() -> Result<()> {
        let left = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
            Some(vec![]),
            Some(vec![Some(1.0), None, Some(3.0)]),
            None,
        ]);
        let right = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(4.0), Some(5.0), Some(6.0)]),
            Some(vec![]),
            Some(vec![Some(1.0), Some(2.0), Some(3.0)]),
            Some(vec![Some(1.0)]),
        ]);

        let actual = compute_inner_product(&left, &right)?;
        let actual = actual.as_primitive::<Float32Type>();
        let expected = Float32Array::from(vec![Some(32.0), Some(0.0), None, None]);
        assert_eq!(actual, &expected);
        Ok(())
    }

    #[test]
    fn direct_child_buffers_match_row_slices() -> Result<()> {
        fn compare<O: OffsetSizeTrait>(
            left: GenericListArray<O>,
            right: GenericListArray<O>,
        ) -> Result<()> {
            let left = left.slice(1, left.len() - 2);
            let right = right.slice(1, right.len() - 2);
            // The final NULL lies outside the compared rows and forces the row-slice path.
            let (field, offsets, children, nulls) = right.clone().into_parts();
            let last = offsets.last().unwrap().as_usize();
            let direct = GenericListArray::try_new(field, offsets, children.slice(0, last), nulls)?;

            let direct = compute_inner_product(&left, &direct)?;
            let sliced = compute_inner_product(&left, &right)?;
            let bits = |array: &ArrayRef| {
                array
                    .as_primitive::<Float32Type>()
                    .iter()
                    .map(|value| value.map(f32::to_bits))
                    .collect::<Vec<_>>()
            };
            assert_eq!(bits(&direct), bits(&sliced));
            assert!(compute_inner_product(&left.slice(0, 0), &right.slice(0, 0))?.is_empty());
            Ok(())
        }

        let payload_nan = f32::from_bits(0x7fc0_0123);
        let cancellation = [
            1.0e20, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0e20, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
            2.0,
        ];
        let left = vec![
            Some(vec![Some(11.0)]),
            Some(cancellation.into_iter().map(Some).collect()),
            None,
            Some(vec![]),
            Some(vec![Some(-0.0), Some(0.0)]),
            Some(vec![Some(f32::INFINITY), Some(payload_nan)]),
            Some(vec![]),
        ];
        let right = vec![
            Some(vec![Some(13.0)]),
            Some(vec![Some(1.0); cancellation.len()]),
            Some(vec![Some(3.0)]),
            Some(vec![]),
            Some(vec![Some(1.0), Some(-1.0)]),
            Some(vec![Some(1.0), Some(f32::INFINITY)]),
            Some(vec![None]),
        ];
        compare(
            ListArray::from_iter_primitive::<Float32Type, _, _>(left.clone()),
            ListArray::from_iter_primitive::<Float32Type, _, _>(right.clone()),
        )?;
        compare(
            LargeListArray::from_iter_primitive::<Float32Type, _, _>(left),
            LargeListArray::from_iter_primitive::<Float32Type, _, _>(right),
        )
    }

    #[test]
    fn rejects_dimension_mismatch() {
        let left = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![Some(1.0)])]);
        let right = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(vec![
            Some(1.0),
            Some(2.0),
        ])]);

        assert!(matches!(
            compute_inner_product(&left, &right),
            Err(error) if error.to_string().contains("matching dimensions")
        ));
    }

    #[test]
    fn accumulation_matches_spark_for_large_cancellation() -> Result<()> {
        // Left:  [1e20, 0,0,0,0,0,0,0, -1e20, 1,1,1,1,1,1,1]
        // Right: [1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1]
        // Spark 8-wide sum => 0.0; naive sequential fold => 7.0.
        let left_vals = [
            1.0e20f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -1.0e20, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
            1.0,
        ];
        let right_vals = [1.0f32; 16];
        let left = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(
            left_vals.iter().copied().map(Some).collect::<Vec<_>>(),
        )]);
        let right = ListArray::from_iter_primitive::<Float32Type, _, _>(vec![Some(
            right_vals.iter().copied().map(Some).collect::<Vec<_>>(),
        )]);

        let actual = compute_inner_product(&left, &right)?;
        let actual = actual.as_primitive::<Float32Type>();
        assert_eq!(actual.value(0), 0.0);
        assert_eq!(dot_product_spark(&left_vals, &right_vals), 0.0);
        Ok(())
    }
}
