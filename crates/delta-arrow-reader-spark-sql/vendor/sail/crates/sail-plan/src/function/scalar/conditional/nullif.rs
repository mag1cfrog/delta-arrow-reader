//! NULLIF compares floating values with Spark equality and returns the original
//! left value when unequal. For example, NULLIF(-0.0, 0.0) is NULL, but
//! NULLIF(-0.0, 1.0) retains negative zero.

use arrow::array::AsArray;
use arrow::compute::kernels::nullif::nullif;
use arrow::datatypes::DataType;
use datafusion::functions::core::nullif::NullIfFunc;
use datafusion::physical_expr_common::datum::apply_cmp;
use datafusion_common::{Result, ScalarValue, utils::take_function_args};
use datafusion_expr::{ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature};

use crate::function::common::SparkComparisonFloat;

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct SparkNullIf(NullIfFunc);

impl ScalarUDFImpl for SparkNullIf {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.0.return_type(args)
    }

    fn invoke_with_args(&self, mut input: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, _] = take_function_args(self.name(), input.args.as_slice())?;
        if !matches!(left.data_type(), DataType::Float32 | DataType::Float64) {
            return self.0.invoke_with_args(input);
        }
        // Only equality with zero or NaN can change under float normalization.
        if input.args.iter().any(|value| match value {
            ColumnarValue::Scalar(ScalarValue::Float32(Some(value))) => {
                *value != 0.0 && !value.is_nan()
            }
            ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) => {
                *value != 0.0 && !value.is_nan()
            }
            _ => false,
        }) {
            return self.0.invoke_with_args(input);
        }
        let left = left.clone();
        let comparison_float = SparkComparisonFloat { widen: false };
        for (value, field) in input.args.iter_mut().zip(&input.arg_fields) {
            *value = comparison_float.invoke_with_args(ScalarFunctionArgs {
                args: vec![value.clone()],
                arg_fields: vec![field.clone()],
                number_rows: input.number_rows,
                return_field: field.clone(),
                config_options: input.config_options.clone(),
            })?;
        }
        // Only copy validity: normalization must not change an unequal value's bits.
        match apply_cmp(Operator::Eq, &input.args[0], &input.args[1])? {
            ColumnarValue::Array(equal) => Ok(ColumnarValue::Array(nullif(
                left.into_array(equal.len())?.as_ref(),
                equal.as_boolean(),
            )?)),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_new_null(&left.data_type())?,
            )),
            ColumnarValue::Scalar(ScalarValue::Boolean(_)) => Ok(left),
            _ => datafusion_common::internal_err!("equality returned a non-Boolean result"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array};
    use arrow::datatypes::{Field, Float32Type, Float64Type};
    use datafusion_common::{ScalarValue, config::ConfigOptions};
    use std::sync::Arc;

    fn invoke(left: ColumnarValue, right: ColumnarValue, rows: usize) -> Result<ColumnarValue> {
        let field = Arc::new(Field::new("value", left.data_type(), true));
        SparkNullIf::default().invoke_with_args(ScalarFunctionArgs {
            args: vec![left, right],
            arg_fields: vec![field.clone(), field.clone()],
            number_rows: rows,
            return_field: field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn bits(array: &dyn Array, row: usize) -> u64 {
        match array.data_type() {
            DataType::Float32 => {
                u64::from(array.as_primitive::<Float32Type>().value(row).to_bits())
            }
            DataType::Float64 => array.as_primitive::<Float64Type>().value(row).to_bits(),
            _ => unreachable!("test inputs are FLOAT or DOUBLE"),
        }
    }

    #[test]
    fn equal_zeros_and_nan_payloads_change_only_validity() -> Result<()> {
        let double_nan = f64::from_bits(0x7ff8_0000_0000_1234);
        let negative_double_nan = f64::from_bits(0xfff8_0000_0000_4321);
        let float_nan = f32::from_bits(0x7fc0_1234);
        let negative_float_nan = f32::from_bits(0xffc0_4321);
        let left: [ArrayRef; 2] = [
            Arc::new(Float32Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(float_nan),
                Some(negative_float_nan),
                Some(float_nan),
                Some(-0.0),
                Some(f32::INFINITY),
                Some(-f32::INFINITY),
                Some(1.0),
                Some(1.0),
                None,
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(double_nan),
                Some(negative_double_nan),
                Some(double_nan),
                Some(-0.0),
                Some(f64::INFINITY),
                Some(-f64::INFINITY),
                Some(1.0),
                Some(1.0),
                None,
                None,
            ])),
        ];
        let right: [ArrayRef; 2] = [
            Arc::new(Float32Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(negative_float_nan),
                Some(float_nan),
                Some(2.0),
                Some(2.0),
                Some(f32::INFINITY),
                Some(f32::INFINITY),
                Some(2.0),
                None,
                Some(1.0),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(negative_double_nan),
                Some(double_nan),
                Some(2.0),
                Some(2.0),
                Some(f64::INFINITY),
                Some(f64::INFINITY),
                Some(2.0),
                None,
                Some(1.0),
                None,
            ])),
        ];
        let expected_nulls = [
            true, true, true, true, false, false, true, false, false, false, true, true,
        ];
        for (left, right) in left.into_iter().zip(right) {
            for (offset, len) in [(0, 12), (1, 7), (0, 0)] {
                let lhs = left.slice(offset, len);
                let rhs = right.slice(offset, len);
                let result = invoke(
                    ColumnarValue::Array(lhs.clone()),
                    ColumnarValue::Array(rhs),
                    len,
                )?
                .into_array(len)?;
                assert_eq!(result.data_type(), lhs.data_type());
                assert_eq!(result.len(), len);
                for row in 0..len {
                    assert_eq!(result.is_null(row), expected_nulls[offset + row]);
                    if !result.is_null(row) {
                        assert_eq!(bits(result.as_ref(), row), bits(lhs.as_ref(), row));
                    }
                }
            }
            for (row, expected_null) in expected_nulls.into_iter().enumerate() {
                let lhs = ScalarValue::try_from_array(&left, row)?;
                let rhs = ScalarValue::try_from_array(&right, row)?;
                for rows in [0, 1] {
                    for (a, b) in [
                        (
                            ColumnarValue::Scalar(lhs.clone()),
                            ColumnarValue::Scalar(rhs.clone()),
                        ),
                        (
                            ColumnarValue::Scalar(lhs.clone()),
                            ColumnarValue::Array(right.slice(row, rows)),
                        ),
                        (
                            ColumnarValue::Array(left.slice(row, rows)),
                            ColumnarValue::Scalar(rhs.clone()),
                        ),
                    ] {
                        let result = invoke(a, b, rows)?.into_array(rows)?;
                        assert_eq!(result.data_type(), left.data_type());
                        assert_eq!(result.len(), rows);
                        if rows != 0 {
                            assert_eq!(result.is_null(0), expected_null);
                            if !expected_null {
                                assert_eq!(bits(result.as_ref(), 0), bits(left.as_ref(), row));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
