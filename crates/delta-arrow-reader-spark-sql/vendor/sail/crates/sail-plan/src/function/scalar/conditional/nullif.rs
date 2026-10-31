//! NULLIF returns its original left value when comparison is false or NULL.
//! For example, NULLIF(9007199254740993L, 0D) keeps the exact BIGINT value even
//! though equality uses DOUBLE. NULL left values skip comparison.

use arrow::array::AsArray;
use arrow::compute::kernels::nullif::nullif;
use arrow::datatypes::DataType;
use datafusion::functions::core::nullif::NullIfFunc;
use datafusion::physical_expr_common::datum::apply_cmp;
use datafusion_common::{Result, ScalarValue, utils::take_function_args};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{
    ColumnarValue, Expr, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, lit,
};

use crate::function::common::{SparkComparisonFloat, spark_comparison_widens_to_double};

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SparkNullIf {
    native: NullIfFunc,
    ansi_mode: bool,
}

impl SparkNullIf {
    pub(crate) fn new(ansi_mode: bool) -> Self {
        Self {
            native: NullIfFunc::new(),
            ansi_mode,
        }
    }

    fn comparison_type(&self, left: &DataType, right: &DataType) -> Result<DataType> {
        if spark_comparison_widens_to_double(left, right, self.ansi_mode) {
            return Ok(DataType::Float64);
        }
        comparison_coercion(left, right).ok_or_else(|| {
            datafusion_common::plan_datafusion_err!(
                "For function 'nullif' {left} and {right} is not comparable"
            )
        })
    }
}

impl ScalarUDFImpl for SparkNullIf {
    fn name(&self) -> &str {
        self.native.name()
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(2, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [left, right] = take_function_args(self.name(), args)?;
        self.comparison_type(left, right)?;
        Ok(left.clone())
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn short_circuits_on_null_first_arg(&self) -> bool {
        true
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        args.split_first()
            .map(|(first, rest)| (vec![first], rest.iter().collect()))
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if matches!(args.first(), Some(Expr::Literal(value, _)) if value.is_null()) {
            // Keep comparability validation even when the right input is discarded.
            let original = ScalarUDF::from(Self::new(self.ansi_mode)).call(args);
            return Ok(ExprSimplifyResult::Simplified(lit(
                ScalarValue::try_new_null(&info.get_data_type(&original)?)?,
            )));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, mut input: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = take_function_args(self.name(), input.args.as_slice())?;
        let left = left.clone();
        let same_type = left.data_type() == right.data_type();
        let comparison_type = self.comparison_type(&left.data_type(), &right.data_type())?;
        if !same_type {
            for (value, field) in input.args.iter_mut().zip(&mut input.arg_fields) {
                *value = value.cast_to(&comparison_type, None)?;
                *field = std::sync::Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(comparison_type.clone()),
                );
            }
        }
        // Only equality with zero or NaN can change under float normalization.
        let normalize_floats = matches!(comparison_type, DataType::Float32 | DataType::Float64)
            && !input.args.iter().any(|value| match value {
                ColumnarValue::Scalar(ScalarValue::Float32(Some(value))) => {
                    *value != 0.0 && !value.is_nan()
                }
                ColumnarValue::Scalar(ScalarValue::Float64(Some(value))) => {
                    *value != 0.0 && !value.is_nan()
                }
                _ => false,
            });
        if same_type && !normalize_floats {
            return self.native.invoke_with_args(input);
        }
        if normalize_floats {
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
        let right_field = Arc::new(Field::new("right", right.data_type(), true));
        SparkNullIf::new(true).invoke_with_args(ScalarFunctionArgs {
            args: vec![left, right],
            arg_fields: vec![field.clone(), right_field],
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
    fn empty_batches_skip_nullif_and_arithmetic_operands() -> Result<()> {
        use arrow::datatypes::Schema;
        use arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;
        use datafusion_common::DFSchema;
        use datafusion_expr::{Volatility, col, create_udf};
        use sail_function::scalar::math::spark_checked_arithmetic::SparkCheckedArithmetic;

        let operand = create_udf(
            "fail_if_evaluated",
            vec![DataType::Int32],
            DataType::Int32,
            Volatility::Volatile,
            Arc::new(|_| datafusion_common::exec_err!("unexpected operand evaluation")),
        );
        let batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int32,
            true,
        )])));
        let functions = std::iter::once(ScalarUDF::from(SparkNullIf::new(true))).chain(
            [Operator::Plus, Operator::Minus, Operator::Multiply]
                .map(|operator| ScalarUDF::from(SparkCheckedArithmetic::new(operator))),
        );
        for function in functions {
            for right in [operand.call(vec![col("a")]), lit(7_i32)] {
                let expression = function.call(vec![operand.call(vec![col("a")]), right]);
                let physical = SessionContext::new()
                    .create_physical_expr(expression, &DFSchema::try_from(batch.schema())?)?;
                let result = physical.evaluate(&batch)?.into_array(0)?;
                assert_eq!(result.len(), 0);
                assert_eq!(result.data_type(), &DataType::Int32);
            }
        }
        Ok(())
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
            let other_width = if right.data_type() == &DataType::Float32 {
                DataType::Float64
            } else {
                DataType::Float32
            };
            let converted_right = arrow::compute::cast(&right, &other_width)?;
            for right in [right, converted_right] {
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
        }
        Ok(())
    }
}
