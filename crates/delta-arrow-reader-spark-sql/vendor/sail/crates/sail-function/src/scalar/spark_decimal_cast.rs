//! Numeric Decimal casts retain Spark schemas and error modes around Arrow conversion.
use std::sync::Arc;

use arrow::array::{ArrayRef, ArrowNativeTypeOp, AsArray, PrimitiveArray};
use arrow::compute::CastOptions;
use arrow::datatypes::{
    ArrowNativeType, ArrowPrimitiveType, DataType, Decimal128Type, Decimal256Type, DecimalType,
    Field, FieldRef, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, i256,
};
use arrow::error::ArrowError;
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::traits::AsPrimitive;

use crate::functions_utils::make_scalar_function;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DecimalCastMode {
    Ansi,
    Legacy,
    Try,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDecimalCast {
    target: DataType,
    signature: Signature,
    mode: DecimalCastMode,
}

impl SparkDecimalCast {
    pub fn to_decimal(precision: u8, scale: i8, mode: DecimalCastMode) -> Self {
        Self {
            target: DataType::Decimal128(precision, scale),
            signature: Signature::numeric(1, Volatility::Immutable),
            mode,
        }
    }

    pub fn mode(&self) -> DecimalCastMode {
        self.mode
    }

    pub fn to_integer(target: DataType, mode: DecimalCastMode) -> Result<Self> {
        if !target.is_signed_integer() {
            return plan_err!("Decimal CAST requires a signed integer target, got {target}");
        }
        Ok(Self {
            target,
            signature: Signature::user_defined(Volatility::Immutable),
            mode,
        })
    }

    fn convert(&self, arg: &ColumnarValue) -> Result<ColumnarValue> {
        let options = CastOptions {
            safe: self.mode != DecimalCastMode::Ansi,
            ..DEFAULT_CAST_OPTIONS
        };
        if self.target.is_signed_integer() {
            self.coerce_types(&[arg.data_type()])?;
            let extreme_scale = matches!(arg.data_type(), DataType::Decimal128(_, scale) if scale < -38)
                || matches!(arg.data_type(), DataType::Decimal256(_, scale) if scale < -76);
            if (self.mode == DecimalCastMode::Legacy || extreme_scale) && !arg.data_type().is_null()
            {
                let target = self.target.clone();
                let mode = self.mode;
                return make_scalar_function(
                    move |arrays| match target {
                        DataType::Int8 => integer_array::<Int8Type>(&arrays[0], mode),
                        DataType::Int16 => integer_array::<Int16Type>(&arrays[0], mode),
                        DataType::Int32 => integer_array::<Int32Type>(&arrays[0], mode),
                        DataType::Int64 => integer_array::<Int64Type>(&arrays[0], mode),
                        _ => unreachable!("the constructor checks the integer target"),
                    },
                    vec![],
                )(&[arg.clone()]);
            }
            return arg
                .cast_to(&self.target, Some(&options))
                .map_err(|error| match &error {
                    DataFusionError::ArrowError(cause, _)
                        if self.mode == DecimalCastMode::Ansi
                            && matches!(
                                cause.as_ref(),
                                ArrowError::CastError(_) | ArrowError::ArithmeticOverflow(_)
                            ) =>
                    {
                        error.context("[CAST_OVERFLOW]")
                    }
                    _ => error,
                });
        }
        match arg.cast_to(&self.target, Some(&options)) {
            Ok(value) => Ok(value),
            Err(error) => {
                // Spark maps non-finite input to NULL even in ANSI mode. Retry
                // after masking only those values; finite overflow still errors.
                let filtered = match arg {
                    ColumnarValue::Scalar(ScalarValue::Float32(v)) => {
                        ColumnarValue::Scalar(ScalarValue::Float32(v.filter(|v| v.is_finite())))
                    }
                    ColumnarValue::Scalar(ScalarValue::Float64(v)) => {
                        ColumnarValue::Scalar(ScalarValue::Float64(v.filter(|v| v.is_finite())))
                    }
                    ColumnarValue::Array(a) if a.data_type() == &DataType::Float32 => {
                        ColumnarValue::Array(Arc::new(
                            a.as_primitive::<Float32Type>()
                                .unary_opt::<_, Float32Type>(|v| v.is_finite().then_some(v)),
                        ))
                    }
                    ColumnarValue::Array(a) if a.data_type() == &DataType::Float64 => {
                        ColumnarValue::Array(Arc::new(
                            a.as_primitive::<Float64Type>()
                                .unary_opt::<_, Float64Type>(|v| v.is_finite().then_some(v)),
                        ))
                    }
                    _ => return Err(error),
                };
                filtered.cast_to(&self.target, None)
            }
        }
    }
}

impl ScalarUDFImpl for SparkDecimalCast {
    fn name(&self) -> &str {
        if self.target.is_signed_integer() {
            "spark_cast_decimal_to_integer"
        } else {
            "spark_cast_numeric_to_decimal"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        if self.target.is_signed_integer() {
            self.coerce_types(arguments)?;
        }
        Ok(self.target.clone())
    }

    fn coerce_types(&self, arguments: &[DataType]) -> Result<Vec<DataType>> {
        let [data_type] = arguments else {
            return exec_err!("Decimal-to-integer CAST expects one argument");
        };
        if !matches!(
            data_type,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) | DataType::Null
        ) {
            return exec_err!("Decimal-to-integer CAST expects DECIMAL, got {data_type}");
        }
        Ok(arguments.to_vec())
    }

    fn simplify(
        &self,
        args: Vec<datafusion_expr::Expr>,
        _info: &datafusion_expr::simplify::SimplifyContext,
    ) -> Result<datafusion_expr::simplify::ExprSimplifyResult> {
        use datafusion_expr::{Expr, lit, simplify::ExprSimplifyResult};
        if matches!(args.as_slice(), [Expr::Literal(value, _)] if value.is_null()) {
            return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::try_from(
                &self.target,
            )?)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = args.arg_fields else {
            return exec_err!(
                "{} CAST expects one argument",
                if self.target.is_signed_integer() {
                    "Decimal-to-integer"
                } else {
                    "numeric-to-Decimal"
                }
            );
        };
        let from = input.data_type();
        let nullable = if let DataType::Decimal128(precision, scale) = self.target {
            // TRY_CAST also declares nullable for Decimal scale reduction without overflow.
            let loses_scale = self.mode == DecimalCastMode::Try
                && matches!(from, DataType::Decimal128(_, source_scale) | DataType::Decimal256(_, source_scale)
                    if scale < *source_scale);
            input.is_nullable() || decimal_cast_can_overflow(from, precision, scale) || loses_scale
        } else {
            self.coerce_types(std::slice::from_ref(from))?;
            true
        };
        Ok(Arc::new(Field::new(
            self.name(),
            self.target.clone(),
            nullable,
        )))
    }

    // A parent that keeps Spark's row order of errors needs this cast's first failing row.
    fn invoke_rows_until_error(
        &self,
        args: ScalarFunctionArgs,
    ) -> Result<datafusion_expr::EvaluatedRowPrefix> {
        crate::scalar::row_errors::invoke_rows_until_first_failing_row(args, |args| {
            self.invoke_with_args(args)
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!(
                "{} CAST expects one argument",
                if self.target.is_signed_integer() {
                    "Decimal-to-integer"
                } else {
                    "numeric-to-Decimal"
                }
            );
        };
        self.convert(arg)
    }
}

fn integer_array<T: ArrowPrimitiveType>(array: &ArrayRef, mode: DecimalCastMode) -> Result<ArrayRef>
where
    i128: AsPrimitive<T::Native>,
{
    match array.data_type() {
        DataType::Decimal128(_, scale) => {
            integer_values::<Decimal128Type, T>(array, *scale, mode, |value| value)
        }
        DataType::Decimal256(_, scale) => {
            integer_values::<Decimal256Type, T>(array, *scale, mode, i256::as_i128)
        }
        other => exec_err!("Decimal-to-integer CAST expects DECIMAL, got {other}"),
    }
}

fn integer_values<D: DecimalType, T: ArrowPrimitiveType>(
    array: &ArrayRef,
    scale: i8,
    mode: DecimalCastMode,
    low_bits: impl Fn(D::Native) -> i128,
) -> Result<ArrayRef>
where
    i128: AsPrimitive<T::Native>,
{
    let divisor = D::Native::usize_as(10).pow_checked(scale.max(0) as u32)?;
    let multiplier = 10_i128.wrapping_pow(u32::from(scale.unsigned_abs()));
    let result: PrimitiveArray<T> = array
        .as_primitive::<D>()
        .iter()
        .map(|value| {
            let Some(value) = value else { return Ok(None) };
            if mode != DecimalCastMode::Legacy {
                // Here 10^-scale exceeds the source width; only zero can fit an integer.
                return if value.is_zero() {
                    Ok(Some(T::Native::usize_as(0)))
                } else if mode == DecimalCastMode::Try {
                    Ok(None)
                } else {
                    exec_err!("[CAST_OVERFLOW] Decimal value cannot fit {}", T::DATA_TYPE)
                };
            }
            // Divide the full coefficient before narrowing; high bits affect the quotient.
            let value = if scale >= 0 {
                low_bits(value.div_wrapping(divisor))
            } else {
                low_bits(value).wrapping_mul(multiplier)
            };
            Ok(Some(value.as_()))
        })
        .collect::<Result<_>>()?;
    Ok(Arc::new(result))
}

pub fn decimal_cast_can_overflow(from: &DataType, precision: u8, scale: i8) -> bool {
    let integer_digits = i16::from(precision) - i16::from(scale);
    match from {
        DataType::Decimal128(from_precision, from_scale)
        | DataType::Decimal256(from_precision, from_scale) => {
            let source_integer_digits = i16::from(*from_precision) - i16::from(*from_scale);
            integer_digits < source_integer_digits
                || (integer_digits == source_integer_digits && scale < *from_scale)
        }
        DataType::Int8 | DataType::UInt8 => scale < 0 || integer_digits < 3,
        DataType::Int16 | DataType::UInt16 => scale < 0 || integer_digits < 5,
        DataType::Int32 | DataType::UInt32 => scale < 0 || integer_digits < 10,
        DataType::Int64 | DataType::UInt64 => scale < 0 || integer_digits < 20,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Decimal128Array, Float64Array, new_null_array};

    use super::*;

    fn assert_scalar_result(actual: ColumnarValue, expected: ScalarValue) -> Result<()> {
        let ColumnarValue::Scalar(actual) = actual else {
            return exec_err!("Decimal scalar CAST returned an array");
        };
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn integer_cast_wraps_after_full_decimal256_truncation_and_keeps_slices() -> Result<()> {
        let wide = i256::from_parts(17, 1);
        let values = [None, Some(wide), Some(-wide), Some(i256::ZERO), None];
        let input = ScalarValue::iter_to_array(
            values
                .into_iter()
                .map(|value| ScalarValue::Decimal256(value, 76, 1)),
        )?;
        for (target, positive, negative) in [
            (DataType::Int8, -101, 101),
            (DataType::Int16, -26213, 26213),
            (DataType::Int32, -1717986917, 1717986917),
            (DataType::Int64, -7378697629483820645, 7378697629483820645),
        ] {
            let udf = SparkDecimalCast::to_integer(target.clone(), DecimalCastMode::Legacy)?;
            let expected = [None, Some(positive), Some(negative), Some(0), None];
            for (offset, length) in [(0, 5), (1, 3), (1, 0)] {
                let ColumnarValue::Array(actual) =
                    udf.convert(&ColumnarValue::Array(input.slice(offset, length)))?
                else {
                    return exec_err!("Decimal array CAST returned a scalar");
                };
                assert_eq!(actual.data_type(), &target);
                assert_eq!(actual.len(), length);
                for (row, value) in expected[offset..offset + length].iter().enumerate() {
                    assert_eq!(
                        ScalarValue::try_from_array(&actual, row)?,
                        ScalarValue::Int64(*value).cast_to(&target)?
                    );
                }
            }
            for mode in [DecimalCastMode::Ansi, DecimalCastMode::Try] {
                let udf = SparkDecimalCast::to_integer(target.clone(), mode)?;
                let result = udf.convert(&ColumnarValue::Scalar(ScalarValue::Decimal256(
                    Some(wide),
                    76,
                    1,
                )));
                if mode == DecimalCastMode::Ansi {
                    let Err(error) = result else {
                        return exec_err!("ANSI Decimal overflow succeeded");
                    };
                    assert!(error.to_string().contains("[CAST_OVERFLOW]"));
                } else {
                    assert_scalar_result(result?, ScalarValue::Int64(None).cast_to(&target)?)?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn integer_cast_keeps_scalar_negative_scales_and_extreme_null_inputs() -> Result<()> {
        for target in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            for decimal256 in [false, true] {
                let decimal = |value: Option<i128>, scale| {
                    if decimal256 {
                        ScalarValue::Decimal256(value.map(i256::from_i128), 76, scale)
                    } else {
                        ScalarValue::Decimal128(value, 38, scale)
                    }
                };
                let legacy = SparkDecimalCast::to_integer(target.clone(), DecimalCastMode::Legacy)?;
                for (value, scale, expected) in [
                    (1299, 1, 129),
                    (-1299, 1, -129),
                    (13, -2, 1300),
                    (-13, -2, -1300),
                ] {
                    let expected = if target == DataType::Int8 {
                        i64::from(expected as i8)
                    } else {
                        expected
                    };
                    assert_scalar_result(
                        legacy.convert(&ColumnarValue::Scalar(decimal(Some(value), scale)))?,
                        ScalarValue::Int64(Some(expected)).cast_to(&target)?,
                    )?;
                }
                for mode in [
                    DecimalCastMode::Ansi,
                    DecimalCastMode::Try,
                    DecimalCastMode::Legacy,
                ] {
                    let udf = SparkDecimalCast::to_integer(target.clone(), mode)?;
                    for value in [None, Some(0), Some(1)] {
                        let result = udf.convert(&ColumnarValue::Scalar(decimal(value, -128)));
                        if mode == DecimalCastMode::Ansi && value == Some(1) {
                            let Err(error) = result else {
                                return exec_err!("ANSI extreme-scale overflow succeeded");
                            };
                            assert!(error.to_string().contains("[CAST_OVERFLOW]"));
                        } else {
                            let expected = if value.is_none()
                                || (mode == DecimalCastMode::Try && value == Some(1))
                            {
                                None
                            } else {
                                Some(0)
                            };
                            assert_scalar_result(
                                result?,
                                ScalarValue::Int64(expected).cast_to(&target)?,
                            )?;
                        }
                    }
                    let input = decimal(None, -128).to_array_of_size(0)?;
                    let ColumnarValue::Array(actual) = udf.convert(&ColumnarValue::Array(input))?
                    else {
                        return exec_err!("empty Decimal CAST returned a scalar");
                    };
                    assert!(actual.is_empty());
                }
            }
        }
        let overflow =
            ColumnarValue::Scalar(ScalarValue::Decimal128(Some(10_i128.pow(37)), 38, -2));
        let ansi = SparkDecimalCast::to_integer(DataType::Int64, DecimalCastMode::Ansi)?;
        let Err(error) = ansi.convert(&overflow) else {
            return exec_err!("negative-scale overflow succeeded");
        };
        assert!(error.to_string().contains("[CAST_OVERFLOW]"));
        Ok(())
    }

    #[test]
    fn integer_cast_validates_arguments_and_clears_source_metadata() -> Result<()> {
        assert!(SparkDecimalCast::to_integer(DataType::Float64, DecimalCastMode::Ansi).is_err());
        for mode in [
            DecimalCastMode::Ansi,
            DecimalCastMode::Legacy,
            DecimalCastMode::Try,
        ] {
            let udf = SparkDecimalCast::to_integer(DataType::Int32, mode)?;
            for arguments in [
                vec![],
                vec![DataType::Boolean],
                vec![DataType::Int32],
                vec![DataType::Decimal128(10, 2); 2],
            ] {
                assert!(udf.coerce_types(&arguments).is_err());
                assert!(udf.return_type(&arguments).is_err());
            }
            for nullable in [false, true] {
                let input = Arc::new(
                    Field::new("input", DataType::Decimal128(10, 2), nullable)
                        .with_metadata([("source".into(), "discard".into())].into()),
                );
                let field = udf.return_field_from_args(ReturnFieldArgs {
                    arg_fields: &[input],
                    scalar_arguments: &[None],
                })?;
                assert_eq!(field.data_type(), &DataType::Int32);
                assert!(field.is_nullable());
                assert!(field.metadata().is_empty());
            }
        }
        Ok(())
    }

    #[test]
    fn nonfinite_float_cast_preserves_finite_overflow_and_nulls() -> Result<()> {
        let udf = SparkDecimalCast::to_decimal(10, 2, DecimalCastMode::Ansi);
        let values = [
            Some(1.25),
            Some(f64::NAN),
            None,
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(-0.0),
            Some(-2.5),
        ];
        let expected = [Some(125), None, None, None, None, Some(0), Some(-250)];
        for data_type in [DataType::Float32, DataType::Float64] {
            for (value, expected) in values.into_iter().zip(expected) {
                let input =
                    ColumnarValue::Scalar(ScalarValue::Float64(value)).cast_to(&data_type, None)?;
                let ColumnarValue::Scalar(actual) = udf.convert(&input)? else {
                    panic!("expected scalar output");
                };
                assert_eq!(actual, ScalarValue::Decimal128(expected, 10, 2));
            }
            let input = ColumnarValue::Array(Arc::new(Float64Array::from_iter(values)))
                .cast_to(&data_type, None)?;
            let ColumnarValue::Array(array) = input else {
                unreachable!()
            };
            for (offset, length) in [(0, values.len()), (1, 5), (0, 0)] {
                let result = udf.convert(&ColumnarValue::Array(array.slice(offset, length)))?;
                let ColumnarValue::Array(result) = result else {
                    unreachable!()
                };
                assert_eq!(result.data_type(), &DataType::Decimal128(10, 2));
                let actual = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
                assert_eq!(
                    actual.iter().collect::<Vec<_>>(),
                    expected[offset..offset + length]
                );
            }
            for values in [vec![Some(1e20)], vec![Some(f64::NAN), Some(1e20)]] {
                let input = ColumnarValue::Array(Arc::new(Float64Array::from(values)))
                    .cast_to(&data_type, None)?;
                assert!(
                    udf.convert(&input).is_err(),
                    "finite overflow must still fail"
                );
            }
        }
        for data_type in [DataType::Null, DataType::Float32, DataType::Float64] {
            let result = udf.convert(&ColumnarValue::Array(new_null_array(&data_type, 3)))?;
            let ColumnarValue::Array(result) = result else {
                unreachable!()
            };
            assert_eq!(result.data_type(), &DataType::Decimal128(10, 2));
            assert_eq!((result.len(), result.null_count()), (3, 3));
        }
        Ok(())
    }
}
