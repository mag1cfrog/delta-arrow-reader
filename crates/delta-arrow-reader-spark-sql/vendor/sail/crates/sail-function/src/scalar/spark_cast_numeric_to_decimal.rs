//! Numeric Decimal casts retain Spark schemas and error modes around Arrow conversion.
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field, FieldRef, Float32Type, Float64Type};
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DecimalCastMode {
    Ansi,
    Legacy,
    Try,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastNumericToDecimal {
    target: DataType,
    signature: Signature,
    mode: DecimalCastMode,
}

impl SparkCastNumericToDecimal {
    pub fn new(precision: u8, scale: i8, mode: DecimalCastMode) -> Self {
        Self {
            target: DataType::Decimal128(precision, scale),
            signature: Signature::numeric(1, Volatility::Immutable),
            mode,
        }
    }

    pub fn mode(&self) -> DecimalCastMode {
        self.mode
    }

    fn convert(&self, arg: &ColumnarValue) -> Result<ColumnarValue> {
        let options = CastOptions {
            safe: self.mode != DecimalCastMode::Ansi,
            ..DEFAULT_CAST_OPTIONS
        };
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

impl ScalarUDFImpl for SparkCastNumericToDecimal {
    fn name(&self) -> &str {
        "spark_cast_numeric_to_decimal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
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
            return exec_err!("numeric-to-Decimal CAST expects one argument");
        };
        let DataType::Decimal128(precision, scale) = self.target else {
            unreachable!("the constructor creates a Decimal128 target")
        };
        let from = input.data_type();
        // TRY_CAST also declares nullable for Decimal scale reduction without overflow.
        let loses_scale = self.mode == DecimalCastMode::Try
            && matches!(from, DataType::Decimal128(_, source_scale) | DataType::Decimal256(_, source_scale)
                if scale < *source_scale);
        let nullable =
            input.is_nullable() || decimal_cast_can_overflow(from, precision, scale) || loses_scale;
        Ok(Arc::new(Field::new(
            self.name(),
            self.target.clone(),
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!("numeric-to-Decimal CAST expects one argument");
        };
        self.convert(arg)
    }
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

    #[test]
    fn nonfinite_float_cast_preserves_finite_overflow_and_nulls() -> Result<()> {
        let udf = SparkCastNumericToDecimal::new(10, 2, DecimalCastMode::Ansi);
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
