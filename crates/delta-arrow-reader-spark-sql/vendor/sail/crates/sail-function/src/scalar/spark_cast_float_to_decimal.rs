// Local Spark adapter around DataFusion/Arrow's strict numeric cast.
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::{DataType, Field, FieldRef, Float32Type, Float64Type};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastFloatToDecimal {
    target: DataType,
    signature: Signature,
}

impl SparkCastFloatToDecimal {
    pub fn new(precision: u8, scale: i8) -> Self {
        Self {
            target: DataType::Decimal128(precision, scale),
            signature: Signature::uniform(
                1,
                vec![DataType::Float32, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }

    fn convert(&self, arg: &ColumnarValue) -> Result<ColumnarValue> {
        match arg.cast_to(&self.target, None) {
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

impl ScalarUDFImpl for SparkCastFloatToDecimal {
    fn name(&self) -> &str {
        "spark_cast_float_to_decimal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn return_field_from_args(&self, _: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), self.target.clone(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = args.args.as_slice() else {
            return exec_err!("float-to-Decimal CAST expects one argument");
        };
        self.convert(arg)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Decimal128Array, Float64Array, new_null_array};

    use super::*;

    #[test]
    fn nonfinite_float_cast_preserves_finite_overflow_and_nulls() -> Result<()> {
        let udf = SparkCastFloatToDecimal::new(10, 2);
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
