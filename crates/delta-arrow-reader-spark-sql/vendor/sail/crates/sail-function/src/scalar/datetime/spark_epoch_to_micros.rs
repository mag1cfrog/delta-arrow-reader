//! Spark numeric epoch conversion, before applying the session timestamp time zone.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float64Type, Int64Type,
};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkEpochToMicros {
    micros_per_unit: i64,
    signature: Signature,
}

impl SparkEpochToMicros {
    pub fn new(micros_per_unit: i64) -> Self {
        Self {
            micros_per_unit,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    fn decimal_micros(&self, unscaled: i128, scale: i8) -> Result<i64> {
        if unscaled == 0 {
            return Ok(0);
        }
        let overflow = || exec_datafusion_err!("timestamp epoch conversion: long overflow");
        let shift = self.micros_per_unit.ilog10() as i16 - i16::from(scale);
        let micros = if shift >= 0 {
            let factor = 10_i128.checked_pow(shift as u32).ok_or_else(overflow)?;
            unscaled.checked_mul(factor).ok_or_else(overflow)?
        } else {
            let divisor = 10_i128.checked_pow((-shift) as u32).ok_or_else(overflow)?;
            if unscaled % divisor != 0 {
                return exec_err!("timestamp epoch conversion: fractional microseconds");
            }
            unscaled / divisor
        };
        i64::try_from(micros).map_err(|_| overflow())
    }

    fn convert(&self, numbers: &ArrayRef) -> Result<ArrayRef> {
        let values: Int64Array = match numbers.data_type() {
            DataType::Int64 => numbers
                .as_primitive::<Int64Type>()
                .iter()
                .map(|value| {
                    value
                        .map(|value| {
                            value.checked_mul(self.micros_per_unit).ok_or_else(|| {
                                exec_datafusion_err!("timestamp epoch conversion: long overflow")
                            })
                        })
                        .transpose()
                })
                .collect::<Result<_>>()?,
            DataType::Float64 => numbers
                .as_primitive::<Float64Type>()
                .iter()
                .map(|value| {
                    // JVM float-to-long truncates toward zero and saturates outside BIGINT.
                    value
                        .filter(|value| value.is_finite())
                        .map(|value| (value * self.micros_per_unit as f64) as i64)
                })
                .collect(),
            DataType::Decimal128(_, scale) => numbers
                .as_primitive::<Decimal128Type>()
                .iter()
                .map(|value| {
                    value
                        .map(|value| self.decimal_micros(value, *scale))
                        .transpose()
                })
                .collect::<Result<_>>()?,
            other => return internal_err!("{} cannot convert {other}", self.name()),
        };
        Ok(Arc::new(values))
    }
}

impl ScalarUDFImpl for SparkEpochToMicros {
    fn name(&self) -> &str {
        "spark_epoch_to_micros"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [number] = args.arg_fields else {
            return internal_err!("{} requires one argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Int64,
            number.is_nullable() || number.data_type() == &DataType::Float64,
        )))
    }

    fn propagates_constant_folding_errors(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [number] = args.args.as_slice() else {
            return internal_err!("{} requires one argument", self.name());
        };
        match number {
            ColumnarValue::Scalar(number) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(&self.convert(&number.to_array()?)?, 0)?,
            )),
            ColumnarValue::Array(numbers) => Ok(ColumnarValue::Array(self.convert(numbers)?)),
        }
    }
}
