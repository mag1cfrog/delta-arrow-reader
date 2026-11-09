// Local Spark MONTHS_BETWEEN rounding. See the host crate's UPSTREAM.md.
//! The last step of Spark's MONTHS_BETWEEN: a month count rounded to 8 digits when `roundOff` is
//! true, and NULL when either is NULL. As a function, it reads its month count first, and with it
//! the arguments, then `roundOff` once per row, and its nullability follows its arguments' at
//! every stage.

use std::sync::Arc;

use datafusion::arrow::array::{AsArray, Float64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float64Type};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMonthsBetweenRoundOff {
    signature: Signature,
}

impl Default for SparkMonthsBetweenRoundOff {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMonthsBetweenRoundOff {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Boolean],
                Volatility::Immutable,
            ),
        }
    }
}

/// `months` rounded to 8 digits as DataFusion's `round(months, 8)` does, or unchanged.
fn round_off(months: Option<f64>, round_off: Option<bool>) -> Option<f64> {
    let factor = 10_f64.powi(8);
    match (months, round_off) {
        (Some(months), Some(true)) => Some((months * factor).round() / factor),
        (Some(months), Some(false)) => Some(months),
        _ => None,
    }
}

impl ScalarUDFImpl for SparkMonthsBetweenRoundOff {
    fn name(&self) -> &str {
        "spark_months_between_round_off"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Float64,
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [months, round] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        if let (ColumnarValue::Scalar(months), ColumnarValue::Scalar(round)) = (months, round) {
            let (ScalarValue::Float64(months), ScalarValue::Boolean(round)) = (months, round)
            else {
                return internal_err!("{} requires a DOUBLE and a BOOLEAN", self.name());
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(round_off(
                *months, *round,
            ))));
        }
        let months = months.to_array(args.number_rows)?;
        let round = round.to_array(args.number_rows)?;
        let result = months
            .as_primitive::<Float64Type>()
            .iter()
            .zip(round.as_boolean().iter())
            .map(|(months, round)| round_off(months, round))
            .collect::<Float64Array>();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounds_to_eight_digits_and_propagates_null() {
        assert_eq!(round_off(Some(1.0 / 3.0), Some(true)), Some(0.33333333));
        assert_eq!(round_off(Some(1.0 / 3.0), Some(false)), Some(1.0 / 3.0));
        assert_eq!(round_off(Some(1.0 / 3.0), None), None);
        assert_eq!(round_off(None, Some(true)), None);
    }
}
