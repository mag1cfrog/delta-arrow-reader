// Local Spark MONTHS_BETWEEN rounding. See the host crate's UPSTREAM.md.
//! The last step of Spark's MONTHS_BETWEEN: a month count rounded to 8 digits when `roundOff` is
//! true, and NULL when either is NULL. As a function, it reads its month count, and with it the
//! arguments, before `roundOff` in each batch, and each row's `roundOff` once, and its
//! nullability follows its arguments' at every stage.

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

/// `months` rounded to 8 digits as DataFusion's `round(months, 8)` does when `round` is true,
/// unchanged when it is false, and NULL when either is NULL.
fn apply_round_off(months: Option<f64>, round: Option<bool>) -> Option<f64> {
    let factor = 10_f64.powi(8);
    match (months, round) {
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
            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                apply_round_off(*months, *round),
            )));
        }
        if let (ColumnarValue::Array(months), ColumnarValue::Scalar(ScalarValue::Boolean(round))) =
            (months, round)
            && let Some(months) = months.as_any().downcast_ref::<Float64Array>()
        {
            let result = months
                .iter()
                .take(args.number_rows)
                .map(|months| apply_round_off(months, *round))
                .collect::<Float64Array>();
            return Ok(ColumnarValue::Array(Arc::new(result)));
        }
        let months = months.to_array(args.number_rows)?;
        let round = round.to_array(args.number_rows)?;
        let result = months
            .as_primitive::<Float64Type>()
            .iter()
            .zip(round.as_boolean().iter())
            .map(|(months, round)| apply_round_off(months, round))
            .collect::<Float64Array>();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_rounding_matches_broadcast_rounding() -> Result<()> {
        let function = SparkMonthsBetweenRoundOff::new();
        let months = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            Some(1.0 / 3.0),
            None,
            Some(-1.0 / 3.0),
            Some(-0.0),
            Some(f64::INFINITY),
            Some(f64::NAN),
        ])));
        for number_rows in [0, 4, 6, 8] {
            let invoke = |round: ColumnarValue| {
                function.invoke_with_args(ScalarFunctionArgs {
                    args: vec![months.clone(), round],
                    arg_fields: vec![
                        Arc::new(Field::new("months", DataType::Float64, true)),
                        Arc::new(Field::new("round", DataType::Boolean, true)),
                    ],
                    return_field: Arc::new(Field::new("result", DataType::Float64, true)),
                    number_rows,
                    config_options: Arc::new(Default::default()),
                })
            };
            for round in [Some(true), Some(false), None] {
                let scalar = ScalarValue::Boolean(round);
                let expected = invoke(ColumnarValue::Array(scalar.to_array_of_size(number_rows)?))?;
                let actual = invoke(ColumnarValue::Scalar(scalar))?;
                let (ColumnarValue::Array(actual), ColumnarValue::Array(expected)) =
                    (actual, expected)
                else {
                    panic!("rounding a month column must return an array");
                };
                assert_eq!(actual.data_type(), expected.data_type());
                assert_eq!(actual.len(), expected.len());
                let actual = actual.as_primitive::<Float64Type>();
                let expected = expected.as_primitive::<Float64Type>();
                assert!(
                    actual
                        .iter()
                        .map(|x| x.map(f64::to_bits))
                        .eq(expected.iter().map(|x| x.map(f64::to_bits)))
                );
            }
        }
        Ok(())
    }

    #[test]
    fn rounds_to_eight_digits_and_propagates_null() {
        assert_eq!(
            apply_round_off(Some(1.0 / 3.0), Some(true)),
            Some(0.33333333)
        );
        assert_eq!(
            apply_round_off(Some(1.0 / 3.0), Some(false)),
            Some(1.0 / 3.0)
        );
        assert_eq!(apply_round_off(Some(1.0 / 3.0), None), None);
        assert_eq!(apply_round_off(None, Some(true)), None);
    }
}
