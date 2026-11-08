// Local Spark DATE subtraction. See the host crate's UPSTREAM.md.
//! Spark's subtraction of two DATE values, as `SubtractDates` gives it outside the legacy
//! interval mode: the DAY interval between the dates, in microseconds, with Java's exact
//! arithmetic. The day numbers are subtracted as INT values, which fails with `integer
//! overflow`, and the days multiplied to BIGINT microseconds, which fails with `long overflow`.
//! TRY_SUBTRACT gives NULL where either fails.

use std::sync::Arc;

use datafusion::arrow::array::types::Date32Type;
use datafusion::arrow::array::{AsArray, DurationMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

const MICROS_PER_DAY: i64 = 86_400_000_000;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSubtractDates {
    /// TRY_SUBTRACT: NULL where the subtraction overflows.
    null_on_error: bool,
    signature: Signature,
}

impl Default for SparkSubtractDates {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSubtractDates {
    /// The `-` operator.
    pub fn new() -> Self {
        Self::with_null_on_error(false)
    }

    /// TRY_SUBTRACT.
    pub fn try_subtract() -> Self {
        Self::with_null_on_error(true)
    }

    fn with_null_on_error(null_on_error: bool) -> Self {
        Self {
            null_on_error,
            signature: Signature::exact(
                vec![DataType::Date32, DataType::Date32],
                Volatility::Immutable,
            ),
        }
    }

    /// The difference between two day numbers, counted from the epoch, in microseconds, or
    /// Java's exact arithmetic message for the step that overflows.
    fn difference(end: i32, start: i32) -> std::result::Result<i64, &'static str> {
        let days = end.checked_sub(start).ok_or("integer overflow")?;
        i64::from(days)
            .checked_mul(MICROS_PER_DAY)
            .ok_or("long overflow")
    }
}

impl ScalarUDFImpl for SparkSubtractDates {
    fn name(&self) -> &str {
        if self.null_on_error {
            "spark_try_subtract_dates"
        } else {
            "spark_subtract_dates"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Duration(TimeUnit::Microsecond))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Duration(TimeUnit::Microsecond),
            self.null_on_error || args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    // Spark folds a constant subtraction during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        !self.null_on_error
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [end, start] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let scalar = matches!(
            (end, start),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        );
        let ends = end.to_array(args.number_rows)?;
        let starts = start.to_array(args.number_rows)?;
        let result = ends
            .as_primitive::<Date32Type>()
            .iter()
            .zip(starts.as_primitive::<Date32Type>().iter())
            .map(|pair| match pair {
                (Some(end), Some(start)) => match Self::difference(end, start) {
                    Ok(difference) => Ok(Some(difference)),
                    Err(_) if self.null_on_error => Ok(None),
                    Err(message) => exec_err!("{message}"),
                },
                _ => Ok(None),
            })
            .collect::<Result<DurationMicrosecondArray>>()?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_difference_is_exact_up_to_int_days_and_bigint_microseconds() {
        assert_eq!(SparkSubtractDates::difference(1, 0), Ok(MICROS_PER_DAY));
        assert_eq!(SparkSubtractDates::difference(0, 1), Ok(-MICROS_PER_DAY));
        assert_eq!(
            SparkSubtractDates::difference(106_751_991, 0),
            Ok(106_751_991 * MICROS_PER_DAY)
        );
        assert_eq!(
            SparkSubtractDates::difference(106_751_992, 0),
            Err("long overflow")
        );
        assert_eq!(
            SparkSubtractDates::difference(-106_751_992, 0),
            Err("long overflow")
        );
        assert_eq!(
            SparkSubtractDates::difference(i32::MAX, i32::MAX - 1),
            Ok(MICROS_PER_DAY)
        );
        assert_eq!(
            SparkSubtractDates::difference(i32::MAX, i32::MIN),
            Err("integer overflow")
        );
    }
}
