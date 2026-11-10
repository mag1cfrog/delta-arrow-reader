// Local Spark interval-to-number cast. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
//! Spark's CAST and TRY_CAST of a year-month or day-time interval to TINYINT, SMALLINT, INT,
//! BIGINT or DECIMAL (#1152). The number counts the interval's end field: whole years or months
//! of a year-month interval, and whole days, hours, minutes or seconds of a day-time interval,
//! truncated toward zero, except that a DECIMAL keeps the microseconds of a SECOND end field. A
//! count beyond an integral target fails with `CAST_OVERFLOW` in both ANSI modes, naming the
//! interval as Spark prints it, and gives NULL for TRY_CAST. A DECIMAL rounds HALF_UP to its
//! scale, and a count beyond its precision fails in both ANSI modes, or gives NULL for TRY_CAST.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Int8Array, Int16Array, Int32Array, Int64Array,
};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, FieldRef, IntervalUnit, IntervalYearMonthType,
    TimeUnit,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use sail_common::spec::IntervalFieldType;

use crate::scalar::datetime::interval_text::{interval_fields_text, interval_literal_text};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntervalToNumber {
    /// The source interval's qualifier, which Spark prints in an overflow error.
    start: IntervalFieldType,
    /// The end field, which the number counts.
    end: IntervalFieldType,
    /// TINYINT, SMALLINT, INT, BIGINT or DECIMAL.
    target: DataType,
    /// TRY_CAST gives NULL where CAST fails.
    is_try: bool,
    signature: Signature,
}

impl SparkIntervalToNumber {
    pub fn try_new(
        start: IntervalFieldType,
        end: IntervalFieldType,
        target: DataType,
        is_try: bool,
    ) -> Result<Self> {
        let year_month =
            |field| matches!(field, IntervalFieldType::Year | IntervalFieldType::Month);
        if start > end || year_month(start) != year_month(end) {
            return internal_err!(
                "spark_interval_to_number got the qualifier {start:?} to {end:?}"
            );
        }
        if !matches!(
            target,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Decimal128(..)
        ) {
            return internal_err!("spark_interval_to_number cannot cast to {target}");
        }
        Ok(Self {
            start,
            end,
            target,
            is_try,
            signature: Signature::user_defined(Volatility::Immutable),
        })
    }

    fn year_month(&self) -> bool {
        matches!(self.end, IntervalFieldType::Year | IntervalFieldType::Month)
    }

    /// The end field's length in stored months or microseconds.
    fn unit_length(&self) -> i64 {
        match self.end {
            IntervalFieldType::Year => 12,
            IntervalFieldType::Month => 1,
            IntervalFieldType::Day => 86_400_000_000,
            IntervalFieldType::Hour => 3_600_000_000,
            IntervalFieldType::Minute => 60_000_000,
            IntervalFieldType::Second => 1_000_000,
        }
    }

    /// The count of end fields in `stored` months or microseconds, as an unscaled number with
    /// its scale: a DECIMAL target keeps a SECOND end field's microseconds.
    fn count(&self, stored: i64) -> (i128, u32) {
        if matches!(self.target, DataType::Decimal128(..)) && self.end == IntervalFieldType::Second
        {
            (i128::from(stored), 6)
        } else {
            (i128::from(stored / self.unit_length()), 0)
        }
    }

    /// The interval as Spark prints it in an error, such as `INTERVAL '128' DAY`.
    fn interval_text(&self, stored: i64) -> Result<String> {
        let value = if self.year_month() {
            ScalarValue::IntervalYearMonth(i32::try_from(stored).ok())
        } else {
            ScalarValue::DurationMicrosecond(Some(stored))
        };
        interval_literal_text(&value, self.start, self.end)
    }

    fn overflow_error(&self, stored: i64) -> Result<DataFusionError> {
        let target = match self.target {
            DataType::Int8 => "TINYINT",
            DataType::Int16 => "SMALLINT",
            DataType::Int32 => "INT",
            _ => "BIGINT",
        };
        Ok(DataFusionError::Execution(format!(
            "[CAST_OVERFLOW] The value {} of the type \"INTERVAL {}\" cannot be cast to \
             \"{target}\" due to an overflow. Use `try_cast` to tolerate overflow and return \
             NULL instead. SQLSTATE: 22003",
            self.interval_text(stored)?,
            interval_fields_text(self.start, self.end),
        )))
    }

    /// The integral count of `stored`, or None beyond the target.
    fn integral(&self, stored: i64) -> Option<i64> {
        let (count, _) = self.count(stored);
        let count = i64::try_from(count).ok()?;
        let fits = match self.target {
            DataType::Int8 => i8::try_from(count).is_ok(),
            DataType::Int16 => i16::try_from(count).is_ok(),
            DataType::Int32 => i32::try_from(count).is_ok(),
            _ => true,
        };
        fits.then_some(count)
    }

    /// The DECIMAL(`precision`, `scale`) unscaled count of `stored`, rounded HALF_UP, or None
    /// beyond the precision.
    fn decimal(&self, stored: i64, precision: u8, scale: i8) -> Option<i128> {
        let (count, count_scale) = self.count(stored);
        let scale = u32::try_from(scale).ok()?;
        let unscaled = if scale >= count_scale {
            count.checked_mul(10_i128.checked_pow(scale - count_scale)?)?
        } else {
            let divisor = 10_i128.pow(count_scale - scale);
            let quotient = count / divisor;
            let remainder = (count % divisor).abs();
            if remainder * 2 >= divisor {
                quotient + count.signum()
            } else {
                quotient
            }
        };
        (unscaled.abs() < 10_i128.checked_pow(u32::from(precision))?).then_some(unscaled)
    }

    fn decimal_out_of_range_error(&self, stored: i64, precision: u8, scale: i8) -> DataFusionError {
        let (count, count_scale) = self.count(stored);
        let digits = count.unsigned_abs().to_string();
        let text = if count_scale == 0 {
            digits
        } else {
            let digits = format!("{digits:0>width$}", width = count_scale as usize + 1);
            let (integer, fraction) = digits.split_at(digits.len() - count_scale as usize);
            format!("{integer}.{fraction}")
        };
        let sign = if count < 0 { "-" } else { "" };
        DataFusionError::Execution(format!(
            "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  {sign}{text} cannot be represented \
             as Decimal({precision}, {scale}). If necessary set \"spark.sql.ansi.enabled\" to \
             \"false\" to bypass this error, and return NULL instead. SQLSTATE: 22003"
        ))
    }

    fn convert(&self, intervals: &ArrayRef) -> Result<ArrayRef> {
        let stored: Vec<Option<i64>> = match intervals.data_type() {
            DataType::Interval(IntervalUnit::YearMonth) if self.year_month() => intervals
                .as_primitive::<IntervalYearMonthType>()
                .iter()
                .map(|months| months.map(i64::from))
                .collect(),
            DataType::Duration(TimeUnit::Microsecond) if !self.year_month() => intervals
                .as_primitive::<DurationMicrosecondType>()
                .iter()
                .collect(),
            other => return internal_err!("{} cannot cast {other}", self.name()),
        };
        if let DataType::Decimal128(precision, scale) = self.target {
            let mut values = Vec::with_capacity(stored.len());
            for value in stored {
                values.push(match value {
                    None => None,
                    Some(value) => match self.decimal(value, precision, scale) {
                        Some(unscaled) => Some(unscaled),
                        None if self.is_try => None,
                        None => {
                            return Err(self.decimal_out_of_range_error(value, precision, scale));
                        }
                    },
                });
            }
            return Ok(Arc::new(
                Decimal128Array::from(values).with_precision_and_scale(precision, scale)?,
            ));
        }
        let mut counts = Vec::with_capacity(stored.len());
        for value in stored {
            counts.push(match value {
                None => None,
                Some(value) => match self.integral(value) {
                    Some(count) => Some(count),
                    None if self.is_try => None,
                    None => return Err(self.overflow_error(value)?),
                },
            });
        }
        // Each count fits the target, which `integral` checked.
        Ok(match self.target {
            DataType::Int8 => Arc::new(Int8Array::from_iter(
                counts
                    .into_iter()
                    .map(|count| count.map(|count| count as i8)),
            )),
            DataType::Int16 => Arc::new(Int16Array::from_iter(
                counts
                    .into_iter()
                    .map(|count| count.map(|count| count as i16)),
            )),
            DataType::Int32 => Arc::new(Int32Array::from_iter(
                counts
                    .into_iter()
                    .map(|count| count.map(|count| count as i32)),
            )),
            _ => Arc::new(Int64Array::from_iter(counts)),
        })
    }
}

impl ScalarUDFImpl for SparkIntervalToNumber {
    fn name(&self) -> &str {
        "spark_interval_to_number"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        match types {
            [
                interval @ (DataType::Interval(IntervalUnit::YearMonth)
                | DataType::Duration(TimeUnit::Microsecond)),
            ] => Ok(vec![interval.clone()]),
            _ => internal_err!("{} requires one ANSI interval, got {types:?}", self.name()),
        }
    }

    fn return_type(&self, _types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    // Spark declares an interval cast to DECIMAL nullable whatever its input, and a cast to an
    // integral type nullable only for a nullable interval or TRY_CAST.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [interval] = args.arg_fields else {
            return internal_err!("{} requires one argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            self.target.clone(),
            interval.is_nullable()
                || self.is_try
                || matches!(self.target, DataType::Decimal128(..)),
        )))
    }

    // Spark folds a constant cast during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        !self.is_try
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [interval] = args.args.as_slice() else {
            return internal_err!("{} requires one argument", self.name());
        };
        match interval {
            ColumnarValue::Scalar(interval) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(&self.convert(&interval.to_array()?)?, 0)?,
            )),
            ColumnarValue::Array(intervals) => Ok(ColumnarValue::Array(self.convert(intervals)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{DurationMicrosecondArray, IntervalYearMonthArray};
    use datafusion::arrow::datatypes::{Decimal128Type, Int8Type, Int32Type};
    use sail_common::spec::IntervalFieldType::{Day, Hour, Month, Second, Year};

    use super::*;

    const DAY: i64 = 86_400_000_000;

    #[test]
    fn intervals_count_their_end_field_toward_zero() -> Result<()> {
        let days: ArrayRef = Arc::new(DurationMicrosecondArray::from(vec![
            Some(3 * DAY + 5),
            Some(-3 * DAY - 5),
            None,
        ]));
        let cast = SparkIntervalToNumber::try_new(Day, Hour, DataType::Int32, false)?;
        assert_eq!(
            cast.convert(&days)?.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(72), Some(-72), None])
        );
        let months: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![Some(-14)]));
        let cast = SparkIntervalToNumber::try_new(Year, Year, DataType::Int32, false)?;
        assert_eq!(
            cast.convert(&months)?.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(-1)])
        );
        let cast = SparkIntervalToNumber::try_new(Year, Month, DataType::Int32, false)?;
        assert_eq!(
            cast.convert(&months)?.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(-14)])
        );
        Ok(())
    }

    #[test]
    fn overflow_fails_with_spark_text_or_gives_null_for_try_cast() -> Result<()> {
        let days: ArrayRef = Arc::new(DurationMicrosecondArray::from(vec![Some(128 * DAY)]));
        let cast = SparkIntervalToNumber::try_new(Day, Day, DataType::Int8, false)?;
        let error = cast.convert(&days).unwrap_err().to_string();
        assert!(
            error.contains(
                "[CAST_OVERFLOW] The value INTERVAL '128' DAY of the type \"INTERVAL DAY\" \
                 cannot be cast to \"TINYINT\" due to an overflow."
            ),
            "{error}"
        );
        let cast = SparkIntervalToNumber::try_new(Day, Day, DataType::Int8, true)?;
        assert_eq!(
            cast.convert(&days)?.as_primitive::<Int8Type>(),
            &Int8Array::from(vec![None])
        );
        Ok(())
    }

    #[test]
    fn decimals_keep_seconds_and_round_half_up_within_their_precision() -> Result<()> {
        let seconds: ArrayRef = Arc::new(DurationMicrosecondArray::from(vec![
            Some(1_250_000),
            Some(-1_250_000),
            Some(1_249_000),
            Some(999_950_000),
        ]));
        let decimal = DataType::Decimal128(4, 1);
        let cast = SparkIntervalToNumber::try_new(Second, Second, decimal.clone(), true)?;
        assert_eq!(
            cast.convert(&seconds)?.as_primitive::<Decimal128Type>(),
            &Decimal128Array::from(vec![Some(13), Some(-13), Some(12), None])
                .with_precision_and_scale(4, 1)?
        );
        let cast = SparkIntervalToNumber::try_new(Second, Second, decimal, false)?;
        let error = cast.convert(&seconds).unwrap_err().to_string();
        assert!(
            error.contains(
                "[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]  999.950000 cannot be represented \
                 as Decimal(4, 1)."
            ),
            "{error}"
        );
        Ok(())
    }
}
