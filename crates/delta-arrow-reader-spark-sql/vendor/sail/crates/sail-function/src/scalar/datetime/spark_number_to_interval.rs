// Local Spark numeric-to-interval cast. See the host crate's UPSTREAM.md.
//! Spark's CAST and TRY_CAST of a TINYINT, SMALLINT, INT, BIGINT or DECIMAL number to a
//! year-month or day-time interval qualifier. The number counts the qualifier's end field, so it
//! is multiplied by that field's length in stored months or microseconds. A DECIMAL product is
//! rounded HALF_UP to a whole stored unit. A result beyond the stored INT months or BIGINT
//! microseconds fails with `CAST_OVERFLOW` in both ANSI modes, or gives NULL for TRY_CAST.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, DurationMicrosecondArray, IntervalYearMonthArray,
};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type,
    IntervalUnit, TimeUnit,
};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNumberToInterval {
    /// `Interval(YearMonth)` or `Duration(Microsecond)`.
    target: DataType,
    /// The end field's length in stored months or microseconds.
    unit_length: i64,
    /// Spark's name of the target type in its error, such as `INTERVAL YEAR`.
    target_name: String,
    /// TRY_CAST gives NULL where CAST fails.
    is_try: bool,
    signature: Signature,
}

impl SparkNumberToInterval {
    pub fn try_new(
        target: DataType,
        unit_length: i64,
        target_name: impl Into<String>,
        is_try: bool,
    ) -> Result<Self> {
        let valid_unit = match target {
            DataType::Interval(IntervalUnit::YearMonth) => matches!(unit_length, 1 | 12),
            DataType::Duration(TimeUnit::Microsecond) => unit_length > 0,
            _ => false,
        };
        if !valid_unit {
            return internal_err!(
                "spark_number_to_interval cannot cast to {target} in units of {unit_length}"
            );
        }
        Ok(Self {
            target,
            unit_length,
            target_name: target_name.into(),
            is_try,
            signature: Signature::user_defined(Volatility::Immutable),
        })
    }

    /// The stored count for `value` units, or None beyond the target's storage.
    fn scale_integer(&self, value: i64) -> Option<i64> {
        let count = value.checked_mul(self.unit_length)?;
        self.in_storage(count.into())
    }

    /// The stored count for a DECIMAL `unscaled` / 10^`scale` units, rounded HALF_UP, or None
    /// beyond the target's storage.
    fn scale_decimal(&self, unscaled: i128, scale: i8) -> Option<i64> {
        let product = unscaled.checked_mul(self.unit_length.into())?;
        let divisor = 10_i128.checked_pow(u32::try_from(scale).ok()?)?;
        let quotient = product / divisor;
        let remainder = product % divisor;
        // HALF_UP moves a half away from zero.
        let rounded = if remainder.unsigned_abs() * 2 >= divisor.unsigned_abs() {
            quotient + product.signum()
        } else {
            quotient
        };
        self.in_storage(rounded)
    }

    fn in_storage(&self, count: i128) -> Option<i64> {
        match self.target {
            DataType::Interval(IntervalUnit::YearMonth) => i32::try_from(count).ok().map(i64::from),
            _ => i64::try_from(count).ok(),
        }
    }

    /// Spark's `CAST_OVERFLOW`, naming the value and its type as Spark prints them.
    fn overflow(&self, value: &str, source: &str) -> datafusion_common::DataFusionError {
        datafusion_common::DataFusionError::Execution(format!(
            "[CAST_OVERFLOW] The value {value} of the type \"{source}\" cannot be cast to \
             \"{}\" due to an overflow. Use `try_cast` to tolerate overflow and return NULL \
             instead. SQLSTATE: 22003",
            self.target_name
        ))
    }

    fn convert(&self, numbers: &ArrayRef) -> Result<ArrayRef> {
        let mut counts = Vec::with_capacity(numbers.len());
        macro_rules! integers {
            ($type:ty, $suffix:expr, $source:expr) => {
                for value in numbers.as_primitive::<$type>().iter() {
                    counts.push(match value {
                        None => None,
                        Some(value) => match self.scale_integer(value.into()) {
                            Some(count) => Some(count),
                            None if self.is_try => None,
                            None => {
                                return Err(self.overflow(&format!("{value}{}", $suffix), $source));
                            }
                        },
                    });
                }
            };
        }
        match numbers.data_type() {
            DataType::Null => counts.resize(numbers.len(), None),
            DataType::Int8 => integers!(Int8Type, "Y", "TINYINT"),
            DataType::Int16 => integers!(Int16Type, "S", "SMALLINT"),
            DataType::Int32 => integers!(Int32Type, "", "INT"),
            DataType::Int64 => integers!(Int64Type, "L", "BIGINT"),
            DataType::Decimal128(precision, scale) => {
                for value in numbers.as_primitive::<Decimal128Type>().iter() {
                    counts.push(match value {
                        None => None,
                        Some(value) => match self.scale_decimal(value, *scale) {
                            Some(count) => Some(count),
                            None if self.is_try => None,
                            None => {
                                return Err(self.overflow(
                                    &format!("{}BD", decimal_text(value, *scale)),
                                    &format!("DECIMAL({precision},{scale})"),
                                ));
                            }
                        },
                    });
                }
            }
            other => return internal_err!("{} cannot cast {other}", self.name()),
        }
        Ok(match self.target {
            DataType::Interval(IntervalUnit::YearMonth) => Arc::new(
                counts
                    .into_iter()
                    .map(|count| count.map(|count| count as i32))
                    .collect::<IntervalYearMonthArray>(),
            ),
            _ => Arc::new(counts.into_iter().collect::<DurationMicrosecondArray>()),
        })
    }
}

/// A DECIMAL's plain text, as Java's `BigDecimal` prints the values that can overflow.
fn decimal_text(unscaled: i128, scale: i8) -> String {
    let digits = unscaled.unsigned_abs().to_string();
    let sign = if unscaled < 0 { "-" } else { "" };
    let Ok(scale) = usize::try_from(scale) else {
        return format!("{sign}{digits}");
    };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let digits = format!("{digits:0>width$}", width = scale + 1);
    let (integer, fraction) = digits.split_at(digits.len() - scale);
    format!("{sign}{integer}.{fraction}")
}

impl ScalarUDFImpl for SparkNumberToInterval {
    fn name(&self) -> &str {
        "spark_number_to_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        match types {
            [
                number @ (DataType::Null
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Decimal128(..)),
            ] => Ok(vec![number.clone()]),
            _ => internal_err!(
                "{} requires one integral or DECIMAL number, got {types:?}",
                self.name()
            ),
        }
    }

    fn return_type(&self, _types: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    // Spark's cast fails rather than giving NULL, so only a nullable number or TRY_CAST gives a
    // nullable interval.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [number] = args.arg_fields else {
            return internal_err!("{} requires one argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            self.target.clone(),
            number.is_nullable() || self.is_try,
        )))
    }

    // Spark folds a constant cast during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        !self.is_try
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Decimal128Array, Int32Array, Int64Array};
    use datafusion::arrow::datatypes::{DurationMicrosecondType, IntervalYearMonthType};

    use super::*;

    fn year() -> Result<SparkNumberToInterval> {
        SparkNumberToInterval::try_new(
            DataType::Interval(IntervalUnit::YearMonth),
            12,
            "INTERVAL YEAR",
            false,
        )
    }

    #[test]
    fn integers_count_the_end_field_and_overflow_with_spark_text() -> Result<()> {
        let years: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), Some(-3), None]));
        assert_eq!(
            year()?
                .convert(&years)?
                .as_primitive::<IntervalYearMonthType>(),
            &IntervalYearMonthArray::from(vec![Some(36), Some(-36), None])
        );
        let overflow: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX]));
        let error = year()?.convert(&overflow).unwrap_err().to_string();
        assert!(error.contains(
            "[CAST_OVERFLOW] The value 9223372036854775807L of the type \"BIGINT\" cannot be \
             cast to \"INTERVAL YEAR\""
        ));
        let try_year = SparkNumberToInterval::try_new(
            DataType::Interval(IntervalUnit::YearMonth),
            12,
            "INTERVAL YEAR",
            true,
        )?;
        assert!(try_year.convert(&overflow)?.is_null(0));
        Ok(())
    }

    #[test]
    fn decimals_round_half_up_and_name_their_value() -> Result<()> {
        let seconds = SparkNumberToInterval::try_new(
            DataType::Duration(TimeUnit::Microsecond),
            1_000_000,
            "INTERVAL SECOND",
            false,
        )?;
        let values: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(5), Some(-5), Some(4), Some(15_000_000)])
                .with_precision_and_scale(20, 7)?,
        );
        assert_eq!(
            seconds
                .convert(&values)?
                .as_primitive::<DurationMicrosecondType>(),
            &DurationMicrosecondArray::from(vec![Some(1), Some(-1), Some(0), Some(1_500_000)])
        );
        assert_eq!(
            decimal_text(-99_999_999_999_999_999_999, 6),
            "-99999999999999.999999"
        );
        assert_eq!(decimal_text(5, 3), "0.005");
        assert_eq!(decimal_text(42, 0), "42");
        Ok(())
    }
}
