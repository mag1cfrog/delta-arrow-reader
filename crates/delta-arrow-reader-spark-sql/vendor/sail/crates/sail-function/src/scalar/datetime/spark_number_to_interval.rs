// Local Spark numeric-to-interval cast. See the host crate's UPSTREAM.md.
//! Spark's CAST and TRY_CAST of a TINYINT, SMALLINT, INT, BIGINT or DECIMAL number to a
//! year-month or day-time interval qualifier. The number counts the qualifier's end field, so it
//! is multiplied by that field's length in stored months or microseconds. Spark multiplies a
//! DECIMAL with Scala's DECIMAL128 context, which keeps 34 significant digits of the product,
//! rounded HALF_EVEN, and then rounds HALF_UP to a whole stored unit. A MONTH target is not
//! multiplied, so its DECIMAL rounds HALF_UP once. A result beyond the stored INT months or BIGINT
//! microseconds fails with `CAST_OVERFLOW` in both ANSI modes, or gives NULL for TRY_CAST.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, DurationMicrosecondArray, IntervalYearMonthArray,
};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type,
    IntervalUnit, TimeUnit, i256,
};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// The significant digits Scala's DECIMAL128 context keeps of a product.
const PRODUCT_DIGITS: u32 = 34;

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
    fn stored_count_of_integer(&self, value: i64) -> Option<i64> {
        let count = value.checked_mul(self.unit_length)?;
        self.fit_storage(count.into())
    }

    /// The stored count for a DECIMAL `unscaled` / 10^`fraction_digits` units, rounded as Spark
    /// does, or None beyond the target's storage.
    fn stored_count_of_decimal(&self, unscaled: i128, fraction_digits: i8) -> Option<i64> {
        let mut fraction_digits = u32::try_from(fraction_digits).ok()?;
        let mut value = i256::from_i128(unscaled);
        if self.unit_length != 1 {
            value = value.checked_mul(i256::from_i128(self.unit_length.into()))?;
            let digits = decimal_digits(value);
            if digits > PRODUCT_DIGITS {
                let dropped = digits - PRODUCT_DIGITS;
                value = divide_rounding(value, dropped, Rounding::HalfEven)?;
                if dropped > fraction_digits {
                    value = value.checked_mul(power_of_ten(dropped - fraction_digits)?)?;
                    fraction_digits = 0;
                } else {
                    fraction_digits -= dropped;
                }
            }
        }
        let count = divide_rounding(value, fraction_digits, Rounding::HalfUp)?;
        self.fit_storage(count.to_i128()?)
    }

    /// The count narrowed to the target's INT months or BIGINT microseconds.
    fn fit_storage(&self, count: i128) -> Option<i64> {
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
        let year_month = matches!(self.target, DataType::Interval(IntervalUnit::YearMonth));
        macro_rules! integers {
            ($type:ty) => {
                for value in numbers.as_primitive::<$type>().iter() {
                    counts.push(match value {
                        None => None,
                        Some(value) => {
                            let value = i64::from(value);
                            match self.stored_count_of_integer(value) {
                                Some(count) => Some(count),
                                None if self.is_try => None,
                                // Spark widens TINYINT and SMALLINT to INT, and narrows a BIGINT
                                // that fits INT for a year-month target, before it multiplies.
                                None => {
                                    return Err(match i32::try_from(value) {
                                        Ok(value)
                                            if year_month
                                                || numbers.data_type() != &DataType::Int64 =>
                                        {
                                            self.overflow(&value.to_string(), "INT")
                                        }
                                        _ => self.overflow(&format!("{value}L"), "BIGINT"),
                                    });
                                }
                            }
                        }
                    });
                }
            };
        }
        match numbers.data_type() {
            DataType::Int8 => integers!(Int8Type),
            DataType::Int16 => integers!(Int16Type),
            DataType::Int32 => integers!(Int32Type),
            DataType::Int64 => integers!(Int64Type),
            DataType::Decimal128(precision, scale) => {
                for value in numbers.as_primitive::<Decimal128Type>().iter() {
                    counts.push(match value {
                        None => None,
                        Some(value) => match self.stored_count_of_decimal(value, *scale) {
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

#[derive(Clone, Copy)]
enum Rounding {
    HalfUp,
    HalfEven,
}

fn power_of_ten(exponent: u32) -> Option<i256> {
    i256::from_i128(10).checked_pow(exponent)
}

/// The number of decimal digits of `value`'s magnitude.
fn decimal_digits(value: i256) -> u32 {
    value.wrapping_abs().to_string().len() as u32
}

/// `value` / 10^`digits`, rounded HALF_UP (a half away from zero) or HALF_EVEN.
fn divide_rounding(value: i256, digits: u32, rounding: Rounding) -> Option<i256> {
    if digits == 0 {
        return Some(value);
    }
    let divisor = power_of_ten(digits)?;
    let quotient = value.checked_div(divisor)?;
    let twice_remainder = value
        .checked_rem(divisor)?
        .checked_abs()?
        .checked_mul(i256::from_i128(2))?;
    let away = match twice_remainder.cmp(&divisor) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => match rounding {
            Rounding::HalfUp => true,
            Rounding::HalfEven => quotient.checked_rem(i256::from_i128(2))? != i256::from_i128(0),
        },
        std::cmp::Ordering::Less => false,
    };
    if away {
        quotient.checked_add(value.signum())
    } else {
        Some(quotient)
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
                number @ (DataType::Int8
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

    #[test]
    fn wide_decimal_products_keep_34_digits_except_for_months() -> Result<()> {
        let decimal = |unscaled: i128, scale: i8| -> Result<ArrayRef> {
            Ok(Arc::new(
                Decimal128Array::from(vec![Some(unscaled), Some(-unscaled)])
                    .with_precision_and_scale(38, scale)?,
            ))
        };
        // 0.9583333333333333333333333333333333333 years is 11.4999...996 months, which keeps
        // 34 digits as 11.5 and rounds to 12.
        let years = decimal(9_583_333_333_333_333_333_333_333_333_333_333_333, 37)?;
        assert_eq!(
            year()?.convert(&years)?.as_primitive::<IntervalYearMonthType>(),
            &IntervalYearMonthArray::from(vec![Some(12), Some(-12)])
        );
        // A MONTH target is not multiplied, so 2.4999...9 rounds to 2.
        let month = SparkNumberToInterval::try_new(
            DataType::Interval(IntervalUnit::YearMonth),
            1,
            "INTERVAL MONTH",
            false,
        )?;
        let months = decimal(24_999_999_999_999_999_999_999_999_999_999_999_999, 37)?;
        assert_eq!(
            month.convert(&months)?.as_primitive::<IntervalYearMonthType>(),
            &IntervalYearMonthArray::from(vec![Some(2), Some(-2)])
        );
        // One day in DECIMAL(38, 30) needs more than 128 bits once multiplied.
        let day = SparkNumberToInterval::try_new(
            DataType::Duration(TimeUnit::Microsecond),
            86_400_000_000,
            "INTERVAL DAY",
            false,
        )?;
        let days = decimal(10_i128.pow(30), 30)?;
        assert_eq!(
            day.convert(&days)?.as_primitive::<DurationMicrosecondType>(),
            &DurationMicrosecondArray::from(vec![Some(86_400_000_000), Some(-86_400_000_000)])
        );
        Ok(())
    }

    #[test]
    fn a_bigint_that_fits_int_overflows_a_year_month_target_as_an_int() -> Result<()> {
        let bigints: ArrayRef = Arc::new(Int64Array::from(vec![178_956_971]));
        let error = year()?.convert(&bigints).unwrap_err().to_string();
        assert!(error.contains("The value 178956971 of the type \"INT\""));
        let day = SparkNumberToInterval::try_new(
            DataType::Duration(TimeUnit::Microsecond),
            86_400_000_000,
            "INTERVAL DAY",
            false,
        )?;
        let bigints: ArrayRef = Arc::new(Int64Array::from(vec![2_147_483_647]));
        let error = day.convert(&bigints).unwrap_err().to_string();
        assert!(error.contains("The value 2147483647L of the type \"BIGINT\""));
        Ok(())
    }
}
