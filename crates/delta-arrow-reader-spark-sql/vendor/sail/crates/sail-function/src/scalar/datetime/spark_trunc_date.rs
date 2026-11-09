// Local Spark TRUNC. See the host crate's UPSTREAM.md.
//! Spark's `TruncDate`: a DATE truncated to the first day of its year, quarter, month or week,
//! which starts on Monday. Spark spells the units YEAR, YYYY and YY; QUARTER; MON, MONTH and MM;
//! and WEEK, in any letter case and without trimming. Another unit, NULL included, gives NULL.
//! Both arguments are evaluated for every row and a unit column's value is read once per row,
//! except that a unit the optimizer makes a constant Spark does not support gives NULL without
//! the date. The truncation works on the DATE's days over the whole DATE range, with Spark's
//! INT arithmetic near the earliest DATE. The result is declared nullable, as Spark's is.

use std::sync::Arc;

use datafusion::arrow::array::{AsArray, Date32Array};
use datafusion::arrow::datatypes::{DataType, Date32Type, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    lit,
};

/// A unit Spark's TRUNC supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruncUnit {
    Year,
    Quarter,
    Month,
    Week,
}

impl TruncUnit {
    /// The unit `spelling` names, upper-cased as Spark's `toUpperCase(Locale.ROOT)` does, or
    /// `None` for a unit Spark does not support.
    pub fn parse(spelling: &str) -> Option<Self> {
        match spelling.to_uppercase().as_str() {
            "YEAR" | "YYYY" | "YY" => Some(Self::Year),
            "QUARTER" => Some(Self::Quarter),
            "MON" | "MONTH" | "MM" => Some(Self::Month),
            "WEEK" => Some(Self::Week),
            _ => None,
        }
    }

    /// The first day of the unit that holds the DATE `days`, counted from 1970-01-01, as Spark's
    /// `DateTimeUtils.truncDate` computes it. Near the earliest DATE, Spark's INT arithmetic
    /// wraps for WEEK, MONTH and YEAR, and its QUARTER overflows.
    pub fn truncate(self, days: i32) -> Result<i32> {
        let day = i64::from(days);
        let (year, month) = year_and_month(day);
        let first_month = match self {
            // `getNextDateForDayOfWeek(days - 7, MONDAY)`, the first Monday after a week back:
            // `startDay + 1 + ((dayOfWeek - 1 - startDay) % 7 + 7) % 7`, where 1970-01-01, a
            // Thursday, is day 0 of the week and Monday day 4, in Java's INT arithmetic.
            Self::Week => {
                let start = days.wrapping_sub(7);
                let to_monday = (3_i32.wrapping_sub(start) % 7 + 7) % 7;
                return Ok(start.wrapping_add(1).wrapping_add(to_monday));
            }
            Self::Year => 1,
            Self::Month => month,
            Self::Quarter => (month - 1) / 3 * 3 + 1,
        };
        let first = first_day_of_month(year, first_month);
        if self == Self::Quarter {
            return i32::try_from(first).map_err(|_| {
                exec_datafusion_err!(
                    "[ARITHMETIC_OVERFLOW] overflow: TRUNC of DATE {days} to QUARTER is before \
                     the earliest DATE"
                )
            });
        }
        // `days - getDayInYear(days) + 1` and `days - getDayOfMonth(days) + 1`.
        let offset = i32::try_from(day - first)
            .map_err(|_| exec_datafusion_err!("TRUNC of DATE {days} to {self:?} out of range"))?;
        Ok(days.wrapping_sub(offset))
    }
}

/// The proleptic Gregorian year and month of `day`, in days since 1970-01-01, by Howard
/// Hinnant's `civil_from_days`.
fn year_and_month(day: i64) -> (i64, i64) {
    let day = day + 719_468;
    let era = day.div_euclid(146_097);
    let day_of_era = day - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    // Months counted from March, so that February ends the year.
    let month_from_march = (5 * day_of_year + 2) / 153;
    let month = if month_from_march < 10 {
        month_from_march + 3
    } else {
        month_from_march - 9
    };
    (year_of_era + era * 400 + i64::from(month <= 2), month)
}

/// The day, in days since 1970-01-01, of the first of a proleptic Gregorian month, by Howard
/// Hinnant's `days_from_civil`.
fn first_day_of_month(year: i64, month: i64) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month_from_march = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_from_march + 2) / 5;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTruncDate {
    signature: Signature,
}

impl Default for SparkTruncDate {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTruncDate {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Date32, DataType::Utf8],
                Volatility::Immutable,
            ),
        }
    }
}

/// One row: NULL for a NULL date or an unsupported unit.
fn truncate_row(days: Option<i32>, unit: Option<TruncUnit>) -> Result<Option<i32>> {
    match (days, unit) {
        (Some(days), Some(unit)) => unit.truncate(days).map(Some),
        _ => Ok(None),
    }
}

impl ScalarUDFImpl for SparkTruncDate {
    fn name(&self) -> &str {
        "spark_trunc_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Date32, true)))
    }

    /// Spark's TRUNC reads no date for a constant unit it does not support, also when its
    /// optimizer makes the unit constant, so a unit that becomes such a literal gives NULL.
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if let [_, Expr::Literal(ScalarValue::Utf8(unit), _)] = args.as_slice()
            && unit.as_deref().and_then(TruncUnit::parse).is_none()
        {
            return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Date32(
                None,
            ))));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [date, unit] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let result = match (date, unit) {
            (ColumnarValue::Scalar(date), ColumnarValue::Scalar(unit)) => {
                let (ScalarValue::Date32(days), ScalarValue::Utf8(unit)) = (date, unit) else {
                    return internal_err!("{} requires a DATE and a STRING", self.name());
                };
                let unit = unit.as_deref().and_then(TruncUnit::parse);
                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(truncate_row(
                    *days, unit,
                )?)));
            }
            (date, ColumnarValue::Scalar(unit)) => {
                let ScalarValue::Utf8(unit) = unit else {
                    return internal_err!("{} requires a STRING unit", self.name());
                };
                let unit = unit.as_deref().and_then(TruncUnit::parse);
                date.to_array(args.number_rows)?
                    .as_primitive::<Date32Type>()
                    .iter()
                    .map(|days| truncate_row(days, unit))
                    .collect::<Result<Date32Array>>()?
            }
            (date, unit) => {
                let dates = date.to_array(args.number_rows)?;
                let units = unit.to_array(args.number_rows)?;
                dates
                    .as_primitive::<Date32Type>()
                    .iter()
                    .zip(units.as_string::<i32>().iter())
                    .map(|(days, unit)| truncate_row(days, unit.and_then(TruncUnit::parse)))
                    .collect::<Result<Date32Array>>()?
            }
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Datelike, Duration, NaiveDate};

    use super::*;

    const UNITS: [TruncUnit; 4] = [
        TruncUnit::Year,
        TruncUnit::Quarter,
        TruncUnit::Month,
        TruncUnit::Week,
    ];

    fn day(text: &str) -> Result<i32> {
        let date = NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .map_err(|error| exec_datafusion_err!("{error}"))?;
        Ok(Date32Type::from_naive_date(date))
    }

    #[test]
    fn truncates_to_spark_units() -> Result<()> {
        for (date, unit, expected) in [
            ("2024-05-17", "yy", "2024-01-01"),
            ("2024-05-17", "Quarter", "2024-04-01"),
            ("2024-05-17", "mon", "2024-05-01"),
            ("2024-05-17", "WEEK", "2024-05-13"),
            ("2024-02-29", "MONTH", "2024-02-01"),
            ("0001-01-02", "MM", "0001-01-01"),
            ("0001-01-07", "week", "0001-01-01"),
            ("9999-12-31", "YEAR", "9999-01-01"),
        ] {
            let unit = TruncUnit::parse(unit);
            assert_eq!(truncate_row(Some(day(date)?), unit)?, Some(day(expected)?));
        }
        for unit in ["DD", " MM", ""] {
            let unit = TruncUnit::parse(unit);
            assert_eq!(truncate_row(Some(day("2024-05-17")?), unit)?, None);
        }
        assert_eq!(truncate_row(None, Some(TruncUnit::Month))?, None);
        Ok(())
    }

    /// The day arithmetic agrees with chrono across chrono's range.
    #[test]
    fn day_arithmetic_matches_chrono() -> Result<()> {
        let mut days = -95_000_000_i32;
        while days < 95_000_000 {
            let date = Date32Type::to_naive_date_opt(days)
                .ok_or_else(|| exec_datafusion_err!("DATE {days} outside chrono's range"))?;
            let expected = [
                NaiveDate::from_ymd_opt(date.year(), 1, 1),
                NaiveDate::from_ymd_opt(date.year(), date.month0() / 3 * 3 + 1, 1),
                NaiveDate::from_ymd_opt(date.year(), date.month(), 1),
                date.checked_sub_signed(Duration::days(i64::from(
                    date.weekday().num_days_from_monday(),
                ))),
            ];
            for (unit, expected) in UNITS.iter().zip(expected) {
                let expected = expected.ok_or_else(|| exec_datafusion_err!("chrono overflow"))?;
                assert_eq!(unit.truncate(days)?, Date32Type::from_naive_date(expected));
            }
            days += 9_973;
        }
        Ok(())
    }

    /// Beyond chrono's range every DATE truncates to a first day no later than itself. Near the
    /// earliest DATE, a Tuesday in June, the first day is before it, and Spark's INT arithmetic
    /// wraps for WEEK, MONTH and YEAR, as `trunc(date_sub(DATE '1970-01-01', 2147483647), unit)`
    /// shows in Spark 4.2.0; QUARTER fails.
    #[test]
    fn truncates_across_the_date_range() -> Result<()> {
        for unit in UNITS {
            for days in [i32::MAX, 100_000_000, -100_000_000, i32::MIN + 400] {
                let first = unit.truncate(days)?;
                assert!(
                    first <= days && days - first < 366,
                    "{unit:?} {days} {first}"
                );
                assert_eq!(unit.truncate(first)?, first);
            }
        }
        let earliest = i32::MIN + 1;
        assert_eq!(TruncUnit::Week.truncate(earliest)?, 2_147_483_643);
        // Spark's `dayOfWeek - 1 - startDay` overflows for these days.
        for (days, monday) in [
            (-2_147_483_641, -2_147_483_646),
            (-2_147_483_640, -2_147_483_646),
            (-2_147_483_639, -2_147_483_639),
            (-2_147_483_638, -2_147_483_639),
            (-2_147_483_637, -2_147_483_642),
        ] {
            assert_eq!(TruncUnit::Week.truncate(days)?, monday);
        }
        assert_eq!(TruncUnit::Month.truncate(earliest)?, 2_147_483_626);
        assert_eq!(TruncUnit::Year.truncate(earliest)?, 2_147_483_475);
        assert!(TruncUnit::Quarter.truncate(earliest).is_err());
        Ok(())
    }
}
