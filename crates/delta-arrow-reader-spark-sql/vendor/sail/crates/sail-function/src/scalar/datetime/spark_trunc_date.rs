// Local Spark TRUNC. See the host crate's UPSTREAM.md.
//! Spark's `TruncDate`: a DATE truncated to the first day of its year, quarter, month or week,
//! which starts on Monday. Spark spells the units YEAR, YYYY and YY; QUARTER; MON, MONTH and MM;
//! and WEEK, in any case and without trimming. Another unit, NULL included, gives NULL. Both
//! arguments are evaluated for every row, the truncation works on the DATE's days whatever its
//! year, and the result is declared nullable, as Spark's is.

use std::sync::Arc;

use chrono::{Datelike, Duration, NaiveDate};
use datafusion::arrow::array::{AsArray, Date32Array};
use datafusion::arrow::datatypes::{DataType, Date32Type, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
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

    /// The first day of the unit that holds the DATE `days`.
    pub fn truncate(self, days: i32) -> Result<i32> {
        let date = Date32Type::to_naive_date_opt(days)
            .ok_or_else(|| exec_datafusion_err!("DATE {days} is outside the supported range"))?;
        let truncated = match self {
            Self::Year => NaiveDate::from_ymd_opt(date.year(), 1, 1),
            Self::Quarter => NaiveDate::from_ymd_opt(date.year(), date.month0() / 3 * 3 + 1, 1),
            Self::Month => NaiveDate::from_ymd_opt(date.year(), date.month(), 1),
            Self::Week => date.checked_sub_signed(Duration::days(i64::from(
                date.weekday().num_days_from_monday(),
            ))),
        }
        .ok_or_else(|| exec_datafusion_err!("cannot truncate DATE {days} to {self:?}"))?;
        Ok(Date32Type::from_naive_date(truncated))
    }
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

fn truncate(days: Option<i32>, unit: Option<&str>) -> Result<Option<i32>> {
    match (days, unit.and_then(TruncUnit::parse)) {
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [date, unit] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        if let (ColumnarValue::Scalar(date), ColumnarValue::Scalar(unit)) = (date, unit) {
            let (ScalarValue::Date32(days), ScalarValue::Utf8(unit)) = (date, unit) else {
                return internal_err!("{} requires a DATE and a STRING", self.name());
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Date32(truncate(
                *days,
                unit.as_deref(),
            )?)));
        }
        let dates = date.to_array(args.number_rows)?;
        let units = unit.to_array(args.number_rows)?;
        let result = dates
            .as_primitive::<Date32Type>()
            .iter()
            .zip(units.as_string::<i32>().iter())
            .map(|(days, unit)| truncate(days, unit))
            .collect::<Result<Date32Array>>()?;
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn day(text: &str) -> Result<i32> {
        let date = NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .map_err(|error| exec_datafusion_err!("{error}"))?;
        Ok(Date32Type::from_naive_date(date))
    }

    #[test]
    fn truncates_to_spark_units_across_the_date_range() -> Result<()> {
        for (date, unit, expected) in [
            ("2024-05-17", "yy", "2024-01-01"),
            ("2024-05-17", "Quarter", "2024-04-01"),
            ("2024-05-17", "mon", "2024-05-01"),
            ("2024-05-17", "WEEK", "2024-05-13"),
            ("0001-01-02", "MM", "0001-01-01"),
            ("9999-12-31", "YEAR", "9999-01-01"),
        ] {
            assert_eq!(
                truncate(Some(day(date)?), Some(unit))?,
                Some(day(expected)?)
            );
        }
        for unit in [Some("DD"), Some(" MM"), Some(""), None] {
            assert_eq!(truncate(Some(day("2024-05-17")?), unit)?, None);
        }
        assert_eq!(truncate(None, Some("MM"))?, None);
        Ok(())
    }
}
