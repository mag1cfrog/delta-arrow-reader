// Local Spark TIMESTAMPADD. See the host crate's UPSTREAM.md.
//! Spark's TIMESTAMPADD, DATEADD and three-argument DATE_ADD, as `DateTimeUtils.timestampAdd`
//! gives it: a BIGINT quantity of DAY, WEEK, MONTH, QUARTER or YEAR units moves the local
//! date-time in the TIMESTAMP's time zone, and smaller units move the instant. A
//! TIMESTAMP_NTZ moves in UTC and stays a TIMESTAMP_NTZ.

use std::sync::Arc;

use datafusion::arrow::array::types::{Int64Type, TimestampMicrosecondType};
use datafusion::arrow::array::{Array, AsArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::scalar::datetime::local_time::{MICROS_PER_DAY, TimestampZone, add_interval};

const MICROS_PER_HOUR: i64 = 3_600_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Unit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampAdd {
    unit: Unit,
    /// The unit as written, upper case, for errors.
    unit_name: String,
    /// Spark's implicit casts of a string or fractional quantity, or of a string TIMESTAMP,
    /// can return NULL, so the result is nullable whatever the arguments declare.
    force_nullable: bool,
    signature: Signature,
}

impl SparkTimestampAdd {
    pub fn try_new(unit: &str, force_nullable: bool) -> Result<Self> {
        let unit_name = unit.trim().to_ascii_uppercase();
        let unit = match unit_name.as_str() {
            "MICROSECOND" => Unit::Microsecond,
            "MILLISECOND" => Unit::Millisecond,
            "SECOND" => Unit::Second,
            "MINUTE" => Unit::Minute,
            "HOUR" => Unit::Hour,
            "DAY" | "DAYOFYEAR" => Unit::Day,
            "WEEK" => Unit::Week,
            "MONTH" => Unit::Month,
            "QUARTER" => Unit::Quarter,
            "YEAR" => Unit::Year,
            _ => return plan_err!("TIMESTAMPADD does not support the unit {unit}"),
        };
        Ok(Self {
            unit,
            unit_name,
            force_nullable,
            signature: Signature::any(2, Volatility::Immutable),
        })
    }

    /// `micros` plus `quantity` units in `zone`, or `None` where Spark overflows an INT unit
    /// count, a BIGINT microsecond count or the result.
    fn add(&self, quantity: i64, micros: i64, zone: &TimestampZone) -> Option<i64> {
        let instant = |factor: i64| add_interval(micros, 0, 0, quantity.checked_mul(factor)?, zone);
        let count = |factor: i32| i32::try_from(quantity).ok()?.checked_mul(factor);
        match self.unit {
            Unit::Microsecond => instant(1),
            Unit::Millisecond => instant(1_000),
            Unit::Second => instant(1_000_000),
            Unit::Minute => instant(60_000_000),
            Unit::Hour => instant(MICROS_PER_HOUR),
            Unit::Day => add_interval(micros, 0, i64::from(count(1)?), 0, zone),
            Unit::Week => add_interval(micros, 0, i64::from(count(7)?), 0, zone),
            Unit::Month => add_interval(micros, i64::from(count(1)?), 0, 0, zone),
            Unit::Quarter => add_interval(micros, i64::from(count(3)?), 0, 0, zone),
            Unit::Year => add_interval(micros, i64::from(count(12)?), 0, 0, zone),
        }
    }
}

impl ScalarUDFImpl for SparkTimestampAdd {
    fn name(&self) -> &str {
        "spark_timestampadd"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [
                DataType::Int64,
                timestamp @ DataType::Timestamp(TimeUnit::Microsecond, _),
            ] => Ok(timestamp.clone()),
            _ => internal_err!("{} requires a BIGINT quantity and a TIMESTAMP", self.name()),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            self.force_nullable || args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    // Spark folds a constant TIMESTAMPADD during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [quantities, timestamps] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let DataType::Timestamp(TimeUnit::Microsecond, zone) = timestamps.data_type() else {
            return internal_err!("{} requires a TIMESTAMP", self.name());
        };
        let Some(parsed) = TimestampZone::parse(zone.as_deref().unwrap_or("UTC")) else {
            return exec_err!("invalid time zone {zone:?}");
        };
        let scalar = matches!(
            (quantities, timestamps),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        );
        let quantities = quantities.to_array(args.number_rows)?;
        let quantities = quantities.as_primitive::<Int64Type>();
        let timestamps = timestamps.to_array(args.number_rows)?;
        let result = timestamps
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .enumerate()
            .map(|(row, micros)| match micros {
                Some(micros) if quantities.is_valid(row) => {
                    let quantity = quantities.value(row);
                    match self.add(quantity, micros, &parsed) {
                        Some(result) => Ok(Some(result)),
                        None => exec_err!(
                            "[DATETIME_OVERFLOW] Datetime operation overflow: add {quantity}L {} \
                             to TIMESTAMP '{}'. SQLSTATE: 22008",
                            self.unit_name,
                            local_text(micros, &parsed)
                        ),
                    }
                }
                _ => Ok(None),
            })
            .collect::<Result<TimestampMicrosecondArray>>()?
            .with_timezone_opt(zone);
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

/// The local date-time of `micros` in `zone` as Spark prints a TIMESTAMP, `+` before a year
/// beyond 9999 and without trailing fraction zeros.
fn local_text(micros: i64, zone: &TimestampZone) -> String {
    let local = i128::from(micros) + zone.offset_micros(micros);
    let micros_per_day = i128::from(MICROS_PER_DAY);
    let Ok(days) = i64::try_from(local.div_euclid(micros_per_day)) else {
        return micros.to_string();
    };
    let time = local.rem_euclid(micros_per_day);
    let (year, month, day) = civil_from_days(days);
    let year = match year {
        year if year > 9999 => format!("+{year}"),
        year if year < 0 => format!("-{:04}", -year),
        year => format!("{year:04}"),
    };
    let seconds = time / 1_000_000;
    let fraction = format!("{:06}", time % 1_000_000);
    let fraction = fraction.trim_end_matches('0');
    format!(
        "{year}-{month:02}-{day:02} {:02}:{:02}:{:02}{}{fraction}",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60,
        if fraction.is_empty() { "" } else { "." },
    )
}

/// The proleptic Gregorian year, month and day of a day count from 1970-01-01.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(unit: &str, quantity: i64, micros: i64, zone: &str) -> Option<i64> {
        let parsed = TimestampZone::parse(zone)?;
        SparkTimestampAdd::try_new(unit, false)
            .ok()?
            .add(quantity, micros, &parsed)
    }

    #[test]
    fn units_up_to_hours_move_the_instant_and_larger_ones_the_local_date_time() {
        let la = "America/Los_Angeles";
        // 2024-03-09 02:30 PST.
        let start = 1_709_980_200_000_000;
        assert_eq!(
            add("HOUR", 24, start, la),
            Some(start + 24 * MICROS_PER_HOUR)
        );
        assert_eq!(add("DAY", 1, start, la), Some(1_710_066_600_000_000));
        assert_eq!(add("dayofyear", 1, start, la), Some(1_710_066_600_000_000));
        assert_eq!(
            add("WEEK", 1, start, la),
            Some(start + 7 * MICROS_PER_DAY - MICROS_PER_HOUR)
        );
        // Spark adds a BIGINT microsecond count exactly.
        assert_eq!(
            add("MICROSECOND", 9_007_199_254_740_993, 0, "UTC"),
            Some(9_007_199_254_740_993)
        );
    }

    #[test]
    fn counts_beyond_int_and_results_beyond_bigint_overflow() {
        assert_eq!(add("DAY", 2_147_483_648, 0, "UTC"), None);
        assert_eq!(add("YEAR", 178_956_971, 0, "UTC"), None);
        assert_eq!(add("SECOND", i64::MAX, 0, "UTC"), None);
        assert_eq!(add("MICROSECOND", 1, i64::MAX, "UTC"), None);
    }

    #[test]
    fn overflow_messages_print_the_local_date_time() -> Result<()> {
        let zone = TimestampZone::parse("America/Los_Angeles")
            .ok_or_else(|| datafusion_common::DataFusionError::Internal("zone".into()))?;
        assert_eq!(local_text(i64::MAX, &zone), "+294247-01-09 20:00:54.775807");
        assert_eq!(
            local_text(1_704_096_000_000_000, &zone),
            "2024-01-01 00:00:00"
        );
        assert_eq!(civil_from_days(-719_162), (1, 1, 1));
        Ok(())
    }
}
