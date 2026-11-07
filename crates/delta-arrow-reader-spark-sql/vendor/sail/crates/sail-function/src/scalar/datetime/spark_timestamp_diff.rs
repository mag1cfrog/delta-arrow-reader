// Local Spark TIMESTAMPDIFF. See the host crate's UPSTREAM.md.
//! Spark's `TimestampDiff`: the whole units from `start` to `end`, counted between their local
//! date-times in the session time zone as Java's `LocalDateTime.until` counts them.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Offset, TimeZone};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::types::TimestampMicrosecondType;
use datafusion::arrow::array::{AsArray, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

const MICROS_PER_DAY: i128 = 86_400_000_000;
/// Gregorian dates repeat every 400 years (146,097 days, a whole number of weeks), and so do
/// fixed offsets and recurring time zone rules.
const MICROS_PER_400_YEARS: i64 = 146_097 * 86_400_000_000;

/// The units Spark's parser accepts for TIMESTAMPDIFF. DAYOFYEAR parses but fails when a row
/// is evaluated.
pub const TIMESTAMP_DIFF_PARSER_UNITS: [&str; 11] = [
    "YEAR",
    "QUARTER",
    "MONTH",
    "WEEK",
    "DAY",
    "DAYOFYEAR",
    "HOUR",
    "MINUTE",
    "SECOND",
    "MILLISECOND",
    "MICROSECOND",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampDiffUnit {
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

impl TimestampDiffUnit {
    /// Spark's difference units, matched case-insensitively.
    pub fn parse(unit: &str) -> Option<Self> {
        Some(match unit.to_ascii_uppercase().as_str() {
            "MICROSECOND" => Self::Microsecond,
            "MILLISECOND" => Self::Millisecond,
            "SECOND" => Self::Second,
            "MINUTE" => Self::Minute,
            "HOUR" => Self::Hour,
            "DAY" => Self::Day,
            "WEEK" => Self::Week,
            "MONTH" => Self::Month,
            "QUARTER" => Self::Quarter,
            "YEAR" => Self::Year,
            _ => return None,
        })
    }

    /// The length of a time-based unit; date-based units count calendar positions instead.
    fn micros(self) -> Option<i128> {
        match self {
            Self::Microsecond => Some(1),
            Self::Millisecond => Some(1_000),
            Self::Second => Some(1_000_000),
            Self::Minute => Some(60_000_000),
            Self::Hour => Some(3_600_000_000),
            Self::Day | Self::Week | Self::Month | Self::Quarter | Self::Year => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampDiff {
    /// The unit as written, for Spark's invalid-unit message.
    unit_text: String,
    /// `None` for a parsed unit that is invalid for differences, which fails per row.
    unit: Option<TimestampDiffUnit>,
    timezone: Arc<str>,
    signature: Signature,
}

impl SparkTimestampDiff {
    pub fn new(unit_text: &str, timezone: Arc<str>) -> Self {
        Self {
            unit_text: unit_text.to_string(),
            unit: TimestampDiffUnit::parse(unit_text),
            timezone,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    fn difference(&self, start: i64, end: i64, zone: &Tz) -> Result<i64> {
        let Some(unit) = self.unit else {
            return exec_err!(
                "[INVALID_PARAMETER_VALUE.DATETIME_UNIT] The value of parameter(s) `unit` in \
                 `TIMESTAMPDIFF` is invalid: expects one of the units without quotes {}, but got \
                 the string literal '{}'.",
                TIMESTAMP_DIFF_PARSER_UNITS.join(", "),
                self.unit_text
            );
        };
        let start = local_micros(start, zone);
        let end = local_micros(end, zone);
        let value = match unit.micros() {
            // Java truncates the local difference toward zero.
            Some(length) => (end - start) / length,
            None => {
                let start_day = start.div_euclid(MICROS_PER_DAY);
                let start_time = start.rem_euclid(MICROS_PER_DAY);
                let mut end_day = end.div_euclid(MICROS_PER_DAY);
                let end_time = end.rem_euclid(MICROS_PER_DAY);
                // A partial last day does not count toward date-based units.
                if end_day > start_day && end_time < start_time {
                    end_day -= 1;
                } else if end_day < start_day && end_time > start_time {
                    end_day += 1;
                }
                let days = end_day - start_day;
                let months = (packed_month(end_day) - packed_month(start_day)) / 32;
                match unit {
                    TimestampDiffUnit::Day => days,
                    TimestampDiffUnit::Week => days / 7,
                    TimestampDiffUnit::Month => months,
                    TimestampDiffUnit::Quarter => months / 3,
                    TimestampDiffUnit::Year => months / 12,
                    _ => return internal_err!("time-based unit {unit:?} has a length"),
                }
            }
        };
        match i64::try_from(value) {
            Ok(value) => Ok(value),
            // Java's exact arithmetic message.
            Err(_) => exec_err!("long overflow"),
        }
    }
}

/// The local wall-clock microseconds of an instant in `zone`, beyond the BIGINT range when
/// the offset moves an extreme instant past it.
fn local_micros(micros: i64, zone: &Tz) -> i128 {
    let mut probe = micros;
    let instant = loop {
        if let Some(instant) = DateTime::from_timestamp_micros(probe) {
            break instant;
        }
        // Chrono's range ends near year 262143; move by whole calendar cycles toward it.
        probe = if probe > 0 {
            probe - MICROS_PER_400_YEARS
        } else {
            probe + MICROS_PER_400_YEARS
        };
    };
    let offset = zone
        .offset_from_utc_datetime(&instant.naive_utc())
        .fix()
        .local_minus_utc();
    i128::from(micros) + i128::from(offset) * 1_000_000
}

/// Java's `LocalDate` month position: the proleptic month times 32 plus the day of month.
fn packed_month(epoch_day: i128) -> i128 {
    let (year, month, day) = civil_from_days(epoch_day);
    (year * 12 + month - 1) * 32 + day
}

/// The proleptic Gregorian year, month and day of an epoch day (Howard Hinnant's algorithm).
fn civil_from_days(days: i128) -> (i128, i128, i128) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i128::from(month <= 2);
    (year, month, day)
}

impl ScalarUDFImpl for SparkTimestampDiff {
    fn name(&self) -> &str {
        "spark_timestamp_diff"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [
                DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
                DataType::Timestamp(TimeUnit::Microsecond, Some(_)),
            ] => Ok(DataType::Int64),
            _ => internal_err!("{} requires two TIMESTAMP arguments", self.name()),
        }
    }

    // Spark folds a constant call during optimization, raising its unit or overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        true
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
            args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [start, end] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let Ok(zone) = Tz::from_str(&self.timezone) else {
            return exec_err!("invalid session time zone {}", self.timezone);
        };
        let scalar = matches!(
            (start, end),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        );
        // Two scalars give one scalar result, whatever the batch's row count.
        let rows = if scalar { 1 } else { args.number_rows };
        let start = start.to_array(rows)?;
        let end = end.to_array(rows)?;
        let start = start.as_primitive::<TimestampMicrosecondType>();
        let end = end.as_primitive::<TimestampMicrosecondType>();
        let result = start
            .iter()
            .zip(end.iter())
            .map(|pair| match pair {
                (Some(start), Some(end)) => self.difference(start, end, &zone).map(Some),
                _ => Ok(None),
            })
            .collect::<Result<Int64Array>>()?;
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
    use datafusion_common::exec_datafusion_err;

    use super::*;

    fn micros(text: &str) -> Result<i64> {
        DateTime::parse_from_rfc3339(text)
            .map(|instant| instant.timestamp_micros())
            .map_err(|error| exec_datafusion_err!("invalid test instant {text}: {error}"))
    }

    fn diff(unit: &str, zone: &str, start: &str, end: &str) -> Result<i64> {
        let tz = Tz::from_str(zone)?;
        SparkTimestampDiff::new(unit, zone.into()).difference(micros(start)?, micros(end)?, &tz)
    }

    #[test]
    fn units_count_local_wall_clock_time() -> Result<()> {
        let la = "America/Los_Angeles";
        // One elapsed hour across the spring gap is two local hours.
        assert_eq!(
            diff("HOUR", la, "2024-03-10T09:30:00Z", "2024-03-10T10:30:00Z")?,
            2
        );
        // One elapsed hour across the fall overlap is no local time.
        assert_eq!(
            diff("MINUTE", la, "2024-11-03T08:30:00Z", "2024-11-03T09:30:00Z")?,
            0
        );
        assert_eq!(
            diff(
                "HOUR",
                "UTC",
                "2024-11-03T08:30:00Z",
                "2024-11-03T09:30:00Z"
            )?,
            1
        );
        // A 23-hour local day is a whole day.
        assert_eq!(
            diff("DAY", la, "2024-03-09T20:00:00Z", "2024-03-10T19:00:00Z")?,
            1
        );
        Ok(())
    }

    #[test]
    fn partial_units_truncate_toward_zero() -> Result<()> {
        let start = "2024-01-01T23:00:00Z";
        let end = "2024-01-02T01:00:00Z";
        assert_eq!(diff("DAY", "UTC", start, end)?, 0);
        assert_eq!(diff("DAY", "UTC", end, start)?, 0);
        assert_eq!(diff("HOUR", "UTC", end, start)?, -2);
        assert_eq!(
            diff(
                "SECOND",
                "UTC",
                "1970-01-01T00:00:00Z",
                "1969-12-31T23:59:58.5Z"
            )?,
            -1
        );
        assert_eq!(
            diff(
                "MILLISECOND",
                "UTC",
                "1970-01-01T00:00:00Z",
                "1969-12-31T23:59:59.9985Z"
            )?,
            -1
        );
        Ok(())
    }

    #[test]
    fn months_follow_java_day_of_month_rules() -> Result<()> {
        // January 31 to February 29 is not a whole month.
        assert_eq!(
            diff(
                "MONTH",
                "UTC",
                "2024-01-31T00:00:00Z",
                "2024-02-29T00:00:00Z"
            )?,
            0
        );
        // An earlier time of day on the same day of month leaves the month incomplete.
        assert_eq!(
            diff(
                "MONTH",
                "UTC",
                "2024-01-29T12:00:00Z",
                "2024-02-29T11:00:00Z"
            )?,
            0
        );
        // A later time of day moves an earlier end date forward: March 1 to March 31.
        assert_eq!(
            diff(
                "MONTH",
                "UTC",
                "2024-03-31T10:00:00Z",
                "2024-02-29T11:00:00Z"
            )?,
            0
        );
        assert_eq!(
            diff("DAY", "UTC", "2024-03-31T10:00:00Z", "2024-02-29T11:00:00Z")?,
            -30
        );
        assert_eq!(
            diff(
                "QUARTER",
                "UTC",
                "2024-05-15T00:00:00Z",
                "2024-01-15T00:00:00Z"
            )?,
            -1
        );
        assert_eq!(
            diff(
                "YEAR",
                "UTC",
                "2000-02-29T00:00:00Z",
                "2001-02-28T00:00:00Z"
            )?,
            0
        );
        assert_eq!(
            diff(
                "YEAR",
                "UTC",
                "0001-01-01T00:00:00Z",
                "9999-12-31T23:59:59.999999Z"
            )?,
            9998
        );
        Ok(())
    }

    #[test]
    fn extreme_instants_keep_their_offsets_and_overflow_like_java() -> Result<()> {
        let tz = Tz::from_str("America/Los_Angeles")?;
        let function = SparkTimestampDiff::new("MICROSECOND", "America/Los_Angeles".into());
        assert_eq!(
            function.difference(0, i64::MAX, &Tz::from_str("UTC")?)?,
            i64::MAX
        );
        let overflow = function.difference(-i64::MAX, i64::MAX, &tz);
        assert!(
            matches!(&overflow, Err(error) if error.to_string().contains("long overflow")),
            "{overflow:?}"
        );
        // Chrono cannot represent these instants. Java places them in January of year 294247
        // (PST) and before the zone's first transition (local mean time, -07:52:58).
        assert_eq!(
            local_micros(i64::MAX, &tz),
            i128::from(i64::MAX) - 8 * 3_600_000_000
        );
        assert_eq!(
            local_micros(-i64::MAX, &tz),
            -i128::from(i64::MAX) - 28_378_000_000
        );
        Ok(())
    }

    #[test]
    fn dayofyear_fails_only_for_evaluated_rows() -> Result<()> {
        let function = SparkTimestampDiff::new("DAYOFYEAR", "UTC".into());
        let timestamp = |value: Option<i64>| {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(value, Some("UTC".into())))
        };
        let field = Arc::new(Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ));
        let invoke = |start: Option<i64>| {
            function.invoke_with_args(ScalarFunctionArgs {
                args: vec![timestamp(start), timestamp(Some(0))],
                arg_fields: vec![field.clone(), field.clone()],
                number_rows: 1,
                return_field: Arc::new(Field::new("r", DataType::Int64, true)),
                config_options: Arc::default(),
            })
        };
        // A NULL argument skips the unit check.
        let null = invoke(None);
        assert!(
            matches!(&null, Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)))),
            "{null:?}"
        );
        let evaluated = invoke(Some(0));
        assert!(
            matches!(
                &evaluated,
                Err(error) if error.to_string().contains("[INVALID_PARAMETER_VALUE.DATETIME_UNIT]")
            ),
            "{evaluated:?}"
        );
        Ok(())
    }

    #[test]
    fn scalar_arguments_give_a_scalar_for_any_row_count() {
        let function = SparkTimestampDiff::new("HOUR", "UTC".into());
        let timestamp = |value: i64| {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                Some(value),
                Some("UTC".into()),
            ))
        };
        let field = Arc::new(Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ));
        for number_rows in [0, 1, 3] {
            let result = function.invoke_with_args(ScalarFunctionArgs {
                args: vec![timestamp(0), timestamp(7_200_000_000)],
                arg_fields: vec![field.clone(), field.clone()],
                number_rows,
                return_field: Arc::new(Field::new("r", DataType::Int64, false)),
                config_options: Arc::default(),
            });
            assert!(
                matches!(
                    &result,
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(2))))
                ),
                "{number_rows} rows: {result:?}"
            );
        }
    }

    #[test]
    fn civil_dates_match_the_proleptic_calendar() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(19_782), (2024, 2, 29));
        assert_eq!(civil_from_days(-719_162), (1, 1, 1));
        assert_eq!(civil_from_days(-719_163), (0, 12, 31));
    }
}
