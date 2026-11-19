// Frozen unformatted conversion bodies from be3c663a8380d5094cb52003f351d5b9a5d48383.
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::compute::kernels::cast_utils::Parser;
use datafusion::arrow::datatypes::{Date32Type, IntervalMonthDayNano};
use datafusion::common::{Result, exec_datafusion_err, exec_err};
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_sql_analyzer::literal::interval::IntervalValue;
use sail_sql_analyzer::parser::{parse_date, parse_interval, parse_timestamp};
use std::sync::Arc;

pub fn string_to_date32(value: &str, is_try: bool) -> Result<Option<i32>> {
    // Native parsing agrees with Spark on this subset, not its other date forms.
    let bytes = value.as_bytes();
    if bytes.len() == 10
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            4 | 7 => *byte == b'-',
            _ => byte.is_ascii_digit(),
        })
        && let Some(date) = Date32Type::parse(value)
    {
        return Ok(Some(date));
    }
    match parse_date(value).and_then(|date| Ok(Date32Type::from_naive_date(date.try_into()?))) {
        Ok(v) => Ok(Some(v)),
        Err(_e) if is_try => Ok(None),
        Err(_) => {
            let sql_value = value.replace('\\', "\\\\").replace('\'', "\\'");
            Err(exec_datafusion_err!(
                "[CAST_INVALID_INPUT] The value '{sql_value}' of the type \"STRING\" cannot be cast to \"DATE\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead."
            ))
        }
    }
}

enum TimestampParser {
    Ltz { default_timezone: String },
    Ntz,
}
impl TimestampParser {
    fn string_to_microseconds(&self, value: &str, safe: bool) -> Result<Option<i64>> {
        let timestamp = match parse_timestamp(value) {
            Ok(v) => v,
            Err(_e) if safe => return Ok(None),
            Err(e) => return Err(exec_datafusion_err!("{e}")),
        };
        if timestamp.time.second == 60 {
            return if safe {
                Ok(None)
            } else {
                exec_err!("invalid timestamp: leap seconds are not supported")
            };
        }
        let (datetime, timezone) = match timestamp.into_naive() {
            Ok(v) => v,
            Err(_e) if safe => return Ok(None),
            Err(e) => return Err(exec_datafusion_err!("{e}")),
        };
        match self {
            TimestampParser::Ltz { default_timezone } => {
                let tz: Tz = if timezone.is_empty() {
                    match default_timezone.parse() {
                        Ok(v) => v,
                        Err(_e) if safe => return Ok(None),
                        Err(e) => return Err(e.into()),
                    }
                } else {
                    match timezone.parse() {
                        Ok(v) => v,
                        Err(_e) if safe => return Ok(None),
                        Err(e) => return Err(e.into()),
                    }
                };
                match localize_with_fallback(&tz, &datetime) {
                    Ok(v) => Ok(Some(v.timestamp_micros())),
                    Err(_e) if safe => Ok(None),
                    Err(e) => Err(e),
                }
            }
            // NTZ ignores any timezone in the input and keeps the wall clock.
            TimestampParser::Ntz => Ok(Some(datetime.and_utc().timestamp_micros())),
        }
    }
}
pub fn timestamp(value: &str, timezone: Option<Arc<str>>, safe: bool) -> Result<Option<i64>> {
    let parser = match timezone {
        Some(timezone) => TimestampParser::Ltz {
            default_timezone: timezone.to_string(),
        },
        None => TimestampParser::Ntz,
    };
    parser.string_to_microseconds(value, safe)
}

pub fn parse_timestamp_string(value: &str, timezone: &str) -> Result<Option<i64>> {
    let parsed = parse_timestamp(value).and_then(|x| x.into_naive());
    let (datetime, parsed_timezone) = match parsed {
        Ok(v) => v,
        Err(_e) => return Ok(None),
    };
    // Use the timezone from the parsed string if present, otherwise use the provided timezone
    let timezone: Tz = if parsed_timezone.is_empty() {
        match timezone.parse() {
            Ok(v) => v,
            Err(_e) => return Ok(None),
        }
    } else {
        match parsed_timezone.parse() {
            Ok(v) => v,
            Err(_e) => return Ok(None),
        }
    };
    let datetime = match localize_with_fallback(&timezone, &datetime) {
        Ok(v) => v,
        Err(_e) => return Ok(None),
    };
    Ok(Some(datetime.timestamp_micros()))
}

pub fn string_to_year_month_interval(value: &str) -> Result<i32> {
    let interval = parse_interval(value).map_err(|e| exec_datafusion_err!("{e}"))?;
    match interval {
        IntervalValue::YearMonth { months, .. } => Ok(months),
        IntervalValue::Microsecond { .. } | IntervalValue::MonthDayNanosecond { .. } => {
            exec_err!("expected year month interval, but got: {value}")
        }
    }
}

pub fn string_to_day_time_interval(value: &str) -> Result<i64> {
    let interval = parse_interval(value).map_err(|e| exec_datafusion_err!("{e}"))?;
    match interval {
        IntervalValue::Microsecond { microseconds, .. } => Ok(microseconds),
        IntervalValue::YearMonth { .. } | IntervalValue::MonthDayNanosecond { .. } => {
            exec_err!("expected day time interval, but got: {value}")
        }
    }
}

pub fn string_to_calendar_interval(value: &str) -> Result<IntervalMonthDayNano> {
    let interval = parse_interval(value).map_err(|e| exec_datafusion_err!("{e}"))?;
    match interval {
        IntervalValue::YearMonth { months, .. } => Ok(IntervalMonthDayNano {
            months,
            days: 0,
            nanoseconds: 0,
        }),
        IntervalValue::Microsecond { microseconds, .. } => {
            day_time_interval_to_calendar_interval(microseconds)
        }
        IntervalValue::MonthDayNanosecond {
            months,
            days,
            nanoseconds,
        } => Ok(IntervalMonthDayNano {
            months,
            days,
            nanoseconds,
        }),
    }
}

fn day_time_interval_to_calendar_interval(microseconds: i64) -> Result<IntervalMonthDayNano> {
    const MICROSECONDS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

    let days = i32::try_from(microseconds / MICROSECONDS_PER_DAY).map_err(|_| {
        exec_datafusion_err!("microseconds overflow for calendar interval: {microseconds}")
    })?;
    Ok(IntervalMonthDayNano {
        months: 0,
        days,
        nanoseconds: microseconds % MICROSECONDS_PER_DAY * 1_000,
    })
}
