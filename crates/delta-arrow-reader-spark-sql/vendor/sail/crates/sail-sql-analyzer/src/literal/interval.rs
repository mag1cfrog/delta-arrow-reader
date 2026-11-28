// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::iter::once;
use std::ops::Neg;
use std::str::FromStr;

use chrono::{self, TimeDelta};
use lazy_static::lazy_static;
use regex::Regex;
use sail_common::spec;
use sail_sql_parser::ast::data_type::{IntervalDayTimeUnit, IntervalYearMonthUnit};
use sail_sql_parser::ast::expression::{
    Expr, IntervalExpr, IntervalLiteral, IntervalQualifier, IntervalUnit, IntervalValueWithUnit,
};

use crate::error::{SqlError, SqlResult};
use crate::literal::utils::{Signed, extract_fraction_match, extract_match, parse_signed_value};
use crate::parser::parse_interval_literal;
use crate::value::from_ast_string;

fn create_regex(regex: Result<Regex, regex::Error>) -> Regex {
    #[expect(clippy::unwrap_used)]
    regex.unwrap()
}

lazy_static! {
    static ref INTERVAL_YEAR_REGEX: Regex =
        create_regex(Regex::new(r"^\s*(?P<sign>[+-]?)(?P<year>\d+)\s*$"));
    static ref INTERVAL_YEAR_TO_MONTH_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<year>\d+)-(?P<month>\d+)\s*$"
    ));
    static ref INTERVAL_MONTH_REGEX: Regex =
        create_regex(Regex::new(r"^\s*(?P<sign>[+-]?)(?P<month>\d+)\s*$"));
    static ref INTERVAL_DAY_REGEX: Regex =
        create_regex(Regex::new(r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s*$"));
    static ref INTERVAL_DAY_TO_HOUR_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s+(?P<hour>\d+)\s*$"
    ));
    static ref INTERVAL_DAY_TO_MINUTE_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s+(?P<hour>\d+):(?P<minute>\d+)\s*$"
    ));
    static ref INTERVAL_DAY_TO_SECOND_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<day>\d+)\s+(?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$"
    ));
    static ref INTERVAL_HOUR_REGEX: Regex =
        create_regex(Regex::new(r"^\s*(?P<sign>[+-]?)(?P<hour>\d+)\s*$"));
    static ref INTERVAL_HOUR_TO_MINUTE_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<hour>\d+):(?P<minute>\d+)\s*$"
    ));
    static ref INTERVAL_HOUR_TO_SECOND_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<hour>\d+):(?P<minute>\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$"
    ));
    static ref INTERVAL_MINUTE_REGEX: Regex =
        create_regex(Regex::new(r"^\s*(?P<sign>[+-]?)(?P<minute>\d+)\s*$"));
    static ref INTERVAL_MINUTE_TO_SECOND_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<minute>\d+):(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$"
    ));
    static ref INTERVAL_SECOND_REGEX: Regex = create_regex(Regex::new(
        r"^\s*(?P<sign>[+-]?)(?P<second>\d+)[.]?(?P<fraction>\d+)?\s*$"
    ));
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IntervalValue {
    YearMonth {
        months: i32,
        start_field: spec::IntervalFieldType,
        end_field: spec::IntervalFieldType,
    },
    Microsecond {
        microseconds: i64,
        start_field: spec::IntervalFieldType,
        end_field: spec::IntervalFieldType,
    },
    MonthDayNanosecond {
        months: i32,
        days: i32,
        nanoseconds: i64,
    },
}

impl From<IntervalValue> for spec::Literal {
    fn from(value: IntervalValue) -> Self {
        match value {
            IntervalValue::YearMonth {
                months,
                start_field,
                end_field,
            } => spec::Literal::IntervalYearMonth {
                months: Some(months),
                start_field: Some(start_field),
                end_field: Some(end_field),
            },
            IntervalValue::Microsecond {
                microseconds,
                start_field,
                end_field,
            } => spec::Literal::DurationMicrosecond {
                microseconds: Some(microseconds),
                start_field: Some(start_field),
                end_field: Some(end_field),
            },
            IntervalValue::MonthDayNanosecond {
                months,
                days,
                nanoseconds,
            } => spec::Literal::IntervalMonthDayNano {
                value: Some(spec::IntervalMonthDayNano {
                    months,
                    days,
                    nanoseconds,
                }),
            },
        }
    }
}

/// Chooses between the year-month and day-time families for a multi-unit interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultiUnitFamilyRule {
    /// Spark SQL interval literals: the written units decide, so `INTERVAL 0 YEARS 0 MONTHS`
    /// stays year-month and `INTERVAL 0 MONTHS 5 DAYS` mixes both families.
    WrittenUnits,
    /// Interval strings converted at run time, such as string casts and `window` durations:
    /// the non-zero values decide, so `'0 months 5 days'` is a day-time interval.
    NonZeroValues,
}

#[derive(Clone, Copy)]
enum UnitKind {
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
}

impl UnitKind {
    fn from_ast(unit: &IntervalUnit) -> Self {
        match unit {
            IntervalUnit::Year(_) | IntervalUnit::Years(_) => Self::Year,
            IntervalUnit::Month(_) | IntervalUnit::Months(_) => Self::Month,
            IntervalUnit::Week(_) | IntervalUnit::Weeks(_) => Self::Week,
            IntervalUnit::Day(_) | IntervalUnit::Days(_) => Self::Day,
            IntervalUnit::Hour(_) | IntervalUnit::Hours(_) => Self::Hour,
            IntervalUnit::Minute(_) | IntervalUnit::Minutes(_) => Self::Minute,
            IntervalUnit::Second(_) | IntervalUnit::Seconds(_) => Self::Second,
            IntervalUnit::Millisecond(_) | IntervalUnit::Milliseconds(_) => Self::Millisecond,
            IntervalUnit::Microsecond(_) | IntervalUnit::Microseconds(_) => Self::Microsecond,
        }
    }

    fn from_text(unit: &str) -> Option<Self> {
        let base = unit.strip_suffix(['s', 'S']).unwrap_or(unit);
        [
            ("year", Self::Year),
            ("month", Self::Month),
            ("week", Self::Week),
            ("day", Self::Day),
            ("hour", Self::Hour),
            ("minute", Self::Minute),
            ("second", Self::Second),
            ("millisecond", Self::Millisecond),
            ("microsecond", Self::Microsecond),
        ]
        .into_iter()
        .find_map(|(name, kind)| base.eq_ignore_ascii_case(name).then_some(kind))
    }

    fn field(self) -> spec::IntervalFieldType {
        use spec::IntervalFieldType::{Day, Hour, Minute, Month, Second, Year};
        match self {
            Self::Year => Year,
            Self::Month => Month,
            Self::Week | Self::Day => Day,
            Self::Hour => Hour,
            Self::Minute => Minute,
            Self::Second | Self::Millisecond | Self::Microsecond => Second,
        }
    }

    fn standard(self) -> Option<StandardIntervalKind> {
        Some(match self {
            Self::Year => StandardIntervalKind::Year,
            Self::Month => StandardIntervalKind::Month,
            Self::Day => StandardIntervalKind::Day,
            Self::Hour => StandardIntervalKind::Hour,
            Self::Minute => StandardIntervalKind::Minute,
            Self::Second => StandardIntervalKind::Second,
            Self::Week | Self::Millisecond | Self::Microsecond => return None,
        })
    }
}

enum UnitAmount<'a> {
    Expression(Expr),
    Text(&'a str),
}

impl UnitAmount<'_> {
    fn parse<T: FromStr + Neg<Output = T>>(self) -> SqlResult<T> {
        match self {
            Self::Expression(value) => parse_signed_value(value),
            Self::Text(text) => {
                // The expression path parses the unsigned magnitude before applying minus.
                let digits = text.strip_prefix('-').unwrap_or(text);
                let value: T = digits
                    .parse()
                    .map_err(|_| SqlError::invalid(format!("literal: {text}")))?;
                Ok(if text.starts_with('-') { -value } else { value })
            }
        }
    }
}

/// Converts a Spark SQL interval literal, choosing a multi-unit family by its written units.
pub fn from_ast_signed_interval(value: Signed<IntervalExpr>) -> SqlResult<IntervalValue> {
    from_ast_signed_interval_with_rule(value, MultiUnitFamilyRule::WrittenUnits)
}

fn from_ast_signed_interval_with_rule(
    value: Signed<IntervalExpr>,
    rule: MultiUnitFamilyRule,
) -> SqlResult<IntervalValue> {
    // TODO: support the legacy calendar interval when `spark.sql.legacy.interval.enabled` is `true`
    let negated = value.is_negative();
    let interval = value.into_inner();
    match interval.clone() {
        IntervalExpr::Standard { value, qualifier } => {
            let kind = from_ast_interval_qualifier(qualifier)?;
            from_ast_standard_interval(value, kind, negated)
        }
        IntervalExpr::MultiUnit { head, tail } => {
            if tail.is_empty()
                && let Some(kind) = UnitKind::from_ast(&head.unit).standard()
            {
                return from_ast_standard_interval(head.value, kind, negated);
            }
            let values = once(head).chain(tail).collect();
            from_ast_multi_unit_interval(values, negated, rule)
        }
        IntervalExpr::Literal(value) => {
            // Decoded text remains scoped to this conversion.
            parse_interval_string(&from_ast_string(value)?, negated, rule)
        }
    }
}

struct DecimalSecond {
    seconds: u32,
    microseconds: u32,
}

impl FromStr for Signed<DecimalSecond> {
    type Err = SqlError;

    fn from_str(s: &str) -> SqlResult<Self> {
        let error = || SqlError::invalid(format!("second: {s:?}"));
        let captures = INTERVAL_SECOND_REGEX.captures(s).ok_or_else(error)?;
        let negated = captures.name("sign").map(|s| s.as_str()) == Some("-");
        let seconds: u32 = extract_match(&captures, "second", error)?.unwrap_or(0);
        let microseconds: u32 =
            extract_fraction_match(&captures, "fraction", 6, error)?.unwrap_or(0);
        let value = DecimalSecond {
            seconds,
            microseconds,
        };
        if negated {
            Ok(Signed::Negative(value))
        } else {
            Ok(Signed::Positive(value))
        }
    }
}

fn parse_interval_year_month_string(
    s: &str,
    negated: bool,
    interval_regex: &Regex,
    start_field: spec::IntervalFieldType,
    end_field: spec::IntervalFieldType,
) -> SqlResult<IntervalValue> {
    let error = || SqlError::invalid(format!("interval: {s}"));
    let captures = interval_regex.captures(s).ok_or_else(error)?;
    let negated = negated ^ (captures.name("sign").map(|s| s.as_str()) == Some("-"));
    let years: i64 = extract_match(&captures, "year", error)?.unwrap_or(0);
    let months: i64 = extract_match(&captures, "month", error)?.unwrap_or(0);
    // Apply the sign before narrowing, so the minimum of -2^31 months fits.
    let magnitude = years
        .checked_mul(12)
        .ok_or_else(error)?
        .checked_add(months)
        .ok_or_else(error)?;
    let n = if negated { -magnitude } else { magnitude };
    Ok(IntervalValue::YearMonth {
        months: i32::try_from(n).map_err(|_| error())?,
        start_field,
        end_field,
    })
}

fn parse_interval_day_time_string(
    s: &str,
    negated: bool,
    interval_regex: &Regex,
    start_field: spec::IntervalFieldType,
    end_field: spec::IntervalFieldType,
) -> SqlResult<IntervalValue> {
    let error = || SqlError::invalid(format!("interval: {s}"));
    let captures = interval_regex.captures(s).ok_or_else(error)?;
    let negated = negated ^ (captures.name("sign").map(|s| s.as_str()) == Some("-"));
    let days: i64 = extract_match(&captures, "day", error)?.unwrap_or(0);
    let hours: i64 = extract_match(&captures, "hour", error)?.unwrap_or(0);
    let minutes: i64 = extract_match(&captures, "minute", error)?.unwrap_or(0);
    let seconds: i64 = extract_match(&captures, "second", error)?.unwrap_or(0);
    let microseconds: i64 = extract_fraction_match(&captures, "fraction", 6, error)?.unwrap_or(0);
    // Sum in i128, which cannot overflow for i64 fields, and apply the sign before narrowing,
    // so the minimum of -2^63 microseconds fits.
    let magnitude = i128::from(days) * 86_400_000_000
        + i128::from(hours) * 3_600_000_000
        + i128::from(minutes) * 60_000_000
        + i128::from(seconds) * 1_000_000
        + i128::from(microseconds);
    let n = if negated { -magnitude } else { magnitude };
    Ok(IntervalValue::Microsecond {
        microseconds: i64::try_from(n).map_err(|_| error())?,
        start_field,
        end_field,
    })
}

enum StandardIntervalKind {
    Year,
    YearToMonth,
    Month,
    Day,
    DayToHour,
    DayToMinute,
    DayToSecond,
    Hour,
    HourToMinute,
    HourToSecond,
    Minute,
    MinuteToSecond,
    Second,
}

fn from_ast_interval_qualifier(qualifier: IntervalQualifier) -> SqlResult<StandardIntervalKind> {
    match qualifier {
        IntervalQualifier::YearMonth(IntervalYearMonthUnit::Year(_), None) => {
            Ok(StandardIntervalKind::Year)
        }
        IntervalQualifier::YearMonth(
            IntervalYearMonthUnit::Year(_),
            Some((_, IntervalYearMonthUnit::Month(_))),
        ) => Ok(StandardIntervalKind::YearToMonth),
        IntervalQualifier::YearMonth(IntervalYearMonthUnit::Month(_), None) => {
            Ok(StandardIntervalKind::Month)
        }
        IntervalQualifier::DayTime(IntervalDayTimeUnit::Day(_), None) => {
            Ok(StandardIntervalKind::Day)
        }
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Day(_),
            Some((_, IntervalDayTimeUnit::Hour(_))),
        ) => Ok(StandardIntervalKind::DayToHour),
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Day(_),
            Some((_, IntervalDayTimeUnit::Minute(_))),
        ) => Ok(StandardIntervalKind::DayToMinute),
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Day(_),
            Some((_, IntervalDayTimeUnit::Second(_))),
        ) => Ok(StandardIntervalKind::DayToSecond),
        IntervalQualifier::DayTime(IntervalDayTimeUnit::Hour(_), None) => {
            Ok(StandardIntervalKind::Hour)
        }
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Hour(_),
            Some((_, IntervalDayTimeUnit::Minute(_))),
        ) => Ok(StandardIntervalKind::HourToMinute),
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Hour(_),
            Some((_, IntervalDayTimeUnit::Second(_))),
        ) => Ok(StandardIntervalKind::HourToSecond),
        IntervalQualifier::DayTime(IntervalDayTimeUnit::Minute(_), None) => {
            Ok(StandardIntervalKind::Minute)
        }
        IntervalQualifier::DayTime(
            IntervalDayTimeUnit::Minute(_),
            Some((_, IntervalDayTimeUnit::Second(_))),
        ) => Ok(StandardIntervalKind::MinuteToSecond),
        IntervalQualifier::DayTime(IntervalDayTimeUnit::Second(_), None) => {
            Ok(StandardIntervalKind::Second)
        }
        _ => Err(SqlError::invalid("interval qualifier")),
    }
}

fn from_ast_standard_interval(
    value: Expr,
    kind: StandardIntervalKind,
    negated: bool,
) -> SqlResult<IntervalValue> {
    let signed: Signed<String> = parse_signed_value(value)?;
    let negated = signed.is_negative() ^ negated;
    let value = signed.into_inner();
    parse_standard_interval_text(&value, kind, negated)
}

fn parse_standard_interval_text(
    value: &str,
    kind: StandardIntervalKind,
    negated: bool,
) -> SqlResult<IntervalValue> {
    use spec::IntervalFieldType::{Day, Hour, Minute, Month, Second, Year};
    let (regex, start_field, end_field): (&Regex, _, _) = match kind {
        StandardIntervalKind::Year => (&INTERVAL_YEAR_REGEX, Year, Year),
        StandardIntervalKind::YearToMonth => (&INTERVAL_YEAR_TO_MONTH_REGEX, Year, Month),
        StandardIntervalKind::Month => (&INTERVAL_MONTH_REGEX, Month, Month),
        StandardIntervalKind::Day => (&INTERVAL_DAY_REGEX, Day, Day),
        StandardIntervalKind::DayToHour => (&INTERVAL_DAY_TO_HOUR_REGEX, Day, Hour),
        StandardIntervalKind::DayToMinute => (&INTERVAL_DAY_TO_MINUTE_REGEX, Day, Minute),
        StandardIntervalKind::DayToSecond => (&INTERVAL_DAY_TO_SECOND_REGEX, Day, Second),
        StandardIntervalKind::Hour => (&INTERVAL_HOUR_REGEX, Hour, Hour),
        StandardIntervalKind::HourToMinute => (&INTERVAL_HOUR_TO_MINUTE_REGEX, Hour, Minute),
        StandardIntervalKind::HourToSecond => (&INTERVAL_HOUR_TO_SECOND_REGEX, Hour, Second),
        StandardIntervalKind::Minute => (&INTERVAL_MINUTE_REGEX, Minute, Minute),
        StandardIntervalKind::MinuteToSecond => (&INTERVAL_MINUTE_TO_SECOND_REGEX, Minute, Second),
        StandardIntervalKind::Second => (&INTERVAL_SECOND_REGEX, Second, Second),
    };
    if start_field <= Month {
        parse_interval_year_month_string(value, negated, regex, start_field, end_field)
    } else {
        parse_interval_day_time_string(value, negated, regex, start_field, end_field)
    }
}

fn from_ast_multi_unit_interval(
    values: Vec<IntervalValueWithUnit>,
    negated: bool,
    rule: MultiUnitFamilyRule,
) -> SqlResult<IntervalValue> {
    let mut parts = MultiUnitInterval::default();
    for IntervalValueWithUnit { value, unit } in values {
        parts.add(UnitKind::from_ast(&unit), UnitAmount::Expression(value))?;
    }
    parts.finish(negated, rule)
}

#[derive(Default)]
struct MultiUnitInterval {
    months: i32,
    delta: TimeDelta,
    // The first and last written field of each family.
    year_month_fields: Option<(spec::IntervalFieldType, spec::IntervalFieldType)>,
    day_time_fields: Option<(spec::IntervalFieldType, spec::IntervalFieldType)>,
}

impl MultiUnitInterval {
    fn add(&mut self, unit: UnitKind, amount: UnitAmount<'_>) -> SqlResult<()> {
        let field = unit.field();
        let fields = if field <= spec::IntervalFieldType::Month {
            &mut self.year_month_fields
        } else {
            &mut self.day_time_fields
        };
        *fields = Some(match *fields {
            Some((start, end)) => (start.min(field), end.max(field)),
            None => (field, field),
        });
        let error = || SqlError::invalid("multi-unit interval");
        match unit {
            UnitKind::Year => {
                let value: i32 = amount.parse()?;
                let months = value.checked_mul(12).ok_or_else(error)?;
                self.months = self.months.checked_add(months).ok_or_else(error)?;
            }
            UnitKind::Month => {
                self.months = self.months.checked_add(amount.parse()?).ok_or_else(error)?;
            }
            UnitKind::Week => {
                let weeks = TimeDelta::try_weeks(amount.parse()?).ok_or_else(error)?;
                self.delta = self.delta.checked_add(&weeks).ok_or_else(error)?;
            }
            UnitKind::Day => {
                let days = TimeDelta::try_days(amount.parse()?).ok_or_else(error)?;
                self.delta = self.delta.checked_add(&days).ok_or_else(error)?;
            }
            UnitKind::Hour => {
                let hours = TimeDelta::try_hours(amount.parse()?).ok_or_else(error)?;
                self.delta = self.delta.checked_add(&hours).ok_or_else(error)?;
            }
            UnitKind::Minute => {
                let minutes = TimeDelta::try_minutes(amount.parse()?).ok_or_else(error)?;
                self.delta = self.delta.checked_add(&minutes).ok_or_else(error)?;
            }
            UnitKind::Second => {
                let value: Signed<DecimalSecond> = amount.parse()?;
                let negated = value.is_negative();
                let value = value.into_inner();
                let seconds = TimeDelta::seconds(value.seconds as i64);
                let microseconds = TimeDelta::microseconds(value.microseconds as i64);
                if negated {
                    self.delta = self.delta.checked_sub(&seconds).ok_or_else(error)?;
                    self.delta = self.delta.checked_sub(&microseconds).ok_or_else(error)?;
                } else {
                    self.delta = self.delta.checked_add(&seconds).ok_or_else(error)?;
                    self.delta = self.delta.checked_add(&microseconds).ok_or_else(error)?;
                }
            }
            UnitKind::Millisecond => {
                let milliseconds =
                    TimeDelta::try_milliseconds(amount.parse()?).ok_or_else(error)?;
                self.delta = self.delta.checked_add(&milliseconds).ok_or_else(error)?;
            }
            UnitKind::Microsecond => {
                let microseconds = TimeDelta::microseconds(amount.parse()?);
                self.delta = self.delta.checked_add(&microseconds).ok_or_else(error)?;
            }
        }
        Ok(())
    }

    fn finish(self, negated: bool, rule: MultiUnitFamilyRule) -> SqlResult<IntervalValue> {
        use spec::IntervalFieldType::{Day, Second};
        let Self {
            months,
            delta,
            year_month_fields,
            day_time_fields,
        } = self;
        let error = || SqlError::invalid("multi-unit interval");
        let (is_year_month, is_day_time) = match rule {
            // Unit presence preserves the family even when values are zero or cancel.
            MultiUnitFamilyRule::WrittenUnits => {
                (year_month_fields.is_some(), day_time_fields.is_some())
            }
            MultiUnitFamilyRule::NonZeroValues => (months != 0, delta != TimeDelta::zero()),
        };
        match (is_year_month, is_day_time) {
            (true, false) => {
                let n = if negated {
                    months.checked_mul(-1).ok_or_else(error)?
                } else {
                    months
                };
                // Both rules report a year-month interval only when a year or month unit was written.
                let (start_field, end_field) = year_month_fields.ok_or_else(error)?;
                Ok(IntervalValue::YearMonth {
                    months: n,
                    start_field,
                    end_field,
                })
            }
            (true, true) => {
                let days = delta.num_days();
                let remainder = delta - chrono::Duration::days(days);
                let microseconds = remainder.num_microseconds().ok_or_else(error)?;

                let months = if negated {
                    months.checked_mul(-1).ok_or_else(error)?
                } else {
                    months
                };
                let days = if negated {
                    days.checked_mul(-1).ok_or_else(error)?
                } else {
                    days
                };
                let days = i32::try_from(days).map_err(|_| {
                    SqlError::invalid(format!("Days value out of range for i32: {days}"))
                })?;
                let microseconds = if negated {
                    microseconds.checked_mul(-1).ok_or_else(error)?
                } else {
                    microseconds
                };
                let nanoseconds = microseconds * 1_000;

                Ok(IntervalValue::MonthDayNanosecond {
                    months,
                    days,
                    nanoseconds,
                })
            }
            (false, _) => {
                let microseconds = delta.num_microseconds().ok_or_else(error)?;
                let n = if negated {
                    microseconds.checked_mul(-1).ok_or_else(error)?
                } else {
                    microseconds
                };
                // With non-zero values deciding, only zero year-month units may have been
                // written, as in `'0 years 0 months'`. That zero day-time value covers the full
                // day-time range.
                let (start_field, end_field) = day_time_fields.unwrap_or((Day, Second));
                Ok(IntervalValue::Microsecond {
                    microseconds: n,
                    start_field,
                    end_field,
                })
            }
        }
    }
}

fn interval_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\r' | '\n')
}

/// Try a plain numeric unit string or an unescaped quoted string in a cast batch.
pub(crate) fn parse_batch_interval(s: &str) -> Option<SqlResult<IntervalValue>> {
    let text = s.trim_matches(interval_space);
    if let Some(decoded) = text
        .strip_prefix('\'')
        .and_then(|text| text.strip_suffix('\''))
        && decoded.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || b"-.".contains(&byte)
                || interval_space(char::from(byte))
        })
    {
        return Some(parse_interval_string(
            decoded,
            false,
            MultiUnitFamilyRule::NonZeroValues,
        ));
    }
    parse_numeric_unit_interval(text).map(Ok)
}

/// Parse complete unqualified numeric unit strings without constructing syntax nodes.
/// Rejections leave validation and diagnostics to the existing parser.
fn parse_numeric_unit_interval(s: &str) -> Option<IntervalValue> {
    fn digits(s: &str) -> bool {
        !s.is_empty() && s.bytes().all(|byte| byte.is_ascii_digit())
    }

    fn amount(s: &str) -> Option<&str> {
        let magnitude = s.strip_prefix('-').unwrap_or(s);
        let valid = match magnitude.split_once('.') {
            Some((whole, fraction)) => digits(whole) && digits(fraction),
            None => digits(magnitude),
        };
        valid.then_some(s)
    }

    let mut terms = s
        .split(interval_space)
        .filter(|term| !term.is_empty())
        .peekable();
    let first = terms.next()?;
    let first = if first.eq_ignore_ascii_case("interval") {
        terms.next()?
    } else {
        first
    };
    let first = amount(first)?;
    let unit = UnitKind::from_text(terms.next()?)?;
    if terms.peek().is_none()
        && let Some(kind) = unit.standard()
    {
        return parse_standard_interval_text(first, kind, false).ok();
    }
    let mut parts = MultiUnitInterval::default();
    parts.add(unit, UnitAmount::Text(first)).ok()?;
    while let Some(text) = terms.next() {
        let unit = UnitKind::from_text(terms.next()?)?;
        parts.add(unit, UnitAmount::Text(amount(text)?)).ok()?;
    }
    parts.finish(false, MultiUnitFamilyRule::NonZeroValues).ok()
}

/// Parses an interval string converted at run time, such as a string cast or a `window`
/// duration. Unlike a SQL literal, a multi-unit string chooses its family by non-zero values.
pub(crate) fn parse_unqualified_interval_string(
    s: &str,
    negated: bool,
) -> SqlResult<IntervalValue> {
    parse_interval_string(s, negated, MultiUnitFamilyRule::NonZeroValues)
}

pub(crate) fn from_ast_unqualified_interval(interval: IntervalLiteral) -> SqlResult<IntervalValue> {
    from_ast_signed_interval_with_rule(
        Signed::Positive(interval.value),
        MultiUnitFamilyRule::NonZeroValues,
    )
}

fn parse_interval_string(
    s: &str,
    negated: bool,
    rule: MultiUnitFamilyRule,
) -> SqlResult<IntervalValue> {
    // Count each decoded string scope, including the direct path within that scope.
    #[cfg(test)]
    crate::parser::INTERVAL_LITERAL_CALLS.with(|calls| calls.set(calls.get() + 1));
    if !negated
        && matches!(rule, MultiUnitFamilyRule::NonZeroValues)
        && let Some(value) = parse_numeric_unit_interval(s)
    {
        return Ok(value);
    }
    let IntervalLiteral {
        interval: _,
        value: interval,
    } = parse_interval_literal(s)?;
    let value = if negated {
        Signed::Negative(interval)
    } else {
        Signed::Positive(interval)
    };
    from_ast_signed_interval_with_rule(value, rule)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses an interval the way a SQL literal does, choosing its family by written units.
    fn parse_sql_interval_literal(s: &str) -> SqlResult<IntervalValue> {
        parse_interval_string(s, false, MultiUnitFamilyRule::WrittenUnits)
    }

    #[test]
    fn sql_literals_reject_mixed_units_while_run_time_strings_use_non_zero_values() -> SqlResult<()>
    {
        use spec::IntervalFieldType::{Day, Hour, Month, Second};
        const DAY: i64 = 86_400_000_000;
        const HOUR: i64 = 3_600_000_000;
        let calendar = |months, days, nanoseconds| IntervalValue::MonthDayNanosecond {
            months,
            days,
            nanoseconds,
        };
        let day_time = |microseconds, start_field, end_field| IntervalValue::Microsecond {
            microseconds,
            start_field,
            end_field,
        };
        // Run-time strings, as used by string casts and `window` durations, keep the family of
        // their non-zero values. Written units from both families make a SQL literal a
        // calendar interval, which the SQL expression rejects, as Spark does.
        for (sql, run_time, written_units) in [
            ("1 MONTH 1 DAY", calendar(1, 1, 0), calendar(1, 1, 0)),
            ("0 MONTHS 0 DAYS", day_time(0, Day, Day), calendar(0, 0, 0)),
            (
                "1 YEAR -12 MONTHS 0 HOURS",
                day_time(0, Hour, Hour),
                calendar(0, 0, 0),
            ),
            (
                "0 MONTHS 5 DAYS",
                day_time(5 * DAY, Day, Day),
                calendar(0, 5, 0),
            ),
            (
                "0 MONTHS 1 HOUR",
                day_time(HOUR, Hour, Hour),
                calendar(0, 0, HOUR * 1_000),
            ),
            (
                "1 MONTH 0 DAYS",
                IntervalValue::YearMonth {
                    months: 1,
                    start_field: Month,
                    end_field: Month,
                },
                calendar(1, 0, 0),
            ),
        ] {
            assert_eq!(crate::parser::parse_interval(sql)?, run_time, "{sql}");
            assert_eq!(parse_sql_interval_literal(sql)?, written_units, "{sql}");
            let statement = crate::parser::parse_one_statement(&format!("SELECT INTERVAL {sql}"))?;
            let result = crate::statement::from_ast_statement(statement);
            assert!(
                result.is_err_and(|error| error.to_string().contains("_LEGACY_ERROR_TEMP_0029")),
                "{sql}"
            );
        }
        // Zero or cancelled year-month units leave a run-time string with no non-zero value, so
        // it stays the zero day-time interval it was before written units were tracked.
        for sql in ["0 YEARS 0 MONTHS", "1 YEAR -12 MONTHS"] {
            assert_eq!(
                crate::parser::parse_interval(sql)?,
                day_time(0, Day, Second),
                "{sql}"
            );
        }
        Ok(())
    }

    #[test]
    fn sql_interval_literals_preserve_fields_when_zero_or_cancelled() -> SqlResult<()> {
        use spec::IntervalFieldType::{Day, Hour, Minute, Month, Second, Year};
        for (sql, expected_start, expected_end) in [
            ("'0' YEAR", Year, Year),
            ("'0-0' YEAR TO MONTH", Year, Month),
            ("'0' MONTH", Month, Month),
            ("'0' DAY", Day, Day),
            ("'0 00' DAY TO HOUR", Day, Hour),
            ("'0 00:00' DAY TO MINUTE", Day, Minute),
            ("'0 00:00:00' DAY TO SECOND", Day, Second),
            ("'0' HOUR", Hour, Hour),
            ("'0:00' HOUR TO MINUTE", Hour, Minute),
            ("'0:00:00' HOUR TO SECOND", Hour, Second),
            ("'0' MINUTE", Minute, Minute),
            ("'0:00' MINUTE TO SECOND", Minute, Second),
            ("'0' SECOND", Second, Second),
            ("0 YEARS 0 MONTHS", Year, Month),
            ("1 YEAR -12 MONTHS", Year, Month),
            ("0 DAYS 0 HOURS", Day, Hour),
            ("1 DAY -24 HOURS", Day, Hour),
            ("0 HOURS 0 MINUTES", Hour, Minute),
            ("0 WEEKS", Day, Day),
            ("0 MILLISECONDS", Second, Second),
            ("0 MICROSECONDS", Second, Second),
        ] {
            let literal: spec::Literal = parse_sql_interval_literal(sql)?.into();
            match literal {
                spec::Literal::IntervalYearMonth {
                    months,
                    start_field,
                    end_field,
                } => {
                    assert_eq!(months, Some(0), "{sql}");
                    assert!(expected_start <= Month, "{sql}");
                    assert_eq!(
                        (start_field, end_field),
                        (Some(expected_start), Some(expected_end)),
                        "{sql}"
                    );
                }
                spec::Literal::DurationMicrosecond {
                    microseconds,
                    start_field,
                    end_field,
                } => {
                    assert_eq!(microseconds, Some(0), "{sql}");
                    assert!(expected_start >= Day, "{sql}");
                    assert_eq!(
                        (start_field, end_field),
                        (Some(expected_start), Some(expected_end)),
                        "{sql}"
                    );
                }
                other => return Err(SqlError::invalid(format!("unexpected {sql}: {other:?}"))),
            }
        }
        Ok(())
    }

    #[test]
    fn test_parse_interval() -> SqlResult<()> {
        let parse = parse_unqualified_interval_string;

        assert!(parse("178956970 year 7 month", false).is_ok());
        assert!(parse("178956970 year 7 month", true).is_ok());
        assert!(parse("178956970 year 8 month", false).is_err());
        assert!(parse("178956970 year 8 month", true).is_err());
        assert!(parse("-178956970 year -8 month", false).is_ok());
        assert!(parse("-178956970 year -8 month", true).is_err());
        assert!(parse("-178956970 year -9 month", false).is_err());
        assert!(parse("-178956970 year -9 month", true).is_err());

        assert!(parse("'178956970-7' year to month", false).is_ok());
        assert!(parse("'178956970-7' year to month", true).is_ok());
        assert!(parse("'178956970-8' year to month", false).is_err());
        // A negated magnitude one past the maximum is the minimum, -2^31 months.
        let minimum_months = |interval| matches!(interval, IntervalValue::YearMonth { months, .. } if months == i32::MIN);
        assert!(minimum_months(parse("'178956970-8' year to month", true)?));
        assert!(minimum_months(parse(
            "-'178956970-8' year to month",
            false
        )?));
        assert!(minimum_months(parse(
            "'-178956970-8' year to month",
            false
        )?));
        assert!(minimum_months(parse("'-2147483648' month", false)?));
        assert!(parse("'-2147483649' month", false).is_err());
        assert!(parse("-'178956970-8' year to month", true).is_err());
        assert!(parse("-'178956970-9' year to month", false).is_err());
        assert!(parse("-'178956970-9' year to month", true).is_err());

        assert_eq!(
            parse("'-2-1' year to month", false)?,
            parse("'2-1' year to month", true)?
        );
        assert_eq!(
            parse("'-2-1' year to month", false)?,
            parse("-'2-1' year to month", false)?
        );
        assert_eq!(
            parse("'-2-1' year to month", false)?,
            parse("-2 year -1 month", false)?
        );

        assert!(parse("106751991 day 14454775807 microsecond", false).is_ok());
        assert!(parse("106751991 day 14454775807 microsecond", true).is_ok());
        assert!(parse("106751991 day 14454775808 microsecond", false).is_err());
        assert!(parse("106751991 day 14454775808 microsecond", true).is_err());
        assert!(parse("-106751991 day -14454775808 microsecond", false).is_ok());
        assert!(parse("-106751991 day -14454775808 microsecond", true).is_err());
        assert!(parse("-106751991 day -14454775809 microsecond", false).is_err());
        assert!(parse("-106751991 day -14454775809 microsecond", true).is_err());

        assert!(parse("'106751991 04:00:54.775807' day to second", false).is_ok());
        assert!(parse("'106751991 04:00:54.775807' day to second", true).is_ok());
        assert!(parse("'106751991 04:00:54.775808' day to second", false).is_err());
        // A negated magnitude one past the maximum is the minimum, -2^63 microseconds.
        let minimum_microseconds = |interval| matches!(interval, IntervalValue::Microsecond { microseconds, .. } if microseconds == i64::MIN);
        assert!(minimum_microseconds(parse(
            "'106751991 04:00:54.775808' day to second",
            true
        )?));
        assert!(minimum_microseconds(parse(
            "-'106751991 04:00:54.775808' day to second",
            false
        )?));
        assert!(minimum_microseconds(parse(
            "'-9223372036854.775808' second",
            false
        )?));
        assert!(parse("'-9223372036854.775809' second", false).is_err());
        assert!(parse("-'106751991 04:00:54.775808' day to second", true).is_err());
        assert!(parse("-'106751991 04:00:54.775809' day to second", false).is_err());
        assert!(parse("-'106751991 04:00:54.775809' day to second", true).is_err());
        // Spark rejects these with a java.lang.ArithmeticException that has no error condition.
        assert!(parse("'106751991 05' day to hour", false).is_err());
        assert!(parse("'-106751991 04:00:54.775809' day to second", false).is_err());

        assert_eq!(
            parse("'-1 2:3:4.567890' day to second", false)?,
            parse("'1 2:3:4.567890' day to second", true)?
        );
        assert_eq!(
            parse("'-1 2:3:4.567890' day to second", false)?,
            parse("-'1 2:3:4.567890' day to second", false)?
        );
        assert_eq!(
            parse("'-1 2:3:4.567890' day to second", false)?,
            parse(
                "-1 day -2 hour -3 minute -4 second -567 millisecond -890 microsecond",
                false
            )?
        );
        Ok(())
    }

    #[test]
    fn test_parse_unqualified_interval_string() -> SqlResult<()> {
        assert!(parse_unqualified_interval_string("1", false).is_err());
        assert!(parse_unqualified_interval_string("1 month", false).is_ok());
        assert_eq!(
            parse_unqualified_interval_string("1 month", true)?,
            parse_unqualified_interval_string("-1 month", false)?
        );
        assert_eq!(
            parse_unqualified_interval_string("1 hour 2 seconds", false)?,
            parse_unqualified_interval_string("-1 hour -2 seconds", true)?
        );
        Ok(())
    }
}
