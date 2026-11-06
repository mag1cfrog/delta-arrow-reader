use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use chumsky::Parser;
use chumsky::extra::ParserExtra;
use chumsky::prelude::{any, end, just, one_of};

use crate::error::{SqlError, SqlResult};

#[derive(Debug, Clone)]
pub struct DateValue {
    pub year: i32,
    pub month: u8,
    pub day: u8,
}

impl TryFrom<DateValue> for NaiveDate {
    type Error = SqlError;

    fn try_from(value: DateValue) -> SqlResult<Self> {
        NaiveDate::from_ymd_opt(value.year, value.month as u32, value.day as u32)
            .ok_or_else(|| SqlError::invalid(format!("{value:?}")))
    }
}

#[derive(Debug, Clone, Default)]
pub struct TimeValue {
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub nanoseconds: u32,
}

impl TryFrom<TimeValue> for NaiveTime {
    type Error = SqlError;

    fn try_from(value: TimeValue) -> SqlResult<Self> {
        let second = if value.second == 60 {
            if value.hour != 23 || value.minute != 59 {
                return Err(SqlError::invalid("leap second must be at 23:59:60"));
            }
            59
        } else {
            value.second
        };

        let time = NaiveTime::from_hms_nano_opt(
            value.hour as u32,
            value.minute as u32,
            second as u32,
            value.nanoseconds,
        )
        .ok_or_else(|| SqlError::invalid(format!("{value:?}")))?;

        Ok(time)
    }
}

#[derive(Debug, Clone)]
pub struct TimestampValue<'a> {
    pub date: DateValue,
    pub time: TimeValue,
    pub timezone: &'a str,
}

impl<'a> TimestampValue<'a> {
    pub fn into_naive(self) -> SqlResult<(NaiveDateTime, &'a str)> {
        let Self {
            date,
            time,
            timezone,
        } = self;

        let is_leap_second = time.second == 60;
        let date = NaiveDate::try_from(date)?;
        let time = NaiveTime::try_from(time)?;
        let datetime = date.and_time(time);

        let datetime = if is_leap_second {
            datetime + chrono::Duration::seconds(1)
        } else {
            datetime
        };

        Ok((datetime, timezone))
    }
}

#[expect(clippy::unwrap_used)]
fn signed<'a, T, E>(min_digits: usize, max_digits: usize) -> impl Parser<'a, &'a str, T, E>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Debug,
    E: ParserExtra<'a, &'a str> + 'a,
{
    one_of(['-', '+'])
        .or_not()
        .then(
            one_of('0'..='9')
                .repeated()
                .at_least(min_digits)
                .at_most(max_digits),
        )
        .to_slice()
        .map(|s: &str| s.parse::<T>().unwrap())
}

#[expect(clippy::unwrap_used)]
fn unsigned<'a, T, E>(min_digits: usize, max_digits: usize) -> impl Parser<'a, &'a str, T, E>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Debug,
    E: ParserExtra<'a, &'a str> + 'a,
{
    one_of('0'..='9')
        .repeated()
        .at_least(min_digits)
        .at_most(max_digits)
        .to_slice()
        .map(|s: &str| s.parse::<T>().unwrap())
}

/// Parses fractional seconds with configurable digit range.
///
/// The parser accepts up to 9 digits (nanoseconds) for compatibility with Spark SQL syntax,
/// but Spark stores timestamps with microsecond precision (6 digits). Nanosecond part is
/// truncated during conversion to match Spark behavior.
#[expect(clippy::unwrap_used)]
fn fraction<'a, T, E>(min_digits: usize, max_digits: usize) -> impl Parser<'a, &'a str, T, E>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Debug,
    E: ParserExtra<'a, &'a str> + 'a,
{
    one_of('0'..='9')
        .repeated()
        .at_least(min_digits)
        .at_most(max_digits)
        .to_slice()
        .map(move |s: &str| {
            s.chars()
                .chain(std::iter::repeat('0'))
                .take(max_digits)
                .collect::<String>()
                .parse::<T>()
                .unwrap()
        })
}

/// Parses a complete or partial date.
///
/// `day_suffix` runs only after a complete year-month-day date. Its output is
/// `None` for a partial date. The suffix must always succeed, for example by
/// ending in `.or_not()`; a failing suffix would discard the parsed day.
fn date<'a, O, E>(
    day_suffix: impl Parser<'a, &'a str, O, E>,
) -> impl Parser<'a, &'a str, (DateValue, Option<O>), E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    let year = signed(4, 7);
    let month = unsigned(1, 2);
    let day = unsigned(1, 2).then(day_suffix);

    year.then(just('-').ignore_then(month).or_not())
        .then(just('-').ignore_then(day).or_not())
        .map(|((year, month), day)| {
            let (day, suffix) = day.unzip();
            let date = DateValue {
                year,
                month: month.unwrap_or(1),
                day: day.unwrap_or(1),
            };
            (date, suffix)
        })
}

fn time<'a, E>() -> impl Parser<'a, &'a str, TimeValue, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    let hour = unsigned(1, 2);
    let minute = unsigned(1, 2);
    let second = unsigned(1, 2);
    let nanoseconds = fraction(0, 9);

    hour.then(just(':').ignore_then(minute).or_not())
        .then(just(':').ignore_then(second).or_not())
        .then(just('.').ignore_then(nanoseconds).or_not())
        .map(|(((hour, minute), second), nanoseconds)| TimeValue {
            hour,
            minute: minute.unwrap_or(0),
            second: second.unwrap_or(0),
            nanoseconds: nanoseconds.unwrap_or(0),
        })
}

fn timezone<'a, E>() -> impl Parser<'a, &'a str, &'a str, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    any().repeated().to_slice().map(|s: &str| {
        let s = s.trim_matches(' ');
        if s.eq_ignore_ascii_case("Z") {
            "UTC"
        } else if s.starts_with("UTC+") || s.starts_with("UTC-") {
            // Convert "UTC+xx:xx" to "+xx:xx" and "UTC-xx:xx" to "-xx:xx"
            &s[3..]
        } else {
            s
        }
    })
}

fn timestamp<'a, E>() -> impl Parser<'a, &'a str, TimestampValue<'a>, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    // Like Spark, exactly one space or uppercase T separates a time from a complete date.
    let time_suffix = one_of([' ', 'T'])
        .ignore_then(time().then(timezone()))
        .or_not();
    date(time_suffix)
        .then_ignore(just(' ').repeated())
        .map(|(date, time_and_timezone)| {
            let (time, timezone) = time_and_timezone.flatten().unwrap_or_default();
            TimestampValue {
                date,
                time,
                timezone,
            }
        })
}

pub fn create_date_parser<'a, E>() -> impl Parser<'a, &'a str, DateValue, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    // Only a complete date may ignore a suffix starting with space or uppercase T.
    date(one_of([' ', 'T']).then(any().repeated()).or_not().ignored())
        .map(|(date, _)| date)
        .then_ignore(end())
}

pub fn create_timestamp_parser<'a, E>() -> impl Parser<'a, &'a str, TimestampValue<'a>, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    timestamp().then_ignore(end())
}

pub fn create_time_parser<'a, E>() -> impl Parser<'a, &'a str, TimeValue, E>
where
    E: ParserExtra<'a, &'a str> + 'a,
{
    time().then_ignore(end())
}
