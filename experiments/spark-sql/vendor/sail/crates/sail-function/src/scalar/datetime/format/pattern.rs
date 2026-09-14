// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use datafusion_common::Result;

use super::parser::parse_datetime_pattern;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DateTimeFormat {
    pub(crate) items: Vec<DateTimeItem>,
}

impl DateTimeFormat {
    pub fn for_parsing(pattern: &str) -> Result<Self> {
        parse_datetime_pattern(pattern, PatternUse::Parsing)
    }

    pub fn for_formatting(pattern: &str) -> Result<Self> {
        parse_datetime_pattern(pattern, PatternUse::Formatting)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PatternUse {
    Parsing,
    Formatting,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DateTimeItem {
    Literal(String),
    Field(DateTimeFieldSpec),
    Fraction(FractionSpec),
    Zone(ZoneSpec),
    Optional(Vec<DateTimeItem>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct DateTimeFieldSpec {
    pub(crate) kind: DateTimeField,
    pub(crate) width: usize,
    pub(crate) style: FieldStyle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DateTimeField {
    Era,
    YearOfEra,
    QuarterOfYear,
    MonthOfYear,
    DayOfMonth,
    DayOfYear,
    DayOfWeek,
    AlignedWeekOfMonth,
    AmPmOfDay,
    ClockHourOfAmPm,
    HourOfAmPm,
    ClockHourOfDay,
    HourOfDay,
    MinuteOfHour,
    SecondOfMinute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum FieldStyle {
    Numeric,
    TextShort,
    TextFull,
    LocalizedNumeric,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FractionSpec {
    pub(crate) min_width: usize,
    pub(crate) max_width: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ZoneSpec {
    pub(crate) kind: ZoneField,
    pub(crate) width: usize,
    pub(crate) zero_as_z: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ZoneField {
    IsoOffset,
    Rfc822Offset,
    LocalizedOffset,
    ZoneId,
    ZoneName,
}
