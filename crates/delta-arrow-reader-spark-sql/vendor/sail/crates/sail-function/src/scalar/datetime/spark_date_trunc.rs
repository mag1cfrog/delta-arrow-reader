use std::sync::Arc;

use chrono::{DateTime, LocalResult, Offset, TimeDelta, TimeZone};
use chrono_tz::{GapInfo, Tz};
use datafusion::arrow::array::types::TimestampMicrosecondType;
use datafusion::arrow::array::{AsArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, FieldRef, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::datetime::date_trunc::DateTruncFunc;

use crate::scalar::datetime::local_time::{MICROS_PER_DAY, TimestampZone, ZoneKind, local_micros};
use crate::scalar::datetime::spark_date_to_timestamp::{SessionZone, start_of_day};
use crate::scalar::datetime::spark_trunc_date::TruncUnit;

const MICROS_PER_MINUTE: i128 = 60_000_000;
const MICROS_PER_HOUR: i128 = 3_600_000_000;

/// A unit Spark's DATE_TRUNC supports, as `date_trunc` spells it once the plan has mapped
/// Spark's other spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TruncLevel {
    Microsecond,
    Millisecond,
    Second,
    /// Java's `ZonedDateTime.truncatedTo` to a unit of this many microseconds: MINUTE, HOUR or
    /// DAY.
    Local(i128),
    /// The first day of the local date's week, month, quarter or year, at its start.
    Date(TruncUnit),
}

impl TruncLevel {
    fn parse(unit: &str) -> Option<Self> {
        Some(match unit.to_ascii_lowercase().as_str() {
            "microsecond" => Self::Microsecond,
            "millisecond" => Self::Millisecond,
            "second" => Self::Second,
            "minute" => Self::Local(MICROS_PER_MINUTE),
            "hour" => Self::Local(MICROS_PER_HOUR),
            "day" => Self::Local(i128::from(MICROS_PER_DAY)),
            other => Self::Date(TruncUnit::parse(other)?),
        })
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateTrunc {
    inner: DateTruncFunc,
}

impl Default for SparkDateTrunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkDateTrunc {
    pub fn new() -> Self {
        Self {
            inner: DateTruncFunc::new(),
        }
    }
}

impl ScalarUDFImpl for SparkDateTrunc {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `TruncInstant.nullable = true`, unconditional.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/datetimeExpressions.scala#L2287>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [ColumnarValue::Scalar(unit), timestamp] = args.args.as_slice()
            && let Some(level) = unit.try_as_str().flatten().and_then(TruncLevel::parse)
            && let DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) = timestamp.data_type()
        {
            return truncate_in_zone(timestamp, level, &zone);
        }
        self.inner.invoke_with_args(args)
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    // A truncation in the local date-time that moves past a skipped interval can end later
    // than the truncation of a later instant, and a local date can go back across a repeated
    // interval, so only SECOND and finer units keep the input's order.
    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        let unit = input
            .first()
            .and_then(|unit| unit.range.lower().try_as_str().flatten())
            .and_then(TruncLevel::parse);
        match unit {
            Some(TruncLevel::Microsecond | TruncLevel::Millisecond | TruncLevel::Second) => {
                self.inner.output_ordering(input)
            }
            _ => Ok(SortProperties::Unordered),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

/// Spark's DATE_TRUNC in `zone`, as `DateTimeUtils.truncTimestamp` computes it on microseconds
/// across the BIGINT range. SECOND and finer units truncate the instant. MINUTE, HOUR and DAY
/// truncate the local date-time like Java's `ZonedDateTime.truncatedTo`; DataFusion truncated
/// the UTC minute, which differs where the offset has seconds, such as local mean time -07:52:58
/// in Los Angeles before 1883. WEEK, MONTH, QUARTER and YEAR truncate the local date and take its
/// start in the zone, like `LocalDate.atStartOfDay`. DataFusion worked in nanoseconds, so it
/// failed outside 1677 to 2262 (#1087).
fn truncate_in_zone(
    timestamps: &ColumnarValue,
    level: TruncLevel,
    zone: &Arc<str>,
) -> Result<ColumnarValue> {
    let (Some(parsed), Some(session)) = (TimestampZone::parse(zone), SessionZone::parse(zone))
    else {
        return exec_err!("invalid time zone {zone}");
    };
    let truncate = |micros: i64| {
        let truncated = match level {
            TruncLevel::Microsecond => Some(micros),
            TruncLevel::Millisecond => Some(micros - micros.rem_euclid(1_000)),
            TruncLevel::Second => Some(micros - micros.rem_euclid(1_000_000)),
            TruncLevel::Local(unit) => truncated_to_local_unit(micros, &parsed, unit),
            TruncLevel::Date(unit) => {
                let day =
                    local_micros(micros, &parsed.arrow).div_euclid(i128::from(MICROS_PER_DAY));
                let day = i32::try_from(day).ok();
                match day.map(|day| unit.truncate(day)).transpose()? {
                    Some(first) => {
                        start_of_day(first, &session).and_then(|s| i64::try_from(s).ok())
                    }
                    None => None,
                }
            }
        };
        match truncated {
            Some(truncated) => Ok(truncated),
            None => exec_err!("long overflow"),
        }
    };
    match timestamps {
        ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(micros, _)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                micros.map(truncate).transpose()?,
                Some(Arc::clone(zone)),
            )))
        }
        ColumnarValue::Array(array) => {
            let truncated = array
                .as_primitive::<TimestampMicrosecondType>()
                .iter()
                .map(|micros| micros.map(truncate).transpose())
                .collect::<Result<TimestampMicrosecondArray>>()?
                .with_timezone(Arc::clone(zone));
            Ok(ColumnarValue::Array(Arc::new(truncated)))
        }
        other => internal_err!("date_trunc expected a TIMESTAMP, got {}", other.data_type()),
    }
}

/// The instant Java's `truncatedTo` gives for `micros` with a unit of `unit` microseconds: the
/// start of its local minute, hour or day, keeping the instant's offset when that start repeats.
/// When an offset change skips the start, the result moves forward by the skipped interval and
/// can be later than `micros`. `None` beyond the BIGINT range.
fn truncated_to_local_unit(micros: i64, zone: &TimestampZone, unit: i128) -> Option<i64> {
    let region = match &zone.kind {
        ZoneKind::Fixed(offset) => {
            let offset = i128::from(offset.local_minus_utc()) * 1_000_000;
            let local = i128::from(micros) + offset;
            return i64::try_from(local.div_euclid(unit) * unit - offset).ok();
        }
        ZoneKind::Region(region) => region,
    };
    match region_unit_start(micros, region, unit) {
        Some(truncated) => Some(truncated),
        // Beyond the date library's years, for the instant or its local date-time, the offset
        // comes from a nearer 400-year cycle and does not change within the unit.
        None => {
            let local = local_micros(micros, &zone.arrow);
            let offset = local - i128::from(micros);
            i64::try_from(local.div_euclid(unit) * unit - offset).ok()
        }
    }
}

/// [`truncated_to_local_unit`] in a region, or `None` when the date library cannot represent
/// the instant or its local date-time.
fn region_unit_start(micros: i64, region: &Tz, unit: i128) -> Option<i64> {
    let utc = DateTime::from_timestamp_micros(micros)?.naive_utc();
    let offset = region.offset_from_utc_datetime(&utc).fix();
    let local = utc.checked_add_signed(TimeDelta::seconds(i64::from(offset.local_minus_utc())))?;
    let local_micros = i128::from(local.and_utc().timestamp_micros());
    let start =
        DateTime::from_timestamp_micros(i64::try_from(local_micros.div_euclid(unit) * unit).ok()?)?
            .naive_utc();
    let truncated = match region.from_local_datetime(&start) {
        LocalResult::Single(start) => start,
        LocalResult::Ambiguous(earlier, later) => {
            if later.offset().fix() == offset {
                later
            } else {
                earlier
            }
        }
        LocalResult::None => {
            let gap = GapInfo::new(&start, region)?;
            let (gap_start, _) = gap.begin?;
            gap.end?.checked_add_signed(start - gap_start)?
        }
    };
    Some(truncated.timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn truncate(zone: &str, micros: i64) -> Option<i64> {
        truncated_to_local_unit(micros, &TimestampZone::parse(zone)?, MICROS_PER_MINUTE)
    }

    #[test]
    fn minutes_start_at_the_local_minute() {
        let la = "America/Los_Angeles";
        // 1800-06-15 12:34:56.789123 UTC is 04:41:58.789123 local mean time, -07:52:58; its
        // minute starts at 04:41:00, 12:33:58 UTC.
        assert_eq!(
            truncate(la, -5_350_361_103_210_877),
            Some(-5_350_361_162_000_000)
        );
        // 1883-11-18 20:00:30 UTC is 12:00:30 PST; 12:00 repeats, and the minute keeps PST.
        assert_eq!(
            truncate(la, -2_717_639_970_000_000),
            Some(-2_717_640_000_000_000)
        );
        // 30 seconds after the epoch is 05:45:30 at +05:45; its minute starts at the epoch.
        assert_eq!(truncate("+05:45", 30_000_000), Some(0));
        // Java's EST is -05:00, not Panama's local mean time.
        assert_eq!(
            truncate("EST", -5_350_361_103_210_877),
            Some(-5_350_361_160_000_000)
        );
    }

    #[test]
    fn zones_arrow_rejects_are_rejected() {
        assert!(TimestampZone::parse("-00:44:30").is_none());
        assert!(TimestampZone::parse("+05:30junk").is_none());
    }

    #[test]
    fn instants_beyond_the_date_library_truncate_with_a_nearer_cycle() {
        // Spark: 9223372036800000000 in Los Angeles and UTC.
        assert_eq!(
            truncate("America/Los_Angeles", i64::MAX),
            Some(9_223_372_036_800_000_000)
        );
        assert_eq!(truncate("UTC", i64::MAX), Some(9_223_372_036_800_000_000));
        // Instants the date library holds whose local date-times it cannot hold (Spark 4.2.0).
        assert_eq!(
            truncate("Asia/Tokyo", 8_210_266_876_799_999_999),
            Some(8_210_266_876_740_000_000)
        );
        assert_eq!(
            truncate("America/Los_Angeles", -8_334_601_228_800_000_000),
            Some(-8_334_601_228_802_000_000)
        );
        assert_eq!(
            truncate("America/Los_Angeles", -8_334_601_203_600_000_000),
            Some(-8_334_601_203_602_000_000)
        );
    }
}
