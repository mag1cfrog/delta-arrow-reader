// Local Spark DATE to TIMESTAMP conversion. See the host crate's UPSTREAM.md.
//! Spark's conversion from DATE to TIMESTAMP in the session time zone: the start of each day, as
//! Java's `LocalDate.atStartOfDay(zone)` gives it. When a DST change skips midnight, the day
//! starts at the end of the skipped interval; when it repeats midnight, at the earlier instant.
//! In UTC it also gives Spark's DATE to TIMESTAMP_NTZ cast, the midnight wall-clock time, which
//! `with_nullable_result` declares nullable as Spark does.

use std::sync::Arc;

use chrono::{LocalResult, NaiveDate, TimeZone};
use chrono_tz::{GapInfo, Tz};
use datafusion::arrow::array::types::Date32Type;
use datafusion::arrow::array::{AsArray, TimestampMicrosecondArray};
use datafusion::arrow::compute::kernels::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::scalar::datetime::convert_tz::ResolvedTimeZone;

/// Gregorian dates repeat every 400 years (146,097 days), and so do recurring time zone rules.
const DAYS_PER_400_YEARS: i64 = 146_097;
const MICROS_PER_DAY: i128 = 86_400_000_000;
const MICROS_PER_400_YEARS: i128 = 146_097 * 86_400_000_000;
/// The days from 0001-01-01 to 1970-01-01.
const UNIX_EPOCH_FROM_CE: i64 = 719_163;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateToTimestamp {
    session_timezone: Arc<str>,
    /// NULL for a start of day beyond the BIGINT range, as under TRY_CAST or inside TRY.
    null_on_overflow: bool,
    /// Spark declares its cast of a DATE to TIMESTAMP_NTZ nullable, unlike the cast to
    /// TIMESTAMP.
    nullable_result: bool,
    signature: Signature,
}

impl SparkDateToTimestamp {
    pub fn new(session_timezone: Arc<str>, null_on_overflow: bool) -> Self {
        Self {
            session_timezone,
            null_on_overflow,
            nullable_result: false,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }

    /// The same conversion, declared nullable whatever its argument declares.
    pub fn with_nullable_result(self) -> Self {
        Self {
            nullable_result: true,
            ..self
        }
    }

    fn zone(&self) -> Result<ResolvedTimeZone> {
        match ResolvedTimeZone::parse(&self.session_timezone) {
            Some(zone) => Ok(zone),
            None => exec_err!(
                "[INVALID_TIMEZONE] The timezone: {:?} is invalid.",
                self.session_timezone
            ),
        }
    }

    /// The start of `day`, in microseconds, which Spark cannot store beyond the BIGINT range.
    fn convert(&self, day: i32, zone: &ResolvedTimeZone) -> Result<Option<i64>> {
        match start_of_day(day, zone).and_then(|micros| i64::try_from(micros).ok()) {
            Some(micros) => Ok(Some(micros)),
            None if self.null_on_overflow => Ok(None),
            // Java's exact arithmetic message.
            None => exec_err!("long overflow"),
        }
    }

    /// Convert DATE values, which may be a scalar or an array of either DATE encoding.
    pub fn convert_dates(&self, value: &ColumnarValue) -> Result<ColumnarValue> {
        let zone = self.zone()?;
        let timestamp = |micros| {
            ScalarValue::TimestampMicrosecond(micros, Some(Arc::clone(&self.session_timezone)))
        };
        match value {
            ColumnarValue::Scalar(scalar) => {
                let days = scalar.cast_to(&DataType::Date32)?;
                let ScalarValue::Date32(day) = days else {
                    return internal_err!("expected a DATE, got {}", scalar.data_type());
                };
                let micros = match day {
                    Some(day) => self.convert(day, &zone)?,
                    None => None,
                };
                Ok(ColumnarValue::Scalar(timestamp(micros)))
            }
            ColumnarValue::Array(array) => {
                let days = cast::cast(array, &DataType::Date32)?;
                let micros = days
                    .as_primitive::<Date32Type>()
                    .iter()
                    .map(|day| match day {
                        Some(day) => self.convert(day, &zone),
                        None => Ok(None),
                    })
                    .collect::<Result<TimestampMicrosecondArray>>()?
                    .with_timezone(Arc::clone(&self.session_timezone));
                Ok(ColumnarValue::Array(Arc::new(micros)))
            }
        }
    }
}

/// The first instant of `day` in `zone`, beyond the BIGINT range for the extreme days.
pub(crate) fn start_of_day(day: i32, zone: &ResolvedTimeZone) -> Option<i128> {
    match zone {
        ResolvedTimeZone::Fixed(offset) => Some(
            i128::from(day) * MICROS_PER_DAY - i128::from(offset.local_minus_utc()) * 1_000_000,
        ),
        ResolvedTimeZone::Region(region) => region_start_of_day(day, region),
    }
}

/// The first instant of `day` in a region. A day beyond the date library's years takes its
/// offsets from the same day in a nearer 400-year cycle, whose DST after 2099 the date library
/// omits (#1052).
fn region_start_of_day(day: i32, zone: &Tz) -> Option<i128> {
    let mut probe = i64::from(day);
    let mut cycles: i128 = 0;
    let date = loop {
        let from_ce = i32::try_from(probe + UNIX_EPOCH_FROM_CE).ok();
        if let Some(date) = from_ce.and_then(NaiveDate::from_num_days_from_ce_opt) {
            break date;
        }
        if probe > 0 {
            probe -= DAYS_PER_400_YEARS;
            cycles += 1;
        } else {
            probe += DAYS_PER_400_YEARS;
            cycles -= 1;
        }
    };
    let midnight = date.and_hms_opt(0, 0, 0)?;
    let start = match zone.from_local_datetime(&midnight) {
        LocalResult::Single(start) | LocalResult::Ambiguous(start, _) => start,
        // Midnight is skipped: the day starts where the skipped interval ends.
        LocalResult::None => GapInfo::new(&midnight, zone)?.end?,
    };
    Some(i128::from(start.timestamp_micros()) + cycles * MICROS_PER_400_YEARS)
}

impl ScalarUDFImpl for SparkDateToTimestamp {
    fn name(&self) -> &str {
        "spark_date_to_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Date32 | DataType::Date64] => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some(Arc::clone(&self.session_timezone)),
            )),
            _ => internal_err!("{} requires a DATE argument", self.name()),
        }
    }

    // Spark folds a constant conversion during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        true
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return internal_err!("{} requires one argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&[field.data_type().clone()])?,
            self.nullable_result || field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return internal_err!("{} requires one argument", self.name());
        };
        if !matches!(value.data_type(), DataType::Date32 | DataType::Date64) {
            return internal_err!("{} requires a DATE argument", self.name());
        }
        self.convert_dates(value)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn day(text: &str) -> i32 {
        let date = NaiveDate::from_str(text).unwrap_or_default();
        date.signed_duration_since(NaiveDate::default()).num_days() as i32
    }

    fn start(zone: &str, text: &str) -> Option<i128> {
        start_of_day(day(text), &ResolvedTimeZone::parse(zone)?)
    }

    #[test]
    fn days_start_at_their_first_local_instant() {
        // Havana skips 00:00-01:00 on 2024-03-10: the day starts at 01:00 CDT, 05:00 UTC.
        assert_eq!(
            start("America/Havana", "2024-03-10"),
            Some(1_710_046_800_000_000)
        );
        // Havana repeats 00:00-01:00 on 2024-11-03: the earlier midnight, 04:00 UTC.
        assert_eq!(
            start("America/Havana", "2024-11-03"),
            Some(1_730_606_400_000_000)
        );
        // Toronto skipped 23:30-00:30 on 1919-03-30: the day starts at 00:30.
        assert_eq!(
            start("America/Toronto", "1919-03-31"),
            Some(-1_601_753_400_000_000)
        );
        assert_eq!(start("UTC", "1970-01-01"), Some(0));
        assert_eq!(start("+05:30", "1970-01-01"), Some(-19_800_000_000));
        assert_eq!(start("-03", "1970-01-01"), Some(10_800_000_000));
        // A legacy three-letter ID names a region.
        assert_eq!(start("PST", "2024-03-10"), Some(1_710_057_600_000_000));
    }

    #[test]
    fn days_beyond_the_date_library_take_a_nearer_cycle() {
        let zone = Tz::UTC;
        let near = region_start_of_day(0, &zone);
        let far = region_start_of_day(146_097 * 700, &zone);
        assert_eq!(far, near.map(|micros| micros + 700 * MICROS_PER_400_YEARS));
    }

    #[test]
    fn starts_beyond_bigint_overflow() -> Result<()> {
        let utc = ResolvedTimeZone::Region(Tz::UTC);
        let conversion = SparkDateToTimestamp::new("UTC".into(), false);
        assert!(conversion.convert(i32::MAX, &utc).is_err());
        let conversion = SparkDateToTimestamp::new("UTC".into(), true);
        assert_eq!(conversion.convert(i32::MAX, &utc)?, None);
        Ok(())
    }
}
