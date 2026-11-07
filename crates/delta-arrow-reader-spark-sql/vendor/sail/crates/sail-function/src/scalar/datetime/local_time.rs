// Local Spark time zone offsets. See the host crate's UPSTREAM.md.
//! The local date-time of an instant in a time zone across Spark's whole BIGINT microsecond
//! range, for TIMESTAMPDIFF and the TIMESTAMP to TIMESTAMP_NTZ conversion.

use chrono::{DateTime, Offset, TimeZone};
use datafusion::arrow::array::timezone::Tz;

/// Gregorian dates repeat every 400 years (146,097 days, a whole number of weeks), and so do
/// fixed offsets and recurring time zone rules.
const MICROS_PER_400_YEARS: i64 = 146_097 * 86_400_000_000;

/// The local wall-clock microseconds of an instant in `zone`, beyond the BIGINT range when
/// the offset moves an extreme instant past it. An instant beyond the date library's years
/// takes the offset of the same position in a nearer 400-year cycle.
pub(crate) fn local_micros(micros: i64, zone: &Tz) -> i128 {
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
