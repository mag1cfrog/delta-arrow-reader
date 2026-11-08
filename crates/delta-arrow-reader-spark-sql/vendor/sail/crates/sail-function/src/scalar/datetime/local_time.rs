// Local Spark time zone offsets. See the host crate's UPSTREAM.md.
//! Local date-times of instants in a time zone across Spark's whole BIGINT microsecond range:
//! the offset of an instant, and Java's `ZonedDateTime` arithmetic on local date-times for
//! TIMESTAMP interval arithmetic, TIMESTAMPADD and TIMESTAMP subtraction.

use std::str::FromStr;

use chrono::{DateTime, FixedOffset, LocalResult, Months, NaiveDateTime, Offset, TimeZone};
use chrono_tz::{GapInfo, Tz};
use datafusion::arrow::array::timezone::Tz as ArrowTz;

/// Gregorian dates repeat every 400 years (146,097 days, a whole number of weeks), and so do
/// fixed offsets and recurring time zone rules.
pub(crate) const MICROS_PER_400_YEARS: i64 = 146_097 * 86_400_000_000;
pub(crate) const MICROS_PER_DAY: i64 = 86_400_000_000;
/// 400 years of months.
const MONTHS_PER_400_YEARS: i64 = 4_800;
/// Local date-times within 250,000 years of the epoch, inside the date library's years with
/// room for 400 more, are resolved where they are; farther ones in a nearer 400-year cycle.
const LOCAL_LIMIT: i128 = 625 * MICROS_PER_400_YEARS as i128;

/// The local wall-clock microseconds of an instant in `zone`, beyond the BIGINT range when
/// the offset moves an extreme instant past it. An instant beyond the date library's years
/// takes the offset of the same position in a nearer 400-year cycle.
pub(crate) fn local_micros(micros: i64, zone: &ArrowTz) -> i128 {
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

/// The zone a TIMESTAMP's type names, as Spark reads it.
pub(crate) struct TimestampZone {
    /// The zone as Arrow reads it, for instants beyond the date library's years.
    pub(crate) arrow: ArrowTz,
    pub(crate) kind: ZoneKind,
}

pub(crate) enum ZoneKind {
    /// A region whose offset changes, with or without DST.
    Region(Tz),
    Fixed(FixedOffset),
}

impl TimestampZone {
    /// The zone `zone` names, accepting exactly what Arrow accepts. Java maps the legacy IDs
    /// `EST`, `MST` and `HST` to fixed offsets, where the date library names regions with
    /// local mean time (#1029).
    pub(crate) fn parse(zone: &str) -> Option<Self> {
        let zone = match zone {
            "EST" => "-05:00",
            "MST" => "-07:00",
            "HST" => "-10:00",
            other => other,
        };
        let arrow = ArrowTz::from_str(zone).ok()?;
        let kind = match Tz::from_str(zone) {
            Ok(region) => ZoneKind::Region(region),
            Err(_) => ZoneKind::Fixed(
                arrow
                    .offset_from_utc_datetime(&NaiveDateTime::default())
                    .fix(),
            ),
        };
        Some(Self { arrow, kind })
    }

    /// The offset of `micros` in microseconds.
    pub(crate) fn offset_micros(&self, micros: i64) -> i128 {
        local_micros(micros, &self.arrow) - i128::from(micros)
    }

    /// Java's `ZonedDateTime.ofLocal(local, zone, preferred)`: the instant of the local
    /// date-time `local` and its offset, both in microseconds. A repeated local date-time
    /// keeps the `preferred` offset when it is one of its two, and otherwise takes the
    /// earlier instant; a skipped one moves forward by the length of the skipped interval.
    /// Local date-times beyond the date library's years take the offsets of the same local
    /// date-time in a nearer 400-year cycle. `None` only where the date library has no
    /// answer.
    pub(crate) fn resolve_local(&self, local: i128, preferred: i128) -> Option<(i128, i128)> {
        let region = match &self.kind {
            ZoneKind::Fixed(offset) => {
                let offset = i128::from(offset.local_minus_utc()) * 1_000_000;
                return Some((local - offset, offset));
            }
            ZoneKind::Region(region) => region,
        };
        let shift = cycle_shift(local);
        let naive = naive_from_micros(local - shift)?;
        let seconds = |offset: FixedOffset| i128::from(offset.local_minus_utc()) * 1_000_000;
        let (applied, offset) = match region.from_local_datetime(&naive) {
            LocalResult::Single(instant) => {
                let offset = seconds(instant.offset().fix());
                (offset, offset)
            }
            LocalResult::Ambiguous(earlier, later) => {
                let later = seconds(later.offset().fix());
                let offset = if later == preferred {
                    later
                } else {
                    seconds(earlier.offset().fix())
                };
                (offset, offset)
            }
            // Java moves the local date-time forward by the skipped interval and takes the
            // offset after it, which is reading it with the offset before the change.
            LocalResult::None => {
                let gap = GapInfo::new(&naive, region)?;
                let (_, before) = gap.begin?;
                (seconds(before.fix()), seconds(gap.end?.offset().fix()))
            }
        };
        Some((local - applied, offset))
    }
}

/// Java's `LocalDateTime.plusMonths`: the local date-time `months` later, with the day of the
/// month clamped to the length of the resulting month.
pub(crate) fn add_local_months(local: i128, months: i64) -> Option<i128> {
    // Whole 400-year cycles of months move the local date-time by whole cycles exactly.
    let cycles = i128::from(months.div_euclid(MONTHS_PER_400_YEARS));
    let months = months.rem_euclid(MONTHS_PER_400_YEARS);
    let local = local.checked_add(cycles.checked_mul(i128::from(MICROS_PER_400_YEARS))?)?;
    let shift = cycle_shift(local);
    let moved = naive_from_micros(local - shift)?
        .checked_add_months(Months::new(u32::try_from(months).ok()?))?;
    Some(i128::from(moved.and_utc().timestamp_micros()) + shift)
}

/// Spark's `timestampAddInterval`: Java's `ZonedDateTime.plusMonths(months).plusDays(days)`
/// on the local date-time of `micros` in `zone`, then `plus(micros)` on the instant. `None`
/// when the result is beyond the BIGINT range.
pub(crate) fn add_interval(
    micros: i64,
    months: i64,
    days: i64,
    added_micros: i64,
    zone: &TimestampZone,
) -> Option<i64> {
    let mut instant = i128::from(micros);
    let mut offset = zone.offset_micros(micros);
    if months != 0 {
        let local = add_local_months(instant + offset, months)?;
        (instant, offset) = zone.resolve_local(local, offset)?;
    }
    if days != 0 {
        let local = instant + offset + i128::from(days) * i128::from(MICROS_PER_DAY);
        instant = zone.resolve_local(local, offset)?.0;
    }
    i64::try_from(instant + i128::from(added_micros)).ok()
}

/// The multiple of 400 years that moves `local` within the date library's years.
fn cycle_shift(local: i128) -> i128 {
    let cycle = i128::from(MICROS_PER_400_YEARS);
    let cycles = if local > LOCAL_LIMIT {
        (local - LOCAL_LIMIT + cycle - 1) / cycle
    } else if local < -LOCAL_LIMIT {
        -((-LOCAL_LIMIT - local + cycle - 1) / cycle)
    } else {
        0
    };
    cycles * cycle
}

fn naive_from_micros(micros: i128) -> Option<NaiveDateTime> {
    Some(DateTime::from_timestamp_micros(i64::try_from(micros).ok()?)?.naive_utc())
}

#[cfg(test)]
mod tests {
    use super::*;

    const LA: &str = "America/Los_Angeles";
    const DAY: i64 = MICROS_PER_DAY;
    const HOUR: i64 = 3_600_000_000;

    fn add(zone: &str, micros: i64, months: i64, days: i64, added: i64) -> Option<i64> {
        add_interval(micros, months, days, added, &TimestampZone::parse(zone)?)
    }

    #[test]
    fn days_keep_the_local_time_across_dst_changes() {
        // 2024-03-09 02:30 PST; 02:30 is skipped a day later, so Spark gives 03:30 PDT.
        assert_eq!(
            add(LA, 1_709_980_200_000_000, 0, 1, 0),
            Some(1_710_066_600_000_000)
        );
        // 2024-11-02 01:30 PDT; 01:30 repeats a day later and keeps PDT.
        assert_eq!(
            add(LA, 1_730_536_200_000_000, 0, 1, 0),
            Some(1_730_622_600_000_000)
        );
        // 2024-11-04 01:30 PST; 01:30 repeats a day earlier and keeps PST.
        assert_eq!(
            add(LA, 1_730_712_600_000_000, 0, -1, 0),
            Some(1_730_626_200_000_000)
        );
        // 2024-11-03 01:30 PDT, the first repeated 01:30, plus a day is 01:30 PST.
        assert_eq!(
            add(LA, 1_730_622_600_000_000, 0, 1, 0),
            Some(1_730_712_600_000_000)
        );
        // Days first, then the time on the instant: 2024-03-09 02:30 PST plus a day and an
        // hour is 04:30 PDT.
        assert_eq!(
            add(LA, 1_709_980_200_000_000, 0, 1, HOUR),
            Some(1_710_070_200_000_000)
        );
    }

    #[test]
    fn months_keep_the_local_time_and_clamp_the_day() {
        // 2024-02-10 02:30 PST plus a month is skipped, so 03:30 PDT.
        assert_eq!(
            add(LA, 1_707_561_000_000_000, 1, 0, 0),
            Some(1_710_066_600_000_000)
        );
        // 2024-01-31 UTC plus a month is 2024-02-29.
        assert_eq!(
            add("UTC", 1_706_659_200_000_000, 1, 0, 0),
            Some(1_709_164_800_000_000)
        );
    }

    #[test]
    fn local_mean_time_is_kept_where_it_repeats() {
        // 1883-11-17 12:03:00 local mean time; 12:03 repeats a day later, first in local mean
        // time, -07:52:58.
        assert_eq!(
            add(LA, -2_717_726_642_000_000, 0, 1, 0),
            Some(-2_717_726_642_000_000 + DAY)
        );
        // 1883-11-17 12:10:00 local mean time; a day later only PST holds 12:10.
        assert_eq!(
            add(LA, -2_717_726_222_000_000, 0, 1, 0),
            Some(-2_717_639_400_000_000)
        );
    }

    #[test]
    fn the_bigint_range_bounds_the_result() {
        assert_eq!(add(LA, i64::MAX, 0, -1, 0), Some(i64::MAX - DAY));
        assert_eq!(add(LA, i64::MIN, 0, 1, 0), Some(i64::MIN + DAY));
        assert_eq!(add(LA, i64::MAX, 0, 1, 0), None);
        assert_eq!(add(LA, i64::MIN, 0, 0, -1), None);
        assert_eq!(add(LA, i64::MAX, 1, 0, 0), None);
        assert_eq!(add("UTC", 0, 0, i64::from(i32::MAX), 0), None);
    }

    #[test]
    fn far_local_date_times_resolve_in_a_nearer_cycle() {
        let la = TimestampZone::parse(LA).unwrap_or_else(|| unreachable!());
        let far = 2 * LOCAL_LIMIT;
        let near = far - cycle_shift(far);
        assert!(near <= LOCAL_LIMIT && near > LOCAL_LIMIT - i128::from(MICROS_PER_400_YEARS));
        let (instant, offset) = la.resolve_local(far, 0).unwrap_or_else(|| unreachable!());
        assert_eq!(offset, -8 * i128::from(HOUR));
        assert_eq!(instant, far + 8 * i128::from(HOUR));
        assert_eq!(
            add_local_months(far, 12 * 400),
            Some(far + i128::from(MICROS_PER_400_YEARS))
        );
    }
}
