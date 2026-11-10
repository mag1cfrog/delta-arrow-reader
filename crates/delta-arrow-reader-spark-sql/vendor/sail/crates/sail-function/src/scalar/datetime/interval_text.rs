// Local Spark interval text. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
//! Spark's text of ANSI interval qualifiers and literals, shared by plan names and by errors
//! that print an interval value (#1152).

use datafusion_common::{Result, ScalarValue, plan_err};
use sail_common::spec::IntervalFieldType;

/// Formats an interval qualifier, such as `DAY` or `HOUR TO SECOND`.
pub fn interval_fields_text(
    start_field: IntervalFieldType,
    end_field: IntervalFieldType,
) -> String {
    let field_name = |field| match field {
        IntervalFieldType::Year => "YEAR",
        IntervalFieldType::Month => "MONTH",
        IntervalFieldType::Day => "DAY",
        IntervalFieldType::Hour => "HOUR",
        IntervalFieldType::Minute => "MINUTE",
        IntervalFieldType::Second => "SECOND",
    };
    let start = field_name(start_field);
    if start_field == end_field {
        start.to_string()
    } else {
        format!("{start} TO {}", field_name(end_field))
    }
}

/// Formats a year-month or microsecond day-time interval literal as Spark names it, for
/// example `INTERVAL '2 03' DAY TO HOUR`. The first field holds all larger units. A NULL
/// value formats as `NULL`. Fields outside the value's family, or a start field after the
/// end field, are an error, as is any other literal type.
pub fn interval_literal_text(
    literal: &ScalarValue,
    start_field: IntervalFieldType,
    end_field: IntervalFieldType,
) -> Result<String> {
    use IntervalFieldType::{Day, Hour, Minute, Month, Second, Year};
    let (negative, value) = match literal {
        ScalarValue::IntervalYearMonth(Some(months)) => {
            let value = months.unsigned_abs();
            let value = match (start_field, end_field) {
                (Year, Year) => (value / 12).to_string(),
                (Year, Month) => format!("{}-{}", value / 12, value % 12),
                (Month, Month) => value.to_string(),
                _ => return plan_err!("invalid year-month interval fields"),
            };
            (*months < 0, value)
        }
        ScalarValue::DurationMicrosecond(Some(microseconds)) => {
            let units = [
                (Day, 86_400_000_000_u64),
                (Hour, 3_600_000_000),
                (Minute, 60_000_000),
                (Second, 1_000_000),
            ];
            if start_field < Day || start_field > end_field {
                return plan_err!("invalid day-time interval fields");
            }
            let mut remainder = microseconds.unsigned_abs();
            let mut value = String::new();
            for (field, unit) in units {
                if field < start_field || field > end_field {
                    continue;
                }
                let part = remainder / unit;
                remainder %= unit;
                if !value.is_empty() {
                    value.push(if field == Hour { ' ' } else { ':' });
                }
                if field == Day {
                    value.push_str(&part.to_string());
                } else {
                    value.push_str(&format!("{part:02}"));
                }
            }
            if end_field == Second && remainder != 0 {
                value.push('.');
                value.push_str(format!("{remainder:06}").trim_end_matches('0'));
            }
            (*microseconds < 0, value)
        }
        ScalarValue::IntervalYearMonth(None) | ScalarValue::DurationMicrosecond(None) => {
            return Ok("NULL".into());
        }
        _ => return plan_err!("expected an interval literal"),
    };
    let interval_type = interval_fields_text(start_field, end_field);
    let sign = if negative { "-" } else { "" };
    Ok(format!("INTERVAL '{sign}{value}' {interval_type}"))
}
