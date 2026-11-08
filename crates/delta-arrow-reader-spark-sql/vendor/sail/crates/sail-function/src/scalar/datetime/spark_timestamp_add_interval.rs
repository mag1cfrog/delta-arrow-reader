// Local Spark TIMESTAMP interval arithmetic. See the host crate's UPSTREAM.md.
//! Spark's TIMESTAMP plus or minus an interval, as `TimestampAddInterval` and
//! `TimestampAddYMInterval` give it: months and days move the local date-time in the
//! TIMESTAMP's time zone, and the rest of a DAY TO SECOND interval moves the instant. Arrow
//! adds a DAY TO SECOND interval as elapsed time and fails where months or days reach a local
//! date-time a DST change skips or repeats.

use std::sync::Arc;

use datafusion::arrow::array::types::{
    DurationMicrosecondType, IntervalMonthDayNanoType, IntervalYearMonthType,
    TimestampMicrosecondType,
};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::scalar::datetime::local_time::{MICROS_PER_DAY, TimestampZone, add_interval};

/// Whether the interval is added, or negated first as Spark subtracts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Direction {
    Add,
    /// Spark negates a calendar interval exactly only under ANSI; DAY TO SECOND and YEAR TO
    /// MONTH intervals always.
    Subtract {
        ansi_mode: bool,
    },
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampAddInterval {
    direction: Direction,
    signature: Signature,
}

impl SparkTimestampAddInterval {
    /// TIMESTAMP plus an interval.
    pub fn add() -> Self {
        Self::new(Direction::Add)
    }

    /// TIMESTAMP minus an interval.
    pub fn subtract(ansi_mode: bool) -> Self {
        Self::new(Direction::Subtract { ansi_mode })
    }

    fn new(direction: Direction) -> Self {
        Self {
            direction,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    /// Whether the kernel adds an interval of `interval` type to a TIMESTAMP of `timestamp`
    /// type: a TIMESTAMP with a time zone and a DAY TO SECOND, YEAR TO MONTH or calendar
    /// interval.
    pub fn accepts(timestamp: &DataType, interval: &DataType) -> bool {
        matches!(
            timestamp,
            DataType::Timestamp(TimeUnit::Microsecond, Some(_))
        ) && matches!(
            interval,
            DataType::Duration(TimeUnit::Microsecond)
                | DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::MonthDayNano)
        )
    }
}

/// Spark's error for negating the smallest DAY TO SECOND or YEAR TO MONTH interval.
const INTERVAL_OVERFLOW: &str = "[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION] Integer \
    overflow while operating with intervals. Try devising appropriate values for the interval \
    parameters. SQLSTATE: 22015";
/// Spark's ANSI error for negating a calendar interval with the smallest part.
const CALENDAR_OVERFLOW: &str = "[ARITHMETIC_OVERFLOW] overflow. If necessary set \
    \"spark.sql.ansi.enabled\" to \"false\" to bypass this error. SQLSTATE: 22003";

/// An interval as Spark splits it: months and days of local date-time, then microseconds.
struct IntervalParts {
    months: i32,
    days: i64,
    micros: i64,
}

impl IntervalParts {
    /// The parts of row `row` of `intervals`, negated as Spark negates them for a subtraction.
    fn at(intervals: &ArrayRef, row: usize, direction: Direction) -> Result<Self> {
        let subtract = direction != Direction::Add;
        match intervals.data_type() {
            // Spark's `timestampAddDayTime`: whole days, then the remainder.
            DataType::Duration(TimeUnit::Microsecond) => {
                let micros = intervals
                    .as_primitive::<DurationMicrosecondType>()
                    .value(row);
                let micros = match (subtract, micros.checked_neg()) {
                    (false, _) => micros,
                    (true, Some(negated)) => negated,
                    (true, None) => return exec_err!("{INTERVAL_OVERFLOW}"),
                };
                let days = micros / MICROS_PER_DAY;
                Ok(Self {
                    months: 0,
                    days,
                    micros: micros - days * MICROS_PER_DAY,
                })
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                let months = intervals.as_primitive::<IntervalYearMonthType>().value(row);
                let months = match (subtract, months.checked_neg()) {
                    (false, _) => months,
                    (true, Some(negated)) => negated,
                    (true, None) => return exec_err!("{INTERVAL_OVERFLOW}"),
                };
                Ok(Self {
                    months,
                    days: 0,
                    micros: 0,
                })
            }
            // Spark's calendar intervals hold microseconds.
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let interval = intervals
                    .as_primitive::<IntervalMonthDayNanoType>()
                    .value(row);
                let (months, days, micros) =
                    (interval.months, interval.days, interval.nanoseconds / 1_000);
                let (months, days, micros) = match direction {
                    Direction::Add => (months, days, micros),
                    Direction::Subtract { ansi_mode: false } => (
                        months.wrapping_neg(),
                        days.wrapping_neg(),
                        micros.wrapping_neg(),
                    ),
                    Direction::Subtract { ansi_mode: true } => {
                        match (
                            months.checked_neg(),
                            days.checked_neg(),
                            micros.checked_neg(),
                        ) {
                            (Some(months), Some(days), Some(micros)) => (months, days, micros),
                            _ => return exec_err!("{CALENDAR_OVERFLOW}"),
                        }
                    }
                };
                Ok(Self {
                    months,
                    days: i64::from(days),
                    micros,
                })
            }
            other => internal_err!("expected an interval, got {other}"),
        }
    }
}

impl ScalarUDFImpl for SparkTimestampAddInterval {
    fn name(&self) -> &str {
        match self.direction {
            Direction::Add => "spark_timestamp_add_interval",
            Direction::Subtract { .. } => "spark_timestamp_subtract_interval",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [timestamp, interval] if Self::accepts(timestamp, interval) => Ok(timestamp.clone()),
            _ => internal_err!("{} requires a TIMESTAMP and an interval", self.name()),
        }
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

    // Spark folds a constant addition during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [timestamps, intervals] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) = timestamps.data_type() else {
            return internal_err!("{} requires a TIMESTAMP", self.name());
        };
        let Some(parsed) = TimestampZone::parse(&zone) else {
            return exec_err!("invalid time zone {zone}");
        };
        let scalar = matches!(
            (timestamps, intervals),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        );
        let timestamps = timestamps.to_array(args.number_rows)?;
        let intervals = intervals.to_array(args.number_rows)?;
        let result = timestamps
            .as_primitive::<TimestampMicrosecondType>()
            .iter()
            .enumerate()
            .map(|(row, micros)| match micros {
                Some(micros) if intervals.is_valid(row) => {
                    let parts = IntervalParts::at(&intervals, row, self.direction)?;
                    let months = i64::from(parts.months);
                    match add_interval(micros, months, parts.days, parts.micros, &parsed) {
                        Some(result) => Ok(Some(result)),
                        // Java's exact arithmetic message.
                        None => exec_err!("long overflow"),
                    }
                }
                _ => Ok(None),
            })
            .collect::<Result<TimestampMicrosecondArray>>()?
            .with_timezone(zone);
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}
