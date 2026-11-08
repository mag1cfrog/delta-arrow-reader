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

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkTimestampAddInterval {
    /// Subtract the interval, which Spark negates before adding it.
    subtract: bool,
    signature: Signature,
}

impl SparkTimestampAddInterval {
    pub fn new(subtract: bool) -> Self {
        Self {
            subtract,
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

/// An interval as Spark splits it: months and days of local date-time, then microseconds.
struct IntervalParts {
    months: i64,
    days: i64,
    micros: i64,
}

impl IntervalParts {
    /// The parts of row `row` of `intervals`, negated for a subtraction.
    fn at(intervals: &ArrayRef, row: usize, subtract: bool) -> Result<Self> {
        let parts = match intervals.data_type() {
            // Spark's `timestampAddDayTime`: whole days, then the remainder.
            DataType::Duration(TimeUnit::Microsecond) => {
                let micros = intervals
                    .as_primitive::<DurationMicrosecondType>()
                    .value(row);
                let micros = if subtract {
                    match micros.checked_neg() {
                        Some(micros) => micros,
                        None => return exec_err!("long overflow"),
                    }
                } else {
                    micros
                };
                let days = micros / MICROS_PER_DAY;
                return Ok(Self {
                    months: 0,
                    days,
                    micros: micros - days * MICROS_PER_DAY,
                });
            }
            DataType::Interval(IntervalUnit::YearMonth) => Self {
                months: i64::from(intervals.as_primitive::<IntervalYearMonthType>().value(row)),
                days: 0,
                micros: 0,
            },
            // Spark's calendar intervals hold microseconds.
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let interval = intervals
                    .as_primitive::<IntervalMonthDayNanoType>()
                    .value(row);
                Self {
                    months: i64::from(interval.months),
                    days: i64::from(interval.days),
                    micros: interval.nanoseconds / 1_000,
                }
            }
            other => return internal_err!("expected an interval, got {other}"),
        };
        if !subtract {
            return Ok(parts);
        }
        // Spark negates each part exactly, months and days as INT.
        let negate_int = |value: i64| match i32::try_from(value).ok().and_then(i32::checked_neg) {
            Some(value) => Ok(i64::from(value)),
            None => exec_err!("integer overflow"),
        };
        Ok(Self {
            months: negate_int(parts.months)?,
            days: negate_int(parts.days)?,
            micros: match parts.micros.checked_neg() {
                Some(micros) => micros,
                None => return exec_err!("long overflow"),
            },
        })
    }
}

impl ScalarUDFImpl for SparkTimestampAddInterval {
    fn name(&self) -> &str {
        if self.subtract {
            "spark_timestamp_subtract_interval"
        } else {
            "spark_timestamp_add_interval"
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
                    let parts = IntervalParts::at(&intervals, row, self.subtract)?;
                    match add_interval(micros, parts.months, parts.days, parts.micros, &parsed) {
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
