// Local Spark MAKE_INTERVAL. See the host crate's UPSTREAM.md.
//! Spark's MAKE_INTERVAL over its INT years, months, weeks, days, hours and minutes and its
//! DECIMAL(18, 6) seconds: `IntervalUtils.makeInterval`'s exact arithmetic. An overflow fails
//! with `ARITHMETIC_OVERFLOW` under ANSI and is NULL otherwise, and the result is nullable only
//! for a nullable argument under ANSI. `datafusion-spark`'s function returned NULL on overflow
//! in both modes and was always nullable.

use std::sync::Arc;

use datafusion::arrow::array::types::{Decimal128Type, Int32Type};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, IntervalMonthDayNanoArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalMonthDayNano, IntervalUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

const MONTHS_PER_YEAR: i32 = 12;
const DAYS_PER_WEEK: i32 = 7;
const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_MINUTE: i64 = 60_000_000;
/// The seconds argument's type: whole microseconds.
pub const SECONDS_TYPE: DataType = DataType::Decimal128(18, 6);

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeInterval {
    /// Fail on overflow, and declare the result nullable only for a nullable argument.
    ansi_mode: bool,
    signature: Signature,
}

impl SparkMakeInterval {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            ansi_mode,
            signature: Signature::any(7, Volatility::Immutable),
        }
    }
}

/// `IntervalUtils.makeInterval`: the months, days and microseconds, or `None` where Java's
/// exact arithmetic overflows.
fn make_interval(parts: [i32; 6], micros: i64) -> Option<(i32, i32, i64)> {
    let [years, months, weeks, days, hours, minutes] = parts;
    let months = months.checked_add(years.checked_mul(MONTHS_PER_YEAR)?)?;
    let days = days.checked_add(weeks.checked_mul(DAYS_PER_WEEK)?)?;
    let micros = micros
        .checked_add(i64::from(hours).checked_mul(MICROS_PER_HOUR)?)?
        .checked_add(i64::from(minutes).checked_mul(MICROS_PER_MINUTE)?)?;
    Some((months, days, micros))
}

impl ScalarUDFImpl for SparkMakeInterval {
    fn name(&self) -> &str {
        "spark_make_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [parts @ .., seconds]
                if parts.len() == 6
                    && parts.iter().all(|part| *part == DataType::Int32)
                    && *seconds == SECONDS_TYPE =>
            {
                Ok(DataType::Interval(IntervalUnit::MonthDayNano))
            }
            _ => internal_err!(
                "{} requires six INT and a DECIMAL(18, 6) argument",
                self.name()
            ),
        }
    }

    // Spark: `if (failOnError) children.exists(_.nullable) else true`.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            !self.ansi_mode || args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    // Spark folds a constant MAKE_INTERVAL during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        self.ansi_mode
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 7 {
            return internal_err!("{} requires seven arguments", self.name());
        }
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        // Constant arguments give one value, even for a batch without rows.
        let rows = if scalar { 1 } else { args.number_rows };
        let arrays = args
            .args
            .iter()
            .map(|arg| arg.to_array(rows))
            .collect::<Result<Vec<ArrayRef>>>()?;
        let parts = arrays[..6]
            .iter()
            .map(|array| array.as_primitive::<Int32Type>())
            .collect::<Vec<_>>();
        let seconds = arrays[6].as_primitive::<Decimal128Type>();
        let result = (0..rows)
            .map(|row| {
                if parts.iter().any(|part| part.is_null(row)) || seconds.is_null(row) {
                    return Ok(None);
                }
                let values = [0, 1, 2, 3, 4, 5].map(|index| parts[index].value(row));
                // DECIMAL(18, 6) holds whole microseconds in a BIGINT.
                let micros = i64::try_from(seconds.value(row)).ok();
                match micros.and_then(|micros| make_interval(values, micros)) {
                    Some((months, days, micros)) => match micros.checked_mul(1_000) {
                        Some(nanos) => Ok(Some(IntervalMonthDayNano::new(months, days, nanos))),
                        // Spark's BIGINT microseconds reach further than Arrow's nanoseconds.
                        None => exec_err!(
                            "MAKE_INTERVAL microseconds {micros} are beyond the calendar \
                             interval range of Arrow's nanoseconds (#1145)"
                        ),
                    },
                    None if self.ansi_mode => exec_err!(
                        "[ARITHMETIC_OVERFLOW] overflow. If necessary set \
                         \"spark.sql.ansi.enabled\" to \"false\" to bypass this error. \
                         SQLSTATE: 22003"
                    ),
                    None => Ok(None),
                }
            })
            .collect::<Result<IntervalMonthDayNanoArray>>()?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intervals_add_exactly_and_overflow_at_java_limits() {
        assert_eq!(
            make_interval([1, 2, 3, 4, 5, 6], 7_000_008),
            Some((14, 25, 18_367_000_008))
        );
        assert_eq!(make_interval([178_956_970, 8, 0, 0, 0, 0], 0), None);
        assert_eq!(make_interval([0, 0, 306_783_378, 7, 0, 0], 0), None);
        assert_eq!(
            make_interval([0, 0, 0, 0, i32::MAX, 0], 0),
            Some((0, 0, 7_730_941_129_200_000_000))
        );
        assert_eq!(make_interval([0, 0, 0, 0, i32::MAX, 0], i64::MAX), None);
    }
}
