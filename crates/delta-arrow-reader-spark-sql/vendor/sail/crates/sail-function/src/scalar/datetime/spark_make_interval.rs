// Local Spark MAKE_INTERVAL. See the host crate's UPSTREAM.md.
//! Spark's MAKE_INTERVAL over its INT years, months, weeks, days, hours and minutes and its
//! DECIMAL(18, 6) seconds: `IntervalUtils.makeInterval`'s exact arithmetic, for MAKE_INTERVAL
//! and TRY_MAKE_INTERVAL. With Spark's `failOnError`, set for MAKE_INTERVAL under ANSI, an
//! overflow fails with `ARITHMETIC_OVERFLOW` and the result is
//! nullable only for a nullable argument; otherwise an overflow is NULL. A NULL argument skips
//! the later arguments for its row, as Spark evaluates them in order. `datafusion-spark`'s
//! function returned NULL on overflow in both modes and was always nullable.

use std::sync::Arc;

use datafusion::arrow::array::types::{Decimal128Type, Int32Type};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array, IntervalMonthDayNanoArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalMonthDayNano, IntervalUnit};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    lit,
};

const MONTHS_PER_YEAR: i32 = 12;
const DAYS_PER_WEEK: i32 = 7;
const MICROS_PER_HOUR: i64 = 3_600_000_000;
const MICROS_PER_MINUTE: i64 = 60_000_000;
/// MAKE_INTERVAL's seconds argument: a DECIMAL whose unscaled value counts microseconds.
pub const MAKE_INTERVAL_SECONDS_TYPE: DataType = DataType::Decimal128(18, 6);

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeInterval {
    /// Spark's `failOnError`: fail on overflow, and declare the result nullable only for a
    /// nullable argument.
    fail_on_error: bool,
    signature: Signature,
}

impl SparkMakeInterval {
    pub fn new(fail_on_error: bool) -> Self {
        Self {
            fail_on_error,
            signature: Signature::any(7, Volatility::Immutable),
        }
    }

    fn arithmetic_overflow(&self) -> Result<Option<IntervalMonthDayNano>> {
        if self.fail_on_error {
            exec_err!(
                "[ARITHMETIC_OVERFLOW] overflow. If necessary set \
                 \"spark.sql.ansi.enabled\" to \"false\" to bypass this error. \
                 SQLSTATE: 22003"
            )
        } else {
            Ok(None)
        }
    }

    /// Combine constant interval fields once when only the year varies.
    fn intervals_for_year_column(
        &self,
        args: &ScalarFunctionArgs,
    ) -> Option<Result<ColumnarValue>> {
        let [
            ColumnarValue::Array(years),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(months))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(weeks))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(days))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(hours))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(minutes))),
            ColumnarValue::Scalar(ScalarValue::Decimal128(Some(seconds), 18, 6)),
        ] = args.args.as_slice()
        else {
            return None;
        };
        let years = years.as_any().downcast_ref::<Int32Array>()?;
        if years.len() != args.number_rows {
            return None;
        }
        // Keep the original error order when any constant part is out of range.
        let fixed = interval_parts(
            [0, *months, *weeks, *days, *hours, *minutes],
            i64::try_from(*seconds).ok()?,
        )?;
        let nanos = fixed.micros.checked_mul(1_000)?;
        let result = years
            .iter()
            .map(|year| match year {
                None => Ok(None),
                Some(year) => match year
                    .checked_mul(MONTHS_PER_YEAR)
                    .and_then(|year_months| fixed.months.checked_add(year_months))
                {
                    Some(months) => Ok(Some(IntervalMonthDayNano::new(months, fixed.days, nanos))),
                    None => self.arithmetic_overflow(),
                },
            })
            .collect::<Result<IntervalMonthDayNanoArray>>();
        Some(result.map(|array| ColumnarValue::Array(Arc::new(array))))
    }
}

/// A calendar interval's parts, with microseconds that may exceed Arrow's nanoseconds.
#[derive(Debug, PartialEq, Eq)]
struct IntervalParts {
    months: i32,
    days: i32,
    micros: i64,
}

/// `IntervalUtils.makeInterval`: the interval's parts, or `None` where Java's exact arithmetic
/// overflows. INT hours and minutes and DECIMAL(18, 6) seconds stay within BIGINT
/// microseconds, so only months and days can overflow; the microseconds are checked anyway.
fn interval_parts(
    [years, months, weeks, days, hours, minutes]: [i32; 6],
    second_micros: i64,
) -> Option<IntervalParts> {
    Some(IntervalParts {
        months: months.checked_add(years.checked_mul(MONTHS_PER_YEAR)?)?,
        days: days.checked_add(weeks.checked_mul(DAYS_PER_WEEK)?)?,
        micros: second_micros
            .checked_add(i64::from(hours).checked_mul(MICROS_PER_HOUR)?)?
            .checked_add(i64::from(minutes).checked_mul(MICROS_PER_MINUTE)?)?,
    })
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
                    && *seconds == MAKE_INTERVAL_SECONDS_TYPE =>
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
            !self.fail_on_error || args.arg_fields.iter().any(|field| field.is_nullable()),
        )))
    }

    // Spark folds a constant MAKE_INTERVAL during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        self.fail_on_error
    }

    // Spark evaluates the arguments in order and returns NULL at the first NULL, so a NULL
    // argument skips the later arguments' casts for its row. Constant arguments still fold,
    // and fail, on their own.
    fn short_circuits_on_any_null_argument(&self) -> bool {
        true
    }

    // Spark propagates a NULL argument before folding constants, so a NULL literal discards
    // errors in the other arguments.
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null()))
        {
            return Ok(ExprSimplifyResult::Simplified(lit(
                ScalarValue::IntervalMonthDayNano(None),
            )));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    // Spark evaluates the arguments and the arithmetic row by row, so with several failing rows
    // it reports the first row's error, whichever argument or the arithmetic fails there. Each
    // argument then stops at its first failing row; this function runs over the rows before the
    // earliest one and fails, if at all, in an earlier row.
    fn requires_row_ordered_errors(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 7 {
            return internal_err!("{} requires seven arguments", self.name());
        }
        if let Some(result) = self.intervals_for_year_column(&args) {
            return result;
        }
        let scalar = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        // Constant arguments give one value, as DataFusion's scalar functions do.
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
                let second_micros = i64::try_from(seconds.value(row)).ok();
                match second_micros.and_then(|micros| interval_parts(values, micros)) {
                    Some(IntervalParts {
                        months,
                        days,
                        micros,
                    }) => match micros.checked_mul(1_000) {
                        Some(nanos) => Ok(Some(IntervalMonthDayNano::new(months, days, nanos))),
                        // Spark's BIGINT microseconds reach further than Arrow's nanoseconds.
                        None => exec_err!(
                            "MAKE_INTERVAL microseconds {micros} are beyond the calendar \
                             interval range of Arrow's nanoseconds"
                        ),
                    },
                    None => self.arithmetic_overflow(),
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
    fn literal_interval_fields_match_broadcast_fields() -> Result<()> {
        let years = Int32Array::from(vec![
            None,
            Some(0),
            Some(-2),
            Some(178_956_970),
            Some(178_956_971),
            Some(i32::MIN),
            None,
            Some(178_956_971),
            Some(0),
        ]);
        for fail_on_error in [false, true] {
            let function = SparkMakeInterval::new(fail_on_error);
            for (offset, length) in (0..=6).map(|length| (0, length)).chain([(6, 2), (6, 3)]) {
                let invoke = |args: Vec<ColumnarValue>| {
                    function.invoke_with_args(ScalarFunctionArgs {
                        arg_fields: args
                            .iter()
                            .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
                            .collect(),
                        args,
                        number_rows: length,
                        return_field: Arc::new(Field::new(
                            "interval",
                            DataType::Interval(IntervalUnit::MonthDayNano),
                            true,
                        )),
                        config_options: Arc::new(Default::default()),
                    })
                };
                for (parts, seconds) in [
                    ([0, 0, 0, 0, 0], 0),
                    ([1, -2, 3, -4, 5], 6_000_007),
                    ([-8, 0, 0, 0, 0], 0),
                    ([i32::MAX, 0, 0, 0, 0], 0),
                    ([0, i32::MAX, 0, 0, 0], 0),
                    ([0, 0, 0, 3_000_000, 0], 0),
                ] {
                    let mut args =
                        vec![ColumnarValue::Array(Arc::new(years.slice(offset, length)))];
                    args.extend(
                        parts
                            .into_iter()
                            .map(|part| ColumnarValue::Scalar(ScalarValue::Int32(Some(part)))),
                    );
                    args.push(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        Some(seconds),
                        18,
                        6,
                    )));
                    let expanded = args
                        .iter()
                        .map(|arg| arg.to_array(length).map(ColumnarValue::Array))
                        .collect::<Result<Vec<_>>>()?;
                    let expected = invoke(expanded);
                    let actual = invoke(args);
                    match (actual, expected) {
                        (Ok(ColumnarValue::Array(actual)), Ok(ColumnarValue::Array(expected))) => {
                            assert_eq!(actual.to_data(), expected.to_data());
                        }
                        (Err(actual), Err(expected)) => {
                            assert_eq!(actual.to_string(), expected.to_string())
                        }
                        outcomes => panic!("different interval outcomes: {outcomes:?}"),
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn intervals_add_exactly_and_overflow_at_java_limits() {
        assert_eq!(
            interval_parts([1, 2, 3, 4, 5, 6], 7_000_008),
            Some(IntervalParts {
                months: 14,
                days: 25,
                micros: 18_367_000_008
            })
        );
        assert_eq!(interval_parts([178_956_970, 8, 0, 0, 0, 0], 0), None);
        assert_eq!(interval_parts([0, 0, 306_783_378, 7, 0, 0], 0), None);
        assert_eq!(
            interval_parts([0, 0, 0, 0, i32::MAX, 0], 0),
            Some(IntervalParts {
                months: 0,
                days: 0,
                micros: 7_730_941_129_200_000_000
            })
        );
        assert_eq!(interval_parts([0, 0, 0, 0, i32::MAX, 0], i64::MAX), None);
    }
}
