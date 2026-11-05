// Local Spark ANSI interval negation. See the host crate's UPSTREAM.md.
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, DurationMicrosecondType, Field, FieldRef, IntervalUnit,
    IntervalYearMonthType, TimeUnit,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use num::traits::CheckedNeg;

/// Spark's `UnaryMinus` and `negative` for year-month and day-time intervals. Spark negates
/// the stored month or microsecond count exactly and raises
/// `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` for the minimum in both ANSI modes.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntervalNegate {
    signature: Signature,
}

impl Default for SparkIntervalNegate {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkIntervalNegate {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// Whether Spark negates a value of `data_type` as an ANSI interval.
    pub fn negates(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Interval(IntervalUnit::YearMonth) | DataType::Duration(TimeUnit::Microsecond)
        )
    }
}

/// Negate every non-NULL count. Only the minimum count has no negation.
fn negate_each<T: ArrowPrimitiveType>(intervals: &ArrayRef) -> Result<ArrayRef>
where
    T::Native: CheckedNeg,
{
    let intervals = intervals.as_primitive::<T>();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(intervals.len());
    for count in intervals.iter() {
        match count {
            None => builder.append_null(),
            Some(count) => builder.append_value(count.checked_neg().ok_or_else(overflow_error)?),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn overflow_error() -> DataFusionError {
    exec_datafusion_err!(
        "[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION] Integer overflow while operating with \
         intervals. Try devising appropriate values for the interval parameters."
    )
}

fn negate_array(intervals: &ArrayRef) -> Result<ArrayRef> {
    match intervals.data_type() {
        DataType::Interval(IntervalUnit::YearMonth) => {
            negate_each::<IntervalYearMonthType>(intervals)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            negate_each::<DurationMicrosecondType>(intervals)
        }
        other => internal_err!("spark_negate_interval cannot negate {other}"),
    }
}

impl ScalarUDFImpl for SparkIntervalNegate {
    fn name(&self) -> &str {
        "spark_negate_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        match types {
            [interval] if Self::negates(interval) => Ok(vec![interval.clone()]),
            _ => internal_err!("{} requires one ANSI interval, got {types:?}", self.name()),
        }
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        Ok(self.coerce_types(types)?.remove(0))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [interval] = args.arg_fields else {
            return internal_err!("{} requires one argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&[interval.data_type().clone()])?,
            interval.is_nullable(),
        )))
    }

    // The result's range is unknown. Typing it as the interval lets an enclosing expression,
    // such as a timestamp sort key, derive its ordering.
    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        let [interval] = inputs else {
            return internal_err!("{} requires one argument", self.name());
        };
        Interval::make_unbounded(&interval.data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [interval] = args.args.as_slice() else {
            return internal_err!("{} requires one argument", self.name());
        };
        match interval {
            ColumnarValue::Scalar(interval) => {
                let negated = negate_array(&interval.to_array()?)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &negated, 0,
                )?))
            }
            ColumnarValue::Array(intervals) => Ok(ColumnarValue::Array(negate_array(intervals)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{DurationMicrosecondArray, IntervalYearMonthArray};

    use super::*;

    #[test]
    fn negation_keeps_nulls_and_rejects_only_the_minimum() -> Result<()> {
        let months: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![
            Some(i32::MAX),
            None,
            Some(-1),
        ]));
        let negated = negate_array(&months)?;
        assert_eq!(
            negated.as_primitive::<IntervalYearMonthType>(),
            &IntervalYearMonthArray::from(vec![Some(-i32::MAX), None, Some(1)])
        );
        for minimum in [
            Arc::new(IntervalYearMonthArray::from(vec![i32::MIN])) as ArrayRef,
            Arc::new(DurationMicrosecondArray::from(vec![i64::MIN])),
        ] {
            let error = negate_array(&minimum).expect_err("the minimum has no negation");
            assert!(
                error
                    .to_string()
                    .contains("[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION]"),
                "{error}"
            );
        }
        Ok(())
    }
}
