// Local Spark interval qualifier cast. See the host crate's UPSTREAM.md.
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, FieldRef, IntervalUnit, IntervalYearMonthType,
    TimeUnit,
};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

/// Spark's cast of a year-month or day-time interval to a qualifier of its family whose end
/// field is YEAR, DAY, HOUR or MINUTE. Spark keeps only the target's units, so the stored month
/// or microsecond count truncates toward zero to a multiple of `unit_length`, that end field's
/// length in stored units. Targets ending in MONTH or SECOND keep the count. The truncation
/// cannot overflow.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntervalTruncate {
    unit_length: i64,
    signature: Signature,
}

impl SparkIntervalTruncate {
    pub fn new(unit_length: i64) -> Self {
        assert!(
            unit_length > 1,
            "a truncating end field spans more than one stored unit"
        );
        Self {
            unit_length,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn truncate(&self, intervals: &ArrayRef) -> Result<ArrayRef> {
        let unit_length = self.unit_length;
        Ok(match intervals.data_type() {
            DataType::Interval(IntervalUnit::YearMonth) => {
                let Ok(unit_length) = i32::try_from(unit_length) else {
                    return internal_err!(
                        "{} unit length {unit_length} exceeds a month count",
                        self.name()
                    );
                };
                let truncated: PrimitiveArray<IntervalYearMonthType> = intervals
                    .as_primitive::<IntervalYearMonthType>()
                    .unary(|months| months - months % unit_length);
                Arc::new(truncated)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                let truncated: PrimitiveArray<DurationMicrosecondType> = intervals
                    .as_primitive::<DurationMicrosecondType>()
                    .unary(|micros| micros - micros % unit_length);
                Arc::new(truncated)
            }
            other => return internal_err!("{} cannot truncate {other}", self.name()),
        })
    }
}

impl ScalarUDFImpl for SparkIntervalTruncate {
    fn name(&self) -> &str {
        "spark_truncate_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        match types {
            [
                interval @ (DataType::Interval(IntervalUnit::YearMonth)
                | DataType::Duration(TimeUnit::Microsecond)),
            ] => Ok(vec![interval.clone()]),
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [interval] = args.args.as_slice() else {
            return internal_err!("{} requires one argument", self.name());
        };
        match interval {
            ColumnarValue::Scalar(interval) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(&self.truncate(&interval.to_array()?)?, 0)?,
            )),
            ColumnarValue::Array(intervals) => Ok(ColumnarValue::Array(self.truncate(intervals)?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{DurationMicrosecondArray, IntervalYearMonthArray};

    use super::*;

    #[test]
    fn truncation_moves_toward_zero_and_keeps_nulls() -> Result<()> {
        let months: ArrayRef = Arc::new(IntervalYearMonthArray::from(vec![
            Some(27),
            Some(-27),
            None,
            Some(i32::MIN),
        ]));
        assert_eq!(
            SparkIntervalTruncate::new(12)
                .truncate(&months)?
                .as_primitive::<IntervalYearMonthType>(),
            &IntervalYearMonthArray::from(vec![Some(24), Some(-24), None, Some(-2_147_483_640)])
        );
        let micros: ArrayRef = Arc::new(DurationMicrosecondArray::from(vec![
            Some(-86_399_999_999),
            Some(i64::MIN),
        ]));
        assert_eq!(
            SparkIntervalTruncate::new(3_600_000_000)
                .truncate(&micros)?
                .as_primitive::<DurationMicrosecondType>(),
            &DurationMicrosecondArray::from(vec![
                Some(-82_800_000_000),
                Some(-9_223_372_036_800_000_000)
            ])
        );
        Ok(())
    }
}
