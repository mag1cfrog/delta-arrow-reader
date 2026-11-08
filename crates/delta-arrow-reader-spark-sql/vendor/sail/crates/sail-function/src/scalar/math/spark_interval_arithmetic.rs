// Local Spark ANSI interval addition and subtraction. See the host crate's UPSTREAM.md.
//! Spark's `+` and `-` of two DAY TO SECOND or two YEAR TO MONTH intervals: exact arithmetic
//! on the stored microsecond or month counts, which fails with
//! `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION` in both ANSI modes. Spark folds a constant
//! overflow while optimizing. Arrow's arithmetic failed only while collecting rows, with its own
//! overflow error.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, ArrowPrimitiveType, AsArray, PrimitiveArray};
use datafusion::arrow::datatypes::{
    DataType, DurationMicrosecondType, Field, FieldRef, IntervalUnit, IntervalYearMonthType,
    TimeUnit,
};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use num::traits::{CheckedAdd, CheckedSub};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntervalArithmetic {
    /// `Operator::Plus` or `Operator::Minus`.
    op: Operator,
    signature: Signature,
}

impl SparkIntervalArithmetic {
    pub fn new(op: Operator) -> Self {
        assert!(matches!(op, Operator::Plus | Operator::Minus));
        Self {
            op,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    /// Whether Spark adds or subtracts values of these types as ANSI intervals of one family.
    pub fn accepts(left: &DataType, right: &DataType) -> bool {
        left == right
            && matches!(
                left,
                DataType::Duration(TimeUnit::Microsecond)
                    | DataType::Interval(IntervalUnit::YearMonth)
            )
    }

    /// The TRY function Spark's overflow error suggests.
    fn try_function(&self) -> &'static str {
        match self.op {
            Operator::Plus => "try_add",
            _ => "try_subtract",
        }
    }

    /// Each row's exact sum or difference, failing on overflow.
    fn apply<T>(&self, left: &ArrayRef, right: &ArrayRef) -> Result<PrimitiveArray<T>>
    where
        T: ArrowPrimitiveType,
        T::Native: CheckedAdd + CheckedSub,
    {
        let left = left.as_primitive::<T>();
        let right = right.as_primitive::<T>();
        left.iter()
            .zip(right.iter())
            .map(|pair| match pair {
                (Some(left), Some(right)) => {
                    let result = match self.op {
                        Operator::Plus => left.checked_add(&right),
                        _ => left.checked_sub(&right),
                    };
                    match result {
                        Some(result) => Ok(Some(result)),
                        None => exec_err!(
                            "[INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION] Integer overflow while \
                             operating with intervals. Use `{}` to tolerate overflow and return \
                             NULL instead. SQLSTATE: 22015",
                            self.try_function()
                        ),
                    }
                }
                _ => Ok(None),
            })
            .collect()
    }
}

impl ScalarUDFImpl for SparkIntervalArithmetic {
    fn name(&self) -> &str {
        match self.op {
            Operator::Plus => "spark_interval_add",
            _ => "spark_interval_subtract",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [left, right] if Self::accepts(left, right) => Ok(left.clone()),
            _ => internal_err!("{} requires two intervals of one family", self.name()),
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

    // Spark folds a constant overflow during optimization, raising its error.
    fn propagates_constant_folding_errors(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let scalar = matches!(
            (left, right),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        );
        let left = left.to_array(args.number_rows)?;
        let right = right.to_array(args.number_rows)?;
        let result: ArrayRef = match left.data_type() {
            DataType::Duration(TimeUnit::Microsecond) => {
                Arc::new(self.apply::<DurationMicrosecondType>(&left, &right)?)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                Arc::new(self.apply::<IntervalYearMonthType>(&left, &right)?)
            }
            other => return internal_err!("{} requires intervals, got {other}", self.name()),
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}
