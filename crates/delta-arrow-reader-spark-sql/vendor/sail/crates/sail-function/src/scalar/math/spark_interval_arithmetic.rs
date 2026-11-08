// Local Spark ANSI interval addition and subtraction. See the host crate's UPSTREAM.md.
//! Spark's `+` and `-` of two DAY TO SECOND or two YEAR TO MONTH intervals: exact arithmetic
//! on the stored microsecond or month counts, which fails with
//! `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION` in both ANSI modes. Spark folds a constant
//! overflow while optimizing, and a NULL operand skips the other for that row. Arrow's checked
//! kernels do the arithmetic; Arrow's native operator failed only while collecting rows, with
//! its own overflow error.

use std::sync::Arc;

use datafusion::arrow::compute::kernels::numeric::{add, sub};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion::physical_expr_common::datum::apply;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    lit,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Operation {
    Add,
    Subtract,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntervalArithmetic {
    operation: Operation,
    signature: Signature,
}

impl SparkIntervalArithmetic {
    /// Interval `+`.
    pub fn add() -> Self {
        Self::new(Operation::Add)
    }

    /// Interval `-`.
    pub fn subtract() -> Self {
        Self::new(Operation::Subtract)
    }

    fn new(operation: Operation) -> Self {
        Self {
            operation,
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
}

impl ScalarUDFImpl for SparkIntervalArithmetic {
    fn name(&self) -> &str {
        match self.operation {
            Operation::Add => "spark_interval_add",
            Operation::Subtract => "spark_interval_subtract",
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

    // Like Spark, a NULL operand skips evaluating the other for that row. Unlike a conditional
    // expression, both operands still fold, and fail, as constants.
    fn short_circuits_on_any_null_argument(&self) -> bool {
        true
    }

    // Spark propagates a NULL operand before folding constants, so a NULL interval discards
    // errors in the other operand.
    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null()))
        {
            let interval_type = info.get_data_type(&args[0])?;
            return Ok(ExprSimplifyResult::Simplified(lit(
                ScalarValue::try_new_null(&interval_type)?,
            )));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    // The result's range is unknown. Typing it as the interval, instead of the default NULL
    // range, lets an enclosing expression such as a timestamp sort key derive its ordering.
    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        let [interval, _] = inputs else {
            return internal_err!("{} requires two arguments", self.name());
        };
        Interval::make_unbounded(&interval.data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let kernel = match self.operation {
            Operation::Add => add,
            Operation::Subtract => sub,
        };
        let function = match self.operation {
            Operation::Add => "try_add",
            Operation::Subtract => "try_subtract",
        };
        match apply(left, right, kernel) {
            Err(DataFusionError::ArrowError(error, _))
                if matches!(error.as_ref(), ArrowError::ArithmeticOverflow(_)) =>
            {
                exec_err!(
                    "[INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION] Integer overflow while \
                     operating with intervals. Use `{function}` to tolerate overflow and return \
                     NULL instead. SQLSTATE: 22015"
                )
            }
            result => result,
        }
    }
}
