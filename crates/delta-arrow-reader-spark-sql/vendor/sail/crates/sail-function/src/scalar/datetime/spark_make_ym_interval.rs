//! Spark's MAKE_YM_INTERVAL over its INT years and months: `MakeYMInterval`'s exact
//! arithmetic, which fails on overflow in both ANSI modes. Like Spark's, the result is
//! nullable only for a nullable argument, a constant overflow fails while optimizing, a NULL
//! literal argument gives NULL before either argument is evaluated, and a NULL year skips the
//! months for its row.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, IntervalYearMonthArray};
use datafusion::arrow::compute::try_binary;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int32Type, IntervalUnit};
use datafusion::arrow::error::ArrowError;
use datafusion_common::types::NativeType;
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    lit,
};

use crate::error::invalid_arg_count_exec_err;

const MONTHS_PER_YEAR: i32 = 12;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMakeYmInterval {
    signature: Signature,
}

impl Default for SparkMakeYmInterval {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMakeYmInterval {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMakeYmInterval {
    fn name(&self) -> &str {
        "spark_make_ym_interval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Interval(IntervalUnit::YearMonth))
    }

    // Spark's `MakeYMInterval` is a `BinaryExpression`, nullable only for a nullable argument;
    // omitted arguments are NOT NULL zeros.
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

    // Spark folds a constant MAKE_YM_INTERVAL during optimization, raising its overflow error.
    fn propagates_constant_folding_errors(&self) -> bool {
        true
    }

    // Spark evaluates the years first and returns NULL for a NULL year without evaluating the
    // months, so a NULL year skips the months' cast for its row.
    fn short_circuits_on_any_null_argument(&self) -> bool {
        true
    }

    // Spark's NullPropagation replaces the call with NULL for a NULL literal argument before
    // folding constants, so a NULL literal discards failures of the other argument's cast.
    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null()))
        {
            return Ok(ExprSimplifyResult::Simplified(lit(
                ScalarValue::IntervalYearMonth(None),
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
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        if args.len() > 2 {
            return Err(invalid_arg_count_exec_err(
                "make_ym_interval",
                (0, 2),
                args.len(),
            ));
        }

        // Omitted arguments default to zero (Spark fills `Literal(0)` for absent years/months).
        let zero = ColumnarValue::Scalar(ScalarValue::Int32(Some(0)));
        let mut args = args.into_iter();
        let year = args.next().unwrap_or_else(|| zero.clone());
        let month = args.next().unwrap_or(zero);

        // Spark's `MakeYMInterval` is null-intolerant: a NULL in a supplied argument yields NULL.
        let scalar_null = |value: &ColumnarValue| matches!(value, ColumnarValue::Scalar(scalar) if scalar.is_null());
        if scalar_null(&year) || scalar_null(&month) {
            return Ok(ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(None)));
        }

        let years = year.to_array(number_rows)?;
        let months = month.to_array(number_rows)?;
        let years = years.as_primitive::<Int32Type>();
        let months = months.as_primitive::<Int32Type>();

        // `try_binary` propagates array nulls and lets us surface Spark's overflow error, which is
        // raised regardless of `spark.sql.ansi.enabled` (year-month interval math uses exact ops).
        let result: IntervalYearMonthArray = try_binary(years, months, checked_ym_interval)?
            .with_data_type(DataType::Interval(IntervalUnit::YearMonth));
        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() > 2 {
            return Err(invalid_arg_count_exec_err(
                "make_ym_interval",
                (0, 2),
                arg_types.len(),
            ));
        }
        // Spark casts any number or STRING to INT and rejects other types.
        for arg_type in arg_types {
            let native: NativeType = arg_type.into();
            if !(native.is_numeric() || matches!(native, NativeType::String | NativeType::Null)) {
                return plan_err!(
                    "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] The arguments of Spark \
                     `make_ym_interval` must be numbers, strings or NULL"
                );
            }
        }
        Ok(vec![DataType::Int32; arg_types.len()])
    }
}

/// Spark's error for a YEAR TO MONTH interval beyond INT months.
const INTERVAL_OVERFLOW: &str = "[INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION] Integer \
    overflow while operating with intervals. Try devising appropriate values for the interval \
    parameters. SQLSTATE: 22015";

/// Computes `years * 12 + months`, raising Spark's interval overflow error on wrap.
///
/// Matches Spark `MakeYMInterval`, which uses `Math.multiplyExact`/`Math.addExact` and therefore
/// errors on Int32 overflow independently of `spark.sql.ansi.enabled`.
fn checked_ym_interval(years: i32, months: i32) -> std::result::Result<i32, ArrowError> {
    years
        .checked_mul(MONTHS_PER_YEAR)
        .and_then(|total| total.checked_add(months))
        .ok_or_else(|| ArrowError::ComputeError(INTERVAL_OVERFLOW.to_string()))
}
