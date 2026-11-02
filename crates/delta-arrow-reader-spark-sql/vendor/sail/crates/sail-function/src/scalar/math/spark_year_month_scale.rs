use std::sync::Arc;

use bigdecimal::num_bigint::BigInt;
use bigdecimal::{Signed, ToPrimitive, Zero};
use datafusion::arrow::array::{ArrayRef, AsArray, IntervalYearMonthArray};
use datafusion::arrow::compute::try_binary;
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Field, FieldRef, Float64Type, Int32Type, Int64Type, IntervalUnit,
    IntervalYearMonthType,
};
use datafusion::arrow::error::ArrowError;
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::{
    ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

use crate::functions_nested_utils::make_scalar_function;

/// Spark year-month scaling rounds HALF_UP and checks errors in both ANSI modes.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkYearMonthScale {
    op: Operator,
    signature: Signature,
}

impl SparkYearMonthScale {
    pub fn new(op: Operator) -> Self {
        assert!(matches!(op, Operator::Multiply | Operator::Divide));
        Self {
            op,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    fn scale_integer_months(
        &self,
        months: i32,
        number: i64,
        scalar_operands: bool,
    ) -> Result<i32, ArrowError> {
        if self.op == Operator::Multiply {
            let product = i64::from(months)
                .checked_mul(number)
                .ok_or_else(|| ArrowError::ComputeError("long overflow".into()))?;
            return i32::try_from(product)
                .map_err(|_| ArrowError::ComputeError("integer overflow".into()));
        }
        if number == 0 {
            return Err(interval_zero_divisor());
        }
        if months == i32::MIN && number == -1 {
            // Spark's constant folding and generated division use different overflow conditions.
            let message = if scalar_operands {
                "[INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION] Integer overflow while operating with intervals. Use `try_divide` to tolerate overflow and return NULL instead."
            } else {
                "[ARITHMETIC_OVERFLOW] Overflow in integral divide. Use 'try_divide' to tolerate overflow and return NULL instead."
            };
            return Err(ArrowError::ComputeError(message.into()));
        }
        let numerator = i128::from(months);
        let denominator = i128::from(number);
        let quotient = numerator / denominator;
        let remainder = numerator % denominator;
        let rounded = quotient
            + if remainder.abs() * 2 >= denominator.abs() {
                numerator.signum() * denominator.signum()
            } else {
                0
            };
        i32::try_from(rounded).map_err(|_| ArrowError::ComputeError("integer overflow".into()))
    }

    fn scale_float_months(&self, months: i32, number: f64) -> Result<i32, ArrowError> {
        let value = if self.op == Operator::Multiply {
            f64::from(months) * number
        } else {
            if number == 0.0 {
                return Err(interval_zero_divisor());
            }
            f64::from(months) / number
        };
        if !value.is_finite() {
            return Err(ArrowError::ComputeError("input is infinite or NaN".into()));
        }
        let rounded = value.round();
        if rounded < f64::from(i32::MIN) || rounded > f64::from(i32::MAX) {
            return Err(ArrowError::ComputeError(format!(
                "rounded value is out of range for input {value} and rounding mode HALF_UP"
            )));
        }
        Ok(rounded as i32)
    }

    fn scale_decimal_months(
        &self,
        months: i32,
        coefficient: i128,
        scale: i8,
    ) -> Result<i32, ArrowError> {
        if self.op == Operator::Divide && coefficient == 0 {
            return Err(interval_zero_divisor());
        }
        let factor = BigInt::from(10).pow(u32::from(scale.unsigned_abs()));
        let (mut numerator, mut denominator) = if self.op == Operator::Multiply {
            (BigInt::from(months) * coefficient, BigInt::from(1))
        } else {
            (BigInt::from(months), BigInt::from(coefficient))
        };
        if self.op == Operator::Multiply {
            if scale >= 0 {
                denominator *= factor;
            } else {
                numerator *= factor;
            }
        } else if scale >= 0 {
            numerator *= factor;
        } else {
            denominator *= factor;
        }
        let mut rounded = &numerator / &denominator;
        let remainder = &numerator % &denominator;
        if !remainder.is_zero() && remainder.abs() * 2 >= denominator.abs() {
            rounded += numerator.signum() * denominator.signum();
        }
        rounded
            .to_i32()
            .ok_or_else(|| ArrowError::ComputeError("Overflow".into()))
    }

    fn scale_arrays(&self, args: &[ArrayRef], scalar_operands: bool) -> Result<ArrayRef> {
        let [intervals, numbers] = args else {
            return internal_err!("year-month scaling requires two arguments");
        };
        let months = intervals.as_primitive::<IntervalYearMonthType>();
        let result: IntervalYearMonthArray = match numbers.data_type() {
            DataType::Int32 => try_binary(months, numbers.as_primitive::<Int32Type>(), |m, n| {
                self.scale_integer_months(m, i64::from(n), scalar_operands)
            })?,
            DataType::Int64 => try_binary(months, numbers.as_primitive::<Int64Type>(), |m, n| {
                self.scale_integer_months(m, n, scalar_operands)
            })?,
            DataType::Float64 => {
                try_binary(months, numbers.as_primitive::<Float64Type>(), |m, n| {
                    self.scale_float_months(m, n)
                })?
            }
            DataType::Decimal128(_, scale) => {
                try_binary(months, numbers.as_primitive::<Decimal128Type>(), |m, n| {
                    self.scale_decimal_months(m, n, *scale)
                })?
            }
            other => return internal_err!("unexpected year-month scaling operand: {other}"),
        };
        Ok(Arc::new(result))
    }
}

fn interval_zero_divisor() -> ArrowError {
    ArrowError::ComputeError("[INTERVAL_DIVIDED_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead.".into())
}

impl ScalarUDFImpl for SparkYearMonthScale {
    fn name(&self) -> &str {
        match self.op {
            Operator::Multiply => "spark_year_month_multiply",
            Operator::Divide => "spark_year_month_divide",
            _ => unreachable!("the constructor validates the operator"),
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [DataType::Interval(IntervalUnit::YearMonth), number] = types else {
            return plan_err!("year-month scaling requires an interval and a number");
        };
        let number = match number {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Null => DataType::Int32,
            DataType::Int64 => DataType::Int64,
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Decimal128(_, _) => number.clone(),
            other => return plan_err!("unsupported year-month scaling operand: {other}"),
        };
        Ok(vec![DataType::Interval(IntervalUnit::YearMonth), number])
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.coerce_types(types)?;
        Ok(DataType::Interval(IntervalUnit::YearMonth))
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

    fn short_circuits(&self) -> bool {
        true
    }

    fn short_circuits_on_null_first_arg(&self) -> bool {
        true
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        args.split_first()
            .map(|(first, rest)| (vec![first], rest.iter().collect()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let scalar_operands = args
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
        make_scalar_function(|arrays| self.scale_arrays(arrays, scalar_operands))(&args.args)
    }
}
