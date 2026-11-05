// Local Spark year-month interval scaling. See the host crate's UPSTREAM.md.
use std::sync::Arc;

use bigdecimal::num_bigint::BigInt;
use bigdecimal::{Signed, ToPrimitive};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, IntervalYearMonthArray, IntervalYearMonthBuilder,
};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, Field, FieldRef, Float64Type, Int32Type,
    Int64Type, IntervalUnit, IntervalYearMonthType,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility, lit,
};

use crate::scalar::spark_evaluation::SparkInterpretedEvaluation;

/// Spark's `MultiplyYMInterval` and `DivideYMInterval`: a year-month interval multiplied or
/// divided by a number, with the interval as the first argument whatever the SQL operand
/// order. Results round HALF_UP to whole months, and every error is raised in both ANSI modes.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkYearMonthIntervalScale {
    op: Operator,
    signature: Signature,
}

impl SparkYearMonthIntervalScale {
    pub fn new(op: Operator) -> Self {
        assert!(matches!(op, Operator::Multiply | Operator::Divide));
        Self {
            op,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// Whether Spark scales a year-month interval by a number of this type: any Spark
    /// numeric type, or an untyped NULL.
    pub fn accepts_number(number_type: &DataType) -> bool {
        matches!(
            number_type,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Null
        )
    }

    /// Scale by a BYTE, SHORT or INT, which Spark reads as a Java `int`.
    fn scale_by_int(&self, months: i32, number: i32, interpreted: bool) -> Result<i32> {
        match self.op {
            Operator::Multiply => months
                .checked_mul(number)
                .ok_or_else(|| exec_datafusion_err!("integer overflow")),
            _ => divide_by_integer(months, i64::from(number), interpreted),
        }
    }

    /// Scale by a BIGINT. Spark multiplies as a Java `long`, then narrows the product.
    fn scale_by_long(&self, months: i32, number: i64, interpreted: bool) -> Result<i32> {
        match self.op {
            Operator::Multiply => {
                let product = i64::from(months)
                    .checked_mul(number)
                    .ok_or_else(|| exec_datafusion_err!("long overflow"))?;
                i32::try_from(product).map_err(|_| exec_datafusion_err!("integer overflow"))
            }
            _ => divide_by_integer(months, number, interpreted),
        }
    }

    /// Scale by a FLOAT or DOUBLE, which Spark widens to a Java `double`.
    fn scale_by_double(&self, months: i32, number: f64) -> Result<i32> {
        let months = f64::from(months);
        let value = match self.op {
            Operator::Multiply => months * number,
            // Negative zero is zero too.
            _ if number == 0.0 => return Err(interval_divided_by_zero_error()),
            _ => months / number,
        };
        round_double_to_months(value)
    }

    /// Scale by a DECIMAL `coefficient * 10^-decimal_scale`, rounding the exact result HALF_UP.
    ///
    /// Spark first truncates a product to 39 significant digits, or a quotient to 39
    /// fractional digits. Truncation keeps a result that fits in a month count on the same
    /// side of the half-month, so it cannot change the rounded result.
    fn scale_by_decimal(&self, months: i32, coefficient: i128, decimal_scale: i8) -> Result<i32> {
        if self.op == Operator::Divide && coefficient == 0 {
            return Err(interval_divided_by_zero_error());
        }
        let power = BigInt::from(10).pow(u32::from(decimal_scale.unsigned_abs()));
        let (number, unit) = if decimal_scale >= 0 {
            (BigInt::from(coefficient), power)
        } else {
            (BigInt::from(coefficient) * power, BigInt::from(1))
        };
        let months = BigInt::from(months);
        // The scaled value is numerator / denominator.
        let (numerator, denominator) = match self.op {
            Operator::Multiply => (months * number, unit),
            _ => (months * unit, number),
        };
        divide_half_up(numerator, denominator)
            .to_i32()
            .ok_or_else(|| exec_datafusion_err!("Overflow"))
    }

    fn scale_arrays(
        &self,
        intervals: &ArrayRef,
        numbers: &ArrayRef,
        interpreted: bool,
    ) -> Result<ArrayRef> {
        let intervals = intervals.as_primitive::<IntervalYearMonthType>();
        Ok(Arc::new(match numbers.data_type() {
            DataType::Int32 => scale_each::<Int32Type>(intervals, numbers, |months, number| {
                self.scale_by_int(months, number, interpreted)
            })?,
            DataType::Int64 => scale_each::<Int64Type>(intervals, numbers, |months, number| {
                self.scale_by_long(months, number, interpreted)
            })?,
            DataType::Float64 => {
                scale_each::<Float64Type>(intervals, numbers, |months, number| {
                    self.scale_by_double(months, number)
                })?
            }
            DataType::Decimal128(_, decimal_scale) => {
                scale_each::<Decimal128Type>(intervals, numbers, |months, number| {
                    self.scale_by_decimal(months, number, *decimal_scale)
                })?
            }
            other => return internal_err!("{} cannot scale by {other}", self.name()),
        }))
    }
}

/// Apply `scale` to each row whose interval and number are both non-NULL.
fn scale_each<T: ArrowPrimitiveType>(
    intervals: &IntervalYearMonthArray,
    numbers: &ArrayRef,
    scale: impl Fn(i32, T::Native) -> Result<i32>,
) -> Result<IntervalYearMonthArray> {
    let numbers = numbers.as_primitive::<T>();
    let mut builder = IntervalYearMonthBuilder::with_capacity(intervals.len());
    for row in 0..intervals.len() {
        if intervals.is_null(row) || numbers.is_null(row) {
            builder.append_null();
        } else {
            builder.append_value(scale(intervals.value(row), numbers.value(row))?);
        }
    }
    Ok(builder.finish())
}

/// Divide by an integral number, rounding HALF_UP like Guava's `IntMath.divide` and
/// `LongMath.divide`.
///
/// Only the minimum interval divided by -1 overflows. Spark names a different condition for
/// it when its optimizer interprets the expression than when generated code evaluates it.
fn divide_by_integer(months: i32, number: i64, interpreted: bool) -> Result<i32> {
    if months == i32::MIN && number == -1 {
        return Err(if interpreted {
            exec_datafusion_err!(
                "[INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION] Integer overflow while operating \
                 with intervals. Use `try_divide` to tolerate overflow and return NULL instead."
            )
        } else {
            exec_datafusion_err!(
                "[ARITHMETIC_OVERFLOW] Overflow in integral divide. Use 'try_divide' to tolerate \
                 overflow and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" \
                 to \"false\" to bypass this error."
            )
        });
    }
    if number == 0 {
        return Err(interval_divided_by_zero_error());
    }
    let rounded = divide_half_up(i128::from(months), i128::from(number));
    // Apart from the minimum divided by -1, the quotient is no larger than the dividend.
    i32::try_from(rounded).map_err(|_| exec_datafusion_err!("integer overflow"))
}

/// The exact quotient rounded to an integer, with halves rounded away from zero. The operands
/// are wide enough that neither the remainder nor the divisor's magnitude overflows.
fn divide_half_up<T: Signed + PartialOrd + Clone>(numerator: T, denominator: T) -> T {
    let quotient = numerator.clone() / denominator.clone();
    let remainder = numerator.clone() % denominator.clone();
    let below_half = remainder.abs() < denominator.abs() - remainder.abs();
    if remainder.is_zero() || below_half {
        quotient
    } else if numerator.is_negative() == denominator.is_negative() {
        quotient + T::one()
    } else {
        quotient - T::one()
    }
}

/// Round like Guava's `DoubleMath.roundToInt(value, HALF_UP)`, with its error messages.
fn round_double_to_months(value: f64) -> Result<i32> {
    if !value.is_finite() {
        return Err(exec_datafusion_err!("input is infinite or NaN"));
    }
    // `f64::round` rounds halves away from zero, which is HALF_UP.
    let rounded = value.round();
    if rounded >= f64::from(i32::MIN) && rounded <= f64::from(i32::MAX) {
        Ok(rounded as i32)
    } else {
        Err(exec_datafusion_err!(
            "rounded value is out of range for input {} and rounding mode HALF_UP",
            java_double_string(value)
        ))
    }
}

/// Format a finite double of magnitude at least 10^7 like Java's `Double.toString`, which
/// uses computerized scientific notation with a fractional digit, such as `2.147483648E9`.
/// Out-of-range month counts always have such magnitudes. The digits are the shortest that
/// identify the value, as Java prints them since JDK 19 and, for nearly every value, before.
fn java_double_string(value: f64) -> String {
    debug_assert!(value.abs() >= 1e7, "Java prints {value} plainly");
    let scientific = format!("{value:e}");
    let Some((mantissa, exponent)) = scientific.split_once('e') else {
        return scientific;
    };
    if mantissa.contains('.') {
        format!("{mantissa}E{exponent}")
    } else {
        format!("{mantissa}.0E{exponent}")
    }
}

fn interval_divided_by_zero_error() -> DataFusionError {
    exec_datafusion_err!(
        "[INTERVAL_DIVIDED_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor \
         being 0 and return NULL instead."
    )
}

impl ScalarUDFImpl for SparkYearMonthIntervalScale {
    fn name(&self) -> &str {
        match self.op {
            Operator::Multiply => "spark_multiply_ym_interval",
            Operator::Divide => "spark_divide_ym_interval",
            _ => unreachable!("the constructor validates the operator"),
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Spark reads BYTE and SHORT numbers as `int` and FLOAT numbers as `double`.
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [DataType::Interval(IntervalUnit::YearMonth), number] = types else {
            return internal_err!(
                "{} requires a year-month interval and a number",
                self.name()
            );
        };
        let number = match number {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Null => DataType::Int32,
            DataType::Int64 => DataType::Int64,
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Decimal128(_, _) => number.clone(),
            other => return internal_err!("{} cannot scale by {other}", self.name()),
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

    // Like Spark, a NULL interval skips evaluating the number for that row. Unlike a
    // conditional expression, both operands still fold, and fail, as constants.
    fn short_circuits_on_any_null_argument(&self) -> bool {
        true
    }

    // Spark propagates a NULL operand before folding constants, so a NULL interval or number
    // discards errors in the other operand.
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [interval, number] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let interpreted = SparkInterpretedEvaluation::applies_to(&args.config_options);
        match (interval, number) {
            (ColumnarValue::Scalar(interval), ColumnarValue::Scalar(number)) => {
                let result =
                    self.scale_arrays(&interval.to_array()?, &number.to_array()?, interpreted)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            }
            _ => Ok(ColumnarValue::Array(self.scale_arrays(
                &interval.to_array(args.number_rows)?,
                &number.to_array(args.number_rows)?,
                interpreted,
            )?)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_print_like_java() {
        for (value, java) in [
            (2_147_483_648.0, "2.147483648E9"),
            (-3_221_225_472.0, "-3.221225472E9"),
            (1e10, "1.0E10"),
            (2.147483647e19, "2.147483647E19"),
            (-1e300, "-1.0E300"),
        ] {
            assert_eq!(java_double_string(value), java);
        }
    }

    #[test]
    fn halves_round_away_from_zero() {
        for (numerator, denominator, quotient) in [
            (27, 2, 14),
            (-27, 2, -14),
            (27, -2, -14),
            (-27, -2, 14),
            (25, 4, 6),
            (27, 4, 7),
            (-1, 3, 0),
            (-2, 3, -1),
        ] {
            assert_eq!(
                divide_half_up(BigInt::from(numerator), BigInt::from(denominator)),
                BigInt::from(quotient)
            );
            assert_eq!(
                divide_half_up(i128::from(numerator), i128::from(denominator)),
                i128::from(quotient)
            );
        }
    }
}
