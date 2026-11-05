// Local Spark ANSI interval scaling. See the host crate's UPSTREAM.md.
use std::sync::Arc;

use bigdecimal::num_bigint::BigInt;
use bigdecimal::{Signed, ToPrimitive};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, PrimitiveBuilder};
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, DurationMicrosecondType, Field, FieldRef,
    Float64Type, Int64Type, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility, lit,
};

use crate::scalar::spark_evaluation::SparkInterpretedEvaluation;

/// Spark's `MultiplyYMInterval`, `DivideYMInterval`, `MultiplyDTInterval` and
/// `DivideDTInterval`: a year-month or day-time interval multiplied or divided by a number,
/// with the interval as the first argument whatever the SQL operand order. Results round
/// HALF_UP to whole months or microseconds, and every error is raised in both ANSI modes.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIntervalScale {
    op: Operator,
    signature: Signature,
}

/// How Spark stores an interval: year-month intervals as an `int` month count and day-time
/// intervals as a `long` microsecond count.
#[derive(Debug, Clone, Copy)]
enum IntervalStorage {
    Months,
    Microseconds,
}

impl IntervalStorage {
    fn of(interval_type: &DataType) -> Option<Self> {
        match interval_type {
            DataType::Interval(IntervalUnit::YearMonth) => Some(Self::Months),
            DataType::Duration(TimeUnit::Microsecond) => Some(Self::Microseconds),
            _ => None,
        }
    }

    fn min(self) -> i64 {
        match self {
            Self::Months => i64::from(i32::MIN),
            Self::Microseconds => i64::MIN,
        }
    }

    fn max(self) -> i64 {
        match self {
            Self::Months => i64::from(i32::MAX),
            Self::Microseconds => i64::MAX,
        }
    }

    fn contains(self, count: i128) -> bool {
        (i128::from(self.min())..=i128::from(self.max())).contains(&count)
    }
}

impl SparkIntervalScale {
    pub fn new(op: Operator) -> Self {
        assert!(matches!(op, Operator::Multiply | Operator::Divide));
        Self {
            op,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    /// Whether Spark scales an interval of `interval_type` by a number of `number_type`: a
    /// year-month or day-time interval by any Spark numeric type or an untyped NULL.
    pub fn scales(interval_type: &DataType, number_type: &DataType) -> bool {
        IntervalStorage::of(interval_type).is_some()
            && matches!(
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

    /// Scale an interval's stored `count` by a BYTE, SHORT, INT or BIGINT. Spark computes a
    /// year-month product as an `int` for narrower numbers and as a `long` narrowed to `int`
    /// for BIGINT, and a day-time product as a `long`. Checking the exact product against the
    /// `long` range and then the stored range reports the same error in each case.
    fn scale_by_integer(
        &self,
        storage: IntervalStorage,
        count: i64,
        number: i64,
        interpreted: bool,
    ) -> Result<i64> {
        let scaled = match self.op {
            Operator::Multiply => {
                let product = i128::from(count) * i128::from(number);
                i64::try_from(product).map_err(|_| exec_datafusion_err!("long overflow"))?;
                product
            }
            _ => divide_by_integer(storage, count, number, interpreted)?,
        };
        narrow(storage, scaled, "integer overflow")
    }

    /// Scale an interval's stored `count` by a FLOAT or DOUBLE, which Spark widens to a Java
    /// `double`.
    fn scale_by_double(&self, storage: IntervalStorage, count: i64, number: f64) -> Result<i64> {
        // A month count converts exactly. A microsecond count rounds to the nearest double, as
        // Java's long-to-double conversion does.
        let count = count as f64;
        let scaled = match self.op {
            Operator::Multiply => count * number,
            // Negative zero is zero too.
            _ if number == 0.0 => return Err(interval_divided_by_zero_error()),
            _ => count / number,
        };
        round_double(storage, scaled)
    }

    /// Scale an interval's stored `count` by a DECIMAL `coefficient * 10^-decimal_scale`,
    /// rounding the exact result HALF_UP.
    ///
    /// Spark first truncates a product to 39 significant digits, or a quotient to 39
    /// fractional digits. Truncation keeps a result that fits in the stored range on the
    /// same side of the half unit, so it cannot change the rounded result.
    fn scale_by_decimal(
        &self,
        storage: IntervalStorage,
        count: i64,
        coefficient: i128,
        decimal_scale: i8,
    ) -> Result<i64> {
        if self.op == Operator::Divide && coefficient == 0 {
            return Err(interval_divided_by_zero_error());
        }
        let power = BigInt::from(10).pow(u32::from(decimal_scale.unsigned_abs()));
        let (number, unit) = if decimal_scale >= 0 {
            (BigInt::from(coefficient), power)
        } else {
            (BigInt::from(coefficient) * power, BigInt::from(1))
        };
        let count = BigInt::from(count);
        // The scaled count is numerator / denominator.
        let (numerator, denominator) = match self.op {
            Operator::Multiply => (count * number, unit),
            _ => (count * unit, number),
        };
        let rounded = divide_half_up(numerator, denominator)
            .to_i128()
            .ok_or_else(|| exec_datafusion_err!("Overflow"))?;
        narrow(storage, rounded, "Overflow")
    }

    fn scale_arrays(
        &self,
        intervals: &ArrayRef,
        numbers: &ArrayRef,
        interpreted: bool,
    ) -> Result<ArrayRef> {
        match IntervalStorage::of(intervals.data_type()) {
            Some(storage @ IntervalStorage::Months) => {
                self.scale_stored::<IntervalYearMonthType>(storage, intervals, numbers, interpreted)
            }
            Some(storage @ IntervalStorage::Microseconds) => self
                .scale_stored::<DurationMicrosecondType>(storage, intervals, numbers, interpreted),
            None => internal_err!("{} cannot scale {}", self.name(), intervals.data_type()),
        }
    }

    fn scale_stored<I: ArrowPrimitiveType>(
        &self,
        storage: IntervalStorage,
        intervals: &ArrayRef,
        numbers: &ArrayRef,
        interpreted: bool,
    ) -> Result<ArrayRef>
    where
        I::Native: Into<i64> + TryFrom<i64>,
    {
        match numbers.data_type() {
            DataType::Int64 => scale_each::<I, Int64Type>(intervals, numbers, |count, number| {
                self.scale_by_integer(storage, count, number, interpreted)
            }),
            DataType::Float64 => {
                scale_each::<I, Float64Type>(intervals, numbers, |count, number| {
                    self.scale_by_double(storage, count, number)
                })
            }
            DataType::Decimal128(_, decimal_scale) => {
                scale_each::<I, Decimal128Type>(intervals, numbers, |count, number| {
                    self.scale_by_decimal(storage, count, number, *decimal_scale)
                })
            }
            other => internal_err!("{} cannot scale by {other}", self.name()),
        }
    }
}

/// Apply `scale` to each row whose interval and number are both non-NULL.
fn scale_each<I: ArrowPrimitiveType, N: ArrowPrimitiveType>(
    intervals: &ArrayRef,
    numbers: &ArrayRef,
    scale: impl Fn(i64, N::Native) -> Result<i64>,
) -> Result<ArrayRef>
where
    I::Native: Into<i64> + TryFrom<i64>,
{
    let intervals = intervals.as_primitive::<I>();
    let numbers = numbers.as_primitive::<N>();
    let mut builder = PrimitiveBuilder::<I>::with_capacity(intervals.len());
    for row in 0..intervals.len() {
        if intervals.is_null(row) || numbers.is_null(row) {
            builder.append_null();
        } else {
            let scaled = scale(intervals.value(row).into(), numbers.value(row))?;
            let stored = I::Native::try_from(scaled)
                .map_err(|_| exec_datafusion_err!("scaled interval {scaled} is out of range"))?;
            builder.append_value(stored);
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Narrow an exact scaled count to the range `storage` holds, failing with
/// `overflow_message`, the message of the Java operation that narrows it in Spark.
fn narrow(storage: IntervalStorage, scaled: i128, overflow_message: &str) -> Result<i64> {
    if storage.contains(scaled) {
        Ok(scaled as i64)
    } else {
        Err(exec_datafusion_err!("{overflow_message}"))
    }
}

/// Divide by an integral number, rounding HALF_UP like Guava's `IntMath.divide` and
/// `LongMath.divide`.
///
/// Only the minimum interval divided by -1 overflows. Spark names a different condition for
/// it when its optimizer interprets the expression than when generated code evaluates it.
fn divide_by_integer(
    storage: IntervalStorage,
    count: i64,
    number: i64,
    interpreted: bool,
) -> Result<i128> {
    if count == storage.min() && number == -1 {
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
    // Apart from the minimum divided by -1, the quotient is no larger than the dividend.
    Ok(divide_half_up(i128::from(count), i128::from(number)))
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

/// Round like Guava's `DoubleMath.roundToInt` or `roundToLong` with HALF_UP, with their error
/// messages.
fn round_double(storage: IntervalStorage, scaled: f64) -> Result<i64> {
    if !scaled.is_finite() {
        return Err(exec_datafusion_err!("input is infinite or NaN"));
    }
    // `f64::round` rounds halves away from zero, which is HALF_UP. The upper bound is one past
    // the maximum, which is exactly 2^31 or 2^63 as a double.
    let rounded = scaled.round();
    if rounded >= storage.min() as f64 && rounded < storage.max() as f64 + 1.0 {
        Ok(rounded as i64)
    } else {
        Err(exec_datafusion_err!(
            "rounded value is out of range for input {} and rounding mode HALF_UP",
            java_double_string(scaled)
        ))
    }
}

/// Format a finite double of magnitude at least 10^7 like Java's `Double.toString`, which
/// uses computerized scientific notation with a fractional digit, such as `2.147483648E9`.
/// Out-of-range month and microsecond counts always have such magnitudes. The digits are the
/// shortest that identify the value, as Java prints them since JDK 19 and, for nearly every
/// value, before.
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

impl ScalarUDFImpl for SparkIntervalScale {
    fn name(&self) -> &str {
        match self.op {
            Operator::Multiply => "spark_multiply_interval",
            Operator::Divide => "spark_divide_interval",
            _ => unreachable!("the constructor validates the operator"),
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Spark reads integral numbers as `int` or `long` and FLOAT numbers as `double`. An
    /// exact BIGINT computation reports the same results and errors for every integral type.
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let [interval, number] = types else {
            return internal_err!("{} requires an interval and a number", self.name());
        };
        if !Self::scales(interval, number) {
            return internal_err!("{} cannot scale {interval} by {number}", self.name());
        }
        let number = match number {
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Decimal128(_, _) => number.clone(),
            _ => DataType::Int64,
        };
        Ok(vec![interval.clone(), number])
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        Ok(self.coerce_types(types)?.remove(0))
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

    // The result's range is unknown. Typing it as the interval, instead of the default NULL
    // range, lets an enclosing expression such as a timestamp sort key derive its ordering.
    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        let [interval, _] = inputs else {
            return internal_err!("{} requires two arguments", self.name());
        };
        Interval::make_unbounded(&interval.data_type())
    }

    // Like Spark, a NULL interval skips evaluating the number for that row. Unlike a
    // conditional expression, both operands still fold, and fail, as constants.
    fn short_circuits_on_any_null_argument(&self) -> bool {
        true
    }

    // Spark propagates a NULL operand before folding constants, so a NULL interval or number
    // discards errors in the other operand.
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

    /// Spark multiplies a month count by a narrower integer as an `int`, by a BIGINT as a
    /// `long` narrowed to `int`, and a microsecond count as a `long`.
    #[test]
    fn integral_overflow_names_the_java_operation() {
        let multiply = SparkIntervalScale::new(Operator::Multiply);
        let outcome = |storage, count, number| {
            multiply
                .scale_by_integer(storage, count, number, false)
                .map_err(|error| error.to_string())
        };
        let (months, micros) = (IntervalStorage::Months, IntervalStorage::Microseconds);
        let int_max = i64::from(i32::MAX);
        assert_eq!(outcome(months, int_max, 1), Ok(int_max));
        assert_eq!(
            outcome(months, int_max, 2),
            Err("Execution error: integer overflow".into())
        );
        assert_eq!(
            outcome(months, 2, i64::MAX),
            Err("Execution error: long overflow".into())
        );
        assert_eq!(
            outcome(micros, i64::MAX, 2),
            Err("Execution error: long overflow".into())
        );
        assert_eq!(outcome(micros, i64::MIN, 1), Ok(i64::MIN));
    }
}
