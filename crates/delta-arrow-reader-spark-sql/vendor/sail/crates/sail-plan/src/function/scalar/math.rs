// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use datafusion::arrow::datatypes::{DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION};
use std::cmp::{max, min};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit, i256};
use datafusion::arrow::error::ArrowError;
use datafusion::functions::expr_fn;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::simplify::SimplifyContextBuilder;
use datafusion_expr::{
    BinaryExpr, Expr, ExprSchemable, Operator, ScalarUDF, cast, expr, lit, try_cast,
};
use datafusion_spark::function::math::expr_fn as math_fn;
use half::f16;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::error::generic_exec_err;
use sail_function::scalar::datetime::negate_duration::NegateDuration;
use sail_function::scalar::datetime::spark_interval::SparkDayTimeIntervalToCalendarInterval;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::math::rand_poisson::RandPoisson;
use sail_function::scalar::math::randn::Randn;
use sail_function::scalar::math::random::Random;
use sail_function::scalar::math::spark_abs::SparkAbs;
use sail_function::scalar::math::spark_bin::SparkBin;
use sail_function::scalar::math::spark_bround::SparkBRound;
use sail_function::scalar::math::spark_ceil_floor::{SparkCeil, SparkFloor};
use sail_function::scalar::math::spark_checked_arithmetic::SparkCheckedArithmetic;
use sail_function::scalar::math::spark_conv::SparkConv;
use sail_function::scalar::math::spark_decimal_round::SparkDecimalRound;
use sail_function::scalar::math::spark_div::SparkIntervalDiv;
use sail_function::scalar::math::spark_float_round::SparkFloatRound;
use sail_function::scalar::math::spark_integer_round::SparkIntegerRound;
use sail_function::scalar::math::spark_negative::SparkNegative;
use sail_function::scalar::math::spark_pmod::SparkPmod;
use sail_function::scalar::math::spark_signum::SparkSignum;
use sail_function::scalar::math::spark_try_add::SparkTryAdd;
use sail_function::scalar::math::spark_try_div::SparkTryDiv;
use sail_function::scalar::math::spark_try_mod::SparkTryMod;
use sail_function::scalar::math::spark_try_mult::SparkTryMult;
use sail_function::scalar::math::spark_try_subtract::SparkTrySubtract;
use sail_function::scalar::math::spark_unhex::SparkUnHex;
use sail_function::scalar::math::spark_uniform::SparkUniform;
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::spark_cast_numeric_to_decimal::{
    DecimalCastMode, SparkCastNumericToDecimal,
};
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};

pub(crate) use super::conditional::SparkNullIfZero;
pub(crate) use super::integer_divide::SparkIntegerDivide;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, ScalarFunction, ScalarFunctionInput};

fn add_day_time_interval_to_string(
    string: Expr,
    interval: Expr,
    string_type: DataType,
    session_timezone: Arc<str>,
    ansi_mode: bool,
) -> PlanResult<Expr> {
    let timestamp = ScalarUDF::from(SparkTimestamp::try_new(
        Some(session_timezone),
        ansi_mode,
        false,
    )?)
    .call(vec![string]);
    let calendar_interval =
        ScalarUDF::from(SparkDayTimeIntervalToCalendarInterval::new()).call(vec![interval]);
    let shifted = timestamp + calendar_interval;
    match string_type {
        DataType::Utf8 => Ok(ScalarUDF::from(SparkToUtf8::new()).call(vec![shifted])),
        DataType::LargeUtf8 => Ok(ScalarUDF::from(SparkToLargeUtf8::new()).call(vec![shifted])),
        DataType::Utf8View => Ok(ScalarUDF::from(SparkToUtf8View::new()).call(vec![shifted])),
        data_type => Err(PlanError::internal(format!(
            "expected string type for interval arithmetic, got {data_type}"
        ))),
    }
}

// Reuse division's Decimal/floating and integer-literal coercion rules.
// ANSI FLOAT/integer pairs also use DOUBLE. Literal casts fold before execution.
fn coerce_spark_arithmetic_operands(
    mut left: Expr,
    mut right: Expr,
    left_type: &DataType,
    right_type: &DataType,
    ansi_mode: bool,
) -> (Expr, Expr) {
    if is_float_decimal_pair(left_type, right_type)
        || (ansi_mode
            && ((left_type == &DataType::Float32 && right_type.is_integer())
                || (right_type == &DataType::Float32 && left_type.is_integer())))
    {
        return (
            cast(left, DataType::Float64),
            cast(right, DataType::Float64),
        );
    }
    if let Some(target) = spark_decimal_literal_datatype(&left, right_type) {
        left = cast_integer_literal_to_decimal(left, target, ansi_mode);
    }
    if let Some(target) = spark_decimal_literal_datatype(&right, left_type) {
        right = cast_integer_literal_to_decimal(right, target, ansi_mode);
    }
    (left, right)
}

// Keep the existing binary coercion and non-integer routes. ANSI is captured in
// the expression, so multiple query configurations can share a SessionContext.
fn spark_arithmetic(
    left: Expr,
    right: Expr,
    left_type: &DataType,
    right_type: &DataType,
    op: Operator,
    ansi_mode: bool,
    schema: &DFSchemaRef,
) -> Expr {
    if ansi_mode
        && (left_type.is_signed_integer() || *left_type == DataType::Null)
        && (right_type.is_signed_integer() || *right_type == DataType::Null)
        && (left_type.is_signed_integer() || right_type.is_signed_integer())
    {
        ScalarUDF::from(SparkCheckedArithmetic::new(op)).call(vec![left, right])
    } else if is_decimal_type(left_type) || is_decimal_type(right_type) {
        let (left, right) = coerce_decimal_peer_operand(left, right, schema);
        spark_decimal_arithmetic(left, right, op, schema, ansi_mode)
    } else {
        Expr::BinaryExpr(datafusion_expr::BinaryExpr::new(
            Box::new(left),
            op,
            Box::new(right),
        ))
    }
}

// Spark keeps the exact intermediate value, then rounds HALF_UP to its
// adjusted result type. Arrow's native Decimal256 arithmetic and Decimal CAST
// already implement these steps. Widen only when declared input bounds need it.
fn spark_decimal_arithmetic(
    mut left: Expr,
    mut right: Expr,
    op: Operator,
    schema: &DFSchemaRef,
    ansi_mode: bool,
) -> Expr {
    let binary = |left, right| {
        if op == Operator::Modulo {
            ScalarUDF::from(SparkCheckedModulo::new()).call(vec![left, right])
        } else {
            Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
        }
    };
    let (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2))) =
        (left.get_type(schema), right.get_type(schema))
    else {
        return binary(left, right);
    };
    let scale = i32::from(max(s1, s2));
    let aligned_precision =
        max(i32::from(p1) - i32::from(s1), i32::from(p2) - i32::from(s2)) + scale;
    let (precision, scale, intermediate_precision) = match op {
        Operator::Plus | Operator::Minus => (aligned_precision + 1, scale, aligned_precision + 1),
        Operator::Multiply => (
            i32::from(p1) + i32::from(p2) + 1,
            i32::from(s1) + i32::from(s2),
            i32::from(p1) + i32::from(p2) + 1,
        ),
        Operator::Modulo => (
            min(i32::from(p1) - i32::from(s1), i32::from(p2) - i32::from(s2)) + scale,
            scale,
            aligned_precision,
        ),
        _ => return binary(left, right),
    };
    if intermediate_precision <= i32::from(DECIMAL128_MAX_PRECISION) {
        let result = binary(left, right);
        if ansi_mode || op == Operator::Modulo {
            return result;
        }
        // Legacy Decimal arithmetic remains nullable even when its bounds fit exactly.
        let (precision, scale) = adjust_precision_scale(precision, scale);
        return try_cast(result, DataType::Decimal128(precision, scale));
    }
    left = cast(left, DataType::Decimal256(p1, s1));
    right = cast(right, DataType::Decimal256(p2, s2));
    let (precision, scale) = adjust_precision_scale(precision, scale);
    let data_type = DataType::Decimal128(precision, scale);
    if ansi_mode {
        cast(binary(left, right), data_type)
    } else {
        try_cast(binary(left, right), data_type)
    }
}

/// Arguments:
///   - left: A numeric, STRING, DATE, TIMESTAMP, or INTERVAL expression.
///   - right: If left is a numeric right must be numeric expression, or an INTERVAL otherwise.
///
/// Returns:
///   - If left is a numeric, the common maximum type of the arguments.
///   - If one expression is a STRING and the other is a day-time interval, the result is a STRING.
///   - If left is a DATE and right is a day-time interval the result is a TIMESTAMP.
///   - If both expressions are interval they must be of the same class.
///   - Otherwise, the result type matches left.
///
/// Most of the above conditions are handled by DataFusion. Spark-specific coercion differences are
/// rewritten here before constructing the DataFusion expression. For DataFusion's rules, see:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
fn spark_plus(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if arguments.len() < 2 {
        Ok(arguments.one()?)
    } else {
        let (left, right) = arguments.two()?;
        let (left_type, right_type) = (
            left.get_type(function_context.schema),
            right.get_type(function_context.schema),
        );
        Ok(match (left_type, right_type) {
            (
                Ok(string_type @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)),
                Ok(DataType::Duration(TimeUnit::Microsecond)),
            ) => add_day_time_interval_to_string(
                left,
                right,
                string_type,
                Arc::clone(&function_context.plan_config.session_timezone),
                function_context.plan_config.ansi_mode,
            )?,
            (
                Ok(DataType::Duration(TimeUnit::Microsecond)),
                Ok(string_type @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)),
            ) => add_day_time_interval_to_string(
                right,
                left,
                string_type,
                Arc::clone(&function_context.plan_config.session_timezone),
                function_context.plan_config.ansi_mode,
            )?,
            (Ok(DataType::Date32), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
                left + cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(DataType::Date32)) => {
                cast(left, DataType::Interval(IntervalUnit::MonthDayNano)) + right
            }
            (Ok(left_type), Ok(DataType::Date32)) if left_type.is_numeric() => {
                cast(left + cast(right, DataType::Int32), DataType::Date32)
            }
            (Ok(DataType::Date32), Ok(right_type)) if right_type.is_numeric() => {
                cast(cast(left, DataType::Int32) + right, DataType::Date32)
            }
            // TODO: In case getting the type fails, we don't want to fail the query.
            //  Future work is needed here, ideally we create something like `Operator::SparkPlus`.
            (Ok(left_type), Ok(right_type)) => {
                let (left, right) = coerce_spark_arithmetic_operands(
                    left,
                    right,
                    &left_type,
                    &right_type,
                    function_context.plan_config.ansi_mode,
                );
                spark_arithmetic(
                    left,
                    right,
                    &left_type,
                    &right_type,
                    Operator::Plus,
                    function_context.plan_config.ansi_mode,
                    function_context.schema,
                )
            }
            (Err(_), _) | (_, Err(_)) => left + right,
        })
    }
}

/// Arguments:
///   - left: A numeric, DATE, TIMESTAMP, or INTERVAL expression.
///   - right: The accepted type depends on the type of expr:
///     - If left is a numeric right must be numeric expression.
///     - If left is a year-month or day-time interval, right must be the same class.
///     - Otherwise right must be a DATE or TIMESTAMP.
///
/// Returns:
///   - If left is a numeric, the result is common maximum type of the arguments.
///   - If left is a DATE and right is a day-time interval the result is a TIMESTAMP.
///   - If left is a TIMESTAMP and right is an interval the result is a TIMESTAMP.
///   - If left and right are DATEs the result is an INTERVAL DAYS.
///   - If left or right are TIMESTAMP the result is an INTERVAL DAY TO SECOND.
///   - If both expressions are interval they must be of the same class.
///   - Otherwise, the result type matches left.
///
/// All of the above conditions should be handled by the DataFusion.
/// If there is a discrepancy in parity, check the link below and adjust Sail's logic accordingly:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
fn spark_minus(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if arguments.len() < 2 {
        let arg = arguments.one()?;
        Ok(spark_unary_negate(
            arg,
            function_context.plan_config.ansi_mode,
            function_context.schema,
        ))
    } else {
        let (left, right) = arguments.two()?;
        let (left_type, right_type) = (
            left.get_type(function_context.schema),
            right.get_type(function_context.schema),
        );
        Ok(match (left_type, right_type) {
            (Ok(DataType::Date32), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
                left - cast(right, DataType::Interval(IntervalUnit::MonthDayNano))
            }
            (Ok(DataType::Date32), Ok(right_type)) if right_type.is_numeric() => {
                cast(cast(left, DataType::Int32) - right, DataType::Date32)
            }
            // TODO: In case getting the type fails, we don't want to fail the query.
            //  Future work is needed here, ideally we create something like `Operator::SparkMinus`.
            (Ok(left_type), Ok(right_type)) => {
                let (left, right) = coerce_spark_arithmetic_operands(
                    left,
                    right,
                    &left_type,
                    &right_type,
                    function_context.plan_config.ansi_mode,
                );
                spark_arithmetic(
                    left,
                    right,
                    &left_type,
                    &right_type,
                    Operator::Minus,
                    function_context.plan_config.ansi_mode,
                    function_context.schema,
                )
            }
            (Err(_), _) | (_, Err(_)) => left - right,
        })
    }
}

/// Arguments:
///   - left: A numeric or INTERVAL expression.
///   - right: A numeric expression or INTERVAL expression.
///
/// You may not specify an INTERVAL for both arguments.
///
/// Returns:
///   - If both left and right are DECIMAL, the result is DECIMAL.
///   - If left or right is an INTERVAL, the result is of the same type.
///   - If both left and right are integral numeric types, the result is the larger of the two types.
///   - In all other cases the result is a DOUBLE.
///
/// All of the above conditions should be handled by the DataFusion.
/// If there is a discrepancy in parity, check the link below and adjust Sail's logic accordingly:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
fn spark_multiply(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (left, right) = arguments.two()?;
    let (left_type, right_type) = (
        left.get_type(function_context.schema),
        right.get_type(function_context.schema),
    );
    Ok(match (left_type, right_type) {
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(_)) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(
                cast(left, DataType::Int64) * right,
                DataType::Duration(TimeUnit::Microsecond),
            )
        }
        (Ok(_), Ok(DataType::Duration(TimeUnit::Microsecond))) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(
                left * cast(right, DataType::Int64),
                DataType::Duration(TimeUnit::Microsecond),
            )
        }
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkMultiply`.
        (Ok(left_type), Ok(right_type)) => {
            let (left, right) = coerce_spark_arithmetic_operands(
                left,
                right,
                &left_type,
                &right_type,
                function_context.plan_config.ansi_mode,
            );
            spark_arithmetic(
                left,
                right,
                &left_type,
                &right_type,
                Operator::Multiply,
                function_context.plan_config.ansi_mode,
                function_context.schema,
            )
        }
        (Err(_), _) | (_, Err(_)) => left * right,
    })
}

/// Check if an expression represents a zero literal value.
/// Handles both direct literals and CAST expressions wrapping literals.
fn is_zero_literal(expr: &Expr) -> bool {
    // Helper to check if a ScalarValue is zero
    fn is_scalar_zero(scalar: &ScalarValue) -> bool {
        match scalar {
            ScalarValue::Int8(Some(0))
            | ScalarValue::Int16(Some(0))
            | ScalarValue::Int32(Some(0))
            | ScalarValue::Int64(Some(0))
            | ScalarValue::UInt8(Some(0))
            | ScalarValue::UInt16(Some(0))
            | ScalarValue::UInt32(Some(0))
            | ScalarValue::UInt64(Some(0))
            | ScalarValue::Decimal128(Some(0), _, _) => true,
            ScalarValue::Float32(Some(v)) if *v == 0.0 => true,
            ScalarValue::Float64(Some(v)) if *v == 0.0 => true,
            ScalarValue::Float16(Some(f)) if *f == f16::from_f32(0.0) => true,
            ScalarValue::Decimal256(Some(v), _, _) if *v == i256::ZERO => true,
            _ => false,
        }
    }

    match expr {
        // Direct literal
        Expr::Literal(scalar, _) => is_scalar_zero(scalar),
        // CAST(literal AS type) - unwrap the cast and check the inner literal
        Expr::Cast(cast_expr) => {
            if let Expr::Literal(scalar, _) = cast_expr.expr.as_ref() {
                is_scalar_zero(scalar)
            } else {
                false
            }
        }
        // TryCast is similar to Cast
        Expr::TryCast(try_cast_expr) => {
            if let Expr::Literal(scalar, _) = try_cast_expr.expr.as_ref() {
                is_scalar_zero(scalar)
            } else {
                false
            }
        }
        Expr::ScalarFunction(f) if f.func.inner().is::<SparkCastNumericToDecimal>() => {
            matches!(f.args.as_slice(), [Expr::Literal(scalar, _)] if is_scalar_zero(scalar))
        }
        _ => false,
    }
}

/// Returns a guarded divisor expression that handles division by zero at runtime.
///
/// In non-ANSI mode: returns `nullif(divisor, 0)` — evaluates to NULL when divisor is zero.
/// In ANSI mode: returns `CASE WHEN divisor = 0 THEN raise_error(msg) ELSE divisor END`.
///
/// This wraps the divisor itself (not the entire division expression) to avoid
/// duplicating complex divisor expressions (e.g., window functions) in the plan.
fn make_safe_divisor(
    divisor: Expr,
    divisor_type: &DataType,
    ansi_mode: bool,
    error_message: &str,
) -> Expr {
    // Skip wrapping for Interval/Duration types (cannot be compared to lit(0)).
    if matches!(divisor_type, DataType::Interval(_) | DataType::Duration(_)) {
        return divisor;
    }

    if ansi_mode {
        let zero_check = divisor.clone().eq(lit(0));
        let raise = Expr::ScalarFunction(expr::ScalarFunction {
            func: Arc::new(ScalarUDF::from(RaiseError::new())),
            args: vec![lit(error_message)],
        });
        Expr::Case(expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(zero_check), Box::new(raise))],
            else_expr: Some(Box::new(divisor)),
        })
    } else if divisor_type.is_floating() {
        ScalarUDF::from(SparkNullIfZero::default()).call(vec![divisor])
    } else {
        let zero = match divisor_type {
            DataType::Decimal128(p, s) => lit(ScalarValue::Decimal128(Some(0), *p, *s)),
            DataType::Decimal256(p, s) => lit(ScalarValue::Decimal256(Some(i256::ZERO), *p, *s)),
            DataType::Null => lit(0),
            _ => cast(lit(0), divisor_type.clone()),
        };
        expr_fn::nullif(divisor, zero)
    }
}

/// Arguments:
///   - dividend: A numeric or INTERVAL expression.
///   - divisor: A numeric expression.
///
/// Returns:
///   - If both dividend and divisor are DECIMAL, the result is DECIMAL.
///   - If dividend is a year-month interval, the result is an INTERVAL YEAR TO MONTH.
///   - If dividend is a day-time interval, the result is an INTERVAL DAY TO SECOND.
///   - In all other cases, a DOUBLE.
///
/// All of the above conditions should be handled by the DataFusion.
/// If there is a discrepancy in parity, check the link below and adjust Sail's logic accordingly:
///   https://github.com/apache/datafusion/blob/a28f2834c6969a0c0eb26165031f8baa1e1156a5/datafusion/expr-common/src/type_coercion/binary.rs#L194
// Experimental subset of Sail PR 2225 at c9fda1f3281d383ee0efbd371d2b43d957d01939.
// Decimal division with numeric/NULL peers; Spark's default decimal settings are used.
const SPARK_MINIMUM_ADJUSTED_SCALE: i32 = 6;
const SPARK_MAX_SCALE: i32 = 38;
const ARROW_DIV_SCALE_INCREMENT: i8 = 4;

fn narrow(precision: i32, scale: i32) -> (u8, i8) {
    (
        precision.clamp(1, DECIMAL128_MAX_PRECISION as i32) as u8,
        scale.clamp(-SPARK_MAX_SCALE, SPARK_MAX_SCALE) as i8,
    )
}

fn adjust_precision_scale(precision: i32, scale: i32) -> (u8, i8) {
    let max_precision = DECIMAL128_MAX_PRECISION as i32;
    if precision <= max_precision {
        narrow(precision, scale)
    } else if scale < 0 {
        // Spark keeps a negative scale unchanged here (DecimalType.scala:182), reachable only
        // with `spark.sql.legacy.allowNegativeScaleOfDecimal` (negative-scale decimals). Mirror it
        // exactly for faithfulness; with non-negative operand scales the `+ - * %` result scale
        // stays `>= 0` (`*` uses `s1 + s2`), so this branch is not hit in practice.
        narrow(max_precision, scale)
    } else {
        let int_digits = precision - scale;
        let min_scale = scale.min(SPARK_MINIMUM_ADJUSTED_SCALE);
        let adjusted_scale = (max_precision - int_digits).max(min_scale);
        narrow(max_precision, adjusted_scale)
    }
}

fn spark_decimal_divide_type(p1: u8, s1: i8, p2: u8, s2: i8) -> (u8, i8) {
    let scale = max(SPARK_MINIMUM_ADJUSTED_SCALE, s1 as i32 + p2 as i32 + 1);
    let precision = (p1 as i32 - s1 as i32) + s2 as i32 + scale;
    adjust_precision_scale(precision, scale)
}

fn is_decimal_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    )
}

fn is_float_decimal_pair(a: &DataType, b: &DataType) -> bool {
    (a.is_floating() && is_decimal_type(b)) || (is_decimal_type(a) && b.is_floating())
}

fn spark_integer_decimal_type(data_type: &DataType) -> Option<DataType> {
    let precision = match data_type {
        DataType::Int8 | DataType::UInt8 => 3,
        DataType::Int16 | DataType::UInt16 => 5,
        DataType::Int32 | DataType::UInt32 => 10,
        DataType::Int64 | DataType::UInt64 => 20,
        _ => return None,
    };
    Some(DataType::Decimal128(precision, 0))
}

fn coerce_decimal_peer_operand(left: Expr, right: Expr, schema: &DFSchemaRef) -> (Expr, Expr) {
    fn peer_decimal_type(data_type: &DataType, decimal_type: &DataType) -> Option<DataType> {
        match data_type {
            DataType::Null => Some(decimal_type.clone()),
            _ => spark_integer_decimal_type(data_type),
        }
    }
    match (left.get_type(schema), right.get_type(schema)) {
        (Ok(left_type), Ok(right_type)) if is_decimal_type(&right_type) => {
            match peer_decimal_type(&left_type, &right_type) {
                Some(target) => (cast(left, target), right),
                None => (left, right),
            }
        }
        (Ok(left_type), Ok(right_type)) if is_decimal_type(&left_type) => {
            match peer_decimal_type(&right_type, &left_type) {
                Some(target) => (left, cast(right, target)),
                None => (left, right),
            }
        }
        _ => (left, right),
    }
}

fn coerce_spark_divide_null_operand(
    dividend: Expr,
    divisor: Expr,
    dividend_type: &DataType,
    divisor_type: &DataType,
) -> (Expr, Expr) {
    match (dividend_type, divisor_type) {
        (DataType::Decimal128(_, _) | DataType::Decimal256(_, _), DataType::Null) => {
            let divisor = cast(divisor, dividend_type.clone());
            (dividend, divisor)
        }
        (DataType::Null, DataType::Decimal128(_, _) | DataType::Decimal256(_, _)) => (
            cast(dividend, DataType::Float64),
            cast(divisor, DataType::Float64),
        ),
        _ => (dividend, divisor),
    }
}

fn spark_decimal_literal_datatype(expr: &Expr, other_type: &DataType) -> Option<DataType> {
    let is_256 = match other_type {
        DataType::Decimal128(_, _) => false,
        DataType::Decimal256(_, _) => true,
        _ => return None,
    };
    let scalar = match expr {
        Expr::Literal(scalar, _) => scalar,
        // A negative integer literal can appear as `Negative(Literal)` in the plan;
        // the digit count is sign-agnostic so only the magnitude matters.
        Expr::Negative(inner) => match inner.as_ref() {
            Expr::Literal(scalar, _) => scalar,
            _ => return None,
        },
        _ => return None,
    };
    let value = scalar_integer_value(scalar)?;
    let precision = integer_digit_count(value);
    Some(if is_256 {
        DataType::Decimal256(precision, 0)
    } else {
        DataType::Decimal128(precision, 0)
    })
}

fn scalar_integer_value(scalar: &ScalarValue) -> Option<i128> {
    Some(match scalar {
        ScalarValue::Int16(Some(v)) => i128::from(*v),
        ScalarValue::Int32(Some(v)) => i128::from(*v),
        ScalarValue::Int64(Some(v)) => i128::from(*v),
        ScalarValue::UInt16(Some(v)) => i128::from(*v),
        ScalarValue::UInt32(Some(v)) => i128::from(*v),
        ScalarValue::UInt64(Some(v)) => i128::from(*v),
        _ => return None,
    })
}

fn integer_digit_count(value: i128) -> u8 {
    // `checked_ilog10` is `None` only for zero, which Spark counts as one digit.
    value.unsigned_abs().checked_ilog10().unwrap_or(0) as u8 + 1
}

// Numeric/NULL subset of Sail PR 2225's operand coercion, used only by `/`.
// The probe uses Spark's default literal.pickMinimumPrecision=true.
fn coerce_decimal_divide_operands(
    left: Expr,
    right: Expr,
    schema: &DFSchemaRef,
    ansi_mode: bool,
) -> (Expr, Expr) {
    let (Ok(left_type), Ok(right_type)) = (left.get_type(schema), right.get_type(schema)) else {
        return (left, right);
    };
    if !is_decimal_type(&left_type) && !is_decimal_type(&right_type) {
        return (left, right);
    }
    if is_float_decimal_pair(&left_type, &right_type) {
        return (
            cast(left, DataType::Float64),
            cast(right, DataType::Float64),
        );
    }
    let left = match spark_decimal_literal_datatype(&left, &right_type) {
        Some(target) => cast_integer_literal_to_decimal(left, target, ansi_mode),
        None => left,
    };
    let right = match spark_decimal_literal_datatype(&right, &left_type) {
        Some(target) => cast_integer_literal_to_decimal(right, target, ansi_mode),
        None => right,
    };
    let (left, right) = coerce_spark_divide_null_operand(left, right, &left_type, &right_type);
    coerce_decimal_peer_operand(left, right, schema)
}

fn cast_integer_literal_to_decimal(expr: Expr, target: DataType, ansi_mode: bool) -> Expr {
    if let DataType::Decimal128(precision, scale) = target {
        let mode = if ansi_mode {
            DecimalCastMode::Ansi
        } else {
            DecimalCastMode::Legacy
        };
        ScalarUDF::from(SparkCastNumericToDecimal::new(precision, scale, mode)).call(vec![expr])
    } else {
        cast(expr, target)
    }
}

// Research prototype: compute Spark's final-scale coefficient in one array pass.
// Valid inputs are Decimal128 with nonnegative scales. No Python runtime is used.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct FusedDecimalDivide {
    signature: datafusion_expr::Signature,
    precision: u8,
    scale: i8,
    factor: i256,
    factor128: Option<i128>,
    limit: i128,
    ansi: bool,
}

// The denominator is nonzero. If scaled multiplication overflows i256, the
// quotient cannot fit 38 digits: a representable quotient times a <=38-digit
// denominator (plus its remainder) needs at most 76 digits.
// Keep wide arithmetic outside the hot array loop and its register allocation.
#[inline(never)]
fn fused_decimal_wide_value(a: i128, b: i128, factor: i256) -> Option<i128> {
    let numerator = i256::from_i128(a).checked_mul(factor)?;
    let denominator = i256::from_i128(b);
    let quotient = numerator.checked_div(denominator)?;
    // Truncating division gives |q*b| <= |numerator| and |remainder| < |b|,
    // so this product and subtraction cannot overflow after checked_div succeeds.
    let remainder = numerator
        .wrapping_sub(quotient.wrapping_mul(denominator))
        .to_i128()?;
    // Rounding moves away from zero and cannot recover a quotient outside i128.
    let quotient = quotient.to_i128()?;
    let threshold = b.unsigned_abs().div_ceil(2);
    let step = if (a < 0) == (b < 0) { 1 } else { -1 };
    quotient.checked_add(if remainder.unsigned_abs() >= threshold {
        step
    } else {
        0
    })
}

fn fused_decimal_value(a: i128, b: i128, factor128: Option<i128>, factor: i256) -> Option<i128> {
    if let Some(numerator) = factor128.and_then(|factor| a.checked_mul(factor)) {
        let quotient = numerator.checked_div(b)?;
        let remainder = numerator - quotient * b;
        let threshold = b.unsigned_abs().div_ceil(2);
        let step = if (a < 0) == (b < 0) { 1 } else { -1 };
        quotient.checked_add(if remainder.unsigned_abs() >= threshold {
            step
        } else {
            0
        })
    } else {
        fused_decimal_wide_value(a, b, factor)
    }
}

impl datafusion_expr::ScalarUDFImpl for FusedDecimalDivide {
    fn name(&self) -> &str {
        "fused_decimal_divide"
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Decimal128(self.precision, self.scale))
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &datafusion_expr::simplify::SimplifyContext,
    ) -> datafusion_common::Result<datafusion_expr::simplify::ExprSimplifyResult> {
        use datafusion_expr::simplify::ExprSimplifyResult;
        // Scalar NULL propagation precedes fallible CAST folding. Subqueries
        // retain their analyzer checks; keep the decorrelator's untyped sentinel.
        let sentinel = args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(ScalarValue::Null, _)));
        let scalar_null = args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null()))
            && !args.iter().any(Expr::contains_scalar_subquery);
        if sentinel || scalar_null {
            return Ok(ExprSimplifyResult::Simplified(lit(
                ScalarValue::Decimal128(None, self.precision, self.scale),
            )));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(
        &self,
        input: datafusion_expr::ScalarFunctionArgs,
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        use datafusion::arrow::array::{AsArray, Decimal128Builder};
        use datafusion::arrow::datatypes::Decimal128Type;
        use datafusion_expr::ColumnarValue;
        let arrays = ColumnarValue::values_to_arrays(&input.args)?;
        let [a, b] = arrays.as_slice() else {
            return Err(datafusion_common::DataFusionError::Internal(
                "expected two operands".into(),
            ));
        };
        let a = a.as_primitive::<Decimal128Type>();
        let b = b.as_primitive::<Decimal128Type>();
        let mut result = Decimal128Builder::with_capacity(a.len());
        for (a, b) in a.iter().zip(b.iter()) {
            let (Some(a), Some(b)) = (a, b) else {
                result.append_null();
                continue;
            };
            if b == 0 {
                if self.ansi {
                    return Err(ArrowError::DivideByZero.into());
                }
                result.append_null();
                continue;
            }
            match fused_decimal_value(a, b, self.factor128, self.factor)
                .filter(|v| *v >= -self.limit && *v <= self.limit)
            {
                Some(value) => result.append_value(value),
                None if self.ansi => {
                    return Err(ArrowError::ArithmeticOverflow(
                        "Spark decimal division result exceeds its precision".into(),
                    )
                    .into());
                }
                None => result.append_null(),
            }
        }
        let result = result
            .finish()
            .with_precision_and_scale(self.precision, self.scale)?;
        if input
            .args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
        {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

#[cfg(test)]
mod fused_null_tests {
    use super::*;
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion_expr::{LogicalPlanBuilder, scalar_subquery, simplify::SimplifyContext};

    #[test]
    fn fused_division_simplifies_empty_aggregate_null_only() -> datafusion_common::Result<()> {
        let info = SimplifyContext::builder().build();
        let simplifier = ExprSimplifier::new(info);
        for (precision, scale) in [(18, 4), (38, 38)] {
            let subquery = scalar_subquery(Arc::new(
                LogicalPlanBuilder::empty(true)
                    .project([lit(ScalarValue::Decimal128(Some(4), precision, scale))])?
                    .build()?,
            ));
            for ansi in [true, false] {
                let udf = ScalarUDF::from(FusedDecimalDivide {
                    signature: datafusion_expr::Signature::exact(
                        vec![DataType::Decimal128(precision, scale); 2],
                        datafusion_expr::Volatility::Immutable,
                    ),
                    precision: 38,
                    scale: 6,
                    factor: i256::from_i128(1_000_000),
                    factor128: Some(1_000_000),
                    limit: 10_i128.pow(38) - 1,
                    ansi,
                });
                let null = lit(ScalarValue::Null);
                for args in [
                    vec![null.clone(), subquery.clone()],
                    vec![subquery.clone(), null],
                ] {
                    assert_eq!(
                        simplifier.simplify(udf.call(args))?,
                        lit(ScalarValue::Decimal128(None, 38, 6)),
                    );
                }
                // SQL NULL must retain subquery errors; COUNT's zero retains
                // the divisor so it can still raise divide-by-zero in ANSI mode.
                for value in [None, Some(0)] {
                    let literal = lit(ScalarValue::Decimal128(value, precision, scale));
                    for args in [
                        vec![literal.clone(), subquery.clone()],
                        vec![subquery.clone(), literal],
                    ] {
                        let expr = udf.call(args);
                        assert_eq!(simplifier.simplify(expr.clone())?, expr);
                    }
                }
            }
        }
        Ok(())
    }
}

// Keep Arrow's vectorized float division, checking zero only on non-NULL rows.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SparkCheckedDivide {
    signature: datafusion_expr::Signature,
}

impl datafusion_expr::ScalarUDFImpl for SparkCheckedDivide {
    fn name(&self) -> &str {
        "spark_checked_divide"
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &datafusion_expr::simplify::SimplifyContext,
    ) -> datafusion_common::Result<datafusion_expr::simplify::ExprSimplifyResult> {
        use datafusion_expr::simplify::ExprSimplifyResult;
        // Preserve the NULL propagation of the BinaryExpr this replaces, including
        // the untyped NULL sentinel used while decorrelating empty aggregates.
        if args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null()))
        {
            return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Float64(
                None,
            ))));
        }
        // A proven nonzero constant needs no runtime zero check. Keep the native
        // expression so nullable arrays retain Arrow's vectorized scalar division.
        if matches!(&args[1], Expr::Literal(ScalarValue::Float64(Some(b)), _) if *b != 0.0) {
            let (left, right) = args.two()?;
            return Ok(ExprSimplifyResult::Simplified(left / right));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(
        &self,
        input: datafusion_expr::ScalarFunctionArgs,
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        use datafusion::arrow::compute::kernels::numeric::div;
        use datafusion::arrow::datatypes::Float64Type;
        use datafusion::physical_expr_common::datum::apply;
        let [left, right] = input.args.as_slice() else {
            return datafusion_common::internal_err!("spark_checked_divide requires two arguments");
        };
        apply(left, right, |left, right| {
            check_float_divisor::<Float64Type>(left, right)?;
            div(left, right)
        })
    }
}

// Division and remainder share this validity-aware zero check. It preserves
// scalar broadcasting and combines both array masks before rejecting a zero.
fn check_float_divisor<T: datafusion::arrow::datatypes::ArrowPrimitiveType>(
    left: &dyn datafusion::arrow::array::Datum,
    right: &dyn datafusion::arrow::array::Datum,
) -> Result<(), ArrowError> {
    use datafusion::arrow::array::{Array, ArrowNativeTypeOp, AsArray, BooleanArray};
    let (a, a_scalar) = left.get();
    let (b, b_scalar) = right.get();
    let a = a.as_primitive::<T>();
    let b = b.as_primitive::<T>();
    let has_zero = if b_scalar {
        b.is_valid(0) && b.value(0).is_zero() && a.null_count() != a.len()
    } else if a_scalar {
        a.is_valid(0) && BooleanArray::from_unary(b, |b| b.is_zero()).has_true()
    } else {
        if a.len() != b.len() {
            return Err(ArrowError::ComputeError(
                "division array lengths differ".into(),
            ));
        }
        BooleanArray::from_binary(a, b, |_, b| b.is_zero()).has_true()
    };
    if has_zero {
        Err(ArrowError::DivideByZero)
    } else {
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SparkCheckedModulo {
    signature: datafusion_expr::Signature,
}

impl SparkCheckedModulo {
    fn new() -> Self {
        Self {
            signature: datafusion_expr::Signature::user_defined(
                datafusion_expr::Volatility::Immutable,
            ),
        }
    }
}

impl datafusion_expr::ScalarUDFImpl for SparkCheckedModulo {
    fn name(&self) -> &str {
        "spark_checked_modulo"
    }
    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }
    fn coerce_types(&self, types: &[DataType]) -> datafusion_common::Result<Vec<DataType>> {
        let [left, right] = types else {
            return datafusion_common::internal_err!("spark_checked_modulo requires two arguments");
        };
        let (left, right) =
            datafusion_expr::binary::BinaryTypeCoercer::new(left, &Operator::Modulo, right)
                .get_input_types()?;
        Ok(vec![left, right])
    }
    fn return_type(&self, types: &[DataType]) -> datafusion_common::Result<DataType> {
        let [left, right] = types else {
            return datafusion_common::internal_err!("spark_checked_modulo requires two arguments");
        };
        datafusion_expr::binary::BinaryTypeCoercer::new(left, &Operator::Modulo, right)
            .get_result_type()
    }
    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &datafusion_expr::simplify::SimplifyContext,
    ) -> datafusion_common::Result<datafusion_expr::simplify::ExprSimplifyResult> {
        use datafusion_expr::simplify::ExprSimplifyResult;
        if args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null()))
        {
            let types = args
                .iter()
                .map(|arg| info.get_data_type(arg))
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::try_from(
                &self.return_type(&types)?,
            )?)));
        }
        // Keep the nullable UDF boundary even for a nonzero constant divisor.
        Ok(ExprSimplifyResult::Original(args))
    }
    fn invoke_with_args(
        &self,
        input: datafusion_expr::ScalarFunctionArgs,
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        use datafusion::arrow::compute::kernels::numeric::rem;
        use datafusion::arrow::datatypes::{Float32Type, Float64Type};
        use datafusion::physical_expr_common::datum::apply;
        let [left, right] = input.args.as_slice() else {
            return datafusion_common::internal_err!("spark_checked_modulo requires two arguments");
        };
        apply(left, right, |left, right| {
            let check = match left.get().0.data_type() {
                DataType::Float32 => check_float_divisor::<Float32Type>(left, right),
                DataType::Float64 => check_float_divisor::<Float64Type>(left, right),
                // Arrow checks integer and Decimal zeros using both validity masks.
                _ => Ok(()),
            };
            check.and_then(|()| rem(left, right)).map_err(|error| {
                if matches!(error, ArrowError::DivideByZero) {
                    ArrowError::ComputeError("[REMAINDER_BY_ZERO] Remainder by zero".into())
                } else {
                    error
                }
            })
        })
    }
}

#[test]
fn checked_divide_masks_and_scalar_positions() -> datafusion_common::Result<()> {
    use datafusion::arrow::array::{AsArray, Float64Array};
    use datafusion::arrow::datatypes::{Field, Float64Type};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    let udf = SparkCheckedDivide {
        signature: datafusion_expr::Signature::exact(
            vec![DataType::Float64, DataType::Float64],
            datafusion_expr::Volatility::Immutable,
        ),
    };
    let run = |left, right, rows| {
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![left, right],
            arg_fields: vec![Arc::new(Field::new("arg", DataType::Float64, true)); 2],
            number_rows: rows,
            return_field: Arc::new(Field::new("r", DataType::Float64, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    };
    let array = |v| ColumnarValue::Array(Arc::new(Float64Array::from(v)));
    let scalar = |v| ColumnarValue::Scalar(ScalarValue::Float64(v));
    for (a, b, expected) in [
        (
            array(vec![None, Some(2.0), Some(4.0)]),
            array(vec![Some(0.0), None, Some(2.0)]),
            vec![None, None, Some(2.0)],
        ),
        (
            array(vec![None, Some(4.0)]),
            scalar(Some(2.0)),
            vec![None, Some(2.0)],
        ),
        (
            scalar(Some(4.0)),
            array(vec![None, Some(2.0)]),
            vec![None, Some(2.0)],
        ),
        (
            array(vec![None, None]),
            scalar(Some(-0.0)),
            vec![None, None],
        ),
        (
            scalar(None),
            array(vec![Some(-0.0), Some(0.0)]),
            vec![None, None],
        ),
        (array(vec![]), scalar(Some(0.0)), vec![]),
        (scalar(Some(4.0)), array(vec![]), vec![]),
    ] {
        let out = run(a, b, expected.len())?.into_array(expected.len())?;
        assert_eq!(
            out.as_primitive::<Float64Type>().iter().collect::<Vec<_>>(),
            expected
        );
    }
    assert!(matches!(
        run(scalar(None), scalar(Some(0.0)), 1)?,
        ColumnarValue::Scalar(ScalarValue::Float64(None))
    ));
    assert!(
        matches!(run(scalar(Some(4.0)), scalar(Some(2.0)), 1)?, ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) if v == 2.0)
    );
    for zero in [0.0, -0.0] {
        for a in [0.0, 2.0, f64::NAN, f64::INFINITY] {
            assert!(
                run(scalar(Some(a)), scalar(Some(zero)), 1)
                    .unwrap_err()
                    .to_string()
                    .contains("Divide by zero")
            );
            assert!(
                run(
                    array(vec![None, Some(a)]),
                    array(vec![Some(zero), Some(zero)]),
                    2
                )
                .is_err()
            );
        }
    }
    Ok(())
}

// Spark promotes strings before operator-specific division/remainder coercion.
// Explicit CAST and other arithmetic keep their own coercion rules.
fn coerce_string_div_rem_operands(
    left: Expr,
    right: Expr,
    schema: &DFSchemaRef,
    ansi: bool,
    integral: bool,
) -> PlanResult<(Expr, Expr)> {
    let (Ok(left_type), Ok(right_type)) = (left.get_type(schema), right.get_type(schema)) else {
        return Ok((left, right));
    };
    if !left_type.is_string() && !right_type.is_string() {
        return Ok((left, right));
    }
    let peer = if left_type.is_string() {
        &right_type
    } else {
        &left_type
    };
    let target = if ansi && peer.is_signed_integer() {
        DataType::Int64
    } else {
        DataType::Float64
    };
    let wrong_type =
        ansi && (peer.is_string() || peer.is_null()) || integral && target == DataType::Float64;
    if wrong_type {
        // Legacy DIV widens integer peers but not FLOAT/NULL peers. Decimal
        // peers promote to DOUBLE. Preserve Spark's two distinct type errors.
        let condition = if !ansi
            && integral
            && (peer.is_signed_integer() || *peer == DataType::Float32 || peer.is_null())
        {
            "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES"
        } else {
            "DATATYPE_MISMATCH.BINARY_OP_WRONG_TYPE"
        };
        return Err(PlanError::invalid(format!(
            "[{condition}] unsupported string division/remainder operands"
        )));
    }
    if !(peer.is_numeric() || peer.is_string() || peer.is_null()) {
        return Ok((left, right));
    }
    let convert = |expr, data_type: &DataType| {
        if data_type.is_string() {
            // Long accepts ASCII control/space bytes and DEL. Ordinary DOUBLE
            // parsing trims only bytes through SPACE.
            let mut trim: String = (0_u8..=32).map(char::from).collect();
            if target == DataType::Int64 {
                trim.push('\u{7f}');
            }
            let expr = expr_fn::btrim(vec![expr, lit(trim)]);
            if ansi {
                cast(expr, target.clone())
            } else {
                try_cast(expr, target.clone())
            }
        } else if data_type != &target {
            cast(expr, target.clone())
        } else {
            expr
        }
    };
    Ok((convert(left, &left_type), convert(right, &right_type)))
}

fn spark_divide(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (dividend, divisor) = arguments.two()?;

    let ansi_mode = function_context.plan_config.ansi_mode;
    let (dividend, divisor) = coerce_string_div_rem_operands(
        dividend,
        divisor,
        function_context.schema,
        ansi_mode,
        false,
    )?;
    let (dividend, divisor) =
        coerce_decimal_divide_operands(dividend, divisor, function_context.schema, ansi_mode);
    let dividend_type = dividend.get_type(function_context.schema);
    let divisor_type = divisor.get_type(function_context.schema);

    if let (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2))) =
        (&dividend_type, &divisor_type)
        && *s1 >= 0
        && *s2 >= 0
    {
        let (precision, scale) = spark_decimal_divide_type(*p1, *s1, *p2, *s2);
        let exponent = u32::try_from(i32::from(scale) + i32::from(*s2) - i32::from(*s1))
            .map_err(|_| PlanError::internal("negative decimal division exponent"))?;
        let factor = i256::from_i128(10)
            .checked_pow(exponent)
            .ok_or_else(|| PlanError::internal("decimal division scale factor overflow"))?;
        let udf = FusedDecimalDivide {
            signature: datafusion_expr::Signature::exact(
                vec![
                    DataType::Decimal128(*p1, *s1),
                    DataType::Decimal128(*p2, *s2),
                ],
                datafusion_expr::Volatility::Immutable,
            ),
            precision,
            scale,
            factor,
            factor128: 10_i128.checked_pow(exponent),
            limit: 10_i128.pow(u32::from(precision)) - 1,
            ansi: ansi_mode,
        };
        return Ok(ScalarUDF::from(udf).call(vec![dividend, divisor]));
    }

    if ansi_mode
        && [&dividend_type, &divisor_type].iter().all(|t| {
            t.as_ref()
                .is_ok_and(|t| t.is_integer() || t.is_floating() || *t == DataType::Null)
        })
    {
        return Ok(ScalarUDF::from(SparkCheckedDivide {
            signature: datafusion_expr::Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                datafusion_expr::Volatility::Immutable,
            ),
        })
        .call(vec![
            cast(dividend, DataType::Float64),
            cast(divisor, DataType::Float64),
        ]));
    }

    // Apply runtime zero-divisor guard to the divisor before building the division expression.
    let effective_divisor_type = divisor_type.as_ref().cloned().unwrap_or(DataType::Int32);
    let divisor = if ansi_mode
        && matches!(dividend_type, Ok(DataType::Decimal128(_, _)))
        && matches!(divisor_type, Ok(DataType::Decimal128(_, _)))
    {
        // Arrow decimal division skips NULL rows and rejects non-NULL zero divisors.
        // A separate raise_error on the divisor would incorrectly reject NULL / 0.
        divisor
    } else {
        make_safe_divisor(
            divisor,
            &effective_divisor_type,
            ansi_mode,
            "Division by zero",
        )
    };

    let div_expr = match (&dividend_type, &divisor_type) {
        (Ok(DataType::Decimal128(p1, s1)), Ok(DataType::Decimal128(p2, s2))) => {
            let (result_precision, result_scale) = spark_decimal_divide_type(*p1, *s1, *p2, *s2);
            // Rescale the dividend so Arrow's quotient carries at least one digit past
            // Spark's scale. One guard digit is exactly enough for HALF_UP over a
            // truncating divide: the digit at `result_scale + 1` survives truncation, so
            // it decides the carry the same way the exact quotient would.
            let dividend_scale = (*s1).max(result_scale - (ARROW_DIV_SCALE_INCREMENT - 1));
            // Arrow's decimal `div` rescales the numerator to
            // `(p1 - s1) + dividend_scale + ARROW_DIV_SCALE_INCREMENT + s2` digits. When that
            // fits Decimal128 the quotient is computed in i128 (2-4x cheaper per row);
            // otherwise it widens to i256. The two are value-identical whenever the
            // intermediate does not overflow, so this is a pure performance gate.
            let intermediate_digits = i32::from(*p1) - i32::from(*s1)
                + i32::from(dividend_scale)
                + i32::from(ARROW_DIV_SCALE_INCREMENT)
                + i32::from(*s2);
            let (dividend_target, divisor_target) =
                if intermediate_digits <= i32::from(DECIMAL128_MAX_PRECISION) {
                    (
                        DataType::Decimal128(DECIMAL128_MAX_PRECISION, dividend_scale),
                        DataType::Decimal128(DECIMAL128_MAX_PRECISION, *s2),
                    )
                } else {
                    (
                        DataType::Decimal256(DECIMAL256_MAX_PRECISION, dividend_scale),
                        DataType::Decimal256(DECIMAL256_MAX_PRECISION, *s2),
                    )
                };
            let quotient = cast(dividend, dividend_target) / cast(divisor, divisor_target);
            let rounded = expr_fn::round(vec![quotient, lit(result_scale as i32)]);
            if ansi_mode {
                cast(
                    rounded,
                    DataType::Decimal128(result_precision, result_scale),
                )
            } else {
                try_cast(
                    rounded,
                    DataType::Decimal128(result_precision, result_scale),
                )
            }
        }
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        // TODO: Cast the precision and scale that matches the Spark's behavior after the division.
        //  See `test_divide` in python/pysail/tests/spark/test_math.py
        (Ok(DataType::Decimal128(_, _)), Ok(_))
        | (Ok(_), Ok(DataType::Decimal128(_, _)))
        | (Ok(DataType::Decimal256(_, _)), Ok(_))
        | (Ok(_), Ok(DataType::Decimal256(_, _)))
        | (Ok(DataType::Interval(IntervalUnit::YearMonth)), Ok(_))
        | (Ok(DataType::Interval(IntervalUnit::DayTime)), Ok(_)) => dividend / divisor,
        (Ok(DataType::Duration(TimeUnit::Microsecond)), Ok(_)) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(
                cast(dividend, DataType::Int64) / divisor,
                DataType::Duration(TimeUnit::Microsecond),
            )
        }
        (Ok(_), Ok(_)) => cast(dividend, DataType::Float64) / cast(divisor, DataType::Float64),
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkDivide`.
        (Err(_), _) | (_, Err(_)) => dividend / divisor,
    };

    Ok(div_expr)
}

// Spark evaluates the divisor first when converting local VALUES. Restrict
// the added check to strict string/numeric-column casts, including integer widening.
// Non-ANSI DIV lowers its divisor through NULLIF and does not match this shape.
pub(crate) fn column_cast(expr: &Expr, schema: &DFSchemaRef) -> bool {
    match expr {
        Expr::Cast(cast) if cast.field.data_type().is_numeric() => {
            // Preserve the same local-column boundary after the CAST resolver
            // adds its native whitespace normalization. Do not unwrap other functions.
            let input = match cast.expr.as_ref() {
                Expr::ScalarFunction(f)
                    if f.func
                        .inner()
                        .is::<datafusion::functions::string::btrim::BTrimFunc>()
                        && matches!(f.args.as_slice(), [_, Expr::Literal(
                            ScalarValue::Utf8(Some(trim))
                            | ScalarValue::LargeUtf8(Some(trim))
                            | ScalarValue::Utf8View(Some(trim)), _)]
                            if trim.bytes().eq(0_u8..=32)
                                || trim.bytes().eq((0_u8..=32).chain([127]))) =>
                {
                    &f.args[0]
                }
                expr => expr,
            };
            match input {
                Expr::Column(_) => input
                    .get_type(schema)
                    .is_ok_and(|t| t.is_string() || t.is_numeric()),
                expr => column_cast(expr, schema),
            }
        }
        Expr::ScalarFunction(f)
            if f.func.inner().downcast_ref::<SparkCastNumericToDecimal>()
                .is_some_and(|cast| cast.mode() == DecimalCastMode::Ansi) =>
        {
            matches!(f.args.as_slice(), [expr] if matches!(expr, Expr::Column(_)) || column_cast(expr, schema))
        }
        Expr::ScalarFunction(f)
            if f.func
                .inner()
                .is::<sail_function::scalar::spark_cast_string_to_float::SparkCastStringToFloat>(
            ) || f.func.inner().is::<sail_function::scalar::spark_cast_string_to_decimal::SparkCastStringToDecimal>() =>
        {
            matches!(f.args.as_slice(), [Expr::Column(_)])
        }
        _ => false,
    }
}

// Division/remainder evaluate the divisor first; other arithmetic evaluates
// the left operand first. Guard fallible column CASTs with native CASE.
pub(crate) fn guard_cast_arithmetic(
    expr: Expr,
    schema: &DFSchemaRef,
) -> datafusion_common::Result<datafusion_common::tree_node::Transformed<Expr>> {
    use datafusion_common::tree_node::Transformed;
    fn simple(expr: &Expr) -> bool {
        match expr {
            Expr::Column(_) | Expr::Literal(_, _) => true,
            Expr::Cast(cast) => simple(&cast.expr),
            Expr::TryCast(cast) => simple(&cast.expr),
            Expr::Alias(alias) => simple(&alias.expr),
            Expr::ScalarFunction(f) if f.func.inner().is::<SparkCastNumericToDecimal>() => {
                matches!(f.args.as_slice(), [expr] if simple(expr))
            }
            _ => false,
        }
    }
    let args = match &expr {
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::Divide | Operator::Modulo) => {
            Some((binary.left.as_ref(), binary.right.as_ref()))
        }
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Plus | Operator::Minus | Operator::Multiply
            ) =>
        {
            Some((binary.right.as_ref(), binary.left.as_ref()))
        }
        Expr::ScalarFunction(f)
            if f.func.inner().is::<SparkCheckedDivide>()
                || f.func.inner().is::<FusedDecimalDivide>()
                || f.func.inner().is::<SparkCheckedModulo>()
                || f.func.inner().is::<SparkIntegerDivide>() =>
        {
            Some((&f.args[0], &f.args[1]))
        }
        _ => None,
    };
    if let Some((left, right)) = args {
        let cast = column_cast(left, schema);
        if cast && (simple(right) || column_cast(right, schema)) && right.nullable(schema)? {
            let data_type = expr.get_type(schema)?;
            // ponytail: live rows repeat the simple divisor; a physical selection
            // node can avoid that copy if the recorded CAST cost warrants it.
            return Ok(Transformed::yes(
                datafusion_expr::expr_fn::when(
                    right.clone().is_null(),
                    lit(ScalarValue::try_new_null(&data_type)?),
                )
                .otherwise(expr)?,
            ));
        }
    }
    Ok(Transformed::no(expr))
}

pub(crate) fn integer_divide_args<'a>(
    expr: &'a Expr,
    schema: &DFSchemaRef,
) -> Option<(&'a Expr, &'a Expr, bool)> {
    match expr {
        Expr::ScalarFunction(f) if f.func.inner().is::<SparkIntegerDivide>() => Some((
            &f.args[0],
            &f.args[1],
            f.func
                .inner()
                .downcast_ref::<SparkIntegerDivide>()
                .unwrap()
                .ansi,
        )),
        Expr::BinaryExpr(binary)
            if binary.op == Operator::Divide
                && [&binary.left, &binary.right]
                    .iter()
                    .all(|arg| arg.get_type(schema).is_ok_and(|t| t.is_signed_integer())) =>
        {
            Some((&binary.left, &binary.right, true))
        }
        _ => None,
    }
}

/// Returns the integral part of the division of dividend by divisor.
///
/// Arguments:
///   - dividend: An expression that evaluates to a numeric or interval.
///   - divisor: A matching interval type if dividend is an interval, a numeric otherwise.
///
/// Returns:
///   A BIGINT
///
fn spark_div(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;

    let (dividend, divisor) = arguments.two()?;
    let (dividend, divisor) = coerce_string_div_rem_operands(
        dividend,
        divisor,
        function_context.schema,
        function_context.plan_config.ansi_mode,
        true,
    )?;

    let dividend_type = dividend.get_type(function_context.schema);
    let divisor_type = divisor.get_type(function_context.schema);
    // Spark rejects floating DIV before constant folding or zero handling.
    if [&dividend_type, &divisor_type]
        .iter()
        .any(|data_type| data_type.as_ref().is_ok_and(DataType::is_floating))
    {
        return Err(PlanError::invalid(
            "DIV does not accept floating-point operands",
        ));
    }

    let integer_operands = matches!((&dividend_type, &divisor_type), (Ok(left), Ok(right))
        if [left, right].iter().all(|t| t.is_signed_integer() || **t == DataType::Null));
    let decimal_operands = matches!(
        (&dividend_type, &divisor_type),
        (
            Ok(DataType::Decimal128(_, _)),
            Ok(DataType::Decimal128(_, _))
        )
    );
    // Native integer/Decimal kernels check zero only on live rows. Keep the
    // existing lowering for intervals and unresolved operands.
    if !integer_operands && !decimal_operands && is_zero_literal(&divisor) {
        if function_context.plan_config.ansi_mode {
            return Err(PlanError::ArrowError(ArrowError::DivideByZero));
        } else {
            return Ok(Expr::Literal(ScalarValue::Null, None));
        }
    }

    if integer_operands {
        // Keep equal small widths for the widening kernel; mixed widths use BIGINT.
        let input_type = match (&dividend_type, &divisor_type) {
            (Ok(left), Ok(right)) if left == right && left.is_signed_integer() => left.clone(),
            _ => DataType::Int64,
        };
        let coerce = |expr, data_type: &datafusion_common::Result<DataType>| {
            if data_type
                .as_ref()
                .is_ok_and(|data_type| *data_type == input_type)
            {
                expr
            } else {
                cast(expr, input_type.clone())
            }
        };
        let dividend = coerce(dividend, &dividend_type);
        let divisor = coerce(divisor, &divisor_type);
        let divisor = if function_context.plan_config.ansi_mode {
            divisor
        } else {
            expr_fn::nullif(divisor, lit(ScalarValue::new_zero(&input_type)?))
        };
        return Ok(ScalarUDF::from(SparkIntegerDivide {
            ansi: function_context.plan_config.ansi_mode,
            signature: datafusion_expr::Signature::exact(
                vec![input_type; 2],
                datafusion_expr::Volatility::Immutable,
            ),
        })
        .call(vec![dividend, divisor]));
    }

    let ansi_mode = function_context.plan_config.ansi_mode;

    // Native Decimal division, like integer division, checks only live zeros.
    // A separate divisor CASE hides a NULL divisor from scalar CAST pruning.
    let effective_divisor_type = divisor_type.as_ref().cloned().unwrap_or(DataType::Int32);
    let divisor = if ansi_mode && decimal_operands {
        divisor
    } else {
        make_safe_divisor(
            divisor,
            &effective_divisor_type,
            ansi_mode,
            "Division by zero",
        )
    };

    let div_expr = match (&dividend_type, &divisor_type) {
        // TODO: Casting DataType::Interval(_) to DataType::Int64 is not supported yet.
        //  Seems to be a bug in DataFusion.
        (Ok(DataType::Duration(_)), Ok(DataType::Duration(_))) => {
            // Match duration because we cast Spark's DayTime interval to Duration.
            cast(dividend, DataType::Int64) / cast(divisor, DataType::Int64)
        }
        // Handle Interval / Interval division using custom UDF
        (Ok(DataType::Interval(_)), Ok(DataType::Interval(_))) => {
            let interval_div = Arc::new(ScalarUDF::from(SparkIntervalDiv::new()));
            Expr::ScalarFunction(expr::ScalarFunction {
                func: interval_div,
                args: vec![dividend, divisor],
            })
        }
        // TODO: In case getting the type fails, we don't want to fail the query.
        //  Future work is needed here, ideally we create something like `Operator::SparkDivide`.
        (Ok(_), Ok(_)) | (Err(_), _) | (_, Err(_)) => dividend / divisor,
    };

    Ok(cast(div_expr, DataType::Int64))
}

#[tokio::test]
async fn div_rejects_floating_types_before_zero_folding() {
    use crate::config::PlanConfig;
    use crate::function::common::FunctionContextInput;
    use datafusion::prelude::SessionContext;
    use datafusion_common::DFSchema;

    let ctx = SessionContext::new();
    let schema = Arc::new(DFSchema::empty());
    for ansi_mode in [true, false] {
        let config = Arc::new(PlanConfig {
            ansi_mode,
            ..Default::default()
        });
        let resolve = |left, right| {
            spark_div(ScalarFunctionInput {
                arguments: vec![left, right],
                function_context: FunctionContextInput {
                    argument_display_names: &[],
                    plan_config: &config,
                    session_context: &ctx,
                    schema: &schema,
                },
            })
        };
        for data_type in [DataType::Float16, DataType::Float32, DataType::Float64] {
            for (left, right) in [
                (cast(lit(7), data_type.clone()), lit(0)),
                (lit(7), cast(lit(0), data_type.clone())),
                (cast(lit(ScalarValue::Null), data_type.clone()), lit(0)),
                (lit(0), cast(lit(2), data_type)),
            ] {
                assert!(
                    matches!(resolve(left, right), Err(PlanError::InvalidArgument(message))
                    if message == "DIV does not accept floating-point operands")
                );
            }
        }
        for (value, expected) in [
            (ScalarValue::Int8(Some(i8::MIN)), 128_i64),
            (ScalarValue::Int16(Some(i16::MIN)), 32768),
            (ScalarValue::Int32(Some(i32::MIN)), 2147483648),
        ] {
            let data_type = value.data_type();
            for divisor in [-3, -2, -1, 1, 2, 3] {
                let expr =
                    resolve(lit(value.clone()), cast(lit(divisor), data_type.clone())).unwrap();
                let batches = ctx
                    .sql("SELECT 1")
                    .await
                    .unwrap()
                    .select(vec![expr])
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                let values = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .unwrap();
                assert_eq!(values.value(0), -expected / i64::from(divisor));
            }
        }
        for data_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            for right_type in [data_type.clone(), DataType::Int64] {
                for (left, right) in [(None, Some(0)), (Some(7), Some(0)), (Some(7), None)] {
                    let expr = resolve(
                        cast(lit(ScalarValue::Int64(left)), data_type.clone()),
                        cast(lit(ScalarValue::Int64(right)), right_type.clone()),
                    )
                    .unwrap();
                    assert_eq!(expr.get_type(&schema).unwrap(), DataType::Int64);
                    let result = ctx
                        .sql("SELECT 1")
                        .await
                        .unwrap()
                        .select(vec![expr])
                        .unwrap()
                        .collect()
                        .await;
                    if ansi_mode && left.is_some() && right == Some(0) {
                        assert!(result.unwrap_err().to_string().contains("Divide by zero"));
                    } else {
                        let batches = result.unwrap();
                        assert_eq!(
                            ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
                            ScalarValue::Int64(None),
                        );
                    }
                }
            }
        }
        assert!(resolve(lit(7), lit(2)).is_ok());
        assert!(
            resolve(
                lit(ScalarValue::Decimal128(Some(750), 12, 2)),
                lit(ScalarValue::Decimal128(Some(200), 12, 2)),
            )
            .is_ok()
        );
    }
}

fn power(base: Expr, exponent: Expr) -> Expr {
    cast(expr_fn::power(base, exponent), DataType::Float64)
}

fn hypot(expr1: Expr, expr2: Expr) -> Expr {
    let sum_squared = expr1.clone() * expr1 + expr2.clone() * expr2;
    cast(expr_fn::sqrt(sum_squared), DataType::Float64)
}

fn positive(expr: Expr) -> Expr {
    expr
}

fn rint(expr: Expr) -> Expr {
    cast(expr_fn::round(vec![expr]), DataType::Float64)
}

fn positive_or_null(expr: Expr) -> Expr {
    Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(expr.clone().gt(lit(0_f64))), Box::new(expr))],
        else_expr: None,
    })
}

#[inline]
fn eulers_constant() -> Expr {
    lit(std::f64::consts::E)
}

fn ceil_floor(input: ScalarFunctionInput, name: &str) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let ansi_mode = function_context.plan_config.ansi_mode;
    // DataFusion bug: `ReturnTypeArgs.scalar_arguments` is None if scalar argument is nested
    let arguments = if arguments.len() == 2 {
        let (arg, target_scale) = arguments.two()?;
        let target_scale = match target_scale {
            Expr::Literal(_, _) => Ok(target_scale),
            Expr::Negative(negative) => {
                if let Expr::Literal(scalar, metadata) = *negative {
                    match scalar {
                        ScalarValue::Int8(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -v as i32)),
                            metadata,
                        )),
                        ScalarValue::Int16(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -v as i32)),
                            metadata,
                        )),
                        ScalarValue::Int32(v) => {
                            Ok(Expr::Literal(ScalarValue::Int32(v.map(|v| -v)), metadata))
                        }
                        ScalarValue::Int64(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt8(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt16(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt32(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        ScalarValue::UInt64(v) => Ok(Expr::Literal(
                            ScalarValue::Int32(v.map(|v| -(v as i32))),
                            metadata,
                        )),
                        other => Err(generic_exec_err(
                            "ceil",
                            format!("Target scale must be Integer literal, got {other}").as_str(),
                        )),
                    }
                } else {
                    Err(generic_exec_err(
                        "ceil",
                        format!("Target scale must be Integer literal, got {negative}").as_str(),
                    ))
                }
            }
            _ => Err(generic_exec_err(
                "ceil",
                format!("Target scale must be Integer literal, got {target_scale}").as_str(),
            )),
        }?;
        vec![arg, target_scale]
    } else {
        arguments
    };
    let func = if matches!(name.to_lowercase().trim(), "ceil") {
        Arc::new(ScalarUDF::from(SparkCeil::new(ansi_mode)))
    } else {
        Arc::new(ScalarUDF::from(SparkFloor::new(ansi_mode)))
    };
    Ok(Expr::ScalarFunction(expr::ScalarFunction {
        func,
        args: arguments,
    }))
}

fn ln(expr: Expr) -> Expr {
    expr_fn::ln(positive_or_null(expr))
}

fn log(base: Expr, num: Expr) -> Expr {
    expr_fn::log(base, positive_or_null(num))
}

fn log10(expr: Expr) -> Expr {
    expr_fn::log10(positive_or_null(expr))
}

fn log1p(expr: Expr) -> Expr {
    expr_fn::ln(positive_or_null(expr + lit(1.0_f64)))
}

fn log2(expr: Expr) -> Expr {
    expr_fn::log2(positive_or_null(expr))
}

fn double(func: impl Fn(Expr) -> Expr) -> impl Fn(Expr) -> Expr {
    move |arg: Expr| func(cast(arg, DataType::Float64))
}

fn double2(func: impl Fn(Expr, Expr) -> Expr) -> impl Fn(Expr, Expr) -> Expr {
    move |arg1: Expr, arg2| func(cast(arg1, DataType::Float64), cast(arg2, DataType::Float64))
}

#[test]
fn checked_modulo_respects_slices_nulls_and_scalar_positions() -> datafusion_common::Result<()> {
    use datafusion::arrow::array::{Array, Float64Array};
    use datafusion::arrow::buffer::NullBuffer;
    use datafusion::arrow::compute::cast as cast_array;
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
    for data_type in [DataType::Float32, DataType::Float64] {
        let udf = SparkCheckedModulo::new();
        let scalar = |v| -> datafusion_common::Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(v).cast_to(&data_type)?,
            ))
        };
        let array = |values: Vec<f64>,
                     valid: Vec<bool>,
                     offset,
                     len|
         -> datafusion_common::Result<ColumnarValue> {
            let a =
                Float64Array::new(values.into(), Some(NullBuffer::from(valid))).slice(offset, len);
            Ok(ColumnarValue::Array(cast_array(&a, &data_type)?))
        };
        let run = |left: ColumnarValue, right: ColumnarValue, len, host_ansi| {
            let mut config = ConfigOptions::default();
            config.execution.enable_ansi_mode = host_ansi;
            udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![left, right],
                number_rows: len,
                arg_fields: vec![
                    Arc::new(Field::new("a", data_type.clone(), true)),
                    Arc::new(Field::new("b", data_type.clone(), true)),
                ],
                return_field: Arc::new(Field::new("r", data_type.clone(), true)),
                config_options: Arc::new(config),
            })
        };
        for host_ansi in [false, true] {
            for (a, b, n, expected) in [
                (scalar(None)?, scalar(Some(-0.0))?, 1, vec![None]),
                (scalar(Some(-5.0))?, scalar(Some(2.0))?, 1, vec![Some(-1.0)]),
                (
                    array(vec![99.0, 5.0, 5.0], vec![true, false, true], 1, 2)?,
                    array(vec![1.0, -0.0, 2.0], vec![true, true, true], 1, 2)?,
                    2,
                    vec![None, Some(1.0)],
                ),
                (
                    array(vec![99.0, 5.0, 5.0], vec![true, false, false], 1, 2)?,
                    scalar(Some(0.0))?,
                    2,
                    vec![None, None],
                ),
                (
                    scalar(None)?,
                    array(vec![0.0, -0.0], vec![true, true], 0, 2)?,
                    2,
                    vec![None, None],
                ),
                (
                    scalar(Some(5.0))?,
                    array(vec![0.0, 2.0], vec![false, true], 0, 2)?,
                    2,
                    vec![None, Some(1.0)],
                ),
                (array(vec![], vec![], 0, 0)?, scalar(Some(0.0))?, 0, vec![]),
            ] {
                let result = run(a, b, n, host_ansi)?.into_array(n)?;
                assert_eq!(result.data_type(), &data_type);
                let values = cast_array(&result, &DataType::Float64)?;
                assert_eq!(
                    values
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    expected
                );
            }
            assert!(matches!(
                run(scalar(Some(5.0))?, scalar(Some(2.0))?, 1, host_ansi)?,
                ColumnarValue::Scalar(_)
            ));
            for zero in [0.0, -0.0] {
                for value in [0.0, 5.0, f64::NAN, f64::INFINITY] {
                    let error = run(scalar(Some(value))?, scalar(Some(zero))?, 1, host_ansi)
                        .unwrap_err()
                        .to_string();
                    assert!(error.contains("[REMAINDER_BY_ZERO]"), "{error}");
                    assert!(
                        run(
                            array(vec![0.0, value], vec![false, true], 0, 2)?,
                            array(vec![zero, zero], vec![true, true], 0, 2)?,
                            2,
                            host_ansi
                        )
                        .is_err()
                    );
                }
            }
        }
    }
    Ok(())
}

/// Modulo checks zero only after NULL propagation, and preserves native kernels.
fn spark_modulo(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (dividend, divisor) = arguments.two()?;
    let ansi_mode = function_context.plan_config.ansi_mode;
    let (dividend, divisor) = coerce_string_div_rem_operands(
        dividend,
        divisor,
        function_context.schema,
        ansi_mode,
        false,
    )?;
    let (dividend, divisor) = match (
        dividend.get_type(function_context.schema),
        divisor.get_type(function_context.schema),
    ) {
        (Ok(DataType::Null), Ok(DataType::Null)) => (
            cast(dividend, DataType::Float64),
            cast(divisor, DataType::Float64),
        ),
        (Ok(left_type), Ok(right_type)) => {
            let (dividend, divisor) = coerce_spark_arithmetic_operands(
                dividend,
                divisor,
                &left_type,
                &right_type,
                ansi_mode,
            );
            if is_decimal_type(&left_type) || is_decimal_type(&right_type) {
                coerce_decimal_peer_operand(dividend, divisor, function_context.schema)
            } else {
                (dividend, divisor)
            }
        }
        _ => (dividend, divisor),
    };
    let modulo = |left, right| {
        spark_decimal_arithmetic(
            left,
            right,
            Operator::Modulo,
            function_context.schema,
            ansi_mode,
        )
    };
    let data_type = modulo(dividend.clone(), divisor.clone()).get_type(function_context.schema)?;
    if !ansi_mode && is_zero_literal(&divisor) {
        // Legacy zero skips the dividend, including a dead failing CAST, but
        // still has the declared remainder result type.
        return Ok(lit(ScalarValue::try_from(&data_type)?));
    }
    if ansi_mode {
        return Ok(modulo(dividend, divisor));
    }
    let divisor_type = divisor.get_type(function_context.schema)?;
    let divisor = make_safe_divisor(divisor, &divisor_type, false, "Remainder by zero");
    Ok(modulo(dividend, divisor))
}

fn spark_abs(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkAbs::new(ansi_mode));
    Ok(udf.call(input.arguments))
}

fn spark_bin(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkBin::new(ansi_mode));
    Ok(udf.call(input.arguments))
}

fn spark_pmod(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkPmod::new(ansi_mode));
    Ok(udf.call(input.arguments))
}

/// Negate a numeric literal at planning time so a constant operand stays a
/// literal (some functions, e.g. `ceil`/`floor` target scale, require a literal
/// argument and run before the optimizer would fold a `SparkNegative` call).
/// Returns `None` when the value is not a foldable numeric literal or the
/// negation overflows (e.g. `-INT_MIN`), leaving such cases to the runtime UDF.
fn negate_literal(arg: &Expr) -> Option<Expr> {
    let Expr::Literal(value, _) = arg else {
        return None;
    };
    let negated = match value {
        ScalarValue::Int8(Some(v)) => ScalarValue::Int8(Some(v.checked_neg()?)),
        ScalarValue::Int16(Some(v)) => ScalarValue::Int16(Some(v.checked_neg()?)),
        ScalarValue::Int32(Some(v)) => ScalarValue::Int32(Some(v.checked_neg()?)),
        ScalarValue::Int64(Some(v)) => ScalarValue::Int64(Some(v.checked_neg()?)),
        ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(-v)),
        ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(-v)),
        _ => return None,
    };
    Some(lit(negated))
}

/// Spark unary minus / `negative(x)`. Duration negation goes through
/// `NegateDuration`; everything else uses `SparkNegative`, which honors the ANSI
/// overflow semantics with `ansi_mode` baked at planning time.
fn spark_unary_negate(arg: Expr, ansi_mode: bool, schema: &DFSchemaRef) -> Expr {
    match arg.get_type(schema) {
        // DataFusion's `Negative` doesn't support Duration types, so route those
        // to the dedicated UDF.
        Ok(DataType::Duration(_)) => ScalarUDF::from(NegateDuration::new()).call(vec![arg]),
        // Spark's unary minus coerces strings to DOUBLE before negating. The
        // cast honors ANSI mode: an invalid string is NULL under ANSI off and
        // errors under ANSI on. (Without this, the `SparkNegative` signature
        // would coerce the string to an interval instead.)
        Ok(DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            let casted = if ansi_mode {
                cast(arg, DataType::Float64)
            } else {
                try_cast(arg, DataType::Float64)
            };
            ScalarUDF::from(SparkNegative::new(ansi_mode)).call(vec![casted])
        }
        // Floating-point negation never overflows and is identical in both ANSI
        // modes, so use the native (vectorized, foldable) operator.
        Ok(DataType::Float16 | DataType::Float32 | DataType::Float64) => {
            Expr::Negative(Box::new(arg))
        }
        // A negated numeric literal folds to a literal so constant-arg functions
        // (e.g. `ceil`/`floor` target scale) still see a constant; overflow
        // (`-INT_MIN`) can't fold and falls through to the runtime UDF.
        _ => match negate_literal(&arg) {
            Some(folded) => folded,
            None => ScalarUDF::from(SparkNegative::new(ansi_mode)).call(vec![arg]),
        },
    }
}

fn spark_negative(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;
    Ok(spark_unary_negate(
        arg,
        function_context.plan_config.ansi_mode,
        function_context.schema,
    ))
}

fn spark_round_scale(
    name: &str,
    target: Expr,
    function_context: &FunctionContextInput,
) -> PlanResult<ScalarValue> {
    let target_type = target.get_type(function_context.schema)?;
    if !(target_type.is_numeric() || target_type.is_null() || target_type.is_string()) {
        return Err(PlanError::invalid(format!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] {name} scale requires INT",
        )));
    }
    if target.is_volatile() || !target.column_refs().is_empty() {
        return Err(PlanError::invalid(format!(
            "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] {name} scale must be constant",
        )));
    }
    let value = match target {
        Expr::Literal(value, _) => value,
        expression => {
            let context = SimplifyContextBuilder::default()
                .with_schema(function_context.schema.clone())
                .build();
            let coerced =
                ExprSimplifier::new(context).coerce(expression, function_context.schema)?;
            LiteralEvaluator::new().evaluate(&coerced)?
        }
    };
    let ansi = function_context.plan_config.ansi_mode;
    let target = if value.is_null() {
        ScalarValue::Int32(None)
    } else {
        match &value {
            ScalarValue::Int64(Some(v)) if !ansi => ScalarValue::Int32(Some(*v as i32)),
            ScalarValue::Float32(Some(v)) if !ansi => ScalarValue::Int32(Some(*v as i32)),
            ScalarValue::Float64(Some(v)) if !ansi => ScalarValue::Int32(Some(*v as i32)),
            ScalarValue::Decimal128(Some(v), _, s) if !ansi && *s >= 0 => {
                ScalarValue::Int32(Some((v / 10_i128.pow(*s as u32)) as i32))
            }
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value))
                if ansi =>
            {
                // Spark's UTF8String.toIntExact trims ASCII whitespace/control bytes.
                let value = value.trim_matches(|c| c <= ' ' || c == '\u{7f}');
                ScalarValue::Int32(Some(value.parse::<i32>().map_err(|e| {
                    PlanError::invalid(format!("[CAST_INVALID_INPUT] {name} scale: {e}"))
                })?))
            }
            _ if target_type.is_string() && !ansi => {
                use sail_function::scalar::spark_cast_string_to_integer::{
                    SparkCastStringToInteger, StringIntegerCastMode,
                };
                LiteralEvaluator::new().evaluate(
                    &ScalarUDF::from(SparkCastStringToInteger::try_new(
                        DataType::Int32,
                        StringIntegerCastMode::Legacy,
                    )?)
                    .call(vec![lit(value)]),
                )?
            }
            _ => value.cast_to(&DataType::Int32).map_err(|e| {
                PlanError::invalid(format!(
                    "[{}] {name} scale: {e}",
                    if target_type.is_string() {
                        "CAST_INVALID_INPUT"
                    } else {
                        "CAST_OVERFLOW"
                    },
                ))
            })?,
        }
    };
    Ok(target)
}

fn spark_bround(input: ScalarFunctionInput, udf: &ScalarUDF) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    if !(1..=2).contains(&arguments.len())
        || !matches!(
            arguments[0].get_type(function_context.schema)?,
            DataType::Decimal128(_, _)
        )
    {
        return Ok(udf.call(arguments));
    }
    let target = arguments.get(1).cloned().unwrap_or_else(|| lit(0_i32));
    let target = spark_round_scale("BROUND", target, &function_context)?;
    let null_scale = target.is_null();
    let call = udf.call(vec![arguments.remove(0), lit(target)]);
    if null_scale && !call.contains_scalar_subquery() {
        // Spark checks NULL scale before evaluating the input, even when it would fail.
        return Ok(lit(ScalarValue::try_from(
            &call.get_type(function_context.schema)?,
        )?));
    }
    Ok(call)
}

fn spark_decimal_round(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    if !(1..=2).contains(&arguments.len()) {
        return Err(PlanError::invalid(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] ROUND requires one or two arguments",
        ));
    }
    let mut value = arguments.remove(0);
    let value_type = value.get_type(function_context.schema)?;
    if !(value_type.is_numeric() || value_type.is_null() || value_type.is_string()) {
        return Err(PlanError::invalid(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] ROUND input requires a numeric value",
        ));
    }
    let target = arguments.pop().unwrap_or_else(|| lit(0_i32));
    if target.contains_scalar_subquery() {
        return Err(PlanError::invalid(
            "[DATATYPE_MISMATCH.NON_FOLDABLE_INPUT] ROUND scale must be constant",
        ));
    }
    let target = spark_round_scale("ROUND", target, &function_context)?;
    let ScalarValue::Int32(scale) = target else {
        return Err(PlanError::internal("ROUND scale was not resolved to INT"));
    };
    // The analyzer prepares subqueries before discarding the outer value at NULL scale.
    let has_subquery = value.contains_scalar_subquery();
    if value_type.is_string() {
        value = if function_context.plan_config.ansi_mode {
            cast(value, DataType::Float64)
        } else {
            try_cast(value, DataType::Float64)
        };
    }
    let call = if value_type == DataType::Float32 {
        ScalarUDF::from(SparkFloatRound::new(scale)).call(vec![value])
    } else if value_type.is_signed_integer() {
        ScalarUDF::from(SparkIntegerRound::new(
            scale,
            function_context.plan_config.ansi_mode,
        ))
        .call(vec![value])
    } else if let DataType::Decimal128(precision, input_scale) = value_type {
        ScalarUDF::from(SparkDecimalRound::new(
            precision,
            input_scale,
            scale.unwrap_or(0),
        ))
        .call(vec![value, lit(target)])
    } else {
        expr_fn::round(vec![value, lit(target)])
    };
    if scale.is_none() && !has_subquery {
        return Ok(lit(ScalarValue::try_from(
            &call.get_type(function_context.schema)?,
        )?));
    }
    Ok(call)
}

pub(super) fn list_built_in_math_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("%", F::custom(spark_modulo)),
        ("*", F::custom(spark_multiply)),
        ("+", F::custom(spark_plus)),
        ("-", F::custom(spark_minus)),
        ("/", F::custom(spark_divide)),
        ("abs", F::custom(spark_abs)),
        ("acos", F::unary(double(expr_fn::acos))),
        ("acosh", F::unary(double(expr_fn::acosh))),
        ("asin", F::unary(double(expr_fn::asin))),
        ("asinh", F::unary(double(expr_fn::asinh))),
        ("atan", F::unary(double(expr_fn::atan))),
        ("atan2", F::binary(double2(expr_fn::atan2))),
        ("atanh", F::unary(double(expr_fn::atanh))),
        ("bin", F::custom(spark_bin)),
        ("bround", {
            let udf = ScalarUDF::from(SparkBRound::new());
            F::custom(move |input| spark_bround(input, &udf))
        }),
        ("cbrt", F::unary(double(expr_fn::cbrt))),
        ("ceil", F::custom(|arg| ceil_floor(arg, "ceil"))),
        ("ceiling", F::custom(|arg| ceil_floor(arg, "ceil"))),
        ("conv", F::udf(SparkConv::new())),
        ("cos", F::unary(double(expr_fn::cos))),
        ("cosh", F::unary(double(expr_fn::cosh))),
        ("cot", F::unary(double(expr_fn::cot))),
        ("csc", F::unary(double(|arg| lit(1.0) / expr_fn::sin(arg)))),
        ("degrees", F::unary(double(expr_fn::degrees))),
        ("div", F::custom(spark_div)),
        ("e", F::nullary(eulers_constant)),
        ("exp", F::unary(double(expr_fn::exp))),
        ("expm1", F::unary(math_fn::expm1)),
        ("factorial", F::unary(expr_fn::factorial)),
        ("floor", F::custom(|arg| ceil_floor(arg, "floor"))),
        ("greatest", F::var_arg(expr_fn::greatest)),
        ("hex", F::unary(math_fn::hex)),
        ("hypot", F::binary(hypot)),
        ("least", F::var_arg(expr_fn::least)),
        ("ln", F::unary(double(ln))),
        ("log", F::binary(double2(log))),
        ("log10", F::unary(double(log10))),
        ("log1p", F::unary(double(log1p))),
        ("log2", F::unary(double(log2))),
        ("mod", F::custom(spark_modulo)),
        ("negative", F::custom(spark_negative)),
        ("pi", F::nullary(expr_fn::pi)),
        ("pmod", F::custom(spark_pmod)),
        ("positive", F::unary(positive)),
        ("pow", F::binary(power)),
        ("power", F::binary(power)),
        ("radians", F::unary(double(expr_fn::radians))),
        ("rand", F::udf(Random::new())),
        ("random_poisson", F::udf(RandPoisson::new())),
        ("randn", F::udf(Randn::new())),
        ("random", F::udf(Random::new())),
        ("rint", F::unary(rint)),
        ("round", F::custom(spark_decimal_round)),
        ("sec", F::unary(double(|arg| lit(1.0) / expr_fn::cos(arg)))),
        ("sign", F::udf(SparkSignum::new())),
        ("signum", F::udf(SparkSignum::new())),
        ("sin", F::unary(double(expr_fn::sin))),
        ("sinh", F::unary(double(expr_fn::sinh))),
        ("sqrt", F::unary(double(expr_fn::sqrt))),
        ("tan", F::unary(double(expr_fn::tan))),
        ("tanh", F::unary(double(expr_fn::tanh))),
        ("try_add", F::udf(SparkTryAdd::new())),
        ("try_divide", F::udf(SparkTryDiv::new())),
        ("try_multiply", F::udf(SparkTryMult::new())),
        ("try_mod", F::udf(SparkTryMod::new())),
        ("try_subtract", F::udf(SparkTrySubtract::new())),
        ("unhex", F::udf(SparkUnHex::new())),
        ("uniform", F::udf(SparkUniform::new())),
        ("width_bucket", F::quaternary(math_fn::width_bucket)),
    ]
}

#[cfg(test)]
#[tokio::test]
async fn arithmetic_coercion_preserves_literal_precision_and_ansi_float_width() -> PlanResult<()> {
    use crate::config::PlanConfig;
    use crate::resolver::PlanResolver;
    use arrow::array::{Array, Decimal128Array, Float64Array};
    use datafusion::prelude::SessionContext;
    for ansi in [true, false] {
        let ctx = SessionContext::new();
        let mut config = PlanConfig::default();
        config.ansi_mode = ansi;
        let resolver = PlanResolver::new(&ctx, Arc::new(config));
        for (operator, expected, decimal_type, coefficient, mixed) in [
            (
                "+",
                16777218.5,
                DataType::Decimal128(9, 2),
                325,
                [3.75, 3.75],
            ),
            (
                "-",
                -16777215.5,
                DataType::Decimal128(9, 2),
                125,
                [-0.75, 0.75],
            ),
            (
                "*",
                25165825.5,
                DataType::Decimal128(10, 2),
                225,
                [3.375, 3.375],
            ),
            ("%", 1.5, DataType::Decimal128(3, 2), 25, [1.5, 0.75]),
        ] {
            let sql = format!(
                "SELECT f {operator} i AS r,d {operator} 1 AS d,f {operator} d AS fd,d {operator} f AS df FROM (VALUES(CAST(1.5 AS FLOAT),16777217,CAST(2.25 AS DECIMAL(8,2))),(CAST(NULL AS FLOAT),1,CAST(NULL AS DECIMAL(8,2)))) t(f,i,d)"
            );
            let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let named = resolver.resolve_named_plan(spec).await?;
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
            let batch = &batches[0];
            assert_eq!(
                batch.column(0).data_type(),
                if ansi {
                    &DataType::Float64
                } else {
                    &DataType::Float32
                }
            );
            if ansi {
                assert_eq!(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(0),
                    expected
                );
            }
            let decimal = batch
                .column(1)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            assert_eq!(decimal.data_type(), &decimal_type);
            assert_eq!(decimal.value(0), coefficient);
            assert!(decimal.is_null(1) && batch.column(0).is_null(1));
            for (index, expected) in mixed.into_iter().enumerate() {
                let values = batch
                    .column(index + 2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(values.value(0), expected);
                assert!(values.is_null(1));
            }
        }
    }
    Ok(())
}
#[cfg(test)]
mod checked_integer_tests {
    use crate::{config::PlanConfig, resolver::PlanResolver};
    use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    async fn frame(
        ctx: &SessionContext,
        ansi: bool,
        sql: &str,
    ) -> datafusion::dataframe::DataFrame {
        let config = PlanConfig {
            ansi_mode: ansi,
            ..Default::default()
        };
        let ast = sail_sql_analyzer::parser::parse_one_statement(sql).unwrap();
        let spec = sail_sql_analyzer::statement::from_ast_statement(ast).unwrap();
        let plan = PlanResolver::new(ctx, Arc::new(config))
            .resolve_named_plan(spec)
            .await
            .unwrap();
        ctx.execute_logical_plan(plan.plan).await.unwrap()
    }

    #[tokio::test]
    async fn string_division_uses_query_coercion_on_native_tables() {
        use arrow::array::{Float64Array, LargeStringArray, StringArray, StringViewArray};
        use datafusion::prelude::SessionConfig;
        for host_ansi in [false, true] {
            let mut settings = SessionConfig::new();
            settings.options_mut().execution.enable_ansi_mode = host_ansi;
            let ctx = SessionContext::new_with_config(settings);
            let strings = vec![Some(" \t+7\n"), Some("-9"), None];
            for input in [
                Arc::new(StringArray::from(strings.clone())) as ArrayRef,
                Arc::new(LargeStringArray::from(strings.clone())) as ArrayRef,
                Arc::new(StringViewArray::from(strings.clone())) as ArrayRef,
            ] {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("a", input.data_type().clone(), true),
                    Field::new("b", DataType::Int32, false),
                ]));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![input, Arc::new(Int32Array::from(vec![2, 2, 0]))],
                )
                .unwrap();
                ctx.deregister_table("strings").unwrap();
                ctx.register_table(
                    "strings",
                    Arc::new(
                        MemTable::try_new(
                            schema.clone(),
                            vec![vec![RecordBatch::new_empty(schema), batch]],
                        )
                        .unwrap(),
                    ),
                )
                .unwrap();
                for query_ansi in [false, true] {
                    for sql in [
                        "SELECT a / b AS r FROM strings",
                        "SELECT `/`(a,b) AS r FROM strings",
                    ] {
                        let out = frame(&ctx, query_ansi, sql).await.collect().await.unwrap();
                        let actual = out
                            .iter()
                            .flat_map(|batch| {
                                batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<Float64Array>()
                                    .unwrap()
                                    .iter()
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(actual, vec![Some(3.5), Some(-4.5), None]);
                    }
                    for sql in [
                        "SELECT a DIV b AS r FROM strings",
                        "SELECT div(a,b) AS r FROM strings",
                    ] {
                        let ast = sail_sql_analyzer::parser::parse_one_statement(sql).unwrap();
                        let spec = sail_sql_analyzer::statement::from_ast_statement(ast).unwrap();
                        let plan = PlanResolver::new(
                            &ctx,
                            Arc::new(PlanConfig {
                                ansi_mode: query_ansi,
                                ..Default::default()
                            }),
                        )
                        .resolve_named_plan(spec)
                        .await;
                        if query_ansi {
                            let out = ctx
                                .execute_logical_plan(plan.unwrap().plan)
                                .await
                                .unwrap()
                                .collect()
                                .await
                                .unwrap();
                            let actual = out
                                .iter()
                                .flat_map(|batch| {
                                    batch
                                        .column(0)
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap()
                                        .iter()
                                })
                                .collect::<Vec<_>>();
                            assert_eq!(actual, vec![Some(3), Some(-4), None]);
                        } else {
                            assert!(
                                plan.err()
                                    .unwrap()
                                    .to_string()
                                    .contains("[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES]")
                            );
                        }
                    }
                }
            }
            // Explicit per-query casts must stay independent of host ANSI mode.
            let sql = "SELECT 'bad' / CAST(2 AS DECIMAL(10,2)) AS r";
            let out = frame(&ctx, false, sql).await.collect().await.unwrap();
            assert!(out[0].column(0).is_null(0));
            let ast = sail_sql_analyzer::parser::parse_one_statement(sql).unwrap();
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast).unwrap();
            let plan = PlanResolver::new(
                &ctx,
                Arc::new(PlanConfig {
                    ansi_mode: true,
                    ..Default::default()
                }),
            )
            .resolve_named_plan(spec)
            .await
            .unwrap();
            let failure = async { ctx.execute_logical_plan(plan.plan).await?.collect().await }
                .await
                .unwrap_err();
            assert!(failure.to_string().contains("Cannot cast"));
            assert_eq!(
                ctx.state().config_options().execution.enable_ansi_mode,
                host_ansi
            );
        }
    }

    #[tokio::test]
    async fn legacy_bigint_divide_is_query_scoped() {
        use datafusion::prelude::SessionConfig;
        for host_ansi in [false, true] {
            let mut settings = SessionConfig::new();
            settings.options_mut().execution.enable_ansi_mode = host_ansi;
            let ctx = SessionContext::new_with_config(settings);
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Int64, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![
                        Some(i64::MIN),
                        Some(i64::MIN + 1),
                        None,
                        Some(7),
                    ])),
                    Arc::new(Int64Array::from(vec![Some(-1), Some(-1), Some(0), Some(2)])),
                ],
            )
            .unwrap();
            ctx.register_table(
                "bounds",
                Arc::new(
                    MemTable::try_new(
                        schema.clone(),
                        vec![vec![RecordBatch::new_empty(schema), batch]],
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
            for sql in [
                "SELECT a DIV b AS q FROM bounds",
                "SELECT div(a,b) AS q FROM bounds",
            ] {
                for _ in 0..2 {
                    let ansi = frame(&ctx, true, sql).await;
                    let legacy = frame(&ctx, false, sql).await;
                    let (error, result) = tokio::join!(ansi.collect(), legacy.collect());
                    assert!(
                        error
                            .unwrap_err()
                            .to_string()
                            .contains("Overflow happened on: -9223372036854775808 / -1")
                    );
                    let values = result
                        .unwrap()
                        .iter()
                        .flat_map(|batch| {
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .iter()
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(values, vec![Some(i64::MIN), Some(i64::MAX), None, Some(3)]);
                    assert_eq!(
                        ctx.state().config_options().execution.enable_ansi_mode,
                        host_ansi
                    );
                }
            }
            let constant_divisor = frame(&ctx, false, "SELECT a DIV 2L AS q FROM bounds").await;
            assert!(constant_divisor.schema().field(0).is_nullable());
            let values = constant_divisor.collect().await.unwrap();
            let values = values
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                values,
                vec![Some(i64::MIN / 2), Some((i64::MIN + 1) / 2), None, Some(3)]
            );
            for query_ansi in [false, true] {
                for sql in [
                    "SELECT a DIV b AS q FROM bounds WHERE a=7",
                    "SELECT CASE WHEN a=7 THEN a DIV b ELSE 0L END AS q FROM bounds",
                    "SELECT a DIV b AS q FROM bounds WHERE false",
                ] {
                    frame(&ctx, query_ansi, sql).await.collect().await.unwrap();
                }
            }
        }
    }

    #[tokio::test]
    async fn checked_integer_columns_and_query_isolation() {
        let ctx = SessionContext::new_with_state(
            datafusion::execution::SessionStateBuilder::new()
                .with_default_features()
                .with_analyzer_rule(Arc::new(
                    crate::expression_errors::PreserveSparkExpressionErrors,
                ))
                .build(),
        );
        let host_ansi = ctx.state().config_options().execution.enable_ansi_mode;
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(i32::MAX),
                    Some(i32::MIN),
                    None,
                    Some(0),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(-1),
                    Some(i32::MAX),
                    Some(1),
                ])),
            ],
        )
        .unwrap();
        // Empty partitions/batches must not raise an overflow or change types.
        let empty = RecordBatch::new_empty(schema.clone());
        ctx.register_table(
            "bounds",
            Arc::new(MemTable::try_new(schema, vec![vec![empty], vec![batch]]).unwrap()),
        )
        .unwrap();
        for (op, rhs, wrapped) in [("+", "b", i32::MIN), ("-", "-b", i32::MIN), ("*", "2", -2)] {
            let sql = format!("SELECT a {op} {rhs} AS r FROM bounds");
            for _ in 0..2 {
                let ansi = frame(&ctx, true, &sql).await;
                let legacy = frame(&ctx, false, &sql).await;
                assert!(
                    ansi.logical_plan()
                        .display_indent()
                        .to_string()
                        .contains("spark_checked_")
                );
                assert!(
                    !legacy
                        .logical_plan()
                        .display_indent()
                        .to_string()
                        .contains("spark_checked_")
                );
                let (error, result) = tokio::join!(ansi.collect(), legacy.collect());
                assert!(
                    error
                        .unwrap_err()
                        .to_string()
                        .contains("[ARITHMETIC_OVERFLOW]")
                );
                let result = result.unwrap();
                let values = result
                    .iter()
                    .flat_map(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .iter()
                    })
                    .collect::<Vec<_>>();
                assert!(values.contains(&Some(wrapped)));
                assert!(values.contains(&None));
                assert_eq!(
                    ctx.state().config_options().execution.enable_ansi_mode,
                    host_ansi
                );
            }
            for sql in [
                format!("SELECT a {op} {rhs} AS r FROM bounds WHERE a = 0"),
                format!("SELECT CASE WHEN a = 0 THEN a {op} {rhs} ELSE 0 END AS r FROM bounds"),
                format!("SELECT a {op} {rhs} AS r FROM bounds WHERE false"),
            ] {
                frame(&ctx, true, &sql).await.collect().await.unwrap();
            }
        }
        // NULL on the left suppresses a failing right child, without evaluating
        // even a volatile left operand twice.
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&calls);
        ctx.register_udf(datafusion_expr::create_udf(
            "counted",
            vec![DataType::Int32],
            DataType::Int32,
            datafusion_expr::Volatility::Volatile,
            Arc::new(move |args| {
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(args[0].clone())
            }),
        ));
        ctx.register_batch(
            "guarded",
            RecordBatch::try_from_iter(vec![
                (
                    "n",
                    Arc::new(Int32Array::from(vec![None, Some(1)])) as ArrayRef,
                ),
                (
                    "a",
                    Arc::new(Int32Array::from(vec![i32::MAX, 0])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .unwrap();
        let output = frame(&ctx, true, "SELECT counted(n) + (a + 1) AS r FROM guarded")
            .await
            .collect()
            .await
            .unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let values = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(values.is_null(0));
        assert_eq!(values.value(1), 2);
        let output = frame(
            &ctx,
            true,
            "SELECT CAST(NULL AS INT) + (a + 1) AS r FROM guarded",
        )
        .await
        .collect()
        .await
        .unwrap();
        assert!(
            output
                .iter()
                .all(|b| b.column(0).null_count() == b.num_rows())
        );

        // Logical projection collapse can replace a RHS column with a fallible
        // expression. NULL propagation still differs for eagerly evaluated VALUES.
        for (op, bound, step) in [
            ("+", 2147483647i64, 1),
            ("-", -2147483648, 1),
            ("*", 1073741824, 2),
        ] {
            for (source, fails) in [("range(2)", false), ("(VALUES(0),(1)) t(id)", true)] {
                let sql = format!(
                    "SELECT n {op} r AS r FROM (SELECT CAST(NULL AS INT) AS n, CAST({bound} AS INT) {op} CAST(id+{step} AS INT) AS r FROM {source}) q"
                );
                let result = frame(&ctx, true, &sql).await.collect().await;
                if fails {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .contains("[ARITHMETIC_OVERFLOW]")
                    );
                } else {
                    assert!(
                        result
                            .unwrap()
                            .iter()
                            .all(|b| b.column(0).null_count() == b.num_rows())
                    );
                }
            }
            let sql = format!("SELECT ({bound} {op} {step}) {op} CAST(NULL AS INT) AS r");
            let result = frame(&ctx, true, &sql).await.collect().await.unwrap();
            assert!(result[0].column(0).is_null(0));
        }

        // Real arrays at each signed width, with a masked overflowing payload.
        for (ty, max) in [
            (DataType::Int8, i8::MAX as i64),
            (DataType::Int16, i16::MAX as i64),
            (DataType::Int32, i32::MAX as i64),
            (DataType::Int64, i64::MAX),
        ] {
            let a = Int64Array::new(vec![max, 1].into(), Some(vec![false, true].into()));
            let a = arrow::compute::cast(&a, &ty).unwrap();
            let b: ArrayRef = arrow::compute::cast(&Int64Array::from(vec![1, 1]), &ty).unwrap();
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", ty.clone(), true),
                Field::new("b", ty.clone(), false),
            ]));
            let batch = RecordBatch::try_new(schema.clone(), vec![a, b]).unwrap();
            ctx.deregister_table("masked").unwrap();
            ctx.register_table(
                "masked",
                Arc::new(
                    MemTable::try_new(
                        schema.clone(),
                        vec![vec![RecordBatch::new_empty(schema), batch]],
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
            for (op, value) in [("+", 2), ("-", 0), ("*", 1)] {
                for sql in [
                    format!("SELECT a {op} b AS r FROM masked"),
                    format!(
                        "SELECT a {op} CAST(1 AS {ty_name}) AS r FROM masked",
                        ty_name = match ty {
                            DataType::Int8 => "TINYINT",
                            DataType::Int16 => "SMALLINT",
                            DataType::Int32 => "INT",
                            _ => "BIGINT",
                        }
                    ),
                ] {
                    let batches = frame(&ctx, true, &sql).await.collect().await.unwrap();
                    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
                    assert_eq!(batch.column(0).data_type(), &ty);
                    assert!(batch.column(0).is_null(0));
                    let scalar = ScalarValue::try_from_array(batch.column(0), 1)
                        .unwrap()
                        .cast_to(&DataType::Int64)
                        .unwrap();
                    assert_eq!(scalar, ScalarValue::Int64(Some(value)));
                }
            }
        }
    }
}

#[cfg(test)]
#[tokio::test]
async fn round_arguments_preserve_coercion_and_null_scale() -> PlanResult<()> {
    use crate::{config::PlanConfig, resolver::PlanResolver};
    use datafusion::arrow::{array::Array, util::display::array_value_to_string};
    use datafusion::prelude::SessionContext;
    for ansi in [true, false] {
        let ctx = SessionContext::new();
        let config = PlanConfig {
            ansi_mode: ansi,
            ..Default::default()
        };
        let resolver = PlanResolver::new(&ctx, Arc::new(config));
        for (expression, expected_type, expected) in [
            (
                "ROUND(CAST(1.255 AS DECIMAL(10,3)), ' 2 ')",
                DataType::Decimal128(10, 2),
                Some("1.26"),
            ),
            (
                "ROUND(CAST(1.255 AS DECIMAL(10,3)), 2.9)",
                DataType::Decimal128(10, 2),
                Some("1.26"),
            ),
            ("ROUND('1.25', 1)", DataType::Float64, Some("1.3")),
            (
                "ROUND(CAST('bad' AS DECIMAL(10,3)), NULL)",
                DataType::Decimal128(8, 0),
                None,
            ),
            (
                "BROUND(CAST(2.345 AS DECIMAL(6,3)), ' 2 ')",
                DataType::Decimal128(6, 2),
                Some("2.34"),
            ),
        ] {
            let ast = sail_sql_analyzer::parser::parse_one_statement(&format!(
                "SELECT {expression} AS r"
            ))?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let plan = resolver.resolve_named_plan(spec).await?;
            let batches = ctx.execute_logical_plan(plan.plan).await?.collect().await?;
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
            let column = batches[0].column(0);
            assert_eq!(column.data_type(), &expected_type);
            let actual = if column.is_null(0) {
                None
            } else {
                Some(array_value_to_string(column, 0)?)
            };
            assert_eq!(actual.as_deref(), expected, "{expression}, ANSI {ansi}");
        }
        for (scale, error) in [
            ("'bad'", "CAST_INVALID_INPUT"),
            ("2147483648L", "CAST_OVERFLOW"),
        ] {
            let ast = sail_sql_analyzer::parser::parse_one_statement(&format!(
                "SELECT ROUND(CAST(0 AS DECIMAL(10,3)), {scale}) AS r"
            ))?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let plan = resolver.resolve_named_plan(spec).await;
            if ansi {
                assert!(plan.err().unwrap().to_string().contains(error));
            } else {
                let batches = ctx
                    .execute_logical_plan(plan?.plan)
                    .await?
                    .collect()
                    .await?;
                let column = batches[0].column(0);
                assert_eq!(column.data_type(), &DataType::Decimal128(8, 0));
                assert_eq!(column.is_null(0), scale == "'bad'");
            }
        }
        for (scale, condition) in [
            ("true", "UNEXPECTED_INPUT_TYPE"),
            ("(SELECT 2)", "NON_FOLDABLE_INPUT"),
        ] {
            let ast = sail_sql_analyzer::parser::parse_one_statement(&format!(
                "SELECT ROUND(CAST(1.255 AS DECIMAL(10,3)), {scale}) AS r"
            ))?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            assert!(
                resolver
                    .resolve_named_plan(spec)
                    .await
                    .err()
                    .unwrap()
                    .to_string()
                    .contains(condition)
            );
        }
    }
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn integer_round_uses_query_ansi_and_preserves_types() -> PlanResult<()> {
    use crate::{config::PlanConfig, resolver::PlanResolver};
    use datafusion::arrow::{array::Array, util::display::array_value_to_string};
    use datafusion::prelude::{SessionConfig, SessionContext};
    for host_ansi in [false, true] {
        let mut settings = SessionConfig::new();
        settings.options_mut().execution.enable_ansi_mode = host_ansi;
        let ctx = SessionContext::new_with_config(settings);
        for ansi in [false, true] {
            let resolver = PlanResolver::new(
                &ctx,
                Arc::new(PlanConfig {
                    ansi_mode: ansi,
                    ..Default::default()
                }),
            );
            for (expression, data_type, expected) in [
                (
                    "ROUND(CAST(127 AS TINYINT), -1)",
                    DataType::Int8,
                    if ansi { None } else { Some("-126") },
                ),
                (
                    "ROUND(9007199254740993L, 2)",
                    DataType::Int64,
                    Some("9007199254740993"),
                ),
                (
                    "ROUND(5000000000000000000L, -19)",
                    DataType::Int64,
                    if ansi {
                        None
                    } else {
                        Some("-8446744073709551616")
                    },
                ),
                (
                    "ROUND(CAST(25 AS SMALLINT), NULL)",
                    DataType::Int16,
                    Some("NULL"),
                ),
            ] {
                let actual: PlanResult<_> = async {
                    let ast = sail_sql_analyzer::parser::parse_one_statement(&format!(
                        "SELECT {expression} AS r"
                    ))?;
                    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                    let plan = resolver.resolve_named_plan(spec).await?;
                    Ok(ctx.execute_logical_plan(plan.plan).await?.collect().await?)
                }
                .await;
                if let Some(expected) = expected {
                    let batches = actual?;
                    let column = batches[0].column(0);
                    assert_eq!(column.data_type(), &data_type);
                    if expected == "NULL" {
                        assert!(column.is_null(0));
                    } else {
                        assert_eq!(array_value_to_string(column, 0)?, expected);
                    }
                } else {
                    assert!(
                        actual
                            .unwrap_err()
                            .to_string()
                            .contains("[ARITHMETIC_OVERFLOW]")
                    );
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn decimal_arithmetic_keeps_exact_intermediates_and_spark_result_types() -> PlanResult<()> {
    use crate::config::PlanConfig;
    use crate::resolver::PlanResolver;
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::Decimal128Type;
    use datafusion::prelude::SessionContext;
    for ansi in [true, false] {
        let ctx = SessionContext::new();
        let mut config = PlanConfig::default();
        config.ansi_mode = ansi;
        let resolver = PlanResolver::new(&ctx, Arc::new(config));
        for (a, b, op, precision, scale, expected) in [
            (
                "CAST(0.123456789012345675 AS DECIMAL(38,18))",
                "1",
                "+",
                38,
                17,
                "112345678901234568",
            ),
            (
                "CAST(0.123456789012345675 AS DECIMAL(38,18))",
                "1",
                "-",
                38,
                17,
                "-87654321098765433",
            ),
            (
                "CAST(0.123456789012345675 AS DECIMAL(38,18))",
                "1",
                "*",
                38,
                7,
                "1234568",
            ),
            (
                "CAST(0.99999999999999999999999999999999999999 AS DECIMAL(38,38))",
                "CAST(0.99999999999999999999999999999999999999 AS DECIMAL(38,38))",
                "*",
                38,
                37,
                "10000000000000000000000000000000000000",
            ),
            (
                "CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))",
                "CAST(0.12345678901234567890123456789012345678 AS DECIMAL(38,38))",
                "%",
                38,
                38,
                "8271507798827209828883258328793215726",
            ),
            (
                "CAST(2.25 AS DECIMAL(8,2))",
                "CAST(1 AS DECIMAL(1,0))",
                "%",
                3,
                2,
                "25",
            ),
        ] {
            let sql = format!("SELECT a {op} b AS r FROM VALUES ({a},{b}),(NULL,NULL) t(a,b)");
            let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let named = resolver.resolve_named_plan(spec).await?;
            assert_eq!(
                named.plan.schema().field(0).data_type(),
                &DataType::Decimal128(precision, scale),
                "{sql}"
            );
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
            let values = batches[0].column(0).as_primitive::<Decimal128Type>();
            assert_eq!(values.data_type(), &DataType::Decimal128(precision, scale));
            assert_eq!(values.value(0), expected.parse::<i128>().unwrap(), "{sql}");
            assert!(values.is_null(1), "{sql}");
        }
        for op in ["+", "-", "*"] {
            let b = if op == "-" { "-2" } else { "2" };
            let sql = format!(
                "SELECT a {op} b AS r FROM VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)),{b}),(NULL,NULL) t(a,b)"
            );
            let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let named = resolver.resolve_named_plan(spec).await?;
            let result = ctx.execute_logical_plan(named.plan).await?.collect().await;
            if ansi {
                assert!(result.is_err(), "{sql}");
            } else {
                let batches = result?;
                assert_eq!(
                    batches[0].column(0).data_type(),
                    &DataType::Decimal128(38, 0)
                );
                assert_eq!(batches[0].column(0).null_count(), 2);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn modulo_one_simplifies_only_integer_operands() -> datafusion_common::Result<()> {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion_common::ToDFSchema;
    use datafusion_expr::{col, simplify::SimplifyContext};
    for data_type in [
        DataType::Decimal32(8, 2),
        DataType::Decimal64(12, 2),
        DataType::Decimal128(18, 4),
        DataType::Decimal256(39, 4),
        DataType::Float32,
        DataType::Float64,
        DataType::Int64,
    ] {
        let schema =
            Schema::new(vec![Field::new("a", data_type.clone(), false)]).to_dfschema_ref()?;
        let simplifier =
            ExprSimplifier::new(SimplifyContext::builder().with_schema(schema).build());
        let expression = col("a") % lit(ScalarValue::new_one(&data_type)?);
        let simplified = simplifier.simplify(expression.clone())?;
        if data_type.is_integer() {
            assert_eq!(simplified, lit(0_i64));
        } else {
            assert_eq!(simplified, expression, "{data_type:?}");
        }
    }
    Ok(())
}

#[cfg(test)]
#[tokio::test]
async fn string_modulo_reuses_spark_coercion_and_null_ordering() -> PlanResult<()> {
    use crate::{
        config::PlanConfig, expression_errors::PreserveSparkExpressionErrors,
        physical_plan::SparkQueryPlanner, resolver::PlanResolver,
    };
    use datafusion::{execution::SessionStateBuilder, prelude::SessionContext};
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
            .build(),
    );
    for ansi in [true, false, true] {
        for token in ["%", "MOD"] {
            for (peer, integral) in [
                ("BIGINT", true),
                ("DOUBLE", false),
                ("DECIMAL(10,2)", false),
            ] {
                for (rows, cast_error, expected) in [
                    (
                        format!("(0,'bad',CAST(NULL AS {peer})),(1,'-7',CAST(2 AS {peer}))"),
                        false,
                        vec![None, Some(-1.0)],
                    ),
                    (format!("(0,'bad',CAST(2 AS {peer}))"), true, vec![None]),
                    (format!("(0,CAST(NULL AS {peer}),'bad')"), true, vec![None]),
                ] {
                    let expr = if token == "%" { "a % b" } else { "MOD(a,b)" };
                    let sql = format!("SELECT {expr} AS r FROM VALUES {rows} t(k,a,b) ORDER BY k");
                    let result = async {
                        let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                        let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                        let config = PlanConfig {
                            ansi_mode: ansi,
                            ..Default::default()
                        };
                        let named = PlanResolver::new(&ctx, Arc::new(config))
                            .resolve_named_plan(spec)
                            .await?;
                        let typ = if ansi && integral {
                            DataType::Int64
                        } else {
                            DataType::Float64
                        };
                        assert_eq!(named.plan.schema().field(0).data_type(), &typ, "{sql}");
                        let batches = ctx
                            .execute_logical_plan(named.plan)
                            .await?
                            .collect()
                            .await?;
                        let mut values = vec![];
                        for batch in batches {
                            for row in 0..batch.num_rows() {
                                let value = ScalarValue::try_from_array(batch.column(0), row)?
                                    .cast_to(&DataType::Float64)?;
                                let ScalarValue::Float64(value) = value else {
                                    unreachable!()
                                };
                                values.push(value);
                            }
                        }
                        Ok::<_, PlanError>(values)
                    }
                    .await;
                    if ansi && cast_error {
                        assert!(
                            result
                                .unwrap_err()
                                .to_string()
                                .contains("Cannot cast string 'bad'"),
                            "{sql}"
                        );
                    } else {
                        assert_eq!(result?, expected, "ANSI {ansi}: {sql}");
                    }
                }
            }
        }
    }
    Ok(())
}
