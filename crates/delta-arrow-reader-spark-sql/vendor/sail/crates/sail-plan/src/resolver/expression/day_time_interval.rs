// Local Spark DATE and day-time interval arithmetic. See the host crate's UPSTREAM.md.
//! The end field of a day-time interval operand, which decides Spark's DATE arithmetic: a DATE
//! plus or minus an interval ending at DAY is a DATE, and one with time units is a TIMESTAMP
//! (#952). DAR types every day-time interval as a microsecond Duration, so the end field comes
//! from the SQL, where the operand writes its qualifier, or from the function that computed it.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::DFSchemaRef;
use datafusion_expr::{BinaryExpr, ExprSchemable, Operator, expr};
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use sail_common::spec;
use sail_function::scalar::datetime::spark_subtract_dates::SparkSubtractDates;
use sail_function::scalar::datetime::spark_subtract_timestamps::SparkSubtractTimestamps;
use sail_function::scalar::math::spark_interval_arithmetic::SparkIntervalArithmetic;
use sail_function::scalar::math::spark_interval_negate::SparkIntervalNegate;
use sail_function::scalar::math::spark_interval_scale::SparkIntervalScale;

use crate::function::common::without_inherited_column_metadata;
use crate::function::temporal_coercion::cast_local_timestamp_expr_to_ltz;

/// Where a day-time interval ends: at DAY, as Spark's `INTERVAL DAY`, or at HOUR, MINUTE or
/// SECOND.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DayTimeEnd {
    Day,
    Time,
}

impl DayTimeEnd {
    fn of_field(field: spec::IntervalFieldType) -> Self {
        match field {
            spec::IntervalFieldType::Day => Self::Day,
            _ => Self::Time,
        }
    }

    /// The end of intervals combined into one, as a sum, a difference or the branches of a
    /// conditional: Spark widens the qualifier, so it ends at a time unit if any does, and at
    /// DAY only if all do.
    fn widen(ends: impl IntoIterator<Item = Option<Self>>) -> Option<Self> {
        let mut all_day = Some(Self::Day);
        for end in ends {
            match end {
                Some(Self::Time) => return Some(Self::Time),
                Some(Self::Day) => {}
                None => all_day = None,
            }
        }
        all_day
    }
}

/// The end field of `expr` where its SQL writes it, read only when `expr` resolves to a
/// day-time interval: an interval literal, a CAST to an interval qualifier, a negation, a sum
/// or difference of such intervals, a conditional over them, ABS and the aggregates that keep
/// their argument's type. Spark types an interval scaled by a number, MAKE_DT_INTERVAL and AVG
/// as `DAY TO SECOND`. `None` where the SQL does not say, such as for a column.
pub(super) fn spec_day_time_end(expr: &spec::Expr) -> Option<DayTimeEnd> {
    match expr {
        spec::Expr::Literal(spec::Literal::DurationMicrosecond {
            start_field,
            end_field,
            ..
        }) => end_field.or(*start_field).map(DayTimeEnd::of_field),
        spec::Expr::Cast {
            cast_to_type:
                spec::DataType::Interval {
                    interval_unit: spec::IntervalUnit::DayTime,
                    start_field,
                    end_field,
                },
            ..
        } => end_field.or(*start_field).map(DayTimeEnd::of_field),
        spec::Expr::UnresolvedFunction(function) => {
            let [name] = function.function_name.parts() else {
                return None;
            };
            let name = name.as_ref().to_ascii_lowercase();
            let arguments = function.arguments.as_slice();
            match (name.as_str(), arguments) {
                ("-" | "+" | "negative" | "positive", [operand]) => spec_day_time_end(operand),
                ("-" | "+" | "try_add" | "try_subtract", [_, _])
                | ("coalesce" | "nvl" | "ifnull" | "greatest" | "least", _) => {
                    DayTimeEnd::widen(arguments.iter().map(spec_day_time_end))
                }
                ("if" | "nvl2", [_, then, otherwise]) => {
                    DayTimeEnd::widen([then, otherwise].map(spec_day_time_end))
                }
                // CASE: each condition precedes its result, and an ELSE result comes last.
                ("when", _) => DayTimeEnd::widen(
                    arguments
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| index % 2 == 1 || *index + 1 == arguments.len())
                        .map(|(_, result)| spec_day_time_end(result)),
                ),
                (
                    "nullif" | "abs" | "sum" | "min" | "max" | "first" | "first_value" | "last"
                    | "last_value" | "any_value",
                    [value, ..],
                ) => spec_day_time_end(value),
                ("*" | "/" | "try_multiply" | "try_divide", [_, _])
                | ("avg" | "mean" | "make_dt_interval", _) => Some(DayTimeEnd::Time),
                _ => None,
            }
        }
        spec::Expr::Window {
            window_function, ..
        } => spec_day_time_end(window_function),
        _ => None,
    }
}

/// The end field of a resolved day-time interval `expr` where the function that computed it
/// fixes it: Spark subtracts two DATE values as a DAY interval, and two TIMESTAMP or
/// TIMESTAMP_NTZ values, MAKE_DT_INTERVAL and an interval scaled by a number as `DAY TO SECOND`.
/// `None` otherwise, such as for a literal, whose qualifier only its SQL keeps, or a column.
pub(super) fn resolved_day_time_end(expr: &expr::Expr) -> Option<DayTimeEnd> {
    let expr = match without_inherited_column_metadata(expr) {
        expr::Expr::Alias(alias) => alias.expr.as_ref(),
        expr => expr,
    };
    let expr = without_inherited_column_metadata(expr);
    // DATE, TIMESTAMP and interval subtractions are Spark functions here, so a native
    // subtraction that gives a day-time interval subtracts two TIMESTAMP_NTZ values.
    if let expr::Expr::BinaryExpr(BinaryExpr {
        op: Operator::Minus,
        ..
    }) = expr
    {
        return Some(DayTimeEnd::Time);
    }
    let expr::Expr::ScalarFunction(function) = expr else {
        return None;
    };
    let inner = function.func.inner();
    if inner.is::<SparkSubtractDates>() {
        Some(DayTimeEnd::Day)
    } else if inner.is::<SparkSubtractTimestamps>()
        || inner.is::<SparkMakeDtInterval>()
        || inner.is::<SparkIntervalScale>()
    {
        Some(DayTimeEnd::Time)
    } else if inner.is::<SparkIntervalNegate>()
        && let [operand] = function.args.as_slice()
    {
        resolved_day_time_end(operand)
    } else if inner.is::<SparkIntervalArithmetic>()
        && let [left, right] = function.args.as_slice()
    {
        DayTimeEnd::widen([left, right].map(resolved_day_time_end))
    } else {
        None
    }
}

/// The resolved `arguments` of `function`, `+`, `-`, TRY_ADD or TRY_SUBTRACT, with a DATE beside
/// a day-time interval with time units read as a TIMESTAMP at its midnight in `timezone`, as
/// Spark casts it before adding the interval (#952). `spec_ends` holds each argument's end field
/// from its SQL. A DATE beside an interval ending at DAY, or one whose end field is unknown,
/// keeps DATE arithmetic.
pub(super) fn cast_date_beside_time_interval(
    function: &str,
    mut arguments: Vec<expr::Expr>,
    spec_ends: &[Option<DayTimeEnd>],
    schema: &DFSchemaRef,
    timezone: &Arc<str>,
) -> Vec<expr::Expr> {
    let interval = DataType::Duration(TimeUnit::Microsecond);
    let date_index = match arguments.as_slice() {
        [left, right]
            if left.get_type(schema).is_ok_and(|t| t == DataType::Date32)
                && right.get_type(schema).is_ok_and(|t| t == interval) =>
        {
            0
        }
        [left, right]
            if matches!(function, "+" | "try_add")
                && left.get_type(schema).is_ok_and(|t| t == interval)
                && right.get_type(schema).is_ok_and(|t| t == DataType::Date32) =>
        {
            1
        }
        _ => return arguments,
    };
    let interval_index = 1 - date_index;
    let end = spec_ends
        .get(interval_index)
        .copied()
        .flatten()
        .or_else(|| resolved_day_time_end(&arguments[interval_index]));
    if end == Some(DayTimeEnd::Time) {
        let date = arguments[date_index].clone();
        arguments[date_index] = cast_local_timestamp_expr_to_ltz(date, &DataType::Date32, timezone);
    }
    arguments
}
