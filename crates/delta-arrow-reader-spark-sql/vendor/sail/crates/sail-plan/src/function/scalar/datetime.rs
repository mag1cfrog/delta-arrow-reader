// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::compute::can_cast_types;
use datafusion::arrow::datatypes::{
    DataType, IntervalDayTimeType, IntervalUnit, IntervalYearMonthType, TimeUnit,
};
use datafusion::functions::expr_fn;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchemaRef, DataFusionError, ScalarValue};
use datafusion_expr::expr::{self, Expr};
use datafusion_expr::simplify::SimplifyContextBuilder;
use datafusion_expr::{
    BinaryExpr, ExprSchemable, HigherOrderUDF, Operator, ScalarUDF, cast, lit, try_cast, when,
};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use sail_common::utils::datetime::time_unit_to_multiplier;
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::is_variant_storage_field;
use sail_function::scalar::datetime::convert_tz::{ConvertTz, ConvertTzLazy};
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_date_day_arithmetic::SparkDateDayArithmetic;
use sail_function::scalar::datetime::spark_date_format::SparkDateFormat;
use sail_function::scalar::datetime::spark_date_part::SparkDatePart;
use sail_function::scalar::datetime::spark_date_timestamp_ntz_cast::SparkDateTimestampNtzCast;
use sail_function::scalar::datetime::spark_date_trunc::SparkDateTrunc;
use sail_function::scalar::datetime::spark_last_day::SparkLastDay;
use sail_function::scalar::datetime::spark_make_interval::{
    MAKE_INTERVAL_SECONDS_TYPE, SparkMakeInterval,
};
use sail_function::scalar::datetime::spark_make_time::SparkMakeTime;
use sail_function::scalar::datetime::spark_make_timestamp_ntz::SparkMakeTimestampNtz;
use sail_function::scalar::datetime::spark_make_ym_interval::SparkMakeYmInterval;
use sail_function::scalar::datetime::spark_months_between_round_off::SparkMonthsBetweenRoundOff;
use sail_function::scalar::datetime::spark_next_day::SparkNextDay;
use sail_function::scalar::datetime::spark_time::SparkTime;
use sail_function::scalar::datetime::spark_time_diff::SparkTimeDiff;
use sail_function::scalar::datetime::spark_time_trunc::SparkTimeTrunc;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::datetime::spark_timestamp_add::SparkTimestampAdd;
use sail_function::scalar::datetime::spark_timestamp_diff::SparkTimestampDiff;
use sail_function::scalar::datetime::spark_trunc_date::{SparkTruncDate, TruncUnit};
use sail_function::scalar::datetime::spark_unix_timestamp::SparkUnixTimestamp;
use sail_function::scalar::datetime::spark_window_buckets::SparkWindowBuckets;
use sail_function::scalar::datetime::spark_year::SparkYear;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_function::scalar::explode::{Explode, ExplodeKind};
use sail_function::scalar::spark_cast_numeric_to_integer::SparkCastNumericToInteger;
use sail_function::scalar::spark_cast_string_to_boolean::SparkCastStringToBoolean;
use sail_function::scalar::spark_cast_string_to_decimal::SparkCastStringToDecimal;
use sail_function::scalar::spark_cast_string_to_integer::{
    SparkCastStringToInteger, StringIntegerCastMode,
};
use sail_function::scalar::spark_decimal_cast::{DecimalCastMode, SparkDecimalCast};
use sail_function::scalar::spark_known_not_null::SparkKnownNotNull;
use sail_function::scalar::spark_known_nullable::SparkKnownNullable;
use sail_function::scalar::spark_with_inputs::SparkWithInputs;
use sail_sql_analyzer::literal::interval::IntervalValue;
use sail_sql_analyzer::parser::parse_interval;

use super::lambda::lambda_with_fresh_parameter;
use crate::config::DefaultTimestampType;
use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunction, ScalarFunctionInput, cast_with_date_ntz_nullability,
    dictionary_string_values, expr_contains_spark_cast_to_variant, unresolved_routine_error,
};
use crate::function::common::{literal_value_without_evaluation, without_metadata_annotations};
use crate::function::temporal_coercion::{
    cast_local_timestamp_expr_to_ltz, ltz_expr_to_local_timestamp,
};

/// How the casts of MAKE_INTERVAL's and MAKE_YM_INTERVAL's arguments treat a failure: as Spark's
/// casts do outside ANSI, under ANSI, and under ANSI inside `TryEval`, which turns every failure
/// into NULL. Only MAKE_INTERVAL re-plans its casts for `TryEval`; MAKE_YM_INTERVAL uses the
/// session's mode. Whether MAKE_INTERVAL's arithmetic fails on overflow is separate: Spark's
/// `failOnError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MakeIntervalCastMode {
    Legacy,
    Ansi,
    Try,
}

impl MakeIntervalCastMode {
    fn of_session(ansi_mode: bool) -> Self {
        if ansi_mode { Self::Ansi } else { Self::Legacy }
    }

    fn decimal_cast_mode(self) -> DecimalCastMode {
        match self {
            Self::Legacy => DecimalCastMode::Legacy,
            Self::Ansi => DecimalCastMode::Ansi,
            Self::Try => DecimalCastMode::Try,
        }
    }

    fn string_integer_cast_mode(self) -> StringIntegerCastMode {
        match self {
            Self::Legacy => StringIntegerCastMode::Legacy,
            Self::Ansi => StringIntegerCastMode::Ansi,
            Self::Try => StringIntegerCastMode::Try,
        }
    }
}

/// Spark's MAKE_INTERVAL: missing arguments are 0, the first six are cast to INT and the
/// seconds to DECIMAL(18, 6) with Spark's casts for the session's ANSI mode.
fn make_interval(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if arguments.len() > 7 {
        return Err(PlanError::invalid(format!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `make_interval` requires [0, 1, 2, 3, 4, 5, 6, 7] parameters but the actual number is {}.",
            arguments.len()
        )));
    }
    let mode = MakeIntervalCastMode::of_session(function_context.plan_config.ansi_mode);
    plan_make_interval(
        arguments,
        function_context.schema,
        mode,
        mode == MakeIntervalCastMode::Ansi,
    )
}

/// Spark's TRY_MAKE_INTERVAL: MAKE_INTERVAL with one to seven arguments whose arithmetic gives
/// NULL on overflow in both ANSI modes (`failOnError = false`). Its argument casts still follow
/// the session's ANSI mode, so under ANSI a malformed or overflowing argument fails.
fn try_make_interval(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    if arguments.is_empty() || arguments.len() > 7 {
        return Err(PlanError::invalid(format!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `try_make_interval` requires [1, 2, 3, 4, 5, 6, 7] parameters but the actual number is {}.",
            arguments.len()
        )));
    }
    let mode = MakeIntervalCastMode::of_session(function_context.plan_config.ansi_mode);
    let fail_on_error = false;
    plan_make_interval(arguments, function_context.schema, mode, fail_on_error)
}

/// MAKE_INTERVAL over `arguments` cast in `mode`, failing on overflow if `fail_on_error`
/// (Spark's `failOnError`) and giving NULL otherwise.
fn plan_make_interval(
    arguments: Vec<Expr>,
    schema: &DFSchemaRef,
    mode: MakeIntervalCastMode,
    fail_on_error: bool,
) -> PlanResult<Expr> {
    let mut arguments = arguments.into_iter();
    let mut cast_arguments = Vec::with_capacity(7);
    for _ in 0..6 {
        let part = match arguments.next() {
            Some(part) => cast_make_interval_part(part, schema, mode)?,
            None => lit(0_i32),
        };
        cast_arguments.push(part);
    }
    let seconds = match arguments.next() {
        Some(seconds) => cast_make_interval_seconds(seconds, schema, mode)?,
        None => lit(ScalarValue::Decimal128(Some(0), 18, 6)),
    };
    cast_arguments.push(seconds);
    Ok(ScalarUDF::from(SparkMakeInterval::new(fail_on_error)).call(cast_arguments))
}

/// Spark's implicit cast of a MAKE_INTERVAL or MAKE_YM_INTERVAL part to INT.
/// `cast_string_function_int` in `string.rs` applies the same rule to string function
/// arguments, without a TRY mode.
fn cast_make_interval_part(
    part: Expr,
    schema: &DFSchemaRef,
    mode: MakeIntervalCastMode,
) -> PlanResult<Expr> {
    Ok(match part.get_type(schema)? {
        DataType::Int32 => part,
        DataType::Int8 | DataType::Int16 | DataType::Null => cast(part, DataType::Int32),
        DataType::Int64 | DataType::Float32 | DataType::Float64 => {
            ScalarUDF::from(SparkCastNumericToInteger::try_new(
                DataType::Int32,
                mode == MakeIntervalCastMode::Ansi,
                mode == MakeIntervalCastMode::Try,
            )?)
            .call(vec![part])
        }
        DataType::Decimal128(..) | DataType::Decimal256(..) => ScalarUDF::from(
            SparkDecimalCast::to_integer(DataType::Int32, mode.decimal_cast_mode())?,
        )
        .call(vec![part]),
        data_type if data_type.is_string() => {
            SparkCastStringToInteger::try_new(DataType::Int32, mode.string_integer_cast_mode())?
                .into_expr(part)
        }
        other => {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] make_interval requires INT parts, got {other}"
            )));
        }
    })
}

/// Spark's implicit cast of MAKE_INTERVAL seconds to DECIMAL(18, 6), with the casts CAST uses.
fn cast_make_interval_seconds(
    seconds: Expr,
    schema: &DFSchemaRef,
    mode: MakeIntervalCastMode,
) -> PlanResult<Expr> {
    let DataType::Decimal128(precision, scale) = MAKE_INTERVAL_SECONDS_TYPE else {
        unreachable!("MAKE_INTERVAL seconds are a DECIMAL");
    };
    Ok(match seconds.get_type(schema)? {
        DataType::Decimal128(18, 6) => seconds,
        DataType::Null => cast(seconds, MAKE_INTERVAL_SECONDS_TYPE),
        data_type if data_type.is_string() => ScalarUDF::new_from_impl(
            SparkCastStringToDecimal::new(precision, scale, mode != MakeIntervalCastMode::Ansi),
        )
        .call(vec![seconds]),
        data_type if data_type.is_numeric() => ScalarUDF::from(SparkDecimalCast::to_decimal(
            precision,
            scale,
            mode.decimal_cast_mode(),
        ))
        .call(vec![seconds]),
        other => {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] make_interval requires DECIMAL seconds, got {other}"
            )));
        }
    })
}

/// The value a MAKE_INTERVAL argument cast converts, or the argument itself.
fn uncast_make_interval_argument(argument: Expr) -> Expr {
    match argument {
        Expr::ScalarFunction(expr::ScalarFunction { func, mut args })
            if args.len() == 1
                && (func.inner().is::<SparkCastNumericToInteger>()
                    || func.inner().is::<SparkDecimalCast>()
                    || func.inner().is::<SparkCastStringToInteger>()
                    || func.inner().is::<SparkCastStringToDecimal>()) =>
        {
            args.remove(0)
        }
        argument => argument,
    }
}

/// Spark evaluates TRY_ADD, TRY_SUBTRACT, TRY_MULTIPLY and TRY_DIVIDE over a non-numeric
/// operand inside `TryEval`, which turns any failure into NULL. Under ANSI this re-plans each
/// MAKE_INTERVAL or TRY_MAKE_INTERVAL in an operand, and the casts of its arguments, to give
/// NULL instead; outside ANSI neither fails. Spark still fails while optimizing a constant
/// MAKE_INTERVAL, or a constant argument cast, beside a non-constant operand, outside TRY
/// (#1133).
pub(crate) fn replan_make_interval_for_try(
    operand: Expr,
    schema: &DFSchemaRef,
    ansi_mode: bool,
) -> PlanResult<Expr> {
    if !ansi_mode {
        return Ok(operand);
    }
    Ok(operand
        .transform_up(|expr| match expr {
            Expr::ScalarFunction(call) if call.func.inner().is::<SparkMakeInterval>() => {
                let arguments = call
                    .args
                    .into_iter()
                    .map(uncast_make_interval_argument)
                    .collect();
                let fail_on_error = false;
                plan_make_interval(arguments, schema, MakeIntervalCastMode::Try, fail_on_error)
                    .map(Transformed::yes)
                    .map_err(|error| DataFusionError::External(Box::new(error)))
            }
            expr => Ok(Transformed::no(expr)),
        })?
        .data)
}

/// Spark's MAKE_YM_INTERVAL: BIGINT, DECIMAL, FLOAT, DOUBLE and STRING arguments, also
/// dictionary-encoded ones, are cast to INT with the casts MAKE_INTERVAL uses for the session's
/// ANSI mode. Spark's DECIMAL, FLOAT, DOUBLE and STRING casts to INT are nullable, so the result
/// is nullable for such an argument.
fn make_ym_interval(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let mut arguments = input.arguments;
    let function = ScalarUDF::from(SparkMakeYmInterval::new());
    let argument_types = arguments
        .iter()
        .map(|argument| argument.get_type(input.function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    if function.coerce_types(&argument_types).is_err() {
        // Preserve the original argument types in native admission diagnostics.
        return Ok(function.call(arguments));
    }
    let known_null = arguments.iter().any(|argument| {
        literal_value_without_evaluation(argument).is_some_and(|value| value.is_null())
    });
    let mode = MakeIntervalCastMode::of_session(input.function_context.plan_config.ansi_mode);
    for (argument, data_type) in arguments.iter_mut().zip(argument_types) {
        // Spark has no dictionary encoding; Delta uses one for STRING partition columns.
        let data_type = match data_type {
            DataType::Dictionary(_, value_type) => {
                *argument = cast(argument.clone(), (*value_type).clone());
                *value_type
            }
            data_type => data_type,
        };
        if matches!(
            data_type,
            DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(..)
                | DataType::Decimal256(..)
        ) || data_type.is_string()
        {
            *argument =
                cast_make_interval_part(argument.clone(), input.function_context.schema, mode)?;
        }
    }
    let interval = function.call(arguments);
    if known_null {
        // Spark's NullPropagation gives NULL for a NULL argument, also one behind casts, before
        // constants fold, so a failing cast of the other argument is discarded. Keep the original
        // call available for type admission before NULL pruning.
        return Ok(when(lit(true), lit(ScalarValue::IntervalYearMonth(None))).otherwise(interval)?);
    }
    Ok(interval)
}

fn integer_part(expr: Expr, part: &str) -> Expr {
    cast(
        expr_fn::date_part(lit(part.to_uppercase()), expr),
        DataType::Int32,
    )
}

/// The implicit DATE argument of WEEKDAY, DAYOFWEEK and WEEKOFYEAR.
fn implicit_date_argument(input: ScalarFunctionInput) -> PlanResult<Expr> {
    spark_date_argument(
        input.arguments.one()?,
        input.function_context.schema,
        input.function_context.plan_config.ansi_mode,
    )
}

/// `argument` as Spark's implicit DATE argument. Spark casts a STRING, also dictionary-encoded,
/// TIMESTAMP_NTZ or NULL argument to DATE, with its STRING-to-DATE rules for a STRING, which fail
/// under ANSI and give NULL otherwise, and a nullable cast for a TIMESTAMP_NTZ. DATE passes
/// through, and a TIMESTAMP is read in its annotated zone, the session zone. Other types pass
/// through unchecked, although Spark rejects them (#1236).
fn spark_date_argument(argument: Expr, schema: &DFSchemaRef, ansi_mode: bool) -> PlanResult<Expr> {
    let argument = dictionary_string_values(argument, schema)?;
    Ok(match argument.get_type(schema)? {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            ScalarUDF::from(SparkDate::new(!ansi_mode)).call(vec![argument])
        }
        source @ (DataType::Timestamp(_, None) | DataType::Null) => {
            cast_with_date_ntz_nullability(argument, &source, &DataType::Date32)
        }
        _ => argument,
    })
}

fn years(arg: Expr) -> Expr {
    integer_part(arg, "YEAR")
}

fn date_trunc_part_conversion(part: Expr) -> Expr {
    Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![
            (
                Box::new(
                    part.clone()
                        .ilike(lit("mon"))
                        .or(part.clone().ilike(lit("mm"))),
                ),
                Box::new(lit("month")),
            ),
            (
                Box::new(
                    part.clone()
                        .ilike(lit("yy"))
                        .or(part.clone().ilike(lit("yyyy"))),
                ),
                Box::new(lit("year")),
            ),
            (
                Box::new(part.clone().ilike(lit("dd"))),
                Box::new(lit("day")),
            ),
        ],
        else_expr: Some(Box::new(part)),
    })
}

/// Whether Spark folds `expr` while planning: it reads no column, outer reference, scalar
/// subquery or lambda variable, and calls no volatile function.
fn is_spark_foldable(expr: &Expr) -> PlanResult<bool> {
    Ok(!expr.is_volatile()
        && expr.column_refs().is_empty()
        && !expr.contains_outer()
        && !expr.contains_scalar_subquery()
        && !expr.exists(|expr| {
            Ok(matches!(
                expr,
                Expr::LambdaVariable(_) | Expr::HigherOrderFunction(_)
            ))
        })?)
}

/// Spark's TRUNC of a DATE. Spark casts a STRING argument to DATE with its STRING-to-DATE rules,
/// read here from a dictionary's values, and a TIMESTAMP or TIMESTAMP_NTZ to DATE; it rejects other
/// types. It casts a unit of a type other than ARRAY, MAP or STRUCT to STRING; BINARY that is not
/// UTF-8 spells no unit. A constant unit it does not support, NULL included, gives NULL without
/// reading the date, also when the optimizer makes the unit constant, which `SparkTruncDate`
/// handles. Otherwise `SparkTruncDate` evaluates the date and the unit for every row, reads each
/// row's unit and truncates the DATE's days, giving NULL for a unit Spark does not support. Its
/// result is nullable, as Spark declares it, until constant folding replaces a literal's.
fn trunc(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let schema = input.function_context.schema;
    let ansi = input.function_context.plan_config.ansi_mode;
    let (date, unit) = input.arguments.two()?;
    let date = dictionary_string_values(date, schema)?;
    let date = match date.get_type(schema)? {
        data_type if data_type.is_string() => {
            ScalarUDF::from(SparkDate::new(!ansi)).call(vec![date])
        }
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Null => {
            cast(date, DataType::Date32)
        }
        data_type => {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] `trunc`: The first parameter requires \
                 the \"DATE\" type, got {data_type}"
            )));
        }
    };
    let unit = match unit.get_type(schema)? {
        data_type if data_type.is_nested() => {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] `trunc`: The second parameter requires \
                 the \"STRING\" type, got {data_type}"
            )));
        }
        _ => Expr::TryCast(expr::TryCast::new(Box::new(unit), DataType::Utf8)),
    };
    if is_spark_foldable(&unit)? {
        let context = SimplifyContextBuilder::default()
            .with_schema(schema.clone())
            .build();
        let constant = ExprSimplifier::new(context).coerce(unit.clone(), schema)?;
        // A unit that fails to evaluate is left to the optimizer, which fails folding it, as
        // Spark's does.
        if let Ok(value) = LiteralEvaluator::new().evaluate(&constant) {
            let supported = match value {
                ScalarValue::Utf8(Some(spelling)) => TruncUnit::parse(&spelling).is_some(),
                _ => false,
            };
            if !supported {
                // Spark computes the date's columns, aggregates and window functions, though
                // it does not read the date.
                return declare_spark_nullability(
                    lit(ScalarValue::Date32(None)),
                    true,
                    Some(&date),
                    schema,
                );
            }
        }
    }
    Ok(ScalarUDF::from(SparkTruncDate::new()).call(vec![date, unit]))
}

/// A DATE function's argument: Spark casts a TIMESTAMP_NTZ to DATE, a nullable cast. Other
/// arguments keep their existing handling.
fn date_argument(argument: Expr, schema: &DFSchemaRef) -> PlanResult<Expr> {
    Ok(match argument.get_type(schema)? {
        source @ DataType::Timestamp(_, None) => {
            cast_with_date_ntz_nullability(argument, &source, &DataType::Date32)
        }
        _ => argument,
    })
}

/// An INT part of a DATE function's argument.
fn date_field(input: ScalarFunctionInput, part: &str) -> PlanResult<Expr> {
    let argument = input.arguments.one()?;
    Ok(integer_part(
        date_argument(argument, input.function_context.schema)?,
        part,
    ))
}

fn date_trunc(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (part, timestamp) = input.arguments.two()?;
    let truncated = ScalarUDF::from(SparkDateTrunc::new())
        .call(vec![date_trunc_part_conversion(part), timestamp]);
    let truncated = match truncated.get_type(input.function_context.schema)? {
        DataType::Timestamp(TimeUnit::Microsecond, _) => truncated,
        DataType::Timestamp(_, tz) => {
            cast(truncated, DataType::Timestamp(TimeUnit::Microsecond, tz))
        }
        other => Err(PlanError::InternalError(format!(
            "date_trunc expected a timestamp result, got {other:?}"
        )))?,
    };
    Ok(truncated)
}

fn interval_arithmetic(input: ScalarFunctionInput, unit: &str, op: Operator) -> PlanResult<Expr> {
    let (date, interval) = input.arguments.two()?;
    let date_type = date.get_type(input.function_context.schema)?;

    let interval = match unit.to_lowercase().as_str() {
        "years" | "year" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(years)), metadata) => Expr::Literal(
                ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(years, 0))),
                metadata,
            ),
            _ => cast(
                format_interval(interval, "years"),
                DataType::Interval(IntervalUnit::YearMonth),
            ),
        },
        "months" | "month" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(months)), metadata) => Expr::Literal(
                ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(0, months))),
                metadata,
            ),
            _ => cast(
                format_interval(interval, "months"),
                DataType::Interval(IntervalUnit::YearMonth),
            ),
        },
        "days" | "day" => match interval {
            Expr::Literal(ScalarValue::Int32(Some(days)), metadata) => Expr::Literal(
                ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(days, 0))),
                metadata,
            ),
            _ => cast(
                format_interval(interval, "days"),
                DataType::Interval(IntervalUnit::DayTime),
            ),
        },
        _ => {
            return Err(PlanError::invalid(format!(
                "add_interval does not support interval unit type '{unit}'"
            )));
        }
    };
    Ok(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(cast_with_date_ntz_nullability(
            date,
            &date_type,
            &DataType::Date32,
        )),
        op,
        right: Box::new(interval),
    }))
}

fn format_interval(interval: Expr, unit: &str) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(interval),
        op: Operator::StringConcat,
        right: Box::new(lit(format!(" {unit}"))),
    })
}

fn date_offset_by_days(
    input: ScalarFunctionInput,
    function_name: &str,
    op: Operator,
) -> PlanResult<Expr> {
    if input.arguments.len() != 2 {
        return Err(PlanError::invalid(format!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] {function_name} requires 2 arguments"
        )));
    }
    let (date, days) = input.arguments.two()?;
    let schema = input.function_context.schema;
    let ansi = input.function_context.plan_config.ansi_mode;
    let date = dictionary_string_values(date, schema)?;
    let date = match date.get_type(schema)? {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            ScalarUDF::from(SparkDate::new(!ansi)).call(vec![date])
        }
        source @ (DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Null) => cast_with_date_ntz_nullability(date, &source, &DataType::Date32),
        other => {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] {function_name} requires a DATE start, got {other}"
            )));
        }
    };
    let days = match days.get_type(schema)? {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Null => {
            cast(days, DataType::Int32)
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if ansi => days,
        other if other.is_string() && is_spark_foldable(&days)? => {
            let context = SimplifyContextBuilder::default()
                .with_schema(schema.clone())
                .build();
            let days = ExprSimplifier::new(context).coerce(days, schema)?;
            let days = LiteralEvaluator::new().evaluate(&days)?;
            match days {
                ScalarValue::Utf8(Some(value))
                | ScalarValue::LargeUtf8(Some(value))
                | ScalarValue::Utf8View(Some(value)) => {
                    let days = value.trim_matches(|c| c <= ' ' || c == '\u{7f}')
                        .parse::<i32>().map_err(|error| PlanError::invalid(format!(
                            "[SECOND_FUNCTION_ARGUMENT_NOT_INTEGER] {function_name} requires integer days: {error}"
                        )))?;
                    lit(days)
                }
                // Spark's legacy string-day coercion converts a foldable NULL string to zero.
                value if value.is_null() => lit(0_i32),
                other => {
                    return Err(PlanError::internal(format!(
                        "expected string days, got {other}"
                    )));
                }
            }
        }
        other => {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] {function_name} requires integer days, got {other}"
            )));
        }
    };
    Ok(ScalarUDF::from(SparkDateDayArithmetic::new(op)).call(vec![date, days]))
}

/// Spark's TIMESTAMPADD: `quantity` and `timestamp` take Spark's implicit casts to BIGINT and
/// TIMESTAMP, which keeps a TIMESTAMP_NTZ.
fn timestampadd_with_name(input: ScalarFunctionInput, function_name: &str) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (unit, quantity, timestamp) = arguments.three()?;
    let unit = match &unit {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => s.clone(),
        Expr::Column(col) => col.name().to_string(),
        _ => {
            return Err(PlanError::invalid(format!(
                "{function_name} unit must be a string literal or keyword"
            )));
        }
    };
    let schema = function_context.schema;
    let config = function_context.plan_config;
    let timezone = &config.session_timezone;
    // Spark's casts from a string or fractional quantity, or from a string TIMESTAMP, are
    // nullable.
    let (quantity, cast_quantity_nullable) = match quantity.get_type(schema)? {
        data_type if data_type.is_integer() || data_type == DataType::Null => {
            (cast(quantity, DataType::Int64), false)
        }
        data_type if data_type.is_string() => {
            let mode = if config.ansi_mode {
                StringIntegerCastMode::Ansi
            } else {
                StringIntegerCastMode::Legacy
            };
            let cast = SparkCastStringToInteger::try_new(DataType::Int64, mode)?;
            (ScalarUDF::from(cast).call(vec![quantity]), true)
        }
        DataType::Float32 | DataType::Float64 => {
            let cast =
                SparkCastNumericToInteger::try_new(DataType::Int64, config.ansi_mode, false)?;
            (ScalarUDF::from(cast).call(vec![quantity]), true)
        }
        DataType::Decimal128(..) | DataType::Decimal256(..) => {
            let mode = if config.ansi_mode {
                DecimalCastMode::Ansi
            } else {
                DecimalCastMode::Legacy
            };
            let cast = SparkDecimalCast::to_integer(DataType::Int64, mode)?;
            (ScalarUDF::from(cast).call(vec![quantity]), true)
        }
        other => {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] {function_name} requires a BIGINT quantity, got {other}"
            )));
        }
    };
    let (timestamp, cast_timestamp_nullable) = match timestamp.get_type(schema)? {
        DataType::Timestamp(TimeUnit::Microsecond, _) => (timestamp, false),
        DataType::Timestamp(_, zone) => (
            cast(timestamp, DataType::Timestamp(TimeUnit::Microsecond, zone)),
            false,
        ),
        source @ (DataType::Date32 | DataType::Date64) => (
            cast_local_timestamp_expr_to_ltz(timestamp, &source, timezone),
            false,
        ),
        data_type if data_type.is_string() => (
            ScalarUDF::from(SparkTimestamp::try_new(
                Some(Arc::clone(timezone)),
                config.ansi_mode,
                false,
            )?)
            .call(vec![timestamp]),
            true,
        ),
        DataType::Null => (
            cast(
                timestamp,
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(timezone))),
            ),
            false,
        ),
        other => {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] {function_name} requires a TIMESTAMP, got {other}"
            )));
        }
    };
    let add = SparkTimestampAdd::try_new(
        &unit,
        cast_quantity_nullable || cast_timestamp_nullable,
        Arc::clone(timezone),
    )?;
    Ok(ScalarUDF::from(add).call(vec![quantity, timestamp]))
}

fn timestampadd(input: ScalarFunctionInput) -> PlanResult<Expr> {
    timestampadd_with_name(input, "timestampadd")
}

fn dateadd(input: ScalarFunctionInput, function_name: &str) -> PlanResult<Expr> {
    match input.arguments.len() {
        2 => date_offset_by_days(input, function_name, Operator::Plus),
        3 => timestampadd_with_name(input, function_name),
        _ => Err(PlanError::invalid(format!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] {function_name} requires 2 or 3 arguments"
        ))),
    }
}

fn make_date(year: Expr, month: Expr, day: Expr) -> Expr {
    match (&year, &month, &day) {
        (Expr::Literal(ScalarValue::Null, metadata), _, _)
        | (_, Expr::Literal(ScalarValue::Null, metadata), _)
        | (_, _, Expr::Literal(ScalarValue::Null, metadata)) => {
            Expr::Literal(ScalarValue::Null, metadata.clone())
        }
        _ => expr_fn::make_date(year, month, day),
    }
}

/// Whether Spark casts an argument of this type to DATE implicitly and successfully, as
/// DATEDIFF's: a DATE, a TIMESTAMP or TIMESTAMP_NTZ, a STRING, also dictionary-encoded as a Delta
/// partition column or as a string view, or an untyped NULL. Spark also casts a TIME, and that
/// cast fails.
fn casts_implicitly_to_date(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => true,
        DataType::Dictionary(_, value) => casts_implicitly_to_date(value),
        _ => false,
    }
}

/// Spark's `UnixDate`: the INT number of days since the epoch of a DATE. Spark casts no argument
/// to DATE, so any type but a DATE or an untyped NULL fails while resolving.
fn unix_date(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let argument = input.arguments.one()?;
    match argument.get_type(input.function_context.schema)? {
        DataType::Date32 | DataType::Date64 | DataType::Null => {
            Ok(cast(cast(argument, DataType::Date32), DataType::Int32))
        }
        data_type => Err(PlanError::invalid(format!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] `unix_date`: The first parameter requires \
             the \"DATE\" type, got {data_type}"
        ))),
    }
}

/// Spark's `DateDiff`: the INT number of days from `start` to `end`, computed as a Java `int`
/// subtraction, which wraps at the DATE limits. TIMESTAMP arguments are cast to DATE. The
/// three-argument form is TIMESTAMPDIFF instead.
fn date_diff_days(
    (end, end_type): (Expr, &DataType),
    (start, start_type): (Expr, &DataType),
) -> Expr {
    let days = |date: Expr, source: &DataType| {
        cast(
            cast_with_date_ntz_nullability(date, source, &DataType::Date32),
            DataType::Int32,
        )
    };
    // A native integer subtraction wraps rather than failing on overflow.
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(days(end, end_type)),
        op: Operator::Minus,
        right: Box::new(days(start, start_type)),
    })
}

/// Casts a TIMESTAMPDIFF argument to the TIMESTAMP instant Spark's implicit cast gives it in
/// the session time zone.
fn cast_timestamp_diff_argument(
    function_name: &str,
    ordinal: &str,
    expr: Expr,
    context: &FunctionContextInput,
) -> PlanResult<Expr> {
    let timezone = &context.plan_config.session_timezone;
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(timezone)));
    match expr.get_type(context.schema)? {
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Ok(expr),
        // Spark has no dictionary encoding; Delta uses one for STRING partition columns.
        DataType::Dictionary(_, value_type) => {
            cast_timestamp_diff_argument(function_name, ordinal, cast(expr, *value_type), context)
        }
        DataType::Timestamp(_, Some(_)) | DataType::Null => Ok(cast(expr, timestamp)),
        source @ (DataType::Timestamp(_, None) | DataType::Date32 | DataType::Date64) => {
            Ok(cast_local_timestamp_expr_to_ltz(expr, &source, timezone))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok(ScalarUDF::from(SparkTimestamp::try_new(
                Some(Arc::clone(timezone)),
                context.plan_config.ansi_mode,
                false,
            )?)
            .call(vec![expr]))
        }
        data_type @ (DataType::Time32(_) | DataType::Time64(_)) => {
            Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] {function_name} cannot cast the \
                 {ordinal} argument from {data_type} to TIMESTAMP"
            )))
        }
        other => Err(PlanError::invalid(format!(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] {function_name} requires a TIMESTAMP \
             {ordinal} argument, got {other}"
        ))),
    }
}

/// Spark's TIMESTAMPDIFF, also spelled as three-argument DATEDIFF, DATE_DIFF and TIMEDIFF. The
/// resolver reaches it only for Spark's datetime unit grammar, with the unit keyword as a string
/// literal.
fn timestamp_diff_with_name(input: ScalarFunctionInput, function_name: &str) -> PlanResult<Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let Ok([unit, start, end]) = <[Expr; 3]>::try_from(arguments) else {
        return Err(unresolved_routine_error(
            function_name,
            function_context.session_context,
        ));
    };
    let Expr::Literal(ScalarValue::Utf8(Some(unit)), _) = unit else {
        return Err(PlanError::invalid(format!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{function_name}` requires 2 parameters \
             but the actual number is 3."
        )));
    };
    let start = cast_timestamp_diff_argument(function_name, "first", start, &function_context)?;
    let end = cast_timestamp_diff_argument(function_name, "second", end, &function_context)?;
    let timezone = Arc::clone(&function_context.plan_config.session_timezone);
    Ok(ScalarUDF::from(SparkTimestampDiff::new(&unit, timezone)).call(vec![start, end]))
}

fn datediff_with_name(input: ScalarFunctionInput, function_name: &str) -> PlanResult<Expr> {
    match input.arguments.len() {
        2 => {
            let schema = input.function_context.schema;
            let [end, start] = <[Expr; 2]>::try_from(input.arguments).map_err(|_| {
                PlanError::invalid(format!("{function_name} requires 2 or 3 arguments"))
            })?;
            for (ordinal, argument) in [("first", &end), ("second", &start)] {
                match argument.get_type(schema)? {
                    // Spark casts a TIME to DATE implicitly, and the cast fails.
                    data_type @ (DataType::Time32(_) | DataType::Time64(_)) => {
                        return Err(PlanError::invalid(format!(
                            "[DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION] `{function_name}` \
                             cannot cast the {ordinal} parameter from {data_type} to \"DATE\""
                        )));
                    }
                    data_type if !casts_implicitly_to_date(&data_type) => {
                        return Err(PlanError::invalid(format!(
                            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] `{function_name}`: The \
                             {ordinal} parameter requires the \"DATE\" type, got {data_type}"
                        )));
                    }
                    _ => {}
                }
            }
            let (end_type, start_type) = (end.get_type(schema)?, start.get_type(schema)?);
            Ok(date_diff_days((end, &end_type), (start, &start_type)))
        }
        3 => timestamp_diff_with_name(input, function_name),
        n => Err(PlanError::invalid(format!(
            "{function_name} requires 2 or 3 arguments, got {n}"
        ))),
    }
}

fn datediff(input: ScalarFunctionInput) -> PlanResult<Expr> {
    datediff_with_name(input, "datediff")
}

fn date_diff(input: ScalarFunctionInput) -> PlanResult<Expr> {
    datediff_with_name(input, "date_diff")
}

fn timestampdiff(input: ScalarFunctionInput) -> PlanResult<Expr> {
    timestamp_diff_with_name(input, "timestampdiff")
}

fn timediff(input: ScalarFunctionInput) -> PlanResult<Expr> {
    timestamp_diff_with_name(input, "timediff")
}

fn session_timezone(input: &ScalarFunctionInput) -> Expr {
    lit(input
        .function_context
        .plan_config
        .session_timezone
        .to_string())
}

fn current_timezone(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = session_timezone(&input);
    input.arguments.zero()?;
    Ok(session_tz)
}

fn coerce_datetime_format(
    function_name: &str,
    format: Expr,
    schema: &DFSchemaRef,
) -> PlanResult<Expr> {
    let data_type = format.get_type(schema)?;
    if data_type.is_nested() {
        Err(PlanError::invalid(format!(
            "{function_name} format argument must be a string, got {data_type}"
        )))
    } else if data_type.is_string() {
        Ok(format)
    } else {
        Ok(cast(format, DataType::Utf8))
    }
}

/// The inputs of `omitted` that operators below a projection compute: its columns, and its
/// aggregate and window functions whole.
fn computed_inputs(omitted: &Expr) -> PlanResult<Vec<Expr>> {
    let mut inputs = Vec::new();
    omitted.apply(|expr| {
        Ok(match expr {
            Expr::Column(_) | Expr::AggregateFunction(_) | Expr::WindowFunction(_) => {
                if !inputs.contains(expr) {
                    inputs.push(expr.clone());
                }
                TreeNodeRecursion::Jump
            }
            _ => TreeNodeRecursion::Continue,
        })
    })?;
    Ok(inputs)
}

/// `expr` declared with Spark's nullability, where DataFusion declares it otherwise. Unlike a
/// `CASE WHEN true` wrapper, the declaration survives optimization over a column. Declaring NOT
/// NULL over a value that can be NULL fails the query when such a value occurs.
/// `omitted_input` is an input of Spark's expression that `expr` leaves out, such as a format
/// Spark does not evaluate. Its columns, aggregates and window functions become unused inputs
/// of the declaration, so they are computed as Spark computes them, and the declaration does
/// not fold over a constant `expr` where Spark's expression, which has them as inputs, is not
/// constant. Where Spark's result is NOT NULL and DataFusion already declares it so,
/// `SparkWithInputs` carries them and keeps `expr`'s own nullability at every stage, which a
/// grouping set can make nullable. `SparkQueryPlanner` drops the inputs once the logical plan is
/// optimized, so they are never evaluated, as Spark never evaluates the argument.
fn declare_spark_nullability(
    expr: Expr,
    nullable: bool,
    omitted_input: Option<&Expr>,
    schema: &DFSchemaRef,
) -> PlanResult<Expr> {
    let inputs = omitted_input
        .map(computed_inputs)
        .transpose()?
        .unwrap_or_default();
    let declared = expr.nullable(schema)?;
    let declaration = match (nullable, declared) {
        (true, true) | (false, false) if inputs.is_empty() => return Ok(expr),
        (true, _) => ScalarUDF::from(SparkKnownNullable::new()),
        (false, false) => ScalarUDF::from(SparkWithInputs::new()),
        (false, true) => ScalarUDF::from(SparkKnownNotNull::new()),
    };
    Ok(declaration.call(std::iter::once(expr).chain(inputs).collect()))
}

/// TO_DATE or Spark's TRY_TO_DATE, which is TO_DATE outside ANSI mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateParser {
    ToDate,
    TryToDate,
}

impl DateParser {
    fn name(self) -> &'static str {
        match self {
            Self::ToDate => "to_date",
            Self::TryToDate => "try_to_date",
        }
    }

    /// Whether a value it cannot parse gives NULL rather than failing.
    fn null_on_error(self, ansi_mode: bool) -> bool {
        self == Self::TryToDate || !ansi_mode
    }
}

fn to_date(input: ScalarFunctionInput) -> PlanResult<Expr> {
    to_date_with_parser(input, DateParser::ToDate)
}

/// Spark's TRY_TO_DATE: TO_DATE that gives NULL for a value it cannot parse in both ANSI modes.
/// An invalid pattern still fails, as in Spark. With a format, its result is nullable at every
/// stage over a DATE or TIMESTAMP column, and over a literal when the format reads a column.
fn try_to_date(input: ScalarFunctionInput) -> PlanResult<Expr> {
    to_date_with_parser(input, DateParser::TryToDate)
}

fn to_date_with_parser(input: ScalarFunctionInput, parser: DateParser) -> PlanResult<Expr> {
    let function_name = parser.name();
    let null_on_error = parser.null_on_error(input.function_context.plan_config.ansi_mode);
    if input.arguments.len() == 1 {
        // If format is not supplied, the function is a synonym for cast(expr AS DATE).
        let expr =
            dictionary_string_values(input.arguments[0].clone(), input.function_context.schema)?;
        let data_type = expr.get_type(input.function_context.schema)?;
        if data_type.is_string() {
            Ok(ScalarUDF::from(SparkDate::new(null_on_error)).call(vec![expr]))
        } else {
            Ok(cast_with_date_ntz_nullability(
                expr,
                &data_type,
                &DataType::Date32,
            ))
        }
    } else if input.arguments.len() == 2 {
        let expr =
            dictionary_string_values(input.arguments[0].clone(), input.function_context.schema)?;
        let format = coerce_datetime_format(
            function_name,
            input.arguments[1].clone(),
            input.function_context.schema,
        )?;
        let expr_type = expr.get_type(input.function_context.schema);
        let date = match &expr_type {
            Ok(DataType::Timestamp(_, _)) => Some(expr_fn::to_local_time(vec![expr.clone()])),
            Ok(DataType::Date32 | DataType::Date64) => Some(expr.clone()),
            _ => None,
        };
        if let Some(date) = date {
            // Spark ignores the format of a DATE or TIMESTAMP: it never evaluates it, but
            // computes its aggregates and window functions. Its result is nullable outside ANSI
            // mode, and in ANSI mode for a nullable argument or format, at every stage over a
            // column, and the format's columns keep it from folding over a literal.
            let nullable = null_on_error
                || expr.nullable(input.function_context.schema)?
                || format.nullable(input.function_context.schema)?;
            return declare_spark_nullability(
                cast(date, DataType::Date32),
                nullable,
                Some(&format),
                input.function_context.schema,
            );
        }
        let expr = match expr_type {
            Ok(_other) => expr,
            Err(_) => cast(expr, DataType::Utf8), // In case of error, cast to string
        };
        Ok(ScalarUDF::from(SparkDate::new(null_on_error)).call(vec![expr, format]))
    } else {
        Err(PlanError::invalid(format!(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{function_name}` requires [1, 2] \
             parameters but the actual number is {}.",
            input.arguments.len()
        )))
    }
}

fn validate_unix_timestamp_format(format: &Expr, schema: &DFSchemaRef) -> PlanResult<()> {
    match format.get_type(schema)? {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => Ok(()),
        data_type => Err(PlanError::invalid(format!(
            "unix_timestamp format argument must be a string, got {data_type}"
        ))),
    }
}

fn unix_timestamp(mut input: ScalarFunctionInput) -> PlanResult<Expr> {
    // A dictionary-encoded STRING parses as its STRING values do.
    let schema = input.function_context.schema;
    input.arguments = input
        .arguments
        .into_iter()
        .map(|arg| dictionary_string_values(arg, schema))
        .collect::<PlanResult<Vec<_>>>()?;
    let timezone = input.function_context.plan_config.session_timezone.clone();
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    if input.arguments.is_empty() {
        let expr = ScalarUDF::from(TimestampNow::new(timezone, TimeUnit::Second)).call(vec![]);
        Ok(cast(expr, DataType::Int64))
    } else if input.arguments.len() == 1 {
        Ok(ScalarUDF::from(SparkUnixTimestamp::new(timezone, ansi_mode)).call(input.arguments))
    } else if input.arguments.len() == 2 {
        let (expr, format) = input.arguments.two()?;
        if matches!(
            expr.get_type(input.function_context.schema)?,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Null
        ) {
            validate_unix_timestamp_format(&format, input.function_context.schema)?;
            let nullable = !ansi_mode
                || expr.nullable(input.function_context.schema)?
                || format.nullable(input.function_context.schema)?;
            let result =
                ScalarUDF::from(SparkUnixTimestamp::new(timezone, ansi_mode)).call(vec![expr]);
            declare_spark_nullability(
                result,
                nullable,
                Some(&format),
                input.function_context.schema,
            )
        } else {
            Ok(
                ScalarUDF::from(SparkUnixTimestamp::new(timezone, ansi_mode))
                    .call(vec![expr, format]),
            )
        }
    } else {
        Err(PlanError::invalid(
            "unix_timestamp requires 0, 1, or 2 arguments",
        ))
    }
}

fn to_unix_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if !(1..=2).contains(&input.arguments.len()) {
        Err(PlanError::invalid(
            "to_unix_timestamp requires 1 or 2 arguments",
        ))
    } else {
        unix_timestamp(input)
    }
}

/// Dispatch for `next_day(date, day_of_week)`.
///
/// Reads `PlanConfig::ansi_mode` at planning time and bakes it into the UDF
/// so the runtime path chooses between erroring (ANSI=true) and returning
/// NULL (ANSI=false) on malformed day-of-week strings.
fn next_day(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let udf = ScalarUDF::from(SparkNextDay::new(ansi_mode));
    Ok(udf.call(input.arguments))
}

pub(super) fn date_format(expr: Expr, format: Expr, timezone: String) -> Expr {
    date_format_with_args(vec![expr, format], timezone)
}

fn date_format_with_args(arguments: Vec<Expr>, timezone: String) -> Expr {
    ScalarUDF::from(SparkDateFormat::new(timezone.into())).call(arguments)
}

fn timestamp_data_type(input: &ScalarFunctionInput, timestamp_ntz: bool) -> DataType {
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone.clone())
    };
    DataType::Timestamp(TimeUnit::Microsecond, timezone)
}

fn timestamp_null(input: &ScalarFunctionInput, timestamp_ntz: bool) -> Expr {
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone.clone())
    };
    lit(ScalarValue::TimestampMicrosecond(None, timezone))
}

fn is_null_literal(expr: &Expr) -> bool {
    matches!(without_metadata_annotations(expr), Expr::Literal(value, _) if value.is_null())
}

fn to_timestamp(input: ScalarFunctionInput, timestamp_ntz: bool) -> PlanResult<Expr> {
    timestamp_with_try(input, timestamp_ntz, false)
}

fn to_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    time_with_try(input, false)
}

fn try_to_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    time_with_try(input, true)
}

/// Shared `to_time` / `try_to_time` planner. Routes through `SparkTime`, which
/// parses strings (with an optional Spark Java datetime pattern) or casts
/// time/timestamp args.
/// `to_time` errors on failure (Spark's `ToTime` is ANSI-invariant); `try_to_time`
/// (`is_try`) returns NULL.
fn time_with_try(input: ScalarFunctionInput, is_try: bool) -> PlanResult<Expr> {
    let udf = ScalarUDF::from(SparkTime::new(is_try));
    if input.arguments.len() == 1 {
        Ok(udf.call(input.arguments))
    } else if input.arguments.len() == 2 {
        // Pass `expr` through unchanged so `SparkTime::coerce_types` validates it
        // and the kernel dispatches by type (strings parse with the format,
        // TIME/TIMESTAMP cast directly), exactly as in the 1-arg form. Forcing a
        // cast to Utf8 here would route non-string inputs through string parsing,
        // bypassing the coercion checks and diverging from the 1-arg behavior.
        let (expr, format) = input.arguments.two()?;
        Ok(udf.call(vec![expr, format]))
    } else {
        let name = if is_try { "try_to_time" } else { "to_time" };
        Err(PlanError::invalid(format!(
            "{name} requires 1 or 2 arguments"
        )))
    }
}

fn try_to_timestamp(input: ScalarFunctionInput, timestamp_ntz: bool) -> PlanResult<Expr> {
    timestamp_with_try(input, timestamp_ntz, true)
}

/// Shared `to_timestamp` / `try_to_timestamp` (+ `_ntz`) planner.
///
/// The 1-arg form goes through `cast` / `try_cast`, which route strings to
/// `SparkTimestamp` (honoring ANSI for the strict variant) and cast other types.
/// The 2-arg form parses the value with the given format via `SparkTimestamp`.
fn timestamp_with_try(
    input: ScalarFunctionInput,
    timestamp_ntz: bool,
    is_try: bool,
) -> PlanResult<Expr> {
    let data_type = timestamp_data_type(&input, timestamp_ntz);
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let timezone = if timestamp_ntz {
        None
    } else {
        Some(input.function_context.plan_config.session_timezone.clone())
    };
    // A dictionary-encoded STRING parses as its STRING values do.
    let arguments = input
        .arguments
        .iter()
        .map(|arg| dictionary_string_values(arg.clone(), input.function_context.schema))
        .collect::<PlanResult<Vec<_>>>()?;
    if arguments.len() == 1 {
        let expr = arguments.one()?;
        let expr_type = expr.get_type(input.function_context.schema)?;
        if matches!(
            expr_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            // Strings parse through SparkTimestamp, which honors ANSI (errors
            // under ANSI, NULL otherwise) for the strict variant.
            let udf = ScalarUDF::from(SparkTimestamp::try_new(timezone, ansi_mode, is_try)?);
            Ok(udf.call(vec![expr]))
        } else {
            // A TIMESTAMP_NTZ result is each instant's local date-time in the session zone.
            if timestamp_ntz
                && let DataType::Timestamp(unit, Some(zone)) = &expr_type
                && let Some(local) = ltz_expr_to_local_timestamp(
                    expr.clone(),
                    *unit,
                    zone,
                    &input.function_context.plan_config.session_timezone,
                    is_try,
                )
            {
                return Ok(local);
            }
            // Spark also accepts session zones that Arrow cannot parse (#1029). A TIMESTAMP
            // result keeps the instant; other types cast.
            let expr = match expr_type {
                DataType::Timestamp(_, Some(_)) if timestamp_ntz => {
                    expr_fn::to_local_time(vec![expr])
                }
                _ => expr,
            };
            // Spark declares a DATE's cast to TIMESTAMP_NTZ nullable. Without a format, its TRY
            // forms are that cast too, not a TRY cast.
            if timestamp_ntz && matches!(expr_type, DataType::Date32 | DataType::Date64) {
                return Ok(
                    ScalarUDF::from(SparkDateTimestampNtzCast::to_timestamp_ntz(false))
                        .call(vec![expr]),
                );
            }
            if is_try {
                Ok(try_cast(expr, data_type))
            } else {
                Ok(cast(expr, data_type))
            }
        }
    } else if arguments.len() == 2 {
        let null = timestamp_null(&input, timestamp_ntz);
        if arguments.iter().any(is_null_literal) {
            return Ok(null);
        }
        Ok(cast(
            ScalarUDF::from(SparkTimestamp::try_new(
                match &data_type {
                    DataType::Timestamp(_, timezone) => timezone.clone(),
                    _ => None,
                },
                ansi_mode,
                is_try,
            )?)
            .call(arguments),
            data_type,
        ))
    } else {
        let name = match (is_try, timestamp_ntz) {
            (false, false) => "to_timestamp",
            (true, false) => "try_to_timestamp",
            (false, true) => "to_timestamp_ntz",
            (true, true) => "try_to_timestamp_ntz",
        };
        Err(PlanError::invalid(format!(
            "{name} requires 1 or 2 arguments"
        )))
    }
}

fn from_unixtime(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let (expr, format) = if input.arguments.len() == 1 {
        let expr = input.arguments.one()?;
        // default format is "yyyy-MM-dd HH:mm:ss"
        Ok((expr, lit("yyyy-MM-dd HH:mm:ss")))
    } else if input.arguments.len() == 2 {
        input.arguments.two()
    } else {
        return Err(PlanError::invalid(
            "from_unixtime requires 1 or 2 arguments",
        ));
    }?;

    let timezone = input.function_context.plan_config.session_timezone.clone();
    let expr = cast(
        expr,
        DataType::Timestamp(TimeUnit::Second, Some(timezone.clone())),
    );
    Ok(date_format(expr, format, timezone.to_string()))
}

fn unix_time_unit(input: ScalarFunctionInput, time_unit: TimeUnit) -> PlanResult<Expr> {
    let arg = input.arguments.one()?;
    Ok(cast(
        cast(
            arg,
            DataType::Timestamp(
                time_unit,
                Some(input.function_context.plan_config.session_timezone.clone()),
            ),
        ),
        DataType::Int64,
    ))
}

/// TIMESTAMP_SECONDS, TIMESTAMP_MILLIS and TIMESTAMP_MICROS: the instant `argument` units of
/// `micros_per_unit` microseconds after the epoch. Like every Spark TIMESTAMP, it is annotated
/// with the session time zone, so zone-dependent functions read its local date-time there. A
/// session zone Arrow cannot parse, such as `PST`, keeps UTC (#1029).
fn timestamp_from_epoch(input: ScalarFunctionInput, micros_per_unit: i64) -> PlanResult<Expr> {
    let argument = input.arguments.one()?;
    let session_timezone = &input.function_context.plan_config.session_timezone;
    let zone = if session_timezone.parse::<Tz>().is_ok() {
        Arc::clone(session_timezone)
    } else {
        "UTC".into()
    };
    let data_type = DataType::Timestamp(TimeUnit::Microsecond, Some(zone));
    if micros_per_unit != 1 {
        return Ok(cast(
            cast(argument, DataType::Int64) * lit(micros_per_unit),
            data_type,
        ));
    }
    let argument_type = argument.get_type(input.function_context.schema)?;
    Ok(
        if argument_type.is_integer() || argument_type == DataType::Null {
            cast(argument, data_type)
        } else {
            // Spark rejects other arguments (#1111). Arrow would read a STRING or DATE as a local
            // date-time in the target zone, so they keep their UTC reading.
            cast(
                argument,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            )
        },
    )
}

fn current_timestamp_microseconds(input: ScalarFunctionInput) -> PlanResult<Expr> {
    if input.arguments.is_empty() {
        let timezone = input.function_context.plan_config.session_timezone.clone();
        Ok(ScalarUDF::from(TimestampNow::new(timezone, TimeUnit::Microsecond)).call(vec![]))
    } else {
        Err(PlanError::invalid(format!(
            "current_timestamp takes 0 arguments, got {:?}",
            input.arguments
        )))
    }
}

fn current_localtimestamp_microseconds(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let expr = current_timestamp_microseconds(input)?;
    Ok(expr_fn::to_local_time(vec![expr]))
}

fn convert_tz(
    from_tz: Expr,
    to_tz: Expr,
    ts: Expr,
    classic: bool,
    with_null_short_circuit: bool,
) -> PlanResult<Expr> {
    let func = ConvertTz::new(classic);
    if with_null_short_circuit {
        let arguments = [from_tz, to_tz, ts]
            .into_iter()
            .map(|argument| lambda_with_fresh_parameter(argument, "_convert_tz"))
            .collect::<PlanResult<Vec<_>>>()?;
        Ok(Expr::HigherOrderFunction(expr::HigherOrderFunction::new(
            Arc::new(HigherOrderUDF::new_from_impl(ConvertTzLazy::new(
                func.with_null_short_circuit(),
            ))),
            arguments,
        )))
    } else {
        Ok(ScalarUDF::from(func).call(vec![from_tz, to_tz, ts]))
    }
}

// TODO: Reject every non-atomic time-zone type during analysis, even when Arrow can cast
//  it to Utf8 (for example, ARRAY). Spark's VariantType remains a supported atomic type.
fn validate_convert_timezone_zone(expr: &Expr, schema: &DFSchemaRef) -> PlanResult<()> {
    let field = expr.to_field(schema)?.1;
    if !can_cast_types(field.data_type(), &DataType::Utf8)
        && !is_variant_storage_field(field.as_ref())
    {
        return Err(PlanError::invalid(format!(
            "convert_timezone time zone arguments must be castable to string, got {}",
            field.data_type()
        )));
    }
    Ok(())
}

/// A helper function for processing the input NTZ timestamp. Spark converts a TIMESTAMP to
/// its local date-time in the session time zone.
fn ntz_timestamp_and_unit(
    ts: Expr,
    schema: &DFSchemaRef,
    session_tz: &Arc<str>,
    ansi_mode: bool,
) -> PlanResult<(Expr, TimeUnit)> {
    let data_type = ts.get_type(schema)?;
    if let DataType::Timestamp(unit, Some(zone)) = &data_type
        && let Some(local) = ltz_expr_to_local_timestamp(ts.clone(), *unit, zone, session_tz, false)
    {
        return Ok((local, TimeUnit::Microsecond));
    }
    match data_type {
        // Spark also accepts session zones that Arrow cannot parse (#1029).
        DataType::Timestamp(unit, Some(_)) => Ok((expr_fn::to_local_time(vec![ts]), unit)),
        DataType::Timestamp(unit, None) => Ok((ts, unit)),
        DataType::Null => {
            let unit = TimeUnit::Microsecond;
            Ok((cast(ts, DataType::Timestamp(unit, None)), unit))
        }
        DataType::Date32 | DataType::Date64 => {
            let unit = TimeUnit::Microsecond;
            Ok((cast(ts, DataType::Timestamp(unit, None)), unit))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let unit = TimeUnit::Microsecond;
            let ts =
                ScalarUDF::from(SparkTimestamp::try_new(None, ansi_mode, false)?).call(vec![ts]);
            Ok((ts, unit))
        }
        x => Err(PlanError::invalid(format!(
            "invalid NTZ timestamp type: {x:?}"
        ))),
    }
}

fn convert_timezone(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let args = input.arguments;
    let (from_tz, to_tz, ts) = match args.len() {
        3 => Ok(args.three()?),
        2 => {
            let (to_tz, ts) = args.two()?;
            Ok((lit(session_tz.to_string()), to_tz, ts))
        }
        _ => Err(PlanError::invalid(format!(
            "convert_timezone takes 2 or 3 arguments, got {args:?}"
        ))),
    }?;
    validate_convert_timezone_zone(&from_tz, input.function_context.schema)?;
    validate_convert_timezone_zone(&to_tz, input.function_context.schema)?;
    let (ts, _unit) = ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
        &session_tz,
        input.function_context.plan_config.ansi_mode,
    )?;
    let requires_scalar_evaluation =
        [&from_tz, &to_tz, &ts]
            .into_iter()
            .try_fold(false, |found, argument| {
                if found {
                    PlanResult::Ok(true)
                } else {
                    expr_contains_spark_cast_to_variant(argument)
                }
            })?;

    if requires_scalar_evaluation {
        Ok(
            ScalarUDF::from(ConvertTz::new(true).with_null_short_circuit())
                .call(vec![from_tz, to_tz, ts]),
        )
    } else {
        convert_tz(from_tz, to_tz, ts, true, true)
    }
}

/// A helper function for processing the input timestamp for
/// `from_utc_timestamp` and `to_utc_timestamp` functions.
/// These functions expect timestamps with time zone, but consider the value
/// relative to the UTC time zone.
fn utc_ntz_timestamp_and_unit(
    ts: Expr,
    schema: &DFSchemaRef,
    session_tz: &Arc<str>,
    ansi_mode: bool,
) -> PlanResult<(Expr, TimeUnit)> {
    let (ts, unit) = match ts.get_type(schema)? {
        DataType::Timestamp(unit, Some(_)) => (ts, unit),
        // Spark casts TIMESTAMP_NTZ and DATE to TIMESTAMP in the session zone. A TIMESTAMP_NTZ
        // skipped by a DST change moves forward and a repeated one takes its earlier instant; a
        // DATE starts at the first instant of its day.
        source @ DataType::Timestamp(unit, None) => (
            cast_local_timestamp_expr_to_ltz(ts, &source, session_tz),
            unit,
        ),
        source @ (DataType::Date32 | DataType::Date64) => (
            cast_local_timestamp_expr_to_ltz(ts, &source, session_tz),
            TimeUnit::Microsecond,
        ),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let unit = TimeUnit::Microsecond;
            let ts = ScalarUDF::from(SparkTimestamp::try_new(
                Some(session_tz.clone()),
                ansi_mode,
                false,
            )?)
            .call(vec![ts]);
            (ts, unit)
        }
        x => {
            return Err(PlanError::invalid(format!(
                "invalid UTC NTZ timestamp type: {x:?}"
            )));
        }
    };
    let ts = cast(ts, DataType::Timestamp(unit, None));
    Ok((ts, unit))
}

// TODO: Match Spark's foldability-aware zone eval for `from_utc_timestamp` and `to_utc_timestamp`:
//  a foldable NULL skips the timestamp, a non-NULL foldable zone is validated eagerly,
//  and a dynamic zone is skipped for a NULL timestamp.
fn from_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let (ts, to_tz) = input.arguments.two()?;
    let (ts, unit) = utc_ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
        &session_tz,
        input.function_context.plan_config.ansi_mode,
    )?;
    let ts = convert_tz(lit("UTC"), to_tz, ts, false, false)?;
    let ts = cast(ts, DataType::Timestamp(unit, Some(Arc::from("UTC"))));
    Ok(cast(ts, DataType::Timestamp(unit, Some(session_tz))))
}

fn to_utc_timestamp(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let (ts, from_tz) = input.arguments.two()?;
    let (ts, unit) = utc_ntz_timestamp_and_unit(
        ts,
        input.function_context.schema,
        &session_tz,
        input.function_context.plan_config.ansi_mode,
    )?;
    let ts = convert_tz(from_tz, lit("UTC"), ts, false, false)?;
    let ts = cast(ts, DataType::Timestamp(unit, Some(Arc::from("UTC"))));
    Ok(cast(ts, DataType::Timestamp(unit, Some(session_tz))))
}

fn make_timestamp_ltz(args: Vec<Expr>, session_tz: &Arc<str>, is_try: bool) -> PlanResult<Expr> {
    let ntz_ts = if args.len() == 2 || args.len() == 6 {
        ScalarUDF::from(SparkMakeTimestampNtz::new(is_try)).call(args)
    } else if args.len() == 3 || args.len() == 7 {
        let mut args = args;
        let Some(from_tz) = args.pop() else {
            unreachable!()
        };
        let ntz_ts = ScalarUDF::from(SparkMakeTimestampNtz::new(is_try)).call(args);
        convert_tz(from_tz, lit(session_tz.to_string()), ntz_ts, true, false)?
    } else {
        return Err(PlanError::invalid(format!(
            "{}make_timestamp_ltz requires 2, 3, 6 or 7 arguments, got {:?}",
            if is_try { "try_" } else { "" },
            args
        )));
    };
    Ok(cast(
        ntz_ts,
        DataType::Timestamp(TimeUnit::Microsecond, Some(session_tz.clone())),
    ))
}

fn make_timestamp_ntz(args: Vec<Expr>, is_try: bool) -> PlanResult<Expr> {
    if args.len() == 2 || args.len() == 6 {
        Ok(ScalarUDF::from(SparkMakeTimestampNtz::new(is_try)).call(args))
    } else {
        Err(PlanError::invalid(format!(
            "{}make_timestamp_ntz requires 2 or 6 arguments, got {:?}",
            if is_try { "try_" } else { "" },
            args
        )))
    }
}

fn make_timestamp(input: ScalarFunctionInput, is_try: bool) -> PlanResult<Expr> {
    let session_tz = &input.function_context.plan_config.session_timezone;
    let mut args = input.arguments;
    if args.len() == 1 {
        args.push(lit(ScalarValue::Time64Microsecond(Some(0))));
    }
    match input.function_context.plan_config.default_timestamp_type {
        DefaultTimestampType::TimestampLtz => make_timestamp_ltz(args, session_tz, is_try),
        DefaultTimestampType::TimestampNtz => {
            if args.len() == 3 || args.len() == 7 {
                args.pop();
            }
            make_timestamp_ntz(args, is_try)
        }
    }
}

fn date_part(part: Expr, date: Expr) -> Expr {
    ScalarUDF::from(SparkDatePart::new()).call(vec![part, date])
}

/// The fields Spark's EXTRACT, DATE_PART and DATEPART read from a DATE, to which it casts a
/// STRING, TIMESTAMP_NTZ or NULL source as for any implicit DATE argument; the other fields read
/// the source itself.
const DATE_FIELDS: [&str; 23] = [
    "YEAR",
    "Y",
    "YEARS",
    "YR",
    "YRS",
    "YEAROFWEEK",
    "QUARTER",
    "QTR",
    "MONTH",
    "MON",
    "MONS",
    "MONTHS",
    "WEEK",
    "W",
    "WEEKS",
    "DAY",
    "D",
    "DAYS",
    "DAYOFWEEK",
    "DOW",
    "DAYOFWEEK_ISO",
    "DOW_ISO",
    "DOY",
];

/// The DataFusion date part that gives one of Spark's day-of-week or week-year fields, and the
/// number to add to it: DAYOFWEEK numbers Sunday 1, where DataFusion's `dow` numbers it 0;
/// DAYOFWEEK_ISO numbers Monday 1, as `isodow` does; YEAROFWEEK is the ISO week-numbering year,
/// `isoyear` (#1116).
fn datafusion_week_part(field: &str) -> Option<(&'static str, i32)> {
    match field {
        "DAYOFWEEK" | "DOW" => Some(("dow", 1)),
        "DAYOFWEEK_ISO" | "DOW_ISO" => Some(("isodow", 0)),
        "YEAROFWEEK" => Some(("isoyear", 0)),
        _ => None,
    }
}

/// EXTRACT, DATE_PART and DATEPART. Spark matches the field name exactly, ignoring case, without
/// trimming it.
fn extract_part(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let schema = input.function_context.schema;
    let ansi_mode = input.function_context.plan_config.ansi_mode;
    let (part, source) = input.arguments.two()?;
    let field = match &part {
        Expr::Literal(
            ScalarValue::Utf8(Some(field))
            | ScalarValue::LargeUtf8(Some(field))
            | ScalarValue::Utf8View(Some(field)),
            _,
        ) => Some(field.to_ascii_uppercase()),
        _ => None,
    };
    let date_field = field
        .as_deref()
        .is_some_and(|field| DATE_FIELDS.contains(&field));
    let source = if date_field {
        spark_date_argument(source, schema, ansi_mode)?
    } else {
        source
    };
    Ok(match field.as_deref().and_then(datafusion_week_part) {
        Some((week_part, offset)) => {
            let value = date_part(lit(week_part), source);
            if offset == 0 {
                value
            } else {
                value + lit(offset)
            }
        }
        None => date_part(part, source),
    })
}

/// Spark's MONTHS_BETWEEN, whose nullability follows from its parts at every stage: NOT NULL for
/// NOT NULL DATE, TIMESTAMP and TIMESTAMP_NTZ arguments and `roundOff`. Spark's implicit cast of
/// a STRING argument to TIMESTAMP is nullable, which `SparkKnownNullable` declares until constant
/// folding replaces a literal's. `SparkMonthsBetweenRoundOff` applies `roundOff` after the month
/// count, reading each row's once, and gives NULL for a NULL one.
fn months_between(input: ScalarFunctionInput) -> PlanResult<Expr> {
    // args extraction:
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let round_off = (arguments.len() == 3)
        .then(|| arguments.pop())
        .flatten()
        .unwrap_or(lit(true));
    let (date1, date2) = arguments.two()?;

    // consts:
    let seconds_per_day: i64 = 24 * 60 * 60;
    let seconds_in_month = cast(lit(31 * seconds_per_day), DataType::Float64);

    // helper functions:
    let ensure_timestamp = |dt: Expr| match dt.get_type(function_context.schema) {
        Ok(DataType::Timestamp(time_unit, _tz)) => (dt.clone(), time_unit),
        Ok(data_type)
            if data_type.is_string()
                || matches!(&data_type, DataType::Dictionary(_, value) if value.is_string()) =>
        {
            (
                ScalarUDF::from(SparkKnownNullable::new()).call(vec![cast(
                    dt.clone(),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                )]),
                TimeUnit::Microsecond,
            )
        }
        _ => (
            cast(dt.clone(), DataType::Timestamp(TimeUnit::Microsecond, None)),
            TimeUnit::Microsecond,
        ),
    };

    let date_to_months =
        |dt: Expr| integer_part(dt.clone(), "YEAR") * lit(12) + integer_part(dt, "MONTH");

    let is_last_day = |dt: Expr| {
        ScalarUDF::from(SparkLastDay::new())
            .call(vec![cast(dt.clone(), DataType::Date32)])
            .eq(cast(dt, DataType::Date32))
    };

    let seconds_in_day = |dt: Expr, tu: TimeUnit| {
        (cast(dt.clone(), DataType::Int64)
            - cast(expr_fn::date_trunc(lit("DAY"), dt), DataType::Int64))
            / lit(time_unit_to_multiplier(&tu))
    };

    // prerequisites
    let (date1, tu1) = ensure_timestamp(date1.clone());
    let (date2, tu2) = ensure_timestamp(date2.clone());

    // calculations:
    let days1 = integer_part(date1.clone(), "DAY");
    let days2 = integer_part(date2.clone(), "DAY");

    let month_diff = cast(
        date_to_months(date1.clone()) - date_to_months(date2.clone()),
        DataType::Float64,
    );

    let seconds_diff = (days1.clone() - days2.clone()) * lit(seconds_per_day)
        + seconds_in_day(date1.clone(), tu1)
        - seconds_in_day(date2.clone(), tu2);

    let months_between = when(
        days1
            .eq(days2)
            .or(is_last_day(date1).and(is_last_day(date2))),
        month_diff.clone(),
    )
    .otherwise(month_diff + seconds_diff / seconds_in_month)?;

    // Spark accepts a BOOLEAN or NULL `roundOff`, and in ANSI mode a STRING, through its
    // implicit cast, which is nullable.
    let round_off = dictionary_string_values(round_off, function_context.schema)?;
    let round_off = match round_off.get_type(function_context.schema)? {
        DataType::Boolean => round_off,
        DataType::Null => cast(round_off, DataType::Boolean),
        data_type if data_type.is_string() && function_context.plan_config.ansi_mode => {
            ScalarUDF::from(SparkKnownNullable::new()).call(vec![
                ScalarUDF::from(SparkCastStringToBoolean::new(false)).call(vec![round_off]),
            ])
        }
        data_type => {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] `months_between`: The third parameter \
                 requires the \"BOOLEAN\" type, got {data_type}"
            )));
        }
    };
    Ok(ScalarUDF::from(SparkMonthsBetweenRoundOff::new()).call(vec![months_between, round_off]))
}

const MICROS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000;

/// Parses a `window` duration/start-time argument (interval string, day-time
/// interval, or integer number of microseconds, matching Spark's
/// `TimeWindow.parseExpression`) into microseconds. Months/years are rejected
/// (non-constant length).
fn window_interval_micros(expr: &Expr) -> PlanResult<i64> {
    let Expr::Literal(value, _) = expr else {
        return Err(PlanError::invalid(
            "window durations and start time must be literal strings, intervals, or integers",
        ));
    };
    if let Some(s) = value.try_as_str().flatten() {
        return match parse_interval(s)
            .map_err(|e| PlanError::invalid(format!("invalid window interval {s:?}: {e}")))?
        {
            IntervalValue::Microsecond { microseconds, .. } => Ok(microseconds),
            _ => Err(PlanError::invalid(format!(
                "window interval must not contain months or years: {s:?}"
            ))),
        };
    }
    match value {
        // Spark interprets integer literals as microseconds.
        ScalarValue::Int32(Some(v)) => Ok(*v as i64),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::DurationMicrosecond(Some(v)) => Ok(*v),
        ScalarValue::DurationMillisecond(Some(v)) => Ok(*v * 1_000),
        ScalarValue::DurationSecond(Some(v)) => Ok(*v * 1_000_000),
        ScalarValue::DurationNanosecond(Some(v)) => Ok(*v / 1_000),
        ScalarValue::IntervalDayTime(Some(v)) => {
            Ok(v.days as i64 * MICROS_PER_DAY + v.milliseconds as i64 * 1_000)
        }
        ScalarValue::IntervalMonthDayNano(Some(v)) if v.months == 0 => {
            Ok(v.days as i64 * MICROS_PER_DAY + v.nanoseconds / 1_000)
        }
        _ => Err(PlanError::invalid(
            "window durations and start time must be literal strings, day-time intervals, or integers",
        )),
    }
}

/// The parsed durations (in microseconds) of a Spark `window` call.
#[derive(Debug, Clone, Copy)]
struct WindowSpec {
    window_duration: i64,
    slide_duration: i64,
    start_time: i64,
}

/// Bound on `ceil(windowDuration / slideDuration)`
const MAX_OVERLAPPING_WINDOWS: i64 = 1_000_000;

/// Parses and validates the `window` durations from the full argument list
/// (`args[0]` is the time column; `args[1..]` are window/slide/start).
fn parse_window_spec(args: &[Expr]) -> PlanResult<WindowSpec> {
    if !(2..=4).contains(&args.len()) {
        return Err(PlanError::invalid(format!(
            "window requires 2 to 4 arguments, got {}",
            args.len()
        )));
    }
    let window_duration = window_interval_micros(&args[1])?;
    let slide_duration = match args.get(2) {
        Some(arg) => window_interval_micros(arg)?,
        None => window_duration,
    };
    let start_time = match args.get(3) {
        Some(arg) => window_interval_micros(arg)?,
        None => 0,
    };
    if window_duration <= 0 {
        return Err(PlanError::invalid(
            "window: the window duration must be greater than 0",
        ));
    }
    if slide_duration <= 0 {
        return Err(PlanError::invalid(
            "window: the slide duration must be greater than 0",
        ));
    }
    if slide_duration > window_duration {
        return Err(PlanError::invalid(
            "window: the slide duration must be less than or equal to the window duration",
        ));
    }
    if start_time >= slide_duration || start_time <= -slide_duration {
        return Err(PlanError::invalid(format!(
            "The `abs(start_time)`({start_time}L) must be < the `slide_duration`({slide_duration}L)."
        )));
    }
    let overlapping = (window_duration + slide_duration - 1) / slide_duration;
    if overlapping > MAX_OVERLAPPING_WINDOWS {
        return Err(PlanError::invalid(format!(
            "window: ceil(windowDuration / slideDuration) = {overlapping} exceeds the limit of {MAX_OVERLAPPING_WINDOWS}"
        )));
    }
    Ok(WindowSpec {
        window_duration,
        slide_duration,
        start_time,
    })
}

/// The `window` struct field type: a microsecond timestamp (non-timestamp inputs
/// are cast, matching Spark's cast of the time column to `TimestampType`).
fn window_field_type(
    time_type: &DataType,
    session_tz: &std::sync::Arc<str>,
) -> PlanResult<DataType> {
    Ok(match time_type {
        DataType::Timestamp(_, tz) => DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
        // Spark casts dates and strings to `TimestampType` (session time zone), so a
        // date becomes midnight in the session time zone, not a naive timestamp.
        DataType::Date32
        | DataType::Date64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => {
            DataType::Timestamp(TimeUnit::Microsecond, Some(session_tz.clone()))
        }
        other => {
            return Err(PlanError::invalid(format!(
                "window requires a timestamp time column, got {other:?}"
            )));
        }
    })
}

/// The Spark `window` time function: buckets a timestamp into `struct<start, end>`
/// windows. The candidate enumeration is deferred to the `SparkWindowBuckets` UDF
/// so the plan stays bounded regardless of the `window/slide` ratio.
fn window(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let schema = input.function_context.schema;
    let args = input.arguments;
    let spec = parse_window_spec(&args)?;
    let session_tz = input.function_context.plan_config.session_timezone.clone();
    let time = args
        .into_iter()
        .next()
        .ok_or_else(|| PlanError::internal("window missing time column"))?;
    let field_type = window_field_type(&time.get_type(schema)?, &session_tz)?;
    let time_ts = cast(time, field_type);
    let buckets = ScalarUDF::from(SparkWindowBuckets::new(
        spec.window_duration,
        spec.slide_duration,
        spec.start_time,
    ))
    .call(vec![time_ts]);
    Ok(ScalarUDF::from(Explode::new(ExplodeKind::Explode)).call(vec![buckets]))
}

/// The Spark `window_time` function: the event-time of a time window, defined as
/// `window.end - 1 microsecond`. Spark validates the argument via column metadata
/// markers; we approximate that with a structural check on the window struct type.
fn window_time(input: ScalarFunctionInput) -> PlanResult<Expr> {
    let schema = input.function_context.schema;
    let arg = input.arguments.one()?;
    let end_type = match arg.get_type(schema)? {
        DataType::Struct(fields)
            if fields.len() == 2
                && fields[0].name() == "start"
                && fields[1].name() == "end"
                && matches!(
                    fields[0].data_type(),
                    DataType::Timestamp(TimeUnit::Microsecond, _)
                )
                && matches!(
                    fields[1].data_type(),
                    DataType::Timestamp(TimeUnit::Microsecond, _)
                ) =>
        {
            fields[1].data_type().clone()
        }
        other => {
            return Err(PlanError::invalid(format!(
                "window_time requires a window column (struct with start and end timestamps), got {other:?}"
            )));
        }
    };
    Ok(cast(
        cast(arg.field("end"), DataType::Int64) - lit(1_i64),
        end_type,
    ))
}

pub(super) fn list_built_in_datetime_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        (
            "add_years",
            F::custom(|input| interval_arithmetic(input, "years", Operator::Plus)),
        ),
        (
            "add_months",
            F::custom(|input| interval_arithmetic(input, "months", Operator::Plus)),
        ),
        (
            "add_days",
            F::custom(|input| interval_arithmetic(input, "days", Operator::Plus)),
        ),
        ("convert_timezone", F::custom(convert_timezone)),
        ("curdate", F::nullary(expr_fn::current_date)),
        ("current_date", F::nullary(expr_fn::current_date)),
        ("current_time", F::nullary(expr_fn::current_time)),
        (
            "current_timestamp",
            F::custom(current_timestamp_microseconds),
        ),
        ("current_timezone", F::custom(current_timezone)),
        ("date_add", F::custom(|input| dateadd(input, "date_add"))),
        ("date_diff", F::custom(date_diff)),
        (
            "date_format",
            F::custom(|input| match input.arguments.len() {
                2 => {
                    let timezone = input.function_context.plan_config.session_timezone.clone();
                    // A dictionary-encoded STRING formats as its STRING values do.
                    let arguments = input
                        .arguments
                        .into_iter()
                        .map(|arg| dictionary_string_values(arg, input.function_context.schema))
                        .collect::<PlanResult<Vec<_>>>()?;
                    Ok(date_format_with_args(arguments, timezone.to_string()))
                }
                _ => Err(PlanError::invalid("date_format requires 2 arguments")),
            }),
        ),
        ("date_from_unix_date", F::cast(DataType::Date32)),
        ("date_part", F::custom(extract_part)),
        (
            "date_sub",
            F::custom(|input| date_offset_by_days(input, "date_sub", Operator::Minus)),
        ),
        ("date_trunc", F::custom(date_trunc)),
        ("dateadd", F::custom(|input| dateadd(input, "dateadd"))),
        ("datediff", F::custom(datediff)),
        ("datepart", F::custom(extract_part)),
        ("day", F::custom(|input| date_field(input, "DAY"))),
        ("dayname", F::unary(|arg| expr_fn::to_char(arg, lit("%a")))),
        ("dayofmonth", F::custom(|input| date_field(input, "DAY"))),
        (
            "dayofweek",
            F::custom(|input| Ok(integer_part(implicit_date_argument(input)?, "DOW") + lit(1))),
        ),
        ("dayofyear", F::custom(|input| date_field(input, "DOY"))),
        ("extract", F::custom(extract_part)),
        ("from_unixtime", F::custom(from_unixtime)),
        ("from_utc_timestamp", F::custom(from_utc_timestamp)),
        ("hour", F::unary(|arg| integer_part(arg, "HOUR"))),
        ("last_day", F::udf(SparkLastDay::new())),
        (
            "localtimestamp",
            F::custom(current_localtimestamp_microseconds),
        ),
        ("make_date", F::ternary(make_date)),
        ("make_dt_interval", F::udf(SparkMakeDtInterval::new())),
        ("make_interval", F::custom(make_interval)),
        ("make_time", F::udf(SparkMakeTime::new())),
        (
            "make_timestamp",
            F::custom(|input| make_timestamp(input, false)),
        ),
        (
            "make_timestamp_ltz",
            F::custom(|input| {
                make_timestamp_ltz(
                    input.arguments,
                    &input.function_context.plan_config.session_timezone,
                    false,
                )
            }),
        ),
        (
            "make_timestamp_ntz",
            F::custom(|input| make_timestamp_ntz(input.arguments, false)),
        ),
        ("make_ym_interval", F::custom(make_ym_interval)),
        ("minute", F::unary(|arg| integer_part(arg, "MINUTE"))),
        ("month", F::custom(|input| date_field(input, "MONTH"))),
        (
            "monthname",
            F::unary(|arg| expr_fn::to_char(arg, lit("%b"))),
        ),
        ("months_between", F::custom(months_between)),
        ("next_day", F::custom(next_day)),
        ("now", F::custom(current_timestamp_microseconds)),
        ("quarter", F::custom(|input| date_field(input, "QUARTER"))),
        ("second", F::unary(|arg| integer_part(arg, "SECOND"))),
        ("session_window", F::unknown("session_window")),
        ("time_bucket", F::unknown("time_bucket")),
        ("time_from_micros", F::unknown("time_from_micros")),
        ("time_from_millis", F::unknown("time_from_millis")),
        ("time_from_seconds", F::unknown("time_from_seconds")),
        ("time_to_micros", F::unknown("time_to_micros")),
        ("time_to_millis", F::unknown("time_to_millis")),
        ("time_to_seconds", F::unknown("time_to_seconds")),
        (
            "timestamp_micros",
            F::custom(|input| timestamp_from_epoch(input, 1)),
        ),
        (
            "timestamp_millis",
            F::custom(|input| timestamp_from_epoch(input, 1_000)),
        ),
        (
            "timestamp_seconds",
            F::custom(|input| timestamp_from_epoch(input, 1_000_000)),
        ),
        ("timestampadd", F::custom(timestampadd)),
        ("timestampdiff", F::custom(timestampdiff)),
        ("timediff", F::custom(timediff)),
        ("to_date", F::custom(to_date)),
        ("try_to_date", F::custom(try_to_date)),
        ("to_time", F::custom(to_time)),
        ("try_to_time", F::custom(try_to_time)),
        (
            "to_timestamp",
            F::custom(|input| {
                let timestamp_ntz = matches!(
                    input.function_context.plan_config.default_timestamp_type,
                    DefaultTimestampType::TimestampNtz
                );
                to_timestamp(input, timestamp_ntz)
            }),
        ),
        // The description for `to_timestamp_ltz` and `to_timestamp_ntz` are the same:
        //  "Parses the timestamp with the format to a timestamp without time zone. Returns null with invalid input."
        // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ltz.html
        // https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_timestamp_ntz.html
        (
            "to_timestamp_ltz",
            F::custom(|input| to_timestamp(input, false)),
        ),
        (
            "to_timestamp_ntz",
            F::custom(|input| to_timestamp(input, true)),
        ),
        ("to_unix_timestamp", F::custom(to_unix_timestamp)),
        ("to_utc_timestamp", F::custom(to_utc_timestamp)),
        ("trunc", F::custom(trunc)),
        ("try_make_interval", F::custom(try_make_interval)),
        (
            "try_make_timestamp",
            F::custom(|input| make_timestamp(input, true)),
        ),
        (
            "try_make_timestamp_ltz",
            F::custom(|input| {
                make_timestamp_ltz(
                    input.arguments,
                    &input.function_context.plan_config.session_timezone,
                    true,
                )
            }),
        ),
        (
            "try_make_timestamp_ntz",
            F::custom(|input| make_timestamp_ntz(input.arguments, true)),
        ),
        (
            "try_to_timestamp",
            F::custom(|input| {
                let timestamp_ntz = matches!(
                    input.function_context.plan_config.default_timestamp_type,
                    DefaultTimestampType::TimestampNtz
                );
                try_to_timestamp(input, timestamp_ntz)
            }),
        ),
        ("time_diff", F::udf(SparkTimeDiff::new())),
        ("time_trunc", F::udf(SparkTimeTrunc::new())),
        ("unix_date", F::custom(unix_date)),
        (
            "unix_micros",
            F::custom(|input| unix_time_unit(input, TimeUnit::Microsecond)),
        ),
        (
            "unix_millis",
            F::custom(|input| unix_time_unit(input, TimeUnit::Millisecond)),
        ),
        (
            "unix_seconds",
            F::custom(|input| unix_time_unit(input, TimeUnit::Second)),
        ),
        ("unix_timestamp", F::custom(unix_timestamp)),
        // Spark numbers Monday 0 through Sunday 6; DOW numbers Sunday 0 through Saturday 6.
        (
            "weekday",
            F::custom(|input| {
                Ok((integer_part(implicit_date_argument(input)?, "DOW") + lit(6)) % lit(7))
            }),
        ),
        (
            "weekofyear",
            F::custom(|input| {
                let date = implicit_date_argument(input)?;
                Ok(cast(expr_fn::to_char(date, lit("%V")), DataType::Int32))
            }),
        ),
        ("window", F::custom(window)),
        ("window_time", F::custom(window_time)),
        ("year", F::udf(SparkYear::new())),
        ("years", F::unary(years)),
    ]
}
