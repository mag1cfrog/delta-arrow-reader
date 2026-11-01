//! Spark comparisons choose a common type without changing NULLIF's result type.
//! In ANSI mode, STRING/INT compares as BIGINT; legacy mode keeps INT.

use std::sync::Arc;

use crate::function::temporal_coercion::common_temporal_type;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::{Result, plan_err};
use datafusion_expr::ScalarUDF;
use sail_function::scalar::spark_cast_string_to_boolean::SparkCastStringToBoolean;
use sail_function::scalar::spark_cast_string_to_decimal::SparkCastStringToDecimal;
use sail_function::scalar::spark_cast_string_to_float::SparkCastStringToFloat;
use sail_function::scalar::spark_cast_string_to_integer::{
    SparkCastStringToInteger, StringIntegerCastMode,
};

mod string_temporal;
mod timestamp;

pub(crate) use string_temporal::SparkStringTemporalComparisonCast;

pub(crate) fn nanosecond_comparison_cast(
    left: &DataType,
    right: &DataType,
    session_timezone: &Arc<str>,
) -> Option<ScalarUDF> {
    if left == right
        || ![left, right]
            .iter()
            .any(|data_type| matches!(data_type, DataType::Timestamp(TimeUnit::Nanosecond, _)))
        || ![left, right].iter().all(|data_type| {
            matches!(
                data_type,
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
            )
        })
    {
        return None;
    }
    if matches!(
        (left, right),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, Some(_)),
            DataType::Timestamp(TimeUnit::Nanosecond, Some(_))
        )
    ) {
        // Tagged timestamps already contain instants in the same unit.
        return None;
    }
    let local_timezone = [left, right]
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
        .then(|| session_timezone.clone());
    Some(ScalarUDF::from(
        timestamp::SparkTimestampComparisonValue::new(local_timezone),
    ))
}

pub(crate) fn string_comparison_type(
    left: &DataType,
    right: &DataType,
    ansi_mode: bool,
    session_timezone: &Arc<str>,
) -> Option<DataType> {
    let other = if left.is_string() {
        right
    } else if right.is_string() {
        left
    } else {
        return None;
    };
    match other {
        DataType::Date32 | DataType::Timestamp(TimeUnit::Microsecond, None) => Some(other.clone()),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(session_timezone.clone()),
        )),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Some(if ansi_mode {
                DataType::Int64
            } else {
                other.clone()
            })
        }
        DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) if ansi_mode => {
            Some(DataType::Float64)
        }
        DataType::Decimal128(_, scale) if *scale > 0 => Some(DataType::Float64),
        DataType::Boolean | DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            Some(other.clone())
        }
        _ => None,
    }
}

pub(crate) fn string_comparison_cast(target: &DataType, ansi_mode: bool) -> Result<ScalarUDF> {
    Ok(match target {
        DataType::Date32 | DataType::Timestamp(TimeUnit::Microsecond, _) => ScalarUDF::from(
            SparkStringTemporalComparisonCast::try_new(target, ansi_mode)?,
        ),
        DataType::Boolean => ScalarUDF::from(SparkCastStringToBoolean::new(!ansi_mode)),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let mode = if ansi_mode {
                StringIntegerCastMode::Ansi
            } else {
                StringIntegerCastMode::Legacy
            };
            ScalarUDF::from(SparkCastStringToInteger::try_new(target.clone(), mode)?)
        }
        DataType::Float32 | DataType::Float64 => {
            ScalarUDF::from(SparkCastStringToFloat::try_new(target.clone(), !ansi_mode)?)
        }
        DataType::Decimal128(precision, scale) => ScalarUDF::from(SparkCastStringToDecimal::new(
            *precision, *scale, !ansi_mode,
        )),
        _ => return plan_err!("unsupported STRING comparison type {target}"),
    })
}

pub(crate) fn temporal_comparison_type(
    left: &DataType,
    right: &DataType,
    session_timezone: &Arc<str>,
) -> Option<DataType> {
    // Nanosecond pairs use exact comparison values or retain their identical unit.
    if [left, right]
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(TimeUnit::Nanosecond, _)))
    {
        return None;
    }
    if ![left, right].iter().all(|data_type| {
        matches!(
            data_type,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) | DataType::Null
        )
    }) {
        return None;
    }
    common_temporal_type(&[left.clone(), right.clone()], session_timezone)
}
