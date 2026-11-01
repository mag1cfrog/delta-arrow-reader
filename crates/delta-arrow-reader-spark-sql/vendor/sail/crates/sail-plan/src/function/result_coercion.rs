//! Select common result types for conditional values and set-operation columns.
//! For example, STRING with INT returns BIGINT in ANSI mode and STRING otherwise.

use arrow::datatypes::{DataType, TimeUnit};
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr};
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_cast_string_to_boolean::SparkCastStringToBoolean;
use sail_function::scalar::spark_cast_string_to_float::SparkCastStringToFloat;
use sail_function::scalar::spark_cast_string_to_integer::{
    SparkCastStringToInteger, StringIntegerCastMode,
};
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::error::{PlanError, PlanResult};
use crate::function::common::FunctionContextInput;
use crate::function::numeric_coercion::common_numeric_type;
use crate::function::temporal_coercion::{cast_local_timestamp_expr_to_ltz, common_temporal_type};

pub(crate) fn coerce_result_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = arguments
        .iter()
        .map(|arg| arg.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(target) = common_numeric_type(&data_types, function_context.plan_config.ansi_mode) {
        return Ok(arguments
            .into_iter()
            .zip(data_types)
            .map(|(arg, source)| {
                if source == target {
                    arg
                } else {
                    cast(arg, target.clone())
                }
            })
            .collect());
    }
    if let Some(target) =
        common_result_type_with_strings(&data_types, function_context.plan_config.ansi_mode)?
    {
        return arguments
            .into_iter()
            .zip(data_types)
            .map(|(arg, source)| {
                if source == target {
                    return Ok(arg);
                }
                let arg = if is_string_type(&source) {
                    match target {
                        DataType::Int64 => SparkCastStringToInteger::try_new(
                            DataType::Int64,
                            StringIntegerCastMode::Ansi,
                        )?
                        .into_expr(arg),
                        DataType::Float64 => ScalarUDF::from(SparkCastStringToFloat::try_new(
                            DataType::Float64,
                            false,
                        )?)
                        .call(vec![arg]),
                        DataType::Boolean => {
                            ScalarUDF::from(SparkCastStringToBoolean::new(false)).call(vec![arg])
                        }
                        _ => cast(arg, target.clone()),
                    }
                } else if target == DataType::Utf8
                    && (is_temporal_type(&source)
                        || matches!(source, DataType::Float32 | DataType::Float64))
                {
                    ScalarUDF::from(SparkToUtf8::new()).call(vec![arg])
                } else {
                    cast(arg, target.clone())
                };
                Ok(arg)
            })
            .collect();
    }
    let has_string = data_types.iter().any(is_string_type);
    let temporal_type =
        common_temporal_type(&data_types, &function_context.plan_config.session_timezone);
    let arguments = if has_string {
        if let Some(temporal_type) = temporal_type {
            if function_context.plan_config.ansi_mode {
                arguments
                    .into_iter()
                    .zip(data_types.iter())
                    .map(|(arg, data_type)| coerce_to_temporal(arg, data_type, &temporal_type))
                    .collect::<PlanResult<Vec<_>>>()?
            } else {
                arguments
                    .into_iter()
                    .zip(data_types)
                    .map(|(arg, data_type)| {
                        if is_temporal_type(&data_type) {
                            ScalarUDF::from(SparkToUtf8::new()).call(vec![arg])
                        } else {
                            arg
                        }
                    })
                    .collect()
            }
        } else {
            arguments
        }
    } else if let Some(temporal_type) = temporal_type {
        arguments
            .into_iter()
            .zip(data_types.iter())
            .map(|(arg, data_type)| coerce_to_temporal(arg, data_type, &temporal_type))
            .collect::<PlanResult<Vec<_>>>()?
    } else {
        arguments
    };
    Ok(arguments)
}

// STRING with INT selects BIGINT in ANSI mode and STRING otherwise.
pub(crate) fn common_result_type_with_strings(
    data_types: &[DataType],
    ansi_mode: bool,
) -> PlanResult<Option<DataType>> {
    if !data_types.iter().any(is_string_type) {
        return Ok(None);
    }
    let alternatives = data_types
        .iter()
        .filter(|data_type| !is_string_type(data_type) && **data_type != DataType::Null)
        .collect::<Vec<_>>();
    let is_numeric = |data_type: &DataType| {
        data_type.is_signed_integer()
            || matches!(
                data_type,
                DataType::Float32 | DataType::Float64 | DataType::Decimal128(..)
            )
    };
    if alternatives.iter().any(|data_type| {
        !is_numeric(data_type)
            && !is_temporal_type(data_type)
            && !matches!(data_type, DataType::Boolean | DataType::Binary)
    }) {
        return Ok(None);
    }
    let target = if alternatives.is_empty() {
        Some(DataType::Utf8)
    } else if !ansi_mode {
        alternatives
            .iter()
            .all(|data_type| !matches!(data_type, DataType::Boolean | DataType::Binary))
            .then_some(DataType::Utf8)
    } else if alternatives.iter().all(|data_type| is_numeric(data_type)) {
        Some(
            if alternatives
                .iter()
                .any(|data_type| !data_type.is_signed_integer())
            {
                DataType::Float64
            } else {
                DataType::Int64
            },
        )
    } else if alternatives
        .iter()
        .all(|data_type| **data_type == DataType::Boolean)
    {
        Some(DataType::Boolean)
    } else if alternatives
        .iter()
        .all(|data_type| **data_type == DataType::Binary)
    {
        Some(DataType::Binary)
    } else if alternatives
        .iter()
        .all(|data_type| is_temporal_type(data_type))
    {
        // Temporal alternatives keep the existing session-timezone conversion.
        return Ok(None);
    } else {
        None
    };
    target.map(Some).ok_or_else(|| {
        PlanError::invalid(format!("Incompatible STRING result types: {data_types:?}"))
    })
}

fn coerce_to_temporal(
    arg: expr::Expr,
    data_type: &DataType,
    target_type: &DataType,
) -> PlanResult<expr::Expr> {
    if data_type == target_type {
        return Ok(arg);
    }
    if is_string_type(data_type) {
        match target_type {
            DataType::Date32 => Ok(ScalarUDF::from(SparkDate::new(false)).call(vec![arg])),
            // This is only reached when ANSI mode requires a temporal common type.
            DataType::Timestamp(_, timezone) => {
                Ok(
                    ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), true, false)?)
                        .call(vec![arg]),
                )
            }
            _ => Ok(cast(arg, target_type.clone())),
        }
    } else if matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
    ) && let DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) = target_type
    {
        Ok(cast_local_timestamp_expr_to_ltz(arg, timezone))
    } else if is_temporal_type(data_type) {
        Ok(cast(arg, target_type.clone()))
    } else {
        Ok(arg)
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

pub(crate) fn is_temporal_type(data_type: &DataType) -> bool {
    is_date_type(data_type) || matches!(data_type, DataType::Timestamp(_, _))
}

fn is_date_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
}
