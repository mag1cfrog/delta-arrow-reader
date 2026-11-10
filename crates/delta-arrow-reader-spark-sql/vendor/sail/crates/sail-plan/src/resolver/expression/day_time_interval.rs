// Local Spark DATE and day-time interval arithmetic. See the host crate's UPSTREAM.md.
//! Spark's DATE arithmetic with a day-time interval depends on the interval's end field: a DATE
//! plus or minus an interval ending at DAY is a DATE, and one with time units is a TIMESTAMP
//! (#952). The resolver tracks each interval's qualifier (#1152).

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion_common::DFSchemaRef;
use datafusion_expr::{ExprSchemable, expr};
use sail_common::spec::IntervalFieldType;

use crate::function::temporal_coercion::cast_local_timestamp_expr_to_ltz;
use crate::resolver::interval_qualifier::IntervalOperand;

/// The resolved `arguments` of `function`, `+`, `-`, TRY_ADD or TRY_SUBTRACT, with a DATE beside
/// a day-time interval with time units read as a TIMESTAMP at its midnight in `timezone`, as
/// Spark casts it before adding the interval (#952). `operands` holds each argument's interval
/// qualifier. A DATE beside an interval ending at DAY, or one whose qualifier is unknown, keeps
/// DATE arithmetic.
pub(super) fn cast_date_beside_time_interval(
    function: &str,
    mut arguments: Vec<expr::Expr>,
    operands: &[IntervalOperand],
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
    let time_units = matches!(
        operands.get(interval_index),
        Some(IntervalOperand::Interval {
            qualifier: Some(qualifier),
            ..
        }) if qualifier.end() != IntervalFieldType::Day
    );
    if time_units {
        let date = arguments[date_index].clone();
        arguments[date_index] = cast_local_timestamp_expr_to_ltz(date, &DataType::Date32, timezone);
    }
    arguments
}
