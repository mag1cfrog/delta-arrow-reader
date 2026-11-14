//! DATE and TIMESTAMP_NTZ values acquire an instant in the session timezone.
//! For example, midnight on 2026-01-02 in Phoenix becomes 07:00 UTC. Tagged
//! timestamps already contain instants and only need their common type selected,
//! unless they are converted to TIMESTAMP_NTZ, which takes their local date-time.
// shortcut: named-zone conversion retains ConvertTz's Chrono range; extend it in
// the full-range timestamp slice before claiming Spark's complete i64 range.

use std::sync::Arc;

#[cfg(test)]
use arrow::datatypes::Field;
use arrow::datatypes::{DataType, TimeUnit};
#[cfg(test)]
use datafusion_common::ScalarValue;
#[cfg(test)]
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_expr::{ScalarUDF, cast, expr, lit};
use sail_function::scalar::datetime::convert_tz::ConvertTz;
pub(crate) use sail_function::scalar::datetime::local_datetime_to_ltz::cast_local_timestamp_value_to_ltz;
use sail_function::scalar::datetime::spark_date_to_timestamp::SparkDateToTimestamp;
use sail_function::scalar::datetime::spark_timestamp_to_ntz::SparkTimestampToNtz;

use crate::function::is_spark_compatible_arrow_fixed_offset;

/// The TIMESTAMP in session zone `timezone` of `argument`, a TIMESTAMP_NTZ or DATE of type
/// `source`. A local time skipped by a DST change moves forward by the skipped interval and a
/// repeated one takes its earlier instant; a DATE starts at the first instant of its day.
pub(crate) fn cast_local_timestamp_expr_to_ltz(
    argument: expr::Expr,
    source: &DataType,
    timezone: &Arc<str>,
) -> expr::Expr {
    if matches!(source, DataType::Date32 | DataType::Date64) {
        return cast_date_expr_to_ltz(argument, timezone, false);
    }
    let timestamp_ntz = cast(argument, DataType::Timestamp(TimeUnit::Microsecond, None));
    let timestamp_ltz = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(timezone)));
    if is_spark_compatible_arrow_fixed_offset(timezone.as_ref()) {
        return cast(timestamp_ntz, timestamp_ltz);
    }
    // Spark's named-zone conversion resolves DST gaps and overlaps differently from Arrow.
    let instant = ScalarUDF::from(ConvertTz::new(false)).call(vec![
        lit(timezone.to_string()),
        lit("UTC"),
        timestamp_ntz,
    ]);
    cast(cast(instant, DataType::Int64), timestamp_ltz)
}

/// Spark's TIMESTAMP to TIMESTAMP_NTZ conversion of `argument`, a TIMESTAMP in `unit` annotated
/// with `argument_zone`: each instant's local date-time in `session_timezone`, whatever zone
/// the annotation names. A local date-time beyond the BIGINT range fails, or is NULL for
/// `null_on_overflow`. `None` when Arrow cannot parse the session zone, such as `Z` or `PST`
/// (#1029); the caller then keeps its previous conversion.
pub(crate) fn ltz_expr_to_local_timestamp(
    argument: expr::Expr,
    unit: TimeUnit,
    argument_zone: &Arc<str>,
    session_timezone: &Arc<str>,
    null_on_overflow: bool,
) -> Option<expr::Expr> {
    let conversion =
        SparkTimestampToNtz::for_session_zone(Arc::clone(session_timezone), null_on_overflow)?;
    let argument = match unit {
        TimeUnit::Microsecond => argument,
        _ => cast(
            argument,
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(argument_zone))),
        ),
    };
    Some(ScalarUDF::from(conversion).call(vec![argument]))
}

/// Spark's DATE to TIMESTAMP conversion in session zone `timezone`: the first instant of each
/// day, as Java's `LocalDate.atStartOfDay` gives it. When a DST change skips midnight the day
/// starts where the skipped interval ends, which differs from a TIMESTAMP_NTZ midnight when
/// the interval begins before midnight. A start beyond the BIGINT range fails, or is NULL for
/// `null_on_overflow`.
pub(crate) fn cast_date_expr_to_ltz(
    argument: expr::Expr,
    timezone: &Arc<str>,
    null_on_overflow: bool,
) -> expr::Expr {
    ScalarUDF::from(SparkDateToTimestamp::new(
        Arc::clone(timezone),
        null_on_overflow,
    ))
    .call(vec![argument])
}

/// Whether `data_types` hold a DATE and a TIMESTAMP_NTZ, and otherwise only DATE, TIMESTAMP_NTZ
/// or NULL types. Spark combines such values as microsecond TIMESTAMP_NTZ values, each DATE at its
/// midnight, where DataFusion's coercion picks nanoseconds and fails outside 1677 to 2262 (#1191).
pub(crate) fn only_dates_and_timestamp_ntz(data_types: &[DataType]) -> bool {
    data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Date32 | DataType::Date64))
        && data_types
            .iter()
            .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
        && data_types.iter().all(|data_type| {
            matches!(
                data_type,
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None) | DataType::Null
            )
        })
}

pub(crate) fn common_temporal_type(
    data_types: &[DataType],
    session_timezone: &Arc<str>,
) -> Option<DataType> {
    if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(session_timezone)),
        ))
    } else if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        data_types
            .iter()
            .any(|data_type| matches!(data_type, DataType::Date32 | DataType::Date64))
            .then_some(DataType::Date32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, AsArray, Date32Array, Date64Array, TimestampMicrosecondArray,
    };
    use arrow::datatypes::TimestampMicrosecondType;
    use datafusion_common::config::ConfigOptions;

    #[test]
    fn local_timestamp_values_keep_slices_scalars_nulls_and_empty_inputs()
    -> datafusion_common::Result<()> {
        let arrays: [ArrayRef; 3] = [
            Arc::new(Date32Array::from(vec![Some(0), Some(20455), None])),
            Arc::new(Date64Array::from(vec![Some(0), Some(1767312000000), None])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(0),
                Some(1767312000000000),
                None,
            ])),
        ];
        for (timezone, expected) in [
            ("UTC", 1767312000000000),
            ("America/Phoenix", 1767337200000000),
            ("+05:45", 1767291300000000),
        ] {
            let timezone: Arc<str> = timezone.into();
            let target = DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone()));
            let invoke = |value: ColumnarValue, rows| {
                let field = Arc::new(Field::new("input", value.data_type(), true));
                cast_local_timestamp_value_to_ltz(
                    ScalarFunctionArgs {
                        args: vec![value],
                        arg_fields: vec![field],
                        number_rows: rows,
                        return_field: Arc::new(Field::new("result", target.clone(), true)),
                        config_options: Arc::new(ConfigOptions::default()),
                    },
                    &timezone,
                )
            };
            for input in &arrays {
                let result = invoke(ColumnarValue::Array(input.slice(1, 2)), 2)?.into_array(2)?;
                assert_eq!(result.data_type(), &target);
                assert_eq!(
                    result
                        .as_primitive::<TimestampMicrosecondType>()
                        .iter()
                        .collect::<Vec<_>>(),
                    [Some(expected), None]
                );
                let empty = invoke(ColumnarValue::Array(input.slice(1, 0)), 0)?.into_array(0)?;
                assert_eq!(empty.len(), 0);
                assert_eq!(empty.data_type(), &target);
                for row in [1, 2] {
                    let input = ScalarValue::try_from_array(input, row)?;
                    let result = invoke(ColumnarValue::Scalar(input), 1)?;
                    assert!(
                        matches!(result, ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(value, ref zone)) if value == (row == 1).then_some(expected) && zone.as_ref() == Some(&timezone))
                    );
                }
            }
        }
        Ok(())
    }
}
