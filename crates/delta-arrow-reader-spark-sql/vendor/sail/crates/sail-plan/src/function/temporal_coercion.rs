//! DATE and TIMESTAMP_NTZ values acquire an instant in the session timezone.
//! For example, midnight on 2026-01-02 in Phoenix becomes 07:00 UTC. Tagged
//! timestamps already contain instants and only need their common type selected.
// shortcut: named-zone conversion retains ConvertTz's Chrono range; extend it in
// the full-range timestamp slice before claiming Spark's complete i64 range.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion_common::{ScalarValue, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, cast, expr, lit,
};
use sail_function::scalar::datetime::convert_tz::ConvertTz;

use crate::function::is_spark_compatible_arrow_fixed_offset;

pub(crate) fn cast_local_timestamp_expr_to_ltz(
    argument: expr::Expr,
    timezone: &Arc<str>,
) -> expr::Expr {
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

pub(crate) fn cast_local_timestamp_value_to_ltz(
    mut input: ScalarFunctionArgs,
    timezone: &Arc<str>,
) -> datafusion_common::Result<ColumnarValue> {
    let [value] = take_function_args("local timestamp cast", input.args.as_slice())?;
    let timestamp_ntz = DataType::Timestamp(TimeUnit::Microsecond, None);
    let value = value.cast_to(&timestamp_ntz, None)?;
    let timestamp_ltz = DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone()));
    if is_spark_compatible_arrow_fixed_offset(timezone.as_ref()) {
        return value.cast_to(&timestamp_ltz, None);
    }
    // Reuse the expression path's timezone kernel without evaluating the original input again.
    let [field] = take_function_args("local timestamp cast", input.arg_fields.as_slice())?;
    let field = Arc::new(field.as_ref().clone().with_data_type(timestamp_ntz));
    let zone_field = Arc::new(Field::new("timezone", DataType::Utf8, false));
    input.args = vec![
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(timezone.to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some("UTC".into()))),
        value,
    ];
    input.arg_fields = vec![zone_field.clone(), zone_field, field.clone()];
    input.return_field = field;
    ConvertTz::new(false)
        .invoke_with_args(input)?
        .cast_to(&DataType::Int64, None)?
        .cast_to(&timestamp_ltz, None)
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
