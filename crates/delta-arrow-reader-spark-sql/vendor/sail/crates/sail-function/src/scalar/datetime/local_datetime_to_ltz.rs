//! Shared runtime conversion of local DATE/NTZ values to an LTZ instant.

use std::sync::Arc;

use chrono::FixedOffset;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion_common::{Result, ScalarValue, utils::take_function_args};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

use super::convert_tz::ConvertTz;
use super::spark_date_to_timestamp::SparkDateToTimestamp;

/// Convert one DATE/NTZ value using Spark day-start and DST rules.
pub fn cast_local_timestamp_value_to_ltz(
    mut input: ScalarFunctionArgs,
    timezone: &Arc<str>,
) -> Result<ColumnarValue> {
    let [value] = take_function_args("local timestamp cast", input.args.as_slice())?;
    if matches!(value.data_type(), DataType::Date32 | DataType::Date64) {
        return SparkDateToTimestamp::new(Arc::clone(timezone), false).convert_dates(value);
    }
    let timestamp_ntz = DataType::Timestamp(TimeUnit::Microsecond, None);
    let value = value.cast_to(&timestamp_ntz, None)?;
    if is_spark_compatible_arrow_fixed_offset(timezone) {
        return value.cast_to(
            &DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone())),
            None,
        );
    }
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
        .cast_to(
            &DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone())),
            None,
        )
}

/// Whether Arrow's fixed-offset cast matches Spark's supported offset forms.
pub fn is_spark_compatible_arrow_fixed_offset(timezone: &str) -> bool {
    if !timezone.starts_with('+') && !timezone.starts_with('-') {
        return false;
    }
    if timezone.parse::<Tz>().is_err() {
        return false;
    }
    let normalized = (timezone.len() == 3).then(|| format!("{timezone}:00"));
    normalized
        .as_deref()
        .unwrap_or(timezone)
        .parse::<FixedOffset>()
        .is_ok_and(|offset| offset.local_minus_utc().unsigned_abs() <= 18 * 60 * 60)
}
