//! Mixed timestamp units compare as exact nanosecond counts.
//! Decimal128 avoids both sub-microsecond rounding and i64 nanosecond overflow.

use std::sync::Arc;

use arrow::array::{AsArray, Decimal128Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Int64Type, TimeUnit, TimestampMicrosecondType};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err, utils::take_function_args};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

use crate::function::temporal_coercion::cast_local_timestamp_value_to_ltz;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct SparkTimestampComparisonValue {
    local_timezone: Option<Arc<str>>,
}

impl SparkTimestampComparisonValue {
    pub(super) fn new(local_timezone: Option<Arc<str>>) -> Self {
        Self { local_timezone }
    }
}

fn nanoseconds_per_tick(data_type: &DataType) -> Result<i128> {
    Ok(match data_type {
        DataType::Date32 => 86_400_000_000_000,
        DataType::Date64 | DataType::Timestamp(TimeUnit::Millisecond, _) => 1_000_000,
        DataType::Timestamp(TimeUnit::Second, _) => 1_000_000_000,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000,
        DataType::Timestamp(TimeUnit::Nanosecond, _) | DataType::Null => 1,
        other => return plan_err!("unsupported timestamp comparison input {other}"),
    })
}

impl ScalarUDFImpl for SparkTimestampComparisonValue {
    fn name(&self) -> &str {
        "spark_timestamp_comparison_value"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [data_type] = take_function_args(self.name(), args)?;
        nanoseconds_per_tick(data_type)?;
        Ok(DataType::Decimal128(38, 0))
    }

    fn invoke_with_args(&self, input: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = take_function_args(self.name(), input.args.as_slice())?;
        let source_type = value.data_type();
        let scale = nanoseconds_per_tick(&source_type)?;
        if input.number_rows == 0 {
            return Ok(ColumnarValue::Array(Arc::new(
                Decimal128Array::from(Vec::<Option<i128>>::new())
                    .with_precision_and_scale(38, 0)?,
            )));
        }
        let scalar = matches!(value, ColumnarValue::Scalar(_));
        let rows = if scalar { 1 } else { input.number_rows };
        let ticks = value.cast_to(&DataType::Int64, None)?.into_array(rows)?;
        // All supported i64 units fit within Decimal128(38,0).
        let mut nanos = ticks
            .as_primitive::<Int64Type>()
            .iter()
            .map(|value| value.map(|value| i128::from(value) * scale))
            .collect::<Vec<_>>();
        if let Some(timezone) = &self.local_timezone
            && matches!(
                source_type,
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
            )
        {
            let micros = nanos
                .iter()
                .map(|value| {
                    value
                        .map(|value| {
                            i64::try_from(value.div_euclid(1_000)).map_err(|_| {
                                datafusion_common::exec_datafusion_err!(
                                    "timestamp comparison exceeds the session timezone converter range"
                                )
                            })
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            let [field] = take_function_args(self.name(), input.arg_fields.as_slice())?;
            let local_field = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, None)),
            );
            let instant_field = Arc::new(local_field.as_ref().clone().with_data_type(
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone.clone())),
            ));
            let instants = cast_local_timestamp_value_to_ltz(
                ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(Arc::new(
                        TimestampMicrosecondArray::from(micros),
                    ))],
                    arg_fields: vec![local_field],
                    number_rows: rows,
                    return_field: instant_field,
                    config_options: input.config_options,
                },
                timezone,
            )?
            .into_array(rows)?;
            for (nanos, instant) in nanos
                .iter_mut()
                .zip(instants.as_primitive::<TimestampMicrosecondType>().iter())
            {
                if let Some(local) = *nanos {
                    let Some(instant) = instant else {
                        return exec_err!(
                            "timestamp comparison exceeds the session timezone converter range"
                        );
                    };
                    // Euclidean division keeps negative fractions on the correct side of DST.
                    *nanos = Some(i128::from(instant) * 1_000 + local.rem_euclid(1_000));
                }
            }
        }
        let values = Decimal128Array::from(nanos).with_precision_and_scale(38, 0)?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &values, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(Arc::new(values)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;

    #[test]
    fn scalar_and_empty_conversion_preserve_shape() -> Result<()> {
        let conversion = SparkTimestampComparisonValue::new(Some("America/Phoenix".into()));
        for (value, expected) in [
            (
                ScalarValue::TimestampNanosecond(Some(-1), None),
                Some(25_199_999_999_999),
            ),
            (ScalarValue::TimestampNanosecond(None, None), None),
            (ScalarValue::Null, None),
            (
                ScalarValue::TimestampNanosecond(Some(-1), Some("UTC".into())),
                Some(-1),
            ),
            (
                ScalarValue::TimestampSecond(Some(i64::MAX), Some("UTC".into())),
                Some(9_223_372_036_854_775_807_000_000_000),
            ),
        ] {
            let input = ScalarFunctionArgs {
                arg_fields: vec![Arc::new(Field::new("input", value.data_type(), true))],
                args: vec![ColumnarValue::Scalar(value)],
                number_rows: 32,
                return_field: Arc::new(Field::new("result", DataType::Decimal128(38, 0), true)),
                config_options: Arc::new(ConfigOptions::default()),
            };
            assert!(matches!(
                conversion.invoke_with_args(input.clone())?,
                ColumnarValue::Scalar(ScalarValue::Decimal128(actual, 38, 0)) if actual == expected
            ));
            let empty = conversion.invoke_with_args(ScalarFunctionArgs {
                number_rows: 0,
                ..input
            })?;
            assert!(
                matches!(empty, ColumnarValue::Array(values) if values.is_empty() && values.data_type() == &DataType::Decimal128(38, 0))
            );
        }
        Ok(())
    }
}
