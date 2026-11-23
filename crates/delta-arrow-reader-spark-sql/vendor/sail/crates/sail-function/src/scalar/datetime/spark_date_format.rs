// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Write;
use std::sync::Arc;

use chrono::{Datelike, NaiveDateTime, Offset, TimeZone, Timelike};
use chrono_tz::OffsetName;
use datafusion::arrow::array::builder::StringBuilder;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{
    Array, Date32Array, Date64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::temporal_conversions::{
    as_datetime, date32_to_datetime, date64_to_datetime,
};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::datetime::localize_with_fallback;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::error::SqlResult;
use sail_sql_analyzer::literal::datetime::TimestampValue;
use sail_sql_analyzer::parser::{parse_timestamp, timestamp_parser};

use crate::scalar::datetime::format::{DateTimeFormat, DateTimeFormatInput, TimeZoneDisplay};
use crate::scalar::datetime::local_datetime_to_ltz::cast_local_timestamp_value_to_ltz;
use crate::scalar::datetime::spark_timestamp::parse_ascii_whole_second_timestamp;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateFormat {
    session_timezone: Arc<str>,
    fixed_zone_name: Option<Arc<str>>,
    signature: Signature,
}

impl SparkDateFormat {
    pub fn new(session_timezone: Arc<str>) -> Self {
        Self {
            session_timezone,
            fixed_zone_name: None,
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
    pub fn with_fixed_zone_name(mut self, name: Option<Arc<str>>) -> Self {
        self.fixed_zone_name = name;
        self
    }
}

impl ScalarUDFImpl for SparkDateFormat {
    fn name(&self) -> &str {
        "spark_date_format"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, mut input: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if input.args.len() == 2
            && !matches!(&input.args[1], ColumnarValue::Scalar(value) if value.is_null())
            && matches!(
                input.args[0].data_type(),
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
            )
        {
            let mut local = input.clone();
            local.args.truncate(1);
            local.arg_fields.truncate(1);
            input.args[0] = cast_local_timestamp_value_to_ltz(local, &self.session_timezone)?;
        }
        let ScalarFunctionArgs { args, .. } = input;
        if args.len() != 2 {
            return exec_err!("spark_date_format requires 2 arguments");
        }
        let (timestamp_arg, format_arg) = args.two()?;

        match (timestamp_arg, format_arg) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(scalar)) => {
                let format_str = match scalar.try_as_str().flatten() {
                    Some(s) => s,
                    None => return Ok(null_string_array(array.len())),
                };

                let format = DateTimeFormat::for_formatting(format_str)?
                    .with_fixed_zone_name(self.fixed_zone_name.as_deref());

                let result: StringArray = match array.data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        format_string_array_as_timestamp(&array, &self.session_timezone, &format)?
                    }
                    DataType::Date32 => format_date32_array(&array, &format)?,
                    DataType::Date64 => format_date64_array(&array, &format)?,
                    DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                        format_timestamp_array_microsecond(&array, tz.as_ref(), &format)?
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                        format_timestamp_array_millisecond(&array, tz.as_ref(), &format)?
                    }
                    DataType::Timestamp(TimeUnit::Second, tz) => format_timestamp_array_second(
                        &array,
                        tz.as_ref(),
                        &format,
                        format_str == "yyyy-MM-dd HH:mm:ss",
                    )?,
                    DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                        format_timestamp_array_nanosecond(&array, tz.as_ref(), &format)?
                    }
                    _ => {
                        return exec_err!(
                            "spark_date_format: expected date or timestamp array, got {:?}",
                            array.data_type()
                        );
                    }
                };

                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            (ColumnarValue::Array(timestamp_array), ColumnarValue::Array(format_array)) => {
                // Convert string arrays to timestamp arrays if needed
                let (timestamp_array, tz): (
                    Arc<dyn datafusion::arrow::array::Array>,
                    Option<Arc<str>>,
                ) = match timestamp_array.clone().data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        let ts_array: Arc<dyn datafusion::arrow::array::Array> =
                            Arc::new(parse_string_array_to_timestamp(
                                &timestamp_array,
                                &self.session_timezone,
                            )?);
                        (ts_array, Some(self.session_timezone.clone()))
                    }
                    DataType::Timestamp(_, tz) => (timestamp_array, tz.clone()),
                    _ => (timestamp_array, None),
                };
                format_timestamp_array_dynamic(
                    &timestamp_array,
                    &format_array,
                    tz.as_ref(),
                    self.fixed_zone_name.as_deref(),
                )
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Scalar(format_scalar)) => {
                let format_str = match format_scalar.try_as_str().flatten() {
                    Some(s) => s,
                    None => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                };

                let format = DateTimeFormat::for_formatting(format_str)?
                    .with_fixed_zone_name(self.fixed_zone_name.as_deref());

                let result = match scalar {
                    ScalarValue::Utf8(Some(value))
                    | ScalarValue::LargeUtf8(Some(value))
                    | ScalarValue::Utf8View(Some(value)) => {
                        let micros = parse_timestamp_string(
                            &value,
                            &self.session_timezone,
                            parse_timestamp,
                        )?;
                        match micros {
                            Some(micros) => format_timestamp_value(
                                micros,
                                &TimeUnit::Microsecond,
                                Some(&self.session_timezone),
                                &format,
                            )?,
                            None => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                        }
                    }
                    ScalarValue::TimestampMicrosecond(Some(value), tz) => {
                        format_timestamp_value(value, &TimeUnit::Microsecond, tz.as_ref(), &format)?
                    }
                    ScalarValue::TimestampMillisecond(Some(value), tz) => {
                        format_timestamp_value(value, &TimeUnit::Millisecond, tz.as_ref(), &format)?
                    }
                    ScalarValue::TimestampSecond(Some(value), tz) => {
                        format_timestamp_value(value, &TimeUnit::Second, tz.as_ref(), &format)?
                    }
                    ScalarValue::TimestampNanosecond(Some(value), tz) => {
                        format_timestamp_value(value, &TimeUnit::Nanosecond, tz.as_ref(), &format)?
                    }
                    ScalarValue::Date32(Some(value)) => format_date32_value(value, &format)?,
                    ScalarValue::Date64(Some(value)) => format_date64_value(value, &format)?,
                    ScalarValue::Utf8(None)
                    | ScalarValue::LargeUtf8(None)
                    | ScalarValue::Utf8View(None)
                    | ScalarValue::TimestampMicrosecond(None, _)
                    | ScalarValue::TimestampMillisecond(None, _)
                    | ScalarValue::TimestampSecond(None, _)
                    | ScalarValue::TimestampNanosecond(None, _)
                    | ScalarValue::Date32(None)
                    | ScalarValue::Date64(None) => {
                        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
                    }
                    _ => {
                        return exec_err!(
                            "spark_date_format: expected timestamp scalar, got {:?}",
                            scalar
                        );
                    }
                };

                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(result))))
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Array(format_array)) => {
                // Convert scalar string to timestamp scalar if needed
                let scalar = match scalar {
                    ScalarValue::Utf8(Some(value))
                    | ScalarValue::LargeUtf8(Some(value))
                    | ScalarValue::Utf8View(Some(value)) => {
                        let micros = parse_timestamp_string(
                            &value,
                            &self.session_timezone,
                            parse_timestamp,
                        )?;
                        match micros {
                            Some(micros) => ScalarValue::TimestampMicrosecond(
                                Some(micros),
                                Some(self.session_timezone.clone()),
                            ),
                            None => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                        }
                    }
                    _ => scalar,
                };
                let arrays = ColumnarValue::values_to_arrays(&[
                    ColumnarValue::Scalar(scalar.clone()),
                    ColumnarValue::Array(format_array),
                ])?;
                let timestamp_array = arrays[0].clone();
                let format_array = arrays[1].clone();
                // Extract timezone from scalar if present, otherwise use session timezone
                let tz = match scalar {
                    ScalarValue::TimestampMicrosecond(_, tz)
                    | ScalarValue::TimestampMillisecond(_, tz)
                    | ScalarValue::TimestampSecond(_, tz)
                    | ScalarValue::TimestampNanosecond(_, tz) => tz.clone(),
                    _ => Some(self.session_timezone.clone()),
                };
                format_timestamp_array_dynamic(
                    &timestamp_array,
                    &format_array,
                    tz.as_ref(),
                    self.fixed_zone_name.as_deref(),
                )
            }
        }
    }
}

fn null_string_array(len: usize) -> ColumnarValue {
    ColumnarValue::Array(Arc::new(StringArray::from(vec![
        Option::<String>::None;
        len
    ])))
}

/// Parse an ordinary timestamp directly and use the supplied parser for other forms.
fn parse_timestamp_string<'a>(
    value: &'a str,
    timezone: &str,
    parse_fallback: impl FnOnce(&'a str) -> SqlResult<TimestampValue<'a>>,
) -> Result<Option<i64>> {
    let (datetime, parsed_timezone) =
        if let Some(datetime) = parse_ascii_whole_second_timestamp(value) {
            (datetime, "")
        } else {
            match parse_fallback(value).and_then(TimestampValue::into_naive) {
                Ok(parsed) => parsed,
                Err(_) => return Ok(None),
            }
        };
    // Use the timezone from the parsed string if present, otherwise use the provided timezone
    let timezone: Tz = if parsed_timezone.is_empty() {
        match timezone.parse() {
            Ok(v) => v,
            Err(_e) => return Ok(None),
        }
    } else {
        match parsed_timezone.parse() {
            Ok(v) => v,
            Err(_e) => return Ok(None),
        }
    };
    let datetime = match localize_with_fallback(&timezone, &datetime) {
        Ok(v) => v,
        Err(_e) => return Ok(None),
    };
    Ok(Some(datetime.timestamp_micros()))
}

/// Parse a string array to a TimestampMicrosecondArray.
fn parse_string_array_to_timestamp(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    timezone: &str,
) -> Result<TimestampMicrosecondArray> {
    let mut builder = TimestampMicrosecondArray::builder(array.len());
    let mut batch_parser = timestamp_parser();
    let mut parse_row = |value| parse_timestamp_string(value, timezone, &mut batch_parser);

    match array.data_type() {
        DataType::Utf8 => {
            let string_array = as_string_array(array)?;
            for value in string_array.iter() {
                match value {
                    Some(v) => match parse_row(v)? {
                        Some(micros) => builder.append_value(micros),
                        None => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
        }
        DataType::LargeUtf8 => {
            let string_array = as_large_string_array(array)?;
            for value in string_array.iter() {
                match value {
                    Some(v) => match parse_row(v)? {
                        Some(micros) => builder.append_value(micros),
                        None => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
        }
        DataType::Utf8View => {
            let string_array = as_string_view_array(array)?;
            for value in string_array.iter() {
                match value {
                    Some(v) => match parse_row(v)? {
                        Some(micros) => builder.append_value(micros),
                        None => builder.append_null(),
                    },
                    None => builder.append_null(),
                }
            }
        }
        _ => {
            return exec_err!(
                "parse_string_array_to_timestamp: expected string array, got {:?}",
                array.data_type()
            );
        }
    }

    Ok(builder.finish())
}

/// Format a string array by first parsing strings as timestamps, then formatting.
fn format_string_array_as_timestamp(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    timezone: &str,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let timestamp_array = parse_string_array_to_timestamp(array, timezone)?;
    let tz: Arc<str> = Arc::from(timezone);
    let timestamp_array: Arc<dyn datafusion::arrow::array::Array> = Arc::new(timestamp_array);
    format_timestamp_array_microsecond(&timestamp_array, Some(&tz), format)
}

fn format_timestamp_array_dynamic(
    timestamp_array: &Arc<dyn datafusion::arrow::array::Array>,
    format_array: &Arc<dyn datafusion::arrow::array::Array>,
    tz: Option<&Arc<str>>,
    fixed_zone_name: Option<&str>,
) -> Result<ColumnarValue> {
    if timestamp_array.len() != format_array.len() {
        return exec_err!(
            "spark_date_format: timestamp and format arrays must have the same length"
        );
    }
    let mut cache = HashMap::<String, DateTimeFormat>::new();
    let result = match format_array.data_type() {
        DataType::Utf8 => {
            let formats = as_string_array(format_array)?;
            format_timestamp_array_with_formats(
                timestamp_array,
                formats.iter(),
                tz,
                &mut cache,
                fixed_zone_name,
            )?
        }
        DataType::LargeUtf8 => {
            let formats = as_large_string_array(format_array)?;
            format_timestamp_array_with_formats(
                timestamp_array,
                formats.iter(),
                tz,
                &mut cache,
                fixed_zone_name,
            )?
        }
        DataType::Utf8View => {
            let formats = as_string_view_array(format_array)?;
            format_timestamp_array_with_formats(
                timestamp_array,
                formats.iter(),
                tz,
                &mut cache,
                fixed_zone_name,
            )?
        }
        _ => return exec_err!("spark_date_format: expected string array for format argument"),
    };
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn format_timestamp_array_with_formats<'f>(
    timestamp_array: &Arc<dyn datafusion::arrow::array::Array>,
    formats: impl Iterator<Item = Option<&'f str>>,
    tz: Option<&Arc<str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
    fixed_zone_name: Option<&str>,
) -> Result<StringArray> {
    match timestamp_array.data_type() {
        DataType::Date32 => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    exec_datafusion_err!("spark_date_format: failed to downcast to Date32Array")
                })?;
            format_date32_values(values.iter(), formats, cache, fixed_zone_name)
        }
        DataType::Date64 => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| {
                    exec_datafusion_err!("spark_date_format: failed to downcast to Date64Array")
                })?;
            format_date64_values(values.iter(), formats, cache, fixed_zone_name)
        }
        DataType::Timestamp(TimeUnit::Microsecond, array_tz) => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "spark_date_format: failed to downcast to TimestampMicrosecondArray"
                    )
                })?;
            // Use array's timezone if present, otherwise use the provided timezone
            let effective_tz = array_tz.as_ref().or(tz);
            format_timestamp_values(
                values.iter(),
                formats,
                &TimeUnit::Microsecond,
                effective_tz,
                cache,
                fixed_zone_name,
            )
        }
        DataType::Timestamp(TimeUnit::Millisecond, array_tz) => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "spark_date_format: failed to downcast to TimestampMillisecondArray"
                    )
                })?;
            // Use array's timezone if present, otherwise use the provided timezone
            let effective_tz = array_tz.as_ref().or(tz);
            format_timestamp_values(
                values.iter(),
                formats,
                &TimeUnit::Millisecond,
                effective_tz,
                cache,
                fixed_zone_name,
            )
        }
        DataType::Timestamp(TimeUnit::Second, array_tz) => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "spark_date_format: failed to downcast to TimestampSecondArray"
                    )
                })?;
            // Use array's timezone if present, otherwise use the provided timezone
            let effective_tz = array_tz.as_ref().or(tz);
            format_timestamp_values(
                values.iter(),
                formats,
                &TimeUnit::Second,
                effective_tz,
                cache,
                fixed_zone_name,
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, array_tz) => {
            let values = timestamp_array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "spark_date_format: failed to downcast to TimestampNanosecondArray"
                    )
                })?;
            // Use array's timezone if present, otherwise use the provided timezone
            let effective_tz = array_tz.as_ref().or(tz);
            format_timestamp_values(
                values.iter(),
                formats,
                &TimeUnit::Nanosecond,
                effective_tz,
                cache,
                fixed_zone_name,
            )
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            // Handle string arrays by parsing them as timestamps
            let tz_str = tz.map(|s| s.as_ref()).unwrap_or_else(|| {
                // This shouldn't happen in practice since we always pass a timezone
                // from the caller, but we provide a fallback
                "UTC"
            });
            let timestamp_array = parse_string_array_to_timestamp(timestamp_array, tz_str)?;
            let timestamp_array: Arc<dyn datafusion::arrow::array::Array> =
                Arc::new(timestamp_array);
            // Recursively call with the parsed timestamp array
            format_timestamp_array_with_formats(
                &timestamp_array,
                formats,
                tz,
                cache,
                fixed_zone_name,
            )
        }
        _ => exec_err!(
            "spark_date_format: expected date or timestamp array, got {:?}",
            timestamp_array.data_type()
        ),
    }
}

fn format_date32_values<'v, 'f>(
    values: impl Iterator<Item = Option<i32>>,
    formats: impl Iterator<Item = Option<&'f str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
    fixed_zone_name: Option<&str>,
) -> Result<StringArray> {
    values
        .zip(formats)
        .map(|(value, format)| match (value, format) {
            (Some(value), Some(format)) => {
                let format = get_or_parse_format(cache, format, fixed_zone_name)?;
                format_date32_value(value, format).map(Some)
            }
            _ => Ok(None),
        })
        .collect::<Result<StringArray>>()
}

fn format_date64_values<'v, 'f>(
    values: impl Iterator<Item = Option<i64>>,
    formats: impl Iterator<Item = Option<&'f str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
    fixed_zone_name: Option<&str>,
) -> Result<StringArray> {
    values
        .zip(formats)
        .map(|(value, format)| match (value, format) {
            (Some(value), Some(format)) => {
                let format = get_or_parse_format(cache, format, fixed_zone_name)?;
                format_date64_value(value, format).map(Some)
            }
            _ => Ok(None),
        })
        .collect::<Result<StringArray>>()
}

fn format_timestamp_values<'v, 'f>(
    values: impl Iterator<Item = Option<i64>>,
    formats: impl Iterator<Item = Option<&'f str>>,
    time_unit: &TimeUnit,
    tz: Option<&Arc<str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
    fixed_zone_name: Option<&str>,
) -> Result<StringArray> {
    values
        .zip(formats)
        .map(|(value, format)| match (value, format) {
            (Some(value), Some(format)) => {
                let format = get_or_parse_format(cache, format, fixed_zone_name)?;
                format_timestamp_value(value, time_unit, tz, format).map(Some)
            }
            _ => Ok(None),
        })
        .collect::<Result<StringArray>>()
}

fn get_or_parse_format<'a>(
    cache: &'a mut HashMap<String, DateTimeFormat>,
    pattern: &str,
    fixed_zone_name: Option<&str>,
) -> Result<&'a DateTimeFormat> {
    match cache.entry(pattern.to_string()) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => Ok(entry.insert(
            DateTimeFormat::for_formatting(pattern)?.with_fixed_zone_name(fixed_zone_name),
        )),
    }
}

fn format_timestamp_value(
    value: i64,
    time_unit: &TimeUnit,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
) -> Result<String> {
    let naive_datetime = timestamp_to_naive_datetime(value, time_unit)?;

    match tz {
        Some(tz_str) => {
            let tz = tz_str
                .parse::<Tz>()
                .map_err(|e| exec_datafusion_err!("Invalid timezone '{}': {}", tz_str, e))?;
            let datetime = tz.from_utc_datetime(&naive_datetime);
            let named = tz_str
                .parse::<chrono_tz::Tz>()
                .ok()
                .map(|zone| zone.from_utc_datetime(&naive_datetime));
            let named_abbreviation = named.as_ref().and_then(|date| date.offset().abbreviation());
            let fixed_name = named_abbreviation.is_none().then(|| {
                if datetime.offset().fix().local_minus_utc() == 0 {
                    "Z".to_owned()
                } else {
                    datetime.offset().fix().to_string()
                }
            });
            let abbreviation = named_abbreviation.or(fixed_name.as_deref());
            format.format(DateTimeFormatInput {
                datetime: datetime.naive_local(),
                timezone: Some(TimeZoneDisplay {
                    offset: datetime.offset().fix(),
                    name: Some(tz_str),
                    abbreviation,
                }),
                zone_id: Some(tz_str),
            })
        }
        None => format.format(DateTimeFormatInput {
            datetime: naive_datetime,
            timezone: None,
            zone_id: None,
        }),
    }
}

fn timestamp_to_naive_datetime(value: i64, time_unit: &TimeUnit) -> Result<NaiveDateTime> {
    match time_unit {
        TimeUnit::Microsecond => {
            as_datetime::<datafusion::arrow::datatypes::TimestampMicrosecondType>(value)
        }
        TimeUnit::Millisecond => {
            as_datetime::<datafusion::arrow::datatypes::TimestampMillisecondType>(value)
        }
        TimeUnit::Second => as_datetime::<datafusion::arrow::datatypes::TimestampSecondType>(value),
        TimeUnit::Nanosecond => {
            as_datetime::<datafusion::arrow::datatypes::TimestampNanosecondType>(value)
        }
    }
    .ok_or_else(|| {
        exec_datafusion_err!(
            "spark_date_format: cannot convert timestamp value {} to datetime",
            value
        )
    })
}

fn format_date32_value(value: i32, format: &DateTimeFormat) -> Result<String> {
    let datetime = date32_to_datetime(value).ok_or_else(|| {
        exec_datafusion_err!("spark_date_format: cannot convert date32 value {value} to datetime")
    })?;
    format.format(DateTimeFormatInput {
        datetime,
        timezone: None,
        zone_id: None,
    })
}

fn format_date64_value(value: i64, format: &DateTimeFormat) -> Result<String> {
    let datetime = date64_to_datetime(value).ok_or_else(|| {
        exec_datafusion_err!("spark_date_format: cannot convert date64 value {value} to datetime")
    })?;
    format.format(DateTimeFormatInput {
        datetime,
        timezone: None,
        zone_id: None,
    })
}

fn format_date32_array(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let date_array = array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or_else(|| {
            exec_datafusion_err!("spark_date_format: failed to downcast to Date32Array")
        })?;

    let result: Vec<Option<String>> = date_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_date32_value(value, format))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}

fn format_date64_array(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let date_array = array
        .as_any()
        .downcast_ref::<Date64Array>()
        .ok_or_else(|| {
            exec_datafusion_err!("spark_date_format: failed to downcast to Date64Array")
        })?;

    let result: Vec<Option<String>> = date_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_date64_value(value, format))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}

fn format_timestamp_array_microsecond(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let ts_array = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            exec_datafusion_err!(
                "spark_date_format: failed to downcast to TimestampMicrosecondArray"
            )
        })?;

    let result: Vec<Option<String>> = ts_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_timestamp_value(value, &TimeUnit::Microsecond, tz, format))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}

fn format_timestamp_array_millisecond(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let ts_array = array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| {
            exec_datafusion_err!(
                "spark_date_format: failed to downcast to TimestampMillisecondArray"
            )
        })?;

    let result: Vec<Option<String>> = ts_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_timestamp_value(value, &TimeUnit::Millisecond, tz, format))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}

fn format_timestamp_array_second(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
    is_default_unix_pattern: bool,
) -> Result<StringArray> {
    let ts_array = array
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .ok_or_else(|| {
            exec_datafusion_err!("spark_date_format: failed to downcast to TimestampSecondArray")
        })?;

    // The default pattern uses local clock fields without displaying the zone.
    let prepared_zone = if is_default_unix_pattern {
        tz.and_then(|name| name.parse::<Tz>().ok())
    } else {
        None
    };
    // Keep the existing error ordering if the result could overflow Utf8 offsets.
    // The pattern uses at most ten year digits and fifteen more bytes.
    if let Some(zone) = prepared_zone
        && ts_array.len() <= (i32::MAX as usize) / 25
    {
        return format_default_second_array(ts_array, zone);
    }
    let format_second = |value| {
        let Some(zone) = prepared_zone else {
            return format_timestamp_value(value, &TimeUnit::Second, tz, format);
        };
        let utc = timestamp_to_naive_datetime(value, &TimeUnit::Second)?;
        format.format(DateTimeFormatInput {
            datetime: zone.from_utc_datetime(&utc).naive_local(),
            timezone: None,
            zone_id: None,
        })
    };
    let result: Vec<Option<String>> = ts_array
        .iter()
        .map(|opt| opt.map(format_second).transpose())
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}

fn format_default_second_array(array: &TimestampSecondArray, zone: Tz) -> Result<StringArray> {
    let value_capacity = (array.len() - array.null_count()) * 19;
    let mut output = StringBuilder::with_capacity(array.len(), value_capacity);
    for value in array {
        let Some(value) = value else {
            output.append_null();
            continue;
        };
        let utc = timestamp_to_naive_datetime(value, &TimeUnit::Second)?;
        let datetime = zone.from_utc_datetime(&utc).naive_local();
        let year = datetime.year();
        let year_of_era = if year <= 0 { 1 - year } else { year };
        write!(
            output,
            "{year_of_era:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            datetime.month(),
            datetime.day(),
            datetime.hour(),
            datetime.minute(),
            datetime.second(),
        )
        .map_err(|_| exec_datafusion_err!("spark_date_format: failed to write a timestamp"))?;
        output.append_value("");
    }
    Ok(output.finish())
}

fn format_timestamp_array_nanosecond(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    tz: Option<&Arc<str>>,
    format: &DateTimeFormat,
) -> Result<StringArray> {
    let ts_array = array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| {
            exec_datafusion_err!(
                "spark_date_format: failed to downcast to TimestampNanosecondArray"
            )
        })?;

    let result: Vec<Option<String>> = ts_array
        .iter()
        .map(|opt| {
            opt.map(|value| format_timestamp_value(value, &TimeUnit::Nanosecond, tz, format))
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(StringArray::from(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_second_builder_skips_value_allocation_for_nulls() -> Result<()> {
        let input = TimestampSecondArray::new_null(8_192);
        let output = format_default_second_array(&input, "UTC".parse::<Tz>()?)?;
        assert_eq!(output.null_count(), input.len());
        assert_eq!(output.values().capacity(), 0);
        Ok(())
    }

    #[test]
    fn default_second_builder_matches_the_formatter_at_year_boundaries() -> Result<()> {
        let format = DateTimeFormat::for_formatting("yyyy-MM-dd HH:mm:ss")?;
        let seconds = [
            Some(NaiveDateTime::MIN.and_utc().timestamp() + 172_800),
            None,
            Some(-62_167_219_201),
            Some(-62_167_219_200),
            Some(0),
            Some(253_402_300_800),
            Some(NaiveDateTime::MAX.and_utc().timestamp() - 172_800),
        ];
        for timezone in ["UTC", "America/Los_Angeles", "+05:45"] {
            let timezone: Arc<str> = timezone.into();
            let format_seconds = |input: &[Option<i64>], use_builder| {
                let array: Arc<dyn Array> = Arc::new(
                    TimestampSecondArray::from(input.to_vec()).with_timezone(Arc::clone(&timezone)),
                );
                format_timestamp_array_second(&array, Some(&timezone), &format, use_builder)
            };
            assert_eq!(
                format_seconds(&seconds, true)?,
                format_seconds(&seconds, false)?
            );

            let mut invalid = seconds.to_vec();
            invalid.extend([Some(i64::MAX), Some(i64::MIN)]);
            assert_eq!(
                format_seconds(&invalid, true).unwrap_err().to_string(),
                format_seconds(&invalid, false).unwrap_err().to_string(),
            );
        }
        Ok(())
    }

    #[test]
    fn ordinary_date_format_timestamps_preserve_fallback_localization() -> Result<()> {
        for (input, expected_micros, expects_fallback) in [
            ("1970-01-01 00:00:01", Some(1_000_000), false),
            ("1969-12-31 23:59:59", Some(-1_000_000), false),
            ("1970-01-01 00:00:01.123456", Some(1_123_456), true),
            ("1970-01-01T00:00:01Z", Some(1_000_000), true),
            ("1970-01-01 01:00:01+01:00", Some(1_000_000), true),
            ("1970-01-01 23:59:60", Some(86_400_000_000), true),
            ("1970-02-30 00:00:01", None, true),
        ] {
            let mut used_fallback = false;
            let micros = parse_timestamp_string(input, "UTC", |value| {
                used_fallback = true;
                parse_timestamp(value)
            })?;
            assert_eq!(micros, expected_micros, "{input}");
            assert_eq!(used_fallback, expects_fallback, "{input}");
        }
        Ok(())
    }
}
