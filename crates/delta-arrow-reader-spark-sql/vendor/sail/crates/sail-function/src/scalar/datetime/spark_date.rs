// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use chrono::NaiveDate;
use datafusion::arrow::array::{Array, Date32Array, new_null_array};
use datafusion::arrow::compute::kernels::cast_utils::Parser;
use datafusion::arrow::datatypes::{DataType, Date32Type};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_sql_analyzer::parser::parse_date;

use crate::scalar::datetime::format::DateTimeFormat;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDate {
    signature: Signature,
    is_try: bool,
}

impl SparkDate {
    /// Creates a SparkDate.
    ///
    /// When `is_try` is true, returns NULL on invalid input (for try_cast).
    /// When `is_try` is false, throws an error on invalid input (for cast).
    pub fn new(is_try: bool) -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            is_try,
        }
    }

    fn string_to_date32(value: &str, is_try: bool) -> Result<Option<i32>> {
        if let Some(date) = parse_complete_numeric_date(value) {
            return Ok(Some(date));
        }
        // Match a complete numeric date before invoking Arrow's broader parser.
        let bytes = value.as_bytes();
        if bytes.len() == 10
            && bytes.iter().enumerate().all(|(index, byte)| match index {
                4 | 7 => *byte == b'-',
                _ => byte.is_ascii_digit(),
            })
            && let Some(date) = Date32Type::parse(value)
        {
            return Ok(Some(date));
        }
        if has_unpadded_date_shape(bytes)
            && let Some(date) = Date32Type::parse(value)
        {
            return Ok(Some(date));
        }
        match parse_date(value).and_then(|date| Ok(Date32Type::from_naive_date(date.try_into()?))) {
            Ok(v) => Ok(Some(v)),
            Err(_e) if is_try => Ok(None),
            Err(_) => {
                let sql_value = value.replace('\\', "\\\\").replace('\'', "\\'");
                Err(exec_datafusion_err!(
                    "[CAST_INVALID_INPUT] The value '{sql_value}' of the type \"STRING\" cannot be cast to \"DATE\" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input and return NULL instead."
                ))
            }
        }
    }

    fn formatted_string_to_date32(
        value: &str,
        format: &DateTimeFormat,
        is_try: bool,
    ) -> Result<Option<i32>> {
        match format
            .parse_date_value(value)
            .map(Date32Type::from_naive_date)
        {
            Ok(v) => Ok(Some(v)),
            Err(_e) if is_try => Ok(None),
            Err(e) => Err(e),
        }
    }
}

fn parse_complete_numeric_date(value: &str) -> Option<i32> {
    fn decimal(bytes: &[u8]) -> Option<u32> {
        bytes.iter().try_fold(0, |value, byte| {
            let digit = byte.wrapping_sub(b'0');
            (digit < 10).then_some(value * 10 + u32::from(digit))
        })
    }

    let bytes = value.as_bytes();
    let [_, _, _, _, b'-', tail @ ..] = bytes else {
        return None;
    };
    let (month, day) = match tail {
        [_, b'-', _] | [_, b'-', _, _] => (&tail[..1], &tail[2..]),
        [_, _, b'-', _] | [_, _, b'-', _, _] => (&tail[..2], &tail[3..]),
        _ => return None,
    };
    // Four ASCII digits fit in the signed year field.
    let year = decimal(&bytes[..4])? as i32;
    let date = NaiveDate::from_ymd_opt(year, decimal(month)?, decimal(day)?)?;
    Some(Date32Type::from_naive_date(date))
}

fn has_unpadded_date_shape(bytes: &[u8]) -> bool {
    let [
        year_thousands,
        year_hundreds,
        year_tens,
        year_ones,
        b'-',
        month,
        tail @ ..,
    ] = bytes
    else {
        return false;
    };
    // The whole-input check excludes Arrow's broader prefix and compact forms.
    [year_thousands, year_hundreds, year_tens, year_ones, month]
        .into_iter()
        .all(u8::is_ascii_digit)
        && match tail {
            [b'-', day] => day.is_ascii_digit(),
            [b'-', first, second] | [first, b'-', second] => {
                first.is_ascii_digit() && second.is_ascii_digit()
            }
            _ => false,
        }
}

impl ScalarUDFImpl for SparkDate {
    fn name(&self) -> &str {
        "spark_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn propagates_constant_folding_errors(&self) -> bool {
        !self.is_try
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let (arg, format) = match args.len() {
            1 => (args.one()?, None),
            2 => {
                let (arg, format) = args.two()?;
                (arg, Some(format))
            }
            _ => return exec_err!("spark_date requires 1 or 2 arguments"),
        };
        let is_try = self.is_try;
        match (arg, format) {
            (ColumnarValue::Array(array), Some(ColumnarValue::Array(format_array))) => {
                parse_array_with_format_array(&array, &format_array, is_try)
            }
            (ColumnarValue::Scalar(scalar), Some(ColumnarValue::Array(format_array))) => {
                let arrays = ColumnarValue::values_to_arrays(&[
                    ColumnarValue::Scalar(scalar),
                    ColumnarValue::Array(format_array),
                ])?;
                let array = arrays[0].clone();
                let format_array = arrays[1].clone();
                parse_array_with_format_array(&array, &format_array, is_try)
            }
            (ColumnarValue::Array(array), format) => {
                let format = match format {
                    Some(ColumnarValue::Scalar(scalar)) => {
                        match parse_scalar_date_format(&scalar)? {
                            Some(format) => Some(format),
                            None => {
                                return Ok(ColumnarValue::Array(new_null_array(
                                    &DataType::Date32,
                                    array.len(),
                                )));
                            }
                        }
                    }
                    Some(ColumnarValue::Array(_)) => unreachable!(),
                    None => None,
                };
                let array = match array.data_type() {
                    DataType::Utf8 => as_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                Some(format) => Self::formatted_string_to_date32(v, format, is_try),
                                None => Self::string_to_date32(v, is_try),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<Date32Array>>()?,
                    DataType::LargeUtf8 => as_large_string_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                Some(format) => Self::formatted_string_to_date32(v, format, is_try),
                                None => Self::string_to_date32(v, is_try),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<Date32Array>>()?,
                    DataType::Utf8View if format.is_none() => {
                        let strings = as_string_view_array(&array)?;
                        let mut dates = Date32Array::builder(strings.len());
                        for text in strings {
                            let date = text
                                .map(|text| Self::string_to_date32(text, is_try))
                                .transpose()?
                                .flatten();
                            dates.append_option(date);
                        }
                        dates.finish()
                    }
                    DataType::Utf8View => as_string_view_array(&array)?
                        .iter()
                        .map(|x| {
                            x.map(|v| match &format {
                                Some(format) => Self::formatted_string_to_date32(v, format, is_try),
                                None => Self::string_to_date32(v, is_try),
                            })
                            .transpose()
                            .map(|opt| opt.flatten())
                        })
                        .collect::<Result<Date32Array>>()?,
                    _ => return exec_err!("expected string array for `date`"),
                };
                Ok(ColumnarValue::Array(Arc::new(array)))
            }
            (ColumnarValue::Scalar(scalar), format) => {
                let format = match format {
                    Some(ColumnarValue::Scalar(scalar)) => {
                        match parse_scalar_date_format(&scalar)? {
                            Some(format) => Some(format),
                            None => {
                                return Ok(ColumnarValue::Scalar(ScalarValue::Date32(None)));
                            }
                        }
                    }
                    Some(ColumnarValue::Array(_)) => unreachable!(),
                    None => None,
                };
                let value = match scalar.try_as_str() {
                    Some(x) => x
                        .map(|v| match &format {
                            Some(format) => Self::formatted_string_to_date32(v, format, is_try),
                            None => Self::string_to_date32(v, is_try),
                        })
                        .transpose()?
                        .flatten(),
                    _ => {
                        return exec_err!("expected string scalar for `date`");
                    }
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Date32(value)))
            }
        }
    }
}

fn parse_scalar_date_format(format: &ScalarValue) -> Result<Option<DateTimeFormat>> {
    match format.try_as_str() {
        Some(Some(pattern)) => Ok(Some(DateTimeFormat::for_parsing(pattern)?)),
        Some(None) => Ok(None),
        None => exec_err!("spark_date format argument must be a string scalar"),
    }
}

fn parse_array_with_format_array(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    format_array: &Arc<dyn datafusion::arrow::array::Array>,
    is_try: bool,
) -> Result<ColumnarValue> {
    if array.len() != format_array.len() {
        return exec_err!("spark_date value and format arrays must have the same length");
    }
    let mut cache = HashMap::<String, DateTimeFormat>::new();
    let array = match format_array.data_type() {
        DataType::Utf8 => {
            let formats = as_string_array(format_array)?;
            parse_array_with_formats(array, formats.iter(), &mut cache, is_try)?
        }
        DataType::LargeUtf8 => {
            let formats = as_large_string_array(format_array)?;
            parse_array_with_formats(array, formats.iter(), &mut cache, is_try)?
        }
        DataType::Utf8View => {
            let formats = as_string_view_array(format_array)?;
            parse_array_with_formats(array, formats.iter(), &mut cache, is_try)?
        }
        _ => return exec_err!("spark_date format argument must be a string array"),
    };
    Ok(ColumnarValue::Array(Arc::new(array)))
}

fn parse_array_with_formats<'f>(
    array: &Arc<dyn datafusion::arrow::array::Array>,
    formats: impl Iterator<Item = Option<&'f str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
    is_try: bool,
) -> Result<Date32Array> {
    match array.data_type() {
        DataType::Utf8 => {
            parse_values_with_formats(as_string_array(array)?.iter(), formats, cache, is_try)
        }
        DataType::LargeUtf8 => {
            parse_values_with_formats(as_large_string_array(array)?.iter(), formats, cache, is_try)
        }
        DataType::Utf8View => {
            parse_values_with_formats(as_string_view_array(array)?.iter(), formats, cache, is_try)
        }
        _ => exec_err!("expected string array for `date`"),
    }
}

fn parse_values_with_formats<'v, 'f>(
    values: impl Iterator<Item = Option<&'v str>>,
    formats: impl Iterator<Item = Option<&'f str>>,
    cache: &mut HashMap<String, DateTimeFormat>,
    is_try: bool,
) -> Result<Date32Array> {
    values
        .zip(formats)
        .map(|(value, format)| match (value, format) {
            (Some(value), Some(format)) => {
                let format = get_or_parse_format(cache, format)?;
                SparkDate::formatted_string_to_date32(value, format, is_try)
            }
            _ => Ok(None),
        })
        .collect::<Result<_>>()
}

fn get_or_parse_format<'a>(
    cache: &'a mut HashMap<String, DateTimeFormat>,
    pattern: &str,
) -> Result<&'a DateTimeFormat> {
    match cache.entry(pattern.to_string()) {
        Entry::Occupied(entry) => Ok(entry.into_mut()),
        Entry::Vacant(entry) => Ok(entry.insert(DateTimeFormat::for_parsing(pattern)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unpadded_date_route_matches_the_original_grammar() -> Result<()> {
        for (text, matches_unpadded_shape) in [
            ("2024-1-2", true),
            ("2024-1-02", true),
            ("2024-01-2", true),
            ("0000-2-29", true),
            ("9999-12-1", true),
            ("2024-2-30", true),
            ("2024-1-2x", false),
            ("2024-1-2T", false),
            ("2024-1-2 ", false),
            ("2024-1-2-0", false),
            ("20240102", false),
            ("+024-1-2", false),
            ("2024-１-2", false),
        ] {
            assert_eq!(
                has_unpadded_date_shape(text.as_bytes()),
                matches_unpadded_shape,
                "{text}"
            );
            let original = parse_date(text)
                .and_then(|date| Ok(Date32Type::from_naive_date(date.try_into()?)))
                .ok();
            assert_eq!(SparkDate::string_to_date32(text, true)?, original, "{text}");
            let strict = SparkDate::string_to_date32(text, false);
            assert_eq!(strict.is_err(), original.is_none(), "{text}");
            if let Some(days) = original {
                assert_eq!(strict?, Some(days), "{text}");
            }
        }
        Ok(())
    }

    #[test]
    fn complete_numeric_date_matches_the_arrow_calendar_parser() {
        for year in [0, 1, 1899, 1900, 1999, 2000, 2024, 2100, 9999] {
            for month in 0..=13 {
                for day in 0..=32 {
                    let inputs = [
                        format!("{year:04}-{month}-{day}"),
                        format!("{year:04}-{month:02}-{day}"),
                        format!("{year:04}-{month}-{day:02}"),
                        format!("{year:04}-{month:02}-{day:02}"),
                    ];
                    for input in inputs {
                        assert_eq!(
                            parse_complete_numeric_date(&input),
                            Date32Type::parse(&input),
                            "{input}",
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn unformatted_date_view_builder_matches_row_collection()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        use datafusion::arrow::array::StringViewArray;
        use datafusion::arrow::datatypes::Field;
        use datafusion::common::config::ConfigOptions;

        let captures: std::collections::BTreeMap<String, Vec<String>> = serde_json::from_str(
            include_str!("../../../tests/fixtures/date-cast-1385-inputs.json"),
        )?;
        for (category, strings) in captures {
            let values = std::iter::once(None)
                .chain(strings.iter().map(|text| Some(text.as_str())))
                .collect::<Vec<_>>();
            let input = Arc::new(StringViewArray::from(values));
            let rows = input.len();
            for is_try in [false, true] {
                let expected = input
                    .iter()
                    .map(|text| {
                        text.map(|text| SparkDate::string_to_date32(text, is_try))
                            .transpose()
                            .map(Option::flatten)
                    })
                    .collect::<Result<Date32Array>>();
                let actual = SparkDate::new(is_try)
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![ColumnarValue::Array(input.clone())],
                        arg_fields: vec![Arc::new(Field::new("text", DataType::Utf8View, true))],
                        return_field: Arc::new(Field::new("date", DataType::Date32, true)),
                        number_rows: rows,
                        config_options: Arc::new(ConfigOptions::default()),
                    })
                    .and_then(|result| result.into_array(rows));
                match (expected, actual) {
                    (Ok(expected), Ok(actual)) => {
                        assert_eq!(expected.to_data(), actual.to_data(), "{category}, {is_try}");
                    }
                    (Err(expected), Err(actual)) => {
                        assert_eq!(
                            std::mem::discriminant(expected.find_root()),
                            std::mem::discriminant(actual.find_root()),
                            "{category}, {is_try}",
                        );
                        assert_eq!(
                            expected.to_string(),
                            actual.to_string(),
                            "{category}, {is_try}"
                        );
                    }
                    (expected, actual) => {
                        assert_eq!(
                            format!("{expected:?}"),
                            format!("{actual:?}"),
                            "{category}, {is_try}"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn date_row_path_matches_existing_parser() -> std::result::Result<(), Box<dyn std::error::Error>>
    {
        let categories: std::collections::BTreeMap<String, Vec<String>> = serde_json::from_str(
            include_str!("../../../tests/fixtures/date-cast-1385-inputs.json"),
        )?;
        assert_eq!(categories.len(), 13);
        let mut comparisons = 0;
        for (category, values) in categories {
            assert!(!values.is_empty());
            for value in values {
                let original = parse_date(&value)
                    .and_then(|date| Ok(Date32Type::from_naive_date(date.try_into()?)));
                for is_try in [false, true] {
                    let expected = match &original {
                        Ok(days) => Ok(Some(*days)),
                        Err(_) if is_try => Ok(None),
                        Err(_) => Err(()),
                    };
                    let actual = SparkDate::string_to_date32(&value, is_try).map_err(|error| {
                        assert!(error.to_string().contains("[CAST_INVALID_INPUT]"));
                    });
                    assert_eq!(actual, expected, "{category}: {value:?}, is_try={is_try}");
                    comparisons += 1;
                }
            }
        }
        assert_eq!(comparisons, 204);
        Ok(())
    }
}
