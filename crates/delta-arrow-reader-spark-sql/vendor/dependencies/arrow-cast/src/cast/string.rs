// Modified for the DAR Spark SQL runtime. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::cast::*;
use arrow_buffer::NullBuffer;

/// The first input row that the strict string parser could not convert.
#[derive(Debug)]
pub struct StringParseFailure {
    /// Zero-based row index in the supplied array, including NULL rows.
    pub row_index: usize,
    /// The original error returned by the strict cast parser.
    pub error: ArrowError,
}

/// Successfully parsed input rows, stopping before the first invalid value.
#[derive(Debug)]
pub struct ParsedStringRows<P: Parser> {
    /// Exactly the rows before `failure.row_index`, or all rows on success.
    /// Input NULLs remain NULL, and the datatype is `P::DATA_TYPE`.
    pub values: PrimitiveArray<P>,
    /// The first invalid row and its original error, if parsing stopped early.
    pub failure: Option<StringParseFailure>,
}

/// Strictly parse string rows, retaining the rows before the first parse error.
///
/// Accepts [`DataType::Utf8`], [`DataType::LargeUtf8`] and [`DataType::Utf8View`].
/// Parsing stops at the first invalid non-NULL value. If its zero-based input
/// index is `r`, `values` contains exactly rows `[0, r)`, preserving NULLs. The
/// failing value is excluded. Indices are relative to the supplied array, even
/// when it is a slice. Each reached non-NULL value is parsed once.
///
/// Invalid values produce a [`StringParseFailure`] inside the successful result.
/// Unsupported input types or invalid primitive array construction return an
/// outer [`ArrowError`]. Existing [`cast_with_options`] entrypoints still return
/// a parse error directly when `safe` is false.
///
/// # Example
///
/// ```
/// use arrow_array::{Int32Array, StringArray};
/// use arrow_array::types::Int32Type;
/// use arrow_cast::parse_string_rows_until_error;
/// use arrow_schema::ArrowError;
///
/// let input = StringArray::from(vec![Some("12"), None, Some("bad"), Some("34")]);
/// let parsed = parse_string_rows_until_error::<Int32Type>(&input)?;
/// assert_eq!(parsed.values, Int32Array::from(vec![Some(12), None]));
/// let failure = parsed.failure.unwrap();
/// assert_eq!(failure.row_index, 2);
/// assert!(matches!(failure.error, ArrowError::CastError(_)));
/// # Ok::<(), ArrowError>(())
/// ```
pub fn parse_string_rows_until_error<P: Parser>(
    array: &dyn Array,
) -> Result<ParsedStringRows<P>, ArrowError> {
    let (values, failure) = match array.data_type() {
        DataType::Utf8 => parse_string_rows_iter::<P, _>(array.as_string::<i32>().iter()),
        DataType::LargeUtf8 => parse_string_rows_iter::<P, _>(array.as_string::<i64>().iter()),
        DataType::Utf8View => parse_string_rows_iter::<P, _>(array.as_string_view().iter()),
        other => {
            return Err(ArrowError::CastError(format!(
                "Cannot parse string rows from {other} type"
            )));
        }
    };
    let nulls = array.nulls().map(|nulls| {
        if values.len() == array.len() {
            nulls.clone()
        } else {
            nulls.slice(0, values.len())
        }
    });
    Ok(ParsedStringRows {
        values: PrimitiveArray::try_new(values.into(), nulls)?,
        failure,
    })
}

pub(crate) fn value_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
    options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let mut builder = GenericStringBuilder::<O>::new();
    let formatter = ArrayFormatter::try_new(array, &options.format_options)?;
    let nulls = array.nulls();
    for i in 0..array.len() {
        match nulls.map(|x| x.is_null(i)).unwrap_or_default() {
            true => builder.append_null(),
            false => {
                formatter.value(i).write(&mut builder)?;
                // tell the builder the row is finished
                builder.append_value("");
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

pub(crate) fn value_to_string_view(
    array: &dyn Array,
    options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let mut builder = StringViewBuilder::with_capacity(array.len());
    let formatter = ArrayFormatter::try_new(array, &options.format_options)?;
    let nulls = array.nulls();
    // buffer to avoid reallocating on each value
    // TODO: replace with write to builder after https://github.com/apache/arrow-rs/issues/6373
    let mut buffer = String::new();
    for i in 0..array.len() {
        match nulls.map(|x| x.is_null(i)).unwrap_or_default() {
            true => builder.append_null(),
            false => {
                // write to buffer first and then copy into target array
                buffer.clear();
                formatter.value(i).write(&mut buffer)?;
                builder.append_value(&buffer)
            }
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Parse UTF-8
pub(crate) fn parse_string<P: Parser, O: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let string_array = array.as_string::<O>();
    parse_string_iter::<P, _, _>(string_array.iter(), cast_options, || {
        string_array.nulls().cloned()
    })
}

/// Parse UTF-8 View
pub(crate) fn parse_string_view<P: Parser>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let string_view_array = array.as_string_view();
    parse_string_iter::<P, _, _>(string_view_array.iter(), cast_options, || {
        string_view_array.nulls().cloned()
    })
}

fn parse_string_iter<
    'a,
    P: Parser,
    I: Iterator<Item = Option<&'a str>>,
    F: FnOnce() -> Option<NullBuffer>,
>(
    iter: I,
    cast_options: &CastOptions,
    nulls: F,
) -> Result<ArrayRef, ArrowError> {
    let array = if cast_options.safe {
        let iter = iter.map(|x| x.and_then(P::parse));

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { PrimitiveArray::<P>::from_trusted_len_iter(iter) }
    } else {
        let (values, failure) = parse_string_rows_iter::<P, _>(iter);
        if let Some(failure) = failure {
            return Err(failure.error);
        }
        PrimitiveArray::try_new(values.into(), nulls())?
    };

    Ok(Arc::new(array) as ArrayRef)
}

fn parse_string_rows_iter<'a, P: Parser, I: Iterator<Item = Option<&'a str>>>(
    iter: I,
) -> (Vec<P::Native>, Option<StringParseFailure>) {
    let mut values = Vec::new();
    for value in iter {
        let parsed = match value {
            Some(value) => match P::parse(value) {
                Some(parsed) => parsed,
                None => {
                    let row_index = values.len();
                    let failure = StringParseFailure {
                        row_index,
                        error: ArrowError::CastError(format!(
                            "Cannot cast string '{value}' to value of {} type",
                            P::DATA_TYPE
                        )),
                    };
                    return (values, Some(failure));
                }
            },
            None => P::Native::default(),
        };
        values.push(parsed);
    }
    (values, None)
}

/// Casts generic string arrays to an ArrowTimestampType (TimeStampNanosecondArray, etc.)
pub(crate) fn cast_string_to_timestamp<O: OffsetSizeTrait, T: ArrowTimestampType>(
    array: &dyn Array,
    to_tz: &Option<Arc<str>>,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_string::<O>();
    let out: PrimitiveArray<T> = match to_tz {
        Some(tz) => {
            let tz: Tz = tz.as_ref().parse()?;
            cast_string_to_timestamp_impl(array.iter(), &tz, cast_options)?
        }
        None => cast_string_to_timestamp_impl(array.iter(), &Utc, cast_options)?,
    };
    Ok(Arc::new(out.with_timezone_opt(to_tz.clone())))
}

/// Casts string view arrays to an ArrowTimestampType (TimeStampNanosecondArray, etc.)
pub(crate) fn cast_view_to_timestamp<T: ArrowTimestampType>(
    array: &dyn Array,
    to_tz: &Option<Arc<str>>,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_string_view();
    let out: PrimitiveArray<T> = match to_tz {
        Some(tz) => {
            let tz: Tz = tz.as_ref().parse()?;
            cast_string_to_timestamp_impl(array.iter(), &tz, cast_options)?
        }
        None => cast_string_to_timestamp_impl(array.iter(), &Utc, cast_options)?,
    };
    Ok(Arc::new(out.with_timezone_opt(to_tz.clone())))
}

fn cast_string_to_timestamp_impl<
    'a,
    I: Iterator<Item = Option<&'a str>>,
    T: ArrowTimestampType,
    Tz: TimeZone,
>(
    iter: I,
    tz: &Tz,
    cast_options: &CastOptions,
) -> Result<PrimitiveArray<T>, ArrowError> {
    if cast_options.safe {
        let iter = iter.map(|v| {
            v.and_then(|v| {
                let naive = string_to_datetime(tz, v).ok()?.naive_utc();
                T::from_naive_datetime(naive, None)
            })
        });
        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.

        Ok(unsafe { PrimitiveArray::from_trusted_len_iter(iter) })
    } else {
        let vec = iter
            .map(|v| {
                v.map(|v| {
                    let naive = string_to_datetime(tz, v)?.naive_utc();
                    T::from_naive_datetime(naive, None).ok_or_else(|| match T::UNIT {
                        TimeUnit::Nanosecond => ArrowError::CastError(format!(
                            "Overflow converting {naive} to Nanosecond. The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804"
                        )),
                        _ => ArrowError::CastError(format!(
                            "Overflow converting {naive} to {:?}",
                            T::UNIT
                        ))
                    })
                })
                    .transpose()
            })
            .collect::<Result<Vec<Option<i64>>, _>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        Ok(unsafe { PrimitiveArray::from_trusted_len_iter(vec.iter()) })
    }
}

pub(crate) fn cast_string_to_interval<Offset, F, ArrowType>(
    array: &dyn Array,
    cast_options: &CastOptions,
    parse_function: F,
) -> Result<ArrayRef, ArrowError>
where
    Offset: OffsetSizeTrait,
    ArrowType: ArrowPrimitiveType,
    F: Fn(&str) -> Result<ArrowType::Native, ArrowError> + Copy,
{
    let string_array = array
        .as_any()
        .downcast_ref::<GenericStringArray<Offset>>()
        .unwrap();
    cast_string_to_interval_impl::<_, ArrowType, F>(
        string_array.iter(),
        cast_options,
        parse_function,
    )
}

pub(crate) fn cast_string_to_year_month_interval<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    cast_string_to_interval::<Offset, _, IntervalYearMonthType>(
        array,
        cast_options,
        parse_interval_year_month,
    )
}

pub(crate) fn cast_string_to_day_time_interval<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    cast_string_to_interval::<Offset, _, IntervalDayTimeType>(
        array,
        cast_options,
        parse_interval_day_time,
    )
}

pub(crate) fn cast_string_to_month_day_nano_interval<Offset: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    cast_string_to_interval::<Offset, _, IntervalMonthDayNanoType>(
        array,
        cast_options,
        parse_interval_month_day_nano,
    )
}

pub(crate) fn cast_view_to_interval<F, ArrowType>(
    array: &dyn Array,
    cast_options: &CastOptions,
    parse_function: F,
) -> Result<ArrayRef, ArrowError>
where
    ArrowType: ArrowPrimitiveType,
    F: Fn(&str) -> Result<ArrowType::Native, ArrowError> + Copy,
{
    let string_view_array = array.as_any().downcast_ref::<StringViewArray>().unwrap();
    cast_string_to_interval_impl::<_, ArrowType, F>(
        string_view_array.iter(),
        cast_options,
        parse_function,
    )
}

pub(crate) fn cast_view_to_year_month_interval(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    cast_view_to_interval::<_, IntervalYearMonthType>(
        array,
        cast_options,
        parse_interval_year_month,
    )
}

pub(crate) fn cast_view_to_day_time_interval(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    cast_view_to_interval::<_, IntervalDayTimeType>(array, cast_options, parse_interval_day_time)
}

pub(crate) fn cast_view_to_month_day_nano_interval(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    cast_view_to_interval::<_, IntervalMonthDayNanoType>(
        array,
        cast_options,
        parse_interval_month_day_nano,
    )
}

fn cast_string_to_interval_impl<'a, I, ArrowType, F>(
    iter: I,
    cast_options: &CastOptions,
    parse_function: F,
) -> Result<ArrayRef, ArrowError>
where
    I: Iterator<Item = Option<&'a str>>,
    ArrowType: ArrowPrimitiveType,
    F: Fn(&str) -> Result<ArrowType::Native, ArrowError> + Copy,
{
    let interval_array = if cast_options.safe {
        let iter = iter.map(|v| v.and_then(|v| parse_function(v).ok()));

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { PrimitiveArray::<ArrowType>::from_trusted_len_iter(iter) }
    } else {
        let vec = iter
            .map(|v| v.map(parse_function).transpose())
            .collect::<Result<Vec<_>, ArrowError>>()?;

        // Benefit:
        //     20% performance improvement
        // Soundness:
        //     The iterator is trustedLen because it comes from an `StringArray`.
        unsafe { PrimitiveArray::<ArrowType>::from_trusted_len_iter(vec) }
    };
    Ok(Arc::new(interval_array) as ArrayRef)
}

/// A specified helper to cast from `GenericBinaryArray` to `GenericStringArray` when they have same
/// offset size so re-encoding offset is unnecessary.
fn extend_valid_utf8<'a, B, I>(builder: &mut B, iter: I)
where
    B: Extend<Option<&'a str>>,
    I: Iterator<Item = Option<&'a [u8]>>,
{
    builder.extend(iter.map(|value| value.and_then(|bytes| std::str::from_utf8(bytes).ok())));
}

pub(crate) fn cast_binary_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<GenericByteArray<GenericBinaryType<O>>>()
        .unwrap();

    match GenericStringArray::<O>::try_from_binary(array.clone()) {
        Ok(a) => Ok(Arc::new(a)),
        Err(e) => match cast_options.safe
            || array.iter().flatten().all(|value| std::str::from_utf8(value).is_ok())
        {
            true => {
                // Only visible non-NULL values contribute to this fallback.
                let mut builder =
                    GenericStringBuilder::<O>::with_capacity(array.len(), array.value_data().len());

                extend_valid_utf8(&mut builder, array.iter());
                Ok(Arc::new(builder.finish()))
            }
            false => Err(e),
        },
    }
}

pub(crate) fn cast_binary_view_to_string_view(
    array: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_binary_view();

    match array.clone().to_string_view() {
        Ok(result) => Ok(Arc::new(result)),
        Err(error) => match cast_options.safe
            || array.iter().flatten().all(|value| std::str::from_utf8(value).is_ok())
        {
            true => {
                let mut builder = StringViewBuilder::with_capacity(array.len());
                extend_valid_utf8(&mut builder, array.iter());
                Ok(Arc::new(builder.finish()))
            }
            false => Err(error),
        },
    }
}

/// Casts string to boolean
fn cast_string_to_boolean<'a, StrArray>(
    array: &StrArray,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    StrArray: StringArrayType<'a>,
{
    let output_array = array
        .iter()
        .map(|value| match value {
            Some(value) => cast_single_string_to_boolean(value, cast_options),
            None => Ok(None),
        })
        .collect::<Result<BooleanArray, _>>()?;

    Ok(Arc::new(output_array))
}

#[inline]
fn cast_single_string_to_boolean(
    value: &str,
    cast_options: &CastOptions,
) -> Result<Option<bool>, ArrowError> {
    match value.to_ascii_lowercase().trim() {
        "t" | "tr" | "tru" | "true" | "y" | "ye" | "yes" | "on" | "1" => Ok(Some(true)),
        "f" | "fa" | "fal" | "fals" | "false" | "n" | "no" | "of" | "off" | "0" => Ok(Some(false)),
        invalid_value => match cast_options.safe {
            true => Ok(None),
            false => Err(ArrowError::CastError(format!(
                "Cannot cast value '{invalid_value}' to value of Boolean type",
            ))),
        },
    }
}

/// Cast a single string to boolean with default cast option(safe=true).
pub fn cast_single_string_to_boolean_default(value: &str) -> Option<bool> {
    cast_single_string_to_boolean(value, &CastOptions::default())
        .ok()
        .flatten()
}

pub(crate) fn cast_utf8_to_boolean<OffsetSize>(
    from: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError>
where
    OffsetSize: OffsetSizeTrait,
{
    let array = from
        .as_any()
        .downcast_ref::<GenericStringArray<OffsetSize>>()
        .unwrap();

    cast_string_to_boolean(&array, cast_options)
}

pub(crate) fn cast_utf8view_to_boolean(
    from: &dyn Array,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = from.as_any().downcast_ref::<StringViewArray>().unwrap();

    cast_string_to_boolean(&array, cast_options)
}
