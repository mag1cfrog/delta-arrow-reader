// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use chrono::{DateTime, FixedOffset, MappedLocalTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::{GapInfo, Tz};
use datafusion::arrow::array::timezone::Tz as ArrowTz;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int64Array, UInt64Array, new_null_array};
use datafusion::arrow::compute::kernels::{cast, numeric, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion_common::error::DataFusionError;
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::function::Hint;
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaParametersProgress, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, ValueOrLambda, Volatility,
};
use datafusion_expr_common::signature::Signature;
use datafusion_functions::utils::make_scalar_function;

use crate::functions_nested_utils::{evaluate_lambdas_until_null, scatter_active_rows};
use crate::scalar::datetime::format::parse_fixed_zone_offset;

/// A helper scalar UDF for converting time zones for timestamps.
/// The timestamp must be NTZ timestamp, which should have [`None`] time zone
/// in the Arrow data type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertTz {
    /// Whether to use the "classic" algorithm to convert time zone.
    /// The "classic" algorithm is used by the `convert_timezone` function in Spark,
    /// while the "non-classic" algorithm is used by the `from_utc_timestamp` and
    /// `to_utc_timestamp` functions in Spark.
    classic: bool,
    /// Whether NULL rows short-circuit before parsing their time zones.
    null_short_circuit: bool,
    signature: Signature,
}

impl ConvertTz {
    pub fn new(classic: bool) -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
            classic,
            null_short_circuit: false,
        }
    }

    pub fn with_null_short_circuit(mut self) -> Self {
        self.null_short_circuit = true;
        self
    }
}

/// Evaluates `convert_timezone` arguments from left to right and stops at the first NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertTzLazy {
    signature: HigherOrderSignature,
    convert_tz: ConvertTz,
    literal_zone_pair: bool,
}

impl ConvertTzLazy {
    pub fn new(convert_tz: ConvertTz) -> Self {
        Self {
            signature: HigherOrderSignature::variadic_any(Volatility::Immutable),
            convert_tz,
            literal_zone_pair: false,
        }
    }

    pub fn with_literal_zone_pair(mut self, literal_zone_pair: bool) -> Self {
        self.literal_zone_pair = literal_zone_pair;
        self
    }
}

impl HigherOrderUDFImpl for ConvertTzLazy {
    fn name(&self) -> &str {
        "convert_tz_lazy"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        check_lazy_convert_tz_args(fields)?;
        let dummy = Arc::new(Field::new("", DataType::Null, true));
        Ok(LambdaParametersProgress::Complete(
            fields.iter().map(|_| vec![Arc::clone(&dummy)]).collect(),
        ))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        check_lazy_convert_tz_args(args.arg_fields)?;
        let fields = args
            .arg_fields
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Lambda(field) => Ok(Arc::clone(field)),
                ValueOrLambda::Value(_) => {
                    exec_err!("convert_timezone expected lambda arguments")
                }
            })
            .collect::<Result<Vec<_>>>()?;

        ScalarUDFImpl::return_field_from_args(
            &self.convert_tz,
            ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: args.scalar_arguments,
            },
        )
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        check_lazy_convert_tz_args(&args.args)?;
        let lambdas = args
            .args
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Lambda(lambda) => Ok(lambda),
                ValueOrLambda::Value(_) => {
                    exec_err!("convert_timezone expected lambda arguments")
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let arg_fields = args
            .arg_fields
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Lambda(field) => Ok(Arc::clone(field)),
                ValueOrLambda::Value(_) => {
                    exec_err!("convert_timezone expected lambda arguments")
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // TODO: Add single-pass row-level lambda evaluation so errors follow Spark's row-major
        // order. Replaying batches is not equivalent because it re-evaluates volatile/stateful
        // bodies, which Spark requires to run at most once per row.
        let (mut values, active_rows) = evaluate_lambdas_until_null(&lambdas, args.number_rows)?;
        if active_rows.is_empty() {
            return Ok(ColumnarValue::Array(new_null_array(
                args.return_type(),
                args.number_rows,
            )));
        }

        if self.literal_zone_pair {
            // Keep arguments lazy, while giving the conversion one copy of each zone.
            values[0] = values[0].slice(0, 1);
            values[1] = values[1].slice(0, 1);
        }

        let value = ScalarUDFImpl::invoke_with_args(
            &self.convert_tz,
            ScalarFunctionArgs {
                args: values.into_iter().map(ColumnarValue::Array).collect(),
                arg_fields,
                number_rows: active_rows.len(),
                return_field: Arc::clone(&args.return_field),
                config_options: Arc::clone(&args.config_options),
            },
        )?
        .into_array(active_rows.len())?;

        Ok(ColumnarValue::Array(scatter_active_rows(
            value,
            &active_rows,
            args.number_rows,
        )?))
    }
}

fn check_lazy_convert_tz_args<V, L>(args: &[ValueOrLambda<V, L>]) -> Result<()> {
    if args.len() != 3 {
        return exec_err!(
            "convert_timezone takes 3 internal arguments, got {}",
            args.len()
        );
    }
    if args
        .iter()
        .any(|arg| matches!(arg, ValueOrLambda::Value(_)))
    {
        return exec_err!("convert_timezone expected lambda arguments");
    }
    Ok(())
}

impl ScalarUDFImpl for ConvertTz {
    fn name(&self) -> &str {
        "convert_tz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_, _, ts] = arg_types else {
            return plan_err!("`convert_tz` takes 3 arguments: from, to, timestamp");
        };
        match ts {
            DataType::Timestamp(unit, None) => Ok(DataType::Timestamp(*unit, None)),
            _ => plan_err!("`convert_tz` expects NTZ timestamp but got {ts:?}"),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = self.return_type(&arg_types)?;
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(
            |args| convert_tz_inner(args, self.classic, self.null_short_circuit),
            [Hint::AcceptsSingular].repeat(args.args.len()),
        )(args.args.as_slice())
    }
}

/// Java's legacy short IDs, resolved to region names or fixed offsets.
pub(crate) fn legacy_time_zone(name: &str) -> Option<&'static str> {
    Some(match name {
        "ACT" => "Australia/Darwin",
        "AET" => "Australia/Sydney",
        "AGT" => "America/Argentina/Buenos_Aires",
        "ART" => "Africa/Cairo",
        "AST" => "America/Anchorage",
        "BET" => "America/Sao_Paulo",
        "BST" => "Asia/Dhaka",
        "CAT" => "Africa/Harare",
        "CNT" => "America/St_Johns",
        "CST" => "America/Chicago",
        "CTT" => "Asia/Shanghai",
        "EAT" => "Africa/Addis_Ababa",
        "ECT" => "Europe/Paris",
        "EST" => "-05:00",
        "HST" => "-10:00",
        "IET" => "America/Indianapolis",
        "IST" => "Asia/Calcutta",
        "JST" => "Asia/Tokyo",
        "MIT" => "Pacific/Apia",
        "MST" => "-07:00",
        "NET" => "Asia/Yerevan",
        "NST" => "Pacific/Auckland",
        "PLT" => "Asia/Karachi",
        "PNT" => "America/Phoenix",
        "PRT" => "America/Puerto_Rico",
        "PST" => "America/Los_Angeles",
        "SST" => "Pacific/Guadalcanal",
        "VST" => "Asia/Saigon",
        _ => return None,
    })
}

/// A named region or fixed offset used by Spark timezone conversions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolvedTimeZone {
    Region(Tz),
    Fixed(FixedOffset),
}

impl ResolvedTimeZone {
    pub(crate) fn parse(name: &str) -> Option<Self> {
        let name = legacy_time_zone(name).unwrap_or(name);
        if let Ok(region) = name.parse::<Tz>() {
            return Some(Self::Region(region));
        }
        let offset = match fixed_zone_prefix(name).map(|(_, offset)| offset) {
            Some("") => "Z",
            Some(offset) => offset,
            None => name,
        };
        parse_fixed_zone_offset(offset).map(Self::Fixed)
    }

    fn fixed_offset_seconds(self) -> Option<i32> {
        match self {
            Self::Region(Tz::UTC) => Some(0),
            Self::Fixed(offset) => Some(offset.local_minus_utc()),
            Self::Region(_) => None,
        }
    }

    fn local_datetime_at_utc_micros(&self, micros: i64) -> Option<NaiveDateTime> {
        match self {
            Self::Region(zone) => zone
                .timestamp_micros(micros)
                .single()
                .map(|dt| dt.naive_local()),
            Self::Fixed(offset) => offset
                .timestamp_micros(micros)
                .single()
                .map(|dt| dt.naive_local()),
        }
    }

    fn resolve_local_datetime(&self, local: NaiveDateTime) -> Option<DateTime<FixedOffset>> {
        match self {
            Self::Region(zone) => {
                disambiguate_local_datetime(local, zone).map(|dt| dt.fixed_offset())
            }
            Self::Fixed(offset) => local.and_local_timezone(*offset).single(),
        }
    }
}

fn fixed_zone_prefix(name: &str) -> Option<(&str, &str)> {
    ["UTC", "GMT", "UT"]
        .into_iter()
        .find_map(|prefix| name.strip_prefix(prefix).map(|offset| (prefix, offset)))
}

/// Resolve a configured Spark zone to an Arrow-compatible operational label.
pub fn normalize_session_timezone(name: &str) -> Option<Arc<str>> {
    let name: Arc<str> = match ResolvedTimeZone::parse(name)? {
        ResolvedTimeZone::Region(region) => region.name().into(),
        ResolvedTimeZone::Fixed(offset) => offset.to_string().into(),
    };
    name.parse::<ArrowTz>().ok()?;
    Some(name)
}

/// Preserve Java's fixed-zone name when normalization removed a UTC/GMT/UT prefix.
pub fn fixed_zone_display_name(configured: &str, normalized: &str) -> Option<Arc<str>> {
    if !normalized.starts_with(['+', '-']) {
        return None;
    }
    if configured == "Z" {
        return Some("Z".into());
    }
    let (prefix, _) = fixed_zone_prefix(configured)?;
    Some(if normalized == "+00:00" {
        prefix.into()
    } else {
        format!("{prefix}{normalized}").into()
    })
}

fn convert_tz_inner(
    args: &[ArrayRef],
    classic: bool,
    null_short_circuit: bool,
) -> Result<ArrayRef> {
    let parse_tz = |input: Option<&str>| {
        input
            .map(|tz_str_opt| {
                let tz_err = |tz_str| {
                    exec_err!(
                        "[INVALID_TIMEZONE] The timezone: {tz_str:?} is invalid. \
        The timezone must be either a region-based zone ID or a zone offset. \
        Region IDs must have the form 'area/city', such as 'America/Los_Angeles'. \
        Zone offsets must be in the format '(+|-)HH', '(+|-)HH:mm' or '(+|-)HH:mm:ss', \
        e.g '-08' , '+01:00' or '-13:33:33', and must be in the range from -18:00 to +18:00. \
        'Z' and 'UTC' are accepted as synonyms for '+00:00'."
                    )
                };

                match ResolvedTimeZone::parse(tz_str_opt) {
                    Some(tz) => Ok(Some(tz)),
                    None => tz_err(tz_str_opt),
                }
            })
            .transpose()
            .map(|opt| opt.flatten())
    };

    let convert = if classic {
        convert_tz_classic
    } else {
        convert_tz_non_classic
    };

    let from_to_utc_timestamp_func =
        |inputs: (Option<i64>, Option<&str>, Option<&str>)| match inputs {
            (Some(ts_micros), Some(from_tz), Some(to_tz)) => {
                match (parse_tz(Some(from_tz))?, parse_tz(Some(to_tz))?) {
                    (Some(from_tz), Some(to_tz)) => Ok(convert(ts_micros, &from_tz, &to_tz)),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        };

    let eager_from_to_utc_timestamp_func = |inputs: (
        Option<i64>,
        Result<Option<ResolvedTimeZone>>,
        Result<Option<ResolvedTimeZone>>,
    )| match inputs {
        (Some(ts_micros), Ok(Some(from_tz)), Ok(Some(to_tz))) => {
            Ok(convert(ts_micros, &from_tz, &to_tz))
        }
        (_, Err(e), _) | (_, _, Err(e)) => Err(e),
        _ => Ok(None),
    };

    let from_tz_strs_arr = cast::cast(&args[0], &DataType::Utf8)?;
    let to_tz_strs_arr = cast::cast(&args[1], &DataType::Utf8)?;
    let ts_arr = &args[2];

    let results: Int64Array = {
        let (from_tz_strs, to_tz_strs) = match (
            from_tz_strs_arr.as_string_opt::<i32>(),
            to_tz_strs_arr.as_string_opt::<i32>(),
        ) {
            (Some(f), Some(t)) => (f, t),
            _ => {
                return exec_err!(
                    "`convert_timezone` first and second arguments must be string literal or array, received {:?}, {:?}",
                    args[0],
                    args[1]
                );
            }
        };

        let arr_lens = args.iter().map(|a| a.len()).collect::<Vec<_>>();
        let max_len = *arr_lens.iter().max().map_or_else(
            || exec_err!("`convert_timezone`: could not get array lengths max"),
            Ok,
        )?;

        let ts_arr = if ts_arr.len() != max_len && ts_arr.len() == 1 {
            let indices = (0..max_len).map(|_| 0u64).collect::<UInt64Array>();
            take::take(&ts_arr, &indices, None)?
        } else {
            ts_arr.clone()
        };

        let micros_arr = timestamp_to_microseconds(&ts_arr)?;

        if null_short_circuit {
            // Time zones are parsed only after all three row values pass null checks.
            let mut from_tzs = from_tz_strs.iter();
            let mut to_tzs = to_tz_strs.iter();

            match (arr_lens[0] == 1, arr_lens[1] == 1) {
                (true, true) => {
                    let from_tz = from_tzs.next().flatten();
                    let to_tz = to_tzs.next().flatten();
                    if micros_arr.null_count() == micros_arr.len()
                        || from_tz.is_none()
                        || to_tz.is_none()
                    {
                        Ok(Int64Array::new_null(micros_arr.len()))
                    } else {
                        let from_tz = parse_tz(from_tz)?;
                        let to_tz = parse_tz(to_tz)?;
                        Ok(convert_singleton_zones(
                            &micros_arr,
                            from_tz,
                            to_tz,
                            convert,
                        ))
                    }
                }
                (true, false) => {
                    let from_tz = from_tzs.next().flatten();
                    micros_arr
                        .iter()
                        .zip(to_tzs)
                        .map(|(ts, to_tz)| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                        .collect::<Result<Int64Array>>()
                }
                (false, true) => {
                    let to_tz = to_tzs.next().flatten();

                    micros_arr
                        .iter()
                        .zip(from_tzs)
                        .map(|(ts, from_tz)| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                        .collect::<Result<Int64Array>>()
                }
                (false, false) => micros_arr
                    .iter()
                    .zip(from_tzs.zip(to_tzs))
                    .map(|(a, (b, c))| (a, b, c))
                    .map(|(ts, from_tz, to_tz)| from_to_utc_timestamp_func((ts, from_tz, to_tz)))
                    .collect::<Result<Int64Array>>(),
            }
        } else {
            let first = |iter: &mut dyn Iterator<Item = Result<Option<ResolvedTimeZone>>>| {
                iter.next().transpose().map(|opt| opt.flatten())
            };
            let mut from_tzs = from_tz_strs.iter().map(parse_tz);
            let mut to_tzs = to_tz_strs.iter().map(parse_tz);

            match (arr_lens[0] == 1, arr_lens[1] == 1) {
                (true, true) => {
                    let from_tz = first(&mut from_tzs)?;
                    let to_tz = first(&mut to_tzs)?;

                    Ok(convert_singleton_zones(
                        &micros_arr,
                        from_tz,
                        to_tz,
                        convert,
                    ))
                }
                (true, false) => {
                    let from_tz = first(&mut from_tzs)?;
                    micros_arr
                        .iter()
                        .zip(to_tzs)
                        .map(|(ts, to_tz)| {
                            eager_from_to_utc_timestamp_func((ts, Ok(from_tz), to_tz))
                        })
                        .collect::<Result<Int64Array>>()
                }
                (false, true) => {
                    let to_tz = first(&mut to_tzs)?;

                    micros_arr
                        .iter()
                        .zip(from_tzs)
                        .map(|(ts, from_tz)| {
                            eager_from_to_utc_timestamp_func((ts, from_tz, Ok(to_tz)))
                        })
                        .collect::<Result<Int64Array>>()
                }
                (false, false) => micros_arr
                    .iter()
                    .zip(from_tzs.zip(to_tzs))
                    .map(|(a, (b, c))| (a, b, c))
                    .map(|(ts, from_tz, to_tz)| {
                        eager_from_to_utc_timestamp_func((ts, from_tz, to_tz))
                    })
                    .collect::<Result<Int64Array>>(),
            }
        }
    }?;

    let time_unit = match args[2].data_type() {
        DataType::Timestamp(unit, None) => *unit,
        x => return exec_err!("invalid timestamp type for `convert_tz`: {x:?}"),
    };

    microseconds_to_timestamp(results, time_unit)
}

fn convert_singleton_zones(
    timestamps: &Int64Array,
    from_zone: Option<ResolvedTimeZone>,
    to_zone: Option<ResolvedTimeZone>,
    convert_with_calendar: fn(i64, &ResolvedTimeZone, &ResolvedTimeZone) -> Option<i64>,
) -> Int64Array {
    const MICROS_PER_SECOND: i64 = 1_000_000;
    // Chrono fixed offsets are each less than a day. Allow both to be applied.
    const CALENDAR_MARGIN: i64 = 2 * 86_400 * MICROS_PER_SECOND;
    const SAFE_MIN: i64 = DateTime::<Utc>::MIN_UTC.timestamp_micros() + CALENDAR_MARGIN;
    const SAFE_MAX: i64 = DateTime::<Utc>::MAX_UTC.timestamp_micros() - CALENDAR_MARGIN;

    let (Some(from_zone), Some(to_zone)) = (from_zone, to_zone) else {
        return Int64Array::new_null(timestamps.len());
    };
    let convert = |value| convert_with_calendar(value, &from_zone, &to_zone);
    match (
        from_zone.fixed_offset_seconds(),
        to_zone.fixed_offset_seconds(),
    ) {
        (Some(0), Some(0)) if from_zone == to_zone => timestamps.clone(),
        (Some(from), Some(to)) => {
            let offset = i64::from(to - from) * MICROS_PER_SECOND;
            timestamps
                .iter()
                .map(|value| {
                    value.and_then(|value| {
                        if (SAFE_MIN..=SAFE_MAX).contains(&value) {
                            Some(value + offset)
                        } else {
                            convert(value)
                        }
                    })
                })
                .collect()
        }
        _ => timestamps
            .iter()
            .map(|value| value.and_then(convert))
            .collect(),
    }
}

// TODO: Support Spark's full i64 microsecond range for conversions between
// different zones. Chrono cannot represent every Spark timestamp, so this needs
// wide-range zone-offset and DST arithmetic outside Chrono.
fn disambiguate_local_datetime(local: NaiveDateTime, tz: &Tz) -> Option<DateTime<Tz>> {
    // Handle ambiguous or non-existent local date time
    // in the same way as `java.time.ZonedDateTime#atZone`.
    match local.and_local_timezone(*tz) {
        MappedLocalTime::Single(x) => Some(x),
        MappedLocalTime::Ambiguous(earliest, _latest) => Some(earliest),
        MappedLocalTime::None => GapInfo::new(&local, tz).and_then(|gap| {
            if let (Some((start, _)), Some(end)) = (gap.begin, gap.end) {
                end.checked_add_signed(local - start)
            } else {
                None
            }
        }),
    }
}

/// Reference:
///   `org.apache.spark.sql.catalyst.util.DateTimeUtils#convertTimestampNtzToAnotherTz`
fn convert_tz_classic(
    ts_micros: i64,
    from_zone: &ResolvedTimeZone,
    to_zone: &ResolvedTimeZone,
) -> Option<i64> {
    let local = match DateTime::<Utc>::from_timestamp_micros(ts_micros) {
        Some(datetime) => datetime.naive_utc(),
        None if from_zone == to_zone => return Some(ts_micros),
        None => return None,
    };
    let dt = from_zone.resolve_local_datetime(local)?;
    Some(
        to_zone
            .local_datetime_at_utc_micros(dt.timestamp_micros())?
            .and_utc()
            .timestamp_micros(),
    )
}

/// Reference:
///   `org.apache.spark.sql.catalyst.util.SparkDateTimeUtils#convertTz`
fn convert_tz_non_classic(
    ts_micros: i64,
    from_zone: &ResolvedTimeZone,
    to_zone: &ResolvedTimeZone,
) -> Option<i64> {
    let local = match to_zone.local_datetime_at_utc_micros(ts_micros) {
        Some(datetime) => datetime,
        None if from_zone == to_zone => return Some(ts_micros),
        None => return None,
    };
    let dt = from_zone.resolve_local_datetime(local)?;
    Some(dt.timestamp_micros())
}

fn timestamp_to_microseconds(array: &dyn Array) -> Result<Int64Array> {
    let values = cast::cast(array, &DataType::Int64)?;
    let values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Execution("expected Int64 timestamp values".to_string()))?;
    let scaled = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, None) => {
            numeric::mul(values, &Int64Array::new_scalar(1_000_000))?
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            numeric::mul(values, &Int64Array::new_scalar(1_000))?
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => return Ok(values.clone()),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            numeric::div(values, &Int64Array::new_scalar(1_000))?
        }
        _ => {
            return exec_err!(
                "`convert_timezone`: third argument type must coerce to NTZ timestamp, received {:?}",
                array.data_type()
            );
        }
    };
    scaled
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| DataFusionError::Execution("expected Int64 timestamp values".to_string()))
}

fn microseconds_to_timestamp(array: Int64Array, time_unit: TimeUnit) -> Result<ArrayRef> {
    if time_unit == TimeUnit::Microsecond {
        return Ok(cast::cast(&array, &DataType::Timestamp(time_unit, None))?);
    }
    let values = match time_unit {
        TimeUnit::Second => numeric::div(&array, &Int64Array::new_scalar(1_000_000))?,
        TimeUnit::Millisecond => numeric::div(&array, &Int64Array::new_scalar(1_000))?,
        TimeUnit::Microsecond => unreachable!(),
        TimeUnit::Nanosecond => numeric::mul(&array, &Int64Array::new_scalar(1_000))?,
    };
    Ok(cast::cast(&values, &DataType::Timestamp(time_unit, None))?)
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use super::*;

    #[test]
    fn fixed_zones_match_calendar_conversion_at_timestamp_boundaries() {
        let min = DateTime::<Utc>::MIN_UTC.timestamp_micros();
        let max = DateTime::<Utc>::MAX_UTC.timestamp_micros();
        let day = 86_400_000_000;
        let timestamps = [
            None,
            Some(i64::MIN),
            Some(min - 1),
            Some(min),
            Some(min + day),
            Some(min + 2 * day - 1),
            Some(min + 2 * day),
            Some(-1),
            Some(0),
            Some(1),
            Some(max - 2 * day),
            Some(max - 2 * day + 1),
            Some(max - day),
            Some(max),
            Some(max + 1),
            Some(i64::MAX),
        ];
        let zones = [
            "UTC",
            "Z",
            "+05:45",
            "+18:00",
            "-18:00",
            "America/Los_Angeles",
        ]
        .map(|zone| ResolvedTimeZone::parse(zone).unwrap());
        let classic =
            convert_tz_classic as fn(i64, &ResolvedTimeZone, &ResolvedTimeZone) -> Option<i64>;
        for convert in [classic, convert_tz_non_classic] {
            for from in zones {
                for to in zones {
                    for timestamp in timestamps {
                        let actual = catch_unwind(|| {
                            let values = Int64Array::from(vec![timestamp]);
                            convert_singleton_zones(&values, Some(from), Some(to), convert)
                                .iter()
                                .next()
                                .unwrap()
                        });
                        let original =
                            catch_unwind(|| timestamp.and_then(|value| convert(value, &from, &to)));
                        // Some Chrono offsets panic at its range limits. Compare that outcome too.
                        assert_eq!(
                            actual.ok(),
                            original.ok(),
                            "{timestamp:?} {from:?} to {to:?}"
                        );
                    }
                }
            }
        }
    }
}
