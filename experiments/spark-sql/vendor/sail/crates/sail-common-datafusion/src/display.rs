// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
/// [Credit]: <https://github.com/apache/arrow-rs/blob/master/arrow-cast/src/display.rs>
use std::fmt::Write;
use std::ops::Range;

use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{
    ArrowDictionaryKeyType, ArrowNativeType, DataType, Date32Type, Date64Type, Decimal32Type,
    Decimal64Type, Decimal128Type, Decimal256Type, DecimalType, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float16Type, Float32Type,
    Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalYearMonthType, RunEndIndexType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type, UnionMode,
};
use datafusion::arrow::error::ArrowError;
use lexical_core::FormattedSize;
use parquet_variant_compute::VariantArray;
use parquet_variant_json::VariantToJson;

use crate::formatter::{
    BinaryFormatter, Date32Formatter, Date64Formatter, DurationMicrosecondFormatter,
    DurationMillisecondFormatter, DurationNanosecondFormatter, DurationSecondFormatter,
    IntervalDayTimeFormatter, IntervalMonthDayNanoFormatter, IntervalYearMonthFormatter,
    Time32MillisecondFormatter, Time32SecondFormatter, Time64MicrosecondFormatter,
    Time64NanosecondFormatter, TimestampMicrosecondFormatter, TimestampMillisecondFormatter,
    TimestampNanosecondFormatter, TimestampSecondFormatter,
};
use crate::variant::is_marked_variant_storage_type;

/// Writes one array value using Spark SQL formatting.
pub struct ValueFormatter<'a> {
    idx: usize,
    formatter: &'a ArrayFormatter<'a>,
}

impl ValueFormatter<'_> {
    /// Writes this value to the provided [`Write`]
    ///
    /// Returns an error on a formatting issue.
    pub fn write(&self, s: &mut dyn Write) -> Result<(), ArrowError> {
        match self.formatter.format.write(self.idx, s) {
            Ok(_) => Ok(()),
            Err(FormatError::Arrow(e)) => Err(e),
            Err(FormatError::Format(_)) => Err(ArrowError::CastError("format error".to_string())),
        }
    }

    /// Fallibly converts this to a string
    pub fn try_to_string(&self) -> Result<String, ArrowError> {
        let mut s = String::new();
        self.write(&mut s)?;
        Ok(s)
    }
}

/// Formats array values for Spark SQL string casts and pivot column names.
pub struct ArrayFormatter<'a> {
    format: Box<dyn DisplayIndex + 'a>,
}

impl<'a> ArrayFormatter<'a> {
    /// Returns an [`ArrayFormatter`] that can be used to format `array`
    ///
    /// This returns an error if an array of the given data type cannot be formatted
    pub fn try_new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        Ok(Self {
            format: make_formatter(array)?,
        })
    }

    /// Returns a writer for the value of the array at `idx`.
    pub fn value(&self, idx: usize) -> ValueFormatter<'_> {
        ValueFormatter {
            formatter: self,
            idx,
        }
    }
}

fn make_formatter<'a>(array: &'a dyn Array) -> Result<Box<dyn DisplayIndex + 'a>, ArrowError> {
    downcast_primitive_array! {
        array => array_format(array),
        DataType::Null => array_format(as_null_array(array)),
        DataType::Boolean => array_format(as_boolean_array(array)),
        DataType::Utf8 => array_format(array.as_string::<i32>()),
        DataType::LargeUtf8 => array_format(array.as_string::<i64>()),
        DataType::Utf8View => array_format(array.as_string_view()),
        DataType::Binary => array_format(array.as_binary::<i32>()),
        DataType::BinaryView => array_format(array.as_binary_view()),
        DataType::LargeBinary => array_format(array.as_binary::<i64>()),
        DataType::FixedSizeBinary(_) => {
            let a = array.as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| {
                ArrowError::CastError(
                    "expected FixedSizeBinaryArray in make_formatter".to_string(),
                )
            })?;
            array_format(a)
        }
        DataType::Dictionary(_, _) => downcast_dictionary_array! {
            array => array_format(array),
            _ => unreachable!()
        }
        DataType::List(_) => array_format(as_generic_list_array::<i32>(array)),
        DataType::LargeList(_) => array_format(as_generic_list_array::<i64>(array)),
        DataType::FixedSizeList(_, _) => {
            let a = array.as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| {
                ArrowError::CastError(
                    "expected FixedSizeListArray in make_formatter".to_string(),
                )
            })?;
            array_format(a)
        }
        DataType::Struct(_) => {
            let struct_array = as_struct_array(array);

            // Check if this is a Variant using Spark's Arrow child-field marker.
            // TODO: Ideally we would check ARROW:extension:name metadata on the parent Field,
            // but make_formatter only receives an &dyn Array (no parent field metadata).
            // Plumbing field metadata through the formatter API is a larger refactor.
            let is_variant = is_marked_variant_storage_type(struct_array.data_type())
                && VariantArray::try_new(struct_array).is_ok();

            if is_variant {
                array_format_variant(struct_array)
            } else {
                array_format(struct_array)
            }
        }
        DataType::Map(_, _) => array_format(as_map_array(array)),
        DataType::Union(_, _) => array_format(as_union_array(array)),
        DataType::RunEndEncoded(_, _) => downcast_run_array! {
            array => array_format(array),
            _ => unreachable!()
        },
        d => Err(ArrowError::NotYetImplemented(format!("formatting {d} is not yet supported"))),
    }
}

/// Either an [`ArrowError`] or [`std::fmt::Error`]
enum FormatError {
    Format(std::fmt::Error),
    Arrow(ArrowError),
}

type FormatResult = Result<(), FormatError>;

impl From<std::fmt::Error> for FormatError {
    fn from(value: std::fmt::Error) -> Self {
        Self::Format(value)
    }
}

impl From<ArrowError> for FormatError {
    fn from(value: ArrowError) -> Self {
        Self::Arrow(value)
    }
}

/// Writes one array value by index.
trait DisplayIndex {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult;
}

/// [`DisplayIndex`] with additional state
trait DisplayIndexState<'a> {
    type State;

    fn prepare(&self) -> Result<Self::State, ArrowError>;

    fn write(&self, state: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult;
}

impl<'a, T: DisplayIndex> DisplayIndexState<'a> for T {
    type State = ();

    fn prepare(&self) -> Result<Self::State, ArrowError> {
        Ok(())
    }

    fn write(&self, _: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        DisplayIndex::write(self, idx, f)
    }
}

struct ArrayFormat<'a, F: DisplayIndexState<'a>> {
    state: F::State,
    array: F,
}

fn array_format<'a, F>(array: F) -> Result<Box<dyn DisplayIndex + 'a>, ArrowError>
where
    F: DisplayIndexState<'a> + Array + 'a,
{
    let state = array.prepare()?;
    Ok(Box::new(ArrayFormat { state, array }))
}

fn array_format_variant<'a>(
    array: &'a StructArray,
) -> Result<Box<dyn DisplayIndex + 'a>, ArrowError> {
    let variant_array = VariantArray::try_new(array)?;
    Ok(Box::new(VariantFormat {
        array: variant_array,
    }))
}

struct VariantFormat {
    array: VariantArray,
}

impl DisplayIndex for VariantFormat {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        if self.array.is_null(idx) {
            f.write_str("NULL")?;
            return Ok(());
        }

        // Try to parse and convert the variant to JSON
        // If it fails (invalid variant data), return an error that will be caught upstream
        let variant = self.array.value(idx);
        let json_str = variant.to_json_string()?;
        write!(f, "{}", json_str)?;
        Ok(())
    }
}

impl<'a, F: DisplayIndexState<'a> + Array> DisplayIndex for ArrayFormat<'a, F> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        if self.array.is_null(idx) {
            f.write_str("NULL")?;
            return Ok(());
        }
        DisplayIndexState::write(&self.array, &self.state, idx, f)
    }
}

impl DisplayIndex for &BooleanArray {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", self.value(idx))?;
        Ok(())
    }
}

impl DisplayIndex for &NullArray {
    fn write(&self, _idx: usize, f: &mut dyn Write) -> FormatResult {
        f.write_str("NULL")?;
        Ok(())
    }
}

macro_rules! primitive_display {
    ($($t:ty),+) => {
        $(impl<'a> DisplayIndex for &'a PrimitiveArray<$t>
        {
            fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
                let value = self.value(idx);
                let mut buffer = [0u8; <$t as ArrowPrimitiveType>::Native::FORMATTED_SIZE];
                // SAFETY:
                // buffer is T::FORMATTED_SIZE
                let b = lexical_core::write(value, &mut buffer);
                // Lexical core produces valid UTF-8
                let s = unsafe { std::str::from_utf8_unchecked(b) };
                f.write_str(s)?;
                Ok(())
            }
        })+
    };
}

macro_rules! primitive_display_float {
    ($($t:ty),+) => {
        $(impl<'a> DisplayIndex for &'a PrimitiveArray<$t>
        {
            fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
                let value = self.value(idx);
                let mut buffer = ryu::Buffer::new();
                if value.is_infinite() {
                    if !value.is_sign_positive() {
                        f.write_str("-")?;
                    }
                    f.write_str("Infinity")?;
                } else {
                    f.write_str(buffer.format(value))?;
                }
                Ok(())
            }
        })+
    };
}

primitive_display!(Int8Type, Int16Type, Int32Type, Int64Type);
primitive_display!(UInt8Type, UInt16Type, UInt32Type, UInt64Type);
primitive_display_float!(Float32Type, Float64Type);

impl DisplayIndex for &PrimitiveArray<Float16Type> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", self.value(idx))?;
        Ok(())
    }
}

macro_rules! decimal_display {
    ($($t:ty),+) => {
        $(impl<'a> DisplayIndexState<'a> for &'a PrimitiveArray<$t> {
            type State = (u8, i8);

            fn prepare(&self) -> Result<Self::State, ArrowError> {
                Ok((self.precision(), self.scale()))
            }

            fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
                write!(f, "{}", <$t>::format_decimal(self.values()[idx], s.0, s.1))?;
                Ok(())
            }
        })+
    };
}

decimal_display!(Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type);

macro_rules! timestamp_display {
    ($t:ty, $formatter:expr_2021 $(,)?) => {
        impl<'a> DisplayIndexState<'a> for &'a PrimitiveArray<$t> {
            type State = Option<Tz>;

            fn prepare(&self) -> Result<Self::State, ArrowError> {
                match self.data_type() {
                    DataType::Timestamp(_, Some(tz)) => Ok(Some(tz.parse()?)),
                    DataType::Timestamp(_, None) => Ok(None),
                    _ => unreachable!(),
                }
            }

            fn write(&self, tz: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
                write!(f, "{}", $formatter(self.value(idx), tz.as_ref()))?;
                Ok(())
            }
        }
    };
}

timestamp_display!(TimestampSecondType, TimestampSecondFormatter);
timestamp_display!(TimestampMillisecondType, TimestampMillisecondFormatter);
timestamp_display!(TimestampMicrosecondType, TimestampMicrosecondFormatter);
timestamp_display!(TimestampNanosecondType, TimestampNanosecondFormatter);

macro_rules! temporal_display {
    ($t:ty, $formatter:ident) => {
        impl DisplayIndex for &PrimitiveArray<$t> {
            fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
                write!(f, "{}", $formatter(self.value(idx)))?;
                Ok(())
            }
        }
    };
}

temporal_display!(Date32Type, Date32Formatter);
temporal_display!(Date64Type, Date64Formatter);
temporal_display!(Time32SecondType, Time32SecondFormatter);
temporal_display!(Time32MillisecondType, Time32MillisecondFormatter);
temporal_display!(Time64MicrosecondType, Time64MicrosecondFormatter);
temporal_display!(Time64NanosecondType, Time64NanosecondFormatter);
temporal_display!(DurationSecondType, DurationSecondFormatter);
temporal_display!(DurationMillisecondType, DurationMillisecondFormatter);
temporal_display!(DurationMicrosecondType, DurationMicrosecondFormatter);
temporal_display!(DurationNanosecondType, DurationNanosecondFormatter);

impl DisplayIndex for &PrimitiveArray<IntervalYearMonthType> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", IntervalYearMonthFormatter(self.value(idx),))?;
        Ok(())
    }
}

impl DisplayIndex for &PrimitiveArray<IntervalDayTimeType> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", IntervalDayTimeFormatter(self.value(idx)))?;
        Ok(())
    }
}

impl DisplayIndex for &PrimitiveArray<IntervalMonthDayNanoType> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", IntervalMonthDayNanoFormatter(self.value(idx)))?;
        Ok(())
    }
}

impl<O: OffsetSizeTrait> DisplayIndex for &GenericStringArray<O> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", self.value(idx))?;
        Ok(())
    }
}

impl DisplayIndex for &StringViewArray {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", self.value(idx))?;
        Ok(())
    }
}

impl<O: OffsetSizeTrait> DisplayIndex for &GenericBinaryArray<O> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", BinaryFormatter(self.value(idx)))?;
        Ok(())
    }
}

impl DisplayIndex for &BinaryViewArray {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", BinaryFormatter(self.value(idx)))?;
        Ok(())
    }
}

impl DisplayIndex for &FixedSizeBinaryArray {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        write!(f, "{}", BinaryFormatter(self.value(idx)))?;
        Ok(())
    }
}

impl<'a, K: ArrowDictionaryKeyType> DisplayIndexState<'a> for &'a DictionaryArray<K> {
    type State = Box<dyn DisplayIndex + 'a>;

    fn prepare(&self) -> Result<Self::State, ArrowError> {
        make_formatter(self.values().as_ref())
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let value_idx = self.keys().values()[idx].as_usize();
        s.as_ref().write(value_idx, f)
    }
}

impl<'a, K: RunEndIndexType> DisplayIndexState<'a> for &'a RunArray<K> {
    type State = Box<dyn DisplayIndex + 'a>;

    fn prepare(&self) -> Result<Self::State, ArrowError> {
        make_formatter(self.values().as_ref())
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let value_idx = self.get_physical_index(idx);
        s.as_ref().write(value_idx, f)
    }
}

fn write_list(
    f: &mut dyn Write,
    mut range: Range<usize>,
    values: &dyn DisplayIndex,
) -> FormatResult {
    f.write_char('[')?;
    if let Some(idx) = range.next() {
        values.write(idx, f)?;
    }
    for idx in range {
        write!(f, ", ")?;
        values.write(idx, f)?;
    }
    f.write_char(']')?;
    Ok(())
}

impl<'a, O: OffsetSizeTrait> DisplayIndexState<'a> for &'a GenericListArray<O> {
    type State = Box<dyn DisplayIndex + 'a>;

    fn prepare(&self) -> Result<Self::State, ArrowError> {
        make_formatter(self.values().as_ref())
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let offsets = self.value_offsets();
        let end = offsets[idx + 1].as_usize();
        let start = offsets[idx].as_usize();
        write_list(f, start..end, s.as_ref())
    }
}

impl<'a> DisplayIndexState<'a> for &'a FixedSizeListArray {
    type State = (usize, Box<dyn DisplayIndex + 'a>);

    fn prepare(&self) -> Result<Self::State, ArrowError> {
        let values = make_formatter(self.values().as_ref())?;
        let length = self.value_length();
        Ok((length as usize, values))
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let start = idx * s.0;
        let end = start + s.0;
        write_list(f, start..end, s.1.as_ref())
    }
}

/// Pairs a boxed [`DisplayIndex`] with its field name
type FieldDisplay<'a> = (&'a str, Box<dyn DisplayIndex + 'a>);

impl<'a> DisplayIndexState<'a> for &'a StructArray {
    type State = Vec<FieldDisplay<'a>>;

    fn prepare(&self) -> Result<Self::State, ArrowError> {
        let fields = match (*self).data_type() {
            DataType::Struct(f) => f,
            _ => unreachable!(),
        };

        self.columns()
            .iter()
            .zip(fields)
            .map(|(a, f)| {
                let format = make_formatter(a.as_ref())?;
                Ok((f.name().as_str(), format))
            })
            .collect()
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let mut iter = s.iter();
        f.write_char('{')?;
        if let Some((_name, display)) = iter.next() {
            display.as_ref().write(idx, f)?;
        }
        for (_name, display) in iter {
            write!(f, ", ")?;
            display.as_ref().write(idx, f)?;
        }
        f.write_char('}')?;
        Ok(())
    }
}

impl<'a> DisplayIndexState<'a> for &'a MapArray {
    type State = (Box<dyn DisplayIndex + 'a>, Box<dyn DisplayIndex + 'a>);

    fn prepare(&self) -> Result<Self::State, ArrowError> {
        let keys = make_formatter(self.keys().as_ref())?;
        let values = make_formatter(self.values().as_ref())?;
        Ok((keys, values))
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let offsets = self.value_offsets();
        let end = offsets[idx + 1].as_usize();
        let start = offsets[idx].as_usize();
        let mut iter = start..end;

        f.write_char('{')?;
        if let Some(idx) = iter.next() {
            s.0.write(idx, f)?;
            write!(f, " -> ")?;
            s.1.write(idx, f)?;
        }

        for idx in iter {
            write!(f, ", ")?;
            s.0.write(idx, f)?;
            write!(f, " -> ")?;
            s.1.write(idx, f)?;
        }
        f.write_char('}')?;
        Ok(())
    }
}

impl<'a> DisplayIndexState<'a> for &'a UnionArray {
    type State = (
        Vec<Option<(&'a str, Box<dyn DisplayIndex + 'a>)>>,
        UnionMode,
    );

    fn prepare(&self) -> Result<Self::State, ArrowError> {
        let (fields, mode) = match (*self).data_type() {
            DataType::Union(fields, mode) => (fields, mode),
            _ => unreachable!(),
        };

        let max_id = fields.iter().map(|(id, _)| id).max().unwrap_or_default() as usize;
        let mut out: Vec<Option<FieldDisplay>> = (0..max_id + 1).map(|_| None).collect();
        for (i, field) in fields.iter() {
            let formatter = make_formatter(self.child(i).as_ref())?;
            out[i as usize] = Some((field.name().as_str(), formatter))
        }
        Ok((out, *mode))
    }

    fn write(&self, s: &Self::State, idx: usize, f: &mut dyn Write) -> FormatResult {
        let id = self.type_id(idx);
        let idx = match s.1 {
            UnionMode::Dense => self.value_offset(idx),
            UnionMode::Sparse => idx,
        };
        let (name, field) = s.0[id as usize].as_ref().ok_or_else(|| {
            ArrowError::CastError(format!(
                "Union type id {id} not found in array with {} fields",
                s.0.len()
            ))
        })?;

        write!(f, "{{{name}=")?;
        field.write(idx, f)?;
        f.write_char('}')?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::builder::StringRunBuilder;
    use datafusion::arrow::datatypes::{Fields, Int32Type};

    use super::*;

    fn array_value_to_string(column: &dyn Array, row: usize) -> Result<String, ArrowError> {
        ArrayFormatter::try_new(column)?.value(row).try_to_string()
    }

    #[test]
    fn spark_display_keeps_values_and_errors() -> Result<(), ArrowError> {
        let cases: Vec<(Box<dyn Array>, &str)> = vec![
            (Box::new(Date32Array::from(vec![0])), "1970-01-01"),
            (Box::new(Date64Array::from(vec![0])), "1970-01-01"),
            (Box::new(Time32SecondArray::from(vec![1])), "00:00:01"),
            (
                Box::new(Time32MillisecondArray::from(vec![1])),
                "00:00:00.001",
            ),
            (
                Box::new(Time64MicrosecondArray::from(vec![1])),
                "00:00:00.000001",
            ),
            (
                Box::new(Time64NanosecondArray::from(vec![1])),
                "00:00:00.000000001",
            ),
            (
                Box::new(TimestampSecondArray::from(vec![1])),
                "1970-01-01 00:00:01",
            ),
            (
                Box::new(TimestampMillisecondArray::from(vec![1])),
                "1970-01-01 00:00:00.001",
            ),
            (
                Box::new(TimestampMicrosecondArray::from(vec![-1])),
                "1969-12-31 23:59:59.999999",
            ),
            (
                Box::new(TimestampNanosecondArray::from(vec![1])),
                "1970-01-01 00:00:00.000000001",
            ),
            (
                Box::new(TimestampSecondArray::from(vec![0]).with_timezone("+05:30")),
                "1970-01-01 05:30:00",
            ),
            (Box::new(Float64Array::from(vec![1.0])), "1.0"),
            (
                Box::new(Decimal128Array::from(vec![123]).with_precision_and_scale(5, 2)?),
                "1.23",
            ),
            (Box::new(Int32Array::from(vec![None])), "NULL"),
        ];
        for (array, expected) in cases {
            let formatter = ArrayFormatter::try_new(array.as_ref())?;
            assert_eq!(
                formatter.value(0).try_to_string()?,
                expected,
                "{:?}",
                array.data_type()
            );
        }
        let invalid_tz = TimestampSecondArray::from(vec![0]).with_timezone("invalid/timezone");
        assert!(ArrayFormatter::try_new(&invalid_tz).is_err());
        struct FailedWrite;
        impl Write for FailedWrite {
            fn write_str(&mut self, _: &str) -> std::fmt::Result {
                Err(std::fmt::Error)
            }
        }
        let array = Int32Array::from(vec![1]);
        let formatter = ArrayFormatter::try_new(&array)?;
        assert!(formatter.value(0).write(&mut FailedWrite).is_err());
        Ok(())
    }

    #[expect(clippy::unwrap_used)]
    #[test]
    fn test_map_array_to_string() {
        let keys = vec!["a", "b", "c", "d", "e", "f", "g", "h"];
        let values_data = UInt32Array::from(vec![0u32, 10, 20, 30, 40, 50, 60, 70]);

        // Construct a buffer for value offsets, for the nested array:
        //  [[a, b, c], [d, e, f], [g, h]]
        let entry_offsets = [0, 3, 6, 8];

        let map_array =
            MapArray::new_from_strings(keys.clone().into_iter(), &values_data, &entry_offsets)
                .unwrap();
        assert_eq!(
            "{d -> 30, e -> 40, f -> 50}",
            array_value_to_string(&map_array, 1).unwrap()
        );
    }

    #[expect(clippy::unwrap_used)]
    fn format_array(array: &dyn Array) -> Vec<String> {
        let fmt = ArrayFormatter::try_new(array).unwrap();
        (0..array.len())
            .map(|x| fmt.value(x).try_to_string().unwrap())
            .collect()
    }

    #[test]
    fn test_array_value_to_string_duration() {
        let array = DurationNanosecondArray::from(vec![
            1,
            -1,
            1000,
            -1000,
            (45 * 60 * 60 * 24 + 14 * 60 * 60 + 2 * 60 + 34) * 1_000_000_000 + 123456789,
            -(45 * 60 * 60 * 24 + 14 * 60 * 60 + 2 * 60 + 34) * 1_000_000_000 - 123456789,
        ]);
        let spark = format_array(&array);

        assert_eq!(spark[0], "INTERVAL '0 00:00:00.000000001' DAY TO SECOND");
        assert_eq!(spark[1], "INTERVAL '-0 00:00:00.000000001' DAY TO SECOND");
        assert_eq!(spark[2], "INTERVAL '0 00:00:00.000001' DAY TO SECOND");
        assert_eq!(spark[3], "INTERVAL '-0 00:00:00.000001' DAY TO SECOND");
        assert_eq!(spark[4], "INTERVAL '45 14:02:34.123456789' DAY TO SECOND");
        assert_eq!(spark[5], "INTERVAL '-45 14:02:34.123456789' DAY TO SECOND");

        let array = DurationMicrosecondArray::from(vec![
            1,
            -1,
            1000,
            -1000,
            (45 * 60 * 60 * 24 + 14 * 60 * 60 + 2 * 60 + 34) * 1_000_000 + 123456,
            -(45 * 60 * 60 * 24 + 14 * 60 * 60 + 2 * 60 + 34) * 1_000_000 - 123456,
        ]);
        let spark = format_array(&array);

        assert_eq!(spark[0], "INTERVAL '0 00:00:00.000001' DAY TO SECOND");
        assert_eq!(spark[1], "INTERVAL '-0 00:00:00.000001' DAY TO SECOND");
        assert_eq!(spark[2], "INTERVAL '0 00:00:00.001' DAY TO SECOND");
        assert_eq!(spark[3], "INTERVAL '-0 00:00:00.001' DAY TO SECOND");
        assert_eq!(spark[4], "INTERVAL '45 14:02:34.123456' DAY TO SECOND");
        assert_eq!(spark[5], "INTERVAL '-45 14:02:34.123456' DAY TO SECOND");

        let array = DurationMillisecondArray::from(vec![
            1,
            -1,
            1000,
            -1000,
            (45 * 60 * 60 * 24 + 14 * 60 * 60 + 2 * 60 + 34) * 1_000 + 123,
            -(45 * 60 * 60 * 24 + 14 * 60 * 60 + 2 * 60 + 34) * 1_000 - 123,
        ]);
        let spark = format_array(&array);

        assert_eq!(spark[0], "INTERVAL '0 00:00:00.001' DAY TO SECOND");
        assert_eq!(spark[1], "INTERVAL '-0 00:00:00.001' DAY TO SECOND");
        assert_eq!(spark[2], "INTERVAL '0 00:00:01' DAY TO SECOND");
        assert_eq!(spark[3], "INTERVAL '-0 00:00:01' DAY TO SECOND");
        assert_eq!(spark[4], "INTERVAL '45 14:02:34.123' DAY TO SECOND");
        assert_eq!(spark[5], "INTERVAL '-45 14:02:34.123' DAY TO SECOND");

        let array = DurationSecondArray::from(vec![
            1,
            -1,
            1000,
            -1000,
            45 * 60 * 60 * 24 + 14 * 60 * 60 + 2 * 60 + 34,
            -45 * 60 * 60 * 24 - 14 * 60 * 60 - 2 * 60 - 34,
        ]);
        let spark = format_array(&array);

        assert_eq!(spark[0], "INTERVAL '0 00:00:01' DAY TO SECOND");
        assert_eq!(spark[1], "INTERVAL '-0 00:00:01' DAY TO SECOND");
        assert_eq!(spark[2], "INTERVAL '0 00:16:40' DAY TO SECOND");
        assert_eq!(spark[3], "INTERVAL '-0 00:16:40' DAY TO SECOND");
        assert_eq!(spark[4], "INTERVAL '45 14:02:34' DAY TO SECOND");
        assert_eq!(spark[5], "INTERVAL '-45 14:02:34' DAY TO SECOND");
    }

    #[test]
    fn test_null() {
        let array = NullArray::new(2);
        let formatted = format_array(&array);
        assert_eq!(formatted, &["NULL".to_string(), "NULL".to_string()])
    }

    #[expect(clippy::unwrap_used)]
    #[test]
    fn test_string_run_arry_to_string() {
        let mut builder = StringRunBuilder::<Int32Type>::new();

        builder.append_value("input_value");
        builder.append_value("input_value");
        builder.append_value("input_value");
        builder.append_value("input_value1");

        let map_array = builder.finish();
        assert_eq!("input_value", array_value_to_string(&map_array, 1).unwrap());
        assert_eq!(
            "input_value1",
            array_value_to_string(&map_array, 3).unwrap()
        );
    }

    #[expect(clippy::unwrap_used)]
    #[test]
    fn test_variant_array_to_string() {
        use parquet_variant_compute::VariantArrayBuilder;
        use parquet_variant_json::JsonToVariant;

        let mut builder = VariantArrayBuilder::new(3);
        builder.append_json(r#"{"name":"norm","age":30}"#).unwrap();
        builder.append_json(r#"[1,2,3]"#).unwrap();
        builder.append_null();

        let variant_array = builder.build();
        let struct_array: StructArray = variant_array.into();
        let fields = struct_array
            .fields()
            .iter()
            .map(|field| {
                if field.name() == "metadata" {
                    crate::variant::variant_metadata_field(
                        field.data_type().clone(),
                        field.is_nullable(),
                    )
                } else {
                    field.as_ref().clone()
                }
            })
            .collect::<Vec<_>>();
        let struct_array = StructArray::new(
            Fields::from(fields),
            struct_array.columns().to_vec(),
            struct_array.nulls().cloned(),
        );

        // Test object - parse JSON to compare since key order is not guaranteed
        let result = array_value_to_string(&struct_array, 0).unwrap();
        assert!(result.contains("\"name\""));
        assert!(result.contains("\"norm\""));
        assert!(result.contains("\"age\""));
        assert!(result.contains("30"));

        // Test array
        assert_eq!("[1,2,3]", array_value_to_string(&struct_array, 1).unwrap());

        // Test null
        assert_eq!("NULL", array_value_to_string(&struct_array, 2).unwrap());
    }
}
