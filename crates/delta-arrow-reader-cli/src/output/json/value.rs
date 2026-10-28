//! Values in a typed JSON row follow the accompanying Arrow schema.
//!
//! For example, Int64 `9007199254740993` becomes `"9007199254740993"`.
//! Structs use field order, so duplicate field names do not overwrite values.

use std::fmt::Display;

use arrow::{
    array::{Array, ArrayRef, ArrowPrimitiveType, AsArray},
    datatypes::*,
    downcast_dictionary_array,
};
use base64::{display::Base64Display, engine::general_purpose::STANDARD};
use serde::{Serialize, Serializer, ser::SerializeSeq};

use crate::diagnostics::Error;

pub(super) fn validate_json_type_support(
    data_type: &DataType,
    command: &'static str,
) -> Result<(), Error> {
    match data_type {
        DataType::Union(..) | DataType::Interval(..) | DataType::RunEndEncoded(..) => {
            Err(Error::UnsupportedJsonType { command })
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => validate_json_type_support(field.data_type(), command),
        DataType::Struct(fields) => fields
            .iter()
            .try_for_each(|field| validate_json_type_support(field.data_type(), command)),
        DataType::Dictionary(_, values) => validate_json_type_support(values, command),
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Timestamp(..)
        | DataType::Duration(_)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Ok(()),
        DataType::Time32(_) | DataType::Time64(_) => Err(Error::UnsupportedJsonType { command }),
    }
}

pub(super) struct JsonRow<'a> {
    pub(super) columns: &'a [ArrayRef],
    pub(super) index: usize,
}

impl Serialize for JsonRow<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut row = serializer.serialize_seq(Some(self.columns.len()))?;
        for column in self.columns {
            row.serialize_element(&JsonCell {
                array: column.as_ref(),
                index: self.index,
            })?;
        }
        row.end()
    }
}

struct JsonArrayValues<'a>(&'a dyn Array);

impl Serialize for JsonArrayValues<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut values = serializer.serialize_seq(Some(self.0.len()))?;
        for index in 0..self.0.len() {
            values.serialize_element(&JsonCell {
                array: self.0,
                index,
            })?;
        }
        values.end()
    }
}

struct JsonCell<'a> {
    array: &'a dyn Array,
    index: usize,
}

impl Serialize for JsonCell<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let Self { array, index } = *self;
        if array.is_null(index) {
            return serializer.serialize_none();
        }
        // Arrow's integer dispatch keeps all signed/unsigned widths exact.
        arrow::array::downcast_integer_array!(array => {
            serializer.collect_str(&array.value(index))
        }
        DataType::Null => serializer.serialize_none(),
        DataType::Boolean => array.as_boolean().value(index).serialize(serializer),
        DataType::Float16 => serialize_float(array.as_primitive::<Float16Type>().value(index).to_f32(), serializer),
        DataType::Float32 => serialize_float(array.as_primitive::<Float32Type>().value(index), serializer),
        DataType::Float64 => serialize_float(array.as_primitive::<Float64Type>().value(index), serializer),
        DataType::Decimal32(..) => self.serialize_integer_string::<Decimal32Type, _>(serializer),
        DataType::Decimal64(..) => self.serialize_integer_string::<Decimal64Type, _>(serializer),
        DataType::Decimal128(..) => self.serialize_integer_string::<Decimal128Type, _>(serializer),
        DataType::Decimal256(..) => self.serialize_integer_string::<Decimal256Type, _>(serializer),
        DataType::Date32 => self.serialize_integer_string::<Date32Type, _>(serializer),
        DataType::Date64 => self.serialize_integer_string::<Date64Type, _>(serializer),
        DataType::Time32(TimeUnit::Second) => self.serialize_integer_string::<Time32SecondType, _>(serializer),
        DataType::Time32(TimeUnit::Millisecond) => self.serialize_integer_string::<Time32MillisecondType, _>(serializer),
        DataType::Time64(TimeUnit::Microsecond) => self.serialize_integer_string::<Time64MicrosecondType, _>(serializer),
        DataType::Time64(TimeUnit::Nanosecond) => self.serialize_integer_string::<Time64NanosecondType, _>(serializer),
        DataType::Timestamp(TimeUnit::Second, _) => self.serialize_integer_string::<TimestampSecondType, _>(serializer),
        DataType::Timestamp(TimeUnit::Millisecond, _) => self.serialize_integer_string::<TimestampMillisecondType, _>(serializer),
        DataType::Timestamp(TimeUnit::Microsecond, _) => self.serialize_integer_string::<TimestampMicrosecondType, _>(serializer),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => self.serialize_integer_string::<TimestampNanosecondType, _>(serializer),
        DataType::Duration(TimeUnit::Second) => self.serialize_integer_string::<DurationSecondType, _>(serializer),
        DataType::Duration(TimeUnit::Millisecond) => self.serialize_integer_string::<DurationMillisecondType, _>(serializer),
        DataType::Duration(TimeUnit::Microsecond) => self.serialize_integer_string::<DurationMicrosecondType, _>(serializer),
        DataType::Duration(TimeUnit::Nanosecond) => self.serialize_integer_string::<DurationNanosecondType, _>(serializer),
        DataType::Utf8 => array.as_string::<i32>().value(index).serialize(serializer),
        DataType::LargeUtf8 => array.as_string::<i64>().value(index).serialize(serializer),
        DataType::Utf8View => array.as_string_view().value(index).serialize(serializer),
        DataType::Binary => serializer.collect_str(&Base64Display::new(array.as_binary::<i32>().value(index), &STANDARD)),
        DataType::LargeBinary => serializer.collect_str(&Base64Display::new(array.as_binary::<i64>().value(index), &STANDARD)),
        DataType::BinaryView => serializer.collect_str(&Base64Display::new(array.as_binary_view().value(index), &STANDARD)),
        DataType::FixedSizeBinary(_) => serializer.collect_str(&Base64Display::new(array.as_fixed_size_binary().value(index), &STANDARD)),
        DataType::List(_) => JsonArrayValues(array.as_list::<i32>().value(index).as_ref()).serialize(serializer),
        DataType::LargeList(_) => JsonArrayValues(array.as_list::<i64>().value(index).as_ref()).serialize(serializer),
        DataType::ListView(_) => JsonArrayValues(array.as_list_view::<i32>().value(index).as_ref()).serialize(serializer),
        DataType::LargeListView(_) => JsonArrayValues(array.as_list_view::<i64>().value(index).as_ref()).serialize(serializer),
        DataType::FixedSizeList(..) => JsonArrayValues(array.as_fixed_size_list().value(index).as_ref()).serialize(serializer),
        DataType::Struct(_) => JsonRow { columns: array.as_struct().columns(), index }.serialize(serializer),
        DataType::Map(..) => JsonArrayValues(&array.as_map().value(index)).serialize(serializer),
        DataType::Dictionary(..) => arrow::array::downcast_dictionary_array!(array => {
            match array.key(index) {
                Some(index) => JsonCell { array: array.values().as_ref(), index }.serialize(serializer),
                None => serializer.serialize_none(),
            }
        }, _ => Err(serde::ser::Error::custom("unsupported_json_type"))),
        _ => Err(serde::ser::Error::custom("unsupported_json_type")))
    }
}

impl JsonCell<'_> {
    fn serialize_integer_string<T: ArrowPrimitiveType, S: Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        T::Native: Display,
    {
        serializer.collect_str(&self.array.as_primitive::<T>().value(self.index))
    }
}

fn serialize_float<T: num_traits::Float + Serialize, S: Serializer>(
    value: T,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    if value.is_nan() {
        serializer.serialize_str("NaN")
    } else if value.is_infinite() {
        serializer.serialize_str(if value.is_sign_negative() {
            "-Infinity"
        } else {
            "Infinity"
        })
    } else if value.is_zero() && value.is_sign_negative() {
        serializer.serialize_str("-0")
    } else {
        value.serialize(serializer)
    }
}

#[cfg(test)]
mod tests;
