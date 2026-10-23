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

use crate::Error;

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
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::Arc;

    use arrow::array::*;
    use serde_json::{Value, json};

    use super::*;

    fn encode_array_to_json(array: &dyn Array) -> Value {
        // Use the actual writer so float formatting and streaming Display match stdout.
        serde_json::from_str(&serde_json::to_string(&JsonArrayValues(array)).unwrap()).unwrap()
    }

    #[test]
    fn numeric_values_keep_precision_units_and_special_floats() {
        let cases: Vec<(ArrayRef, Value)> = vec![
            (Arc::new(Int8Array::from(vec![i8::MIN])), json!(["-128"])),
            (
                Arc::new(Int16Array::from(vec![i16::MIN])),
                json!(["-32768"]),
            ),
            (
                Arc::new(Int32Array::from(vec![i32::MIN])),
                json!(["-2147483648"]),
            ),
            (
                Arc::new(Int64Array::from(vec![
                    Some(i64::MIN),
                    Some(9007199254740993),
                    None,
                ])),
                json!(["-9223372036854775808", "9007199254740993", null]),
            ),
            (Arc::new(UInt8Array::from(vec![u8::MAX])), json!(["255"])),
            (
                Arc::new(UInt16Array::from(vec![u16::MAX])),
                json!(["65535"]),
            ),
            (
                Arc::new(UInt32Array::from(vec![u32::MAX])),
                json!(["4294967295"]),
            ),
            (
                Arc::new(UInt64Array::from(vec![u64::MAX])),
                json!(["18446744073709551615"]),
            ),
            (
                Arc::new(
                    Decimal32Array::from(vec![1234])
                        .with_precision_and_scale(7, 2)
                        .unwrap(),
                ),
                json!(["1234"]),
            ),
            (
                Arc::new(
                    Decimal64Array::from(vec![-1234])
                        .with_precision_and_scale(12, -2)
                        .unwrap(),
                ),
                json!(["-1234"]),
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![123456789012345678901234567890_i128])
                        .with_precision_and_scale(38, 9)
                        .unwrap(),
                ),
                json!(["123456789012345678901234567890"]),
            ),
            (
                Arc::new(
                    Decimal256Array::from(vec![
                        "12345678901234567890123456789012345678901234567890"
                            .parse::<i256>()
                            .unwrap(),
                    ])
                    .with_precision_and_scale(76, 18)
                    .unwrap(),
                ),
                json!(["12345678901234567890123456789012345678901234567890"]),
            ),
            (
                Arc::new(Float32Array::from(vec![0.1, -0.0, f32::NAN])),
                json!([0.1, "-0", "NaN"]),
            ),
            (
                Arc::new(Float64Array::from(vec![
                    f64::MAX,
                    0.0,
                    -0.0,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    f64::NAN,
                ])),
                json!([f64::MAX, 0.0, "-0", "Infinity", "-Infinity", "NaN"]),
            ),
        ];
        for (array, expected) in cases {
            validate_json_type_support(array.data_type(), "query").unwrap();
            assert_eq!(
                encode_array_to_json(array.as_ref()),
                expected,
                "{:?}",
                array.data_type()
            );
        }
        let half =
            arrow::compute::cast(&Float32Array::from(vec![1.5, -0.0]), &DataType::Float16).unwrap();
        assert_eq!(encode_array_to_json(half.as_ref()), json!([1.5, "-0"]));
        let temporal: Vec<ArrayRef> = vec![
            Arc::new(Date32Array::from(vec![-1])),
            Arc::new(Date64Array::from(vec![-1])),
            Arc::new(Time32SecondArray::from(vec![-1])),
            Arc::new(Time32MillisecondArray::from(vec![-1])),
            Arc::new(Time64MicrosecondArray::from(vec![-1])),
            Arc::new(Time64NanosecondArray::from(vec![-1])),
            Arc::new(TimestampSecondArray::from(vec![-1])),
            Arc::new(TimestampMillisecondArray::from(vec![-1])),
            Arc::new(TimestampMicrosecondArray::from(vec![-1]).with_timezone("Asia/Shanghai")),
            Arc::new(TimestampNanosecondArray::from(vec![-1])),
            Arc::new(DurationSecondArray::from(vec![-1])),
            Arc::new(DurationMillisecondArray::from(vec![-1])),
            Arc::new(DurationMicrosecondArray::from(vec![-1])),
            Arc::new(DurationNanosecondArray::from(vec![-1])),
        ];
        for array in temporal {
            assert_eq!(encode_array_to_json(array.as_ref()), json!(["-1"]));
        }
    }

    #[test]
    fn strings_binary_nulls_and_dictionary_values_are_distinct() {
        for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let source = StringArray::from(vec![Some("中文\n\""), Some(""), None]);
            let array = arrow::compute::cast(&source, &data_type).unwrap();
            assert_eq!(
                encode_array_to_json(array.as_ref()),
                json!(["中文\n\"", "", null])
            );
        }
        for data_type in [
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
        ] {
            let source = BinaryArray::from(vec![Some(&[0_u8, 255][..]), Some(&[][..]), None]);
            let array = arrow::compute::cast(&source, &data_type).unwrap();
            assert_eq!(
                encode_array_to_json(array.as_ref()),
                json!(["AP8=", "", null])
            );
        }
        let fixed = FixedSizeBinaryArray::try_from_iter([&[0_u8, 255][..]].into_iter()).unwrap();
        assert_eq!(encode_array_to_json(&fixed), json!(["AP8="]));
        assert_eq!(
            encode_array_to_json(&BooleanArray::from(vec![Some(true), Some(false), None])),
            json!([true, false, null])
        );
        assert_eq!(encode_array_to_json(&NullArray::new(1)), json!([null]));
        let dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(1), Some(0), None, Some(2)]),
            Arc::new(StringArray::from(vec![Some("value"), None, Some("")])),
        )
        .unwrap();
        assert_eq!(
            encode_array_to_json(&dictionary.slice(1, 3)),
            json!(["value", null, ""])
        );
        assert_eq!(
            encode_array_to_json(&dictionary),
            json!([null, "value", null, ""])
        );
    }

    #[test]
    fn nested_values_keep_order_duplicates_and_parent_nulls() {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ];
        let fields = vec![
            Field::new("same", DataType::Int64, false),
            Field::new("same", DataType::Utf8, false),
        ];
        let structure = StructArray::new(fields.into(), columns, Some(vec![true, false].into()));
        assert_eq!(encode_array_to_json(&structure), json!([["1", "a"], null]));

        let mut map = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map.keys().append_value("k");
        map.values().append_value(1);
        map.keys().append_value("k");
        map.values().append_null();
        map.append(true).unwrap();
        map.append(true).unwrap();
        map.append(false).unwrap();
        assert_eq!(
            encode_array_to_json(&map.finish()),
            json!([[["k", "1"], ["k", null]], [], null])
        );

        let list = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(1), None]),
            Some(vec![]),
            None,
        ]);
        let child = Arc::new(Field::new("item", DataType::Int64, true));
        for data_type in [
            DataType::List(child.clone()),
            DataType::LargeList(child.clone()),
            DataType::ListView(child.clone()),
            DataType::LargeListView(child),
        ] {
            let array = arrow::compute::cast(&list, &data_type).unwrap();
            assert_eq!(
                encode_array_to_json(array.as_ref()),
                json!([["1", null], [], null])
            );
        }
        let mut fixed = FixedSizeListBuilder::new(Int64Builder::new(), 2);
        fixed.values().append_value(1);
        fixed.values().append_null();
        fixed.append(true);
        assert_eq!(encode_array_to_json(&fixed.finish()), json!([["1", null]]));
    }
}
