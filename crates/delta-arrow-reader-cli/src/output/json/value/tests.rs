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
