use serde_json::{Value, json};

use super::*;

fn parse(value: Value) -> DeltaPredicate {
    serde_json::from_str::<PredicateInput>(&value.to_string())
        .unwrap()
        .0
}

fn parse_scalar(value: Value) -> DeltaScalar {
    match parse(json!({"op": "eq", "column": "id", "value": value})) {
        DeltaPredicate::Compare { value, .. } => value,
        other => panic!("expected comparison, got {other:?}"),
    }
}

#[test]
fn maps_every_operator_and_preserves_columns() {
    for (op, expected) in [
        ("eq", DeltaComparison::Eq),
        ("ne", DeltaComparison::NotEq),
        ("lt", DeltaComparison::Lt),
        ("le", DeltaComparison::LtEq),
        ("gt", DeltaComparison::Gt),
        ("ge", DeltaComparison::GtEq),
    ] {
        for column in ["id", "", " a,b.c ", "ID"] {
            assert_eq!(
                parse(json!({"op": op, "column": column,
                    "value": {"type": "int32", "value": "10"}})),
                DeltaPredicate::Compare {
                    column: column.into(),
                    op: expected,
                    value: DeltaScalar::Int32(10),
                }
            );
        }
    }
    for value in [false, true] {
        assert_eq!(
            parse(json!({"op": "constant", "value": value})),
            DeltaPredicate::Constant(value)
        );
    }
    assert_eq!(
        parse(json!({"op": "and", "args": []})),
        DeltaPredicate::And(vec![])
    );
    assert_eq!(
        parse(json!({"op": "or", "args": []})),
        DeltaPredicate::Or(vec![])
    );
    assert_eq!(
        parse(json!({"op": "and", "args": [
            {"op": "is_null", "column": " a.b "},
            {"op": "or", "args": [
                {"op": "is_not_null", "column": ""},
                {"op": "not", "arg": {"op": "constant", "value": false}}
            ]}
        ]})),
        DeltaPredicate::And(vec![
            DeltaPredicate::IsNull {
                column: " a.b ".into()
            },
            DeltaPredicate::Or(vec![
                DeltaPredicate::IsNotNull { column: "".into() },
                DeltaPredicate::Not(Box::new(DeltaPredicate::Constant(false))),
            ]),
        ])
    );
}

#[test]
fn maps_every_scalar_without_losing_integer_precision() {
    for (input, expected) in [
        (
            json!({"type": "boolean", "value": false}),
            DeltaScalar::Boolean(false),
        ),
        (
            json!({"type": "int8", "value": "-128"}),
            DeltaScalar::Int8(i8::MIN),
        ),
        (
            json!({"type": "int8", "value": "127"}),
            DeltaScalar::Int8(i8::MAX),
        ),
        (
            json!({"type": "int16", "value": "-32768"}),
            DeltaScalar::Int16(i16::MIN),
        ),
        (
            json!({"type": "int16", "value": "32767"}),
            DeltaScalar::Int16(i16::MAX),
        ),
        (
            json!({"type": "int32", "value": "-2147483648"}),
            DeltaScalar::Int32(i32::MIN),
        ),
        (
            json!({"type": "int32", "value": "2147483647"}),
            DeltaScalar::Int32(i32::MAX),
        ),
        (
            json!({"type": "int64", "value": "-9223372036854775808"}),
            DeltaScalar::Int64(i64::MIN),
        ),
        (
            json!({"type": "int64", "value": "9223372036854775807"}),
            DeltaScalar::Int64(i64::MAX),
        ),
        (
            json!({"type": "int64", "value": "9007199254740993"}),
            DeltaScalar::Int64(9_007_199_254_740_993),
        ),
        (
            json!({"type": "int64", "value": "0"}),
            DeltaScalar::Int64(0),
        ),
        (
            json!({"type": "float32", "value": 0.1}),
            DeltaScalar::Float32(0.1),
        ),
        (
            json!({"type": "float32", "value": f32::MAX as f64}),
            DeltaScalar::Float32(f32::MAX),
        ),
        (
            json!({"type": "float32", "value": 1e-100}),
            DeltaScalar::Float32(0.0),
        ),
        (
            json!({"type": "float64", "value": -1.25e100}),
            DeltaScalar::Float64(-1.25e100),
        ),
        (
            json!({"type": "float64", "value": 10}),
            DeltaScalar::Float64(10.0),
        ),
        (
            json!({"type": "utf8", "value": " a\n\u{0}b "}),
            DeltaScalar::Utf8(" a\n\u{0}b ".into()),
        ),
        (
            json!({"type": "large_utf8", "value": ""}),
            DeltaScalar::LargeUtf8("".into()),
        ),
        (
            json!({"type": "binary", "value": []}),
            DeltaScalar::Binary(vec![]),
        ),
        (
            json!({"type": "large_binary", "value": [0, 255]}),
            DeltaScalar::LargeBinary(vec![0, 255]),
        ),
        (
            json!({"type": "fixed_size_binary", "size": 2, "value": [0, 255]}),
            DeltaScalar::FixedSizeBinary {
                size: 2,
                value: vec![0, 255],
            },
        ),
        (
            json!({"type": "date32", "value": "-1"}),
            DeltaScalar::Date32(-1),
        ),
        (
            json!({"type": "date32", "value": "-2147483648"}),
            DeltaScalar::Date32(i32::MIN),
        ),
        (
            json!({"type": "date32", "value": "2147483647"}),
            DeltaScalar::Date32(i32::MAX),
        ),
        (
            json!({"type": "timestamp_us", "value": "-9223372036854775808", "timezone": null}),
            DeltaScalar::TimestampMicrosecond {
                value: i64::MIN,
                timezone: None,
            },
        ),
        (
            json!({"type": "timestamp_us", "value": "9223372036854775807", "timezone": "UTC"}),
            DeltaScalar::TimestampMicrosecond {
                value: i64::MAX,
                timezone: Some("UTC".into()),
            },
        ),
        (
            json!({"type": "timestamp_us", "value": "-1", "timezone": " Custom/Zone "}),
            DeltaScalar::TimestampMicrosecond {
                value: -1,
                timezone: Some(" Custom/Zone ".into()),
            },
        ),
        (
            json!({"type": "decimal128", "value": "123456789012345678901234567890", "precision": 30, "scale": -2}),
            DeltaScalar::Decimal128 {
                value: 123_456_789_012_345_678_901_234_567_890,
                precision: 30,
                scale: -2,
            },
        ),
        // Precision, scale, and value compatibility belong to the core planner.
        (
            json!({"type": "decimal128", "value": i128::MIN.to_string(), "precision": 0, "scale": 127}),
            DeltaScalar::Decimal128 {
                value: i128::MIN,
                precision: 0,
                scale: 127,
            },
        ),
        (
            json!({"type": "decimal128", "value": i128::MAX.to_string(), "precision": 255, "scale": -128}),
            DeltaScalar::Decimal128 {
                value: i128::MAX,
                precision: 255,
                scale: -128,
            },
        ),
    ] {
        assert_eq!(parse_scalar(input.clone()), expected, "{input}");
    }
}

#[test]
fn preserves_float64_bits() {
    for expected in [
        1.9651349465042103,
        -1.9651349465042103,
        2.3318716180463287e-130,
        -7.386266683291869e223,
        f64::MAX,
        f64::MIN,
        f64::MIN_POSITIVE,
        f64::from_bits(1),
        0.0,
        -0.0,
    ] {
        let actual = parse_scalar(json!({"type": "float64", "value": expected}));
        let DeltaScalar::Float64(actual) = actual else {
            panic!("expected Float64, got {actual:?}");
        };
        assert_eq!(actual.to_bits(), expected.to_bits(), "{expected:?}");
    }
}

#[test]
fn rejects_wrong_shapes_unknown_fields_and_duplicate_keys() {
    for text in [
        "",
        "{}",
        "[]",
        "null",
        "true",
        r#""constant""#,
        "1",
        r#"{"op":"constant","value":true} {}"#,
        r#"["constant",true]"#,
        r#"{"op":"Constant","value":true}"#,
        r#"{"op":"constant","value":true,"extra":0}"#,
        r#"{"op":"constant","value":null}"#,
        r#"{"op":"constant","value":"true"}"#,
        r#"{"op":"constant"}"#,
        r#"{"op":"constant","op":"constant","value":true}"#,
        r#"{"op":"constant","value":true,"\u0076alue":false}"#,
        r#"{"op":"constant","value":true,"value":true}"#,
        r#"{"op":"is_null","column":null}"#,
        r#"{"op":"is_null","column":"id","value":null}"#,
        r#"{"op":"is_null","column":"id","column":"id"}"#,
        r#"{"op":"is_not_null"}"#,
        r#"{"op":"and","args":null}"#,
        r#"{"op":"and","args":{}}"#,
        r#"{"op":"and","args":[null]}"#,
        r#"{"op":"and","args":[["constant",true]]}"#,
        r#"{"op":"and","args":[],"args":[]}"#,
        r#"{"op":"or","args":[{"op":"constant","value":true,"extra":0}]}"#,
        r#"{"op":"not","args":[]}"#,
        r#"{"op":"not","arg":[]}"#,
        r#"{"op":"not","arg":["constant",true]}"#,
        r#"{"op":"not","arg":{"op":"constant","value":true},"arg":{}}"#,
        r#"{"op":"eq","column":"id","value":null}"#,
        r#"{"op":"eq","column":"id","value":["int32","1"]}"#,
        r#"{"op":"eq","column":"id","value":{"type":"int32","value":"1"},"value":null}"#,
    ] {
        assert!(
            serde_json::from_str::<PredicateInput>(text).is_err(),
            "{text}"
        );
    }
    for scalar in [
        "{}",
        "[]",
        "null",
        "true",
        "1",
        r#""int32""#,
        r#"{"type":"int32"}"#,
        r#"{"type":"int32","value":"1","extra":0}"#,
        r#"{"type":"int32","type":"int32","value":"1"}"#,
        r#"{"type":"int32","\u0074ype":"int32","value":"1"}"#,
        r#"{"type":"int32","value":"1","value":"2"}"#,
        r#"{"type":"uint32","value":"1"}"#,
        r#"{"type":"boolean","value":1}"#,
        r#"{"type":"utf8","value":null}"#,
        r#"{"type":"utf8","value":1}"#,
        r#"{"type":"float64","value":"1"}"#,
        r#"{"type":"float32","value":null}"#,
        r#"{"type":"float32","value":3.5e38}"#,
        r#"{"type":"float32","value":-3.5e38}"#,
        r#"{"type":"float64","value":1e400}"#,
        r#"{"type":"float64","value":NaN}"#,
        r#"{"type":"binary","value":"AA=="}"#,
        r#"{"type":"binary","value":[-1]}"#,
        r#"{"type":"binary","value":[256]}"#,
        r#"{"type":"binary","value":[1.0]}"#,
        r#"{"type":"binary","value":[1e0]}"#,
        r#"{"type":"large_binary","value":["1"]}"#,
        r#"{"type":"fixed_size_binary","value":[],"size":0}"#,
        r#"{"type":"fixed_size_binary","value":[1],"size":-1}"#,
        r#"{"type":"fixed_size_binary","value":[1],"size":2}"#,
        r#"{"type":"fixed_size_binary","value":[1],"size":2147483648}"#,
        r#"{"type":"fixed_size_binary","value":[1],"size":1.0}"#,
        r#"{"type":"fixed_size_binary","value":[1],"size":1e0}"#,
        r#"{"type":"fixed_size_binary","value":[1],"size":1,"size":1}"#,
        r#"{"type":"timestamp_us","value":"0"}"#,
        r#"{"type":"timestamp_us","value":"0","timezone":""}"#,
        r#"{"type":"timestamp_us","value":"0","timezone":0}"#,
        r#"{"type":"timestamp_us","value":"0","timezone":null,"timezone":null}"#,
        r#"{"type":"decimal128","value":"1","precision":256,"scale":0}"#,
        r#"{"type":"decimal128","value":"1","precision":-1,"scale":0}"#,
        r#"{"type":"decimal128","value":"1","precision":1.0,"scale":0}"#,
        r#"{"type":"decimal128","value":"1","precision":1e0,"scale":0}"#,
        r#"{"type":"decimal128","value":"1","precision":1,"scale":128}"#,
        r#"{"type":"decimal128","value":"1","precision":1,"scale":-129}"#,
        r#"{"type":"decimal128","value":"1","precision":1,"scale":0.0}"#,
        r#"{"type":"decimal128","value":"1","precision":1,"scale":0e0}"#,
        r#"{"type":"decimal128","value":"1","precision":1,"scale":0,"scale":0}"#,
        r#"{"type":"decimal128","value":"1","precision":1,"precision":1,"scale":0}"#,
    ] {
        let text = format!(r#"{{"op":"eq","column":"id","value":{scalar}}}"#);
        assert!(
            serde_json::from_str::<PredicateInput>(&text).is_err(),
            "{text}"
        );
    }
}

#[test]
fn rejects_noncanonical_integers_and_width_overflow() {
    for (kind, underflow, overflow) in [
        ("int8", "-129", "128"),
        ("int16", "-32769", "32768"),
        ("int32", "-2147483649", "2147483648"),
        ("int64", "-9223372036854775809", "9223372036854775808"),
        ("date32", "-2147483649", "2147483648"),
        (
            "timestamp_us",
            "-9223372036854775809",
            "9223372036854775808",
        ),
        (
            "decimal128",
            "-170141183460469231731687303715884105729",
            "170141183460469231731687303715884105728",
        ),
    ] {
        for value in [
            "", "-", "+1", "-0", "00", "01", "-01", " 1", "1 ", "1\n", "1.0", "1e2", "0x1",
            "\u{661}", underflow, overflow,
        ]
        .map(Value::from)
        .into_iter()
        .chain([
            json!(0),
            json!(1.0),
            json!(true),
            json!(null),
            json!([]),
            json!({}),
        ]) {
            let mut scalar = json!({"type": kind, "value": value});
            if kind == "timestamp_us" {
                scalar["timezone"] = Value::Null;
            }
            if kind == "decimal128" {
                scalar["precision"] = json!(38);
                scalar["scale"] = json!(0);
            }
            let predicate = json!({"op": "eq", "column": "id", "value": scalar});
            assert!(
                serde_json::from_str::<PredicateInput>(&predicate.to_string()).is_err(),
                "{predicate}"
            );
        }
    }
}

#[test]
fn enforces_depth_and_node_limits_including_the_root() {
    let mut value = json!({"op": "constant", "value": true});
    for depth in 1..=33 {
        let result = serde_json::from_str::<PredicateInput>(&value.to_string());
        assert_eq!(result.is_ok(), depth <= 32, "depth {depth}");
        value = match depth % 3 {
            0 => json!({"op": "not", "arg": value}),
            1 => json!({"op": "and", "args": [value]}),
            _ => json!({"op": "or", "args": [value]}),
        };
    }
    for nodes in [1024, 1025] {
        let args = vec![json!({"op": "constant", "value": true}); nodes - 1];
        let value = json!({"op": "and", "args": args});
        assert_eq!(
            serde_json::from_str::<PredicateInput>(&value.to_string()).is_ok(),
            nodes == 1024
        );
    }
    // Count across sibling subtrees, not just within each branch.
    let args = vec![json!({"op": "not", "arg": {"op": "constant", "value": true}}); 512];
    let value = json!({"op": "or", "args": args});
    assert!(serde_json::from_str::<PredicateInput>(&value.to_string()).is_err());
    let deeply_nested = format!(
        "{}{}{}",
        r#"{"op":"not","arg":"#.repeat(256),
        r#"{"op":"constant","value":true}"#,
        "}".repeat(256)
    );
    assert!(serde_json::from_str::<PredicateInput>(&deeply_nested).is_err());
}
