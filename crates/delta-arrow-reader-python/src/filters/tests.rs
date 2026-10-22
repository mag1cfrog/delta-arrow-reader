use super::*;
use pyo3::{PyTypeInfo, types::PyByteArray};

fn assert_error<E: PyTypeInfo>(data_type: &DataType, value: &Bound<'_, PyAny>) {
    let py = value.py();
    let error = to_scalar(data_type, value).unwrap_err();
    assert!(error.get_type(py).is(py.get_type::<E>()), "{error}");
    assert!(!error.to_string().contains("secret"), "{error}");
    assert!(error.cause(py).is_none());
    assert!(error.context(py).is_none());
}

#[test]
fn boolean_and_integer_conversion_checks_type_and_width() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        for value in [false, true] {
            assert_eq!(
                to_scalar(&DataType::Boolean, PyBool::new(py, value).as_any())?,
                DeltaScalar::Boolean(value),
            );
        }
        for (data_type, bits) in [
            (DataType::Int8, 8),
            (DataType::Int16, 16),
            (DataType::Int32, 32),
            (DataType::Int64, 64),
        ] {
            let max = (1_i128 << (bits - 1)) - 1;
            for value in [-max - 1, -1, 0, 1, max] {
                let expected = match data_type {
                    DataType::Int8 => DeltaScalar::Int8(value.try_into().unwrap()),
                    DataType::Int16 => DeltaScalar::Int16(value.try_into().unwrap()),
                    DataType::Int32 => DeltaScalar::Int32(value.try_into().unwrap()),
                    _ => DeltaScalar::Int64(value.try_into().unwrap()),
                };
                assert_eq!(
                    to_scalar(&data_type, PyInt::new(py, value).as_any())?,
                    expected
                );
            }
            for value in [-max - 2, max + 1, -(1_i128 << 100), 1_i128 << 100] {
                assert_error::<PyOverflowError>(&data_type, PyInt::new(py, value).as_any());
            }
            for value in [false, true] {
                assert_error::<PyTypeError>(&data_type, PyBool::new(py, value).as_any());
            }
        }
        for value in [-1, 0, 1] {
            assert_error::<PyTypeError>(&DataType::Boolean, PyInt::new(py, value).as_any());
        }
        Ok(())
    })
}

#[test]
fn floats_preserve_rounding_subnormals_and_signed_zero() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let max32 = f64::from(f32::MAX);
        let min32 = f64::from(f32::from_bits(1));
        for (input, expected) in [
            (-max32, -f32::MAX),
            (max32, f32::MAX),
            (-1.5, -1.5),
            (1.5, 1.5),
            (-0.0, -0.0),
            (0.0, 0.0),
            (0.1, 0.1),
            (min32, f32::from_bits(1)),
            (-max32.next_up(), -f32::MAX),
            (max32.next_up(), f32::MAX),
            (-min32 / 2.0, -0.0),
            (min32 / 2.0, 0.0),
        ] {
            let DeltaScalar::Float32(actual) =
                to_scalar(&DataType::Float32, PyFloat::new(py, input).as_any())?
            else {
                panic!("expected Float32");
            };
            assert_eq!(actual.to_bits(), expected.to_bits());
        }
        for input in [
            -f64::MAX,
            f64::MAX,
            -1.5,
            1.5,
            -0.0,
            0.0,
            0.1,
            f64::from_bits(1),
        ] {
            let DeltaScalar::Float64(actual) =
                to_scalar(&DataType::Float64, PyFloat::new(py, input).as_any())?
            else {
                panic!("expected Float64");
            };
            assert_eq!(actual.to_bits(), input.to_bits());
        }
        for data_type in [DataType::Float32, DataType::Float64] {
            for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
                assert_error::<PyValueError>(&data_type, PyFloat::new(py, value).as_any());
            }
            assert_error::<PyTypeError>(&data_type, PyInt::new(py, 1).as_any());
            assert_error::<PyTypeError>(&data_type, PyBool::new(py, true).as_any());
        }
        for value in [2_f64.powi(128), f64::MAX, 2_f64.powi(128) - 2_f64.powi(103)] {
            for input in [value, -value] {
                assert_error::<PyValueError>(&DataType::Float32, PyFloat::new(py, input).as_any());
            }
        }
        Ok(())
    })
}

#[test]
fn text_and_bytes_are_preserved_without_coercion() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        for text in ["", "a\0b", "caf\u{e9}", "cafe\u{301}", "\u{1f642}"] {
            assert_eq!(
                to_scalar(&DataType::Utf8, PyString::new(py, text).as_any())?,
                DeltaScalar::Utf8(text.into()),
            );
        }
        for bytes in [b"".as_slice(), b"a\0b", b"\xff", b"\0", b"\xc3\xa9"] {
            assert_eq!(
                to_scalar(&DataType::Binary, PyBytes::new(py, bytes).as_any())?,
                DeltaScalar::Binary(bytes.to_vec()),
            );
        }
        for codepoint in [0xd800, 0xdfff] {
            let surrogate = py.import("builtins")?.getattr("chr")?.call1((codepoint,))?;
            let value = surrogate.add("secret")?;
            assert_error::<PyValueError>(&DataType::Utf8, &value);
        }
        let bytes = PyBytes::new(py, b"secret");
        let view = py
            .import("builtins")?
            .getattr("memoryview")?
            .call1((&bytes,))?;
        for data_type in [DataType::Utf8, DataType::Binary] {
            assert_error::<PyTypeError>(&data_type, PyByteArray::new(py, b"secret").as_any());
            assert_error::<PyTypeError>(&data_type, &view);
        }
        assert_error::<PyTypeError>(&DataType::Utf8, bytes.as_any());
        assert_error::<PyTypeError>(&DataType::Binary, PyString::new(py, "secret").as_any());
        Ok(())
    })
}

#[test]
fn decimals_preserve_exact_values_at_precision_and_scale_boundaries() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let decimal = py.import("decimal")?.getattr("Decimal")?;
        let wide = 10_i128.pow(38) - 1;
        for (text, precision, scale, unscaled) in [
            ("-999.99".into(), 5, 2, -99999),
            ("999.99".into(), 5, 2, 99999),
            ("-99999".into(), 5, 0, -99999),
            ("99999".into(), 5, 0, 99999),
            ("1.2300E2".into(), 5, 0, 123),
            (format!("1.23{}", "0".repeat(200)), 5, 2, 123),
            (wide.to_string(), 38, 0, wide),
            ((-wide).to_string(), 38, 0, -wide),
            (format!("{wide}{}E-200", "0".repeat(200)), 38, 0, wide),
            (format!("0.{wide}"), 38, 38, wide),
            (format!("-0.{wide}"), 38, 38, -wide),
            ("1E-38".into(), 38, 38, 1),
        ] {
            assert_eq!(
                to_scalar(
                    &DataType::Decimal128(precision, scale),
                    &decimal.call1((text,))?
                )?,
                DeltaScalar::Decimal128 {
                    value: unscaled,
                    precision,
                    scale
                },
            );
        }
        for (precision, scale, invalid) in [
            (5, 2, vec!["1000".into(), "-1000".into(), "1.234".into()]),
            (5, 0, vec!["100000".into(), "1.1".into()]),
            (
                38,
                0,
                vec!["1E38".into(), i128::MIN.to_string(), i128::MAX.to_string()],
            ),
            (38, 38, vec!["1".into(), "1E-39".into()]),
        ] {
            let data_type = DataType::Decimal128(precision, scale);
            for text in ["0", "-0E-999999999999999999", "0E+999999999999999999"] {
                assert_eq!(
                    to_scalar(&data_type, &decimal.call1((text,))?)?,
                    DeltaScalar::Decimal128 {
                        value: 0,
                        precision,
                        scale
                    }
                );
            }
            for text in invalid.iter().map(String::as_str).chain([
                "NaN123",
                "-sNaN456",
                "Infinity",
                "-Infinity",
                "1E-999999999999999999",
            ]) {
                assert_error::<PyValueError>(&data_type, &decimal.call1((text,))?);
            }
            for text in [
                "1E999999999999999999".into(),
                "-1E999999999999999999".into(),
                (1_u128 << 127).to_string(),
                format!("-{}", (1_u128 << 127) + 1),
                "9".repeat(200),
            ] {
                assert_error::<PyOverflowError>(&data_type, &decimal.call1((text,))?);
            }
            assert_error::<PyTypeError>(&data_type, PyFloat::new(py, 1.23).as_any());
        }
        Ok(())
    })
}

#[test]
fn dates_preserve_calendar_days_and_exclude_datetimes() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let datetime = py.import("datetime")?;
        let date = datetime.getattr("date")?;
        for (fields, days) in [
            ((1, 1, 1), -719162),
            ((1900, 3, 1), -25508),
            ((1969, 12, 31), -1),
            ((1970, 1, 1), 0),
            ((2000, 2, 29), 11016),
            ((9999, 12, 31), 2932896),
        ] {
            assert_eq!(
                to_scalar(&DataType::Date32, &date.call1(fields)?)?,
                DeltaScalar::Date32(days)
            );
        }
        let naive = datetime.getattr("datetime")?.call1((1970, 1, 1))?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("tzinfo", datetime.getattr("timezone")?.getattr("utc")?)?;
        let aware = naive.call_method("replace", (), Some(&kwargs))?;
        for value in [naive, aware] {
            assert_error::<PyTypeError>(&DataType::Date32, &value);
        }
        Ok(())
    })
}

#[test]
fn timestamps_preserve_microseconds_and_utc_offset_at_calendar_boundaries() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let module = py.import("datetime")?;
        let datetime = module.getattr("datetime")?;
        let timezone = module.getattr("timezone")?;
        let timedelta = module.getattr("timedelta")?;
        for (fields, micros) in [
            ((1, 1, 1, 0, 0, 0, 0), -62_135_596_800_000_000_i64),
            ((1969, 12, 31, 23, 59, 58, 999999), -1_000_001),
            ((1969, 12, 31, 23, 59, 59, 999999), -1),
            ((1970, 1, 1, 0, 0, 0, 0), 0),
            ((1970, 1, 1, 0, 0, 0, 1), 1),
            ((2000, 2, 29, 12, 34, 56, 123456), 951_827_696_123_456),
            ((9999, 12, 31, 23, 59, 59, 999999), 253_402_300_799_999_999),
        ] {
            for offset in [
                None,
                Some(0),
                Some(1),
                Some(-1),
                Some(19_800_000_000_i64),
                Some(-25_200_000_000),
                Some(30_000_001),
            ] {
                let value = datetime.call1(fields)?;
                let value = if let Some(offset) = offset {
                    let kwargs = PyDict::new(py);
                    kwargs.set_item(
                        "tzinfo",
                        timezone.call1((timedelta.call1((0, 0, offset))?,))?,
                    )?;
                    value.call_method("replace", (), Some(&kwargs))?
                } else {
                    value
                };
                let field_timezone = offset.map(|_| "UTC".into());
                assert_eq!(
                    to_scalar(
                        &DataType::Timestamp(TimeUnit::Microsecond, field_timezone),
                        &value
                    )?,
                    DeltaScalar::TimestampMicrosecond {
                        value: micros - offset.unwrap_or(0),
                        timezone: offset.map(|_| "UTC".into()),
                    },
                );
                let wrong_timezone = if offset.is_some() {
                    None
                } else {
                    Some("UTC".into())
                };
                assert_error::<PyValueError>(
                    &DataType::Timestamp(TimeUnit::Microsecond, wrong_timezone),
                    &value,
                );
            }
        }
        Ok(())
    })
}

#[test]
fn scalar_conversion_rejects_wrong_types_and_null_comparisons() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let decimal = py.import("decimal")?.getattr("Decimal")?.call1(("1",))?;
        for data_type in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Decimal128(5, 2),
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ] {
            assert_error::<PyValueError>(&data_type, py.None().bind(py));
            assert_error::<PyTypeError>(&data_type, PyList::empty(py).as_any());
            assert_error::<PyTypeError>(&data_type, PyDict::new(py).as_any());
            if data_type != DataType::Boolean {
                assert_error::<PyTypeError>(&data_type, PyBool::new(py, true).as_any());
            }
            if !matches!(
                data_type,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            ) {
                assert_error::<PyTypeError>(&data_type, PyInt::new(py, 1).as_any());
            }
            if !matches!(data_type, DataType::Float32 | DataType::Float64) {
                assert_error::<PyTypeError>(&data_type, PyFloat::new(py, 1.0).as_any());
            }
            if data_type != DataType::Utf8 {
                assert_error::<PyTypeError>(&data_type, PyString::new(py, "secret").as_any());
            }
            if data_type != DataType::Binary {
                assert_error::<PyTypeError>(&data_type, PyBytes::new(py, b"secret").as_any());
            }
            if !matches!(data_type, DataType::Decimal128(..)) {
                assert_error::<PyTypeError>(&data_type, &decimal);
            }
            if data_type != DataType::Date32 {
                let date = py
                    .import("datetime")?
                    .getattr("date")?
                    .call1((1970, 1, 1))?;
                assert_error::<PyTypeError>(&data_type, &date);
            }
        }
        Ok(())
    })
}

#[test]
fn timestamp_conversion_preserves_field_timezone() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let datetime = py.import("datetime")?;
        let offset = datetime.getattr("timedelta")?.call1((0, 3600))?;
        let timezone = datetime.getattr("timezone")?.call1((offset,))?;
        let value = datetime
            .getattr("datetime")?
            .call1((1970, 1, 1, 1, 0, 0, 0, timezone))?;
        // Delta schemas use UTC; exercise other native field metadata here.
        let field_timezone = "America/New_York";
        assert_eq!(
            to_scalar(
                &DataType::Timestamp(TimeUnit::Microsecond, Some(field_timezone.into())),
                &value,
            )?,
            DeltaScalar::TimestampMicrosecond {
                value: 0,
                timezone: Some(field_timezone.into()),
            },
        );
        Ok(())
    })
}

#[test]
fn converts_decimals_with_negative_scales() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let decimal = py.import("decimal")?.getattr("Decimal")?;
        // Delta schemas cannot express negative scales; the native type can.
        for (text, precision, scale, unscaled) in [
            ("12300", 5, -2, 123),
            ("-12300.0000", 5, -2, -123),
            ("9999900", 5, -2, 99999),
            ("-0E-999999999999999999", 5, -2, 0),
            ("1E128", 1, -128, 1),
        ] {
            let value = decimal.call1((text,))?;
            assert_eq!(
                to_scalar(&DataType::Decimal128(precision, scale), &value)?,
                DeltaScalar::Decimal128 {
                    value: unscaled,
                    precision,
                    scale
                },
            );
        }
        for text in ["12301", "-12300.01", "10000000"] {
            let value = decimal.call1((text,))?;
            let error = to_scalar(&DataType::Decimal128(5, -2), &value)
                .expect_err("inexact or oversized decimal must fail");
            assert!(error.is_instance_of::<PyValueError>(py));
        }
        Ok(())
    })
}

#[test]
fn converts_large_and_fixed_width_scalars() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        // Delta schemas expose Utf8/Binary; test the other native variants here.
        let text = PyString::new(py, "a\0\u{1f642}");
        assert_eq!(
            to_scalar(&DataType::LargeUtf8, text.as_any())?,
            DeltaScalar::LargeUtf8("a\0\u{1f642}".into()),
        );
        let bytes = PyBytes::new(py, b"\0\xff");
        assert_eq!(
            to_scalar(&DataType::LargeBinary, bytes.as_any())?,
            DeltaScalar::LargeBinary(b"\0\xff".to_vec()),
        );
        assert_eq!(
            to_scalar(&DataType::FixedSizeBinary(2), bytes.as_any())?,
            DeltaScalar::FixedSizeBinary {
                size: 2,
                value: b"\0\xff".to_vec(),
            },
        );
        for size in [-1, 0, 1, 3] {
            let error = to_scalar(&DataType::FixedSizeBinary(size), bytes.as_any())
                .expect_err("invalid fixed width must fail");
            assert!(error.is_instance_of::<PyValueError>(py));
        }
        for (data_type, value) in [
            (DataType::LargeUtf8, bytes.as_any()),
            (DataType::LargeBinary, text.as_any()),
            (DataType::FixedSizeBinary(2), text.as_any()),
        ] {
            let error = to_scalar(&data_type, value).expect_err("wrong type must fail");
            assert!(error.is_instance_of::<PyTypeError>(py));
        }
        Ok(())
    })
}
