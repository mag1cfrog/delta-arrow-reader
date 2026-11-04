//! Strict Arrow string parsing retains complete rows before its first failure.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int32Array, LargeStringArray, PrimitiveArray, StringArray, StringViewArray,
};
use arrow::compute::kernels::cast::{ParsedStringRows, parse_string_rows_until_error};
use arrow::compute::kernels::cast_utils::Parser;
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{
    ArrowPrimitiveType, Date32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Time32SecondType, UInt32Type,
};
use arrow::error::ArrowError;

fn string_layouts(values: &[Option<&str>]) -> [ArrayRef; 3] {
    [
        Arc::new(StringArray::from(values.to_vec())),
        Arc::new(LargeStringArray::from(values.to_vec())),
        Arc::new(StringViewArray::from(values.to_vec())),
    ]
}

fn assert_rows<P: Parser>(
    input: &[Option<&str>],
    expected: &[Option<P::Native>],
    failure: Option<(usize, &str)>,
) -> Result<(), ArrowError> {
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let expected: PrimitiveArray<P> = expected.iter().copied().collect();
    for sliced in [false, true] {
        let mut padded = vec![Some("invalid-before-slice")];
        padded.extend_from_slice(input);
        padded.push(Some("invalid-after-slice"));
        for source in string_layouts(if sliced { &padded } else { input }) {
            let input = if sliced {
                source.slice(1, input.len())
            } else {
                source
            };
            input.to_data().validate_full()?;
            let ParsedStringRows {
                values,
                failure: actual_failure,
            } = parse_string_rows_until_error::<P>(input.as_ref())?;
            values.to_data().validate_full()?;
            assert_eq!(values.data_type(), &P::DATA_TYPE);
            assert_eq!(values.to_data(), expected.to_data(), "input: {input:?}");
            let strict_cast = cast_with_options(input.as_ref(), &P::DATA_TYPE, &options);
            if let Some((row_index, bad_value)) = failure {
                let actual_failure = actual_failure.ok_or_else(|| {
                    ArrowError::InvalidArgumentError("missing parse failure".into())
                })?;
                assert_eq!(actual_failure.row_index, row_index);
                assert_eq!(values.len(), row_index);
                let message = format!(
                    "Cannot cast string '{bad_value}' to value of {} type",
                    P::DATA_TYPE
                );
                assert!(
                    matches!(&actual_failure.error, ArrowError::CastError(actual)
                    if actual == &message)
                );
                assert!(matches!(strict_cast, Err(ArrowError::CastError(actual))
                    if actual == message));
            } else {
                assert!(actual_failure.is_none());
                let strict_cast = strict_cast?;
                strict_cast.to_data().validate_full()?;
                assert_eq!(strict_cast.to_data(), expected.to_data());
            }
        }
    }
    Ok(())
}

#[test]
fn strict_rows_preserve_nulls_and_stop_at_the_first_error() -> Result<(), ArrowError> {
    assert_rows::<Int64Type>(&[], &[], None)?;
    assert_rows::<Int64Type>(&[None, None], &[None, None], None)?;
    assert_rows::<Int64Type>(
        &[
            Some("-9223372036854775808"),
            Some("0"),
            Some("9223372036854775807"),
        ],
        &[Some(i64::MIN), Some(0), Some(i64::MAX)],
        None,
    )?;
    assert_rows::<Int64Type>(
        &[Some("12"), None, Some("-34"), None],
        &[Some(12), None, Some(-34), None],
        None,
    )?;
    assert_rows::<Int64Type>(
        &[Some("first-invalid"), Some("7"), Some("second-invalid")],
        &[],
        Some((0, "first-invalid")),
    )?;
    assert_rows::<Int64Type>(
        &[None, Some("9"), None, Some("bad"), Some("bad"), Some("18")],
        &[None, Some(9), None],
        Some((3, "bad")),
    )?;
    assert_rows::<Int64Type>(
        &[
            Some("7"),
            Some("9223372036854775808"),
            Some("different-invalid"),
        ],
        &[Some(7)],
        Some((1, "9223372036854775808")),
    )?;
    assert_rows::<Int64Type>(
        &[Some("-9223372036854775809"), None],
        &[],
        Some((0, "-9223372036854775809")),
    )
}

#[test]
fn every_spark_ansi_integer_width_uses_the_existing_checked_parser() -> Result<(), ArrowError> {
    assert_rows::<Int8Type>(
        &[Some("-128"), None, Some("127")],
        &[Some(i8::MIN), None, Some(i8::MAX)],
        None,
    )?;
    assert_rows::<Int8Type>(
        &[Some("127"), None, Some("128")],
        &[Some(127), None],
        Some((2, "128")),
    )?;
    assert_rows::<Int8Type>(&[Some("-129")], &[], Some((0, "-129")))?;
    assert_rows::<Int16Type>(
        &[Some("-32768"), None, Some("32767")],
        &[Some(i16::MIN), None, Some(i16::MAX)],
        None,
    )?;
    assert_rows::<Int16Type>(
        &[Some("32767"), None, Some("32768")],
        &[Some(32767), None],
        Some((2, "32768")),
    )?;
    assert_rows::<Int16Type>(&[Some("-32769")], &[], Some((0, "-32769")))?;
    assert_rows::<Int32Type>(
        &[Some("-2147483648"), None, Some("2147483647")],
        &[Some(i32::MIN), None, Some(i32::MAX)],
        None,
    )?;
    assert_rows::<Int32Type>(
        &[Some("2147483647"), None, Some("2147483648")],
        &[Some(2147483647), None],
        Some((2, "2147483648")),
    )?;
    assert_rows::<Int32Type>(&[Some("-2147483649")], &[], Some((0, "-2147483649")))
}

#[test]
fn safe_casts_keep_parsing_after_invalid_values() -> Result<(), ArrowError> {
    let expected = Int32Array::from(vec![Some(12), None, None, None, None, Some(-34)]);
    for input in string_layouts(&[
        Some("12"),
        None,
        Some("bad"),
        Some("2147483648"),
        Some("bad"),
        Some("-34"),
    ]) {
        let output = cast_with_options(
            input.as_ref(),
            &Int32Type::DATA_TYPE,
            &CastOptions::default(),
        )?;
        output.to_data().validate_full()?;
        assert_eq!(output.to_data(), expected.to_data());
    }
    Ok(())
}

#[test]
fn shared_strict_loop_preserves_other_primitive_casts() -> Result<(), ArrowError> {
    assert_rows::<UInt32Type>(
        &[Some("0"), None, Some("4294967295"), Some("-1")],
        &[Some(0), None, Some(u32::MAX)],
        Some((3, "-1")),
    )?;
    assert_rows::<Float64Type>(
        &[Some("1.5"), None, Some("-2.25")],
        &[Some(1.5), None, Some(-2.25)],
        None,
    )?;
    assert_rows::<Float64Type>(&[Some("1.5"), Some("bad")], &[Some(1.5)], Some((1, "bad")))?;
    assert_rows::<Date32Type>(
        &[Some("1970-01-02"), None, Some("1969-12-31")],
        &[Some(1), None, Some(-1)],
        None,
    )?;
    assert_rows::<Date32Type>(&[Some("bad")], &[], Some((0, "bad")))?;
    assert_rows::<Time32SecondType>(
        &[Some("00:00:01"), None, Some("01:00:00")],
        &[Some(1), None, Some(3600)],
        None,
    )?;
    assert_rows::<Time32SecondType>(&[Some("bad")], &[], Some((0, "bad")))
}

#[test]
fn non_string_input_is_rejected_without_downcasting() {
    let result = parse_string_rows_until_error::<Int32Type>(&Int32Array::from(vec![12]));
    assert!(matches!(result, Err(ArrowError::CastError(message))
        if message == "Cannot parse string rows from Int32 type"));
}
