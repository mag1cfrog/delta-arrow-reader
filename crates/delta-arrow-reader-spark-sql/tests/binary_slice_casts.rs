//! Binary-to-string casts validate visible non-NULL values across storage layouts.

use arrow::array::{Array, AsArray, BinaryArray, BinaryViewArray};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow::compute::{CastOptions, cast, cast_with_options};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;

#[allow(
    dead_code,
    reason = "This test uses the shared fixture checker without open compatibility profiles."
)]
#[path = "support/string_binary_contracts.rs"]
mod contracts;
#[allow(
    dead_code,
    reason = "The fixture checker uses capture_observation and input registration."
)]
#[path = "support/string_binary.rs"]
mod string_binary;

#[tokio::test]
async fn guarded_binary_cast_preserves_complete_spark_outcomes() -> string_binary::TestResult {
    contracts::check_fixture(
        include_str!("fixtures/binary-slice-cast-spark.jsonl"),
        2,
        |_, _, _, _| None,
    )
    .await
}

fn assert_casts(
    input: &dyn Array,
    expected: &[Option<&[u8]>],
    strict_errors: Option<(&str, &str)>,
) -> Result<(), ArrowError> {
    input.to_data().validate_full()?;
    for target in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        for safe in [false, true] {
            let options = CastOptions {
                safe,
                ..Default::default()
            };
            let result = cast_with_options(input, &target, &options);
            if !safe
                && let Some((byte_error, view_error)) = strict_errors
            {
                let expected_error = if input.data_type() == &DataType::BinaryView
                    && target == DataType::Utf8View
                {
                    view_error
                } else {
                    byte_error
                };
                assert!(
                    matches!(&result, Err(ArrowError::InvalidArgumentError(message))
                        if message == expected_error),
                    "{} to {target}: {result:?}",
                    input.data_type()
                );
                continue;
            }
            let actual = result?;
            actual.to_data().validate_full()?;
            assert_eq!(actual.data_type(), &target);
            let values: Vec<_> = if target == DataType::Utf8 {
                actual.as_string::<i32>().iter().collect()
            } else if target == DataType::LargeUtf8 {
                actual.as_string::<i64>().iter().collect()
            } else {
                actual.as_string_view().iter().collect()
            };
            let bytes: Vec<_> = values
                .into_iter()
                .map(|value| value.map(str::as_bytes))
                .collect();
            assert_eq!(
                bytes,
                expected,
                "{} to {target}, safe={safe}",
                input.data_type()
            );
        }
    }
    Ok(())
}

fn assert_binary_layouts(
    backing: &BinaryArray,
    offset: usize,
    length: usize,
    expected: &[Option<&[u8]>],
    strict_errors: Option<(&str, &str)>,
) -> Result<(), ArrowError> {
    for source in [DataType::Binary, DataType::LargeBinary, DataType::BinaryView] {
        let backing = cast(backing, &source)?;
        let visible = backing.slice(offset, length);
        assert_casts(visible.as_ref(), expected, strict_errors)?;
    }
    Ok(())
}

#[test]
fn valid_binary_slices_ignore_retained_bytes_and_null_payloads() -> Result<(), ArrowError> {
    let expected = [
        Some(b"".as_slice()),
        Some(b"a\0"),
        Some("é😀".as_bytes()),
        None,
    ];
    let compact = BinaryArray::from(expected.to_vec());
    let mut suffix = expected.to_vec();
    suffix.push(Some(b"\xff"));
    let suffix = BinaryArray::from(suffix);
    let mut padded = vec![Some(b"\xff".as_slice())];
    padded.extend_from_slice(&expected);
    padded.push(Some(b"\xff"));
    let padded = BinaryArray::from(padded);
    for (backing, offset) in [(&compact, 0), (&suffix, 0), (&padded, 1)] {
        assert_binary_layouts(backing, offset, expected.len(), &expected, None)?;
    }
    assert_binary_layouts(&padded, 2, 0, &[], None)?;

    let nullable = BinaryArray::try_new(
        OffsetBuffer::new(vec![0_i32, 1, 2, 3, 4, 5].into()),
        Buffer::from_slice_ref(b"\xff\xffa\xff\xff"),
        Some(NullBuffer::from(vec![true, false, true, false, true])),
    )?;
    assert_binary_layouts(&nullable, 1, 3, &[None, Some(b"a"), None], None)?;
    assert_binary_layouts(&nullable, 1, 1, &[None], None)?;

    let valid_offset = BinaryArray::from_vec(vec![
        b"prefix".as_slice(),
        b"",
        "é😀".as_bytes(),
        b"suffix",
    ]);
    assert_binary_layouts(&valid_offset, 1, 2, &[Some(b""), Some("é😀".as_bytes())], None)
}

#[test]
fn checked_binary_views_ignore_null_payloads_but_keep_active_errors() -> Result<(), ArrowError> {
    // Binary-to-view conversion clears NULL payloads, so retain them with a checked constructor.
    let view = BinaryViewArray::from(vec![
        b"prefix".as_slice(),
        b"\xff",
        b"a",
        b"\xff",
        b"\xff",
        b"suffix",
    ]);
    let (views, buffers, _) = view.into_parts();
    let view = BinaryViewArray::try_new(
        views,
        buffers,
        Some(NullBuffer::from(vec![true, false, true, false, true, true])),
    )?;
    assert_casts(&view.slice(1, 3), &[None, Some(b"a"), None], None)?;
    assert_casts(&view.slice(1, 1), &[None], None)?;
    assert_casts(&view.slice(2, 0), &[], None)?;
    assert_casts(
        &view.slice(1, 4),
        &[None, Some(b"a"), None, None],
        Some((
            "Encountered non UTF-8 data: invalid utf-8 sequence of 1 bytes from index 1",
            "Encountered non-UTF-8 data at index 0: invalid utf-8 sequence of 1 bytes from index 0",
        )),
    )
}

#[test]
fn active_invalid_binary_values_preserve_strict_errors_and_safe_nulls() -> Result<(), ArrowError> {
    let padded = BinaryArray::from_vec(vec![b"\xff".as_slice(), b"a", b"\xff"]);
    assert_binary_layouts(
        &padded,
        2,
        1,
        &[None],
        Some((
            "Encountered non UTF-8 data: invalid utf-8 sequence of 1 bytes from index 0",
            "Encountered non-UTF-8 data at index 0: invalid utf-8 sequence of 1 bytes from index 0",
        )),
    )?;
    let mixed = BinaryArray::from(vec![
        Some(b"".as_slice()),
        Some(b"a"),
        Some(b"\xff"),
        None,
        Some("é😀".as_bytes()),
    ]);
    assert_binary_layouts(
        &mixed,
        0,
        5,
        &[Some(b""), Some(b"a"), None, None, Some("é😀".as_bytes())],
        Some((
            "Encountered non UTF-8 data: invalid utf-8 sequence of 1 bytes from index 1",
            "Encountered non-UTF-8 data at index 2: invalid utf-8 sequence of 1 bytes from index 0",
        )),
    )?;
    let split = BinaryArray::try_new(
        OffsetBuffer::new(vec![0_i32, 1, 2].into()),
        Buffer::from_slice_ref(b"\xc3\xa9"),
        None,
    )?;
    assert_binary_layouts(
        &split,
        0,
        2,
        &[None, None],
        Some((
            "Split UTF-8 codepoint at offset 1",
            "Encountered non-UTF-8 data at index 0: incomplete utf-8 byte sequence from index 0",
        )),
    )
}
