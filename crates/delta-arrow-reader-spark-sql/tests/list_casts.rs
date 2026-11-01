//! Unencoded scalar list casts ignore unselected values; complex arrays keep sharing.
//! Gapped nested/encoded conversion bounds remain tracked in #666.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, GenericListViewArray, ListArray, StringArray,
    make_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{CastOptions, cast, cast_with_options};
use arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef, Int32Type};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn list_layouts(field: FieldRef) -> [DataType; 5] {
    [
        DataType::List(field.clone()),
        DataType::LargeList(field.clone()),
        DataType::ListView(field.clone()),
        DataType::LargeListView(field.clone()),
        DataType::FixedSizeList(field, 2),
    ]
}

fn row_values(array: &dyn Array, index: usize) -> Result<ArrayRef, arrow::error::ArrowError> {
    Ok(match array.data_type() {
        DataType::List(_) => array.as_list::<i32>().value(index),
        DataType::LargeList(_) => array.as_list::<i64>().value(index),
        DataType::ListView(_) => array.as_list_view::<i32>().value(index),
        DataType::LargeListView(_) => array.as_list_view::<i64>().value(index),
        DataType::FixedSizeList(_, _) => array.as_fixed_size_list().value(index),
        data_type => {
            return Err(arrow::error::ArrowError::InvalidArgumentError(format!(
                "expected list fixture, received {data_type}"
            )));
        }
    })
}

#[test]
fn list_casts_only_convert_visible_values() -> TestResult {
    let source = Arc::new(Field::new("source", DataType::Decimal128(8, 1), true));
    let target = Arc::new(
        Field::new("amount", DataType::Decimal128(10, 2), true)
            .with_metadata([("unit".to_owned(), "synthetic".to_owned())].into()),
    );
    let backing = ListArray::try_new(
        source.clone(),
        OffsetBuffer::from_repeated_length(2, 2048),
        Arc::new(
            Decimal128Array::from_iter((0..2048).flat_map(|i| [Some(i * 10), None]))
                .with_precision_and_scale(8, 1)?,
        ),
        None,
    )?;
    for source_type in list_layouts(source) {
        let backing = cast(&backing, &source_type)?;
        for target_type in list_layouts(target.clone()) {
            for len in [0, 1, 2] {
                let input = backing.slice(1001, len);
                let result = cast(&input, &target_type)?;
                assert_eq!(result.data_type(), &target_type);
                assert_eq!(result.len(), len);
                assert_eq!(result.null_count(), 0);
                let child = make_array(result.to_data().child_data()[0].clone());
                assert_eq!(child.len(), 2 * len, "{source_type} -> {target_type}");
                for index in 0..len {
                    let values = row_values(result.as_ref(), index)?;
                    let values = values.as_primitive::<Decimal128Type>();
                    assert_eq!(
                        values.iter().collect::<Vec<_>>(),
                        [Some((1001 + index as i128) * 100), None]
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn list_casts_reject_only_visible_invalid_values() -> TestResult {
    let source = Arc::new(Field::new("source", DataType::Utf8, true));
    let target = Arc::new(Field::new("parsed", DataType::Int32, true));
    let backing = ListArray::try_new(
        source.clone(),
        OffsetBuffer::from_repeated_length(2, 3),
        Arc::new(StringArray::from(vec![
            Some("invalid"),
            None,
            Some("12"),
            None,
            Some("unused"),
            None,
        ])),
        None,
    )?;
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    for source_type in list_layouts(source) {
        let backing = cast(&backing, &source_type)?;
        for target_type in list_layouts(target.clone()) {
            let input = backing.slice(1, 1);
            let result = cast_with_options(&input, &target_type, &strict)?;
            assert_eq!(result.data_type(), &target_type);
            assert_eq!(result.len(), 1);
            let row = row_values(result.as_ref(), 0)?;
            assert_eq!(
                row.as_primitive::<Int32Type>().iter().collect::<Vec<_>>(),
                [Some(12), None]
            );
            let invalid = backing.slice(0, 1);
            assert!(cast_with_options(&invalid, &target_type, &strict).is_err());
            let nullable = cast(&invalid, &target_type)?;
            assert_eq!(row_values(nullable.as_ref(), 0)?.null_count(), 2);
        }
    }
    Ok(())
}

#[test]
fn list_view_casts_preserve_sharing_without_converting_gaps() -> TestResult {
    let source = Arc::new(Field::new("source", DataType::Utf8, true));
    let target = Arc::new(Field::new("parsed", DataType::Int32, true));
    let values: ArrayRef = Arc::new(StringArray::from(vec!["bad", "12", "unused", "34", "bad"]));
    let nulls = NullBuffer::from(vec![true, true, true, true, false, true]);
    let inputs: [ArrayRef; 2] = [
        Arc::new(GenericListViewArray::<i32>::try_new(
            source.clone(),
            vec![4, 3, 1, 1, 4, 0].into(),
            vec![1, 1, 1, 1, 0, 0].into(),
            values.clone(),
            Some(nulls.clone()),
        )?),
        Arc::new(GenericListViewArray::<i64>::try_new(
            source,
            vec![4, 3, 1, 1, 4, 0].into(),
            vec![1, 1, 1, 1, 0, 0].into(),
            values,
            Some(nulls),
        )?),
    ];
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    for input in inputs {
        let input = input.slice(1, 5);
        for target_type in list_layouts(target.clone()).into_iter().take(4) {
            let result = cast_with_options(&input, &target_type, &strict)?;
            assert_eq!(result.data_type(), &target_type);
            assert_eq!(result.len(), 5);
            assert_eq!(result.null_count(), 1);
            assert!(result.is_null(3));
            for (index, expected) in [34, 12, 12].into_iter().enumerate() {
                let row = row_values(result.as_ref(), index)?;
                assert_eq!(
                    row.as_primitive::<Int32Type>().iter().collect::<Vec<_>>(),
                    [Some(expected)]
                );
            }
            assert_eq!(row_values(result.as_ref(), 4)?.len(), 0);
            let child = make_array(result.to_data().child_data()[0].clone());
            let expected_len = if matches!(
                target_type,
                DataType::ListView(_) | DataType::LargeListView(_)
            ) {
                2
            } else {
                3
            };
            assert_eq!(child.len(), expected_len);
        }
    }
    Ok(())
}

fn nested_view_fixtures(values: ArrayRef) -> Result<Vec<ArrayRef>, arrow::error::ArrowError> {
    use arrow::array::{FixedSizeListArray, StructArray};
    let field = Arc::new(Field::new("value", values.data_type().clone(), true));
    let inner: ArrayRef = Arc::new(GenericListViewArray::<i32>::try_new(
        field,
        vec![0; 5].into(),
        vec![3; 5].into(),
        values,
        None,
    )?);
    let field = Arc::new(Field::new("inner", inner.data_type().clone(), true));
    Ok(vec![
        inner.clone(),
        Arc::new(ListArray::try_new(
            field.clone(),
            OffsetBuffer::from_repeated_length(1, 5),
            inner.clone(),
            None,
        )?),
        Arc::new(StructArray::try_new(
            vec![field.clone()].into(),
            vec![inner.clone()],
            None,
        )?),
        Arc::new(FixedSizeListArray::try_new(field, 1, inner, None)?),
    ])
}

#[test]
fn list_view_casts_do_not_expand_nested_shared_values() -> TestResult {
    use arrow::array::{Int32Array, Int64Array};
    let inputs = nested_view_fixtures(Arc::new(Int32Array::from(vec![1, 2, 3])))?;
    let targets = nested_view_fixtures(Arc::new(Int64Array::from(vec![1, 2, 3])))?;
    for (values, expected_values) in inputs.into_iter().zip(targets) {
        for offsets in [vec![1, 2], vec![1, 3]] {
            let source = GenericListViewArray::<i32>::try_new(
                Arc::new(Field::new("outer", values.data_type().clone(), true)),
                offsets.clone().into(),
                vec![1, 1].into(),
                values.clone(),
                None,
            )?;
            let expected = GenericListViewArray::<i32>::try_new(
                Arc::new(Field::new(
                    "outer",
                    expected_values.data_type().clone(),
                    true,
                )),
                offsets.into(),
                vec![1, 1].into(),
                expected_values.clone(),
                None,
            )?;
            let actual = cast(&source, expected.data_type())?;
            assert_eq!(actual.to_data(), expected.to_data());
            let mut leaf = actual.to_data();
            while !leaf.child_data().is_empty() {
                leaf = leaf.child_data()[0].clone();
            }
            assert_eq!(
                leaf.len(),
                3,
                "shared nested values expanded for {}",
                source.data_type()
            );
        }
    }
    Ok(())
}

#[test]
fn list_view_field_changes_preserve_contiguous_value_buffers() -> TestResult {
    use arrow::array::Int32Array;
    let values = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
    let input = GenericListViewArray::<i32>::try_new(
        Arc::new(Field::new("before", DataType::Int32, true)),
        vec![1].into(),
        vec![2].into(),
        values.clone(),
        None,
    )?;
    for target in [
        DataType::ListView(Arc::new(Field::new("after", DataType::Int32, true))),
        DataType::LargeListView(Arc::new(Field::new("after", DataType::Int32, true))),
    ] {
        let output = cast(&input, &target)?;
        let child = make_array(output.to_data().child_data()[0].clone());
        let expected = values.slice(1, 2);
        assert_eq!(
            child.as_primitive::<Int32Type>().values().as_ptr(),
            expected.values().as_ptr()
        );
    }
    Ok(())
}

#[test]
fn list_view_casts_preserve_encoded_children() -> TestResult {
    use arrow::array::{DictionaryArray, Int8Array, Int32Array};
    use arrow::datatypes::Int8Type;
    let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0, 1, 2]),
        Arc::new(Int32Array::from_iter_values(0..128)),
    )?);
    let input = GenericListViewArray::<i32>::try_new(
        Arc::new(Field::new("before", dictionary.data_type().clone(), true)),
        vec![0, 2].into(),
        vec![1, 1].into(),
        dictionary.clone(),
        None,
    )?;
    let target = DataType::LargeListView(Arc::new(Field::new(
        "after",
        dictionary.data_type().clone(),
        true,
    )));
    let output = cast(&input, &target)?;
    assert_eq!(output.len(), 2);
    for (index, expected) in [0, 2].into_iter().enumerate() {
        let row = cast(&row_values(output.as_ref(), index)?, &DataType::Int32)?;
        assert_eq!(row.as_primitive::<Int32Type>().value(0), expected);
    }
    assert_eq!(output.to_data().child_data()[0].child_data()[0].len(), 128);
    Ok(())
}

#[test]
fn list_view_casts_merge_partial_overlaps_without_copying_gaps() -> TestResult {
    let input = GenericListViewArray::<i32>::try_new(
        Arc::new(Field::new("source", DataType::Utf8, true)),
        vec![1, 2, 5].into(),
        vec![2, 2, 1].into(),
        Arc::new(StringArray::from(vec![
            "bad", "12", "23", "34", "unused", "56", "bad",
        ])),
        None,
    )?;
    let target = DataType::ListView(Arc::new(Field::new("parsed", DataType::Int32, true)));
    let output = cast_with_options(
        &input,
        &target,
        &CastOptions {
            safe: false,
            ..CastOptions::default()
        },
    )?;
    for (index, expected) in [vec![12, 23], vec![23, 34], vec![56]]
        .into_iter()
        .enumerate()
    {
        let row = row_values(output.as_ref(), index)?;
        assert_eq!(row.as_primitive::<Int32Type>().values().as_ref(), expected);
    }
    assert_eq!(output.to_data().child_data()[0].len(), 4);
    Ok(())
}

#[test]
fn list_casts_preserve_identity_and_apply_target_fields() -> TestResult {
    let source = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), None]),
        Some(vec![Some(2), None]),
    ]);
    let DataType::List(field) = source.data_type() else {
        unreachable!()
    };
    for source_type in list_layouts(field.clone()) {
        let input = cast(&source, &source_type)?.slice(1, 1);
        let unchanged = cast(&input, &source_type)?;
        assert_eq!(unchanged.to_data(), input.to_data());
        assert_eq!(
            unchanged.to_data().child_data()[0].buffers()[0].as_ptr(),
            input.to_data().child_data()[0].buffers()[0].as_ptr()
        );
        for child_type in [DataType::Int32, DataType::Int64] {
            let required = Arc::new(Field::new("required", child_type.clone(), false));
            for target_type in list_layouts(required) {
                let error =
                    cast(&input, &target_type).expect_err("non-nullable child must reject NULL");
                assert!(
                    error
                        .to_string()
                        .to_ascii_lowercase()
                        .contains("non-nullable"),
                    "{error}"
                );
            }
            let renamed = Arc::new(
                Field::new("renamed", child_type, true)
                    .with_metadata([("unit".to_owned(), "synthetic".to_owned())].into()),
            );
            for target_type in list_layouts(renamed) {
                let output = cast(&input, &target_type)?;
                assert_eq!(output.data_type(), &target_type, "{source_type}");
                assert_eq!(output.len(), 1);
                assert_eq!(row_values(output.as_ref(), 0)?.null_count(), 1);
            }
        }
    }
    Ok(())
}

#[test]
fn zero_width_list_casts_keep_rows_and_nulls() -> TestResult {
    use arrow::array::{FixedSizeListArray, Int32Array};
    let source = Arc::new(Field::new("source", DataType::Int32, true));
    let mut source_types = list_layouts(source.clone());
    source_types[4] = DataType::FixedSizeList(source.clone(), 0);
    for nulls in [None, Some(NullBuffer::from(vec![true, false]))] {
        let backing = FixedSizeListArray::try_new_with_length(
            source.clone(),
            0,
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            nulls.clone(),
            2,
        )?;
        for source_type in &source_types {
            let input = cast(&backing, source_type)?;
            assert_eq!(input.len(), 2, "{source_type}");
            for child_type in [DataType::Int32, DataType::Int64] {
                let target = Arc::new(
                    Field::new("renamed", child_type, true)
                        .with_metadata([("unit".to_owned(), "synthetic".to_owned())].into()),
                );
                let mut target_types = list_layouts(target.clone());
                target_types[4] = DataType::FixedSizeList(target, 0);
                for target_type in target_types {
                    let output = cast(&input, &target_type)?;
                    assert_eq!(output.data_type(), &target_type, "{source_type}");
                    assert_eq!(output.len(), 2, "{source_type} -> {target_type}");
                    assert_eq!(output.nulls(), nulls.as_ref());
                    for index in 0..2 {
                        assert!(row_values(output.as_ref(), index)?.is_empty());
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn list_view_casts_preserve_run_end_compression() -> TestResult {
    use arrow::array::{Int32Array, RunArray};
    let values: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![1000]),
        &Int32Array::from(vec![42]),
    )?);
    let input = GenericListViewArray::<i32>::try_new(
        Arc::new(Field::new("before", values.data_type().clone(), true)),
        vec![1, 999].into(),
        vec![1, 1].into(),
        values.clone(),
        None,
    )?;
    let target = DataType::LargeListView(Arc::new(Field::new(
        "after",
        values.data_type().clone(),
        true,
    )));
    let output = cast(&input, &target)?;
    assert_eq!(output.len(), 2);
    for index in 0..2 {
        let row = cast(&row_values(output.as_ref(), index)?, &DataType::Int32)?;
        assert_eq!(
            row.as_primitive::<Int32Type>().iter().collect::<Vec<_>>(),
            [Some(42)]
        );
    }
    assert_eq!(output.to_data().child_data()[0].child_data()[1].len(), 1);
    Ok(())
}
