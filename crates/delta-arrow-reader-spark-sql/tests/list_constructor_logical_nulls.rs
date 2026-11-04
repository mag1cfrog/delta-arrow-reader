//! Required List elements use logical NULLs without rewriting child storage.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, DictionaryArray, GenericListArray, Int8Array, Int32Array,
    NullArray, OffsetSizeTrait, RunArray, UnionArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Int8Type, Int32Type, UnionFields};
use arrow::error::ArrowError;

fn encoded_integer_at(array: &dyn Array, row: usize) -> Result<Option<i32>, ArrowError> {
    if array.logical_nulls().is_some_and(|nulls| nulls.is_null(row)) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Int32 => Ok(Some(array.as_primitive::<Int32Type>().value(row))),
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_dictionary::<Int8Type>();
            let key = dictionary.key(row).ok_or_else(|| {
                ArrowError::InvalidArgumentError("missing dictionary key".into())
            })?;
            encoded_integer_at(dictionary.values().as_ref(), key)
        }
        DataType::RunEndEncoded(_, _) => {
            let runs = array.as_run::<Int32Type>();
            encoded_integer_at(runs.values().as_ref(), runs.get_physical_index(row))
        }
        DataType::Union(_, _) => {
            let union = array.as_union();
            encoded_integer_at(
                union.child(union.type_id(row)).as_ref(),
                union.value_offset(row),
            )
        }
        other => Err(ArrowError::InvalidArgumentError(format!(
            "unexpected integer encoding: {other}"
        ))),
    }
}

fn assert_list_constructor_preserves_encoded_children<O: OffsetSizeTrait>() -> Result<(), ArrowError> {
    let union_fields = UnionFields::try_new(
        vec![0, 1],
        vec![
            Field::new("integer", DataType::Int32, true)
                .with_metadata([("unit".into(), "count".into())].into()),
            Field::new("unused", DataType::Null, true),
        ],
    )?;
    let hidden_null_runs: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![1, 3, 4]),
        &Int32Array::from(vec![None, Some(3), None]),
    )?);
    let cases: Vec<(&str, ArrayRef, Vec<Option<i32>>)> = vec![
        (
            "primitive",
            Arc::new(Int32Array::from(vec![3, 4])),
            vec![Some(3), Some(4)],
        ),
        (
            "inactive_null_union_child",
            Arc::new(UnionArray::try_new(
                union_fields.clone(),
                vec![0, 0, 0, 0].into(),
                None,
                vec![
                    Arc::new(Int32Array::from(vec![900, 3, 4, 800])),
                    Arc::new(NullArray::new(4)),
                ],
            )?),
            vec![Some(900), Some(3), Some(4), Some(800)],
        ),
        (
            "sliced_run_with_hidden_nulls",
            hidden_null_runs.slice(1, 2),
            vec![Some(3), Some(3)],
        ),
        (
            "all_valid_dictionary_key_bitmap",
            Arc::new(DictionaryArray::<Int8Type>::try_new(
                Int8Array::new(vec![0, 1, 0, 1].into(), Some(NullBuffer::new_valid(4))),
                Arc::new(Int32Array::from(vec![3, 4])),
            )?),
            vec![Some(3), Some(4), Some(3), Some(4)],
        ),
        (
            "unused_null_dictionary_values",
            Arc::new(DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![1, 3, 1, 3]),
                Arc::new(Int32Array::from(vec![None, Some(3), None, Some(4)])),
            )?),
            vec![Some(3), Some(4), Some(3), Some(4)],
        ),
        (
            "primitive_null",
            Arc::new(Int32Array::from(vec![Some(3), None])),
            vec![Some(3), None],
        ),
        (
            "dictionary_null_key",
            Arc::new(DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), None]),
                Arc::new(Int32Array::from(vec![3])),
            )?),
            vec![Some(3), None],
        ),
        (
            "dictionary_null_value",
            Arc::new(DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0, 1]),
                Arc::new(Int32Array::from(vec![Some(3), None])),
            )?),
            vec![Some(3), None],
        ),
        (
            "selected_null_union_child",
            Arc::new(UnionArray::try_new(
                union_fields,
                vec![0, 1].into(),
                None,
                vec![
                    Arc::new(Int32Array::from(vec![3, 4])),
                    Arc::new(NullArray::new(2)),
                ],
            )?),
            vec![Some(3), None],
        ),
        (
            "selected_null_run",
            Arc::new(RunArray::<Int32Type>::try_new(
                &Int32Array::from(vec![1, 2]),
                &Int32Array::from(vec![Some(3), None]),
            )?),
            vec![Some(3), None],
        ),
    ];
    let mut failures = Vec::new();
    for (id, values, expected) in cases {
        values.to_data().validate_full()?;
        for (offset, length) in [
            (0, values.len()),
            (1, values.len() - 1),
            (1, values.len() - 2),
            (1, 0),
        ] {
            let values = values.slice(offset, length);
            let expected = &expected[offset..offset + length];
            let actual = (0..length)
                .map(|row| encoded_integer_at(values.as_ref(), row))
                .collect::<Result<Vec<_>, _>>()?;
            assert_eq!(actual, expected, "{id}");
            let null_count = expected.iter().filter(|value| value.is_none()).count();
            assert_eq!(values.logical_null_count(), null_count, "{id}");
            for parent_null in [false, true] {
                let offsets = OffsetBuffer::<O>::from_lengths([length]);
                let nulls = parent_null.then(|| NullBuffer::new_null(1));
                for element_nullable in [false, true] {
                    let field = Arc::new(
                        Field::new("entry", values.data_type().clone(), element_nullable)
                            .with_metadata([("origin".into(), "retained".into())].into()),
                    );
                    let result = GenericListArray::<O>::try_new(
                        field.clone(),
                        offsets.clone(),
                        values.clone(),
                        nulls.clone(),
                    );
                    let empty = GenericListArray::<O>::try_new(
                        field.clone(),
                        OffsetBuffer::from_lengths([]),
                        values.clone(),
                        None,
                    );
                    let context = format!("{id} offset={offset} len={length} parent_null={parent_null} element_nullable={element_nullable}");
                    if !element_nullable && null_count != 0 {
                        for result in [result, empty] {
                            let error = result.err().ok_or_else(|| {
                                ArrowError::InvalidArgumentError(format!(
                                    "accepted a logical NULL: {context}"
                                ))
                            })?;
                            assert!(error.to_string().contains("Non-nullable field"), "{context}: {error}");
                        }
                        continue;
                    }
                    let list = match result {
                        Ok(list) => list,
                        Err(error) => {
                            failures.push(format!("{context}: {error}"));
                            continue;
                        }
                    };
                    list.to_data().validate_full()?;
                    assert_eq!(list.data_type(), &GenericListArray::<O>::DATA_TYPE_CONSTRUCTOR(field));
                    assert!(Arc::ptr_eq(list.values(), &values), "{context}");
                    assert!(list.offsets().ptr_eq(&offsets), "{context}");
                    assert_eq!(list.nulls(), nulls.as_ref(), "{context}");
                    assert_eq!(list.is_null(0), parent_null, "{context}");
                    let child = list.value(0);
                    let actual = (0..child.len())
                        .map(|row| encoded_integer_at(child.as_ref(), row))
                        .collect::<Result<Vec<_>, _>>()?;
                    assert_eq!(actual, expected, "{context}");
                    let empty = empty?;
                    empty.to_data().validate_full()?;
                    assert!(empty.is_empty());
                    assert!(Arc::ptr_eq(empty.values(), &values));
                }
            }
        }
    }
    let values: ArrayRef = Arc::new(Int32Array::from(vec![3, 4]));
    let field = Arc::new(Field::new("entry", DataType::Int32, false));
    for (field, offsets, nulls, message) in [
        (field.clone(), OffsetBuffer::from_lengths([3]), None, "Max offset"),
        (field, OffsetBuffer::from_lengths([2]), Some(NullBuffer::new_valid(2)), "Incorrect length of null buffer"),
        (Arc::new(Field::new("entry", DataType::Int64, false)), OffsetBuffer::from_lengths([2]), None, "expected data type"),
    ] {
        let error = GenericListArray::<O>::try_new(field, offsets, values.clone(), nulls)
            .err()
            .ok_or_else(|| ArrowError::InvalidArgumentError(format!("accepted invalid constructor input: {message}")))?;
        assert!(error.to_string().contains(message), "{error}");
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[test]
fn list_constructor_uses_logical_child_nulls() -> Result<(), ArrowError> {
    assert_list_constructor_preserves_encoded_children::<i32>()
}

#[test]
fn large_list_constructor_uses_logical_child_nulls() -> Result<(), ArrowError> {
    assert_list_constructor_preserves_encoded_children::<i64>()
}

fn assert_nonzero_dense_union_nulls<O: OffsetSizeTrait>() -> Result<(), ArrowError> {
    let fields = UnionFields::try_new(
        vec![7],
        vec![Field::new("integer", DataType::Int32, true)],
    )?;
    let mut failures = Vec::new();
    for value in [Some(3), None] {
        let values: ArrayRef = Arc::new(UnionArray::try_new(
            fields.clone(),
            vec![7].into(),
            Some(vec![0].into()),
            vec![Arc::new(Int32Array::from(vec![value]))],
        )?);
        values.to_data().validate_full()?;
        let union = values.as_union();
        assert_eq!(
            union.child(union.type_id(0)).is_null(union.value_offset(0)),
            value.is_none()
        );
        let expected_nulls = usize::from(value.is_none());
        let actual_nulls = values.logical_null_count();
        let result = GenericListArray::<O>::try_new(
            Arc::new(Field::new("entry", values.data_type().clone(), false)),
            OffsetBuffer::from_lengths([1]),
            values.clone(),
            None,
        );
        eprintln!(
            "LIST_CONSTRUCTOR_DENSE_UNION_OBSERVATION {}",
            serde_json::json!({"large":O::IS_LARGE,"value":value,
                "expected_logical_nulls":expected_nulls,"actual_logical_nulls":actual_nulls,
                "constructor_ok":result.is_ok(),"error":result.as_ref().err().map(ToString::to_string)})
        );
        if actual_nulls != expected_nulls || result.is_ok() != value.is_some() {
            failures.push(format!(
                "value={value:?}: expected NULLs={expected_nulls}, actual NULLs={actual_nulls}, result={result:?}"
            ));
        }
        if let Ok(list) = result {
            list.to_data().validate_full()?;
            assert!(Arc::ptr_eq(list.values(), &values));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

#[test]
fn list_constructor_checks_nonzero_dense_union_nulls() -> Result<(), ArrowError> {
    assert_nonzero_dense_union_nulls::<i32>()
}

#[test]
fn large_list_constructor_checks_nonzero_dense_union_nulls() -> Result<(), ArrowError> {
    assert_nonzero_dense_union_nulls::<i64>()
}
