// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, NullArray, NullBufferBuilder, StructArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions::utils::make_scalar_function;
use datafusion_spark::function::map::map_from_arrays::MapFromArrays;
use datafusion_spark::function::map::map_from_entries::MapFromEntries;

use crate::scalar::map::utils::{
    get_element_type, get_list_offsets, get_list_values, map_from_keys_values_offsets_nulls,
    map_type_from_key_value_types,
};

fn map_return_field_with_value_nullability(
    field: FieldRef,
    value_contains_null: Option<bool>,
) -> Result<FieldRef> {
    let Some(nullable) = value_contains_null else {
        return Ok(field);
    };
    let DataType::Map(entries, sorted) = field.data_type() else {
        return internal_err!("expected MAP return type");
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return internal_err!("expected MAP entries");
    };
    let [key, value] = fields.as_ref() else {
        return internal_err!("expected MAP key/value fields");
    };
    let value = Arc::new(value.as_ref().clone().with_nullable(nullable));
    let entries = Arc::new(
        entries
            .as_ref()
            .clone()
            .with_data_type(DataType::Struct(vec![Arc::clone(key), value].into())),
    );
    Ok(Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(DataType::Map(entries, *sorted)),
    ))
}

fn redeclare_map_value_nullability(
    value: ColumnarValue,
    return_type: &DataType,
    value_contains_null: Option<bool>,
) -> Result<ColumnarValue> {
    if value_contains_null.is_none() || value.data_type() == *return_type {
        return Ok(value);
    }
    match value {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast(array.as_ref(), return_type)?)),
        ColumnarValue::Scalar(value) => {
            let array = cast(value.to_array()?.as_ref(), return_type)?;
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                array.as_ref(),
                0,
            )?))
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromArrays {
    delegate: MapFromArrays,
    last_value_wins: bool,
    non_nullable_result: bool,
    value_contains_null: Option<bool>,
}

impl SparkMapFromArrays {
    pub fn new(last_value_wins: bool) -> Self {
        Self {
            delegate: MapFromArrays::new(),
            last_value_wins,
            non_nullable_result: false,
            value_contains_null: None,
        }
    }

    /// SQL MAP constructs both arrays, so its result container cannot be NULL.
    pub fn with_non_nullable_result(mut self) -> Self {
        self.non_nullable_result = true;
        self
    }

    /// Keep resolved value nullability while deriving nested types from live arguments.
    pub fn with_value_contains_null(mut self, value_contains_null: bool) -> Self {
        self.value_contains_null = Some(value_contains_null);
        self
    }
}

impl ScalarUDFImpl for SparkMapFromArrays {
    fn name(&self) -> &str {
        self.delegate.name()
    }

    fn signature(&self) -> &Signature {
        self.delegate.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.delegate.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.delegate.return_field_from_args(args)?;
        let field = if self.non_nullable_result {
            Arc::new(field.as_ref().clone().with_nullable(false))
        } else {
            field
        };
        map_return_field_with_value_nullability(field, self.value_contains_null)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let last_value_wins = self.last_value_wins;
        let value = make_scalar_function(
            move |args| map_from_arrays_inner(args, last_value_wins),
            vec![],
        )(&args.args)?;
        redeclare_map_value_nullability(value, args.return_type(), self.value_contains_null)
    }
}

fn map_from_arrays_inner(args: &[ArrayRef], last_value_wins: bool) -> Result<ArrayRef> {
    let [keys, values] = take_function_args("map_from_arrays", args)?;

    if *keys.data_type() == DataType::Null || *values.data_type() == DataType::Null {
        return Ok(cast(
            &NullArray::new(keys.len()),
            &map_type_from_key_value_types(
                get_element_type(keys.data_type())?,
                get_element_type(values.data_type())?,
            ),
        )?);
    }

    map_from_keys_values_offsets_nulls(
        get_list_values(keys)?,
        get_list_values(values)?,
        &get_list_offsets(keys)?,
        &get_list_offsets(values)?,
        keys.nulls(),
        values.nulls(),
        last_value_wins,
    )
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromEntries {
    delegate: MapFromEntries,
    last_value_wins: bool,
    value_contains_null: Option<bool>,
}

impl SparkMapFromEntries {
    pub fn new(last_value_wins: bool) -> Self {
        Self {
            delegate: MapFromEntries::new(),
            last_value_wins,
            value_contains_null: None,
        }
    }

    /// Keep resolved value nullability while deriving nested types from live arguments.
    pub fn with_value_contains_null(mut self, value_contains_null: bool) -> Self {
        self.value_contains_null = Some(value_contains_null);
        self
    }
}

impl ScalarUDFImpl for SparkMapFromEntries {
    fn name(&self) -> &str {
        self.delegate.name()
    }

    fn signature(&self) -> &Signature {
        self.delegate.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.delegate.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.delegate.return_field_from_args(args)?;
        map_return_field_with_value_nullability(field, self.value_contains_null)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let last_value_wins = self.last_value_wins;
        let value = make_scalar_function(
            move |args| map_from_entries_inner(args, last_value_wins),
            vec![],
        )(&args.args)?;
        redeclare_map_value_nullability(value, args.return_type(), self.value_contains_null)
    }
}

fn map_from_entries_inner(args: &[ArrayRef], last_value_wins: bool) -> Result<ArrayRef> {
    let [entries] = take_function_args("map_from_entries", args)?;
    let entries_offsets = get_list_offsets(entries)?;
    let entries_values = get_list_values(entries)?;

    let (flat_keys, flat_values) = match entries_values.as_any().downcast_ref::<StructArray>() {
        Some(entries) => Ok((entries.column(0), entries.column(1))),
        None => exec_err!(
            "map_from_entries: expected array<struct<key, value>>, got {:?}",
            entries_values.data_type()
        ),
    }?;

    let entries_with_nulls = entries_values.nulls().and_then(|entries_inner_nulls| {
        let mut builder = NullBufferBuilder::new_with_len(0);
        let mut current_offset = entries_offsets
            .first()
            .map(|offset| *offset as usize)
            .unwrap_or(0);

        for next_offset in entries_offsets.iter().skip(1) {
            let entry_count = *next_offset as usize - current_offset;
            builder.append(
                entries_inner_nulls
                    .slice(current_offset, entry_count)
                    .null_count()
                    == 0,
            );
            current_offset = *next_offset as usize;
        }
        builder.finish()
    });

    let result_nulls = NullBuffer::union(entries.nulls(), entries_with_nulls.as_ref());

    map_from_keys_values_offsets_nulls(
        flat_keys,
        flat_values,
        &entries_offsets,
        &entries_offsets,
        None,
        result_nulls.as_ref(),
        last_value_wins,
    )
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn constructed_map_refines_only_outer_nullability() -> Result<()> {
        let value_type = DataType::Struct(
            vec![
                Field::new("item", DataType::Int32, true)
                    .with_metadata([("source".into(), "nullable".into())].into()),
                Field::new("item", DataType::Int64, false),
            ]
            .into(),
        );
        let default = SparkMapFromArrays::new(false);
        let constructed = SparkMapFromArrays::new(false).with_non_nullable_result();
        assert_ne!(default, constructed);
        for (keys_nullable, values_nullable) in [(false, false), (true, false), (false, true)] {
            let fields = [
                Arc::new(Field::new(
                    "keys",
                    DataType::new_list(DataType::Utf8, false),
                    keys_nullable,
                )),
                Arc::new(Field::new(
                    "values",
                    DataType::new_list(value_type.clone(), true),
                    values_nullable,
                )),
            ];
            let field = default.return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[],
            })?;
            assert_eq!(field.is_nullable(), keys_nullable || values_nullable);
            let result = constructed.return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[],
            })?;
            assert_eq!(
                result.as_ref(),
                &field.as_ref().clone().with_nullable(false)
            );
            let DataType::Map(entries, _) = result.data_type() else {
                panic!("expected Map");
            };
            let DataType::Struct(children) = entries.data_type() else {
                panic!("expected entry STRUCT");
            };
            assert_eq!(children[1].data_type(), &value_type);
            assert!(children[1].is_nullable());
        }
        Ok(())
    }
}

#[cfg(test)]
#[test]
fn constructor_nullability_keeps_live_nested_metadata() -> Result<()> {
    use datafusion::arrow::array::{AsArray, Int32Array, ListArray, StringArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::Field;
    use std::collections::HashMap;

    let constructor = SparkMapFromArrays::new(false).with_value_contains_null(false);
    for metadata in [
        HashMap::new(),
        HashMap::from([("source".into(), "plain".into())]),
    ] {
        let child = Arc::new(Field::new("item", DataType::Int32, true).with_metadata(metadata));
        let values: ArrayRef = Arc::new(StructArray::try_new(
            vec![Arc::clone(&child), child].into(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(Int32Array::from(vec![Some(10), Some(20)])),
            ],
            None,
        )?);
        let keys: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::new(StringArray::from(vec!["k", "k"])),
            None,
        )?);
        let value_lists: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::clone(&values),
            None,
        )?);
        let arg_fields = vec![
            Arc::new(Field::new("keys", keys.data_type().clone(), true)),
            Arc::new(Field::new("values", value_lists.data_type().clone(), true)),
        ];
        let returned = constructor.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        })?;
        assert!(returned.is_nullable());
        let constructed = SparkMapFromArrays::new(false)
            .with_non_nullable_result()
            .with_value_contains_null(false)
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &[None, None],
            })?;
        assert_eq!(
            constructed.as_ref(),
            &returned.as_ref().clone().with_nullable(false),
        );
        let DataType::Map(entries, _) = returned.data_type() else {
            return internal_err!("expected MAP declaration");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            return internal_err!("expected MAP entries");
        };
        assert!(!fields[1].is_nullable());
        assert_eq!(fields[1].data_type(), values.data_type());
        let default = SparkMapFromArrays::new(false).return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        })?;
        let DataType::Map(entries, _) = default.data_type() else {
            return internal_err!("expected default MAP declaration");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            return internal_err!("expected default MAP entries");
        };
        assert!(fields[1].is_nullable());
        for scalar in [false, true] {
            let rows = if scalar { 1 } else { 2 };
            let args = if scalar {
                vec![
                    ColumnarValue::Scalar(ScalarValue::try_from_array(keys.as_ref(), 0)?),
                    ColumnarValue::Scalar(ScalarValue::try_from_array(value_lists.as_ref(), 0)?),
                ]
            } else {
                vec![
                    ColumnarValue::Array(Arc::clone(&keys)),
                    ColumnarValue::Array(Arc::clone(&value_lists)),
                ]
            };
            let result = constructor
                .invoke_with_args(ScalarFunctionArgs {
                    args,
                    arg_fields: arg_fields.clone(),
                    number_rows: rows,
                    return_field: Arc::clone(&returned),
                    config_options: Arc::new(Default::default()),
                })?
                .into_array(rows)?;
            assert_eq!(result.data_type(), returned.data_type());
            result.to_data().validate_full()?;
            assert!(result.nulls().is_none());
            assert_eq!(result.as_map().offsets().as_ref(), &[0, 1, 2][..=rows]);
            assert_eq!(
                result.as_map().keys().to_data(),
                StringArray::from(vec!["k", "k"]).slice(0, rows).to_data()
            );
            assert_eq!(
                result.as_map().entries().column(1).to_data(),
                values.slice(0, rows).to_data()
            );
        }
        let duplicate_keys: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(StringArray::from(vec!["k", "k"])),
            None,
        )?);
        let duplicate_values: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::clone(&values),
            None,
        )?);
        let error = constructor
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::clone(&duplicate_keys)),
                    ColumnarValue::Array(Arc::clone(&duplicate_values)),
                ],
                arg_fields: arg_fields.clone(),
                number_rows: 1,
                return_field: Arc::clone(&returned),
                config_options: Arc::new(Default::default()),
            })
            .expect_err("duplicate keys must retain their original error");
        assert!(error.to_string().contains("[DUPLICATED_MAP_KEY]"));
        let last_wins = SparkMapFromArrays::new(true)
            .with_value_contains_null(false)
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(duplicate_keys),
                    ColumnarValue::Array(duplicate_values),
                ],
                arg_fields: arg_fields.clone(),
                number_rows: 1,
                return_field: Arc::clone(&returned),
                config_options: Arc::new(Default::default()),
            })?
            .into_array(1)?;
        last_wins.to_data().validate_full()?;
        assert_eq!(last_wins.data_type(), returned.data_type());
        assert_eq!(last_wins.as_map().offsets().as_ref(), &[0, 1]);
        assert_eq!(
            last_wins.as_map().keys().to_data(),
            StringArray::from(vec!["k"]).to_data()
        );
        assert_eq!(
            last_wins.as_map().values().to_data(),
            values.slice(1, 1).to_data()
        );

        let null_values: ArrayRef = Arc::new(StructArray::try_new(
            values.as_struct().fields().clone(),
            values.as_struct().columns().to_vec(),
            Some(NullBuffer::from(vec![true, false])),
        )?);
        let bad_values: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", null_values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::clone(&null_values),
            None,
        )?);
        let error = constructor
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::clone(&keys)),
                    ColumnarValue::Array(Arc::clone(&bad_values)),
                ],
                arg_fields: arg_fields.clone(),
                number_rows: 2,
                return_field: Arc::clone(&returned),
                config_options: Arc::new(Default::default()),
            })
            .expect_err("required MAP values must reject actual NULLs");
        assert!(
            error
                .to_string()
                .contains("Found unmasked nulls for non-nullable StructArray field")
        );
        let nullable = SparkMapFromArrays::new(false).with_value_contains_null(true);
        let nullable_field = nullable.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None, None],
        })?;
        let nullable_result = nullable
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(keys), ColumnarValue::Array(bad_values)],
                arg_fields,
                number_rows: 2,
                return_field: Arc::clone(&nullable_field),
                config_options: Arc::new(Default::default()),
            })?
            .into_array(2)?;
        nullable_result.to_data().validate_full()?;
        assert_eq!(nullable_result.data_type(), nullable_field.data_type());
        assert_eq!(nullable_result.as_map().offsets().as_ref(), &[0, 1, 2]);
        assert_eq!(
            nullable_result.as_map().values().to_data(),
            null_values.to_data()
        );
    }
    Ok(())
}

#[cfg(test)]
#[test]
fn entries_nullability_keeps_live_metadata_and_guards() -> Result<()> {
    use datafusion::arrow::array::{AsArray, Int32Array, ListArray, StringArray};
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::Field;
    use std::collections::HashMap;

    let child = Arc::new(
        Field::new("item", DataType::Int32, true)
            .with_metadata(HashMap::from([("source".into(), "plain".into())])),
    );
    let values: ArrayRef = Arc::new(StructArray::try_new(
        vec![Arc::clone(&child), child].into(),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None])),
            Arc::new(Int32Array::from(vec![Some(10), Some(20)])),
        ],
        None,
    )?);
    let constructor = SparkMapFromEntries::new(false).with_value_contains_null(false);
    for null_entry in [false, true] {
        let entries = StructArray::try_new(
            vec![
                Arc::new(Field::new("named_key", DataType::Utf8, false)),
                Arc::new(Field::new("named_value", values.data_type().clone(), false)),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(vec!["k", "k"])),
                Arc::clone(&values),
            ],
            null_entry.then(|| NullBuffer::from(vec![true, false])),
        )?;
        let input: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", entries.data_type().clone(), null_entry)),
            OffsetBuffer::new(vec![0, 1, 2].into()),
            Arc::new(entries),
            None,
        )?);
        let arg_fields = vec![Arc::new(Field::new(
            "entries",
            input.data_type().clone(),
            false,
        ))];
        let returned = constructor.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        })?;
        assert_eq!(returned.is_nullable(), null_entry);
        let DataType::Map(entries, _) = returned.data_type() else {
            return internal_err!("expected MAP declaration");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            return internal_err!("expected MAP entries");
        };
        assert!(!fields[1].is_nullable());
        assert_eq!(fields[1].data_type(), values.data_type());
        for scalar in [false, true] {
            let rows = if scalar { 1 } else { 2 };
            let input = if scalar {
                ColumnarValue::Scalar(ScalarValue::try_from_array(input.as_ref(), 0)?)
            } else {
                ColumnarValue::Array(Arc::clone(&input))
            };
            let result = constructor
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![input],
                    arg_fields: arg_fields.clone(),
                    number_rows: rows,
                    return_field: Arc::clone(&returned),
                    config_options: Arc::new(Default::default()),
                })?
                .into_array(rows)?;
            assert_eq!(result.data_type(), returned.data_type());
            result.to_data().validate_full()?;
            let entries = if null_entry && !scalar { 1 } else { rows };
            let offsets = if null_entry { [0, 1, 1] } else { [0, 1, 2] };
            assert_eq!(result.as_map().offsets().as_ref(), &offsets[..=rows]);
            assert_eq!(result.null_count(), usize::from(null_entry && !scalar));
            assert!(!result.is_null(0));
            if rows == 2 {
                assert_eq!(result.is_null(1), null_entry);
            }
            assert_eq!(
                result.as_map().keys().to_data(),
                StringArray::from(vec!["k", "k"])
                    .slice(0, entries)
                    .to_data()
            );
            assert_eq!(
                result.as_map().values().to_data(),
                values.slice(0, entries).to_data()
            );
        }
    }
    for null_key in [false, true] {
        let keys = if null_key {
            StringArray::from(vec![Some("k"), None])
        } else {
            StringArray::from(vec![Some("k"), Some("k")])
        };
        let entries = StructArray::try_new(
            vec![
                Arc::new(Field::new("key", DataType::Utf8, null_key)),
                Arc::new(Field::new("value", values.data_type().clone(), false)),
            ]
            .into(),
            vec![Arc::new(keys), Arc::clone(&values)],
            None,
        )?;
        let input: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new("item", entries.data_type().clone(), false)),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(entries),
            None,
        )?);
        let arg_fields = vec![Arc::new(Field::new(
            "entries",
            input.data_type().clone(),
            false,
        ))];
        let returned = constructor.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &[None],
        })?;
        let error = constructor
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::clone(&input))],
                arg_fields: arg_fields.clone(),
                number_rows: 1,
                return_field: Arc::clone(&returned),
                config_options: Arc::new(Default::default()),
            })
            .expect_err("invalid entries must retain the original key error");
        assert!(error.to_string().contains(if null_key {
            "Found unmasked nulls for non-nullable StructArray field"
        } else {
            "[DUPLICATED_MAP_KEY]"
        }));
        if !null_key {
            let last_wins = SparkMapFromEntries::new(true)
                .with_value_contains_null(false)
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(input)],
                    arg_fields,
                    number_rows: 1,
                    return_field: Arc::clone(&returned),
                    config_options: Arc::new(Default::default()),
                })?
                .into_array(1)?;
            last_wins.to_data().validate_full()?;
            assert_eq!(last_wins.data_type(), returned.data_type());
            assert_eq!(last_wins.as_map().offsets().as_ref(), &[0, 1]);
            assert_eq!(
                last_wins.as_map().keys().to_data(),
                StringArray::from(vec!["k"]).to_data()
            );
            assert_eq!(
                last_wins.as_map().values().to_data(),
                values.slice(1, 1).to_data()
            );
        }
    }
    let error = constructor
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &[],
            scalar_arguments: &[],
        })
        .expect_err("MAP_FROM_ENTRIES must retain its argument count check");
    assert!(
        error
            .to_string()
            .contains("map_from_entries: expected one argument")
    );
    let invalid = Arc::new(Field::new(
        "entries",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Field::new("key", DataType::Utf8, false)].into()),
            false,
        ))),
        false,
    ));
    let error = constructor
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &[invalid],
            scalar_arguments: &[None],
        })
        .expect_err("MAP_FROM_ENTRIES must retain its key/value shape check");
    assert!(
        error
            .to_string()
            .contains("map_from_entries: expected array<struct<key, value>>")
    );
    Ok(())
}
