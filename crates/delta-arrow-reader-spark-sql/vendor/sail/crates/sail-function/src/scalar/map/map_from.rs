// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, NullArray, NullBufferBuilder, StructArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err};
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

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromArrays {
    delegate: MapFromArrays,
    last_value_wins: bool,
    non_nullable_result: bool,
}

impl SparkMapFromArrays {
    pub fn new(last_value_wins: bool) -> Self {
        Self {
            delegate: MapFromArrays::new(),
            last_value_wins,
            non_nullable_result: false,
        }
    }

    /// SQL MAP constructs both arrays, so its result container cannot be NULL.
    pub fn with_non_nullable_result(mut self) -> Self {
        self.non_nullable_result = true;
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
        if self.non_nullable_result {
            Ok(Arc::new(field.as_ref().clone().with_nullable(false)))
        } else {
            Ok(field)
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let last_value_wins = self.last_value_wins;
        make_scalar_function(
            move |args| map_from_arrays_inner(args, last_value_wins),
            vec![],
        )(&args.args)
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
}

impl SparkMapFromEntries {
    pub fn new(last_value_wins: bool) -> Self {
        Self {
            delegate: MapFromEntries::new(),
            last_value_wins,
        }
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
        self.delegate.return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let last_value_wins = self.last_value_wins;
        make_scalar_function(
            move |args| map_from_entries_inner(args, last_value_wins),
            vec![],
        )(&args.args)
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
        let value_type = DataType::Struct(vec![
            Field::new("item", DataType::Int32, true)
                .with_metadata([("source".into(), "nullable".into())].into()),
            Field::new("item", DataType::Int64, false),
        ].into());
        let default = SparkMapFromArrays::new(false);
        let constructed = SparkMapFromArrays::new(false).with_non_nullable_result();
        assert_ne!(default, constructed);
        for (keys_nullable, values_nullable) in [(false, false), (true, false), (false, true)] {
            let fields = [
                Arc::new(Field::new("keys", DataType::new_list(DataType::Utf8, false), keys_nullable)),
                Arc::new(Field::new("values", DataType::new_list(value_type.clone(), true), values_nullable)),
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
            assert_eq!(result.as_ref(), &field.as_ref().clone().with_nullable(false));
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
