// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, FixedSizeListArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

/// Spark-compatible "rename struct fields by position".
///
/// DataFusion only allows casting between structs that share at least one
/// field name. Spark allows positional casts between structs of matching arity
/// even when no names overlap (e.g. `CAST(struct(x, x) AS struct<f1:T, f2:T>)`).
///
/// This UDF takes a single input column and a target type (struct or
/// list-of-struct, possibly nested) and returns the same underlying values with
/// the field names rewritten positionally to match the target. It is a
/// metadata-only operation when the leaf types already match; if leaf types
/// differ, callers should wrap the result in a regular CAST after the rename.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparkStructRename {
    signature: Signature,
    target_type: DataType,
}

impl SparkStructRename {
    pub fn new(target_type: DataType) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            target_type,
        }
    }
}

impl ScalarUDFImpl for SparkStructRename {
    fn name(&self) -> &str {
        "spark_struct_rename"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.target_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 1 {
            return exec_err!("spark_struct_rename expects exactly one argument");
        }
        Ok(Arc::new(Field::new(self.name(), self.target_type.clone(), args.arg_fields[0].is_nullable())))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("spark_struct_rename expects exactly one argument");
        }
        let arr = ColumnarValue::values_to_arrays(&args.args)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "spark_struct_rename: missing input array".to_string(),
                )
            })?;
        let renamed = rename_positionally(&arr, &self.target_type)?;
        Ok(ColumnarValue::Array(renamed))
    }
}

/// Recursively rebuild `arr` so that any `Struct` field names match those in
/// `target_type` positionally. Leaf data types must already match.
fn rename_positionally(arr: &ArrayRef, target_type: &DataType) -> Result<ArrayRef> {
    match (arr.data_type(), target_type) {
        (DataType::Struct(src_fields), DataType::Struct(tgt_fields)) => {
            if src_fields.len() != tgt_fields.len() {
                return exec_err!(
                    "spark_struct_rename: struct field count mismatch: {} vs {}",
                    src_fields.len(),
                    tgt_fields.len()
                );
            }
            let src = arr.as_struct();
            let new_columns: Vec<ArrayRef> = src
                .columns()
                .iter()
                .zip(tgt_fields.iter())
                .map(|(c, tgt)| rename_positionally(c, tgt.data_type()))
                .collect::<Result<_>>()?;
            let new_fields: Fields = tgt_fields
                .iter()
                .zip(src_fields.iter())
                .zip(new_columns.iter())
                .map(|((tgt, src), col)| {
                    Arc::new(
                        Field::new(tgt.name(), col.data_type().clone(), src.is_nullable())
                            .with_metadata(src.metadata().clone()),
                    )
                })
                .collect();
            let renamed = StructArray::try_new(new_fields, new_columns, src.nulls().cloned())?;
            Ok(Arc::new(renamed))
        }
        (DataType::List(src_field), DataType::List(tgt_field)) => {
            let src = arr.as_list::<i32>();
            let new_values = rename_positionally(src.values(), tgt_field.data_type())?;
            let new_field = Arc::new(
                Field::new(
                    tgt_field.name(),
                    new_values.data_type().clone(),
                    src_field.is_nullable(),
                )
                .with_metadata(src_field.metadata().clone()),
            );
            let renamed = datafusion::arrow::array::GenericListArray::<i32>::try_new(
                new_field,
                src.offsets().clone(),
                new_values,
                src.nulls().cloned(),
            )?;
            Ok(Arc::new(renamed))
        }
        (DataType::LargeList(src_field), DataType::LargeList(tgt_field)) => {
            let src = arr.as_list::<i64>();
            let new_values = rename_positionally(src.values(), tgt_field.data_type())?;
            let new_field = Arc::new(
                Field::new(
                    tgt_field.name(),
                    new_values.data_type().clone(),
                    src_field.is_nullable(),
                )
                .with_metadata(src_field.metadata().clone()),
            );
            let renamed = datafusion::arrow::array::GenericListArray::<i64>::try_new(
                new_field,
                src.offsets().clone(),
                new_values,
                src.nulls().cloned(),
            )?;
            Ok(Arc::new(renamed))
        }
        (
            DataType::FixedSizeList(src_field, src_size),
            DataType::FixedSizeList(tgt_field, tgt_size),
        ) => {
            if src_size != tgt_size {
                return exec_err!(
                    "spark_struct_rename: fixed-size list length mismatch: {} vs {}",
                    src_size,
                    tgt_size
                );
            }
            let src = arr
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "spark_struct_rename: expected FixedSizeListArray".to_string(),
                    )
                })?;
            let new_values = rename_positionally(src.values(), tgt_field.data_type())?;
            let new_field = Arc::new(
                Field::new(
                    tgt_field.name(),
                    new_values.data_type().clone(),
                    src_field.is_nullable(),
                )
                .with_metadata(src_field.metadata().clone()),
            );
            let renamed = FixedSizeListArray::try_new(
                new_field,
                *tgt_size,
                new_values,
                src.nulls().cloned(),
            )?;
            Ok(Arc::new(renamed))
        }
        (DataType::Map(src_entry, src_sorted), DataType::Map(tgt_entry, tgt_sorted)) => {
            if src_sorted != tgt_sorted {
                return exec_err!(
                    "spark_struct_rename: map sortedness mismatch: {} vs {}",
                    src_sorted,
                    tgt_sorted
                );
            }
            // Maps are list-of-struct under the hood; recurse into the entries struct.
            let src = arr.as_map();
            let entries = src.entries();
            let new_entries_arr = rename_positionally(
                &(Arc::new(entries.clone()) as ArrayRef),
                tgt_entry.data_type(),
            )?;
            let new_entries_struct = new_entries_arr
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal(
                        "spark_struct_rename: map entries must be Struct".to_string(),
                    )
                })?;
            let new_entry_field = Arc::new(
                Field::new(
                    tgt_entry.name(),
                    new_entries_struct.data_type().clone(),
                    src_entry.is_nullable(),
                )
                .with_metadata(src_entry.metadata().clone()),
            );
            let renamed = datafusion::arrow::array::MapArray::try_new(
                new_entry_field,
                src.offsets().clone(),
                new_entries_struct.clone(),
                src.nulls().cloned(),
                *tgt_sorted,
            )?;
            Ok(Arc::new(renamed))
        }
        // For leaves and matching-type containers (e.g. primitive arrays under
        // the same type), nothing to rename — return as-is.
        _ => Ok(Arc::clone(arr)),
    }
}

/// Build a target type that has the names from `to` but the data types from
/// `from`. The result is what `SparkStructRename` produces; the subsequent
/// regular CAST then handles any leaf-type conversion.
pub fn build_rename_target_type(from: &DataType, to: &DataType) -> DataType {
    match (from, to) {
        (DataType::Struct(src_fields), DataType::Struct(tgt_fields))
            if src_fields.len() == tgt_fields.len() =>
        {
            let fields: Fields = src_fields
                .iter()
                .zip(tgt_fields.iter())
                .map(|(src, tgt)| {
                    Arc::new(
                        Field::new(
                            tgt.name(),
                            build_rename_target_type(src.data_type(), tgt.data_type()),
                            src.is_nullable(),
                        )
                        .with_metadata(src.metadata().clone()),
                    )
                })
                .collect();
            DataType::Struct(fields)
        }
        (DataType::List(src), DataType::List(tgt)) => DataType::List(Arc::new(
            Field::new(
                tgt.name(),
                build_rename_target_type(src.data_type(), tgt.data_type()),
                src.is_nullable(),
            )
            .with_metadata(src.metadata().clone()),
        )),
        (DataType::LargeList(src), DataType::LargeList(tgt)) => DataType::LargeList(Arc::new(
            Field::new(
                tgt.name(),
                build_rename_target_type(src.data_type(), tgt.data_type()),
                src.is_nullable(),
            )
            .with_metadata(src.metadata().clone()),
        )),
        (DataType::FixedSizeList(src, sa), DataType::FixedSizeList(tgt, _)) => {
            DataType::FixedSizeList(
                Arc::new(
                    Field::new(
                        tgt.name(),
                        build_rename_target_type(src.data_type(), tgt.data_type()),
                        src.is_nullable(),
                    )
                    .with_metadata(src.metadata().clone()),
                ),
                *sa,
            )
        }
        (DataType::Map(src, sorted), DataType::Map(tgt, _)) => DataType::Map(
            Arc::new(
                Field::new(
                    tgt.name(),
                    build_rename_target_type(src.data_type(), tgt.data_type()),
                    src.is_nullable(),
                )
                .with_metadata(src.metadata().clone()),
            ),
            *sorted,
        ),
        // Leaves: keep the source data type unchanged.
        _ => from.clone(),
    }
}


/// Align constructor STRUCT names with Spark's resolver, without coercing values.
/// Choose spelling from the first non-NULL input at each depth; retain each input's leaf types.
pub fn align_constructor_struct_field_names(data_types: &[DataType]) -> Result<Vec<DataType>> {
    fn name_template(data_types: &[&DataType]) -> Result<DataType> {
        let data_types = data_types.iter().copied().filter(|data_type| !data_type.is_null())
            .collect::<Vec<_>>();
        let Some(&first) = data_types.first() else { return Ok(DataType::Null); };
        match first {
            DataType::Struct(first_fields) => {
                let Some(structs) = data_types.iter().map(|data_type| match data_type {
                    DataType::Struct(fields) => Some(fields),
                    _ => None,
                }).collect::<Option<Vec<_>>>() else { return Ok(DataType::Null); };
                for fields in structs.iter().skip(1) {
                    if first_fields.len() != fields.len() || !first_fields.iter().zip(fields.iter())
                        .all(|(left, right)| left.name().eq_ignore_ascii_case(right.name())) {
                        return exec_err!("Expect same keys for struct type but got mismatched pair {:?} and {:?}",
                            first_fields.iter().map(|field| field.name()).collect::<Vec<_>>(),
                            fields.iter().map(|field| field.name()).collect::<Vec<_>>());
                    }
                }
                let fields = first_fields.iter().enumerate().map(|(index, field)| {
                    let children = structs.iter().map(|fields| fields[index].data_type()).collect::<Vec<_>>();
                    Ok(Arc::new(field.as_ref().clone().with_data_type(name_template(&children)?)))
                }).collect::<Result<Vec<_>>>()?;
                Ok(DataType::Struct(fields.into()))
            }
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                let children = data_types.iter().filter_map(|data_type| match data_type {
                    DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => Some(field.data_type()),
                    _ => None,
                }).collect::<Vec<_>>();
                if children.len() != data_types.len() { return Ok(DataType::Null); }
                let child = name_template(&children)?;
                Ok(match first {
                    DataType::List(field) => DataType::List(Arc::new(field.as_ref().clone().with_data_type(child))),
                    DataType::LargeList(field) => DataType::LargeList(Arc::new(field.as_ref().clone().with_data_type(child))),
                    DataType::FixedSizeList(field, size) => DataType::FixedSizeList(Arc::new(field.as_ref().clone().with_data_type(child)), *size),
                    _ => DataType::Null,
                })
            }
            DataType::Map(first_field, sorted) => {
                let children = data_types.iter().filter_map(|data_type| match data_type {
                    DataType::Map(field, _) => Some(field.data_type()),
                    _ => None,
                }).collect::<Vec<_>>();
                if children.len() != data_types.len() { return Ok(DataType::Null); }
                Ok(DataType::Map(Arc::new(first_field.as_ref().clone()
                    .with_data_type(name_template(&children)?)), *sorted))
            }
            _ => Ok(DataType::Null),
        }
    }
    let template = name_template(&data_types.iter().collect::<Vec<_>>())?;
    Ok(data_types.iter().map(|data_type| build_rename_target_type(data_type, &template)).collect())
}
