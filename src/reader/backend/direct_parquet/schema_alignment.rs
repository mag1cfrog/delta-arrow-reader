//! Parquet-to-Arrow schema alignment and recursive batch reshaping.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, ListArray, MapArray, StructArray, new_null_array},
    compute::cast,
    datatypes::{DataType, Field, Fields, SchemaRef},
    record_batch::RecordBatch,
};
use parquet::{
    arrow::PARQUET_FIELD_ID_META_KEY,
    schema::types::{SchemaDescriptor, TypePtr},
};

#[derive(Clone)]
pub(super) struct ParquetSchemaAlignment {
    target_schema: SchemaRef,
    projected_roots: Vec<usize>,
    target_column_plans: Vec<TargetColumnPlan>,
    pub(super) needs_batch_reshape: bool,
}

#[derive(Clone)]
enum TargetColumnPlan {
    ProjectedStreamColumn {
        stream_index: usize,
        field_plan: FieldPlan,
    },
    Null,
}

#[derive(Clone)]
enum FieldPlan {
    Identity,
    Cast {
        target_type: DataType,
    },
    Struct {
        child_plans: Vec<StructChildPlan>,
    },
    List {
        element_plan: Box<FieldPlan>,
    },
    Map {
        key_plan: Box<FieldPlan>,
        value_plan: Box<FieldPlan>,
    },
}

impl FieldPlan {
    fn is_identity(&self) -> bool {
        matches!(self, Self::Identity)
    }
}

#[derive(Clone)]
enum StructChildPlan {
    ProjectedChild {
        child_index: usize,
        field_plan: FieldPlan,
    },
    Null,
}

#[derive(Clone)]
struct RootMatch {
    parquet_root_index: usize,
    field_plan: FieldPlan,
}

impl ParquetSchemaAlignment {
    pub(super) fn projected_roots(&self) -> impl Iterator<Item = usize> + '_ {
        self.projected_roots.iter().copied()
    }

    pub(super) fn reshape_batch_to_target_schema(
        &self,
        batch: RecordBatch,
    ) -> Result<RecordBatch, delta_kernel::Error> {
        let columns = self
            .target_column_plans
            .iter()
            .zip(self.target_schema.fields())
            .map(|(column_plan, field)| match column_plan {
                TargetColumnPlan::ProjectedStreamColumn {
                    stream_index,
                    field_plan,
                } => reshape_array_to_target_field(
                    Arc::clone(batch.column(*stream_index)),
                    field,
                    field_plan,
                ),
                TargetColumnPlan::Null => Ok(new_null_array(field.data_type(), batch.num_rows())),
            })
            .collect::<Result<Vec<ArrayRef>, _>>()?;

        RecordBatch::try_new(Arc::clone(&self.target_schema), columns)
            .map_err(delta_kernel::Error::from)
    }
}

pub(super) fn build_schema_alignment(
    parquet_schema: &SchemaDescriptor,
    parquet_arrow_schema: &SchemaRef,
    target_schema: SchemaRef,
) -> Result<ParquetSchemaAlignment, delta_kernel::Error> {
    let root_matches = target_schema
        .fields()
        .iter()
        .map(|target_field| {
            match_target_field_to_parquet_root(
                target_field,
                parquet_schema.root_schema().get_fields(),
                parquet_arrow_schema,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut projected_roots = root_matches
        .iter()
        .filter_map(|root_match| {
            root_match
                .as_ref()
                .map(|root_match| root_match.parquet_root_index)
        })
        .collect::<Vec<_>>();
    projected_roots.sort_unstable();
    projected_roots.dedup();
    let target_column_plans = root_matches
        .iter()
        .zip(target_schema.fields())
        .map(|(root_match, target_field)| match root_match {
            Some(root_match) => projected_roots
                .iter()
                .position(|root| *root == root_match.parquet_root_index)
                .map(|stream_index| TargetColumnPlan::ProjectedStreamColumn {
                    stream_index,
                    field_plan: root_match.field_plan.clone(),
                })
                .ok_or_else(|| {
                    delta_kernel::Error::generic("matched Parquet root was not projected")
                }),
            None if target_field.is_nullable() => Ok(TargetColumnPlan::Null),
            None => Err(delta_kernel::Error::generic(format!(
                "non-nullable target field '{}' is missing from the Parquet file",
                target_field.name()
            ))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let needs_batch_reshape = target_column_plans
        .iter()
        .zip(target_schema.fields())
        .enumerate()
        .any(
            |(target_index, (column_plan, target_field))| match column_plan {
                TargetColumnPlan::ProjectedStreamColumn {
                    stream_index,
                    field_plan,
                } => {
                    *stream_index != target_index
                        || !field_plan.is_identity()
                        || projected_roots
                            .get(*stream_index)
                            .and_then(|root| parquet_arrow_schema.fields().get(*root))
                            .is_none_or(|file_field| file_field.name() != target_field.name())
                }
                TargetColumnPlan::Null => true,
            },
        );

    Ok(ParquetSchemaAlignment {
        target_schema,
        projected_roots,
        target_column_plans,
        needs_batch_reshape,
    })
}

fn match_target_field_to_parquet_root(
    target_field: &Field,
    parquet_roots: &[TypePtr],
    parquet_arrow_schema: &SchemaRef,
) -> Result<Option<RootMatch>, delta_kernel::Error> {
    if let Some(field_id) = arrow_field_id(target_field)? {
        let matches = parquet_roots
            .iter()
            .enumerate()
            .filter_map(|(index, root)| (parquet_field_id(root) == Some(field_id)).then_some(index))
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [index] => {
                return Ok(Some(RootMatch {
                    parquet_root_index: *index,
                    field_plan: build_matched_field_plan(
                        target_field,
                        parquet_arrow_schema.field(*index),
                        parquet_roots[*index].as_ref(),
                        target_field.name(),
                    )?,
                }));
            }
            [] => {}
            _ => {
                return Err(delta_kernel::Error::generic(format!(
                    "multiple Parquet fields matched target field id {field_id}"
                )));
            }
        }
    }

    let Some((index, file_field)) = parquet_arrow_schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, file_field)| file_field.name() == target_field.name())
    else {
        return Ok(None);
    };

    Ok(Some(RootMatch {
        parquet_root_index: index,
        field_plan: build_matched_field_plan(
            target_field,
            file_field,
            parquet_roots[index].as_ref(),
            target_field.name(),
        )?,
    }))
}

fn build_matched_field_plan(
    target_field: &Field,
    file_field: &Field,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<FieldPlan, delta_kernel::Error> {
    match (target_field.data_type(), file_field.data_type()) {
        (DataType::Struct(target_fields), DataType::Struct(file_fields)) => {
            build_matched_struct_field_plan(
                target_field,
                target_fields,
                file_field,
                file_fields,
                parquet_field,
                path,
            )
        }
        (DataType::List(target_element), DataType::List(file_element)) => {
            build_matched_list_field_plan(
                target_field,
                target_element,
                file_field,
                file_element,
                parquet_field,
                path,
            )
        }
        (DataType::Map(target_entries, target_ordered), DataType::Map(file_entries, _)) => {
            build_matched_map_field_plan(
                target_entries,
                *target_ordered,
                file_field,
                file_entries,
                parquet_field,
                path,
            )
        }
        _ => leaf_cast_plan(target_field.data_type(), file_field.data_type())
            .map(|target_type| match target_type {
                Some(target_type) => FieldPlan::Cast { target_type },
                None => FieldPlan::Identity,
            })
            .map_err(|()| {
                incompatible_parquet_type(path, target_field.data_type(), file_field.data_type())
            }),
    }
}

fn build_matched_map_field_plan(
    target_entries: &Arc<Field>,
    target_ordered: bool,
    file_field: &Field,
    file_entries: &Arc<Field>,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<FieldPlan, delta_kernel::Error> {
    let (target_key, target_value) = map_entry_fields(target_entries, path)?;
    let (file_key, file_value) = map_entry_fields(file_entries, path)?;
    let key_plan = build_matched_field_plan(
        target_key,
        file_key,
        parquet_map_entry_field(parquet_field, path, 0)?,
        &format!("{path}.key"),
    )?;
    let value_plan = build_matched_field_plan(
        target_value,
        file_value,
        parquet_map_entry_field(parquet_field, path, 1)?,
        &format!("{path}.value"),
    )?;
    let target_type = DataType::Map(Arc::clone(target_entries), target_ordered);
    if file_field.data_type() != &target_type
        || !key_plan.is_identity()
        || !value_plan.is_identity()
    {
        Ok(FieldPlan::Map {
            key_plan: Box::new(key_plan),
            value_plan: Box::new(value_plan),
        })
    } else {
        Ok(FieldPlan::Identity)
    }
}

fn map_entry_fields<'a>(
    entries: &'a Field,
    path: &str,
) -> Result<(&'a Field, &'a Field), delta_kernel::Error> {
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(delta_kernel::Error::generic(format!(
            "target field '{path}' expected map entries struct but has type {}",
            entries.data_type()
        )));
    };
    if fields.len() != 2 {
        return Err(delta_kernel::Error::generic(format!(
            "target field '{path}' expected map entries to contain key and value fields but found {}",
            fields.len()
        )));
    }
    Ok((fields[0].as_ref(), fields[1].as_ref()))
}

fn parquet_map_entry_field<'a>(
    parquet_field: &'a parquet::schema::types::Type,
    path: &str,
    entry_index: usize,
) -> Result<&'a parquet::schema::types::Type, delta_kernel::Error> {
    let parquet_children = parquet_field.get_fields();
    let Some(repeated_child) = parquet_children.first() else {
        return Err(delta_kernel::Error::generic(format!(
            "target field '{path}' expected Parquet map entry metadata"
        )));
    };
    if parquet_children.len() != 1 {
        return Err(delta_kernel::Error::generic(format!(
            "target field '{path}' expected one Parquet map entry child but found {}",
            parquet_children.len()
        )));
    }
    let entry_children = repeated_child.get_fields();
    if entry_children.len() != 2 {
        return Err(delta_kernel::Error::generic(format!(
            "target field '{path}' expected Parquet map entry to contain two fields but found {}",
            entry_children.len()
        )));
    }
    entry_children
        .get(entry_index)
        .map(AsRef::as_ref)
        .ok_or_else(|| {
            delta_kernel::Error::generic(format!(
                "target field '{path}' expected Parquet map entry key and value fields"
            ))
        })
}

fn build_matched_list_field_plan(
    target_field: &Field,
    target_element: &Arc<Field>,
    file_field: &Field,
    file_element: &Arc<Field>,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<FieldPlan, delta_kernel::Error> {
    let element_path = format!("{path}.element");
    let element_plan = build_matched_field_plan(
        target_element,
        file_element,
        parquet_list_element_field(parquet_field, path)?,
        &element_path,
    )?;
    if matches!(element_plan, FieldPlan::Cast { .. }) {
        return Err(incompatible_parquet_type(
            &element_path,
            target_element.data_type(),
            file_element.data_type(),
        ));
    }
    if file_field.data_type() != target_field.data_type() || !element_plan.is_identity() {
        Ok(FieldPlan::List {
            element_plan: Box::new(element_plan),
        })
    } else {
        Ok(FieldPlan::Identity)
    }
}

fn parquet_list_element_field<'a>(
    parquet_field: &'a parquet::schema::types::Type,
    path: &str,
) -> Result<&'a parquet::schema::types::Type, delta_kernel::Error> {
    let parquet_children = parquet_field.get_fields();
    let Some(repeated_child) = parquet_children.first() else {
        return Err(delta_kernel::Error::generic(format!(
            "target field '{path}' expected Parquet list element metadata"
        )));
    };
    if parquet_children.len() != 1 {
        return Err(delta_kernel::Error::generic(format!(
            "target field '{path}' expected one Parquet list child but found {}",
            parquet_children.len()
        )));
    }
    let repeated_child_fields = repeated_child.get_fields();
    if repeated_child_fields.len() == 1 {
        Ok(repeated_child_fields[0].as_ref())
    } else {
        Ok(repeated_child.as_ref())
    }
}

fn build_matched_struct_field_plan(
    target_field: &Field,
    target_fields: &Fields,
    file_field: &Field,
    file_fields: &Fields,
    parquet_field: &parquet::schema::types::Type,
    path: &str,
) -> Result<FieldPlan, delta_kernel::Error> {
    let parquet_children = parquet_field.get_fields();
    if parquet_children.len() != file_fields.len() {
        return Err(delta_kernel::Error::generic(format!(
            "target field '{path}' expected Parquet struct field metadata to match Arrow child count"
        )));
    }
    let child_plans = target_fields
        .iter()
        .map(|target_child| {
            match_target_struct_child(
                target_child,
                file_fields,
                parquet_children,
                &format!("{path}.{}", target_child.name()),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let needs_reshape = file_field.data_type() != target_field.data_type()
        || child_plans
            .iter()
            .zip(target_fields.iter())
            .enumerate()
            .any(
                |(target_index, (child_plan, target_child))| match child_plan {
                    StructChildPlan::ProjectedChild {
                        child_index,
                        field_plan,
                    } => {
                        *child_index != target_index
                            || !field_plan.is_identity()
                            || file_fields
                                .get(*child_index)
                                .is_none_or(|file_child| file_child.name() != target_child.name())
                    }
                    StructChildPlan::Null => true,
                },
            );
    if needs_reshape {
        Ok(FieldPlan::Struct { child_plans })
    } else {
        Ok(FieldPlan::Identity)
    }
}

fn match_target_struct_child(
    target_child: &Field,
    file_fields: &Fields,
    parquet_children: &[TypePtr],
    path: &str,
) -> Result<StructChildPlan, delta_kernel::Error> {
    if let Some(field_id) = arrow_field_id(target_child)? {
        let matches = parquet_children
            .iter()
            .enumerate()
            .filter_map(|(index, child)| {
                (parquet_field_id(child) == Some(field_id)).then_some(index)
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [index] => {
                let file_child = file_fields.get(*index).ok_or_else(|| {
                    delta_kernel::Error::generic(format!(
                        "target field '{path}' matched Parquet field id {field_id} without Arrow metadata"
                    ))
                })?;
                return Ok(StructChildPlan::ProjectedChild {
                    child_index: *index,
                    field_plan: build_matched_field_plan(
                        target_child,
                        file_child,
                        parquet_children[*index].as_ref(),
                        path,
                    )?,
                });
            }
            [] => {}
            _ => {
                return Err(delta_kernel::Error::generic(format!(
                    "multiple Parquet fields matched target field id {field_id} at '{path}'"
                )));
            }
        }
    }
    let Some((index, file_child)) = file_fields
        .iter()
        .enumerate()
        .find(|(_, file_child)| file_child.name() == target_child.name())
    else {
        return if target_child.is_nullable() {
            Ok(StructChildPlan::Null)
        } else {
            Err(delta_kernel::Error::generic(format!(
                "non-nullable target field '{path}' is missing from the Parquet file"
            )))
        };
    };
    Ok(StructChildPlan::ProjectedChild {
        child_index: index,
        field_plan: build_matched_field_plan(
            target_child,
            file_child,
            parquet_children[index].as_ref(),
            path,
        )?,
    })
}

fn incompatible_parquet_type(
    path: &str,
    target_type: &DataType,
    file_type: &DataType,
) -> delta_kernel::Error {
    delta_kernel::Error::generic(format!(
        "target field '{path}' expected Parquet type {target_type} but found {file_type}"
    ))
}

fn leaf_cast_plan(target_type: &DataType, file_type: &DataType) -> Result<Option<DataType>, ()> {
    use DataType::{Date32, Decimal128, Float32, Float64, Int8, Int16, Int32, Int64, Timestamp};

    if file_type.equals_datatype(target_type) {
        return Ok(None);
    }
    match (file_type, target_type) {
        (Timestamp(_, _), Timestamp(_, _)) => Ok(Some(target_type.clone())),
        (Int8, Int16 | Int32 | Int64 | Float64) => Ok(Some(target_type.clone())),
        (Int16, Int32 | Int64 | Float64) => Ok(Some(target_type.clone())),
        (Int32, Int64 | Float64) => Ok(Some(target_type.clone())),
        (Float32, Float64) => Ok(Some(target_type.clone())),
        (source_type, Decimal128(precision, scale))
            if can_upcast_to_decimal(source_type, *precision, *scale) =>
        {
            Ok(Some(target_type.clone()))
        }
        (Date32, Timestamp(_, None)) => Ok(Some(target_type.clone())),
        (Int32, Date32) => Ok(Some(target_type.clone())),
        (Int64, Timestamp(arrow::datatypes::TimeUnit::Microsecond, _)) => {
            Ok(Some(target_type.clone()))
        }
        _ => Err(()),
    }
}

fn can_upcast_to_decimal(source_type: &DataType, target_precision: u8, target_scale: i8) -> bool {
    use DataType::{Decimal128, Int8, Int16, Int32, Int64};

    let (source_precision, source_scale) = match source_type {
        Decimal128(precision, scale) => (*precision, *scale),
        Int8 => (3, 0),
        Int16 => (5, 0),
        Int32 => (10, 0),
        Int64 => (20, 0),
        _ => return false,
    };
    target_precision >= source_precision
        && target_scale >= source_scale
        && target_precision - source_precision >= (target_scale - source_scale) as u8
}

fn arrow_field_id(field: &Field) -> Result<Option<i32>, delta_kernel::Error> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .map(|field_id| {
            field_id.parse::<i32>().map_err(|error| {
                delta_kernel::Error::generic(format!(
                    "invalid target field id metadata on '{}': {error}",
                    field.name()
                ))
            })
        })
        .transpose()
}

fn parquet_field_id(parquet_field: &TypePtr) -> Option<i32> {
    let basic_info = parquet_field.get_basic_info();
    basic_info.has_id().then(|| basic_info.id())
}

fn reshape_array_to_target_field(
    array: ArrayRef,
    target_field: &Field,
    field_plan: &FieldPlan,
) -> Result<ArrayRef, delta_kernel::Error> {
    match field_plan {
        FieldPlan::Identity => Ok(array),
        FieldPlan::Cast { target_type } => {
            cast(array.as_ref(), target_type).map_err(delta_kernel::Error::from)
        }
        FieldPlan::Struct { child_plans } => {
            let DataType::Struct(target_fields) = target_field.data_type() else {
                return Err(delta_kernel::Error::generic(format!(
                    "target field '{}' expected struct reshape plan but has type {}",
                    target_field.name(),
                    target_field.data_type()
                )));
            };
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    delta_kernel::Error::generic(format!(
                        "target field '{}' expected Parquet struct array but found {}",
                        target_field.name(),
                        array.data_type()
                    ))
                })?;
            let columns = child_plans
                .iter()
                .zip(target_fields.iter())
                .map(|(child_plan, target_child)| match child_plan {
                    StructChildPlan::ProjectedChild {
                        child_index,
                        field_plan,
                    } => reshape_array_to_target_field(
                        Arc::clone(struct_array.column(*child_index)),
                        target_child,
                        field_plan,
                    ),
                    StructChildPlan::Null => {
                        Ok(new_null_array(target_child.data_type(), struct_array.len()))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(StructArray::new(
                target_fields.clone(),
                columns,
                struct_array.nulls().cloned(),
            )))
        }
        FieldPlan::List { element_plan } => {
            let DataType::List(target_element) = target_field.data_type() else {
                return Err(delta_kernel::Error::generic(format!(
                    "target field '{}' expected list reshape plan but has type {}",
                    target_field.name(),
                    target_field.data_type()
                )));
            };
            let list_array = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                delta_kernel::Error::generic(format!(
                    "target field '{}' expected Parquet list array but found {}",
                    target_field.name(),
                    array.data_type()
                ))
            })?;
            let values = reshape_array_to_target_field(
                Arc::clone(list_array.values()),
                target_element,
                element_plan,
            )?;
            ListArray::try_new(
                Arc::clone(target_element),
                list_array.offsets().clone(),
                values,
                list_array.nulls().cloned(),
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(delta_kernel::Error::from)
        }
        FieldPlan::Map {
            key_plan,
            value_plan,
        } => {
            let DataType::Map(target_entries, target_ordered) = target_field.data_type() else {
                return Err(delta_kernel::Error::generic(format!(
                    "target field '{}' expected map reshape plan but has type {}",
                    target_field.name(),
                    target_field.data_type()
                )));
            };
            let map_array = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                delta_kernel::Error::generic(format!(
                    "target field '{}' expected Parquet map array but found {}",
                    target_field.name(),
                    array.data_type()
                ))
            })?;
            let (target_key, target_value) = map_entry_fields(target_entries, target_field.name())?;
            let keys =
                reshape_array_to_target_field(Arc::clone(map_array.keys()), target_key, key_plan)?;
            let values = reshape_array_to_target_field(
                Arc::clone(map_array.values()),
                target_value,
                value_plan,
            )?;
            let entries = StructArray::new(
                vec![Arc::new(target_key.clone()), Arc::new(target_value.clone())].into(),
                vec![keys, values],
                map_array.entries().nulls().cloned(),
            );
            MapArray::try_new(
                Arc::clone(target_entries),
                map_array.offsets().clone(),
                entries,
                map_array.nulls().cloned(),
                *target_ordered,
            )
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(delta_kernel::Error::from)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs, sync::Arc};

    use arrow::{
        array::{
            Array, ArrayRef, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
            TimestampMicrosecondArray, TimestampNanosecondArray,
        },
        buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use parquet::arrow::{
        ArrowWriter, PARQUET_FIELD_ID_META_KEY, ProjectionMask,
        arrow_reader::ParquetRecordBatchReaderBuilder,
    };

    use super::super::tests::{TestDir, metrics, parquet_bytes_for, reader, task};
    use crate::DeltaScanExecutionOptions;

    fn field_with_id(name: &str, data_type: DataType, nullable: bool, id: i32) -> Field {
        Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            id.to_string(),
        )]))
    }

    fn field_id_metadata(field_id: i32) -> HashMap<String, String> {
        HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_owned(), field_id.to_string())])
    }

    fn struct_field(name: &str, fields: Vec<Field>, nullable: bool) -> Field {
        Field::new(name, DataType::Struct(fields.into()), nullable)
    }

    fn timestamp_us_utc_field(name: &str, nullable: bool) -> Field {
        Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            nullable,
        )
    }

    fn struct_array(fields: Vec<Field>, columns: Vec<ArrayRef>) -> ArrayRef {
        struct_array_with_nulls(fields, columns, None)
    }

    fn struct_array_with_nulls(
        fields: Vec<Field>,
        columns: Vec<ArrayRef>,
        nulls: Option<NullBuffer>,
    ) -> ArrayRef {
        Arc::new(StructArray::new(
            fields.into_iter().map(Arc::new).collect::<Vec<_>>().into(),
            columns,
            nulls,
        ))
    }

    fn list_array(
        element: Field,
        offsets: Vec<i32>,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Result<ArrayRef, Box<dyn std::error::Error>> {
        Ok(Arc::new(ListArray::try_new(
            Arc::new(element),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            values,
            nulls,
        )?))
    }

    fn map_field(name: &str, key: Field, value: Field, nullable: bool) -> Field {
        Field::new(
            name,
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![key, value].into()),
                    false,
                )),
                false,
            ),
            nullable,
        )
    }

    fn map_array(
        key: Field,
        value: Field,
        offsets: Vec<i32>,
        keys: ArrayRef,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Result<ArrayRef, Box<dyn std::error::Error>> {
        let entries = vec![key.clone(), value.clone()].into();
        Ok(Arc::new(MapArray::try_new(
            Arc::new(Field::new("entries", DataType::Struct(entries), false)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            StructArray::new(
                vec![Arc::new(key), Arc::new(value)].into(),
                vec![keys, values],
                None,
            ),
            nulls,
            false,
        )?))
    }

    fn project_parquet_batch_to_target_schema(
        name: &str,
        file_schema: Arc<Schema>,
        columns: Vec<ArrayRef>,
        target_schema: Arc<Schema>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let root = TestDir::new(name)?;
        let file_path = root.path().join("part.parquet");
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), columns)?;
        let mut writer = ArrowWriter::try_new(fs::File::create(&file_path)?, file_schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(file_path)?)?;
        let schema_alignment = super::build_schema_alignment(
            builder.parquet_schema(),
            builder.schema(),
            target_schema,
        )?;
        let projection =
            ProjectionMask::roots(builder.parquet_schema(), schema_alignment.projected_roots());
        let projected = builder
            .with_projection(projection)
            .build()?
            .next()
            .transpose()?
            .ok_or("expected one projected Parquet batch")?;
        Ok(schema_alignment.reshape_batch_to_target_schema(projected)?)
    }

    #[test]
    fn leaf_cast_plan_matches_timestamp_compatibility() -> Result<(), Box<dyn std::error::Error>> {
        let target = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

        assert_eq!(
            super::leaf_cast_plan(&target, &DataType::Timestamp(TimeUnit::Nanosecond, None)),
            Ok(Some(target.clone()))
        );
        assert_eq!(
            super::leaf_cast_plan(
                &target,
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
            ),
            Ok(Some(target))
        );

        Ok(())
    }

    #[test]
    fn leaf_cast_plan_rejects_incompatible_primitive_types()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            super::leaf_cast_plan(&DataType::Int32, &DataType::Utf8),
            Err(())
        );

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_casts_top_level_timestamp_leaf()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_schema = Arc::new(Schema::new(vec![timestamp_us_utc_field("event_ts", true)]));
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let batch = project_parquet_batch_to_target_schema(
            "top-level-timestamp-leaf-cast",
            file_schema,
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_704_067_200_000_000_000),
                None,
            ])) as ArrayRef],
            target_schema,
        )?;
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or("expected TimestampMicrosecondArray")?;

        assert_eq!(timestamps.timezone(), Some("UTC"));
        assert_eq!(timestamps.value(0), 1_704_067_200_000_000);
        assert!(timestamps.is_null(1));

        Ok(())
    }

    #[test]
    fn direct_parquet_reshape_casts_top_level_timestamp_leaf()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_field = timestamp_us_utc_field("event_ts", true);
        let array = Arc::new(TimestampNanosecondArray::from(vec![
            Some(1_704_067_200_000_000_000),
            None,
        ])) as ArrayRef;

        let reshaped = super::reshape_array_to_target_field(
            array,
            &target_field,
            &super::FieldPlan::Cast {
                target_type: target_field.data_type().clone(),
            },
        )?;
        let timestamps = reshaped
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or("expected TimestampMicrosecondArray")?;

        assert_eq!(timestamps.timezone(), Some("UTC"));
        assert_eq!(timestamps.value(0), 1_704_067_200_000_000);
        assert!(timestamps.is_null(1));

        Ok(())
    }

    #[test]
    fn direct_parquet_reshape_casts_nested_struct_timestamp_leaf()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_child = timestamp_us_utc_field("event_ts", true);
        let target_field = struct_field("payload", vec![target_child.clone()], true);
        let file_child = Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        );
        let array = struct_array(
            vec![file_child],
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_704_153_600_000_000_000),
                None,
            ])) as ArrayRef],
        );

        let reshaped = super::reshape_array_to_target_field(
            array,
            &target_field,
            &super::FieldPlan::Struct {
                child_plans: vec![super::StructChildPlan::ProjectedChild {
                    child_index: 0,
                    field_plan: super::FieldPlan::Cast {
                        target_type: target_child.data_type().clone(),
                    },
                }],
            },
        )?;
        let payload = reshaped
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected StructArray")?;
        let timestamps = payload
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or("expected TimestampMicrosecondArray")?;

        assert_eq!(payload.fields()[0].name(), "event_ts");
        assert_eq!(timestamps.timezone(), Some("UTC"));
        assert_eq!(timestamps.value(0), 1_704_153_600_000_000);
        assert!(timestamps.is_null(1));

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_casts_list_struct_leaf()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_element = Field::new(
            "element",
            DataType::Struct(
                vec![
                    Field::new("city", DataType::Utf8, true),
                    Field::new("zip", DataType::Int64, true),
                ]
                .into(),
            ),
            true,
        );
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(target_element)),
            true,
        )]));
        let file_address_fields = vec![
            Field::new("city", DataType::Utf8, true),
            Field::new("zip", DataType::Int32, true),
        ];
        let file_element = Field::new(
            "element",
            DataType::Struct(file_address_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(file_element.clone())),
            true,
        )]));
        let values = struct_array(
            file_address_fields,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
                Arc::new(Int32Array::from(vec![
                    Some(94110),
                    Some(10001),
                    Some(60601),
                ])) as ArrayRef,
            ],
        );
        let addresses = list_array(
            file_element,
            vec![0, 2, 2, 3],
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_target_schema(
            "list-struct-leaf-cast-schema-match",
            file_schema,
            vec![addresses],
            target_schema,
        )?;
        let addresses = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected addresses ListArray")?;
        let values = addresses
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected StructArray list values")?;
        let cities = values
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = values
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("expected Int64Array zip values")?;

        assert_eq!(addresses.value_offsets(), &[0, 2, 2, 3]);
        assert!(addresses.is_valid(0));
        assert!(addresses.is_null(1));
        assert!(addresses.is_valid(2));
        assert_eq!(values.fields()[0].name(), "city");
        assert_eq!(values.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.values(), &[94110, 10001, 60601]);

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_rejects_list_primitive_leaf_cast()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_schema = Arc::new(Schema::new(vec![
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("element", DataType::Int64, true))),
                true,
            ),
            Field::new("id", DataType::Int32, false),
        ]));
        let file_element = Field::new("element", DataType::Int32, true);
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("customer_name", DataType::Utf8, true),
            Field::new("tags", DataType::List(Arc::new(file_element.clone())), true),
        ]));
        let tags = list_array(
            file_element,
            vec![0, 2, 2, 3],
            Arc::new(Int32Array::from(vec![Some(7), Some(11), None])) as ArrayRef,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let error = match project_parquet_batch_to_target_schema(
            "list-primitive-leaf-cast-schema-match",
            file_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob"), None])) as ArrayRef,
                tags,
            ],
            target_schema,
        ) {
            Ok(_) => return Err("primitive list element cast must fail".into()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("tags.element"), "{error}");
        assert!(error.contains("expected Parquet type Int64"), "{error}");
        assert!(error.contains("found Int32"), "{error}");

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_casts_map_key_leaf() -> Result<(), Box<dyn std::error::Error>>
    {
        let target_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, true),
            true,
        )]));
        let file_key = Field::new("key", DataType::Int32, false);
        let file_value = Field::new("value", DataType::Utf8, true);
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "attributes",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(vec![file_key.clone(), file_value.clone()].into()),
                    false,
                )),
                false,
            ),
            true,
        )]));
        let attributes = map_array(
            file_key,
            file_value,
            vec![0, 2, 2, 3],
            Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("mailing"),
            ])) as ArrayRef,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_target_schema(
            "map-key-leaf-cast-schema-match",
            file_schema,
            vec![attributes],
            target_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("expected Int64Array map keys")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected StringArray map values")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_valid(0));
        assert!(attributes.is_null(1));
        assert!(attributes.is_valid(2));
        assert_eq!(keys.values(), &[10, 20, 30]);
        assert_eq!(values.value(0), "home");
        assert_eq!(values.value(2), "mailing");

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_recurses_by_nested_field_id_before_names()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_profile_fields = vec![
            Field::new("first_name", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("age", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let target_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            target_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![
            Field::new("stale_age", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_name", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array_with_nulls(
            file_profile_fields,
            vec![
                Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])) as ArrayRef,
            ],
            Some(NullBuffer::from(vec![true, false])),
        );

        let batch = project_parquet_batch_to_target_schema(
            "nested-field-id-schema-match",
            file_schema,
            vec![profile],
            target_schema,
        )?;
        let profile = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected profile StructArray")?;
        let names = profile
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected first_name StringArray")?;
        let ages = profile
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected age Int32Array")?;

        assert_eq!(profile.fields()[0].name(), "first_name");
        assert_eq!(profile.fields()[1].name(), "age");
        assert!(profile.is_valid(0));
        assert!(profile.is_null(1));
        assert_eq!(names.value(0), "alice");
        assert_eq!(ages.value(0), 34);

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_reshapes_list_struct_elements_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_address_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let target_element =
            Field::new("item", DataType::Struct(target_address_fields.into()), true);
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(target_element)),
            true,
        )]));
        let file_address_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_element = Field::new(
            "item",
            DataType::Struct(file_address_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(file_element.clone())),
            true,
        )]));
        let values = struct_array(
            file_address_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let addresses = list_array(
            file_element,
            vec![0, 2, 2, 3],
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_target_schema(
            "list-struct-field-id-schema-match",
            file_schema,
            vec![addresses],
            target_schema,
        )?;
        let addresses = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected addresses ListArray")?;
        let values = addresses
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected address element StructArray")?;
        let cities = values
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = values
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(addresses.value_offsets(), &[0, 2, 2, 3]);
        assert!(addresses.is_valid(0));
        assert!(addresses.is_null(1));
        assert!(addresses.is_valid(2));
        assert_eq!(values.fields()[0].name(), "city");
        assert_eq!(values.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_recurses_by_local_nested_name_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_profile_fields = vec![
            Field::new("age", DataType::Int32, true),
            Field::new("first_name", DataType::Utf8, true),
        ];
        let target_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            target_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![
            Field::new("first_name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
        ];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array(
            file_profile_fields,
            vec![
                Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])) as ArrayRef,
                Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef,
            ],
        );

        let batch = project_parquet_batch_to_target_schema(
            "nested-name-fallback-schema-match",
            file_schema,
            vec![profile],
            target_schema,
        )?;
        let profile = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected profile StructArray")?;
        let ages = profile
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected age Int32Array")?;
        let names = profile
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected first_name StringArray")?;

        assert_eq!(profile.fields()[0].name(), "age");
        assert_eq!(profile.fields()[1].name(), "first_name");
        assert_eq!(ages.values(), &[34, 41]);
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_null_fills_missing_nullable_nested_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_profile_fields = vec![
            Field::new("age", DataType::Int32, true),
            Field::new("loyalty_tier", DataType::Utf8, true),
        ];
        let target_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            target_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![Field::new("age", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array(
            file_profile_fields,
            vec![Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef],
        );

        let batch = project_parquet_batch_to_target_schema(
            "nested-missing-nullable-schema-match",
            file_schema,
            vec![profile],
            target_schema,
        )?;
        let profile = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected profile StructArray")?;
        let loyalty_tiers = profile
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected loyalty_tier StringArray")?;

        assert_eq!(profile.fields()[1].name(), "loyalty_tier");
        assert_eq!(loyalty_tiers.len(), 2);
        assert_eq!(loyalty_tiers.null_count(), 2);

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_null_fills_missing_nullable_list_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_address_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("country", DataType::Utf8, true),
        ];
        let target_element =
            Field::new("item", DataType::Struct(target_address_fields.into()), true);
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(target_element)),
            true,
        )]));
        let file_address_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_element = Field::new(
            "item",
            DataType::Struct(file_address_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(file_element.clone())),
            true,
        )]));
        let values = struct_array(
            file_address_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001, 60601, 85001, 73301])) as ArrayRef],
        );
        let addresses = list_array(
            file_element,
            vec![0, 2, 2, 5],
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_target_schema(
            "list-struct-missing-nullable-schema-match",
            file_schema,
            vec![addresses],
            target_schema,
        )?;
        let addresses = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected addresses ListArray")?;
        let values = addresses
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected address element StructArray")?;
        let countries = values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected country StringArray")?;

        assert_eq!(addresses.value_offsets(), &[0, 2, 2, 5]);
        assert!(addresses.is_null(1));
        assert_eq!(values.fields()[1].name(), "country");
        assert_eq!(countries.len(), 5);
        assert_eq!(countries.null_count(), 5);

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_rejects_missing_non_nullable_list_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_address_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("required_country", DataType::Utf8, false),
        ];
        let target_element =
            Field::new("item", DataType::Struct(target_address_fields.into()), true);
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(target_element)),
            true,
        )]));
        let file_address_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_element = Field::new(
            "item",
            DataType::Struct(file_address_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "addresses",
            DataType::List(Arc::new(file_element.clone())),
            true,
        )]));
        let values = struct_array(
            file_address_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001])) as ArrayRef],
        );
        let addresses = list_array(file_element, vec![0, 2], values, None)?;
        let error = match project_parquet_batch_to_target_schema(
            "list-struct-missing-required-schema-match",
            file_schema,
            vec![addresses],
            target_schema,
        ) {
            Ok(_) => return Err("missing non-nullable list struct child must fail".into()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("non-nullable target field"), "{error}");
        assert!(
            error.contains("addresses.element.required_country"),
            "{error}"
        );
        assert!(
            error.contains("is missing from the Parquet file"),
            "{error}"
        );

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_reshapes_map_key_struct_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_key_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let target_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Struct(target_key_fields.into()), false),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let file_key_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new(
                "keys",
                DataType::Struct(file_key_fields.clone().into()),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let keys = struct_array(
            file_key_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let values = Arc::new(StringArray::from(vec![
            Some("home"),
            Some("work"),
            Some("other"),
        ])) as ArrayRef;
        let attributes = map_array(
            Field::new(
                "keys",
                DataType::Struct(
                    vec![
                        Field::new("stale_zip", DataType::Int32, true)
                            .with_metadata(field_id_metadata(10)),
                        Field::new("stale_city", DataType::Utf8, true)
                            .with_metadata(field_id_metadata(11)),
                    ]
                    .into(),
                ),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            vec![0, 2, 2, 3],
            keys,
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_target_schema(
            "map-key-struct-field-id-schema-match",
            file_schema,
            vec![attributes],
            target_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map key StructArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected map value StringArray")?;
        let cities = keys
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = keys
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_valid(0));
        assert!(attributes.is_null(1));
        assert!(attributes.is_valid(2));
        assert_eq!(keys.fields()[0].name(), "city");
        assert_eq!(keys.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);
        assert_eq!(values.value(0), "home");
        assert_eq!(values.value(2), "other");

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_null_fills_missing_nullable_map_key_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_key_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("country", DataType::Utf8, true),
        ];
        let target_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Struct(target_key_fields.into()), false),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let file_key_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new(
                "keys",
                DataType::Struct(file_key_fields.clone().into()),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let keys = struct_array(
            file_key_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001, 60601, 85001, 73301])) as ArrayRef],
        );
        let attributes = map_array(
            Field::new(
                "keys",
                DataType::Struct(vec![Field::new("zip", DataType::Int32, true)].into()),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            vec![0, 2, 2, 5],
            keys,
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
                Some("billing"),
                Some("shipping"),
            ])) as ArrayRef,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_target_schema(
            "map-key-struct-missing-nullable-schema-match",
            file_schema,
            vec![attributes],
            target_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map key StructArray")?;
        let countries = keys
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected country StringArray")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 5]);
        assert!(attributes.is_null(1));
        assert_eq!(keys.fields()[1].name(), "country");
        assert_eq!(countries.len(), 5);
        assert_eq!(countries.null_count(), 5);

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_rejects_missing_non_nullable_map_key_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_key_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("required_country", DataType::Utf8, false),
        ];
        let target_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Struct(target_key_fields.into()), false),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let file_key_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new(
                "keys",
                DataType::Struct(file_key_fields.clone().into()),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let keys = struct_array(
            file_key_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001])) as ArrayRef],
        );
        let attributes = map_array(
            Field::new(
                "keys",
                DataType::Struct(vec![Field::new("zip", DataType::Int32, true)].into()),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            vec![0, 2],
            keys,
            Arc::new(StringArray::from(vec![Some("home"), Some("work")])) as ArrayRef,
            None,
        )?;
        let error = match project_parquet_batch_to_target_schema(
            "map-key-struct-missing-required-schema-match",
            file_schema,
            vec![attributes],
            target_schema,
        ) {
            Ok(_) => return Err("missing non-nullable map key struct child must fail".into()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("non-nullable target field"), "{error}");
        assert!(error.contains("attributes.key.required_country"), "{error}");
        assert!(
            error.contains("is missing from the Parquet file"),
            "{error}"
        );

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_reshapes_map_list_key_struct_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_element_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let target_element =
            Field::new("item", DataType::Struct(target_element_fields.into()), true);
        let target_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::List(Arc::new(target_element)), false),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let file_element_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_element = Field::new(
            "item",
            DataType::Struct(file_element_fields.clone().into()),
            true,
        );
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new(
                "keys",
                DataType::List(Arc::new(file_element.clone())),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let key_elements = struct_array(
            file_element_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let keys = list_array(file_element, vec![0, 2, 2, 3], key_elements, None)?;
        let attributes = map_array(
            Field::new(
                "keys",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(
                        vec![
                            Field::new("stale_zip", DataType::Int32, true)
                                .with_metadata(field_id_metadata(10)),
                            Field::new("stale_city", DataType::Utf8, true)
                                .with_metadata(field_id_metadata(11)),
                        ]
                        .into(),
                    ),
                    true,
                ))),
                false,
            ),
            Field::new("values", DataType::Utf8, true),
            vec![0, 2, 2, 3],
            keys,
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
            ])) as ArrayRef,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_target_schema(
            "map-list-key-struct-field-id-schema-match",
            file_schema,
            vec![attributes],
            target_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or("expected map key ListArray")?;
        let key_elements = keys
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected key element StructArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected map value StringArray")?;
        let cities = key_elements
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = key_elements
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_valid(0));
        assert!(attributes.is_null(1));
        assert!(attributes.is_valid(2));
        assert_eq!(keys.value_offsets(), &[0, 2, 2, 3]);
        assert_eq!(key_elements.fields()[0].name(), "city");
        assert_eq!(key_elements.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);
        assert_eq!(values.value(0), "home");
        assert_eq!(values.value(2), "other");

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_reshapes_nested_map_key_struct_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_inner_key_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let target_inner_key = Field::new(
            "keys",
            DataType::Struct(target_inner_key_fields.into()),
            false,
        );
        let target_outer_key = map_field(
            "keys",
            target_inner_key,
            Field::new("values", DataType::Int32, true),
            false,
        );
        let target_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            target_outer_key,
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let file_inner_key_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_inner_key = Field::new(
            "keys",
            DataType::Struct(file_inner_key_fields.clone().into()),
            false,
        );
        let file_outer_key = map_field(
            "keys",
            file_inner_key.clone(),
            Field::new("values", DataType::Int32, true),
            false,
        );
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            file_outer_key.clone(),
            Field::new("values", DataType::Utf8, true),
            true,
        )]));
        let inner_keys = struct_array(
            file_inner_key_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let outer_keys = map_array(
            file_inner_key,
            Field::new("values", DataType::Int32, true),
            vec![0, 2, 2, 3],
            inner_keys,
            Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef,
            None,
        )?;
        let attributes = map_array(
            file_outer_key,
            Field::new("values", DataType::Utf8, true),
            vec![0, 2, 2, 3],
            outer_keys,
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
            ])) as ArrayRef,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_target_schema(
            "nested-map-key-struct-field-id-schema-match",
            file_schema,
            vec![attributes],
            target_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let outer_keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected outer key MapArray")?;
        let inner_keys = outer_keys
            .keys()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected inner key StructArray")?;
        let outer_values = attributes
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected outer value StringArray")?;
        let inner_values = outer_keys
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected inner value Int32Array")?;
        let cities = inner_keys
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = inner_keys
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert_eq!(outer_keys.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_null(1));
        assert_eq!(inner_keys.fields()[0].name(), "city");
        assert_eq!(inner_keys.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);
        assert_eq!(inner_values.value(0), 7);
        assert_eq!(inner_values.value(2), 9);
        assert_eq!(outer_values.value(0), "home");
        assert_eq!(outer_values.value(2), "other");

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_reshapes_map_key_and_value_structs_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_key_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let target_value_fields = vec![
            Field::new("label", DataType::Utf8, true).with_metadata(field_id_metadata(21)),
            Field::new("score", DataType::Int32, true).with_metadata(field_id_metadata(20)),
        ];
        let target_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Struct(target_key_fields.into()), false),
            Field::new("values", DataType::Struct(target_value_fields.into()), true),
            true,
        )]));
        let file_key_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_value_fields = vec![
            Field::new("stale_score", DataType::Int32, true).with_metadata(field_id_metadata(20)),
            Field::new("stale_label", DataType::Utf8, true).with_metadata(field_id_metadata(21)),
        ];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new(
                "keys",
                DataType::Struct(file_key_fields.clone().into()),
                false,
            ),
            Field::new(
                "values",
                DataType::Struct(file_value_fields.clone().into()),
                true,
            ),
            true,
        )]));
        let keys = struct_array(
            file_key_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let values = struct_array(
            file_value_fields,
            vec![
                Arc::new(Int32Array::from(vec![7, 8, 9])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("home"),
                    Some("work"),
                    Some("other"),
                ])) as ArrayRef,
            ],
        );
        let attributes = map_array(
            Field::new(
                "keys",
                DataType::Struct(
                    vec![
                        Field::new("stale_zip", DataType::Int32, true)
                            .with_metadata(field_id_metadata(10)),
                        Field::new("stale_city", DataType::Utf8, true)
                            .with_metadata(field_id_metadata(11)),
                    ]
                    .into(),
                ),
                false,
            ),
            Field::new(
                "values",
                DataType::Struct(
                    vec![
                        Field::new("stale_score", DataType::Int32, true)
                            .with_metadata(field_id_metadata(20)),
                        Field::new("stale_label", DataType::Utf8, true)
                            .with_metadata(field_id_metadata(21)),
                    ]
                    .into(),
                ),
                true,
            ),
            vec![0, 2, 2, 3],
            keys,
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_target_schema(
            "map-key-and-value-struct-field-id-schema-match",
            file_schema,
            vec![attributes],
            target_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map key StructArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map value StructArray")?;
        let cities = keys
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = keys
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;
        let labels = values
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected label StringArray")?;
        let scores = values
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected score Int32Array")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_null(1));
        assert_eq!(keys.fields()[0].name(), "city");
        assert_eq!(keys.fields()[1].name(), "zip");
        assert_eq!(values.fields()[0].name(), "label");
        assert_eq!(values.fields()[1].name(), "score");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(labels.value(0), "home");
        assert_eq!(scores.value(0), 7);
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(2), 60601);
        assert_eq!(labels.value(2), "other");
        assert_eq!(scores.value(2), 9);

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_reshapes_map_value_struct_by_field_id()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_value_fields = vec![
            Field::new("city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
            Field::new("zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
        ];
        let target_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Struct(target_value_fields.into()), true),
            true,
        )]));
        let file_value_fields = vec![
            Field::new("stale_zip", DataType::Int32, true).with_metadata(field_id_metadata(10)),
            Field::new("stale_city", DataType::Utf8, true).with_metadata(field_id_metadata(11)),
        ];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(file_value_fields.clone().into()),
                true,
            ),
            true,
        )]));
        let values = struct_array(
            file_value_fields,
            vec![
                Arc::new(Int32Array::from(vec![94110, 10001, 60601])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("san francisco"),
                    Some("new york"),
                    Some("chicago"),
                ])) as ArrayRef,
            ],
        );
        let attributes = map_array(
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(
                    vec![
                        Field::new("stale_zip", DataType::Int32, true)
                            .with_metadata(field_id_metadata(10)),
                        Field::new("stale_city", DataType::Utf8, true)
                            .with_metadata(field_id_metadata(11)),
                    ]
                    .into(),
                ),
                true,
            ),
            vec![0, 2, 2, 3],
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
            ])) as ArrayRef,
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_target_schema(
            "map-value-struct-field-id-schema-match",
            file_schema,
            vec![attributes],
            target_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let keys = attributes
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected map key StringArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map value StructArray")?;
        let cities = values
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected city StringArray")?;
        let zips = values
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("expected zip Int32Array")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 3]);
        assert!(attributes.is_valid(0));
        assert!(attributes.is_null(1));
        assert!(attributes.is_valid(2));
        assert_eq!(keys.value(0), "home");
        assert_eq!(keys.value(2), "other");
        assert_eq!(values.fields()[0].name(), "city");
        assert_eq!(values.fields()[1].name(), "zip");
        assert_eq!(cities.value(0), "san francisco");
        assert_eq!(cities.value(2), "chicago");
        assert_eq!(zips.value(0), 94110);
        assert_eq!(zips.value(2), 60601);

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_null_fills_missing_nullable_map_value_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_value_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("country", DataType::Utf8, true),
        ];
        let target_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Struct(target_value_fields.into()), true),
            true,
        )]));
        let file_value_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(file_value_fields.clone().into()),
                true,
            ),
            true,
        )]));
        let values = struct_array(
            file_value_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001, 60601, 85001, 73301])) as ArrayRef],
        );
        let attributes = map_array(
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(vec![Field::new("zip", DataType::Int32, true)].into()),
                true,
            ),
            vec![0, 2, 2, 5],
            Arc::new(StringArray::from(vec![
                Some("home"),
                Some("work"),
                Some("other"),
                Some("billing"),
                Some("shipping"),
            ])) as ArrayRef,
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;

        let batch = project_parquet_batch_to_target_schema(
            "map-value-struct-missing-nullable-schema-match",
            file_schema,
            vec![attributes],
            target_schema,
        )?;
        let attributes = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or("expected attributes MapArray")?;
        let values = attributes
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or("expected map value StructArray")?;
        let countries = values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("expected country StringArray")?;

        assert_eq!(attributes.value_offsets(), &[0, 2, 2, 5]);
        assert!(attributes.is_null(1));
        assert_eq!(values.fields()[1].name(), "country");
        assert_eq!(countries.len(), 5);
        assert_eq!(countries.null_count(), 5);

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_rejects_missing_non_nullable_map_value_struct_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_value_fields = vec![
            Field::new("zip", DataType::Int32, true),
            Field::new("required_country", DataType::Utf8, false),
        ];
        let target_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Struct(target_value_fields.into()), true),
            true,
        )]));
        let file_value_fields = vec![Field::new("zip", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![map_field(
            "attributes",
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(file_value_fields.clone().into()),
                true,
            ),
            true,
        )]));
        let values = struct_array(
            file_value_fields,
            vec![Arc::new(Int32Array::from(vec![94110, 10001])) as ArrayRef],
        );
        let attributes = map_array(
            Field::new("keys", DataType::Utf8, false),
            Field::new(
                "values",
                DataType::Struct(vec![Field::new("zip", DataType::Int32, true)].into()),
                true,
            ),
            vec![0, 2],
            Arc::new(StringArray::from(vec![Some("home"), Some("work")])) as ArrayRef,
            values,
            None,
        )?;
        let error = match project_parquet_batch_to_target_schema(
            "map-value-struct-missing-required-schema-match",
            file_schema,
            vec![attributes],
            target_schema,
        ) {
            Ok(_) => return Err("missing non-nullable map value struct child must fail".into()),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("non-nullable target field"), "{error}");
        assert!(
            error.contains("attributes.value.required_country"),
            "{error}"
        );
        assert!(
            error.contains("is missing from the Parquet file"),
            "{error}"
        );

        Ok(())
    }

    #[test]
    fn direct_parquet_schema_alignment_rejects_missing_non_nullable_nested_child()
    -> Result<(), Box<dyn std::error::Error>> {
        let target_profile_fields = vec![
            Field::new("age", DataType::Int32, true),
            Field::new("required_code", DataType::Utf8, false),
        ];
        let target_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            target_profile_fields,
            true,
        )]));
        let file_profile_fields = vec![Field::new("age", DataType::Int32, true)];
        let file_schema = Arc::new(Schema::new(vec![struct_field(
            "profile",
            file_profile_fields.clone(),
            true,
        )]));
        let profile = struct_array(
            file_profile_fields,
            vec![Arc::new(Int32Array::from(vec![34, 41])) as ArrayRef],
        );
        let error = match project_parquet_batch_to_target_schema(
            "nested-missing-required-schema-match",
            file_schema,
            vec![profile],
            target_schema,
        ) {
            Ok(_) => return Err("missing nested required child must fail".into()),
            Err(error) => error,
        };
        let display = error.to_string();

        assert!(display.contains("non-nullable target field"), "{display}");
        assert!(display.contains("profile.required_code"), "{display}");
        assert!(
            display.contains("is missing from the Parquet file"),
            "{display}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn matches_top_level_fields_by_id_reorders_casts_and_null_fills()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-top-level-schema-match")?;
        let file_schema = Arc::new(Schema::new(vec![
            field_with_id("stale_name", DataType::Utf8, true, 2),
            field_with_id("stale_id", DataType::Int32, false, 1),
        ]));
        let bytes = parquet_bytes_for(
            file_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let target_schema = Arc::new(Schema::new(vec![
            field_with_id("id", DataType::Int64, false, 1),
            field_with_id("name", DataType::Utf8, true, 2),
            Field::new("added", DataType::Utf8, true),
        ]));
        let reader = reader(&root, DeltaScanExecutionOptions::new(), metrics())?;
        let task = task("part.parquet", Some(u64::try_from(bytes.len())?))?;
        let mut stream = reader
            .open_physical_parquet_stream(&task, target_schema, None, None, None, false)
            .await?;
        let batch = stream.next_batch().await?.ok_or("expected one batch")?;

        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name", "added"]
        );
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("expected cast Int64Array")?
                .values(),
            &[1, 2, 3]
        );
        assert_eq!(batch.column(2).null_count(), 3);
        assert!(stream.next_batch().await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn casts_top_level_timestamp_and_rejects_incompatible_or_missing_required_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = TestDir::new("direct-top-level-casts")?;
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let bytes = parquet_bytes_for(
            file_schema,
            vec![Arc::new(TimestampNanosecondArray::from(vec![
                Some(1_704_067_200_000_000_000),
                None,
            ]))],
        )?;
        fs::write(root.path().join("part.parquet"), &bytes)?;
        let reader = reader(&root, DeltaScanExecutionOptions::new(), metrics())?;
        let task = task("part.parquet", Some(u64::try_from(bytes.len())?))?;
        let timestamp_schema = Arc::new(Schema::new(vec![Field::new(
            "event_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        )]));
        let mut stream = reader
            .open_physical_parquet_stream(&task, timestamp_schema, None, None, None, false)
            .await?;
        let batch = stream.next_batch().await?.ok_or("expected one batch")?;
        let timestamps = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or("expected TimestampMicrosecondArray")?;
        assert_eq!(timestamps.timezone(), Some("UTC"));
        assert_eq!(timestamps.value(0), 1_704_067_200_000_000);
        assert!(timestamps.is_null(1));

        for target_schema in [
            Arc::new(Schema::new(vec![Field::new(
                "event_ts",
                DataType::Utf8,
                true,
            )])),
            Arc::new(Schema::new(vec![Field::new(
                "required",
                DataType::Int32,
                false,
            )])),
        ] {
            let error = match reader
                .open_physical_parquet_stream(&task, target_schema, None, None, None, false)
                .await
            {
                Ok(_) => return Err("unsupported schema must fail".into()),
                Err(error) => error,
            };
            assert_eq!(error.code(), "data_file_read");
            assert_eq!(
                error.to_string(),
                "delta reader error: phase=data_file_read code=data_file_read reason=parquet_schema_match_failed"
            );
        }
        Ok(())
    }
}
