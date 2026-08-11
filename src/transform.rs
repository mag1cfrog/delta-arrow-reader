//! Private physical-to-logical transform service.

use std::sync::Arc;

use arrow::{
    compute::cast,
    datatypes::{DataType, FieldRef, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use snafu::ResultExt;

use crate::{
    DeltaReaderError,
    error::{DataFileReadSnafu, PhysicalToLogicalTransformSnafu},
    planning::{DeltaScanFileTask, DeltaScanPlan},
};

pub(crate) fn align_batch_to_logical_schema(
    batch: RecordBatch,
    logical_schema: &SchemaRef,
    mismatch_message: &'static str,
) -> Result<RecordBatch, DeltaReaderError> {
    if batch.schema().as_ref() == logical_schema.as_ref() {
        return Ok(batch);
    }
    let compatible = batch.num_columns() == logical_schema.fields().len()
        && batch
            .schema()
            .fields()
            .iter()
            .zip(logical_schema.fields())
            .all(|(actual, expected)| {
                actual.name() == expected.name()
                    && actual.is_nullable() == expected.is_nullable()
                    && view_compatible(actual.data_type(), expected.data_type())
            });
    if !compatible {
        return Err(delta_kernel::Error::generic(mismatch_message))
            .boxed()
            .context(DataFileReadSnafu {
                reason: "backend_logical_schema_mismatch",
            });
    }
    let columns = batch
        .columns()
        .iter()
        .zip(logical_schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column.as_ref(), field.data_type())
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .boxed()
        .context(DataFileReadSnafu {
            reason: "backend_logical_schema_mismatch",
        })?;
    RecordBatch::try_new(Arc::clone(logical_schema), columns)
        .boxed()
        .context(DataFileReadSnafu {
            reason: "backend_logical_schema_mismatch",
        })
}

pub(crate) fn schema_with_view_types(schema: &Schema) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(field_with_view_types)
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

pub(crate) fn schema_uses_view_types(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| data_type_uses_view_types(field.data_type()))
}

fn field_with_view_types(field: &FieldRef) -> FieldRef {
    let data_type = match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
        DataType::Binary | DataType::LargeBinary => DataType::BinaryView,
        DataType::Struct(fields) => {
            DataType::Struct(fields.iter().map(field_with_view_types).collect())
        }
        DataType::List(inner) => DataType::List(field_with_view_types(inner)),
        DataType::LargeList(inner) => DataType::LargeList(field_with_view_types(inner)),
        DataType::ListView(inner) => DataType::ListView(field_with_view_types(inner)),
        DataType::LargeListView(inner) => DataType::LargeListView(field_with_view_types(inner)),
        DataType::Map(inner, ordered) => DataType::Map(field_with_view_types(inner), *ordered),
        _ => return Arc::clone(field),
    };
    Arc::new(field.as_ref().clone().with_data_type(data_type))
}

fn data_type_uses_view_types(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8View | DataType::BinaryView => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_uses_view_types(field.data_type())),
        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner)
        | DataType::Map(inner, _) => data_type_uses_view_types(inner.data_type()),
        _ => false,
    }
}

fn view_compatible(actual: &DataType, expected: &DataType) -> bool {
    if actual.equals_datatype(expected) {
        return true;
    }
    match (actual, expected) {
        (DataType::Utf8 | DataType::LargeUtf8, DataType::Utf8View)
        | (DataType::Binary | DataType::LargeBinary, DataType::BinaryView) => true,
        (actual, DataType::Dictionary(_, expected)) => actual.equals_datatype(expected),
        (DataType::Struct(actual), DataType::Struct(expected)) => {
            actual.len() == expected.len()
                && actual.iter().zip(expected).all(|(actual, expected)| {
                    actual.is_nullable() == expected.is_nullable()
                        && view_compatible(actual.data_type(), expected.data_type())
                })
        }
        (DataType::List(actual), DataType::List(expected))
        | (DataType::LargeList(actual), DataType::LargeList(expected))
        | (DataType::ListView(actual), DataType::ListView(expected))
        | (DataType::LargeListView(actual), DataType::LargeListView(expected)) => {
            actual.is_nullable() == expected.is_nullable()
                && view_compatible(actual.data_type(), expected.data_type())
        }
        (DataType::Map(actual, actual_ordered), DataType::Map(expected, expected_ordered)) => {
            actual_ordered == expected_ordered
                && actual.is_nullable() == expected.is_nullable()
                && view_compatible(actual.data_type(), expected.data_type())
        }
        _ => false,
    }
}

#[allow(dead_code)]
impl DeltaScanPlan {
    pub(crate) fn apply_transform(
        &self,
        task: &DeltaScanFileTask,
        batch: RecordBatch,
    ) -> Result<RecordBatch, DeltaReaderError> {
        let physical_rows = batch.num_rows();
        let batch = task
            .transform
            .apply(&self.engine_context, &self.kernel_schemas, batch)
            .boxed()
            .context(PhysicalToLogicalTransformSnafu {
                reason: "kernel_transform_failed",
            })?;

        if batch.num_rows() != physical_rows
            || batch.schema().as_ref() != self.logical_schema.as_ref()
        {
            return Err(delta_kernel::Error::generic("transform_output_mismatch"))
                .boxed()
                .context(PhysicalToLogicalTransformSnafu {
                    reason: "transform_output_mismatch",
                });
        }

        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{Array, DictionaryArray, StringArray, StringViewArray},
        datatypes::{DataType, Field, Schema, UInt16Type},
        record_batch::RecordBatch,
    };

    use super::{align_batch_to_logical_schema, schema_uses_view_types, schema_with_view_types};

    #[test]
    fn view_schema_recurses_and_preserves_schema_contract() {
        let field_metadata = HashMap::from([("field-key".to_owned(), "field-value".to_owned())]);
        let schema_metadata = HashMap::from([("schema-key".to_owned(), "schema-value".to_owned())]);
        let map_entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Binary, true),
                ]
                .into(),
            ),
            false,
        );
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("text", DataType::Utf8, true).with_metadata(field_metadata.clone()),
                Field::new(
                    "nested",
                    DataType::Struct(vec![Field::new("label", DataType::LargeUtf8, true)].into()),
                    true,
                ),
                Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
                    true,
                ),
                Field::new(
                    "properties",
                    DataType::Map(Arc::new(map_entries), false),
                    true,
                ),
            ],
            schema_metadata.clone(),
        );

        let mapped = schema_with_view_types(&schema);
        let expected_map_entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8View, false),
                    Field::new("value", DataType::BinaryView, true),
                ]
                .into(),
            ),
            false,
        );
        assert_eq!(
            mapped.as_ref(),
            &Schema::new_with_metadata(
                vec![
                    Field::new("text", DataType::Utf8View, true).with_metadata(field_metadata),
                    Field::new(
                        "nested",
                        DataType::Struct(
                            vec![Field::new("label", DataType::Utf8View, true)].into(),
                        ),
                        true,
                    ),
                    Field::new(
                        "items",
                        DataType::List(Arc::new(Field::new("item", DataType::BinaryView, true,))),
                        true,
                    ),
                    Field::new(
                        "properties",
                        DataType::Map(Arc::new(expected_map_entries), false),
                        true,
                    ),
                ],
                schema_metadata,
            )
        );
        assert!(schema_uses_view_types(mapped.as_ref()));
        assert!(!schema_uses_view_types(&schema));
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn alignment_accepts_only_explicit_representations_and_fails_closed() {
        let source_schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, true)]));
        let source = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(StringArray::from(vec![Some("safe"), None]))],
        )
        .expect("source batch");
        let view_schema = Arc::new(Schema::new(vec![Field::new(
            "text",
            DataType::Utf8View,
            true,
        )]));
        let aligned =
            align_batch_to_logical_schema(source.clone(), &view_schema, "hostile schema mismatch")
                .expect("Utf8 should align to Utf8View");
        let values = aligned
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("StringView output");
        assert_eq!(values.iter().collect::<Vec<_>>(), [Some("safe"), None]);

        for hostile_schema in [
            Schema::new(vec![Field::new("renamed", DataType::Utf8View, true)]),
            Schema::new(vec![Field::new("text", DataType::Utf8View, false)]),
            Schema::new(vec![Field::new("text", DataType::Int32, true)]),
        ] {
            assert!(
                align_batch_to_logical_schema(
                    source.clone(),
                    &Arc::new(hostile_schema),
                    "hostile schema mismatch",
                )
                .is_err()
            );
        }

        let partition_source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "region",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["west", "west"]))],
        )
        .expect("partition batch");
        let partition_schema = Arc::new(Schema::new(vec![Field::new(
            "region",
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            false,
        )]));
        let partition = align_batch_to_logical_schema(
            partition_source,
            &partition_schema,
            "hostile schema mismatch",
        )
        .expect("partition should dictionary encode");
        let partition = partition
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<UInt16Type>>()
            .expect("UInt16 dictionary");
        assert_eq!(partition.keys().values(), &[0, 0]);
        let partition_values = partition
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 dictionary values");
        assert_eq!(partition_values.iter().collect::<Vec<_>>(), [Some("west")]);

        let too_many_values = (0..=usize::from(u8::MAX) + 1)
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let overflow = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "region",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(too_many_values))],
        )
        .expect("overflow source");
        let undersized_dictionary = Arc::new(Schema::new(vec![Field::new(
            "region",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            false,
        )]));
        assert!(
            align_batch_to_logical_schema(
                overflow,
                &undersized_dictionary,
                "hostile schema mismatch",
            )
            .is_err()
        );
    }
}
