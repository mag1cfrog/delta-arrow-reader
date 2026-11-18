//! Real Delta table with an ordinary stored metadata struct.
use super::reader_support::RealParquetDeltaTable;
use arrow::{
    array::{Array, Int32Array, StringArray, StructArray},
    buffer::NullBuffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use serde_json::json;
use std::sync::Arc;
type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

pub(super) fn stored_metadata_fixture() -> TestResult<RealParquetDeltaTable> {
    let children = vec![Arc::new(Field::new("file_path", DataType::Utf8, true))].into();
    let metadata = StructArray::new(
        children,
        vec![Arc::new(StringArray::from(vec![
            "ordinary-a",
            "hidden-under-null",
            "ordinary-c",
        ]))],
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("_metadata", metadata.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(metadata),
        ],
    )?;
    let mut bytes = vec![];
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    let metadata = json!({"metaData":{"id":"ordinary-metadata-field","format":{"provider":"parquet","options":{}},"schemaString":json!({"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"_metadata","type":{"type":"struct","fields":[{"name":"file_path","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}).to_string(),"partitionColumns":[],"configuration":{},"createdTime":0}});
    RealParquetDeltaTable::new_with_raw_parquet(
        "stored-metadata-field",
        &bytes,
        3,
        &json!({"protocol":{"minReaderVersion":1,"minWriterVersion":2}}),
        &metadata,
    )
}
