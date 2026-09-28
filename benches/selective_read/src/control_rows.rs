//! Shared deterministic rows for the public controls and the DAR page-index A/B.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

const PAYLOAD_FILLER: &str = concat!(
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
);

pub fn payload_name(index: usize) -> String {
    format!("payload_{index:03}")
}

pub fn payload_value(row_id: i32, index: usize) -> Option<String> {
    let row = usize::try_from(row_id).ok()?;
    (!(row + index).is_multiple_of(17))
        .then(|| format!("payload-{index:03}-{row_id:08}-{}", PAYLOAD_FILLER))
}

pub fn schema(payload_columns: usize) -> SchemaRef {
    let mut fields = vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("event_id", DataType::Utf8, false),
    ];
    fields.extend((0..payload_columns).map(|j| Field::new(payload_name(j), DataType::Utf8, true)));
    Arc::new(Schema::new(fields))
}

pub fn batch(
    schema: SchemaRef,
    first: usize,
    rows: usize,
    matches: impl Fn(usize) -> bool,
) -> Result<RecordBatch, ArrowError> {
    let ids = (first..first + rows)
        .map(|row| i32::try_from(row).map_err(|e| ArrowError::InvalidArgumentError(e.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let mut columns = vec![
        Arc::new(Int32Array::from(ids.clone())) as ArrayRef,
        Arc::new(StringArray::from_iter_values(
            (first..first + rows).map(|row| if matches(row) { "match" } else { "other" }),
        )),
    ];
    for j in 0..schema.fields().len() - 2 {
        columns.push(Arc::new(StringArray::from_iter(
            ids.iter().map(|id| payload_value(*id, j)),
        )));
    }
    RecordBatch::try_new(schema, columns)
}
