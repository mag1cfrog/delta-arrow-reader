//! Register recorded floating-point inputs in native test sessions.

use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use serde_json::Value;
use std::sync::Arc;

pub(crate) fn register_float_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
    partition_count: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for input in metadata["inputs"].as_array().ok_or("missing inputs")? {
        let rows = input["rows"].as_array().ok_or("missing input rows")?;
        let bits = rows
            .iter()
            .map(|row| {
                row[0]
                    .as_str()
                    .map(|value| u64::from_str_radix(value, 16))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let column: ArrayRef = match input["type"].as_str() {
            Some("float") => Arc::new(Float32Array::from(
                bits.into_iter()
                    .map(|v| v.map(|v| u32::try_from(v).map(f32::from_bits)).transpose())
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Some("double") => Arc::new(Float64Array::from(
                bits.into_iter()
                    .map(|v| v.map(f64::from_bits))
                    .collect::<Vec<_>>(),
            )),
            _ => return Err("unexpected input type".into()),
        };
        let labels = Arc::new(StringArray::from(
            rows.iter().map(|row| row[1].as_str()).collect::<Vec<_>>(),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", column.data_type().clone(), true),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![column, labels])?;
        let mut partitions = vec![Vec::new(); partition_count];
        for (partition, offset) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
            partitions[partition % partition_count]
                .push(batch.slice(offset, batch_size.min(batch.num_rows() - offset)));
        }
        session.register_table(
            input["name"].as_str().ok_or("missing table name")?,
            Arc::new(MemTable::try_new(schema, partitions)?),
        )?;
    }
    Ok(())
}
