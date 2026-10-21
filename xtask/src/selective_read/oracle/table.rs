//! Arrow operations for bounded capability fixtures and page experiments.

use super::{Result, io::Decoder, require};
use arrow_array::{Array, Int64Array, RecordBatch, UInt64Array};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, SchemaRef};
use arrow_select::{concat::concat_batches, take::take};
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

pub(super) fn stream(path: &Path, max_batch_rows: Option<usize>) -> Result<RecordBatch> {
    let mut reader = StreamReader::try_new(File::open(path)?, None)?;
    let schema = reader.schema();
    let batches = reader
        .by_ref()
        .map(|batch| {
            let batch = batch?;
            require(
                max_batch_rows.is_none_or(|max| batch.num_rows() <= max),
                "oversized exported batch",
            )?;
            Ok(batch)
        })
        .collect::<Result<Vec<_>>>()?;
    require(
        reader.get_mut().read(&mut [0])? == 0,
        "trailing bytes after Arrow stream",
    )?;
    Ok(concat_batches(&schema, &batches)?)
}

pub(super) fn parquet(
    decoder: &mut Decoder,
    paths: &[PathBuf],
    columns: &[String],
) -> Result<RecordBatch> {
    let mut reader = decoder.open_paths(paths, columns)?;
    let schema = reader.schema();
    let batches = reader.by_ref().collect::<Result<Vec<_>>>()?;
    reader.finish()?;
    Ok(concat_batches(&schema, &batches)?)
}

pub(super) fn cast(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    require(
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .eq(schema.fields().iter().map(|f| f.name())),
        "output field names/order differ",
    )?;
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let columns = batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(column, field)| {
            require(
                field.is_nullable() || column.null_count() == 0,
                "unexpected null in exported field",
            )?;
            Ok(cast_with_options(column, field.data_type(), &options)?)
        })
        .collect::<Result<_>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

pub(super) fn sort(batch: &RecordBatch, key: &str) -> Result<RecordBatch> {
    let key = batch.column_by_name(key).ok_or("missing sort key")?;
    require(key.null_count() == 0, "null sort key")?;
    let key = cast_with_options(
        key,
        &DataType::Int64,
        &CastOptions {
            safe: false,
            ..Default::default()
        },
    )?;
    let key = key
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("invalid sort key")?;
    let mut indices: Vec<u64> = (0..batch.num_rows() as u64).collect();
    indices.sort_by_key(|&i| key.value(i as usize));
    let indices = UInt64Array::from(indices);
    let columns = batch
        .columns()
        .iter()
        .map(|column| Ok(take(column, &indices, None)?))
        .collect::<Result<_>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

pub(super) fn equal(actual: &RecordBatch, expected: &RecordBatch) -> Result<()> {
    require(
        actual.num_rows() == expected.num_rows()
            && actual
                .schema()
                .fields()
                .iter()
                .map(|f| (f.name(), f.data_type()))
                .eq(expected
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| (f.name(), f.data_type()))),
        "query row count or fields differ",
    )?;
    // ArrayData equality ignores bytes under nulls and compares floats by their bits.
    require(
        actual
            .columns()
            .iter()
            .zip(expected.columns())
            .all(|(a, b)| a.to_data() == b.to_data()),
        "query values differ (floats compared by bits)",
    )
}

pub(super) fn write(path: &Path, batch: &RecordBatch) -> Result<()> {
    let mut writer = StreamWriter::try_new(File::create_new(path)?, &batch.schema())?;
    for offset in (0..batch.num_rows()).step_by(8192) {
        writer.write(&batch.slice(offset, (batch.num_rows() - offset).min(8192)))?;
    }
    writer.finish()?;
    Ok(())
}
