//! Typed JSON rows for complete queries and small preview samples.
//!
//! JSON stdout is emitted only after exhaustion within its budget. File output
//! writes rows incrementally into unpublished storage. Both paths encode each
//! value directly, without allocating an intermediate JSON row.

mod preview;
mod value;

pub(crate) use preview::write_preview_to_stdout;

use std::io::{self, Write};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use futures_util::{Stream, StreamExt};
use serde::Serialize;
use serde_json::Value;

use super::{
    OutputLimits, OutputStats, field_descriptions::field_details, write_on_output_thread,
    write_stdout,
};
use crate::diagnostics::Error;
use value::{JsonRow, validate_json_type_support};

pub(crate) const DEFAULT_MAX_BYTES: usize = 8 * 1024;

pub(super) struct BoundedJsonBuffer {
    pub(super) bytes: Vec<u8>,
    pub(super) max_bytes: u64,
}

impl Write for BoundedJsonBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() as u64 > self.max_bytes.saturating_sub(self.bytes.len() as u64) {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                super::OutputBytesExceeded,
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl BoundedJsonBuffer {
    fn with_rows_header(
        schema: &SchemaRef,
        bindings: &[Value],
        max_bytes: u64,
        command: &'static str,
    ) -> Result<Self, Error> {
        let mut output = Self {
            bytes: Vec::new(),
            max_bytes,
        };
        write_rows_header(&mut output, schema, bindings, command)?;
        Ok(output)
    }

    fn append_bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
        write_json_bytes(self, bytes)
    }

    fn append_json(&mut self, value: &impl Serialize) -> Result<(), Error> {
        write_json_value(self, value)
    }
}

fn write_json_bytes(output: &mut impl Write, bytes: &[u8]) -> Result<(), Error> {
    output.write_all(bytes).map_err(classify_json_write_error)
}

fn write_json_value(output: &mut impl Write, value: &impl Serialize) -> Result<(), Error> {
    serde_json::to_writer(output, value).map_err(|source| {
        if source.is_io() {
            classify_json_write_error(source.into())
        } else {
            Error::JsonOutput { source }
        }
    })
}

fn classify_json_write_error(source: io::Error) -> Error {
    if source
        .get_ref()
        .is_some_and(|error| error.is::<super::OutputBytesExceeded>())
    {
        Error::OutputLimit {
            reason: "json_output_limit_exceeded",
        }
    } else {
        Error::Output { source }
    }
}

fn write_rows_header(
    output: &mut impl Write,
    schema: &SchemaRef,
    bindings: &[Value],
    command: &'static str,
) -> Result<(), Error> {
    for field in schema.fields() {
        validate_json_type_support(field.data_type(), command)?;
    }
    write_json_bytes(
        output,
        b"{\"format_version\":1,\"kind\":\"rows\",\"bindings\":",
    )?;
    write_json_value(output, &bindings)?;
    write_json_bytes(output, b",\"schema\":{\"fields\":[")?;
    for (index, field) in schema.fields().iter().enumerate() {
        if index > 0 {
            write_json_bytes(output, b",")?;
        }
        write_json_value(output, &field_details(field, false))?;
    }
    write_json_bytes(output, b"]},\"rows\":[")
}

pub(crate) async fn write_query_to_stdout(
    schema: SchemaRef,
    stream: impl Stream<Item = Result<RecordBatch, Error>> + Send + 'static,
    bindings: Vec<Value>,
    limits: OutputLimits,
) -> Result<OutputStats, Error> {
    write_on_output_thread(move || async move {
        let (bytes, stats) = write_query(Vec::new(), schema, stream, &bindings, limits).await?;
        write_stdout(&bytes)?;
        Ok(stats)
    })
    .await
}

pub(super) async fn write_query<W: Write>(
    output: W,
    schema: SchemaRef,
    stream: impl Stream<Item = Result<RecordBatch, Error>>,
    bindings: &[Value],
    limits: OutputLimits,
) -> Result<(W, OutputStats), Error> {
    let mut output = super::ByteBudgetWriter {
        inner: output,
        bytes_written: 0,
        max_bytes: Some(limits.max_bytes),
    };
    write_rows_header(&mut output, &schema, bindings, "query")?;
    let mut stream = Box::pin(stream);
    let mut stats = OutputStats::default();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() as u64 > limits.max_rows.saturating_sub(stats.rows_written) {
            return Err(Error::OutputLimit {
                reason: "output_row_limit_exceeded",
            });
        }
        for index in 0..batch.num_rows() {
            if stats.rows_written > 0 {
                write_json_bytes(&mut output, b",")?;
            }
            write_json_value(
                &mut output,
                &JsonRow {
                    columns: batch.columns(),
                    index,
                },
            )?;
            stats.rows_written += 1;
        }
        stats.batches_written += 1;
    }
    write_json_bytes(&mut output, b"],\"row_count\":")?;
    write_json_value(&mut output, &stats.rows_written.to_string())?;
    write_json_bytes(
        &mut output,
        b",\"completion\":\"complete\",\"scope\":\"query\"}\n",
    )?;
    stats.bytes_written = output.bytes_written;
    Ok((output.inner, stats))
}

#[cfg(test)]
mod tests;
