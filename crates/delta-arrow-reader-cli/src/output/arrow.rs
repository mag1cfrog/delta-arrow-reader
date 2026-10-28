//! Arrow IPC output on a dedicated thread, with optional complete-result ceilings.

use std::{
    io::{self, Write},
    time::Duration,
};

use arrow::{array::RecordBatch, ipc::writer::StreamWriter};
use arrow_schema::SchemaRef;
use futures_util::{Stream, StreamExt};
use snafu::ResultExt;

use crate::diagnostics::{Error, OutputSnafu};

use super::{ByteBudgetWriter, OutputLimits, OutputStats, capacity, write_on_output_thread};

/// Writes IPC to stdout on a dedicated thread so a full pipe cannot block the async runtime.
pub(crate) async fn write_arrow_stream_to_stdout(
    schema: SchemaRef,
    stream: impl Stream<Item = Result<RecordBatch, Error>> + Send + 'static,
    limits: Option<OutputLimits>,
) -> Result<OutputStats, Error> {
    write_on_output_thread(move || async move {
        write_arrow_stream(io::stdout().lock(), schema, stream, limits, true)
            .await
            .map(|(_, stats)| stats)
    })
    .await
}

pub(super) async fn write_arrow_stream<W: Write>(
    output: W,
    schema: SchemaRef,
    stream: impl Stream<Item = Result<RecordBatch, Error>>,
    limits: Option<OutputLimits>,
    is_stdout: bool,
) -> Result<(W, OutputStats), Error> {
    let mut output = ByteBudgetWriter {
        inner: output,
        bytes_written: 0,
        max_bytes: limits.as_ref().map(|limits| limits.max_bytes),
    };
    let mut writer = StreamWriter::try_new(&mut output, schema.as_ref())?;
    // Flush before polling so output failures stop further reads.
    writer.flush()?;
    let mut stream = Box::pin(stream);
    let mut stats = OutputStats::default();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if limits.as_ref().is_some_and(|limits| {
            batch.num_rows() as u64 > limits.max_rows.saturating_sub(stats.rows_written)
        }) {
            return Err(Error::OutputLimit {
                reason: "output_row_limit_exceeded",
            });
        }
        writer.write(&batch)?;
        writer.flush()?;
        stats.rows_written += batch.num_rows() as u64;
        stats.batches_written += 1;
    }
    // A resumed pipe write must observe cancellation before emitting the end marker.
    tokio::task::yield_now().await;
    if is_stdout {
        while !capacity::can_write(&io::stdout(), 8).context(OutputSnafu)? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    writer.finish()?;
    drop(writer);
    stats.bytes_written = output.bytes_written;
    Ok((output.inner, stats))
}
