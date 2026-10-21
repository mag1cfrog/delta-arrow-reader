//! Arrow IPC output on a dedicated thread, with optional complete-result ceilings.

use std::io::{self, Write};

use arrow::{array::RecordBatch, ipc::writer::StreamWriter};
use arrow_schema::SchemaRef;
use futures_util::{Stream, StreamExt};
use snafu::{ResultExt, Snafu};

use crate::{Error, RuntimeSnafu, StdoutStats};

/// Resource ceilings fail the query instead of silently truncating its result.
pub(crate) struct ArrowOutputLimits {
    pub(crate) max_rows: u64,
    pub(crate) max_bytes: u64,
}

/// Preserves the resource error through Arrow's io::Error wrapper.
#[derive(Debug, Snafu)]
#[snafu(display("Arrow output exceeded its byte ceiling."))]
pub(crate) struct OutputBytesExceeded;

struct ByteCountingWriter<W> {
    inner: W,
    bytes_written: u64,
    max_bytes: Option<u64>,
}

impl<W: Write> Write for ByteCountingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .max_bytes
            .is_some_and(|limit| bytes.len() as u64 > limit.saturating_sub(self.bytes_written))
        {
            return Err(io::Error::other(OutputBytesExceeded));
        }
        let count = self.inner.write(bytes)?;
        self.bytes_written += count as u64;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Writes IPC to stdout on a dedicated thread so a full pipe cannot block the async runtime.
pub(crate) async fn write_arrow_stream_to_stdout(
    schema: SchemaRef,
    stream: impl Stream<Item = Result<RecordBatch, Error>> + Send + 'static,
    limits: Option<ArrowOutputLimits>,
) -> Result<StdoutStats, Error> {
    let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
    let runtime = tokio::runtime::Handle::current();
    // A blocked stdout must leave the runtime and its blocking pool free to read files.
    std::thread::Builder::new()
        .name("dar-stdout".into())
        .spawn(move || {
            let result = runtime.block_on(async {
                let mut writer = StreamWriter::try_new(
                    ByteCountingWriter {
                        inner: io::stdout().lock(),
                        bytes_written: 0,
                        max_bytes: limits.as_ref().map(|limits| limits.max_bytes),
                    },
                    schema.as_ref(),
                )?;
                // Flush before polling so output failures stop further reads.
                writer.flush()?;
                let mut stream = Box::pin(stream);
                let mut stdout_stats = StdoutStats::default();
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if limits.as_ref().is_some_and(|limits| {
                        batch.num_rows() as u64
                            > limits.max_rows.saturating_sub(stdout_stats.rows_written)
                    }) {
                        return Err(Error::OutputLimit {
                            reason: "output_row_limit_exceeded",
                        });
                    }
                    writer.write(&batch)?;
                    writer.flush()?;
                    stdout_stats.rows_written += batch.num_rows() as u64;
                    stdout_stats.batches_written += 1;
                }
                // finish writes the end marker and flushes; never call it after an error.
                writer.finish()?;
                stdout_stats.bytes_written = writer.get_ref().bytes_written;
                Ok(stdout_stats)
            });
            let _ = result_sender.send(result);
        })
        .context(RuntimeSnafu {
            reason: "output_thread_spawn_failed",
        })?;
    result_receiver
        .await
        .map_err(io::Error::other)
        .context(RuntimeSnafu {
            reason: "output_thread_result_lost",
        })?
}
