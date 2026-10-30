//! A preview contains whole rows that fit alongside their schema and sample state.
//!
//! For example, if the second row is too large, the first row can still be returned
//! with `stop_reason: "byte_limit"` and `has_more: true`. If even the first row
//! cannot fit, the command fails with empty stdout.

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use futures_util::{Stream, StreamExt};
use serde::Serialize;
use serde_json::Value;

use super::{BoundedJsonBuffer, JsonRow, OutputLimits, write_on_output_thread};
use crate::{
    diagnostics::Error,
    output::{OutputStats, write_stdout},
};

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum SampleStopReason {
    EndOfInput,
    RowLimit,
    ByteLimit,
}

impl SampleStopReason {
    fn has_more(&self) -> Option<bool> {
        match self {
            Self::EndOfInput => Some(false),
            Self::RowLimit => None,
            Self::ByteLimit => Some(true),
        }
    }
}

fn append_sample_footer(
    output: &mut BoundedJsonBuffer,
    row_count: usize,
    stop_reason: &SampleStopReason,
) -> Result<(), Error> {
    output.append_bytes(b"],\"row_count\":")?;
    output.append_json(&row_count.to_string())?;
    output.append_bytes(b",\"completion\":\"sample\",\"stop_reason\":")?;
    output.append_json(stop_reason)?;
    output.append_bytes(b",\"has_more\":")?;
    output.append_json(&stop_reason.has_more())?;
    output.append_bytes(b"}\n")
}

pub(crate) async fn write_preview_to_stdout(
    schema: SchemaRef,
    stream: impl Stream<Item = Result<RecordBatch, Error>> + Send + 'static,
    binding: Value,
    limits: OutputLimits,
) -> Result<OutputStats, Error> {
    write_on_output_thread(move || async move {
        let (bytes, stats) = encode_preview(schema, stream, binding, limits).await?;
        write_stdout(&bytes)?;
        Ok(stats)
    })
    .await
}

async fn encode_preview(
    schema: SchemaRef,
    stream: impl Stream<Item = Result<RecordBatch, Error>>,
    binding: Value,
    limits: OutputLimits,
) -> Result<(Vec<u8>, OutputStats), Error> {
    let mut output =
        BoundedJsonBuffer::with_rows_header(&schema, &[binding], limits.max_bytes, "preview")
            .map_err(|error| match error {
                Error::OutputLimit { .. } => Error::OutputLimit {
                    reason: "preview_schema_limit_exceeded",
                },
                error => error,
            })?;
    let mut row_offsets = Vec::new();
    let mut stream = Box::pin(stream);
    let mut stop_reason = SampleStopReason::RowLimit;
    'read: while (row_offsets.len() as u64) < limits.max_rows {
        let Some(batch) = stream.next().await else {
            stop_reason = SampleStopReason::EndOfInput;
            break;
        };
        let batch = batch?;
        for index in 0..batch.num_rows() {
            if row_offsets.len() as u64 == limits.max_rows {
                break 'read;
            }
            let row_start = output.bytes.len();
            let encoded = (|| {
                if !row_offsets.is_empty() {
                    output.append_bytes(b",")?;
                }
                output.append_json(&JsonRow {
                    columns: batch.columns(),
                    index,
                })?;
                let row_end = output.bytes.len();
                let row_count = row_offsets.len() + 1;
                // Before reading again, this prefix must fit at least its shortest possible footer.
                let shortest_footer = if row_count as u64 == limits.max_rows {
                    SampleStopReason::RowLimit
                } else {
                    SampleStopReason::ByteLimit
                };
                let result = append_sample_footer(&mut output, row_count, &shortest_footer);
                output.bytes.truncate(row_end);
                result
            })();
            match encoded {
                Ok(()) => row_offsets.push(row_start),
                Err(Error::OutputLimit { .. }) => {
                    output.bytes.truncate(row_start);
                    if row_offsets.is_empty() {
                        return Err(Error::OutputLimit {
                            reason: "preview_row_too_large",
                        });
                    }
                    stop_reason = SampleStopReason::ByteLimit;
                    break 'read;
                }
                Err(error) => return Err(error),
            }
        }
    }
    loop {
        let footer_start = output.bytes.len();
        let encoded = append_sample_footer(&mut output, row_offsets.len(), &stop_reason);
        match encoded {
            Ok(()) => break,
            Err(Error::OutputLimit { .. }) => {
                output.bytes.truncate(footer_start);
                let Some(last_row_start) = row_offsets.pop() else {
                    return Err(Error::OutputLimit {
                        reason: "preview_schema_limit_exceeded",
                    });
                };
                if row_offsets.is_empty() {
                    return Err(Error::OutputLimit {
                        reason: "preview_row_too_large",
                    });
                }
                // Reserve the actual footer, so a result that fits exactly keeps every row.
                output.bytes.truncate(last_row_start);
                stop_reason = SampleStopReason::ByteLimit;
            }
            Err(error) => return Err(error),
        }
    }
    let stats = OutputStats {
        rows_written: row_offsets.len() as u64,
        bytes_written: output.bytes.len() as u64,
        ..Default::default()
    };
    Ok((output.bytes, stats))
}

#[cfg(test)]
mod tests;
