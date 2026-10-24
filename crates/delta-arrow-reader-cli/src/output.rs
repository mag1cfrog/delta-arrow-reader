//! Row output formats and their resource ceilings.
//!
//! Stdout supports incremental Arrow or bounded, complete JSON. File outputs
//! stream into temporary storage and become usable only with a matching manifest.

pub(crate) mod arrow;
pub(crate) mod artifact;
pub(crate) mod field_descriptions;
pub(crate) mod json;

use std::{
    future::Future,
    io::{self, Write},
};

use snafu::{ResultExt, Snafu};

use crate::{Error, RuntimeSnafu};

/// Queries fail at these ceilings; previews return an explicitly labeled sample.
pub(crate) struct OutputLimits {
    pub(crate) max_rows: u64,
    pub(crate) max_bytes: u64,
}

// A blocked pipe must leave the runtime and its blocking pool free to read files.
async fn write_on_output_thread<F, T: Send + 'static>(
    write: impl FnOnce() -> F + Send + 'static,
) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>>,
{
    let runtime = tokio::runtime::Handle::current();
    let (mut sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("dar-output".into())
        .spawn(move || {
            let result = runtime.block_on(async {
                tokio::select! {
                    biased;
                    () = sender.closed() => None,
                    result = write() => Some(result),
                }
            });
            if let Some(result) = result {
                let _ = sender.send(result);
            }
        })
        .context(RuntimeSnafu {
            reason: "output_thread_spawn_failed",
        })?;
    receiver
        .await
        .map_err(io::Error::other)
        .context(RuntimeSnafu {
            reason: "output_thread_result_lost",
        })?
}

/// Row serialization selected by scan/query output consumers.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RowOutputFormat {
    Arrow,
    Json,
}

struct ByteBudgetWriter<W> {
    inner: W,
    bytes_written: u64,
    max_bytes: Option<u64>,
}

impl<W: Write> Write for ByteBudgetWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .max_bytes
            .is_some_and(|limit| bytes.len() as u64 > limit.saturating_sub(self.bytes_written))
        {
            return Err(io::Error::new(
                io::ErrorKind::FileTooLarge,
                OutputBytesExceeded,
            ));
        }
        let count = self.inner.write(bytes)?;
        self.bytes_written += count as u64;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Preserves the resource error through Arrow's io::Error wrapper.
#[derive(Debug, Snafu)]
#[snafu(display("Output exceeded its byte ceiling."))]
pub(crate) struct OutputBytesExceeded;

#[derive(Clone, Copy, Default)]
pub(crate) struct OutputStats {
    pub(crate) rows_written: u64,
    pub(crate) batches_written: u64,
    pub(crate) bytes_written: u64,
}
