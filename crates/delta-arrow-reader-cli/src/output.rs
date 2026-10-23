//! Row output formats and their resource ceilings.
//!
//! Arrow streams can be consumed incrementally. JSON stays buffered until the
//! whole result fits, so a failed query cannot be mistaken for a complete result.

pub(crate) mod arrow;
pub(crate) mod field_descriptions;
pub(crate) mod json;

use std::{future::Future, io};

use snafu::ResultExt;

use crate::{Error, RuntimeSnafu, StdoutStats};

/// Queries fail at these ceilings; previews return an explicitly labeled sample.
pub(crate) struct OutputLimits {
    pub(crate) max_rows: u64,
    pub(crate) max_bytes: u64,
}

// A blocked pipe must leave the runtime and its blocking pool free to read files.
async fn write_on_stdout_thread<F>(
    write: impl FnOnce() -> F + Send + 'static,
) -> Result<StdoutStats, Error>
where
    F: Future<Output = Result<StdoutStats, Error>>,
{
    let runtime = tokio::runtime::Handle::current();
    let (mut sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("dar-stdout".into())
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
