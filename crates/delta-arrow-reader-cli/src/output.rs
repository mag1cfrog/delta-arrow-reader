//! Row output formats and their resource ceilings.
//!
//! Arrow streams can be consumed incrementally. JSON stays buffered until the
//! whole result fits, so a failed query cannot be mistaken for a complete result.

pub(crate) mod arrow;
pub(crate) mod field_descriptions;
pub(crate) mod json;

use std::io;

use snafu::ResultExt;

use crate::{Error, RuntimeSnafu, StdoutStats};

/// Queries fail at these ceilings; previews return an explicitly labeled sample.
pub(crate) struct OutputLimits {
    pub(crate) max_rows: u64,
    pub(crate) max_bytes: u64,
}

// A blocked pipe must leave the runtime and its blocking pool free to read files.
async fn on_stdout_thread(
    write: impl FnOnce() -> Result<StdoutStats, Error> + Send + 'static,
) -> Result<StdoutStats, Error> {
    let (sender, receiver) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("dar-stdout".into())
        .spawn(move || {
            let _ = sender.send(write());
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
