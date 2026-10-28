//! Whether an output can accept a small completion marker without waiting for its reader.
//!
//! A full pipe delays completion until the consumer drains it. Checking capacity
//! lets the command handle cancellation during that wait without queuing a success
//! record. Each pipe must have one writer; terminal writes within the process
//! are serialized by the diagnostics module.
//! The caller limits each write to 512 bytes, the POSIX minimum atomic pipe write.

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub(crate) use unix::can_write;
#[cfg(windows)]
pub(crate) use windows::can_write;
