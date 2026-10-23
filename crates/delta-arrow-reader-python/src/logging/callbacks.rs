//! Safely invoke Python logging code from reader operations and worker threads.
//!
//! Logging methods can run application code. Calls share one reentry guard and
//! exception policy: ordinary handler errors are ignored; Python control-flow
//! exceptions interrupt the reader. Streams retain only an interruption flag.

use std::{
    cell::Cell,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

use pyo3::{
    exceptions::{PyException, PyRuntimeError},
    prelude::*,
};

pub(crate) const REENTRY_MESSAGE: &std::ffi::CStr =
    c"reader operations cannot be called from a logging callback";

/// Where a Python handler interruption is delivered after a logging callback.
pub(super) enum HandlerInterruptTarget {
    /// A waiting Python call can return the original control-flow exception.
    ExceptionChannel(mpsc::SyncSender<PyErr>),
    /// A traceback can refer back to the Arrow reader and prevent its cleanup.
    StreamFlag(Arc<AtomicBool>),
}

impl HandlerInterruptTarget {
    pub(super) fn invoke_and_route_interrupt<T>(
        &self,
        callback: impl FnOnce(Python<'_>) -> PyResult<T>,
    ) -> Option<T> {
        match run_logging_callback(callback) {
            Ok(value) => value,
            Err(error) => {
                match self {
                    Self::ExceptionChannel(sender) => {
                        // Late workers cannot retain a traceback after the wait ends.
                        let _ = sender.try_send(error);
                    }
                    Self::StreamFlag(interrupted) => {
                        interrupted.store(true, Ordering::Relaxed);
                    }
                }
                None
            }
        }
    }
}

/// Run Python code under the logging reentry and shutdown policy.
///
/// `Ok(None)` means reentry, interpreter shutdown, or an ordinary Python exception.
/// Control-flow exceptions such as KeyboardInterrupt and SystemExit remain errors.
pub(super) fn run_logging_callback<T>(
    callback: impl FnOnce(Python<'_>) -> PyResult<T>,
) -> PyResult<Option<T>> {
    let Some(_guard) = LoggingCallbackGuard::enter() else {
        return Ok(None);
    };
    // Workers can outlive an interrupted call; try_attach checks finalization.
    Python::try_attach(|py| match callback(py) {
        Ok(value) => Ok(Some(value)),
        Err(error) if error.is_instance_of::<PyException>(py) => Ok(None),
        // KeyboardInterrupt and SystemExit remain Python control flow.
        Err(error) => Err(error),
    })
    .unwrap_or(Ok(None))
}

/// Reject reader operations called from Python logging code on this thread.
pub(crate) fn reject_logging_reentry() -> PyResult<()> {
    if in_logging_callback() {
        return Err(PyRuntimeError::new_err(
            REENTRY_MESSAGE.to_string_lossy().into_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn in_logging_callback() -> bool {
    IN_LOGGING_CALLBACK.get()
}

thread_local! {
    static IN_LOGGING_CALLBACK: Cell<bool> = const { Cell::new(false) };
}

// Reset the thread's callback state on every exit, including early error returns.
struct LoggingCallbackGuard;

impl LoggingCallbackGuard {
    fn enter() -> Option<Self> {
        if IN_LOGGING_CALLBACK.replace(true) {
            None
        } else {
            Some(Self)
        }
    }
}

impl Drop for LoggingCallbackGuard {
    fn drop(&mut self) {
        IN_LOGGING_CALLBACK.set(false);
    }
}
