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

/// Invoke logging methods and route handler interruptions to the active reader.
pub(super) enum HandlerCallbacks {
    /// A waiting Python call can return the original control-flow exception.
    Call(mpsc::SyncSender<PyErr>),
    /// A traceback can refer back to the Arrow reader and prevent its cleanup.
    Stream(Arc<AtomicBool>),
}

impl HandlerCallbacks {
    pub(super) fn invoke<T>(&self, callback: impl FnOnce(Python<'_>) -> PyResult<T>) -> Option<T> {
        match call_python(callback) {
            Ok(value) => value,
            Err(error) => {
                match self {
                    Self::Call(sender) => {
                        // Late workers cannot retain a traceback after the wait ends.
                        let _ = sender.try_send(error);
                    }
                    Self::Stream(interrupted) => {
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
pub(super) fn call_python<T>(
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
pub(crate) fn check_reentry() -> PyResult<()> {
    if is_logging_callback() {
        return Err(PyRuntimeError::new_err(
            REENTRY_MESSAGE.to_string_lossy().into_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn is_logging_callback() -> bool {
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
