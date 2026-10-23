//! Report failures whose diagnostic belongs to the Python binding.
//!
//! Core operation failures already emit their own events. These helpers cover
//! return-only validation errors and binding-owned failures such as schema export.

use pyo3::PyErr;

use crate::logging;

/// Emit a failure under the Python subscriber and return the exception to raise.
///
/// Ordinary handler errors preserve the original exception; a handler requesting
/// interruption takes precedence, as it does during a runtime wait.
pub(crate) fn log_failure(error: PyErr, emit: impl FnOnce()) -> PyErr {
    // A handler's pure validation must not replace tracing's currently borrowed dispatcher.
    if logging::in_logging_callback() {
        return error;
    }
    match logging::new_call_dispatch() {
        Ok((dispatch, interrupts)) => {
            tracing::dispatcher::with_default(&dispatch, emit);
            interrupts.try_recv().unwrap_or(error)
        }
        Err(interruption) => interruption,
    }
}

/// Report a return-only Core validation error using the Core's classification.
pub(crate) fn log_validation_error(
    error: &delta_arrow_reader::DeltaReaderError,
    argument: &'static str,
    exception: PyErr,
) -> PyErr {
    log_failure(exception, || {
        tracing::error!(target: "delta_arrow_reader_python",
            event = "validation.failed", operation = "validation", outcome = "failed", argument,
            error_phase = error.phase().as_str(), error_code = error.code(),
            error_reason = error.reason(), error_source_kind = error.source_kind());
    })
}
