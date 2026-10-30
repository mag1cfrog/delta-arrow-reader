//! Report failures whose diagnostic belongs to the Python binding.
//!
//! Core operation failures already emit their own events. These helpers cover
//! return-only validation errors and binding-owned failures such as schema export.

use std::fmt;

use datafusion::common::{DataFusionError, SchemaError};
use delta_arrow_reader::{DeltaReaderError, error_source_kind};
use pyo3::{prelude::*, types::PyDict};

use crate::logging;

pub(crate) struct SqlErrorDiagnostic<'a> {
    pub(crate) code: &'static str,
    pub(crate) source: &'a DataFusionError,
}

impl SqlErrorDiagnostic<'_> {
    fn reason(&self) -> &'static str {
        if self.code == "sql_planning"
            && DeltaReaderError::find_source(self.source).is_none()
            && let DataFusionError::SchemaError(error, _) = self.source.find_root()
        {
            match error.as_ref() {
                SchemaError::FieldNotFound { .. } => return "column_not_found",
                SchemaError::AmbiguousReference { .. } => return "ambiguous_column",
                _ => {}
            }
        }
        self.code
    }

    pub(crate) fn to_exception(&self, py: Python<'_>) -> PyErr {
        let exception = crate::new_binding_exception(py, self.to_string(), "datafusion", self.code);
        let value = exception.value(py);
        let set_details = || -> PyResult<()> {
            value.setattr("reason", self.reason())?;
            value.setattr("source_kind", error_source_kind(self.source))?;
            if let Some(reader) = DeltaReaderError::find_source(self.source) {
                let cause = PyDict::new(py);
                cause.set_item("phase", reader.phase().as_str())?;
                cause.set_item("code", reader.code())?;
                cause.set_item("reason", reader.reason())?;
                cause.set_item("source_kind", reader.source_kind())?;
                value.setattr("cause", cause)?;
            }
            Ok(())
        };
        match set_details() {
            Ok(()) => exception,
            Err(error) => error,
        }
    }
}

impl fmt::Display for SqlErrorDiagnostic<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "delta reader error: phase=datafusion code={} reason={} source_kind={}",
            self.code,
            self.reason(),
            error_source_kind(self.source)
        )?;
        if let Some(reader) = DeltaReaderError::find_source(self.source) {
            write!(
                formatter,
                " cause={{phase={} code={} reason={}",
                reader.phase().as_str(),
                reader.code(),
                reader.reason()
            )?;
            if let Some(kind) = reader.source_kind() {
                write!(formatter, " source_kind={kind}")?;
            }
            formatter.write_str("}")?;
        }
        Ok(())
    }
}

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
