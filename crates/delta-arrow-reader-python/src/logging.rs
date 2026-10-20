//! Forward scoped core events to the application's Python logging configuration.
//!
//! `PythonLoggingLayer` selects reader events and passes them to `log`, which
//! collects their fields and calls Python's `logger.log(level, message, extra=...)`.
//! The `callbacks` module handles reentry and exceptions when Python code runs.
//!
//! Each binding operation or stream attaches a local `Dispatch` to its work.
//! Python owns handlers, formatting, and output; helpers for binding-owned failure
//! diagnostics live in `crate::diagnostics`.

use std::{
    fmt,
    sync::{Arc, atomic::AtomicBool, mpsc},
};

use pyo3::{
    prelude::*,
    types::{PyDict, PyString},
};
use tracing::{Dispatch, Event, Level, Metadata, Subscriber, field::Visit, subscriber::Interest};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

mod callbacks;

use callbacks::HandlerCallbacks;
pub(crate) use callbacks::{REENTRY_MESSAGE, check_reentry, is_logging_callback};

const LOGGER_NAME: &str = "delta_arrow_reader";
// Bound the context attached to each Python log record.
const MAX_EVENT_FIELDS: usize = 64;
const MAX_FIELD_CHARS: usize = 1024;

/// Install a NullHandler so unconfigured applications stay quiet.
pub(crate) fn initialize() -> PyResult<()> {
    callbacks::call_python(|py| {
        let logging = py.import("logging")?;
        let logger = logging.call_method1("getLogger", (LOGGER_NAME,))?;
        logger.call_method1("addHandler", (logging.getattr("NullHandler")?.call0()?,))?;
        Ok(())
    })?;
    Ok(())
}

/// Create a subscriber for one Python call and a receiver for handler interruptions.
pub(crate) fn dispatch() -> PyResult<(Dispatch, mpsc::Receiver<PyErr>)> {
    let (interrupts, receiver) = mpsc::sync_channel(1);
    Ok((new_dispatch(HandlerCallbacks::Call(interrupts))?, receiver))
}

/// Create a subscriber retained by an Arrow stream across successive batch reads.
///
/// Handler interruptions set a flag; retaining a Python traceback could keep the
/// consuming reader alive through a reference cycle.
pub(crate) fn stream_dispatch(interrupted: Arc<AtomicBool>) -> PyResult<Dispatch> {
    new_dispatch(HandlerCallbacks::Stream(interrupted))
}

fn new_dispatch(handler_callbacks: HandlerCallbacks) -> PyResult<Dispatch> {
    let dispatch = callbacks::call_python(|py| {
        let logger = py
            .import("logging")?
            .call_method1("getLogger", (LOGGER_NAME,))?;
        Ok(Dispatch::new(tracing_subscriber::registry().with(
            PythonLoggingLayer {
                logger: logger.unbind(),
                callbacks: handler_callbacks,
            },
        )))
    })?;
    Ok(dispatch.unwrap_or_else(Dispatch::none))
}

struct PythonLoggingLayer {
    logger: Py<PyAny>,
    callbacks: HandlerCallbacks,
}

impl<S: Subscriber> Layer<S> for PythonLoggingLayer {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        // Python levels can change without rebuilding tracing's callsite cache.
        Interest::sometimes()
    }

    fn enabled(&self, metadata: &Metadata<'_>, _: Context<'_, S>) -> bool {
        // Only our targets have the redacted fields this bridge is meant to expose.
        if metadata.target() != LOGGER_NAME
            && metadata.target() != "delta_arrow_reader_python"
            && !metadata.target().starts_with("delta_arrow_reader::")
        {
            return false;
        }
        self.is_enabled(python_level(metadata.level()))
    }

    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        self.log(event);
    }
}

impl PythonLoggingLayer {
    fn is_enabled(&self, level: u8) -> bool {
        self.callbacks
            .invoke(|py| {
                self.logger
                    .bind(py)
                    .call_method1("isEnabledFor", (level,))?
                    .extract::<bool>()
            })
            // A skipped or failed logging callback disables this event.
            .unwrap_or(false)
    }

    fn log(&self, event: &Event<'_>) {
        self.callbacks.invoke(|py| {
            let metadata = event.metadata();
            // Rejected recursive calls must not evaluate or format event fields.
            let mut fields = PythonFields {
                values: PyDict::new(py),
            };
            event.record(&mut fields);
            let fields = fields.values;
            let message = fields
                .get_item("event")?
                .or(fields.get_item("message")?)
                .unwrap_or_else(|| PyString::new(py, metadata.target()).into_any());
            // Python's extra argument attaches these attributes to the LogRecord.
            let extra = PyDict::new(py);
            extra.set_item("dar_fields", fields)?;
            extra.set_item("dar_target", metadata.target())?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("extra", extra)?;
            self.logger.bind(py).call_method(
                "log",
                (python_level(metadata.level()), message),
                Some(&kwargs),
            )?;
            Ok(())
        });
    }
}

/// Python's standard severity numbers; TRACE uses 5, below DEBUG, by convention.
fn python_level(level: &Level) -> u8 {
    match *level {
        Level::ERROR => 40,
        Level::WARN => 30,
        Level::INFO => 20,
        Level::DEBUG => 10,
        Level::TRACE => 5,
    }
}

/// Collect tracing fields into a Python dict, preserving numbers and booleans.
///
/// `Visit` is tracing's interface for reading field values of different types.
struct PythonFields<'py> {
    values: Bound<'py, PyDict>,
}

impl<'py> PythonFields<'py> {
    fn insert(&self, field: &tracing::field::Field, value: impl IntoPyObject<'py>) {
        if self.values.len() < MAX_EVENT_FIELDS {
            let _ = self.values.set_item(field.name(), value);
        }
    }
}

impl Visit for PythonFields<'_> {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.insert(
            field,
            value.chars().take(MAX_FIELD_CHARS).collect::<String>(),
        );
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.insert(field, value);
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.insert(field, value);
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.insert(field, value);
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.insert(field, value);
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        self.insert(field, value);
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.insert(field, value);
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.record_str(field, &format!("{value:?}"));
    }
}

#[cfg(test)]
mod tests;
