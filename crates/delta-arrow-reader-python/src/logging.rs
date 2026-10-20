//! Forward scoped core events to the application's Python logging configuration.

use std::{
    cell::Cell,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

use pyo3::{
    exceptions::{PyException, PyRuntimeError},
    prelude::*,
    types::{PyDict, PyString},
};
use tracing::{Dispatch, Event, Level, Metadata, Subscriber, field::Visit, subscriber::Interest};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

const LOGGER_NAME: &str = "delta_arrow_reader";
pub(crate) const REENTRY_MESSAGE: &std::ffi::CStr =
    c"reader operations cannot be called from a logging callback";
const MAX_EVENT_FIELDS: usize = 64;
const MAX_FIELD_CHARS: usize = 1024;

thread_local! {
    static IN_LOGGING_CALLBACK: Cell<bool> = const { Cell::new(false) };
}

struct LoggingCallback;

impl LoggingCallback {
    fn enter() -> Option<Self> {
        if IN_LOGGING_CALLBACK.replace(true) {
            None
        } else {
            Some(Self)
        }
    }
}

impl Drop for LoggingCallback {
    fn drop(&mut self) {
        IN_LOGGING_CALLBACK.set(false);
    }
}

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

fn python_callback<T>(callback: impl FnOnce(Python<'_>) -> PyResult<T>) -> PyResult<Option<T>> {
    let Some(_guard) = LoggingCallback::enter() else {
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

pub(crate) fn initialize() -> PyResult<()> {
    python_callback(|py| {
        let logging = py.import("logging")?;
        let logger = logging.call_method1("getLogger", (LOGGER_NAME,))?;
        // Suppress Python's lastResort handler without configuring application output.
        logger.call_method1("addHandler", (logging.getattr("NullHandler")?.call0()?,))?;
        Ok(())
    })?;
    Ok(())
}

pub(crate) fn dispatch() -> PyResult<(Dispatch, mpsc::Receiver<PyErr>)> {
    let (interrupts, receiver) = mpsc::sync_channel(1);
    Ok((new_dispatch(HandlerInterrupts::Call(interrupts))?, receiver))
}

pub(crate) fn stream_dispatch(interrupted: Arc<AtomicBool>) -> PyResult<Dispatch> {
    new_dispatch(HandlerInterrupts::Stream(interrupted))
}

// Synchronous conversions use the same handler interruption policy as runtime waits.
pub(crate) fn log_failure(error: PyErr, emit: impl FnOnce()) -> PyErr {
    // A handler's pure validation must not replace tracing's currently borrowed dispatcher.
    if is_logging_callback() {
        return error;
    }
    match dispatch() {
        Ok((dispatch, interrupts)) => {
            tracing::dispatcher::with_default(&dispatch, emit);
            interrupts.try_recv().unwrap_or(error)
        }
        Err(interruption) => interruption,
    }
}

pub(crate) fn validation_error(
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

enum HandlerInterrupts {
    Call(mpsc::SyncSender<PyErr>),
    // A traceback can refer back to the reader and form an uncollectable cycle.
    Stream(Arc<AtomicBool>),
}

fn new_dispatch(interrupts: HandlerInterrupts) -> PyResult<Dispatch> {
    Ok(python_callback(|py| {
        let logger = py
            .import("logging")?
            .call_method1("getLogger", (LOGGER_NAME,))?;
        Ok(Dispatch::new(tracing_subscriber::registry().with(
            PythonLogging {
                logger: logger.unbind(),
                interrupts,
            },
        )))
    })?
    .unwrap_or_else(Dispatch::none))
}

struct PythonLogging {
    logger: Py<PyAny>,
    interrupts: HandlerInterrupts,
}

impl PythonLogging {
    fn callback<T>(&self, callback: impl FnOnce(Python<'_>) -> PyResult<T>) -> Option<T> {
        match python_callback(callback) {
            Ok(value) => value,
            Err(error) => {
                match &self.interrupts {
                    HandlerInterrupts::Call(sender) => {
                        // Late workers cannot retain a traceback after the wait ends.
                        let _ = sender.try_send(error);
                    }
                    HandlerInterrupts::Stream(interrupted) => {
                        interrupted.store(true, Ordering::Relaxed);
                    }
                }
                None
            }
        }
    }
}

fn python_level(level: &Level) -> u8 {
    match *level {
        Level::ERROR => 40,
        Level::WARN => 30,
        Level::INFO => 20,
        Level::DEBUG => 10,
        Level::TRACE => 5,
    }
}

impl<S: Subscriber> Layer<S> for PythonLogging {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        // Python levels can change without rebuilding tracing's callsite cache.
        Interest::sometimes()
    }

    fn enabled(&self, metadata: &Metadata<'_>, _: Context<'_, S>) -> bool {
        if metadata.target() != LOGGER_NAME
            && metadata.target() != "delta_arrow_reader_python"
            && !metadata.target().starts_with("delta_arrow_reader::")
        {
            return false;
        }
        self.callback(|py| {
            self.logger
                .bind(py)
                .call_method1("isEnabledFor", (python_level(metadata.level()),))?
                .extract::<bool>()
        })
        .unwrap_or(false)
    }

    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        self.callback(|py| {
            let fields = PyDict::new(py);
            event.record(&mut PythonFields {
                values: fields.clone(),
            });
            let message = fields
                .get_item("event")?
                .or(fields.get_item("message")?)
                .unwrap_or_else(|| PyString::new(py, event.metadata().target()).into_any());
            let extra = PyDict::new(py);
            extra.set_item("dar_fields", fields)?;
            extra.set_item("dar_target", event.metadata().target())?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("extra", extra)?;
            self.logger.bind(py).call_method(
                "log",
                (python_level(event.metadata().level()), message),
                Some(&kwargs),
            )?;
            Ok(())
        });
    }
}

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
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    fn capture(py: Python<'_>) -> (Dispatch, Bound<'_, PyDict>) {
        let locals = PyDict::new(py);
        py.run(
            c"
import logging
records = []
class Collect(logging.Handler):
    def emit(self, record):
        records.append(record)
manager = logging.Manager(logging.RootLogger(30))
logger = manager.getLogger('bridge-test')
logger.setLevel(5)
logger.addHandler(Collect())
",
            Some(&locals),
            None,
        )
        .unwrap();
        let logger = locals.get_item("logger").unwrap().unwrap().unbind();
        let (interrupts, _) = mpsc::sync_channel(1);
        let dispatch = Dispatch::new(tracing_subscriber::registry().with(PythonLogging {
            logger,
            interrupts: HandlerInterrupts::Call(interrupts),
        }));
        (dispatch, locals)
    }

    fn all_levels() {
        tracing::trace!(target: "delta_arrow_reader", event = "trace");
        tracing::debug!(target: "delta_arrow_reader", event = "debug");
        tracing::info!(target: "delta_arrow_reader", event = "info");
        tracing::warn!(target: "delta_arrow_reader", event = "warning");
        tracing::error!(target: "delta_arrow_reader", event = "error");
    }

    #[test]
    fn python_levels_are_live_at_existing_callsites() {
        Python::initialize();
        Python::attach(|py| {
            let (dispatch, locals) = capture(py);
            let logger = locals.get_item("logger").unwrap().unwrap();
            for level in [40, 5, 30, 20, 10] {
                logger.call_method1("setLevel", (level,)).unwrap();
                locals.set_item("threshold", level).unwrap();
                py.run(c"records.clear()", Some(&locals), None).unwrap();
                tracing::dispatcher::with_default(&dispatch, all_levels);
                py.run(
                    c"
assert [r.levelno for r in records] == [n for n in [5, 10, 20, 30, 40] if n >= threshold]
assert [r.msg for r in records] == [r.dar_fields['event'] for r in records]
assert all(r.dar_target == 'delta_arrow_reader' for r in records)
",
                    Some(&locals),
                    None,
                )
                .unwrap();
            }
        });
    }

    #[test]
    fn fields_preserve_types_and_bound_text() {
        Python::initialize();
        Python::attach(|py| {
            let (dispatch, locals) = capture(py);
            tracing::dispatcher::with_default(&dispatch, || {
                tracing::info!(target: "delta_arrow_reader::diagnostics::test",
                    event = "typed", operation_id = u64::MAX,
                    elapsed_micros = u128::MAX, signed = i64::MIN,
                    wide_signed = i128::MIN, flag = true, fraction = 0.5,
                    text = "a".repeat(2048), unicode = "\u{754c}".repeat(2048),
                    detail = ?Some(12));
            });
            py.run(
                c"
assert len(records) == 1
fields = records[0].dar_fields
assert fields['operation_id'] == 2**64 - 1
assert fields['elapsed_micros'] == 2**128 - 1
assert fields['signed'] == -(2**63)
assert fields['wide_signed'] == -(2**127)
assert fields['flag'] is True
assert fields['fraction'] == 0.5
assert fields['text'] == 'a' * 1024
assert len(fields['unicode']) == 1024
assert fields['detail'] == 'Some(12)'
",
                Some(&locals),
                None,
            )
            .unwrap();
        });
    }

    #[test]
    fn disabled_and_foreign_events_do_not_format_fields() {
        struct NeverFormat;
        impl fmt::Debug for NeverFormat {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("disabled fields must not be formatted");
            }
        }
        Python::initialize();
        Python::attach(|py| {
            let (dispatch, locals) = capture(py);
            locals
                .get_item("logger")
                .unwrap()
                .unwrap()
                .call_method1("setLevel", (40,))
                .unwrap();
            tracing::dispatcher::with_default(&dispatch, || {
                tracing::debug!(target: "delta_arrow_reader", secret = ?NeverFormat);
                tracing::error!(target: "object_store", secret = ?NeverFormat);
                tracing::error!(target: "delta_arrow_reader_unrelated", secret = ?NeverFormat);
            });
            py.run(c"assert records == []", Some(&locals), None)
                .unwrap();
        });
    }

    #[test]
    fn scoped_bridge_coexists_with_the_hosts_global_subscriber() {
        const CHILD: &str = "DELTA_ARROW_READER_LOGGING_GLOBAL_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "logging::tests::scoped_bridge_coexists_with_the_hosts_global_subscriber",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        struct Count(Arc<AtomicUsize>);
        impl<S: Subscriber> Layer<S> for Count {
            fn on_event(&self, _: &Event<'_>, _: Context<'_, S>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        Python::initialize();
        Python::attach(|py| {
            initialize().unwrap();
            drop(dispatch().unwrap());
            let (bridge, locals) = capture(py);
            let count = Arc::new(AtomicUsize::new(0));
            let host =
                Dispatch::new(tracing_subscriber::registry().with(Count(Arc::clone(&count))));
            tracing::dispatcher::set_global_default(host).unwrap();
            tracing::info!("before bridge");
            tracing::dispatcher::with_default(&bridge, all_levels);
            tracing::info!("after bridge");
            assert_eq!(count.load(Ordering::Relaxed), 2);
            py.run(c"assert len(records) == 5", Some(&locals), None)
                .unwrap();
        });
    }
}
