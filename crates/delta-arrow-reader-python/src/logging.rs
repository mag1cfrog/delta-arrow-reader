//! Forward scoped core events to the application's Python logging configuration.
//!
//! The application controls handlers, formatting, and log levels, including changes
//! made between reads. Each call or stream uses its own subscriber so other native
//! extensions keep their tracing configuration.
//!
//! Binding-owned failure diagnostics live in `crate::diagnostics`; `callbacks`
//! defines how handler interruptions return to the reader.

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

use callbacks::HandlerInterruptTarget;
pub(crate) use callbacks::{REENTRY_MESSAGE, in_logging_callback, reject_logging_reentry};

const LOGGER_NAME: &str = "delta_arrow_reader";
// Bound the context attached to each Python log record.
const MAX_EVENT_FIELDS: usize = 64;
const MAX_FIELD_CHARS: usize = 1024;

/// Install a NullHandler so unconfigured applications stay quiet.
pub(crate) fn install_null_handler() -> PyResult<()> {
    callbacks::run_logging_callback(|py| {
        let logging = py.import("logging")?;
        let logger = logging.call_method1("getLogger", (LOGGER_NAME,))?;
        logger.call_method1("addHandler", (logging.getattr("NullHandler")?.call0()?,))?;
        Ok(())
    })?;
    Ok(())
}

/// Create a subscriber for one Python call and a receiver for handler interruptions.
pub(crate) fn new_call_dispatch() -> PyResult<(Dispatch, mpsc::Receiver<PyErr>)> {
    let (interrupts, receiver) = mpsc::sync_channel(1);
    Ok((
        new_dispatch(HandlerInterruptTarget::ExceptionChannel(interrupts))?,
        receiver,
    ))
}

/// Create a subscriber retained by an Arrow stream across successive batch reads.
///
/// Handler interruptions set a flag; retaining a Python traceback could keep the
/// consuming reader alive through a reference cycle.
pub(crate) fn new_stream_dispatch(interrupted: Arc<AtomicBool>) -> PyResult<Dispatch> {
    new_dispatch(HandlerInterruptTarget::StreamFlag(interrupted))
}

fn new_dispatch(interrupt_target: HandlerInterruptTarget) -> PyResult<Dispatch> {
    let dispatch = callbacks::run_logging_callback(|py| {
        let logger = py
            .import("logging")?
            .call_method1("getLogger", (LOGGER_NAME,))?;
        Ok(Dispatch::new(tracing_subscriber::registry().with(
            PythonLoggingLayer {
                logger: logger.unbind(),
                interrupt_target,
            },
        )))
    })?;
    Ok(dispatch.unwrap_or_else(Dispatch::none))
}

struct PythonLoggingLayer {
    logger: Py<PyAny>,
    interrupt_target: HandlerInterruptTarget,
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
        self.forward_event(event);
    }
}

impl PythonLoggingLayer {
    fn is_enabled(&self, level: u8) -> bool {
        self.interrupt_target
            .invoke_and_route_interrupt(|py| {
                self.logger
                    .bind(py)
                    .call_method1("isEnabledFor", (level,))?
                    .extract::<bool>()
            })
            // A skipped or failed logging callback disables this event.
            .unwrap_or(false)
    }

    fn forward_event(&self, event: &Event<'_>) {
        self.interrupt_target.invoke_and_route_interrupt(|py| {
            let metadata = event.metadata();
            // Rejected recursive calls must not evaluate or format event fields.
            let mut fields = PythonLogFieldVisitor {
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
struct PythonLogFieldVisitor<'py> {
    values: Bound<'py, PyDict>,
}

impl<'py> PythonLogFieldVisitor<'py> {
    fn insert(&self, field: &tracing::field::Field, value: impl IntoPyObject<'py>) {
        if self.values.len() < MAX_EVENT_FIELDS {
            let _ = self.values.set_item(field.name(), value);
        }
    }
}

impl Visit for PythonLogFieldVisitor<'_> {
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

    fn build_log_capture(py: Python<'_>) -> (Dispatch, Bound<'_, PyDict>) {
        let locals = PyDict::new(py);
        py.run(
            c"
import logging
records = []
class RecordCollector(logging.Handler):
    def emit(self, record):
        records.append(record)
manager = logging.Manager(logging.RootLogger(30))
logger = manager.getLogger('bridge-test')
logger.setLevel(5)
logger.addHandler(RecordCollector())
",
            Some(&locals),
            None,
        )
        .unwrap();
        let logger = locals.get_item("logger").unwrap().unwrap().unbind();
        let (interrupts, _) = mpsc::sync_channel(1);
        let dispatch = Dispatch::new(tracing_subscriber::registry().with(PythonLoggingLayer {
            logger,
            interrupt_target: HandlerInterruptTarget::ExceptionChannel(interrupts),
        }));
        (dispatch, locals)
    }

    fn emit_all_levels() {
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
            let (dispatch, locals) = build_log_capture(py);
            let logger = locals.get_item("logger").unwrap().unwrap();
            for level in [40, 5, 30, 20, 10] {
                logger.call_method1("setLevel", (level,)).unwrap();
                locals.set_item("threshold", level).unwrap();
                py.run(c"records.clear()", Some(&locals), None).unwrap();
                tracing::dispatcher::with_default(&dispatch, emit_all_levels);
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
            let (dispatch, locals) = build_log_capture(py);
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
            let (dispatch, locals) = build_log_capture(py);
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
        struct EventCounter(Arc<AtomicUsize>);
        impl<S: Subscriber> Layer<S> for EventCounter {
            fn on_event(&self, _: &Event<'_>, _: Context<'_, S>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        Python::initialize();
        Python::attach(|py| {
            install_null_handler().unwrap();
            drop(new_call_dispatch().unwrap());
            let (bridge, locals) = build_log_capture(py);
            let count = Arc::new(AtomicUsize::new(0));
            let host = Dispatch::new(
                tracing_subscriber::registry().with(EventCounter(Arc::clone(&count))),
            );
            tracing::dispatcher::set_global_default(host).unwrap();
            tracing::info!("before bridge");
            tracing::dispatcher::with_default(&bridge, emit_all_levels);
            tracing::info!("after bridge");
            assert_eq!(count.load(Ordering::Relaxed), 2);
            py.run(c"assert len(records) == 5", Some(&locals), None)
                .unwrap();
        });
    }
}
