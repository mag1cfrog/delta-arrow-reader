use std::{
    ffi::CStr,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

use arrow::{
    array::{ArrayRef, Int32Array, StructArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
    ffi::from_ffi,
    record_batch::{RecordBatch, RecordBatchReader},
};
use pyo3::{prelude::*, types::PyDict};

use super::*;

struct CallbackReader {
    next: Box<dyn FnMut() -> Option<Result<RecordBatch, ArrowError>> + Send>,
    dropped: Arc<AtomicUsize>,
    on_drop: Option<Box<dyn FnOnce() + Send>>,
}

impl Iterator for CallbackReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        (self.next)()
    }
}

impl RecordBatchReader for CallbackReader {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }
}

impl Drop for CallbackReader {
    fn drop(&mut self) {
        if let Some(on_drop) = self.on_drop.take() {
            on_drop();
        }
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn missing_native_callbacks_return_errors_and_preserve_cleanup() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut stream = export(Box::new(CallbackReader {
        next: Box::new(|| None),
        dropped: Arc::clone(&dropped),
        on_drop: None,
    }));
    // SAFETY: The stream stays live and every callback has exclusive access.
    unsafe {
        with_native_stream(&mut stream, |native| {
            native.get_schema = None;
            native.get_next = None;
            native.get_last_error = None;
        })
        .unwrap();
        let mut schema = FFI_ArrowSchema::empty();
        let mut array = FFI_ArrowArray::empty();
        assert_eq!(stream.get_schema.unwrap()(&mut stream, &mut schema), EINVAL);
        assert_eq!(stream.get_next.unwrap()(&mut stream, &mut array), EINVAL);
        assert_eq!(
            CStr::from_ptr(stream.get_last_error.unwrap()(&mut stream)),
            UNAVAILABLE
        );
    }
    drop(stream);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn recursive_close_retains_the_active_reader_and_returned_array() {
    let stream = Box::into_raw(Box::new(FFI_ArrowArrayStream::empty()));
    let address = stream as usize;
    let dropped = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&dropped);
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_from_iter([("id", values)]).unwrap();
    let schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).unwrap();
    let reader = CallbackReader {
        dropped: Arc::clone(&dropped),
        on_drop: None,
        next: Box::new(move || {
            // Simulate a consumer's recursive calls during the native get_next borrow.
            let stream = address as *mut FFI_ArrowArrayStream;
            unsafe {
                let mut nested = FFI_ArrowArray::empty();
                assert_eq!((*stream).get_next.unwrap()(stream, &mut nested), EINVAL);
                let mut nested_schema = FFI_ArrowSchema::empty();
                assert_eq!(
                    (*stream).get_schema.unwrap()(stream, &mut nested_schema),
                    EINVAL
                );
                assert_eq!(
                    CStr::from_ptr((*stream).get_last_error.unwrap()(stream)),
                    UNAVAILABLE
                );
                let release = (*stream).release.unwrap();
                release(stream);
                release(stream);
                assert!((*stream).release.is_none());
                assert_eq!(observed.load(Ordering::SeqCst), 0);
            }
            Some(Ok(batch.clone()))
        }),
    };
    unsafe {
        ptr::write(stream, export(Box::new(reader)));
        let mut array = FFI_ArrowArray::empty();
        assert_eq!((*stream).get_next.unwrap()(stream, &mut array), 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        drop(Box::from_raw(stream));
        let data = from_ffi(array, &schema).unwrap();
        let batch = RecordBatch::from(StructArray::from(data));
        assert_eq!(batch.column(0).as_ref(), &Int32Array::from(vec![1, 2, 3]));
    }
}

#[test]
fn ordinary_errors_survive_moves_and_close_during_failure_is_safe() {
    for close_in_next in [false, true] {
        let stream = Box::into_raw(Box::new(FFI_ArrowArrayStream::empty()));
        let address = stream as usize;
        let dropped = Arc::new(AtomicUsize::new(0));
        let reader = CallbackReader {
            dropped: Arc::clone(&dropped),
            on_drop: None,
            next: Box::new(move || {
                if close_in_next {
                    let stream = address as *mut FFI_ArrowArrayStream;
                    unsafe { (*stream).release.unwrap()(stream) };
                }
                Some(Err(ArrowError::ComputeError("redacted failure".into())))
            }),
        };
        let mut exported = export(Box::new(reader));
        unsafe {
            ptr::write(stream, FFI_ArrowArrayStream::from_raw(&mut exported));
            assert!(exported.release.is_none());
            for _ in 0..2 {
                let mut array = FFI_ArrowArray::empty();
                assert_eq!((*stream).get_next.unwrap()(stream, &mut array), EINVAL);
                let error = CStr::from_ptr((*stream).get_last_error.unwrap()(stream));
                if close_in_next {
                    assert_eq!(error, UNAVAILABLE);
                } else {
                    assert!(error.to_str().unwrap().contains("redacted failure"));
                }
            }
            drop(Box::from_raw(stream));
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn python_handler_reentry_is_rejected_before_the_native_borrow() {
    // This test replaces the process-wide Python logger while releasing the GIL.
    const CHILD: &str = "DELTA_ARROW_READER_REENTRY_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "arrow_stream::tests::python_handler_reentry_is_rejected_before_the_native_borrow",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(status.success());
        return;
    }
    Python::initialize();
    for worker in [false, true] {
        Python::attach(|py| {
            let locals = PyDict::new(py);
            let dropped = Arc::new(AtomicUsize::new(0));
            let reader = CallbackReader {
                dropped: Arc::clone(&dropped),
                on_drop: None,
                next: Box::new(move || {
                    let emit =
                        || tracing::info!(target: "delta_arrow_reader", event = "test.active_read");
                    if worker {
                        let dispatch = tracing::dispatcher::get_default(Clone::clone);
                        std::thread::spawn(move || {
                            tracing::dispatcher::with_default(&dispatch, emit)
                        })
                        .join()
                        .unwrap();
                    } else {
                        emit();
                    }
                    None
                }),
            };
            let mut stream = export(Box::new(reader));
            let gil_on_drop = Arc::new(AtomicBool::new(false));
            let observed_gil = Arc::clone(&gil_on_drop);
            let python_owner = PyDict::new(py).into_any().unbind();
            let mut idle = export(Box::new(CallbackReader {
                dropped: Arc::clone(&dropped),
                next: Box::new(|| panic!("logging callback reached an idle reader")),
                on_drop: Some(Box::new(move || unsafe {
                    // Observe the actual GIL without relying on PyO3's cached attach count.
                    // Ensure also makes a deliberately broken release safe to test.
                    let state = pyo3::ffi::PyGILState_Ensure();
                    observed_gil.store(
                        matches!(state, pyo3::ffi::PyGILState_STATE::PyGILState_LOCKED),
                        Ordering::SeqCst,
                    );
                    drop(python_owner);
                    pyo3::ffi::PyGILState_Release(state);
                })),
            }));
            locals
                .set_item("idle", (&mut idle as *mut FFI_ArrowArrayStream) as usize)
                .unwrap();
            locals
                .set_item(
                    "stream",
                    (&mut stream as *mut FFI_ArrowArrayStream) as usize,
                )
                .unwrap();
            locals
                .set_item("get_next", stream.get_next.unwrap() as usize)
                .unwrap();
            locals
                .set_item("get_schema", stream.get_schema.unwrap() as usize)
                .unwrap();
            locals
                .set_item("get_error", stream.get_last_error.unwrap() as usize)
                .unwrap();
            locals
                .set_item("release", stream.release.unwrap() as usize)
                .unwrap();
            py.run(
                c"
import ctypes
import logging
results = []
callback = ctypes.CFUNCTYPE(ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p)
read = callback(get_next)
schema = callback(get_schema)
error = ctypes.CFUNCTYPE(ctypes.c_char_p, ctypes.c_void_p)(get_error)
close = ctypes.CFUNCTYPE(None, ctypes.c_void_p)(release)
class Reenter(logging.Handler):
    def emit(self, record):
        # Rejected callbacks cannot touch the null output. CFUNCTYPE releases the GIL.
        for source in (stream, idle):
            for operation in (read, schema):
                results.append((operation(source, None), error(source)))
        close(idle)
        close(stream)
logger = logging.getLogger('delta_arrow_reader')
previous = (logger.handlers[:], logger.level, logger.propagate)
logger.handlers = [Reenter()]
logger.setLevel(logging.INFO)
logger.propagate = False
",
                Some(&locals),
                None,
            )
            .unwrap();
            let (dispatch, _interrupts) = crate::logging::dispatch().unwrap();
            let address = (&mut stream as *mut FFI_ArrowArrayStream) as usize;
            py.detach(|| {
                tracing::dispatcher::with_default(&dispatch, || unsafe {
                    let stream = address as *mut FFI_ArrowArrayStream;
                    let mut array = FFI_ArrowArray::empty();
                    assert_eq!((*stream).get_next.unwrap()(stream, &mut array), 0);
                });
            });
            py.run(
                c"
logger.handlers, level, logger.propagate = previous
logger.setLevel(level)
assert results == [(22, b'reader operations cannot be called from a logging callback')] * 4
",
                Some(&locals),
                None,
            )
            .unwrap();
            assert!(stream.release.is_none());
            assert!(idle.release.is_none());
            assert!(gil_on_drop.load(Ordering::SeqCst));
            assert_eq!(dropped.load(Ordering::SeqCst), 2);
        });
    }
}
