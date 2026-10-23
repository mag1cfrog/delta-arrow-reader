//! Export a Rust batch reader to PyArrow through Arrow's C Stream interface.
//!
//! PyArrow obtains this stream through `RecordBatchStream.__arrow_c_stream__`.
//! It then calls the Rust functions below through C function pointers to get the
//! schema, read batches, retrieve errors, and close the stream. `extern "C"` sets
//! the calling convention PyArrow expects. `unsafe` means the caller must supply
//! valid pointers whose lifetimes Rust cannot check across this interface.
//!
//! Arrow's own stream handles schema, batch, and error conversion. Our outer
//! stream forwards calls through `with_native_stream`: a Python logging handler
//! can try to read or close a stream while a read is still running. We reject
//! reads from logging handlers and keep the reader alive until active calls finish.
//!
//! Consumers must keep the exported C stream structure alive throughout each
//! callback and serialize calls as required by the Arrow interface.
//! See [Arrow's C Stream interface](https://arrow.apache.org/docs/format/CStreamInterface.html).

use std::{
    ffi::{c_char, c_int},
    ptr,
    sync::{Arc, Mutex},
};

use arrow::{
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::FFI_ArrowArrayStream,
    record_batch::RecordBatchReader,
};

// Protect the boxed owner pointer while a call clones it or release removes it.
// Drop this lock before calling Arrow or destructors, which may invoke Python.
static OWNER_POINTER_LOCK: Mutex<()> = Mutex::new(());
type NativeStreamOwner = Arc<Mutex<FFI_ArrowArrayStream>>;

// C errno code for "invalid argument". 22 matches arrow-rs's private EINVAL
// constant in ffi_stream.rs. We return it when a stream call is rejected;
// get_last_error supplies the reason. Successful calls return 0.
const EINVAL: c_int = 22;
const STREAM_UNAVAILABLE_MESSAGE: &std::ffi::CStr = c"Arrow stream is closed or already in use";

/// Wrap Arrow's reader export with callbacks that guard against logging reentry.
///
/// `private_data` holds a boxed Arc: the exported stream owns one reference, and
/// each active callback clones it so closing the stream cannot destroy its reader.
pub(crate) fn export_reader(reader: Box<dyn RecordBatchReader + Send>) -> FFI_ArrowArrayStream {
    let native_stream = FFI_ArrowArrayStream::new(reader);
    let owner = Arc::new(Mutex::new(native_stream));
    let owner_pointer = Box::into_raw(Box::new(owner));
    FFI_ArrowArrayStream {
        get_schema: Some(get_schema),
        get_next: Some(get_next),
        get_last_error: Some(get_last_error),
        release: Some(release),
        private_data: owner_pointer.cast(),
    }
}

/// Write column names and types to `out_schema` without consuming a batch.
///
/// Returns 0 on success or an error code; `get_last_error` provides the message.
/// The consumer owns the returned schema and must call its release callback.
///
/// # Safety
/// `exported_stream` must point to a live structure produced by `export_reader`.
/// `out_schema` must point to separate, aligned, writable schema storage.
unsafe extern "C" fn get_schema(
    exported_stream: *mut FFI_ArrowArrayStream,
    out_schema: *mut FFI_ArrowSchema,
) -> c_int {
    // SAFETY: Arrow supplies a live stream and writable output; access is guarded below.
    unsafe {
        with_native_stream(exported_stream, |native| {
            native
                .get_schema
                .map_or(EINVAL, |callback| callback(native, out_schema))
        })
    }
    .unwrap_or(EINVAL)
}

/// Write the next record batch to `out_array` in Arrow's C representation.
///
/// Returns 0 for both a batch and end of stream. `out_array.release == None`
/// signals end of stream; otherwise the consumer must release the returned batch.
/// A nonzero return is an error, with details available through `get_last_error`.
///
/// # Safety
/// `exported_stream` must point to a live structure produced by `export_reader`.
/// `out_array` must point to separate, aligned, writable array storage.
unsafe extern "C" fn get_next(
    exported_stream: *mut FFI_ArrowArrayStream,
    out_array: *mut FFI_ArrowArray,
) -> c_int {
    // SAFETY: Same stream/output contract as get_schema.
    unsafe {
        with_native_stream(exported_stream, |native| {
            native
                .get_next
                .map_or(EINVAL, |callback| callback(native, out_array))
        })
    }
    .unwrap_or(EINVAL)
}

/// Return the error text after `get_schema` or `get_next` reports a failure.
///
/// The pointer is a borrowed, null-terminated UTF-8 string, or null if Arrow has
/// no message. The consumer must not free it and must copy it before another
/// stream callback can invalidate it. Rejected calls return a static message.
///
/// # Safety
/// `exported_stream` must point to a live structure produced by `export_reader`.
/// No other callback may run while the consumer reads the returned message.
unsafe extern "C" fn get_last_error(exported_stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    // SAFETY: Arrow supplies a live stream; its error pointer lasts until the next callback.
    unsafe {
        with_native_stream(exported_stream, |native| {
            native
                .get_last_error
                .map_or(STREAM_UNAVAILABLE_MESSAGE.as_ptr(), |callback| {
                    callback(native)
                })
        })
    }
    .unwrap_or_else(|message| message.as_ptr())
}

/// Keep Arrow's stream alive and exclusively borrowed while running one callback.
///
/// Reject logging reentry, closed streams, and a failed nonblocking lock attempt
/// before calling Arrow. The C callbacks translate this rejection into an error
/// code or message because the C interface cannot return a Rust `Result`.
///
/// # Safety
/// `exported_stream` must point to a live structure produced by `export_reader`;
/// its `private_data` must still be managed by this module.
unsafe fn with_native_stream<T>(
    exported_stream: *mut FFI_ArrowArrayStream,
    call: impl FnOnce(&mut FFI_ArrowArrayStream) -> T,
) -> Result<T, &'static std::ffi::CStr> {
    // Reject before Arrow creates an exclusive borrow, including reads of idle streams.
    if crate::logging::in_logging_callback() {
        return Err(crate::logging::REENTRY_MESSAGE);
    }
    let active_owner = {
        let _pointer_guard = OWNER_POINTER_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // SAFETY: The lock prevents release from freeing the boxed owner during this clone.
        unsafe {
            (*exported_stream)
                .private_data
                .cast::<NativeStreamOwner>()
                .as_ref()
                .cloned()
        }
    }
    .ok_or(STREAM_UNAVAILABLE_MESSAGE)?;
    // Waiting here would deadlock a recursive call. The owner also keeps close from
    // dropping Arrow's reader while this call holds its exclusive borrow.
    let mut native_stream = active_owner
        .try_lock()
        .map_err(|_| STREAM_UNAVAILABLE_MESSAGE)?;
    Ok(call(&mut native_stream))
}

/// Close the exported stream by giving up its reference to Arrow's reader.
///
/// Setting `release` to `None` marks the stream closed. Any active callback keeps
/// the reader alive until it returns. This frees neither the consumer's stream
/// structure nor previously returned schemas and batches; they have separate owners.
///
/// # Safety
/// `exported_stream` must point to a live, writable structure produced by `export_reader`.
/// The consumer must keep that structure alive until all active callbacks return.
unsafe extern "C" fn release(exported_stream: *mut FFI_ArrowArrayStream) {
    let released_owner = {
        let _pointer_guard = OWNER_POINTER_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        // SAFETY: Arrow supplies a live stream. Take the owner exactly once and
        // mark the outer stream released before any destructor can invoke logging.
        unsafe {
            let owner_pointer = ptr::replace(&mut (*exported_stream).private_data, ptr::null_mut());
            (*exported_stream).release = None;
            if owner_pointer.is_null() {
                return;
            }
            Box::from_raw(owner_pointer.cast::<NativeStreamOwner>())
        }
    };
    if crate::logging::in_logging_callback() {
        // PyArrow may release Python's global interpreter lock (GIL) without updating
        // PyO3's attach count. Reacquire it before reader cleanup drops Python objects.
        // SAFETY: The active logging callback keeps Python alive throughout this call.
        unsafe {
            let gil_state = pyo3::ffi::PyGILState_Ensure();
            drop(released_owner);
            pyo3::ffi::PyGILState_Release(gil_state);
        }
    } else {
        drop(released_owner);
    }
}

#[cfg(test)]
mod tests {
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
        let mut stream = export_reader(Box::new(CallbackReader {
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
                STREAM_UNAVAILABLE_MESSAGE
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
                        STREAM_UNAVAILABLE_MESSAGE
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
            ptr::write(stream, export_reader(Box::new(reader)));
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
            let mut exported = export_reader(Box::new(reader));
            unsafe {
                ptr::write(stream, FFI_ArrowArrayStream::from_raw(&mut exported));
                assert!(exported.release.is_none());
                for _ in 0..2 {
                    let mut array = FFI_ArrowArray::empty();
                    assert_eq!((*stream).get_next.unwrap()(stream, &mut array), EINVAL);
                    let error = CStr::from_ptr((*stream).get_last_error.unwrap()(stream));
                    if close_in_next {
                        assert_eq!(error, STREAM_UNAVAILABLE_MESSAGE);
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
                        let emit = || tracing::info!(target: "delta_arrow_reader", event = "test.active_read");
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
                let mut stream = export_reader(Box::new(reader));
                let gil_on_drop = Arc::new(AtomicBool::new(false));
                let observed_gil = Arc::clone(&gil_on_drop);
                let python_owner = PyDict::new(py).into_any().unbind();
                let mut idle = export_reader(Box::new(CallbackReader {
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
class ReentrantReadHandler(logging.Handler):
    def emit(self, record):
        # Rejected callbacks cannot touch the null output. CFUNCTYPE releases the GIL.
        for source in (stream, idle):
            for operation in (read, schema):
                results.append((operation(source, None), error(source)))
        close(idle)
        close(stream)
logger = logging.getLogger('delta_arrow_reader')
previous = (logger.handlers[:], logger.level, logger.propagate)
logger.handlers = [ReentrantReadHandler()]
logger.setLevel(logging.INFO)
logger.propagate = False
",
                    Some(&locals),
                    None,
                )
                .unwrap();
                let (dispatch, _interrupts) = crate::logging::new_call_dispatch().unwrap();
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
}
