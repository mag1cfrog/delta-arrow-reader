//! Export a Rust batch reader to PyArrow through Arrow's C Stream interface.
//!
//! Arrow handles schemas, batches, and error strings. The C callbacks below
//! forward those operations through `with_native_stream`, which rejects logging
//! reentry and keeps the reader alive for the call. `release` removes the export's
//! ownership; active calls keep their own reference until they finish.
//!
//! Consumers must keep the exported C stream structure alive throughout each callback.
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

// Arrow callbacks and destructors must run after releasing this lock.
static OWNER_POINTER_LOCK: Mutex<()> = Mutex::new(());
type NativeStreamOwner = Arc<Mutex<FFI_ArrowArrayStream>>;

const EINVAL: c_int = 22;
const UNAVAILABLE: &std::ffi::CStr = c"Arrow stream is closed or already in use";

pub(crate) fn export(reader: Box<dyn RecordBatchReader + Send>) -> FFI_ArrowArrayStream {
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

unsafe extern "C" fn get_schema(
    exported_stream: *mut FFI_ArrowArrayStream,
    out_schema: *mut FFI_ArrowSchema,
) -> c_int {
    // SAFETY: Arrow supplies a live stream and writable output; access is guarded below.
    unsafe {
        with_native_stream(exported_stream, |native| {
            native.get_schema.unwrap()(native, out_schema)
        })
    }
    .unwrap_or(EINVAL)
}

unsafe extern "C" fn get_next(
    exported_stream: *mut FFI_ArrowArrayStream,
    out_array: *mut FFI_ArrowArray,
) -> c_int {
    // SAFETY: Same stream/output contract as get_schema.
    unsafe {
        with_native_stream(exported_stream, |native| {
            native.get_next.unwrap()(native, out_array)
        })
    }
    .unwrap_or(EINVAL)
}

unsafe extern "C" fn get_last_error(exported_stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    // SAFETY: Arrow supplies a live stream; its error pointer lasts until the next callback.
    unsafe {
        with_native_stream(exported_stream, |native| {
            native.get_last_error.unwrap()(native)
        })
    }
    .unwrap_or_else(|message| message.as_ptr())
}

// SAFETY: The consumer keeps the exported C stream structure alive for the call.
unsafe fn with_native_stream<T>(
    exported_stream: *mut FFI_ArrowArrayStream,
    call: impl FnOnce(&mut FFI_ArrowArrayStream) -> T,
) -> Result<T, &'static std::ffi::CStr> {
    // Reject before Arrow creates an exclusive borrow, including reads of idle streams.
    if crate::logging::is_logging_callback() {
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
    .ok_or(UNAVAILABLE)?;
    // Waiting here would deadlock a recursive call. The owner also keeps close from
    // dropping Arrow's reader while this call holds its exclusive borrow.
    let mut native_stream = active_owner.try_lock().map_err(|_| UNAVAILABLE)?;
    Ok(call(&mut native_stream))
}

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
    if crate::logging::is_logging_callback() {
        // A Python handler can call an Arrow consumer that releases the GIL without
        // updating PyO3's attach count. Restore the real GIL before dropping Py handles.
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
mod tests;
