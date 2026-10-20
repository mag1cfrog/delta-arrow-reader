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
const UNAVAILABLE: &std::ffi::CStr = c"Arrow stream is closed or already in use";

/// Wrap Arrow's reader export with callbacks that guard against logging reentry.
///
/// `private_data` holds a boxed Arc: the exported stream owns one reference, and
/// each active callback clones it so closing the stream cannot destroy its reader.
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

/// Write column names and types to `out_schema` without consuming a batch.
///
/// Returns 0 on success or an error code; `get_last_error` provides the message.
/// The consumer owns the returned schema and must call its release callback.
///
/// # Safety
/// `exported_stream` must point to a live structure produced by `export`.
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
/// `exported_stream` must point to a live structure produced by `export`.
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
/// `exported_stream` must point to a live structure produced by `export`.
/// No other callback may run while the consumer reads the returned message.
unsafe extern "C" fn get_last_error(exported_stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    // SAFETY: Arrow supplies a live stream; its error pointer lasts until the next callback.
    unsafe {
        with_native_stream(exported_stream, |native| {
            native
                .get_last_error
                .map_or(UNAVAILABLE.as_ptr(), |callback| callback(native))
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
/// `exported_stream` must point to a live structure produced by `export`;
/// its `private_data` must still be managed by this module.
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

/// Close the exported stream by giving up its reference to Arrow's reader.
///
/// Setting `release` to `None` marks the stream closed. Any active callback keeps
/// the reader alive until it returns. This frees neither the consumer's stream
/// structure nor previously returned schemas and batches; they have separate owners.
///
/// # Safety
/// `exported_stream` must point to a live, writable structure produced by `export`.
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
    if crate::logging::is_logging_callback() {
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
mod tests;
