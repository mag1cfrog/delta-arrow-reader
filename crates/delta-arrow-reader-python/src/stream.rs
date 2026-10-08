use std::sync::{Arc, Mutex};

use arrow::{
    datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    error::ArrowError,
    ffi::FFI_ArrowSchema,
    ffi_stream::FFI_ArrowArrayStream,
    record_batch::{RecordBatch, RecordBatchReader},
};
use delta_arrow_reader::DeltaBatchStream;
use pyo3::{
    exceptions::{PyNotImplementedError, PyRuntimeError, PyTypeError, PyValueError},
    prelude::*,
    types::PyCapsule,
};

use crate::runtime::Runtime;

/// Single-use Arrow stream exporter, created by DeltaTable.scan().
///
/// Pass this object to pyarrow.RecordBatchReader.from_stream(). After export,
/// the consumer owns cleanup; closing this object does not close the consumer.
#[pyclass(module = "delta_arrow_reader", frozen)]
pub(crate) struct RecordBatchStream {
    reader: Mutex<Option<BlockingBatchReader>>,
}

impl RecordBatchStream {
    pub(crate) fn new(stream: DeltaBatchStream, runtime: Arc<Runtime>) -> Self {
        Self {
            reader: Mutex::new(Some(BlockingBatchReader {
                schema: stream.schema(),
                stream: Some(stream),
                runtime,
                terminal_error: None,
            })),
        }
    }
}

#[pymethods]
impl RecordBatchStream {
    /// Export once, accepting None or an identical requested schema capsule.
    ///
    /// Incompatible schemas raise ValueError; alternate representations raise
    /// NotImplementedError. A rejected request leaves the stream available.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let mut slot = self
            .reader
            .lock()
            .map_err(|_| PyRuntimeError::new_err("stream is unavailable"))?;
        let reader = slot
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("stream has already been exported or closed"))?;
        if let Some(requested) = requested_schema {
            validate_requested_schema(requested, &reader.schema)?;
        }
        let reader = slot.take().expect("stream ownership checked under lock");
        drop(slot);
        PyCapsule::new_with_value(
            py,
            FFI_ArrowArrayStream::new(Box::new(reader)),
            c"arrow_array_stream",
        )
    }

    /// Release an unexported stream. Repeated calls have no effect.
    fn close(&self) -> PyResult<()> {
        let reader = self
            .reader
            .lock()
            .map_err(|_| PyRuntimeError::new_err("stream is unavailable"))?
            .take();
        // Drop scan resources after releasing the mutex.
        drop(reader);
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.close()
    }
}

fn validate_requested_schema(requested: &Bound<'_, PyAny>, actual: &Schema) -> PyResult<()> {
    let capsule = requested.cast::<PyCapsule>().map_err(|_| {
        PyTypeError::new_err("requested_schema must be an arrow_schema capsule or None")
    })?;
    let pointer = capsule
        .pointer_checked(Some(c"arrow_schema"))
        .map_err(|_| PyValueError::new_err("invalid requested schema capsule"))?;
    // SAFETY: The named capsule promises an ArrowSchema and remains alive here.
    // Borrow without consuming it; no Python code runs while reading its fields.
    let ffi_schema = unsafe { pointer.cast::<FFI_ArrowSchema>().as_ref() };
    if ffi_schema.release.is_none() || ffi_schema.format.is_null() {
        return Err(PyValueError::new_err(
            "requested schema is released or invalid",
        ));
    }
    let requested = Schema::try_from(ffi_schema)
        .map_err(|_| PyValueError::new_err("invalid requested schema"))?;
    let same_metadata = actual.metadata == requested.metadata;
    if same_metadata && matches_requested_fields(&actual.fields, &requested.fields, false) {
        return Ok(());
    }
    if same_metadata && matches_requested_fields(&actual.fields, &requested.fields, true) {
        return Err(PyNotImplementedError::new_err(
            "alternate Arrow representations are not supported",
        ));
    }
    Err(PyValueError::new_err(
        "requested schema is incompatible with the scan schema",
    ))
}

// Alternate representations only select an error category; the exporter never casts.
fn matches_requested_fields(actual: &Fields, requested: &Fields, allow_alternates: bool) -> bool {
    actual.len() == requested.len()
        && actual
            .iter()
            .zip(requested)
            .all(|(a, b)| matches_requested_field(a, b, allow_alternates))
}

fn matches_requested_field(actual: &Field, requested: &Field, allow_alternates: bool) -> bool {
    actual.name() == requested.name()
        && actual.is_nullable() == requested.is_nullable()
        && actual.metadata() == requested.metadata()
        && matches_requested_type(actual.data_type(), requested.data_type(), allow_alternates)
}

fn matches_requested_type(actual: &DataType, requested: &DataType, allow_alternates: bool) -> bool {
    use DataType::*;
    if actual == requested {
        return true;
    }
    match (actual, requested) {
        (Struct(a), Struct(b)) => matches_requested_fields(a, b, allow_alternates),
        (List(a), List(b)) => matches_requested_field(a, b, allow_alternates),
        (Map(a, a_sorted), Map(b, b_sorted)) => {
            // PyArrow normalizes the map wrapper name from "key_value" to "entries".
            // Compare the key/value fields, wrapper metadata, and nullability.
            a_sorted == b_sorted
                && a.is_nullable() == b.is_nullable()
                && a.metadata() == b.metadata()
                && matches_requested_type(a.data_type(), b.data_type(), allow_alternates)
        }
        _ if !allow_alternates => false,
        (_, Dictionary(_, value)) => matches_requested_type(actual, value, true),
        (_, RunEndEncoded(_, values)) => matches_requested_type(actual, values.data_type(), true),
        (List(a), LargeList(b) | ListView(b) | LargeListView(b) | FixedSizeList(b, _)) => {
            matches_requested_field(a, b, true)
        }
        (Timestamp(_, a_zone), Timestamp(_, b_zone)) => a_zone == b_zone,
        (Date32, Date64) => true,
        (
            Decimal128(a_precision, a_scale),
            Decimal32(b_precision, b_scale)
            | Decimal64(b_precision, b_scale)
            | Decimal128(b_precision, b_scale)
            | Decimal256(b_precision, b_scale),
        ) => a_precision == b_precision && a_scale == b_scale,
        _ => {
            (actual.is_integer() && requested.is_integer())
                || (actual.is_floating() && requested.is_floating())
                || (actual.is_string() && requested.is_string())
                || (actual.is_binary() && requested.is_binary())
        }
    }
}

/// Adapts an async Delta stream to Arrow's synchronous reader callbacks.
struct BlockingBatchReader {
    schema: SchemaRef,
    stream: Option<DeltaBatchStream>,
    runtime: Arc<Runtime>,
    // Repeat failures so a failed scan never appears successfully exhausted.
    terminal_error: Option<String>,
}

impl Iterator for BlockingBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(message) = &self.terminal_error {
            return Some(Err(ArrowError::ExternalError(message.clone().into())));
        }
        let stream = self.stream.as_mut()?;
        let result = Python::attach(|py| {
            self.runtime.wait(py, stream.next_batch()).map_err(|_| {
                "delta reader error: phase=execution code=python_signal reason=reader_wait_interrupted"
                    .to_owned()
            })
        })
        .and_then(|result| result.map_err(|error| error.to_string()));
        match result {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                self.stream = None;
                None
            }
            Err(message) => {
                self.stream = None;
                self.terminal_error = Some(message.clone());
                Some(Err(ArrowError::ExternalError(message.into())))
            }
        }
    }
}

impl RecordBatchReader for BlockingBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
