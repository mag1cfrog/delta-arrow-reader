use std::sync::{Arc, Mutex};

use arrow::{
    datatypes::SchemaRef,
    error::ArrowError,
    ffi_stream::FFI_ArrowArrayStream,
    record_batch::{RecordBatch, RecordBatchReader},
};
use delta_arrow_reader::DeltaBatchStream;
use pyo3::{
    exceptions::{PyNotImplementedError, PyRuntimeError},
    prelude::*,
    types::PyCapsule,
};

use crate::runtime::Runtime;

/// Single-use exporter consumed by PyArrow's public from_stream method.
#[pyclass(module = "delta_arrow_reader", frozen)]
pub(crate) struct RecordBatchStream {
    reader: Mutex<Option<BatchReader>>,
}

impl RecordBatchStream {
    pub(crate) fn new(stream: DeltaBatchStream, runtime: Arc<Runtime>) -> Self {
        Self {
            reader: Mutex::new(Some(BatchReader {
                schema: stream.schema(),
                stream: Some(stream),
                runtime,
                error: None,
            })),
        }
    }
}

#[pymethods]
impl RecordBatchStream {
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        if requested_schema.is_some() {
            return Err(PyNotImplementedError::new_err(
                "schema requests are not supported",
            ));
        }
        let reader = self
            .reader
            .lock()
            .map_err(|_| PyRuntimeError::new_err("stream is unavailable"))?
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("stream has already been exported"))?;
        PyCapsule::new_with_value(
            py,
            FFI_ArrowArrayStream::new(Box::new(reader)),
            c"arrow_array_stream",
        )
    }
}

struct BatchReader {
    schema: SchemaRef,
    stream: Option<DeltaBatchStream>,
    runtime: Arc<Runtime>,
    error: Option<String>,
}

impl Iterator for BatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(message) = &self.error {
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
                self.error = Some(message.clone());
                Some(Err(ArrowError::ExternalError(message.into())))
            }
        }
    }
}

impl RecordBatchReader for BatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
