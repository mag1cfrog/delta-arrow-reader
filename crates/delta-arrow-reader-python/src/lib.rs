//! Python package entrypoint.

mod runtime;

use std::sync::Arc;

use ::delta_arrow_reader::{
    DeltaSnapshotSelection, DeltaStorageOptions, DeltaTable as CoreDeltaTable, DeltaTableBuilder,
};
use arrow::ffi::FFI_ArrowSchema;
use pyo3::{
    exceptions::{PyException, PyRuntimeError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyCapsule, PyDict, PyInt, PyMapping},
};

use crate::runtime::Runtime;

pyo3::create_exception!(
    delta_arrow_reader,
    DeltaReaderError,
    PyException,
    "A redacted reader failure with phase and code attributes."
);

fn reader_error(py: Python<'_>, message: String, phase: &str, code: &str) -> PyErr {
    let exception = DeltaReaderError::new_err(message);
    let value = exception.value(py);
    if let Err(error) = value
        .setattr("phase", phase)
        .and_then(|()| value.setattr("code", code))
    {
        return error;
    }
    exception
}

/// One immutable Delta snapshot, loaded from a string or os.PathLike[str].
///
/// With version=None, load the latest snapshot. Otherwise, version must be an
/// integer from 0 to 2**64 - 1. Booleans are not accepted.
/// storage_options accepts a mapping of string keys to string values.
#[pyclass(module = "delta_arrow_reader", frozen)]
struct DeltaTable {
    table: CoreDeltaTable,
    // Keep the executor alive for the snapshot's storage engine.
    _runtime: Arc<Runtime>,
}

#[pymethods]
impl DeltaTable {
    #[new]
    #[pyo3(signature = (location, *, version=None, storage_options=None))]
    fn new(
        py: Python<'_>,
        location: &Bound<'_, PyAny>,
        version: Option<&Bound<'_, PyInt>>,
        storage_options: Option<&Bound<'_, PyMapping>>,
    ) -> PyResult<Self> {
        let location: String = py
            .import("os")?
            .call_method1("fspath", (location,))?
            .extract()?;
        let selection = match version {
            None => DeltaSnapshotSelection::Latest,
            Some(version) => {
                if version.is_instance_of::<PyBool>() {
                    return Err(PyTypeError::new_err("version must be an integer, not bool"));
                }
                // Validate the integer value even if a subclass overrides comparisons.
                let version = py
                    .get_type::<PyInt>()
                    .call_method1("__index__", (version,))?;
                if version.lt(0)? {
                    return Err(PyValueError::new_err("version must be nonnegative"));
                }
                DeltaSnapshotSelection::Version(version.extract::<u64>()?)
            }
        };
        let options: DeltaStorageOptions = match storage_options {
            Some(options) => py.get_type::<PyDict>().call1((options,))?.extract()?,
            None => DeltaStorageOptions::new(),
        };
        let builder = DeltaTableBuilder::new(location)
            .with_snapshot_selection(selection)
            .with_storage_options(options);
        let runtime = Arc::new(
            Runtime::new()
                .map_err(|_| PyRuntimeError::new_err("failed to create the reader runtime"))?,
        );
        let table = runtime.wait(py, builder.load_table())?.map_err(|error| {
            reader_error(py, error.to_string(), error.phase().as_str(), error.code())
        })?;
        Ok(Self {
            table,
            _runtime: runtime,
        })
    }

    /// The loaded snapshot version.
    #[getter]
    fn version(&self) -> u64 {
        self.table.version()
    }

    /// The logical schema as a pyarrow.Schema, independent of this table's lifetime.
    #[getter]
    fn schema(slf: Bound<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        slf.py().import("pyarrow")?.call_method1("schema", (slf,))
    }

    /// Export a fresh Arrow schema capsule through the public Arrow protocol.
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let schema = self.table.schema();
        let export_error = || {
            reader_error(
                py,
                "delta reader error: phase=schema code=schema_conversion reason=arrow_schema_export_failed"
                    .to_owned(),
                "schema",
                "schema_conversion",
            )
        };
        // Arrow 58's FFI exporter panics on NUL bytes in field names.
        if schema
            .flattened_fields()
            .iter()
            .any(|field| field.name().contains('\0'))
        {
            return Err(export_error());
        }
        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref()).map_err(|_| export_error())?;
        // The capsule drops the schema; Arrow's Drop releases it only if still owned.
        PyCapsule::new_with_value(py, ffi_schema, c"arrow_schema")
    }

    fn __repr__(&self) -> String {
        format!("DeltaTable(version={})", self.version())
    }
}

#[pymodule]
fn delta_arrow_reader(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Wheel metadata already contains Maturin's normalized Python version.
    let version = module
        .py()
        .import("importlib.metadata")?
        .call_method1("version", ("delta-arrow-reader",))?;
    module.add("__version__", version)?;
    module.add_class::<DeltaTable>()?;
    module.add(
        "DeltaReaderError",
        module.py().get_type::<DeltaReaderError>(),
    )?;
    module.add("__all__", ["__version__", "DeltaTable", "DeltaReaderError"])
}
