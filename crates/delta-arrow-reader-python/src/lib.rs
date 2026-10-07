//! Python package entrypoint.

use std::sync::Arc;

use ::delta_arrow_reader::{DeltaTable as CoreDeltaTable, DeltaTableBuilder};
use pyo3::{
    exceptions::{PyException, PyRuntimeError},
    prelude::*,
};
use tokio::runtime::Runtime;

pyo3::create_exception!(
    delta_arrow_reader,
    DeltaReaderError,
    PyException,
    "A redacted reader failure with phase and code attributes."
);

/// One immutable Delta snapshot, loaded from a string or os.PathLike[str].
#[pyclass(module = "delta_arrow_reader", frozen)]
struct DeltaTable {
    table: CoreDeltaTable,
    // Keep the executor alive for the snapshot's storage engine.
    _runtime: Arc<Runtime>,
}

#[pymethods]
impl DeltaTable {
    #[new]
    fn new(py: Python<'_>, location: &Bound<'_, PyAny>) -> PyResult<Self> {
        let location: String = py
            .import("os")?
            .call_method1("fspath", (location,))?
            .extract()?;
        let runtime = Arc::new(
            Runtime::new()
                .map_err(|_| PyRuntimeError::new_err("failed to create the reader runtime"))?,
        );
        // ponytail: blocking wait; #417 adds signal checks and cleanup from runtime workers.
        let result = py.detach(|| runtime.block_on(DeltaTableBuilder::new(location).load_table()));
        let table = match result {
            Ok(table) => table,
            Err(error) => {
                let exception = DeltaReaderError::new_err(error.to_string());
                exception
                    .value(py)
                    .setattr("phase", error.phase().as_str())?;
                exception.value(py).setattr("code", error.code())?;
                return Err(exception);
            }
        };
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
