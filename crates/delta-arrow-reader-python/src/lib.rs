//! Python package entrypoint.

use pyo3::prelude::*;

#[pymodule]
fn delta_arrow_reader(module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Wheel metadata already contains Maturin's normalized Python version.
    let version = module
        .py()
        .import("importlib.metadata")?
        .call_method1("version", ("delta-arrow-reader",))?;
    module.add("__version__", version)?;
    module.add("__all__", ["__version__"])
}
