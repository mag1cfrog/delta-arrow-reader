//! Immutable Python scan execution settings.

use delta_arrow_reader::{DeltaScanExecutionOptions, ParquetReaderBackend};
use pyo3::{exceptions::PyValueError, prelude::*};

/// Immutable execution settings shared by tables and individual scans.
#[pyclass(module = "delta_arrow_reader", frozen)]
pub(crate) struct ScanExecutionOptions {
    pub(crate) options: DeltaScanExecutionOptions,
}

#[pymethods]
impl ScanExecutionOptions {
    #[new]
    #[pyo3(signature = (*, parquet_backend="direct"))]
    fn new(parquet_backend: &str) -> PyResult<Self> {
        let backend = match parquet_backend {
            "direct" => ParquetReaderBackend::Direct,
            "delta_kernel" => ParquetReaderBackend::DeltaKernel,
            _ => {
                return Err(PyValueError::new_err(
                    "parquet_backend must be 'direct' or 'delta_kernel'",
                ));
            }
        };
        Ok(Self {
            options: DeltaScanExecutionOptions::new().with_parquet_backend(backend),
        })
    }

    #[getter]
    fn parquet_backend(&self) -> &'static str {
        match self.options.parquet_backend() {
            ParquetReaderBackend::Direct => "direct",
            ParquetReaderBackend::DeltaKernel => "delta_kernel",
        }
    }
}
