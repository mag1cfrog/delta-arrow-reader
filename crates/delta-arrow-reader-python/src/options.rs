//! Immutable Python scan execution settings.

use delta_arrow_reader::{DeltaScanExecutionOptions, ParquetReaderBackend};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyInt},
};

fn positive_usize(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected an integer, not bool"));
    }
    // The builtin rejects non-integers and bypasses subclass overrides.
    let value = value
        .py()
        .get_type::<PyInt>()
        .call_method1("__index__", (value,))?;
    if value.le(0)? {
        return Err(PyValueError::new_err("expected a positive integer"));
    }
    value.extract()
}

fn optional_positive_usize(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    if value.is_none() {
        Ok(None)
    } else {
        positive_usize(value).map(Some)
    }
}

/// Immutable execution settings shared by tables and individual scans.
///
/// max_concurrent_file_reads_per_partition defaults to 3 and must be a positive
/// integer within the core's concurrency capacity. Booleans are not accepted.
/// max_concurrent_file_reads_per_scan accepts the same integer range or None.
/// Its default, None, derives the total from the partition target and limit.
#[pyclass(module = "delta_arrow_reader", frozen)]
pub(crate) struct ScanExecutionOptions {
    pub(crate) options: DeltaScanExecutionOptions,
}

#[pymethods]
impl ScanExecutionOptions {
    #[new]
    #[pyo3(signature = (
        *,
        parquet_backend="direct",
        max_concurrent_file_reads_per_scan=DeltaScanExecutionOptions::new().max_concurrent_file_reads_per_scan(),
        max_concurrent_file_reads_per_partition=DeltaScanExecutionOptions::new().max_concurrent_file_reads_per_partition(),
    ))]
    fn new(
        parquet_backend: &str,
        #[pyo3(from_py_with = optional_positive_usize)] max_concurrent_file_reads_per_scan: Option<
            usize,
        >,
        #[pyo3(from_py_with = positive_usize)] max_concurrent_file_reads_per_partition: usize,
    ) -> PyResult<Self> {
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
            options: DeltaScanExecutionOptions::new()
                .with_parquet_backend(backend)
                .with_max_concurrent_file_reads_per_scan(max_concurrent_file_reads_per_scan)
                .map_err(|error| PyValueError::new_err(error.to_string()))?
                .with_max_concurrent_file_reads_per_partition(
                    max_concurrent_file_reads_per_partition,
                )
                .map_err(|error| PyValueError::new_err(error.to_string()))?,
        })
    }

    #[getter]
    fn parquet_backend(&self) -> &'static str {
        match self.options.parquet_backend() {
            ParquetReaderBackend::Direct => "direct",
            ParquetReaderBackend::DeltaKernel => "delta_kernel",
        }
    }

    #[getter]
    fn max_concurrent_file_reads_per_scan(&self) -> Option<usize> {
        self.options.max_concurrent_file_reads_per_scan()
    }

    #[getter]
    fn max_concurrent_file_reads_per_partition(&self) -> usize {
        self.options.max_concurrent_file_reads_per_partition()
    }
}
