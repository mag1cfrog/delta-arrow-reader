//! Immutable Python scan execution settings.

use delta_arrow_reader::{DeltaScanExecutionOptions, ParquetReaderBackend};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyInt},
};

fn usize_at_least(value: &Bound<'_, PyAny>, minimum: usize) -> PyResult<usize> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected an integer, not bool"));
    }
    // The builtin rejects non-integers and bypasses subclass overrides.
    let value = value
        .py()
        .get_type::<PyInt>()
        .call_method1("__index__", (value,))?;
    if value.lt(minimum)? {
        return Err(PyValueError::new_err(format!(
            "expected an integer >= {minimum}"
        )));
    }
    value.extract()
}

fn positive_usize(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    usize_at_least(value, 1)
}

fn nonnegative_usize(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    usize_at_least(value, 0)
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
/// output_buffer_batches_per_partition defaults to 1 queued batch per partition
/// and accepts the same positive integer range as the per-partition read limit.
/// prefetch_files_per_partition defaults to 2 future files for the direct backend.
/// It accepts nonnegative integers fitting usize; 0 disables file prefetch.
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
        output_buffer_batches_per_partition=DeltaScanExecutionOptions::new().output_buffer_batches_per_partition(),
        prefetch_files_per_partition=DeltaScanExecutionOptions::new().prefetch_files_per_partition(),
    ))]
    fn new(
        parquet_backend: &str,
        #[pyo3(from_py_with = optional_positive_usize)] max_concurrent_file_reads_per_scan: Option<
            usize,
        >,
        #[pyo3(from_py_with = positive_usize)] max_concurrent_file_reads_per_partition: usize,
        #[pyo3(from_py_with = positive_usize)] output_buffer_batches_per_partition: usize,
        #[pyo3(from_py_with = nonnegative_usize)] prefetch_files_per_partition: usize,
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
                .map_err(|error| PyValueError::new_err(error.to_string()))?
                .with_output_buffer_batches_per_partition(output_buffer_batches_per_partition)
                .map_err(|error| PyValueError::new_err(error.to_string()))?
                .with_prefetch_files_per_partition(prefetch_files_per_partition),
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

    #[getter]
    fn output_buffer_batches_per_partition(&self) -> usize {
        self.options.output_buffer_batches_per_partition()
    }

    #[getter]
    fn prefetch_files_per_partition(&self) -> usize {
        self.options.prefetch_files_per_partition()
    }
}
