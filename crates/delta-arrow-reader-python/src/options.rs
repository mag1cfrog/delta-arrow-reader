//! Immutable Python scan execution settings.

use delta_arrow_reader::{DeltaReaderError, DeltaScanExecutionOptions, ParquetReaderBackend};
use pyo3::{
    exceptions::{PyOverflowError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyInt},
};

fn usize_at_least(value: &Bound<'_, PyAny>, minimum: usize, name: &str) -> PyResult<usize> {
    if !value.is_instance_of::<PyInt>() || value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(format!(
            "{name} must be an integer, not bool"
        )));
    }
    // Validate the integer value even if a subclass overrides comparisons.
    let value = value
        .py()
        .get_type::<PyInt>()
        .call_method1("__index__", (value,))?;
    if value.lt(minimum)? {
        return Err(PyValueError::new_err(format!(
            "{name} must be >= {minimum}"
        )));
    }
    value.extract().map_err(|error: PyErr| {
        if error.is_instance_of::<PyOverflowError>(value.py()) {
            PyOverflowError::new_err(format!("{name} does not fit usize"))
        } else {
            error
        }
    })
}

fn optional_positive_usize(value: &Bound<'_, PyAny>, name: &str) -> PyResult<Option<usize>> {
    if value.is_none() {
        Ok(None)
    } else {
        usize_at_least(value, 1, name).map(Some)
    }
}

fn scan_read_limit(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    optional_positive_usize(value, "max_concurrent_file_reads_per_scan")
}

fn partition_read_limit(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    usize_at_least(value, 1, "max_concurrent_file_reads_per_partition")
}

fn output_buffer_batches(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    usize_at_least(value, 1, "output_buffer_batches_per_partition")
}

fn file_prefetch_depth(value: &Bound<'_, PyAny>) -> PyResult<usize> {
    usize_at_least(value, 0, "prefetch_files_per_partition")
}

fn parquet_metadata_size_hint(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    optional_positive_usize(value, "parquet_metadata_size_hint_bytes")
}

fn parquet_full_file_read_threshold(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    optional_positive_usize(value, "parquet_full_file_read_threshold_bytes")
}

fn option_error(error: DeltaReaderError) -> PyErr {
    Python::attach(|py| {
        crate::diagnostics::validation_error(
            &error,
            "execution_options",
            crate::core_error::<PyValueError>(py, &error),
        )
    })
}

/// Control how a scan reads files and buffers results.
///
/// Start with the defaults. If a query uses too many resources, try lowering
/// `max_concurrent_file_reads_per_scan`. It caps files read at once in one scan,
/// not network requests or total memory. A lower limit may slow the query.
///
/// Examples:
///     ```python
///     from delta_arrow_reader import ScanExecutionOptions
///
///     options = ScanExecutionOptions(max_concurrent_file_reads_per_scan=4)
///     with table.to_reader(execution_options=options, limit=10) as reader:
///         print(reader.read_all())
///     ```
///
/// Pass the options to `DeltaTable(...)` for direct-scan defaults, to `scan()`
/// or `to_reader()` for one scan, or to `Session.register_table()` for SQL.
/// An override replaces the entire options object. Instances are immutable;
/// create a new one to change settings. These resource limits do not change
/// the connection check's budget during table initialization.
///
/// Args:
///     parquet_backend: `"direct"` uses the reader's asynchronous Parquet backend.
///         `"delta_kernel"` delegates data-file reads to Delta Kernel.
///     max_concurrent_file_reads_per_scan: File-read limit across all partitions
///         of one scan. `None` derives it from the partition target times the
///         per-partition limit, capped at the concurrency capacity below.
///     max_concurrent_file_reads_per_partition: File-read limit for each group
///         of files processed together. Both this and the scan limit apply.
///     output_buffer_batches_per_partition: Batches each partition can queue
///         while waiting for the consumer. Larger buffers use more memory.
///     prefetch_files_per_partition: Upcoming files to prepare ahead of time
///         per partition. `0` disables prefetch. Direct backend only.
///     parquet_metadata_size_hint_bytes: Bytes to request initially from each
///         file's end when loading Parquet metadata. `None` disables the hint.
///         If the metadata is larger, the reader makes another request.
///         Direct backend only.
///     parquet_full_file_read_threshold_bytes: Read a file entirely into memory
///         when its size is at or below this threshold. `None` disables this.
///         This can reduce requests for small files but reads unneeded data
///         and uses more memory. Direct backend only.
///
/// When set, the file-read limits and output buffer size must be positive integers
/// no greater than `sys.maxsize >> 2`. Prefetch accepts nonnegative integers.
/// Byte settings accept positive integers or `None`. All integers must fit
/// `2 * sys.maxsize + 1`; booleans are not accepted.
///
/// Raises:
///     TypeError: A setting has the wrong type, including a Boolean for an integer.
///     ValueError: A setting is out of range or the backend name is unknown.
///     OverflowError: An integer exceeds `2 * sys.maxsize + 1`.
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
        parquet_metadata_size_hint_bytes=DeltaScanExecutionOptions::new().parquet_metadata_size_hint_bytes(),
        parquet_full_file_read_threshold_bytes=DeltaScanExecutionOptions::new().parquet_full_file_read_threshold_bytes(),
    ))]
    #[pyo3(text_signature = "(*, parquet_backend='direct', \
        max_concurrent_file_reads_per_scan=None, \
        max_concurrent_file_reads_per_partition=3, \
        output_buffer_batches_per_partition=1, \
        prefetch_files_per_partition=2, \
        parquet_metadata_size_hint_bytes=65536, \
        parquet_full_file_read_threshold_bytes=None)")]
    fn new(
        parquet_backend: &str,
        #[pyo3(from_py_with = scan_read_limit)] max_concurrent_file_reads_per_scan: Option<usize>,
        #[pyo3(from_py_with = partition_read_limit)] max_concurrent_file_reads_per_partition: usize,
        #[pyo3(from_py_with = output_buffer_batches)] output_buffer_batches_per_partition: usize,
        #[pyo3(from_py_with = file_prefetch_depth)] prefetch_files_per_partition: usize,
        #[pyo3(from_py_with = parquet_metadata_size_hint)] parquet_metadata_size_hint_bytes: Option<
            usize,
        >,
        #[pyo3(from_py_with = parquet_full_file_read_threshold)]
        parquet_full_file_read_threshold_bytes: Option<usize>,
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
                .map_err(option_error)?
                .with_max_concurrent_file_reads_per_partition(
                    max_concurrent_file_reads_per_partition,
                )
                .map_err(option_error)?
                .with_output_buffer_batches_per_partition(output_buffer_batches_per_partition)
                .map_err(option_error)?
                .with_prefetch_files_per_partition(prefetch_files_per_partition)
                .with_parquet_metadata_size_hint_bytes(parquet_metadata_size_hint_bytes)
                .map_err(option_error)?
                .with_parquet_full_file_read_threshold_bytes(parquet_full_file_read_threshold_bytes)
                .map_err(option_error)?,
        })
    }

    /// Selected Parquet backend: `"direct"` or `"delta_kernel"`.
    #[getter]
    fn parquet_backend(&self) -> &'static str {
        match self.options.parquet_backend() {
            ParquetReaderBackend::Direct => "direct",
            ParquetReaderBackend::DeltaKernel => "delta_kernel",
        }
    }

    /// File-read limit for a whole scan, or `None` to derive it automatically.
    #[getter]
    fn max_concurrent_file_reads_per_scan(&self) -> Option<usize> {
        self.options.max_concurrent_file_reads_per_scan()
    }

    /// Maximum number of files each scan partition can read at once.
    #[getter]
    fn max_concurrent_file_reads_per_partition(&self) -> usize {
        self.options.max_concurrent_file_reads_per_partition()
    }

    /// Maximum number of output batches queued per partition.
    #[getter]
    fn output_buffer_batches_per_partition(&self) -> usize {
        self.options.output_buffer_batches_per_partition()
    }

    /// Number of upcoming files prepared per partition; `0` disables prefetch.
    #[getter]
    fn prefetch_files_per_partition(&self) -> usize {
        self.options.prefetch_files_per_partition()
    }

    /// Initial Parquet metadata request size in bytes, or `None` for no hint.
    #[getter]
    fn parquet_metadata_size_hint_bytes(&self) -> Option<usize> {
        self.options.parquet_metadata_size_hint_bytes()
    }

    /// Largest file to buffer entirely, in bytes, or `None` to disable this.
    #[getter]
    fn parquet_full_file_read_threshold_bytes(&self) -> Option<usize> {
        self.options.parquet_full_file_read_threshold_bytes()
    }
}
