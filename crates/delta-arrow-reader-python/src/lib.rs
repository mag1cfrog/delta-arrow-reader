//! Python package entrypoint.

mod filters;
mod options;
mod runtime;
mod stream;

use std::sync::Arc;

use ::delta_arrow_reader::{
    DeltaSnapshotSelection, DeltaStorageOptions, DeltaTable as CoreDeltaTable, DeltaTableBuilder,
    WarmupMode,
};
use arrow::{datatypes::Schema, ffi::FFI_ArrowSchema};
use pyo3::{
    exceptions::{PyException, PyRuntimeError, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyCapsule, PyDict, PyInt, PyList, PyMapping, PyTuple},
};

use crate::options::ScanExecutionOptions;
use crate::runtime::Runtime;
use crate::stream::RecordBatchStream;

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

fn export_schema(py: Python<'_>, schema: &Schema) -> PyResult<FFI_ArrowSchema> {
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
    FFI_ArrowSchema::try_from(schema).map_err(|_| export_error())
}

/// One immutable Delta snapshot, loaded from a string or os.PathLike[str].
///
/// With version=None, load the latest snapshot. Otherwise, version must be an
/// integer from 0 to 2**64 - 1. Booleans are not accepted.
/// storage_options accepts a mapping of string keys to string values.
/// warmup="none" defers planning metadata to scans; "query_planning" prepares
/// reusable planning metadata during loading without reading Parquet data.
/// execution_options=None keeps the default scan execution settings.
#[pyclass(module = "delta_arrow_reader", frozen)]
struct DeltaTable {
    table: CoreDeltaTable,
    // Keep the executor alive for the snapshot's storage engine.
    runtime: Arc<Runtime>,
}

#[pymethods]
impl DeltaTable {
    #[new]
    #[pyo3(signature = (location, *, version=None, storage_options=None, warmup="none", execution_options=None))]
    fn new(
        py: Python<'_>,
        location: &Bound<'_, PyAny>,
        version: Option<&Bound<'_, PyInt>>,
        storage_options: Option<&Bound<'_, PyMapping>>,
        warmup: &str,
        execution_options: Option<PyRef<'_, ScanExecutionOptions>>,
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
        let warmup = match warmup {
            "none" => WarmupMode::None,
            "query_planning" => WarmupMode::QueryPlanning,
            _ => {
                return Err(PyValueError::new_err(
                    "warmup must be 'none' or 'query_planning'",
                ));
            }
        };
        let mut builder = DeltaTableBuilder::new(location)
            .with_snapshot_selection(selection)
            .with_storage_options(options)
            .with_warmup(warmup);
        if let Some(options) = execution_options {
            builder = builder.with_execution_options(options.options);
        }
        let runtime = Arc::new(
            Runtime::new()
                .map_err(|_| PyRuntimeError::new_err("failed to create the reader runtime"))?,
        );
        let table = runtime.wait(py, builder.load_table())?.map_err(|error| {
            reader_error(py, error.to_string(), error.phase().as_str(), error.code())
        })?;
        Ok(Self { table, runtime })
    }

    /// Load the latest snapshot and return it as a new table.
    ///
    /// This table and its existing scans keep their original snapshot, including
    /// when refresh fails. The new table shares this table's runtime.
    fn refresh(&self, py: Python<'_>) -> PyResult<Self> {
        let table = self
            .runtime
            .wait(py, self.table.refresh())?
            .map_err(|error| {
                reader_error(py, error.to_string(), error.phase().as_str(), error.code())
            })?;
        Ok(Self {
            table,
            runtime: Arc::clone(&self.runtime),
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
        let ffi_schema = export_schema(py, self.table.schema().as_ref())?;
        // The capsule drops the schema; Arrow's Drop releases it only if still owned.
        PyCapsule::new_with_value(py, ffi_schema, c"arrow_schema")
    }

    /// Plan a scan and return a single-use Arrow stream exporter.
    ///
    /// columns=None selects all columns. A list or tuple of names selects columns
    /// in that order; an empty list selects no columns while retaining row counts.
    /// filters=None or [] disables filtering. A list of (column, operator, value)
    /// tuples combines conditions with AND; a list of lists combines AND groups
    /// with OR. An empty AND group is true. "is" and "is not" require None.
    /// Comparisons (==, !=, <, <=, >, >=) accept bool for Boolean columns and int
    /// for signed integer columns. Integers must fit the column's bit width;
    /// booleans cannot be used as integers. Comparisons with None are invalid.
    /// Float columns require finite float values. Float32 rounds to its precision
    /// and rejects overflow. Native Arrow comparisons distinguish -0.0 from 0.0.
    /// String columns require str values encodable as UTF-8. Binary columns
    /// require bytes; fixed-size binary values must match the column's width.
    /// Decimal128 columns require finite decimal.Decimal values exactly fitting
    /// the column's precision and scale, independent of the decimal context.
    /// Date32 columns require datetime.date values; datetime.datetime is rejected.
    /// Microsecond timestamps require naive datetime.datetime values for columns
    /// without a timezone, and aware values for columns with a timezone. Conversion
    /// preserves microsecond precision and does not use the process timezone.
    /// Filter columns need not appear in columns. Filters apply before limit.
    /// limit=None reads all rows. Otherwise, limit must be a nonnegative integer
    /// that fits the platform's usize. Booleans are not accepted.
    /// target_partitions=None uses automatic partition planning. An override must
    /// be a positive integer that fits usize; booleans are not accepted.
    /// execution_options=None inherits the table's settings. A supplied object
    /// replaces the complete settings for this scan without changing the table.
    /// Planning reads Delta metadata; data-file reads start on the first pull.
    /// The stream retains its snapshot and runtime independently of this table.
    #[pyo3(signature = (*, columns=None, filters=None, limit=None, target_partitions=None, execution_options=None))]
    fn scan(
        &self,
        py: Python<'_>,
        columns: Option<&Bound<'_, PyAny>>,
        filters: Option<&Bound<'_, PyAny>>,
        limit: Option<&Bound<'_, PyInt>>,
        target_partitions: Option<&Bound<'_, PyInt>>,
        execution_options: Option<PyRef<'_, ScanExecutionOptions>>,
    ) -> PyResult<RecordBatchStream> {
        let mut builder = self.table.scan();
        if let Some(options) = execution_options {
            builder = builder.with_execution_options(options.options);
        }
        if let Some(columns) = columns {
            if !columns.is_instance_of::<PyList>() && !columns.is_instance_of::<PyTuple>() {
                return Err(PyTypeError::new_err(
                    "columns must be a list or tuple of strings, or None",
                ));
            }
            builder = builder.with_projection(columns.extract::<Vec<String>>()?);
        }
        if let Some(filters) = filters
            && let Some(predicate) = filters::to_predicate(&self.table, filters)?
        {
            builder = builder.with_predicate(predicate);
        }
        if let Some(limit) = limit {
            if limit.is_instance_of::<PyBool>() {
                return Err(PyTypeError::new_err("limit must be an integer, not bool"));
            }
            // Validate the integer value even if a subclass overrides comparisons.
            let limit = py.get_type::<PyInt>().call_method1("__index__", (limit,))?;
            if limit.lt(0)? {
                return Err(PyValueError::new_err("limit must be nonnegative"));
            }
            builder = builder.with_limit(limit.extract::<usize>()?);
        }
        if let Some(target_partitions) = target_partitions {
            if target_partitions.is_instance_of::<PyBool>() {
                return Err(PyTypeError::new_err(
                    "target_partitions must be an integer, not bool",
                ));
            }
            // Validate the integer value even if a subclass overrides comparisons.
            let target_partitions = py
                .get_type::<PyInt>()
                .call_method1("__index__", (target_partitions,))?;
            if target_partitions.le(0)? {
                return Err(PyValueError::new_err("target_partitions must be positive"));
            }
            builder = builder
                .with_target_partitions(target_partitions.extract::<usize>()?)
                .map_err(|error| {
                    reader_error(py, error.to_string(), error.phase().as_str(), error.code())
                })?;
        }
        let scan = self.runtime.wait(py, builder.build())?.map_err(|error| {
            reader_error(py, error.to_string(), error.phase().as_str(), error.code())
        })?;
        // Validate before Arrow's C callback exports the schema.
        export_schema(py, scan.schema().as_ref())?;
        Ok(RecordBatchStream::new(
            scan.into_stream(),
            Arc::clone(&self.runtime),
        ))
    }

    /// Plan a scan and consume it as a pyarrow.RecordBatchReader.
    ///
    /// Accepts the same keyword arguments as scan().
    /// Use a with block to close the reader, including when stopping early.
    #[pyo3(signature = (*, columns=None, filters=None, limit=None, target_partitions=None, execution_options=None))]
    fn to_reader<'py>(
        &self,
        py: Python<'py>,
        columns: Option<&Bound<'_, PyAny>>,
        filters: Option<&Bound<'_, PyAny>>,
        limit: Option<&Bound<'_, PyInt>>,
        target_partitions: Option<&Bound<'_, PyInt>>,
        execution_options: Option<PyRef<'_, ScanExecutionOptions>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let stream = Py::new(
            py,
            self.scan(
                py,
                columns,
                filters,
                limit,
                target_partitions,
                execution_options,
            )?,
        )?;
        py.import("pyarrow")?
            .getattr("RecordBatchReader")?
            .call_method1("from_stream", (stream,))
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
    module.add_class::<RecordBatchStream>()?;
    module.add_class::<ScanExecutionOptions>()?;
    module.add(
        "DeltaReaderError",
        module.py().get_type::<DeltaReaderError>(),
    )?;
    module.add(
        "__all__",
        [
            "__version__",
            "DeltaTable",
            "DeltaReaderError",
            "RecordBatchStream",
            "ScanExecutionOptions",
        ],
    )
}
