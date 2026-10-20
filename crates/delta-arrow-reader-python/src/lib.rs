//! Python package entrypoint.

mod arrow_stream;
mod filters;
mod logging;
mod options;
mod runtime;
mod session;
mod stream;

use std::sync::Arc;

use ::delta_arrow_reader::{
    DeltaSnapshotSelection, DeltaStorageOptions, DeltaTable as CoreDeltaTable, DeltaTableBuilder,
    WarmupMode,
};
use arrow::{
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    ffi::FFI_ArrowSchema,
};
use pyo3::{
    exceptions::{PyException, PyTypeError, PyValueError},
    prelude::*,
    types::{PyBool, PyCapsule, PyDict, PyInt, PyList, PyMapping, PyTuple},
};

use crate::options::ScanExecutionOptions;
use crate::runtime::Runtime;
use crate::session::Session;
use crate::stream::RecordBatchStream;

pyo3::create_exception!(
    delta_arrow_reader,
    DeltaReaderError,
    PyException,
    r#"A failure while opening a table or preparing a query.

Inspect `phase` and `code` to identify the failure without parsing the message.
Messages omit sensitive details such as table paths and storage credentials.

Attributes:
    phase: The stage that failed, such as `schema` or `datafusion`.
    code: The error category, such as `schema_conversion` or `sql_planning`.

Invalid Python arguments can raise built-in exceptions such as `TypeError`
or `ValueError` instead. Failures while consuming a PyArrow reader use PyArrow
exceptions; catch those around iteration or `read_all()` as well.
"#
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
    let export_error = |reason: &'static str, source: Option<&ArrowError>| {
        logging::log_failure(
            reader_error(
                py,
                "delta reader error: phase=schema code=schema_conversion reason=arrow_schema_export_failed"
                    .to_owned(),
                "schema",
                "schema_conversion",
            ),
            || {
                tracing::error!(target: "delta_arrow_reader_python",
                    event = "schema_export.failed", operation = "schema_export", outcome = "failed",
                    error_phase = "schema", error_code = "schema_conversion", error_reason = reason,
                    error_source_kind = source.map(|error| ::delta_arrow_reader::error_source_kind(error)));
            },
        )
    };
    // Arrow 58's FFI exporter panics on NUL bytes in names or timestamp timezones.
    if schema
        .fields()
        .iter()
        .any(|field| field_contains_nul(field))
    {
        return Err(export_error("arrow_schema_contains_nul", None));
    }
    FFI_ArrowSchema::try_from(schema)
        .map_err(|error| export_error("arrow_schema_export_failed", Some(&error)))
}

fn field_contains_nul(field: &Field) -> bool {
    field.name().contains('\0') || type_contains_nul(field.data_type())
}

fn type_contains_nul(data_type: &DataType) -> bool {
    use DataType::*;
    match data_type {
        Timestamp(_, Some(timezone)) => timezone.contains('\0'),
        List(field)
        | LargeList(field)
        | ListView(field)
        | LargeListView(field)
        | FixedSizeList(field, _)
        | Map(field, _) => field_contains_nul(field),
        Struct(fields) => fields.iter().any(|field| field_contains_nul(field)),
        Union(fields, _) => fields.iter().any(|(_, field)| field_contains_nul(field)),
        Dictionary(key, value) => type_contains_nul(key) || type_contains_nul(value),
        RunEndEncoded(run_ends, values) => {
            field_contains_nul(run_ends) || field_contains_nul(values)
        }
        _ => false,
    }
}

/// Open a Delta table for repeated queries against the same version.
///
/// Use `to_reader()` to read batches or register the table with a `Session`
/// to query it with SQL. The loaded version stays fixed even if another process
/// writes to the table. Call `refresh()` to get a new table with the latest version.
///
/// Examples:
///     Replace the path with your table's directory or S3 URL.
///
///     ```python
///     from delta_arrow_reader import DeltaTable
///
///     table = DeltaTable("path/to/table")
///     with table.to_reader(limit=10) as reader:
///         print(reader.read_all())
///     ```
///
/// Args:
///     location: Local directory or storage URL containing the Delta table.
///         Accepts a string or a path object that resolves to a string.
///     version: Version to load. `None` selects the latest available version.
///         An explicit version must be an integer from `0` through `2**64 - 1`;
///         booleans are not accepted.
///     storage_options: Storage configuration as string keys and string values,
///         for example `{"aws_region": "us-west-2"}` for S3.
///     warmup: How much preparation to do when opening the table.
///         `"automatic"` prepares reusable file information and checks connection
///         speed on supported S3 stores; local tables skip this extra work.
///         `"query_planning"` prepares file information without test reads of data.
///         `"none"` leaves that preparation to each query. Both explicit modes
///         skip the connection check and keep partial-page reads inactive.
///     execution_options: Defaults for this table's `scan()` and `to_reader()`
///         calls. `None` uses `ScanExecutionOptions()`.
///         SQL registrations have their own defaults.
///
/// The automatic connection check has a five-second budget and may finish sooner.
/// Loading file information takes additional time. Keep using the same table
/// object to reuse this work; query results are not cached.
///
/// Raises:
///     TypeError: An argument has the wrong type, including a Boolean version.
///     ValueError: The version is negative or the warmup mode is unknown.
///     OverflowError: The version exceeds `2**64 - 1`.
///     DeltaReaderError: The table cannot be loaded or prepared.
#[pyclass(module = "delta_arrow_reader", frozen)]
struct DeltaTable {
    table: CoreDeltaTable,
    // Keep the executor alive for the snapshot's storage engine.
    runtime: Arc<Runtime>,
}

#[pymethods]
impl DeltaTable {
    #[new]
    #[pyo3(signature = (location, *, version=None, storage_options=None, warmup="automatic", execution_options=None))]
    fn new(
        py: Python<'_>,
        location: &Bound<'_, PyAny>,
        version: Option<&Bound<'_, PyInt>>,
        storage_options: Option<&Bound<'_, PyMapping>>,
        warmup: &str,
        execution_options: Option<PyRef<'_, ScanExecutionOptions>>,
    ) -> PyResult<Self> {
        logging::check_reentry()?;
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
            "automatic" => WarmupMode::Automatic,
            "none" => WarmupMode::None,
            "query_planning" => WarmupMode::QueryPlanning,
            _ => {
                return Err(PyValueError::new_err(
                    "warmup must be 'automatic', 'none' or 'query_planning'",
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
        let runtime = Arc::new(Runtime::for_python("reader")?);
        let table = runtime
            .wait_logged(py, builder.load_table())?
            .map_err(|error| {
                reader_error(py, error.to_string(), error.phase().as_str(), error.code())
            })?;
        Ok(Self { table, runtime })
    }

    /// Load the latest snapshot and return it as a new table.
    ///
    /// The original table and its readers keep their version, even if refresh
    /// fails. The new table keeps the storage, warmup and execution settings.
    ///
    /// Examples:
    ///     ```python
    ///     latest = table.refresh()
    ///     print(table.version, latest.version)
    ///     ```
    ///
    /// Returns:
    ///     A table at the latest version, which may be the same version as before.
    ///
    /// Raises:
    ///     DeltaReaderError: The latest snapshot cannot be loaded or prepared.
    fn refresh(&self, py: Python<'_>) -> PyResult<Self> {
        logging::check_reentry()?;
        let table = self
            .runtime
            .wait_logged(py, self.table.refresh())?
            .map_err(|error| {
                reader_error(py, error.to_string(), error.phase().as_str(), error.code())
            })?;
        Ok(Self {
            table,
            runtime: Arc::clone(&self.runtime),
        })
    }

    /// The version loaded by this table. It stays fixed until you open a new table.
    #[getter]
    fn version(&self) -> u64 {
        self.table.version()
    }

    /// Column names and types as a `pyarrow.Schema`, without reading data files.
    ///
    /// The schema remains usable after the table object is released.
    #[getter]
    fn schema(slf: Bound<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        slf.py().import("pyarrow")?.call_method1("schema", (slf,))
    }

    /// Export the schema for Arrow-compatible consumers such as `pyarrow.schema()`.
    ///
    /// Usually, use the `schema` property instead. Each call returns a fresh
    /// Arrow schema capsule that owns its resources independently of this table.
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let ffi_schema = export_schema(py, self.table.schema().as_ref())?;
        // The capsule drops the schema; Arrow's Drop releases it only if still owned.
        PyCapsule::new_with_value(py, ffi_schema, c"arrow_schema")
    }

    /// Plan a scan and return a single-use Arrow stream exporter.
    ///
    /// Use `to_reader()` for a PyArrow reader directly. This lower-level method
    /// is for consumers of the Arrow stream protocol. It accepts the same
    /// parameters and has the same filtering, planning and error behavior as
    /// `to_reader()`.
    ///
    /// Examples:
    ///     ```python
    ///     import pyarrow as pa
    ///
    ///     stream = table.scan(limit=10)
    ///     with pa.RecordBatchReader.from_stream(stream) as reader:
    ///         print(reader.read_all())
    ///     ```
    ///
    /// Returns:
    ///     An exporter that keeps the table snapshot alive. Once exported,
    ///         the consuming reader owns cleanup. Close that reader when finished;
    ///         closing the exporter does not close the consumer.
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
        logging::check_reentry()?;
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
                    logging::validation_error(
                        &error,
                        "target_partitions",
                        reader_error(py, error.to_string(), error.phase().as_str(), error.code()),
                    )
                })?;
        }
        let scan = self
            .runtime
            .wait_logged(py, builder.build())?
            .map_err(|error| {
                reader_error(py, error.to_string(), error.phase().as_str(), error.code())
            })?;
        // Validate before Arrow's C callback exports the schema.
        export_schema(py, scan.schema().as_ref())?;
        Ok(RecordBatchStream::new(
            scan.into_stream(),
            Arc::clone(&self.runtime),
        ))
    }

    /// Select rows and columns, then read them as PyArrow batches.
    ///
    /// Use a `with` block so the reader closes even when you stop early.
    /// Iterate over batches to process the result incrementally, or use
    /// `read_all()` to collect it in memory. Rows have no guaranteed order;
    /// use SQL with `ORDER BY` when order matters.
    ///
    /// Examples:
    ///     For a table with `id` and `region` columns:
    ///
    ///     ```python
    ///     with table.to_reader(
    ///         columns=["id", "region"],
    ///         filters=[("id", ">=", 100)],
    ///         limit=10,
    ///     ) as reader:
    ///         for batch in reader:
    ///             print(batch.to_pydict())
    ///     ```
    ///
    /// Args:
    ///     columns: Column names in output order. `None` selects all columns.
    ///         An empty list selects no columns but preserves row counts.
    ///     filters: Conditions written as `(column, operator, value)` tuples.
    ///         A list of tuples combines conditions with AND. A list of lists
    ///         combines AND groups with OR. `None` or `[]` disables filtering;
    ///         an empty inner group is true. Filter columns need not be in the
    ///         output. See the filtering rules below for operators and values.
    ///     limit: Maximum number of rows after filtering. `None` reads all
    ///         matching rows; `0` returns no rows. Must be a nonnegative integer.
    ///     target_partitions: Target number of groups of files to process in
    ///         parallel. `None` chooses automatically; an override must be a
    ///         positive integer. This does not change the table's stored layout.
    ///     execution_options: Settings for this scan. `None` inherits the table's
    ///         settings. A supplied `ScanExecutionOptions` replaces the whole
    ///         settings object for this scan without changing later scans.
    ///
    /// Returns:
    ///     A reader that yields `pyarrow.RecordBatch` objects and retains its
    ///         snapshot independently of the table object. Result data is read when
    ///         you request the first batch.
    ///
    /// Raises:
    ///     TypeError: An argument or filter value has the wrong type. Booleans
    ///         are not accepted for integer settings or integer filter values.
    ///     ValueError: A limit, partition target, operator or filter value is invalid.
    ///     OverflowError: An integer does not fit its column type, or a limit or
    ///         partition target exceeds `2 * sys.maxsize + 1`.
    ///     DeltaReaderError: A column is unknown or scan planning fails.
    ///
    /// Planning may read table metadata before this method returns. If automatic
    /// connection preparation was incomplete, an eligible filtered scan retries
    /// it once, which can add up to five seconds of test reads. Use
    /// `warmup="none"` or `"query_planning"` when opening the table to skip that
    /// check entirely. Errors during result consumption use PyArrow exceptions.
    ///
    /// **Filtering rules**
    ///
    /// Compare with `==`, `!=`, `<`, `<=`, `>` or `>=`. Use `"is"` or
    /// `"is not"` with `None` to test for nulls; other comparisons with `None`
    /// are invalid. For example, `[("region", "is not", None)]` excludes null
    /// regions. `[[("region", "==", "east")], [("id", "<", 10)]]` selects
    /// rows matching either condition. Filters use top-level logical names.
    /// Deleted rows remain excluded.
    ///
    /// Values must match the column type:
    ///
    /// | Column type | Python value |
    /// | --- | --- |
    /// | Boolean | `bool` |
    /// | Signed integer | `int` within the column's range |
    /// | Float32, Float64 | Finite `float`; Float32 rounds and rejects overflow |
    /// | Utf8, LargeUtf8 | UTF-8 encodable `str` |
    /// | Binary, LargeBinary, FixedSizeBinary | `bytes`; fixed-size values must match the width |
    /// | Decimal128 | Finite `decimal.Decimal` that exactly fits the precision and scale |
    /// | Date32 | `datetime.date`, excluding `datetime.datetime` |
    /// | Microsecond timestamp | `datetime.datetime`; naive for columns without a timezone, aware otherwise |
    ///
    /// Float comparisons distinguish `-0.0` from `0.0`. Decimal conversion is
    /// exact and does not depend on the decimal context. Timestamp conversion
    /// preserves microseconds without using the process timezone. Timestamps
    /// must be exact `datetime.datetime` instances, not subclasses, and UTC
    /// offsets must be exact `datetime.timedelta` instances or `None`.
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
        self.scan(
            py,
            columns,
            filters,
            limit,
            target_partitions,
            execution_options,
        )?
        .into_reader(py)
    }

    fn __repr__(&self) -> String {
        format!("DeltaTable(version={})", self.version())
    }
}

#[pymodule]
fn delta_arrow_reader(module: &Bound<'_, PyModule>) -> PyResult<()> {
    logging::initialize()?;
    // Wheel metadata already contains Maturin's normalized Python version.
    let version = module
        .py()
        .import("importlib.metadata")?
        .call_method1("version", ("delta-arrow-reader",))?;
    module.add("__version__", version)?;
    module.add_class::<DeltaTable>()?;
    module.add_class::<RecordBatchStream>()?;
    module.add_class::<ScanExecutionOptions>()?;
    module.add_class::<Session>()?;
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
            "Session",
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;

    #[test]
    fn schema_export_keeps_distinct_safe_failure_causes() {
        // Isolate Python's package logger from other Rust tests in this process.
        const CHILD: &str = "DELTA_ARROW_READER_SCHEMA_LOG_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tests::schema_export_keeps_distinct_safe_failure_causes",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        Python::initialize();
        Python::attach(|py| {
            let locals = PyDict::new(py);
            py.run(
                c"
import logging
records = []
class Collect(logging.Handler):
    def emit(self, record):
        records.append(record)
logger = logging.getLogger('delta_arrow_reader')
logger.handlers = [Collect()]
logger.setLevel(logging.ERROR)
logger.propagate = False
",
                Some(&locals),
                None,
            )
            .unwrap();
            for (data_type, reason, source) in [
                (
                    DataType::Timestamp(TimeUnit::Microsecond, Some("private\0zone".into())),
                    "arrow_schema_contains_nul",
                    None,
                ),
                (
                    DataType::Time32(TimeUnit::Nanosecond),
                    "arrow_schema_export_failed",
                    Some("arrow_c_data"),
                ),
            ] {
                py.run(c"records.clear()", Some(&locals), None).unwrap();
                let schema = Schema::new(vec![Field::new("private_field", data_type, true)]);
                let error = export_schema(py, &schema).unwrap_err();
                assert!(error.is_instance_of::<DeltaReaderError>(py));
                locals.set_item("reason", reason).unwrap();
                locals.set_item("source", source).unwrap();
                py.run(
                    c"
record, = records
assert record.levelno == logging.ERROR
assert record.dar_fields['event'] == 'schema_export.failed'
assert record.dar_fields['error_reason'] == reason
assert record.dar_fields.get('error_source_kind') == source
assert 'private' not in repr(record.__dict__)
",
                    Some(&locals),
                    None,
                )
                .unwrap();
            }
            export_schema(py, &Schema::empty()).unwrap();
        });
    }
}
