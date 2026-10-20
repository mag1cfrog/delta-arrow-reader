//! SQL sessions use the existing DataFusion provider and Python stream exporter.

use std::sync::{Arc, Mutex};

use datafusion::execution::context::{SQLOptions, SessionContext};
use delta_arrow_reader::datafusion::{ScanOptions, register_table};
use pyo3::{exceptions::PyRuntimeError, prelude::*};

use crate::{
    DeltaTable, export_schema, options::ScanExecutionOptions, reader_error, runtime::Runtime,
    stream::RecordBatchStream,
};

/// Query Delta tables with SQL and read the results as PyArrow batches.
///
/// Register tables you have already opened, then call `sql()`. The package
/// includes the Rust DataFusion engine; you do not need to install the separate
/// `datafusion` Python package.
///
/// Examples:
///     ```python
///     from delta_arrow_reader import DeltaTable, Session
///
///     table = DeltaTable("path/to/table")
///     session = Session()
///     session.register_table("events", table)
///     with session.sql("SELECT COUNT(*) AS rows FROM events") as reader:
///         print(reader.read_all().to_pydict())
///     ```
///
/// Register more tables to query them together with a JOIN. Only read-only SQL
/// is supported. Joins, sorts and aggregations can use substantial memory even
/// when you read their output in batches.
#[pyclass(module = "delta_arrow_reader", frozen)]
pub(crate) struct Session {
    state: Arc<SessionState>,
}

pub(crate) struct SessionState {
    context: SessionContext,
    pub(crate) runtime: Arc<Runtime>,
    // A table's storage engine can retain work on the runtime that loaded it.
    table_runtimes: Mutex<Vec<Arc<Runtime>>>,
}

#[pymethods]
impl Session {
    #[new]
    fn new() -> PyResult<Self> {
        crate::logging::check_reentry()?;
        let runtime = Arc::new(
            Runtime::new()
                .map_err(|_| PyRuntimeError::new_err("failed to create the SQL runtime"))?,
        );
        let context = runtime.block_on(async { SessionContext::new() });
        Ok(Self {
            state: Arc::new(SessionState {
                context,
                runtime,
                table_runtimes: Mutex::new(Vec::new()),
            }),
        })
    }

    /// Give an opened table a name to use in SQL queries.
    ///
    /// Registration reuses the table's prepared file information and connection
    /// measurements. It does not read Parquet data or repeat initialization.
    ///
    /// Examples:
    ///     ```python
    ///     session.register_table("latest_events", table.refresh())
    ///     ```
    ///
    /// Args:
    ///     name: A simple identifier, such as `events` or `order_items`. Start
    ///         with an ASCII letter or underscore, then use ASCII letters, digits
    ///         or underscores. SQL keywords and duplicate names are rejected.
    ///     table: The loaded `DeltaTable` snapshot to query. Refreshing the
    ///         original variable later does not change this registration.
    ///         Register the refreshed table under a new name or in a new session.
    ///     execution_options: Read settings for this registration. `None` uses
    ///         the DataFusion adapter defaults, independently of custom settings
    ///         passed to `DeltaTable`. Pass a `ScanExecutionOptions` explicitly
    ///         to override them.
    ///
    /// Raises:
    ///     TypeError: An argument has the wrong type.
    ///     DeltaReaderError: Registration fails, including an invalid or duplicate
    ///         name. The error has phase `datafusion` and code `sql_registration`.
    ///         A duplicate does not replace the existing table.
    #[pyo3(signature = (name, table, *, execution_options=None))]
    fn register_table(
        &self,
        py: Python<'_>,
        name: &str,
        table: PyRef<'_, DeltaTable>,
        execution_options: Option<PyRef<'_, ScanExecutionOptions>>,
    ) -> PyResult<()> {
        crate::logging::check_reentry()?;
        let options = ScanOptions {
            execution_options: execution_options
                .map(|options| options.options)
                .unwrap_or_default(),
            ..ScanOptions::default()
        };
        let mut runtimes = self
            .state
            .table_runtimes
            .lock()
            .map_err(|_| sql_error(py, "sql_registration"))?;
        register_table(&self.state.context, name, table.table.clone(), options)
            .map_err(|_| sql_error(py, "sql_registration"))?;
        runtimes.push(Arc::clone(&table.runtime));
        Ok(())
    }

    /// Run a read-only SQL query over registered tables.
    ///
    /// This call plans the query. Result execution starts when you request the
    /// first batch. Use a `with` block to release resources if you stop early.
    ///
    /// Examples:
    ///     For a registered `events` table with a `region` column:
    ///
    ///     ```python
    ///     with session.sql(
    ///         "SELECT region, COUNT(*) AS rows FROM events GROUP BY region"
    ///     ) as reader:
    ///         for batch in reader:
    ///             print(batch.to_pydict())
    ///     ```
    ///
    /// Args:
    ///     query: One read-only SQL statement. Table creation, writes, COPY and
    ///         session-changing statements are rejected before execution.
    ///
    /// Returns:
    ///     An independent PyArrow reader. It retains the session and table
    ///         snapshots even if those Python variables are deleted.
    ///
    /// Raises:
    ///     TypeError: The query is not a string.
    ///     DeltaReaderError: The query is invalid, unsupported or cannot be
    ///         planned, or its schema cannot be exported. Planning failures have
    ///         phase `datafusion` and code `sql_planning`.
    ///
    /// Execution errors are raised by PyArrow while consuming results, with
    /// `code=sql_execution` in the message. A failed reader remains failed.
    /// SQL error messages omit query text, literal values and table paths.
    fn sql<'py>(&self, py: Python<'py>, query: &str) -> PyResult<Bound<'py, PyAny>> {
        crate::logging::check_reentry()?;
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let frame = self
            .state
            .runtime
            .wait(py, self.state.context.sql_with_options(query, options))?
            .map_err(|_| sql_error(py, "sql_planning"))?;
        export_schema(py, frame.schema().as_arrow())?;
        RecordBatchStream::from_sql(frame, Arc::clone(&self.state)).into_reader(py)
    }
}

fn sql_error(py: Python<'_>, code: &'static str) -> PyErr {
    reader_error(
        py,
        format!("delta reader error: phase=datafusion code={code}"),
        "datafusion",
        code,
    )
}
