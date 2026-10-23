//! SQL sessions use the existing DataFusion provider and Python stream exporter.

use std::sync::{Arc, Mutex};

use datafusion::execution::context::{SQLOptions, SessionContext};
use delta_arrow_reader::{
    datafusion::{ScanOptions, register_table},
    error_source_kind,
};
use pyo3::prelude::*;

use crate::{
    DeltaReaderError, DeltaTable, core_error_to_python_exception, export_schema,
    new_binding_exception, options::ScanExecutionOptions, runtime::Runtime,
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
        let runtime = Arc::new(Runtime::for_python("SQL")?);
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
    ///         name. Core failures retain their phase, code, reason, and source
    ///         category. Invalid or duplicate names have code `datafusion_adapter`.
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
        let runtime = Arc::clone(&table.runtime);
        let table = table.table.clone();
        let lock_result = self.state.runtime.wait_logged(py, async {
            // Wait for the catalog lock without the GIL: logging handlers may release it.
            let mut runtimes = self.state.table_runtimes.lock().map_err(|_| {
                tracing::error!(target: "delta_arrow_reader_python",
                    event = "sql_registration.failed", operation = "sql_registration", outcome = "failed",
                    error_phase = "datafusion", error_code = "sql_registration",
                    error_reason = "registration_state_poisoned");
            })?;
            // Convert Core failures after releasing the lock, on the Python calling thread.
            Ok(register_table(&self.state.context, name, table, options)
                .map(|_| runtimes.push(runtime)))
        })?;
        let registration_result =
            lock_result.map_err(|()| new_sql_exception(py, "sql_registration"))?;
        registration_result
            .map_err(|error| core_error_to_python_exception::<DeltaReaderError>(py, &error))
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
            .wait_logged(py, async {
                let result = self.state.context.sql_with_options(query, options).await;
                result.inspect_err(|error| {
                    // Logical SQL planning is owned by the binding; provider execution starts later.
                    tracing::error!(target: "delta_arrow_reader_python",
                        event = "sql_planning.failed", operation = "sql_planning", outcome = "failed",
                        error_phase = "datafusion", error_code = "sql_planning",
                        error_reason = "sql_planning_failed", error_source_kind = error_source_kind(error));
                })
            })?
            .map_err(|_| new_sql_exception(py, "sql_planning"))?;
        export_schema(py, frame.schema().as_arrow())?;
        RecordBatchStream::from_sql(frame, Arc::clone(&self.state)).into_reader(py)
    }
}

fn new_sql_exception(py: Python<'_>, code: &'static str) -> PyErr {
    new_binding_exception(
        py,
        format!("delta reader error: phase=datafusion code={code}"),
        "datafusion",
        code,
    )
}

#[cfg(test)]
mod tests;
