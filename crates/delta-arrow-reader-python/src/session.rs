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
        crate::logging::reject_logging_reentry()?;
        let runtime = Arc::new(Runtime::new_with_python_diagnostics("SQL")?);
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
        crate::logging::reject_logging_reentry()?;
        let options = ScanOptions {
            execution_options: execution_options
                .map(|options| options.options)
                .unwrap_or_default(),
            ..ScanOptions::default()
        };
        let runtime = Arc::clone(&table.runtime);
        let table = table.table.clone();
        let lock_result = self.state.runtime.wait_with_logging(py, async {
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
        crate::logging::reject_logging_reentry()?;
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let frame = self
            .state
            .runtime
            .wait_with_logging(py, async {
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
mod tests {
    use super::*;
    use crate::binding_tests::{assert_error, table};
    use pyo3::{
        exceptions::PyTypeError,
        types::{PyBytes, PyDict, PyInt},
    };

    #[test]
    fn rejected_calls_preserve_the_session_and_have_safe_errors() -> PyResult<()> {
        Python::initialize();
        Python::attach(|py| {
            let table = table(py)?;
            let session = py.get_type::<Session>().call0()?;
            session.call_method1("register_table", ("events", &table))?;
            let state = Arc::clone(&session.extract::<PyRef<'_, Session>>()?.state);
            let count = || {
                state
                    .runtime
                    .wait_with_logging(py, async {
                        state
                            .context
                            .sql("SELECT COUNT(*) AS n FROM events")
                            .await?
                            .collect()
                            .await
                    })
                    .unwrap()
                    .unwrap()
            };
            let expected = count();
            assert_error::<PyTypeError>(
                session.call_method1("register_table", (py.None(), &table)),
            );
            assert_error::<PyTypeError>(
                session.call_method1("register_table", ("valid", PyDict::new(py))),
            );
            let kwargs = PyDict::new(py);
            kwargs.set_item("execution_options", PyDict::new(py))?;
            assert_error::<PyTypeError>(session.call_method(
                "register_table",
                ("valid", &table),
                Some(&kwargs),
            ));
            assert_eq!(count(), expected);
            let temporary = py
                .import("tempfile")?
                .getattr("TemporaryDirectory")?
                .call0()?;
            let output = std::path::PathBuf::from(temporary.getattr("name")?.extract::<String>()?)
                .join("secret-output.csv");
            let copy = format!(
                "COPY events TO '{}' STORED AS CSV",
                output
                    .to_string_lossy()
                    .replace('\\', "/")
                    .replace('\'', "''")
            );
            for query in [
                "",
                "SELECT",
                "SELECT * FROM secret_missing",
                "SELECT secret_missing FROM events",
                "SELECT 1; SELECT 2",
                "CREATE TABLE changed AS SELECT * FROM events",
                "DROP TABLE events",
                "INSERT INTO events SELECT * FROM events",
                "DELETE FROM events",
                "UPDATE events SET id = 1",
                "SET datafusion.execution.target_partitions = 1",
                "BEGIN TRANSACTION",
                &copy,
            ] {
                let error = assert_error::<DeltaReaderError>(session.call_method1("sql", (query,)));
                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "datafusion"
                );
                assert_eq!(
                    error.value(py).getattr("code")?.extract::<String>()?,
                    "sql_planning"
                );
                assert!(error.cause(py).is_none());
                assert!(error.context(py).is_none());
                assert_eq!(count(), expected);
                assert!(!output.exists());
                assert!(!state.context.table_exist("changed").unwrap());
            }
            assert_error::<DeltaReaderError>(
                session.call_method1("sql", ("SELECT * FROM changed",)),
            );
            for query in [
                py.None().into_bound(py),
                PyInt::new(py, 42).into_any(),
                PyBytes::new(py, b"SELECT 1").into_any(),
            ] {
                assert_error::<PyTypeError>(session.call_method1("sql", (query,)));
            }
            temporary.call_method0("cleanup")?;
            Ok(())
        })
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn runtime_creation_failures_are_diagnosable_and_recoverable() -> PyResult<()> {
        use pyo3::{exceptions::PyRuntimeError, types::PyList};
        use std::{
            process::{Command, Stdio},
            time::{Duration, Instant},
        };

        // File descriptor limits and Python logging are process-wide.
        const CHILD: &str = "DELTA_ARROW_READER_RUNTIME_FAILURE_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let mut child = Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "session::tests::runtime_creation_failures_are_diagnosable_and_recoverable",
                ])
                .env(CHILD, "1")
                .stderr(Stdio::piped())
                .spawn()?;
            let deadline = Instant::now() + Duration::from_secs(30);
            while child.try_wait()?.is_none() {
                if Instant::now() >= deadline {
                    child.kill()?;
                    child.wait()?;
                    panic!("runtime failure test timed out");
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let output = child.wait_with_output()?;
            assert!(output.status.success(), "{output:?}");
            assert!(output.stderr.is_empty(), "{output:?}");
            return Ok(());
        }
        Python::initialize();
        Python::attach(|py| {
            let resource = py.import("resource")?;
            let nofile = resource.getattr("RLIMIT_NOFILE")?;
            let limits = resource.call_method1("getrlimit", (&nofile,))?;
            let emfile = py.import("errno")?.getattr("EMFILE")?.extract::<i32>()?;
            let logger = py
                .import("logging")?
                .call_method1("getLogger", ("delta_arrow_reader",))?;
            let handler = py
                .import("logging.handlers")?
                .getattr("BufferingHandler")?
                .call1((1000,))?;
            let records = handler.getattr("buffer")?.cast_into::<PyList>()?;
            logger.setattr("handlers", PyList::new(py, [&handler])?)?;
            logger.setattr("propagate", false)?;
            for level in [40, 30, 20, 10, 5] {
                logger.call_method1("setLevel", (level,))?;
                for kind in ["reader", "SQL"] {
                    records.call_method0("clear")?;
                    resource.call_method1("setrlimit", (&nofile, (0, limits.get_item(1)?)))?;
                    let result = if kind == "reader" {
                        table(py)
                    } else {
                        py.get_type::<Session>().call0()
                    };
                    // Restore the limit before propagating or inspecting the operation's result.
                    resource.call_method1("setrlimit", (&nofile, &limits))?;
                    let error = assert_error::<PyRuntimeError>(result);
                    assert_eq!(
                        error.value(py).str()?.to_str()?,
                        format!("failed to create the {kind} runtime")
                    );
                    assert_eq!(records.len(), 1);
                    let record = records.get_item(0)?;
                    assert_eq!(record.getattr("levelno")?.extract::<u8>()?, 40);
                    let fields = record.getattr("dar_fields")?;
                    for (name, expected) in [
                        ("event", "runtime_creation.failed"),
                        ("runtime", kind),
                        ("error_source_kind", "unknown"),
                    ] {
                        assert_eq!(fields.get_item(name)?.extract::<String>()?, expected);
                    }
                    assert_eq!(fields.get_item("error_os_code")?.extract::<i32>()?, emfile);
                    assert!(
                        !record
                            .getattr("__dict__")?
                            .repr()?
                            .to_str()?
                            .contains("external_writer")
                    );
                }
            }
            let table = table(py)?;
            let session = py.get_type::<Session>().call0()?;
            session.call_method1("register_table", ("events", table))?;
            let state = Arc::clone(&session.extract::<PyRef<'_, Session>>()?.state);
            let batches = state
                .runtime
                .wait_with_logging(py, async {
                    state
                        .context
                        .sql("SELECT id FROM events LIMIT 1")
                        .await?
                        .collect()
                        .await
                })?
                .unwrap();
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                1
            );
            Ok(())
        })
    }
}
