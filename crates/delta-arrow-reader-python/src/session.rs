//! SQL sessions use the existing DataFusion provider and Python stream exporter.

use std::sync::{Arc, Mutex};

use datafusion::execution::context::{SQLOptions, SessionContext};
use delta_arrow_reader::datafusion::{ScanOptions, register_table};
use pyo3::{exceptions::PyRuntimeError, prelude::*};

use crate::{
    DeltaTable, export_schema, options::ScanExecutionOptions, reader_error, runtime::Runtime,
    stream::RecordBatchStream,
};

/// A read-only SQL session backed by Rust DataFusion.
///
/// Register loaded DeltaTable snapshots, then consume sql() results as PyArrow
/// readers. Joins, sorts and aggregations may retain memory while producing a
/// streamed result. This is independent of the separate datafusion Python package.
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

    /// Register an immutable snapshot without reading Parquet data.
    ///
    /// Names follow the Rust adapter's identifier rules. Duplicate names fail
    /// without replacing the existing table. Refreshing the source DeltaTable
    /// does not change a registration. Omitted execution_options uses the
    /// DataFusion adapter defaults, independently of the table's direct scans.
    #[pyo3(signature = (name, table, *, execution_options=None))]
    fn register_table(
        &self,
        py: Python<'_>,
        name: &str,
        table: PyRef<'_, DeltaTable>,
        execution_options: Option<PyRef<'_, ScanExecutionOptions>>,
    ) -> PyResult<()> {
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

    /// Plan one read-only SQL statement and return a pyarrow.RecordBatchReader.
    ///
    /// Results start executing on the first pull. Each call creates an
    /// independent reader that retains its session and snapshots. Use a with
    /// block to release query resources when stopping early. DDL, DML, COPY
    /// and session-changing statements are rejected before execution.
    fn sql<'py>(&self, py: Python<'py>, query: &str) -> PyResult<Bound<'py, PyAny>> {
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
