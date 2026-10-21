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
                .wait_logged(py, async {
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
        assert_error::<PyTypeError>(session.call_method1("register_table", (py.None(), &table)));
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
        assert_error::<DeltaReaderError>(session.call_method1("sql", ("SELECT * FROM changed",)));
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
