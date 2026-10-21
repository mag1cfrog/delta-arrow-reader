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
            .wait_logged(py, async {
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
