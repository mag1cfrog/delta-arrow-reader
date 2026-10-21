use super::*;

fn assert_diagnostics(
    error: &PyErr,
    phase: &str,
    code: &str,
    reason: &str,
    source: Option<&str>,
) -> PyResult<()> {
    Python::attach(|py| {
        let value = error.value(py);
        for (name, expected) in [
            ("phase", Some(phase)),
            ("code", Some(code)),
            ("reason", Some(reason)),
            ("source_kind", source),
        ] {
            assert_eq!(
                value.getattr(name)?.extract::<Option<String>>()?.as_deref(),
                expected
            );
        }
        for text in [
            value.str()?,
            value.repr()?,
            value.getattr("args")?.repr()?,
            value.getattr("__dict__")?.repr()?,
        ] {
            assert!(!text.to_str()?.contains("secret"), "{text}");
        }
        assert!(error.cause(py).is_none());
        assert!(error.context(py).is_none());
        Ok(())
    })
}

#[test]
fn public_errors_preserve_fields_and_validation_logs() -> PyResult<()> {
    // These calls release the GIL while changing Python's process-wide logger.
    const CHILD: &str = "DELTA_ARROW_READER_ERROR_FIELDS_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "binding_tests::diagnostics::public_errors_preserve_fields_and_validation_logs",
            ])
            .env(CHILD, "1")
            .status()?;
        assert!(status.success());
        return Ok(());
    }
    Python::initialize();
    Python::attach(|py| {
        let kwargs = PyDict::new(py);
        kwargs.set_item("prefix", "secret-errors-")?;
        let temporary = py
            .import("tempfile")?
            .getattr("TemporaryDirectory")?
            .call((), Some(&kwargs))?;
        let location = std::path::PathBuf::from(temporary.getattr("name")?.extract::<String>()?);
        let log = location.join("_delta_log");
        std::fs::create_dir(&log)?;
        std::fs::copy(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table/_delta_log/00000000000000000000.json"
            ),
            log.join("00000000000000000000.json"),
        )?;
        let class = py.get_type::<DeltaTable>();
        kwargs.clear();
        kwargs.set_item("warmup", "none")?;
        let table = class.call((&location,), Some(&kwargs))?;
        let session = py.get_type::<Session>().call0()?;
        std::fs::write(
            log.join("00000000000000000001.json"),
            "secret-invalid-json\n",
        )?;
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
        for (disabled, level) in [(false, 50), (true, 5), (false, 5)] {
            logger.setattr("disabled", disabled)?;
            logger.call_method1("setLevel", (level,))?;
            for (operation, phase, code, reason, source) in [
                (
                    "empty",
                    "table_location",
                    "invalid_table_location",
                    "empty_table_location",
                    None,
                ),
                (
                    "scheme",
                    "storage",
                    "storage_initialization",
                    "storage_initialization_failed",
                    Some("unknown"),
                ),
                (
                    "load",
                    "snapshot",
                    "snapshot_load",
                    "snapshot_load_failed",
                    Some("arrow_json"),
                ),
                (
                    "refresh",
                    "snapshot",
                    "snapshot_load",
                    "snapshot_refresh_failed",
                    Some("arrow_json"),
                ),
                (
                    "projection",
                    "scan_planning",
                    "invalid_projection",
                    "column_not_found",
                    None,
                ),
                (
                    "filter",
                    "scan_planning",
                    "unsupported_predicate",
                    "column_not_found",
                    None,
                ),
                (
                    "nested_filter",
                    "scan_planning",
                    "unsupported_predicate",
                    "invalid_column_reference",
                    None,
                ),
                (
                    "max_concurrent_file_reads_per_scan",
                    "configuration",
                    "invalid_configuration",
                    "max_concurrent_file_reads_per_scan_exceeds_tokio_max_permits",
                    None,
                ),
                (
                    "max_concurrent_file_reads_per_partition",
                    "configuration",
                    "invalid_configuration",
                    "max_concurrent_file_reads_per_partition_exceeds_tokio_max_permits",
                    None,
                ),
                (
                    "output_buffer_batches_per_partition",
                    "configuration",
                    "invalid_configuration",
                    "output_buffer_batches_per_partition_exceeds_tokio_max_permits",
                    None,
                ),
                (
                    "register",
                    "datafusion",
                    "datafusion_adapter",
                    "table_registration_name_invalid",
                    Some("datafusion_plan"),
                ),
            ] {
                let argument = match operation {
                    "filter" | "nested_filter" => Some("filters"),
                    _ if phase == "configuration" => Some("execution_options"),
                    _ => None,
                };
                if !disabled && level == 5 && argument.is_none() {
                    continue;
                }
                records.call_method0("clear")?;
                kwargs.clear();
                let result = match operation {
                    "empty" => class.call1(("",)),
                    "scheme" => class.call1((
                        "unknown://secret-user:secret-password@host/table?token=secret-token",
                    )),
                    "load" => {
                        kwargs.set_item("warmup", "none")?;
                        class.call((&location,), Some(&kwargs))
                    }
                    "refresh" => table.call_method0("refresh"),
                    "projection" => {
                        kwargs.set_item("columns", ["secret_missing"])?;
                        table.call_method("scan", (), Some(&kwargs))
                    }
                    "filter" | "nested_filter" => {
                        let name = if operation == "filter" {
                            "secret_missing"
                        } else {
                            "secret.nested"
                        };
                        kwargs.set_item("filters", [(name, "==", 1)])?;
                        table.call_method("scan", (), Some(&kwargs))
                    }
                    "register" => {
                        session.call_method1("register_table", ("secret-invalid", &table))
                    }
                    option => {
                        kwargs.set_item(option, isize::MAX)?;
                        py.get_type::<ScanExecutionOptions>()
                            .call((), Some(&kwargs))
                    }
                };
                let error = if phase == "configuration" {
                    assert_error::<PyValueError>(result)
                } else {
                    assert_error::<DeltaReaderError>(result)
                };
                assert_diagnostics(&error, phase, code, reason, source)?;
                if disabled || level == 50 {
                    assert!(records.is_empty());
                    continue;
                }
                assert_eq!(records.len(), 1);
                let record = records.get_item(0)?;
                assert_eq!(record.getattr("levelno")?.extract::<u8>()?, 40);
                assert_eq!(
                    record.getattr("msg")?.extract::<String>()?,
                    "validation.failed"
                );
                assert_eq!(
                    record.getattr("dar_target")?.extract::<String>()?,
                    "delta_arrow_reader_python"
                );
                let fields = record.getattr("dar_fields")?;
                assert_eq!(
                    fields.get_item("event")?.extract::<String>()?,
                    "validation.failed"
                );
                assert_eq!(
                    fields.get_item("argument")?.extract::<String>()?,
                    argument.unwrap()
                );
                for name in ["phase", "code", "reason", "source_kind"] {
                    let logged = fields.call_method1("get", (format!("error_{name}"),))?;
                    assert!(logged.eq(error.value(py).getattr(name)?)?);
                }
                assert!(
                    !record
                        .getattr("__dict__")?
                        .repr()?
                        .to_str()?
                        .contains("secret")
                );
                assert_eq!(
                    error.value(py).str()?.to_str()?,
                    format!("delta reader error: phase={phase} code={code} reason={reason}")
                );
            }
        }
        assert_eq!(table.getattr("version")?.extract::<u64>()?, 0);
        kwargs.clear();
        kwargs.set_item("limit", 0)?;
        table
            .call_method("scan", (), Some(&kwargs))?
            .call_method0("close")?;
        temporary.call_method0("cleanup")?;
        Ok(())
    })
}
