//! Exercise the generated PyO3 call boundaries without a wheel or PyArrow.

use super::*;
use pyo3::{
    exceptions::{PyAttributeError, PyOverflowError},
    types::{PyBytes, PyFloat, PyString},
};

mod diagnostics;

fn assert_error<E: PyTypeInfo>(result: PyResult<Bound<'_, PyAny>>) -> PyErr {
    let error = result.unwrap_err();
    Python::attach(|py| assert!(error.get_type(py).is(py.get_type::<E>()), "{error}"));
    assert!(!error.to_string().contains("secret"), "{error}");
    error
}

fn non_integers(py: Python<'_>) -> Vec<Bound<'_, PyAny>> {
    vec![
        PyBool::new(py, true).to_owned().into_any(),
        PyBool::new(py, false).to_owned().into_any(),
        PyFloat::new(py, 1.0).into_any(),
        PyString::new(py, "secret-value").into_any(),
        PyBytes::new(py, b"1").into_any(),
        PyList::empty(py).into_any(),
        PyDict::new(py).into_any(),
        py.import("builtins")
            .unwrap()
            .getattr("object")
            .unwrap()
            .call0()
            .unwrap(),
    ]
}

fn table(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table"
    );
    let kwargs = PyDict::new(py);
    kwargs.set_item("warmup", "none")?;
    py.get_type::<DeltaTable>().call((path,), Some(&kwargs))
}

#[test]
fn numeric_options_check_bounds_defaults_and_readonly_properties() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let class = py.get_type::<ScanExecutionOptions>();
        let defaults = class.call0()?;
        let capacity = (isize::MAX >> 2) as usize;
        for (name, default, minimum, maximum, optional) in [
            (
                "max_concurrent_file_reads_per_partition",
                Some(3),
                1,
                capacity,
                false,
            ),
            (
                "max_concurrent_file_reads_per_scan",
                None,
                1,
                capacity,
                true,
            ),
            (
                "output_buffer_batches_per_partition",
                Some(1),
                1,
                capacity,
                false,
            ),
            (
                "prefetch_files_per_partition",
                Some(2),
                0,
                usize::MAX,
                false,
            ),
            (
                "parquet_metadata_size_hint_bytes",
                Some(65536),
                1,
                usize::MAX,
                true,
            ),
            (
                "parquet_full_file_read_threshold_bytes",
                None,
                1,
                usize::MAX,
                true,
            ),
        ] {
            assert_eq!(defaults.getattr(name)?.extract::<Option<usize>>()?, default);
            let kwargs = PyDict::new(py);
            for count in [Some(minimum), Some(2), Some(maximum), default] {
                kwargs.set_item(name, count)?;
                let options = class.call((), Some(&kwargs))?;
                assert_eq!(options.getattr(name)?.extract::<Option<usize>>()?, count);
                assert!(
                    options
                        .setattr(name, 1)
                        .unwrap_err()
                        .is_instance_of::<PyAttributeError>(py)
                );
            }
            let mut invalid = non_integers(py);
            if optional {
                kwargs.set_item(name, py.None())?;
                assert!(class.call((), Some(&kwargs))?.getattr(name)?.is_none());
            } else {
                invalid.push(py.None().into_bound(py));
            }
            for value in invalid {
                kwargs.set_item(name, value)?;
                let error = assert_error::<PyTypeError>(class.call((), Some(&kwargs)));
                assert!(error.to_string().contains(name), "{error}");
            }
            for value in [minimum as i128 - 1, -(1_i128 << 100)] {
                kwargs.set_item(name, value)?;
                assert_error::<PyValueError>(class.call((), Some(&kwargs)));
            }
            if maximum < usize::MAX {
                kwargs.set_item(name, maximum + 1)?;
                assert_error::<PyValueError>(class.call((), Some(&kwargs)));
            }
            for value in [usize::MAX as u128 + 1, 1_u128 << 100] {
                kwargs.set_item(name, value)?;
                let error = assert_error::<PyOverflowError>(class.call((), Some(&kwargs)));
                assert!(error.to_string().contains(name), "{error}");
            }
        }
        Ok(())
    })
}

#[test]
fn named_options_validate_before_opening_storage() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let options = py.get_type::<ScanExecutionOptions>();
        let kwargs = PyDict::new(py);
        assert_eq!(
            options
                .call0()?
                .getattr("parquet_backend")?
                .extract::<String>()?,
            "direct"
        );
        for backend in ["direct", "delta_kernel"] {
            kwargs.set_item("parquet_backend", backend)?;
            let value = options.call((), Some(&kwargs))?;
            assert_eq!(
                value.getattr("parquet_backend")?.extract::<String>()?,
                backend
            );
            assert!(
                value
                    .setattr("parquet_backend", "direct")
                    .unwrap_err()
                    .is_instance_of::<PyAttributeError>(py)
            );
            assert!(
                value
                    .delattr("parquet_backend")
                    .unwrap_err()
                    .is_instance_of::<PyAttributeError>(py)
            );
            assert!(
                value
                    .setattr("extra", true)
                    .unwrap_err()
                    .is_instance_of::<PyAttributeError>(py)
            );
        }
        assert_error::<PyTypeError>(options.call1(("direct",)));
        for name in ["warmup", "parquet_backend"] {
            kwargs.clear();
            let call = || {
                if name == "warmup" {
                    py.get_type::<DeltaTable>()
                        .call(("/missing",), Some(&kwargs))
                } else {
                    options.call((), Some(&kwargs))
                }
            };
            for value in non_integers(py)
                .into_iter()
                .filter(|value| !value.is_instance_of::<PyString>())
                .chain([py.None().into_bound(py)])
            {
                kwargs.set_item(name, value)?;
                assert_error::<PyTypeError>(call());
            }
            for value in [
                "",
                "NONE",
                "DIRECT",
                "query-planning",
                "delta-kernel",
                "secret-invalid",
            ] {
                kwargs.set_item(name, value)?;
                assert_error::<PyValueError>(call());
            }
        }
        let table = table(py)?;
        for value in non_integers(py) {
            kwargs.clear();
            kwargs.set_item("execution_options", value)?;
            assert_error::<PyTypeError>(
                py.get_type::<DeltaTable>()
                    .call(("/missing",), Some(&kwargs)),
            );
            for method in ["scan", "to_reader"] {
                assert_error::<PyTypeError>(table.call_method(method, (), Some(&kwargs)));
            }
        }
        Ok(())
    })
}

#[test]
fn scan_integer_arguments_reject_wrong_types_and_overflow() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let table = table(py)?;
        let kwargs = PyDict::new(py);
        for method in ["scan", "to_reader"] {
            for (name, minimum) in [("limit", 0_i128), ("target_partitions", 1)] {
                kwargs.clear();
                for value in non_integers(py) {
                    kwargs.set_item(name, value)?;
                    assert_error::<PyTypeError>(table.call_method(method, (), Some(&kwargs)));
                }
                for value in [minimum - 1, -1, -(1_i128 << 100)] {
                    kwargs.set_item(name, value)?;
                    assert_error::<PyValueError>(table.call_method(method, (), Some(&kwargs)));
                }
                for value in [usize::MAX as u128 + 1, 1_u128 << 100] {
                    kwargs.set_item(name, value)?;
                    assert_error::<PyOverflowError>(table.call_method(method, (), Some(&kwargs)));
                }
                // The success path exports to PyArrow only in to_reader().
                for value in [None, Some(minimum as usize), Some(2), Some(usize::MAX)] {
                    kwargs.set_item(name, value)?;
                    table
                        .call_method("scan", (), Some(&kwargs))?
                        .call_method0("close")?;
                }
            }
            assert_error::<PyTypeError>(table.call_method1(method, (py.None(), 1)));
            assert_error::<PyTypeError>(table.call_method1(method, (py.None(), py.None(), 1)));
        }
        Ok(())
    })
}

#[test]
fn constructor_version_and_storage_arguments_are_checked() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let class = py.get_type::<DeltaTable>();
        let kwargs = PyDict::new(py);
        for value in non_integers(py) {
            kwargs.set_item("version", value)?;
            assert_error::<PyTypeError>(class.call(("/missing",), Some(&kwargs)));
        }
        for value in [-1_i128, -(1_i128 << 100)] {
            kwargs.set_item("version", value)?;
            assert_error::<PyValueError>(class.call(("/missing",), Some(&kwargs)));
        }
        for value in [u128::from(u64::MAX) + 1, 1_u128 << 100] {
            kwargs.set_item("version", value)?;
            assert_error::<PyOverflowError>(class.call(("/missing",), Some(&kwargs)));
        }
        kwargs.set_item("version", u64::MAX)?;
        assert_error::<DeltaReaderError>(class.call(("/missing",), Some(&kwargs)));
        assert_error::<PyTypeError>(class.call1(("/missing", 0)));
        kwargs.clear();
        let mut values = non_integers(py);
        values.retain(|value| !value.is_instance_of::<PyDict>());
        values.push(PyList::new(py, [("key", "value")])?.into_any());
        for (key, value) in [
            (
                PyInt::new(py, 1).into_any(),
                PyString::new(py, "value").into_any(),
            ),
            (
                PyBytes::new(py, b"key").into_any(),
                PyString::new(py, "value").into_any(),
            ),
            (
                PyString::new(py, "key").into_any(),
                PyInt::new(py, 1).into_any(),
            ),
            (
                PyString::new(py, "key").into_any(),
                py.None().into_bound(py),
            ),
            (
                PyString::new(py, "key").into_any(),
                PyBytes::new(py, b"value").into_any(),
            ),
        ] {
            let dict = PyDict::new(py);
            dict.set_item(key, value)?;
            values.push(dict.into_any());
        }
        for value in values {
            kwargs.set_item("storage_options", value)?;
            assert_error::<PyTypeError>(class.call(("/missing",), Some(&kwargs)));
        }
        for value in non_integers(py)
            .into_iter()
            .filter(|value| !value.is_instance_of::<PyString>())
            .chain([py.None().into_bound(py)])
        {
            assert_error::<PyTypeError>(class.call1((value,)));
        }
        Ok(())
    })
}

#[test]
fn projection_validation_is_shared_and_redacted() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let table = table(py)?;
        let kwargs = PyDict::new(py);
        let mut invalid = non_integers(py);
        invalid.retain(|value| !value.is_instance_of::<PyList>());
        invalid.push(py.import("builtins")?.getattr("set")?.call1((["id"],))?);
        invalid.push(PyList::new(py, ["id"])?.call_method0("__iter__")?);
        for value in [
            PyInt::new(py, 1).into_any(),
            py.None().into_bound(py),
            PyBytes::new(py, b"id").into_any(),
        ] {
            invalid.push(PyList::new(py, [value])?.into_any());
        }
        for method in ["scan", "to_reader"] {
            for value in &invalid {
                kwargs.set_item("columns", value)?;
                assert_error::<PyTypeError>(table.call_method(method, (), Some(&kwargs)));
            }
            assert_error::<PyTypeError>(table.call_method1(method, (["id"],)));
            for (columns, reason) in [
                (vec!["secret-column"], "column_not_found"),
                (vec!["id", "id"], "duplicate_column"),
            ] {
                kwargs.set_item("columns", columns)?;
                let error =
                    assert_error::<DeltaReaderError>(table.call_method(method, (), Some(&kwargs)));
                assert_eq!(
                    error.value(py).getattr("phase")?.extract::<String>()?,
                    "scan_planning"
                );
                assert_eq!(
                    error.value(py).getattr("code")?.extract::<String>()?,
                    "invalid_projection"
                );
                assert!(error.to_string().contains(&format!("reason={reason}")));
            }
        }
        Ok(())
    })
}
