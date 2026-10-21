use super::*;

#[test]
fn filter_shapes_operators_and_columns_are_checked_before_planning() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let table = table(py)?;
        let null_test = ("id", "is", py.None()).into_pyobject(py)?.into_any();
        let group = PyList::new(py, [&null_test])?.into_any();
        let disjunction = PyList::new(py, [&group])?.into_any();
        let short = ("id", "is").into_pyobject(py)?.into_any();
        let empty = PyList::empty(py).into_any();
        let mut malformed = vec![
            PyBool::new(py, true).to_owned().into_any(),
            PyInt::new(py, 1).into_any(),
            PyString::new(py, "secret-filter").into_any(),
            PyDict::new(py).into_any(),
            null_test.clone(),
            PyList::new(py, [&disjunction])?.into_any(),
            PyList::new(py, [&null_test, &group])?.into_any(),
            PyList::new(py, [&group, &null_test])?.into_any(),
            PyList::new(py, [&empty, &PyList::new(py, [&short])?.into_any()])?.into_any(),
        ];
        for condition in [
            short,
            ("id", "is", py.None(), "secret-extra")
                .into_pyobject(py)?
                .into_any(),
            PyList::new(py, null_test.cast::<PyTuple>()?.iter())?.into_any(),
            (1, "is", py.None()).into_pyobject(py)?.into_any(),
            (PyBytes::new(py, b"id"), "is", py.None())
                .into_pyobject(py)?
                .into_any(),
            ("id", 1, py.None()).into_pyobject(py)?.into_any(),
            ("id", PyBytes::new(py, b"is"), py.None())
                .into_pyobject(py)?
                .into_any(),
            ("id", "==", true).into_pyobject(py)?.into_any(),
        ] {
            malformed.push(PyList::new(py, [condition])?.into_any());
        }
        let mut invalid = vec![
            ("id", "is", 1).into_pyobject(py)?.into_any(),
            ("id", "is not", "secret-value")
                .into_pyobject(py)?
                .into_any(),
        ];
        for operator in ["secret-operator", "IS", "==", "!=", "<", "<=", ">", ">="] {
            invalid.push(("id", operator, py.None()).into_pyobject(py)?.into_any());
        }
        let kwargs = PyDict::new(py);
        for method in ["scan", "to_reader"] {
            for value in &malformed {
                kwargs.set_item("filters", value)?;
                assert_error::<PyTypeError>(table.call_method(method, (), Some(&kwargs)));
            }
            for condition in &invalid {
                kwargs.set_item("filters", PyList::new(py, [condition])?)?;
                assert_error::<PyValueError>(table.call_method(method, (), Some(&kwargs)));
            }
            for column in ["", "missing-secret", "id.secret"] {
                for operator in ["is", "is not"] {
                    let condition = PyList::new(py, [(column, operator, py.None())])?.into_any();
                    let filters = if operator == "is" {
                        condition
                    } else {
                        PyList::new(py, [&empty, &condition])?.into_any()
                    };
                    kwargs.set_item("filters", filters)?;
                    let error = assert_error::<DeltaReaderError>(table.call_method(
                        method,
                        (),
                        Some(&kwargs),
                    ));
                    assert_eq!(
                        error.value(py).getattr("phase")?.extract::<String>()?,
                        "scan_planning"
                    );
                    assert_eq!(
                        error.value(py).getattr("code")?.extract::<String>()?,
                        "unsupported_predicate"
                    );
                }
            }
        }
        Ok(())
    })
}
