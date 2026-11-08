// Local Spark row-ordered errors. See the host crate's UPSTREAM.md.
//! Spark evaluates an expression row by row, so with several failing rows it reports the first
//! row's error. A row-wise function can take part in DataFusion's row-ordered evaluation by
//! reporting which row failed first: the rows before it are the successful prefix.

use datafusion::arrow::array::new_empty_array;
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, EvaluatedRowPrefix, RowEvaluationFailure, ScalarFunctionArgs,
};

/// Invoke `invoke` over `args`. Where it fails, find the first row that fails alone, and return
/// the rows before it with that row's error. `invoke` must evaluate each row on its own, as
/// casts and constructors do, so that a row fails alone exactly when it fails in the batch.
/// The search runs only after a failure. If no single row fails, the batch's error is
/// returned as it is.
pub fn invoke_rows_until_first_failing_row(
    args: ScalarFunctionArgs,
    invoke: impl Fn(ScalarFunctionArgs) -> Result<ColumnarValue>,
) -> Result<EvaluatedRowPrefix> {
    let error = match invoke(args.clone()) {
        Ok(values) => return Ok(EvaluatedRowPrefix::complete(values)),
        Err(error) => error,
    };
    let slice = |offset: usize, length: usize| {
        let mut rows = args.clone();
        rows.args = args
            .args
            .iter()
            .map(|arg| match arg {
                ColumnarValue::Array(array) => ColumnarValue::Array(array.slice(offset, length)),
                ColumnarValue::Scalar(_) => arg.clone(),
            })
            .collect();
        rows.number_rows = length;
        rows
    };
    for row in 0..args.number_rows {
        if let Err(row_error) = invoke(slice(row, 1)) {
            let values = if row == 0 {
                new_empty_array(args.return_field.data_type())
            } else {
                invoke(slice(0, row))?.into_array(row)?
            };
            return Ok(EvaluatedRowPrefix {
                values: ColumnarValue::Array(values),
                failure: Some(RowEvaluationFailure {
                    row_index: row,
                    error: row_error,
                }),
            });
        }
    }
    Err(error)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, AsArray, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::exec_err;

    use super::*;

    fn args(values: Vec<i32>) -> ScalarFunctionArgs {
        let rows = values.len();
        ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Int32Array::from(values)))],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Int32, false))],
            number_rows: rows,
            return_field: Arc::new(Field::new("r", DataType::Int32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    /// Doubles each value and fails on a negative one, naming it.
    fn double(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ColumnarValue::Array(values) = &args.args[0] else {
            return exec_err!("expected an array");
        };
        let values = values.as_primitive::<Int32Type>();
        if let Some(negative) = values.iter().flatten().find(|value| *value < 0) {
            return exec_err!("negative {negative}");
        }
        Ok(ColumnarValue::Array(Arc::new(
            values.unary::<_, Int32Type>(|value| value * 2),
        )))
    }

    #[test]
    fn the_first_failing_row_ends_the_prefix() -> Result<()> {
        let output = invoke_rows_until_first_failing_row(args(vec![1, 2, -3, -4]), double)?;
        let failure = output.failure.as_ref().map(|failure| failure.row_index);
        assert_eq!(failure, Some(2));
        assert!(
            output
                .failure
                .unwrap()
                .error
                .to_string()
                .contains("negative -3"),
            "the first failing row's own error"
        );
        let ColumnarValue::Array(values) = output.values else {
            unreachable!("a prefix is an array")
        };
        assert_eq!(
            values.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![2, 4])
        );
        Ok(())
    }

    #[test]
    fn a_failing_first_row_has_an_empty_prefix() -> Result<()> {
        let output = invoke_rows_until_first_failing_row(args(vec![-1, 2]), double)?;
        assert_eq!(output.failure.map(|failure| failure.row_index), Some(0));
        let ColumnarValue::Array(values) = output.values else {
            unreachable!("a prefix is an array")
        };
        assert_eq!(values.len(), 0);
        Ok(())
    }

    #[test]
    fn success_keeps_the_whole_result() -> Result<()> {
        let output = invoke_rows_until_first_failing_row(args(vec![1, 2]), double)?;
        assert!(output.failure.is_none());
        Ok(())
    }
}
