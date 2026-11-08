// Local Spark row-ordered errors. See the host crate's UPSTREAM.md.
//! Spark evaluates an expression row by row, so with several failing rows it reports the first
//! row's error. A cast takes part in DataFusion's row-ordered evaluation by reporting its first
//! failing row and the rows before it, from one pass over its input.

use datafusion::arrow::array::Array;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{ColumnarValue, EvaluatedRowPrefix, RowEvaluationFailure};

/// A strict cast's result up to its first failing row, from one pass over `input`. `lenient`
/// is the same cast with TRY_CAST's semantics over all of `input`, which gives NULL wherever the
/// strict cast fails; the strict cast can also give NULL itself, as for a NaN DOUBLE cast to a
/// DECIMAL. So each row whose input is valid and whose lenient result is NULL is checked, in
/// order, by `strict_value` over that one value, and the first such row that fails is the first
/// failing row. A row the strict cast gives a value for is an internal error, since the two casts
/// then disagree. The cast is not run again over the input.
pub fn strict_cast_rows_until_error(
    input: &ColumnarValue,
    lenient: ColumnarValue,
    number_rows: usize,
    strict_value: impl Fn(&ColumnarValue) -> Result<ColumnarValue>,
) -> Result<EvaluatedRowPrefix> {
    let candidates: Vec<usize> = match (input, &lenient) {
        (ColumnarValue::Scalar(input), ColumnarValue::Scalar(output)) => {
            if !input.is_null() && output.is_null() {
                vec![0]
            } else {
                vec![]
            }
        }
        _ => {
            let inputs = input.to_array(number_rows)?;
            let outputs = lenient.to_array(number_rows)?;
            (0..number_rows)
                .filter(|&row| inputs.is_valid(row) && outputs.is_null(row))
                .collect()
        }
    };
    for row in candidates {
        let value = match input {
            ColumnarValue::Scalar(_) => input.clone(),
            ColumnarValue::Array(array) => ColumnarValue::Array(array.slice(row, 1)),
        };
        match strict_value(&value) {
            Ok(strict) if strict.to_array(1)?.is_null(0) => {}
            Ok(_) => {
                return internal_err!(
                    "the lenient cast gave NULL in row {row}, where the strict cast gives a value"
                );
            }
            Err(error) => {
                return Ok(EvaluatedRowPrefix {
                    values: ColumnarValue::Array(lenient.to_array(number_rows)?.slice(0, row)),
                    failure: Some(RowEvaluationFailure {
                        row_index: row,
                        error,
                    }),
                });
            }
        }
    }
    Ok(EvaluatedRowPrefix::complete(lenient))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{AsArray, Int32Array};
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion_common::{ScalarValue, exec_err};

    use super::*;

    /// TRY semantics of a cast that fails on negative values: NULL there.
    fn lenient(input: &ColumnarValue) -> Result<ColumnarValue> {
        let values = input.to_array(1)?;
        Ok(ColumnarValue::Array(Arc::new(
            values
                .as_primitive::<Int32Type>()
                .iter()
                .map(|value| value.filter(|value| *value >= 0))
                .collect::<Int32Array>(),
        )))
    }

    fn strict(input: &ColumnarValue) -> Result<ColumnarValue> {
        let values = input.to_array(1)?;
        match values
            .as_primitive::<Int32Type>()
            .iter()
            .flatten()
            .find(|value| *value < 0)
        {
            Some(value) => exec_err!("negative {value}"),
            None => Ok(input.clone()),
        }
    }

    #[test]
    fn the_first_failing_row_ends_the_prefix() -> Result<()> {
        let input = ColumnarValue::Array(Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(-3),
            Some(-4),
        ])));
        let output = strict_cast_rows_until_error(&input, lenient(&input)?, 4, strict)?;
        let failure = output.failure.as_ref().expect("a failure");
        assert_eq!(failure.row_index, 2);
        assert!(failure.error.to_string().contains("negative -3"));
        let ColumnarValue::Array(values) = output.values else {
            unreachable!("a prefix is an array")
        };
        assert_eq!(
            values.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(1), None])
        );
        Ok(())
    }

    #[test]
    fn a_failing_scalar_fails_in_row_0_with_an_empty_prefix() -> Result<()> {
        let input = ColumnarValue::Scalar(ScalarValue::Int32(Some(-1)));
        let lenient = ColumnarValue::Scalar(ScalarValue::Int32(None));
        let output = strict_cast_rows_until_error(&input, lenient, 3, strict)?;
        assert_eq!(output.failure.map(|failure| failure.row_index), Some(0));
        let ColumnarValue::Array(values) = output.values else {
            unreachable!("a prefix is an array")
        };
        assert_eq!(values.len(), 0);
        Ok(())
    }

    /// A strict cast that also gives NULL for 0, as a strict DECIMAL cast does for NaN.
    fn strict_null_for_zero(input: &ColumnarValue) -> Result<ColumnarValue> {
        let values = strict(input)?.to_array(1)?;
        Ok(ColumnarValue::Array(Arc::new(
            values
                .as_primitive::<Int32Type>()
                .iter()
                .map(|value| value.filter(|value| *value != 0))
                .collect::<Int32Array>(),
        )))
    }

    #[test]
    fn a_null_the_strict_cast_also_gives_is_not_a_failure() -> Result<()> {
        let input = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(0), Some(-2)])));
        let lenient = ColumnarValue::Array(Arc::new(Int32Array::from(vec![None, None])));
        let output = strict_cast_rows_until_error(&input, lenient, 2, strict_null_for_zero)?;
        assert_eq!(output.failure.map(|failure| failure.row_index), Some(1));
        Ok(())
    }

    #[test]
    fn a_value_only_the_strict_cast_gives_is_an_internal_error() {
        // The lenient cast gives NULL for 0, where `strict` gives 0.
        let input = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(0), Some(-2)])));
        let lenient = ColumnarValue::Array(Arc::new(Int32Array::from(vec![None, None])));
        let error = strict_cast_rows_until_error(&input, lenient, 2, strict)
            .err()
            .expect("an internal error");
        assert!(
            error.to_string().contains(
                "the lenient cast gave NULL in row 0, where the strict cast gives a value"
            )
        );
    }

    #[test]
    fn null_inputs_and_success_keep_the_lenient_result() -> Result<()> {
        let input = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(1), None])));
        let output = strict_cast_rows_until_error(&input, lenient(&input)?, 2, strict)?;
        assert!(output.failure.is_none());
        Ok(())
    }
}
