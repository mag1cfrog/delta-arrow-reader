//! Spark accepts whole boolean tokens: `yes` is true, while `on` is invalid.
//! CAST fails on invalid strings in ANSI mode; TRY_CAST and legacy CAST return NULL.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions_utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkCastStringToBoolean {
    null_on_error: bool,
}

impl SparkCastStringToBoolean {
    pub fn new(null_on_error: bool) -> Self {
        Self { null_on_error }
    }
}

impl ScalarUDFImpl for SparkCastStringToBoolean {
    fn name(&self) -> &str {
        "spark_cast_string_to_boolean"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            )
        });
        &SIGNATURE
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = take_function_args(self.name(), args.arg_fields)?;
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Boolean,
            self.null_on_error || input.is_nullable(),
        )))
    }

    fn simplify(&self, args: Vec<Expr>, _: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if let [
            Expr::Literal(
                ScalarValue::Utf8(value)
                | ScalarValue::LargeUtf8(value)
                | ScalarValue::Utf8View(value),
                _,
            ),
        ] = args.as_slice()
        {
            // Literal CAST failures must survive empty-result pruning.
            let value = parse_boolean(value.as_deref(), self.null_on_error)?;
            return Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                ScalarValue::Boolean(value),
                None,
            )));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let null_on_error = self.null_on_error;
        make_scalar_function(
            move |arrays| {
                let [array] = take_function_args("spark_cast_string_to_boolean", arrays)?;
                let output = match array.data_type() {
                    DataType::Utf8 => {
                        parse_boolean_values(as_string_array(array)?.iter(), null_on_error)
                    }
                    DataType::LargeUtf8 => {
                        parse_boolean_values(as_large_string_array(array)?.iter(), null_on_error)
                    }
                    DataType::Utf8View => {
                        parse_boolean_values(as_string_view_array(array)?.iter(), null_on_error)
                    }
                    other => return exec_err!("STRING-to-BOOLEAN CAST does not accept {other}"),
                }?;
                Ok(Arc::new(output) as ArrayRef)
            },
            vec![],
        )(&args.args)
    }
}

fn parse_boolean_values<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
    null_on_error: bool,
) -> Result<BooleanArray> {
    values
        .map(|value| parse_boolean(value, null_on_error))
        .collect()
}

fn parse_boolean(value: Option<&str>, null_on_error: bool) -> Result<Option<bool>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let token = value.trim_matches(|c: char| c <= ' ' || c == '\u{7f}');
    if ["t", "true", "y", "yes", "1"]
        .iter()
        .any(|accepted| token.eq_ignore_ascii_case(accepted))
    {
        Ok(Some(true))
    } else if ["f", "false", "n", "no", "0"]
        .iter()
        .any(|accepted| token.eq_ignore_ascii_case(accepted))
    {
        Ok(Some(false))
    } else if null_on_error {
        Ok(None)
    } else {
        exec_err!("[CAST_INVALID_INPUT] Cannot cast STRING to BOOLEAN")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray, LargeStringArray, StringArray, StringViewArray};
    use datafusion_common::config::ConfigOptions;

    fn invoke(value: ColumnarValue, null_on_error: bool) -> Result<ColumnarValue> {
        let rows = match &value {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        };
        let input = Arc::new(Field::new("input", value.data_type(), true));
        SparkCastStringToBoolean::new(null_on_error).invoke_with_args(ScalarFunctionArgs {
            args: vec![value],
            arg_fields: vec![input],
            number_rows: rows,
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    #[test]
    fn string_layouts_preserve_slices_nulls_and_empty_inputs() -> Result<()> {
        let values = [
            Some("outside"),
            Some(" TRUE "),
            None,
            Some("off"),
            Some("\u{7f}false\u{7f}"),
            Some("\u{a0}true\u{a0}"),
            Some("y"),
            Some("outside"),
        ];
        let arrays: [ArrayRef; 3] = [
            Arc::new(StringArray::from(values.to_vec())),
            Arc::new(LargeStringArray::from(values.to_vec())),
            Arc::new(StringViewArray::from(values.to_vec())),
        ];
        for array in arrays {
            let result = invoke(ColumnarValue::Array(array.slice(1, 6)), true)?.into_array(6)?;
            assert_eq!(result.data_type(), &DataType::Boolean);
            assert_eq!(
                result.as_boolean().iter().collect::<Vec<_>>(),
                [Some(true), None, None, Some(false), None, Some(true)]
            );
            let error = invoke(ColumnarValue::Array(array.slice(3, 1)), false).unwrap_err();
            assert!(error.to_string().contains("[CAST_INVALID_INPUT]"));
            let result = invoke(ColumnarValue::Array(array.slice(1, 2)), false)?.into_array(2)?;
            assert_eq!(
                result.as_boolean().iter().collect::<Vec<_>>(),
                [Some(true), None]
            );
            for null_on_error in [true, false] {
                let empty = invoke(ColumnarValue::Array(array.slice(3, 0)), null_on_error)?
                    .into_array(0)?;
                assert_eq!(empty.len(), 0);
                assert_eq!(empty.data_type(), &DataType::Boolean);
            }
        }
        Ok(())
    }

    #[test]
    fn string_scalar_layouts_preserve_null_and_boolean_values() -> Result<()> {
        for text in [Some("YeS"), None, Some("off")] {
            for value in [
                ScalarValue::Utf8(text.map(str::to_owned)),
                ScalarValue::LargeUtf8(text.map(str::to_owned)),
                ScalarValue::Utf8View(text.map(str::to_owned)),
            ] {
                let expected = (text == Some("YeS")).then_some(true);
                let result = invoke(ColumnarValue::Scalar(value.clone()), true)?;
                assert!(
                    matches!(result, ColumnarValue::Scalar(ScalarValue::Boolean(actual)) if actual == expected)
                );
                let strict = invoke(ColumnarValue::Scalar(value), false);
                if text == Some("off") {
                    assert!(
                        strict
                            .unwrap_err()
                            .to_string()
                            .contains("[CAST_INVALID_INPUT]")
                    );
                } else {
                    assert!(
                        matches!(strict?, ColumnarValue::Scalar(ScalarValue::Boolean(actual)) if actual == expected)
                    );
                }
            }
        }
        Ok(())
    }
}
