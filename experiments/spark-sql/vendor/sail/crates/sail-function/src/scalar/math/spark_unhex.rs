// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{BinaryBuilder, OffsetSizeTrait};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs};
use datafusion_expr_common::signature::TypeSignature;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkUnHex {
    signature: Signature,
}

impl Default for SparkUnHex {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkUnHex {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkUnHex {
    fn name(&self) -> &str {
        "spark_unhex"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `Unhex.nullable = true`, unconditional (not narrowed by `failOnError`).
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L1247>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Binary, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        spark_unhex(&args)
    }
}

// [Credit]: <https://github.com/apache/datafusion-comet/blob/bfd7054c02950219561428463d3926afaf8edbba/native/spark-expr/src/scalar_funcs/unhex.rs>

/// Helper function to convert a hex digit to a binary value.
fn unhex_digit(c: u8) -> Result<u8, DataFusionError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'A'..=b'F' => Ok(10 + c - b'A'),
        b'a'..=b'f' => Ok(10 + c - b'a'),
        _ => Err(DataFusionError::Execution(
            "Input to unhex_digit is not a valid hex digit".to_string(),
        )),
    }
}

/// Convert a hex string to binary and store the result in `result`. Returns an error if the input
/// is not a valid hex string.
fn unhex(hex_str: &str, result: &mut Vec<u8>) -> Result<(), DataFusionError> {
    let bytes = hex_str.as_bytes();

    let mut i = 0;

    if (bytes.len() & 0x01) != 0 {
        let v = unhex_digit(bytes[0])?;

        result.push(v);
        i += 1;
    }

    while i < bytes.len() {
        let first = unhex_digit(bytes[i])?;
        let second = unhex_digit(bytes[i + 1])?;
        result.push((first << 4) | second);

        i += 2;
    }

    Ok(())
}

fn spark_unhex_inner<T: OffsetSizeTrait>(
    array: &ColumnarValue,
    fail_on_error: bool,
) -> Result<ColumnarValue, DataFusionError> {
    match array {
        ColumnarValue::Array(array) => {
            let string_array = as_generic_string_array::<T>(array)?;

            let mut encoded = Vec::new();
            let mut builder = BinaryBuilder::new();

            for item in string_array.iter() {
                if let Some(s) = item {
                    if unhex(s, &mut encoded).is_ok() {
                        builder.append_value(encoded.as_slice());
                    } else if fail_on_error {
                        return exec_err!("Input to unhex is not a valid hex string: {s}");
                    } else {
                        builder.append_null();
                    }
                    encoded.clear();
                } else {
                    builder.append_null();
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(string)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(string)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(string))) => {
            let mut encoded = Vec::new();

            if unhex(string, &mut encoded).is_ok() {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(encoded))))
            } else if fail_on_error {
                exec_err!("Input to unhex is not a valid hex string: {string}")
            } else {
                Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
            }
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(None))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(None))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Binary(None)))
        }
        _ => {
            exec_err!(
                "The first argument must be a string scalar or array, but got: {:?}",
                array
            )
        }
    }
}

/// Spark-compatible `unhex` expression
pub fn spark_unhex(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() > 2 {
        return exec_err!("unhex takes at most 2 arguments, but got: {}", args.len());
    }

    let val_to_unhex = &args[0];
    let fail_on_error = if args.len() == 2 {
        match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(fail_on_error))) => *fail_on_error,
            _ => {
                return exec_err!(
                    "The second argument must be boolean scalar, but got: {:?}",
                    args[1]
                );
            }
        }
    } else {
        false
    };

    match val_to_unhex.data_type() {
        DataType::Utf8 | DataType::Utf8View => {
            spark_unhex_inner::<i32>(val_to_unhex, fail_on_error)
        }
        DataType::LargeUtf8 => spark_unhex_inner::<i64>(val_to_unhex, fail_on_error),
        other => exec_err!("The first argument must be a Utf8, Utf8View, or LargeUtf8: {other:?}"),
    }
}
