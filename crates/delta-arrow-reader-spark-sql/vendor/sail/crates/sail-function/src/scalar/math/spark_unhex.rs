// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, BinaryBuilder, OffsetSizeTrait};
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

fn unhex_strings<'a>(
    strings: impl Iterator<Item = Option<&'a str>>,
    fail_on_error: bool,
) -> Result<ColumnarValue> {
    let mut encoded = Vec::new();
    let mut builder = BinaryBuilder::new();
    for item in strings {
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

fn spark_unhex_inner<T: OffsetSizeTrait>(
    array: &ColumnarValue,
    fail_on_error: bool,
) -> Result<ColumnarValue, DataFusionError> {
    match array {
        ColumnarValue::Array(array) if array.data_type() == &DataType::Utf8View => {
            unhex_strings(array.as_string_view().iter(), fail_on_error)
        }
        ColumnarValue::Array(array) => {
            let string_array = as_generic_string_array::<T>(array)?;
            unhex_strings(string_array.iter(), fail_on_error)
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{
        ArrayRef, BinaryArray, LargeStringArray, StringArray, StringViewArray,
    };

    use super::*;

    #[test]
    fn unhex_borrows_each_string_representation_and_resets_after_invalid_text() -> Result<()> {
        let values = vec![
            Some("prefix"),
            Some(""),
            Some("00FF"),
            None,
            Some("F"),
            Some("aBc"),
            Some("414g"),
            Some("42"),
            Some("é"),
            Some("0123456789abcdef0123456789abcdef"),
            Some("suffix"),
        ];
        let arrays: [ArrayRef; 3] = [
            Arc::new(StringArray::from(values.clone())),
            Arc::new(LargeStringArray::from(values.clone())),
            Arc::new(StringViewArray::from(values)),
        ];
        let expected = BinaryArray::from(vec![
            Some(b"".as_slice()),
            Some(b"\x00\xff".as_slice()),
            None,
            Some(b"\x0f".as_slice()),
            Some(b"\x0a\xbc".as_slice()),
            None,
            Some(b"B".as_slice()),
            None,
            Some(b"\x01\x23\x45\x67\x89\xab\xcd\xef\x01\x23\x45\x67\x89\xab\xcd\xef".as_slice()),
        ]);
        for array in arrays {
            let input = ColumnarValue::Array(array.slice(1, 9));
            let output = spark_unhex(std::slice::from_ref(&input))?.into_array(9)?;
            assert_eq!(output.data_type(), &DataType::Binary);
            assert_eq!(output.as_binary::<i32>(), &expected);
            let Err(error) = spark_unhex(&[
                input,
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            ]) else {
                return internal_err!("strict conversion must reject invalid text");
            };
            assert!(error.to_string().contains("414g"));
            let empty = spark_unhex(&[ColumnarValue::Array(array.slice(0, 0))])?.into_array(0)?;
            assert_eq!(empty.data_type(), &DataType::Binary);
            assert!(empty.is_empty());
        }
        Ok(())
    }

    #[test]
    fn unhex_preserves_scalar_empty_odd_invalid_and_null_results() -> Result<()> {
        for (text, expected) in [
            (Some(""), Some(vec![])),
            (Some("F"), Some(vec![15])),
            (Some("aBc"), Some(vec![10, 188])),
            (Some("414g"), None),
            (None, None),
        ] {
            for value in [
                ScalarValue::Utf8(text.map(str::to_owned)),
                ScalarValue::LargeUtf8(text.map(str::to_owned)),
                ScalarValue::Utf8View(text.map(str::to_owned)),
            ] {
                let output = spark_unhex(&[ColumnarValue::Scalar(value)])?;
                let ColumnarValue::Scalar(output) = output else {
                    return internal_err!("expected scalar UNHEX output");
                };
                assert_eq!(output, ScalarValue::Binary(expected.clone()));
            }
        }
        Ok(())
    }
}
