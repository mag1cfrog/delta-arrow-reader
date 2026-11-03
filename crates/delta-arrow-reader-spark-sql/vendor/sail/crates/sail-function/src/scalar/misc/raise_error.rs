use std::sync::Arc;

use datafusion::arrow::array::{Array, NullArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_common::cast::{as_large_string_array, as_string_array, as_string_view_array};
use datafusion_common::{ScalarValue, internal_err};
use datafusion_expr::{ReturnFieldArgs, ScalarFunctionArgs};
use sail_common_datafusion::utils::items::ItemTaker;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RaiseError {
    signature: Signature,
    user_error: bool,
}

impl Default for RaiseError {
    fn default() -> Self {
        Self::new()
    }
}

impl RaiseError {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
            user_error: false,
        }
    }

    // SQL user errors need their own condition; internal callers carry other causes.
    pub fn new_user_error() -> Self {
        Self {
            user_error: true,
            ..Self::new()
        }
    }

    fn raise(&self, message: Option<&str>) -> Result<ColumnarValue> {
        let message = match message {
            Some(message) => message,
            None if self.user_error => "null",
            None => return internal_err!("raise_error expects a single UTF-8 string argument"),
        };
        Err(DataFusionError::Execution(if self.user_error {
            format!("[USER_RAISED_EXCEPTION] {message}")
        } else {
            message.to_string()
        }))
    }

    fn raise_from_strings<'a>(
        &self,
        mut iter: impl Iterator<Item = Option<&'a str>>,
    ) -> Result<ColumnarValue> {
        self.raise(if self.user_error {
            iter.next().flatten()
        } else {
            iter.find_map(|value| value)
        })
    }
}

impl ScalarUDFImpl for RaiseError {
    fn name(&self) -> &str {
        "raise_error"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `RaiseError.nullable = true`, unconditional.
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/misc.scala#L86>
    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Null, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        let Ok(arg) = args.one() else {
            return internal_err!("raise_error should only be called with one argument");
        };
        if self.user_error && number_rows == 0 && arg.data_type().is_string() {
            return Ok(ColumnarValue::Array(Arc::new(NullArray::new(0))));
        }
        match arg {
            ColumnarValue::Scalar(ScalarValue::Utf8(message))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(message))
            | ColumnarValue::Scalar(ScalarValue::Utf8View(message)) => {
                self.raise(message.as_deref())
            }
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Utf8 => self.raise_from_strings(as_string_array(array.as_ref())?.iter()),
                DataType::LargeUtf8 => {
                    self.raise_from_strings(as_large_string_array(array.as_ref())?.iter())
                }
                DataType::Utf8View => {
                    self.raise_from_strings(as_string_view_array(array.as_ref())?.iter())
                }
                _ => internal_err!("raise_error expects a single UTF-8 string argument"),
            },
            _ => internal_err!("raise_error expects a single UTF-8 string argument"),
        }
    }
}
