// Local Spark nullability declaration. See the host crate's UPSTREAM.md.
//! Its argument unchanged, declared nullable whatever the argument declares, for a Spark
//! expression that is nullable at every stage, such as TRY_TO_DATE. Unlike a `CASE WHEN true`
//! wrapper, the optimizer cannot simplify it away over a column; over a constant it folds like
//! any immutable function, so the folded value is NOT NULL, as Spark's is.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNullableResult {
    signature: Signature,
}

impl Default for SparkNullableResult {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkNullableResult {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkNullableResult {
    fn name(&self) -> &str {
        "spark_nullable_result"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [data_type] => Ok(data_type.clone()),
            _ => internal_err!("{} requires one argument", self.name()),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [argument] = args.arg_fields else {
            return internal_err!("{} requires one argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            argument.data_type().clone(),
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [argument] = args.args.as_slice() else {
            return internal_err!("{} requires one argument", self.name());
        };
        Ok(argument.clone())
    }
}
