// Local Spark nullability declaration. See the host crate's UPSTREAM.md.
//! Like Spark's `KnownNotNull`: its argument unchanged, declared NOT NULL, for a Spark expression
//! that is NOT NULL for NOT NULL inputs where DataFusion declares a step of it nullable, such as
//! formatted TO_DATE of a NOT NULL TIMESTAMP in ANSI mode. A NULL value fails, since the
//! declaration would make the batch invalid.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkKnownNotNull {
    signature: Signature,
}

impl Default for SparkKnownNotNull {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkKnownNotNull {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkKnownNotNull {
    fn name(&self) -> &str {
        "spark_known_not_null"
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
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [argument] = args.args.as_slice() else {
            return internal_err!("{} requires one argument", self.name());
        };
        let has_null = match argument {
            ColumnarValue::Array(array) => array.null_count() > 0,
            ColumnarValue::Scalar(scalar) => scalar.is_null(),
        };
        if has_null {
            return internal_err!("{} received a NULL value", self.name());
        }
        Ok(argument.clone())
    }
}
