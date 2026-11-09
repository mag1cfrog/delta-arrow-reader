// Local Spark input carrier. See the host crate's UPSTREAM.md.
//! Its first argument unchanged, with that argument's own nullability at every stage. Further
//! arguments are inputs that Spark's expression has and DAR's omits, such as the columns,
//! aggregates and window functions of a format Spark does not evaluate, where DataFusion already
//! declares Spark's nullability. They are not used: they keep the operators below computing what
//! Spark's do, and keep the value from folding over a constant where Spark's expression is not
//! constant. Unlike `SparkKnownNotNull`, it checks no value, so a grouping set that makes the
//! value NULL keeps it nullable, as Spark's is.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkWithInputs {
    signature: Signature,
}

impl Default for SparkWithInputs {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkWithInputs {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkWithInputs {
    fn name(&self) -> &str {
        "spark_with_inputs"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [data_type, ..] => Ok(data_type.clone()),
            [] => internal_err!("{} requires an argument", self.name()),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [argument, ..] = args.arg_fields else {
            return internal_err!("{} requires an argument", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            argument.data_type().clone(),
            argument.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [argument, ..] = args.args.as_slice() else {
            return internal_err!("{} requires an argument", self.name());
        };
        Ok(argument.clone())
    }
}
