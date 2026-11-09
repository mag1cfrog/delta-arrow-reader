// Local Spark nullability declaration. See the host crate's UPSTREAM.md.
//! Like Spark's `KnownNullable`: its argument unchanged, declared nullable whatever the argument
//! declares, for a Spark expression that is nullable over a column at every stage, such as
//! TRY_TO_DATE with a format. Unlike a `CASE WHEN true` wrapper, the optimizer cannot simplify
//! it away over a column; over a constant it folds like any immutable function, so the folded
//! value is NOT NULL, as Spark's is. Further arguments are columns that Spark's expression has
//! as inputs but DAR's does not read, such as an ignored format's; they are not used, and keep
//! the declaration from folding over a constant value where Spark's expression is not constant.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkKnownNullable {
    signature: Signature,
}

impl Default for SparkKnownNullable {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkKnownNullable {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkKnownNullable {
    fn name(&self) -> &str {
        "spark_known_nullable"
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
            true,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [argument, ..] = args.args.as_slice() else {
            return internal_err!("{} requires an argument", self.name());
        };
        Ok(argument.clone())
    }
}
