// Local Spark nullability declaration. See the host crate's UPSTREAM.md.
//! Like Spark's `KnownNotNull`: its argument unchanged, declared NOT NULL, for a Spark expression
//! that is NOT NULL for NOT NULL inputs where DataFusion declares a step of it nullable, such as
//! formatted TO_DATE of a NOT NULL TIMESTAMP in ANSI mode. A NULL value fails, since the
//! declaration would make the batch invalid. Further arguments are inputs that Spark's expression
//! has and DAR's omits, as for `SparkKnownNullable`; they are not used.

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
            signature: Signature::variadic_any(Volatility::Immutable),
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
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [argument, ..] = args.args.as_slice() else {
            return internal_err!("{} requires an argument", self.name());
        };
        let has_null = match argument {
            ColumnarValue::Array(array) => array.logical_null_count() > 0,
            ColumnarValue::Scalar(scalar) => scalar.is_null(),
        };
        if has_null {
            return internal_err!("{} received a NULL value", self.name());
        }
        Ok(argument.clone())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, Date32Array};
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;

    use super::*;

    fn invoke(argument: ColumnarValue) -> Result<ColumnarValue> {
        let field = Arc::new(Field::new("a", DataType::Date32, true));
        SparkKnownNotNull::new().invoke_with_args(ScalarFunctionArgs {
            args: vec![argument],
            arg_fields: vec![Arc::clone(&field)],
            number_rows: 2,
            return_field: Arc::new(Field::new("r", DataType::Date32, false)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    #[test]
    fn passes_values_through_and_fails_on_null() -> Result<()> {
        let values: ArrayRef = Arc::new(Date32Array::from(vec![1, 2]));
        let result = invoke(ColumnarValue::Array(Arc::clone(&values)))?;
        assert!(matches!(result, ColumnarValue::Array(array) if array == values));
        let with_null: ArrayRef = Arc::new(Date32Array::from(vec![Some(1), None]));
        assert!(invoke(ColumnarValue::Array(with_null)).is_err());
        assert!(invoke(ColumnarValue::Scalar(ScalarValue::Date32(None))).is_err());
        assert!(invoke(ColumnarValue::Scalar(ScalarValue::Date32(Some(3)))).is_ok());
        Ok(())
    }
}
