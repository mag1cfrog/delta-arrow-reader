use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions_nested::{cardinality::Cardinality, length::ArrayLength};

/// Native collection counts with Spark's container-based result nullability.
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum SparkCollectionLength {
    Array(ArrayLength),
    Map(Cardinality),
}

impl SparkCollectionLength {
    fn inner(&self) -> &dyn ScalarUDFImpl {
        match self {
            Self::Array(inner) => inner,
            Self::Map(inner) => inner,
        }
    }
}

impl ScalarUDFImpl for SparkCollectionLength {
    fn name(&self) -> &str {
        self.inner().name()
    }

    fn signature(&self) -> &Signature {
        self.inner().signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner().return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = match (self, args.arg_fields) {
            (Self::Array(_), [field]) => Some(field.is_nullable()),
            (Self::Map(_), [field]) if matches!(field.data_type(), DataType::Map(..)) => {
                Some(field.is_nullable())
            }
            // Native dimensions and deep array cardinality have different NULL rules.
            _ => None,
        };
        let field = self.inner().return_field_from_args(args)?;
        Ok(match nullable {
            Some(nullable) => Arc::new(field.as_ref().clone().with_nullable(nullable)),
            None => field,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.inner().invoke_with_args(args)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Field;

    use super::*;

    #[test]
    fn collection_length_fields_follow_containers_and_keep_native_fallbacks() -> Result<()> {
        for children_nullable in [false, true] {
            let array = Arc::new(
                Field::new(
                    "a",
                    DataType::new_list(DataType::Int32, children_nullable),
                    false,
                )
                .with_metadata([("source".into(), "array".into())].into()),
            );
            let map = Arc::new(Field::new(
                "m",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Int32, children_nullable),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            ));
            for (kernel, fields, nullable) in [
                (
                    SparkCollectionLength::Array(ArrayLength::new()),
                    vec![array.clone()],
                    false,
                ),
                (
                    SparkCollectionLength::Array(ArrayLength::new()),
                    vec![Arc::new(array.as_ref().clone().with_nullable(true))],
                    true,
                ),
                (
                    SparkCollectionLength::Map(Cardinality::new()),
                    vec![map.clone()],
                    false,
                ),
                (
                    SparkCollectionLength::Map(Cardinality::new()),
                    vec![Arc::new(map.as_ref().clone().with_nullable(true))],
                    true,
                ),
                (
                    SparkCollectionLength::Array(ArrayLength::new()),
                    vec![
                        array.clone(),
                        Arc::new(Field::new("dimension", DataType::Int64, false)),
                    ],
                    true,
                ),
                (
                    SparkCollectionLength::Map(Cardinality::new()),
                    vec![array],
                    true,
                ),
            ] {
                let scalar_arguments = vec![None; fields.len()];
                let field = kernel.return_field_from_args(ReturnFieldArgs {
                    arg_fields: &fields,
                    scalar_arguments: &scalar_arguments,
                })?;
                assert_eq!(
                    field.as_ref(),
                    &Field::new(kernel.name(), DataType::UInt64, nullable)
                );
                assert_eq!(kernel.signature(), kernel.inner().signature());
            }
        }
        Ok(())
    }
}
