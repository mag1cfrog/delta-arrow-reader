use std::sync::Arc;

use datafusion::arrow::array::Array;
use datafusion::arrow::compute::{is_null, nullif};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_functions_nested::string::ArrayToString;

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct ArrayJoin {
    inner: ArrayToString,
}

impl ScalarUDFImpl for ArrayJoin {
    fn name(&self) -> &str {
        "array_join"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(nullable)))
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let Some(array) = args.args.first_mut()
            && array.data_type() == DataType::Null
        {
            // Native joining requires a typed list even when every array is NULL.
            let array_type = DataType::new_list(DataType::Utf8, true);
            *array = array.cast_to(&array_type, None)?;
            if let Some(field) = args.arg_fields.first_mut() {
                *field = Arc::new(field.as_ref().clone().with_data_type(array_type));
            }
        }
        let replacement = args.args.get(2).cloned();
        let result = self.inner.invoke_with_args(args)?;
        // Spark distinguishes an omitted replacement from an explicitly NULL one.
        match replacement {
            Some(ColumnarValue::Scalar(value)) if value.is_null() => {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
            }
            Some(ColumnarValue::Array(replacement)) if replacement.null_count() > 0 => {
                Ok(ColumnarValue::Array(nullif(
                    result.into_array(replacement.len())?.as_ref(),
                    &is_null(replacement.as_ref())?,
                )?))
            }
            _ => Ok(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{ArrayRef, ListBuilder, NullArray, StringArray, StringBuilder};
    use datafusion::arrow::datatypes::Field;

    use super::*;

    fn invoke_join(arguments: Vec<ColumnarValue>, rows: usize) -> Result<ArrayRef> {
        let fields = arguments
            .iter()
            .map(|value| Arc::new(Field::new("", value.data_type(), true)))
            .collect();
        ArrayJoin::default()
            .invoke_with_args(ScalarFunctionArgs {
                args: arguments,
                arg_fields: fields,
                number_rows: rows,
                return_field: Arc::new(Field::new("", DataType::Utf8, true)),
                config_options: Arc::new(Default::default()),
            })?
            .into_array(rows)
    }

    #[test]
    fn replacement_nulls_preserve_empty_and_nonempty_batch_lengths() -> Result<()> {
        let delimiter = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".into())));
        for rows in [0, 1, 3] {
            let mut lists = ListBuilder::new(StringBuilder::new());
            for _ in 0..rows {
                lists.values().append_value("a");
                lists.values().append_null();
                lists.append(true);
            }
            let values = ColumnarValue::Array(Arc::new(lists.finish()));
            let null_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(None));
            let replacements = [Some("?"), None, Some("")];
            let replaced = invoke_join(vec![values.clone(), delimiter.clone(), null_scalar], rows)?;
            assert_eq!(replaced.len(), rows);
            assert_eq!(replaced.null_count(), rows);
            let replaced = invoke_join(
                vec![
                    values,
                    delimiter.clone(),
                    ColumnarValue::Array(Arc::new(StringArray::from(
                        replacements[..rows].to_vec(),
                    ))),
                ],
                rows,
            )?;
            assert_eq!(
                replaced.as_ref(),
                &StringArray::from([Some("a,?"), None, Some("a,")][..rows].to_vec())
            );
            let nulls = invoke_join(
                vec![
                    ColumnarValue::Array(Arc::new(NullArray::new(rows))),
                    delimiter.clone(),
                ],
                rows,
            )?;
            assert_eq!(nulls.len(), rows);
            assert_eq!(nulls.null_count(), rows);
        }
        Ok(())
    }

    #[test]
    fn null_arguments_do_not_bypass_argument_validation() {
        let null_array = ColumnarValue::Scalar(ScalarValue::Null);
        let null_string = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        for arguments in [
            vec![null_array.clone()],
            vec![
                null_array.clone(),
                null_string.clone(),
                null_string.clone(),
                null_string.clone(),
            ],
        ] {
            assert!(matches!(invoke_join(arguments, 1), Err(error)
                if error.to_string().contains("array_to_string expects two or three arguments")));
        }
        let result = invoke_join(
            vec![
                null_array,
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                null_string,
            ],
            1,
        );
        assert!(matches!(result, Err(error)
            if error.to_string().contains("unsupported type for second argument")));
    }
}
