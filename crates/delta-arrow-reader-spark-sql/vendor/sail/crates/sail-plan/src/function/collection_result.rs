//! Refine filtered COLLECT results while rejecting unexpected NULL elements.

use std::sync::Arc;

use arrow::array::{Array, ListArray};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::cast::as_list_array;
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct RequireNonNullCollectionElements;

impl ScalarUDFImpl for RequireNonNullCollectionElements {
    fn name(&self) -> &str {
        "spark_require_non_null_collection_elements"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, arguments: &[DataType]) -> Result<DataType> {
        let [DataType::List(field)] = arguments else {
            return plan_err!("expected one collection result list");
        };
        Ok(DataType::List(Arc::new(
            field.as_ref().clone().with_nullable(false),
        )))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = &args.arg_fields[0];
        Ok(Arc::new(
            field
                .as_ref()
                .clone()
                .with_name(self.name())
                .with_data_type(self.return_type(&[field.data_type().clone()])?),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = datafusion_common::utils::take_function_args(self.name(), args.args)?;
        let scalar = matches!(value, ColumnarValue::Scalar(_));
        let array = value.to_array(1)?;
        let list = as_list_array(array.as_ref())?;
        let DataType::List(field) = self.return_type(&[array.data_type().clone()])? else {
            unreachable!("collection result is a list");
        };
        // ARRAY_AGG already removed NULLs; Arrow still checks the claimed element contract.
        let result = Arc::new(ListArray::try_new(
            field,
            list.offsets().clone(),
            list.values().clone(),
            list.nulls().cloned(),
        )?);
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::List(result))
        } else {
            ColumnarValue::Array(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Array, ArrayRef, Int32Array, StructArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;

    fn invoke_collection_element_check(
        value: ColumnarValue,
        nullable: bool,
    ) -> Result<(ColumnarValue, FieldRef)> {
        let field = Arc::new(
            Field::new("collected", value.data_type(), nullable)
                .with_metadata([("purpose".into(), "test".into())].into()),
        );
        let udf = RequireNonNullCollectionElements;
        let result_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&field),
            scalar_arguments: &[None],
        })?;
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![value],
            arg_fields: vec![field],
            number_rows: 3,
            return_field: result_field.clone(),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        Ok((result, result_field))
    }

    #[test]
    fn collection_elements_keep_nested_nulls_metadata_and_storage() -> Result<()> {
        let child = Arc::new(
            Field::new("value", DataType::Int32, true)
                .with_metadata([("unit".into(), "count".into())].into()),
        );
        let values: ArrayRef = Arc::new(StructArray::try_new(
            vec![child].into(),
            vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))],
            None,
        )?);
        let element = Arc::new(
            Field::new("element", values.data_type().clone(), true)
                .with_metadata([("origin".into(), "collection".into())].into()),
        );
        let list = ListArray::try_new(
            element.clone(),
            OffsetBuffer::new(vec![0, 1, 1, 3].into()),
            values.clone(),
            None,
        )?;
        for len in [0, 1, 2] {
            let input = list.slice(1, len);
            let (result, field) = invoke_collection_element_check(
                ColumnarValue::Array(Arc::new(input.clone())),
                false,
            )?;
            let ColumnarValue::Array(result) = result else {
                panic!("expected array");
            };
            let output = as_list_array(result.as_ref())?;
            assert_eq!(output.len(), len);
            assert_eq!(output.offsets(), input.offsets());
            assert_eq!(output.nulls(), input.nulls());
            assert!(Arc::ptr_eq(output.values(), &values));
            assert_eq!(
                output.data_type(),
                &DataType::List(Arc::new(element.as_ref().clone().with_nullable(false)))
            );
            assert_eq!(field.data_type(), output.data_type());
            assert!(!field.is_nullable());
            assert_eq!(
                field.metadata().get("purpose").map(String::as_str),
                Some("test")
            );
        }
        let scalar = Arc::new(list.slice(2, 1));
        let (result, _) = invoke_collection_element_check(
            ColumnarValue::Scalar(ScalarValue::List(scalar.clone())),
            false,
        )?;
        let ColumnarValue::Scalar(ScalarValue::List(result)) = result else {
            panic!("expected scalar list");
        };
        assert_eq!(result.offsets(), scalar.offsets());
        assert!(Arc::ptr_eq(result.values(), &values));

        let nullable = ListArray::try_new(
            element,
            OffsetBuffer::new(vec![0, 1, 1, 3].into()),
            values,
            Some(NullBuffer::from(vec![true, false, true])),
        )?;
        let (result, field) = invoke_collection_element_check(
            ColumnarValue::Array(Arc::new(nullable.clone())),
            true,
        )?;
        let ColumnarValue::Array(result) = result else {
            panic!("expected array");
        };
        assert_eq!(result.nulls(), nullable.nulls());
        assert!(field.is_nullable());
        Ok(())
    }

    #[test]
    fn collection_elements_reject_actual_nulls_and_non_list_types() -> Result<()> {
        let values: ArrayRef = Arc::new(StructArray::try_new(
            vec![Field::new("value", DataType::Int32, true)].into(),
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
            Some(NullBuffer::from(vec![true, false])),
        )?);
        let list = ListArray::try_new(
            Arc::new(Field::new("element", values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 2].into()),
            values,
            None,
        )?;
        for input in [
            ColumnarValue::Array(Arc::new(list.clone())),
            ColumnarValue::Scalar(ScalarValue::List(Arc::new(list))),
        ] {
            let error = invoke_collection_element_check(input, false)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("Non-nullable field") && error.contains("nulls"),
                "{error}"
            );
        }
        assert!(
            RequireNonNullCollectionElements
                .return_type(&[DataType::Int32])
                .is_err()
        );
        Ok(())
    }
}
