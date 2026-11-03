//! ARRAY keeps its declared outer List while retaining each child's storage type.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, FixedSizeListArray, Int32Array, LargeListArray, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::Field;
use datafusion::functions_nested::make_array::MakeArray;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use sail_function::scalar::array::spark_array::SparkArray;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn spark_array_matches_native_outer_list_storage() -> TestResult {
    let primitive: ArrayRef = Arc::new(Int32Array::from(vec![Some(11), None, Some(21), Some(22), Some(31), Some(32)]));
    let duplicate_struct: ArrayRef = Arc::new(StructArray::try_new(vec![
        Field::new("item", primitive.data_type().clone(), true)
            .with_metadata([("position".to_owned(), "first".to_owned())].into()),
        Field::new("item", primitive.data_type().clone(), false),
    ].into(), vec![primitive.clone(), Arc::new(Int32Array::from(vec![12, 13, 23, 24, 33, 34]))],
        Some(NullBuffer::from(vec![true, false, true, true, true, true])))?);
    for values in [primitive, duplicate_struct] {
        let field = Arc::new(Field::new("element", values.data_type().clone(), true)
            .with_metadata([("source".to_owned(), "retained".to_owned())].into()));
        let nulls = Some(NullBuffer::from(vec![true, false, true]));
        let children: Vec<ArrayRef> = vec![
            Arc::new(ListArray::try_new(field.clone(),
                OffsetBuffer::new(vec![0_i32, 2, 4, 6].into()), values.clone(), nulls.clone())?),
            Arc::new(LargeListArray::try_new(field.clone(),
                OffsetBuffer::new(vec![0_i64, 2, 4, 6].into()), values.clone(), nulls.clone())?),
            Arc::new(FixedSizeListArray::try_new(field, 2, values, nulls)?),
        ];
        for child in children {
            for (offset, length) in [(0, 3), (1, 2), (1, 0)] {
                let child = child.slice(offset, length);
                let arg_fields = vec![Arc::new(Field::new("input", child.data_type().clone(), true)); 2];
                let functions: [&dyn ScalarUDFImpl; 2] = [&MakeArray::new(), &SparkArray::new()];
                let mut results = Vec::new();
                for function in functions {
                    let return_field = function.return_field_from_args(ReturnFieldArgs {
                        arg_fields: &arg_fields, scalar_arguments: &[],
                    })?;
                    let actual = function.invoke_with_args(ScalarFunctionArgs {
                        args: vec![ColumnarValue::Array(child.clone()); 2],
                        arg_fields: arg_fields.clone(), number_rows: length,
                        return_field: return_field.clone(), config_options: Default::default(),
                    })?.into_array(length)?;
                    actual.to_data().validate_full()?;
                    assert_eq!(actual.data_type(), return_field.data_type());
                    assert_eq!(actual.as_list::<i32>().values().data_type(), child.data_type());
                    for row in 0..length {
                        let elements = actual.as_list::<i32>().value(row);
                        assert_eq!(elements.len(), 2);
                        for index in 0..2 {
                            assert_eq!(elements.slice(index, 1).to_data(), child.slice(row, 1).to_data());
                        }
                    }
                    results.push(actual);
                }
                assert_eq!(results[0].to_data(), results[1].to_data());
            }
        }
    }
    Ok(())
}
