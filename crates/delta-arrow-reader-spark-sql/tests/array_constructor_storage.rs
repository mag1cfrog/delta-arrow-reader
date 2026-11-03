//! ARRAY keeps its declared outer List while retaining each child's storage type.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, Int32Array, LargeListArray, ListArray,
    StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::functions_nested::make_array::MakeArray;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::scalar::array::spark_array::SparkArray;
use serde_json::{Value, json};

#[path = "support/spark_results.rs"]
#[expect(
    dead_code,
    reason = "Only shared schema/batch validation and STRUCT encoding are used."
)]
mod spark_results;
use spark_results::TestResult;

#[test]
fn spark_array_matches_native_outer_list_storage() -> TestResult {
    let primitive: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(11),
        None,
        Some(21),
        Some(22),
        Some(31),
        Some(32),
    ]));
    let duplicate_struct: ArrayRef = Arc::new(StructArray::try_new(
        vec![
            Field::new("item", primitive.data_type().clone(), true)
                .with_metadata([("position".to_owned(), "first".to_owned())].into()),
            Field::new("item", primitive.data_type().clone(), false),
        ]
        .into(),
        vec![
            primitive.clone(),
            Arc::new(Int32Array::from(vec![12, 13, 23, 24, 33, 34])),
        ],
        Some(NullBuffer::from(vec![true, false, true, true, true, true])),
    )?);
    for values in [primitive, duplicate_struct] {
        let field = Arc::new(
            Field::new("element", values.data_type().clone(), true)
                .with_metadata([("source".to_owned(), "retained".to_owned())].into()),
        );
        let nulls = Some(NullBuffer::from(vec![true, false, true]));
        let children: Vec<ArrayRef> = vec![
            Arc::new(ListArray::try_new(
                field.clone(),
                OffsetBuffer::new(vec![0_i32, 2, 4, 6].into()),
                values.clone(),
                nulls.clone(),
            )?),
            Arc::new(LargeListArray::try_new(
                field.clone(),
                OffsetBuffer::new(vec![0_i64, 2, 4, 6].into()),
                values.clone(),
                nulls.clone(),
            )?),
            Arc::new(FixedSizeListArray::try_new(field, 2, values, nulls)?),
        ];
        for child in children {
            for (offset, length) in [(0, 3), (1, 2), (1, 0)] {
                let child = child.slice(offset, length);
                let input_field = Arc::new(Field::new("input", child.data_type().clone(), true));
                let arg_fields = vec![input_field; 2];
                let functions: [&dyn ScalarUDFImpl; 2] = [&MakeArray::new(), &SparkArray::new()];
                let mut results = Vec::new();
                for function in functions {
                    let return_field = function.return_field_from_args(ReturnFieldArgs {
                        arg_fields: &arg_fields,
                        scalar_arguments: &[],
                    })?;
                    let actual = function
                        .invoke_with_args(ScalarFunctionArgs {
                            args: vec![ColumnarValue::Array(child.clone()); 2],
                            arg_fields: arg_fields.clone(),
                            number_rows: length,
                            return_field: return_field.clone(),
                            config_options: Default::default(),
                        })?
                        .into_array(length)?;
                    actual.to_data().validate_full()?;
                    assert_eq!(actual.data_type(), return_field.data_type());
                    assert_eq!(
                        actual.as_list::<i32>().values().data_type(),
                        child.data_type()
                    );
                    for row in 0..length {
                        let elements = actual.as_list::<i32>().value(row);
                        assert_eq!(elements.len(), 2);
                        for index in 0..2 {
                            assert_eq!(
                                elements.slice(index, 1).to_data(),
                                child.slice(row, 1).to_data()
                            );
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

#[tokio::test]
async fn spark_array_retains_large_list_columns_through_sql() -> TestResult {
    let fields: Fields = vec![
        Field::new("X", DataType::Int32, true)
            .with_metadata([("position".to_owned(), "first".to_owned())].into()),
        Field::new("Y", DataType::Int32, false),
    ]
    .into();
    let left_values: ArrayRef = Arc::new(StructArray::try_new(
        fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![Some(11), None, None, Some(31)])),
            Arc::new(Int32Array::from(vec![12, 13, 23, 32])),
        ],
        Some(NullBuffer::from(vec![true, true, false, true])),
    )?);
    let right_values: ArrayRef = Arc::new(StructArray::try_new(
        fields,
        vec![
            Arc::new(Int32Array::from(vec![Some(101), Some(301), None])),
            Arc::new(Int32Array::from(vec![102, 302, 303])),
        ],
        Some(NullBuffer::from(vec![true, true, false])),
    )?);
    let element = Arc::new(
        Field::new("element", left_values.data_type().clone(), true)
            .with_metadata([("source".to_owned(), "retained".to_owned())].into()),
    );
    let left_nulls = Some(NullBuffer::from(vec![true, false, true, true]));
    let left_list: ArrayRef = Arc::new(ListArray::try_new(
        element.clone(),
        OffsetBuffer::from_lengths([2, 0, 0, 2]),
        left_values.clone(),
        left_nulls.clone(),
    )?);
    let left_large: ArrayRef = Arc::new(LargeListArray::try_new(
        element.clone(),
        OffsetBuffer::from_lengths([2, 0, 0, 2]),
        left_values,
        left_nulls,
    )?);
    let right_large: ArrayRef = Arc::new(LargeListArray::try_new(
        element.clone(),
        OffsetBuffer::from_lengths([1, 0, 0, 2]),
        right_values,
        Some(NullBuffer::from(vec![true, true, false, true])),
    )?);
    let expected_schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "result",
            DataType::List(Arc::new(Field::new_list_field(
                DataType::LargeList(element),
                true,
            ))),
            false,
        ),
    ]);
    // Literal rows preserve argument positions, NULL lists, empty lists and nested NULLs.
    let expected_rows = json!([
        [0, [[], null]],
        [1, [null, []]],
        [2, [[null, ["31", "32"]], [["301", "302"], null]]],
        [3, [[["11", "12"], [null, "13"]], [["101", "102"]]]]
    ]);
    for batch_size in [1, 32] {
        for left in [&left_large, &left_list] {
            let session = SessionContext::new_with_state(
                configure_spark_sql(
                    SessionStateBuilder::new()
                        .with_default_features()
                        .with_config(SessionConfig::new().with_batch_size(batch_size)),
                )
                .build(),
            );
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("left_value", left.data_type().clone(), true),
                Field::new("right_value", right_large.data_type().clone(), true),
            ]));
            let input = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![3, 1, 0, 2])),
                    left.clone(),
                    right_large.clone(),
                ],
            )?;
            let batches = (0..input.num_rows())
                .step_by(batch_size)
                .map(|offset| input.slice(offset, batch_size.min(input.num_rows() - offset)))
                .collect();
            session.register_table(
                "retained_lists",
                Arc::new(MemTable::try_new(schema, vec![batches])?),
            )?;
            for ansi_mode in [true, false] {
                let query = plan_query(
                    &session,
                    "SELECT id,ARRAY(left_value,right_value) AS result FROM retained_lists ORDER BY id",
                    &SparkSqlOptions {
                        ansi_mode,
                        ..SparkSqlOptions::default()
                    },
                )
                .await?;
                let declared = query.logical_plan.schema().field(1);
                assert_eq!(declared.data_type(), expected_schema.field(1).data_type());
                assert!(!declared.is_nullable());
                let physical = query.create_physical_plan(&session).await?;
                let schema = physical.schema();
                assert_eq!(schema.as_ref(), &expected_schema);
                spark_results::validate_physical_schema(
                    query.logical_plan.schema().fields(),
                    &query.column_names,
                    schema.fields(),
                )?;
                let mut rows = Vec::new();
                for batch in collect(physical, session.task_ctx()).await? {
                    spark_results::validate_batch(schema.as_ref(), &batch)?;
                    let ids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or("expected INT IDs")?;
                    let outer = batch.column(1).as_list::<i32>();
                    assert_eq!(outer.data_type(), expected_schema.field(1).data_type());
                    for row in 0..batch.num_rows() {
                        let children = outer.value(row);
                        let children = children.as_list::<i64>();
                        let mut arguments = Vec::new();
                        for argument in 0..children.len() {
                            arguments.push(if children.is_null(argument) {
                                Value::Null
                            } else {
                                let values = children.value(argument);
                                json!(
                                    (0..values.len())
                                        .map(|index| {
                                            spark_results::encode_value(values.as_ref(), index)
                                        })
                                        .collect::<TestResult<Vec<_>>>()?
                                )
                            });
                        }
                        rows.push(json!([ids.value(row), arguments]));
                    }
                }
                assert_eq!(
                    json!(rows),
                    expected_rows,
                    "left={:?} ANSI={ansi_mode} batch={batch_size}",
                    left.data_type()
                );
            }
        }
    }
    Ok(())
}
