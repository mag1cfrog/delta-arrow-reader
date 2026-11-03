//! Shared STRUCT casts retain duplicate positions and native name matching.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::ScalarValue;
use datafusion::common::nested_struct::{cast_column, validate_data_type_compatibility};
use datafusion::logical_expr::ColumnarValue;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn fields(first: DataType, second: DataType, label: &str) -> Fields {
    [first, second]
        .into_iter()
        .enumerate()
        .map(|(index, data_type)| {
            Arc::new(Field::new("item", data_type, true).with_metadata(
                [("position".to_owned(), format!("{label}:{index}"))].into(),
            ))
        })
        .collect()
}

#[test]
fn duplicate_struct_scalar_and_array_casts_keep_each_child() -> TestResult {
    let first = vec![Some(11), None, Some(31)];
    let second = vec![Some(12), Some(22), None];
    let nulls = Some(NullBuffer::from(vec![true, false, true]));
    let source: ArrayRef = Arc::new(StructArray::try_new(
        fields(DataType::Int32, DataType::Int32, "source"),
        vec![
            Arc::new(Int32Array::from(first.clone())),
            Arc::new(Int32Array::from(second.clone())),
        ],
        nulls.clone(),
    )?);
    for (first_type, second_type) in [
        (DataType::Int64, DataType::Int32),
        (DataType::Int32, DataType::Int64),
        (DataType::Int32, DataType::Int32),
    ] {
        let columns = [(&first, &first_type), (&second, &second_type)]
            .into_iter()
            .map(|(values, data_type)| -> ArrayRef {
                if data_type == &DataType::Int64 {
                    Arc::new(Int64Array::from(
                        values.iter().map(|value| value.map(i64::from)).collect::<Vec<_>>(),
                    ))
                } else {
                    Arc::new(Int32Array::from(values.clone()))
                }
            })
            .collect();
        let expected: ArrayRef = Arc::new(StructArray::try_new(
            fields(first_type, second_type, "target"),
            columns,
            nulls.clone(),
        )?);
        for len in [0, 1, 3] {
            let actual = ColumnarValue::Array(source.slice(0, len))
                .cast_to(expected.data_type(), None)?
                .into_array(len)?;
            actual.to_data().validate_full()?;
            assert_eq!(actual.to_data(), expected.slice(0, len).to_data());
        }
        for row in 0..source.len() {
            let actual = ScalarValue::try_from_array(&source, row)?
                .cast_to(expected.data_type())?;
            assert_eq!(actual, ScalarValue::try_from_array(&expected, row)?);
        }
    }
    Ok(())
}

#[test]
fn duplicate_struct_casts_validate_each_positional_field() -> TestResult {
    let source: ArrayRef = Arc::new(StructArray::try_new(
        vec![
            Field::new("item", DataType::Int32, false),
            Field::new("item", DataType::Int32, true),
        ].into(),
        vec![Arc::new(Int32Array::from(vec![1])), Arc::new(Int32Array::from(vec![None]))],
        None,
    )?);
    let required = DataType::Struct(vec![
        Field::new("item", DataType::Int64, false),
        Field::new("item", DataType::Int32, false),
    ].into());
    assert!(validate_data_type_compatibility("r", source.data_type(), &required).is_err());
    assert!(cast_column(&source, &required, &CastOptions::default()).is_err());
    let incompatible = DataType::Struct(vec![
        Field::new("item", DataType::Int64, true),
        Field::new("item", DataType::Struct(vec![Field::new("child", DataType::Int32, true)].into()), true),
    ].into());
    assert!(validate_data_type_compatibility("r", source.data_type(), &incompatible).is_err());
    assert!(cast_column(&source, &incompatible, &CastOptions::default()).is_err());
    let missing_required = DataType::Struct(vec![
        Field::new("item", DataType::Int64, true),
        Field::new("item", DataType::Int32, true),
        Field::new("missing", DataType::Int32, false),
    ].into());
    assert!(cast_column(&source, &missing_required, &CastOptions::default()).is_err());
    Ok(())
}

#[test]
fn duplicate_struct_casts_keep_strict_and_safe_leaf_errors() -> TestResult {
    let source: ArrayRef = Arc::new(StructArray::try_new(
        fields(DataType::Utf8, DataType::Int32, "source"),
        vec![Arc::new(StringArray::from(vec!["invalid"])), Arc::new(Int32Array::from(vec![22]))],
        None,
    )?);
    let target = fields(DataType::Int64, DataType::Int32, "target");
    let target_type = DataType::Struct(target.clone());
    let strict = CastOptions { safe: false, ..CastOptions::default() };
    assert!(cast_column(&source, &target_type, &strict).is_err());
    let actual = cast_column(&source, &target_type, &CastOptions::default())?;
    let expected = StructArray::try_new(target, vec![
        Arc::new(Int64Array::from(vec![None])), Arc::new(Int32Array::from(vec![22])),
    ], None)?;
    actual.to_data().validate_full()?;
    assert_eq!(actual.to_data(), expected.to_data());
    let required_source: ArrayRef = Arc::new(StructArray::try_new(vec![
        Field::new("item", DataType::Utf8, false),
        Field::new("item", DataType::Int32, false),
    ].into(), vec![
        Arc::new(StringArray::from(vec!["invalid"])), Arc::new(Int32Array::from(vec![22])),
    ], None)?);
    let required_target = DataType::Struct(vec![
        Field::new("item", DataType::Int64, false),
        Field::new("item", DataType::Int32, false),
    ].into());
    assert!(cast_column(&required_source, &required_target, &CastOptions::default()).is_err());
    Ok(())
}

#[test]
fn unique_struct_names_keep_native_reordering_and_missing_fields() -> TestResult {
    let source: ArrayRef = Arc::new(StructArray::try_new(vec![
        Field::new("left", DataType::Int32, false),
        Field::new("right", DataType::Int32, false),
        Field::new("extra", DataType::Int32, false),
    ].into(), vec![
        Arc::new(Int32Array::from(vec![11])), Arc::new(Int32Array::from(vec![22])),
        Arc::new(Int32Array::from(vec![33])),
    ], None)?);
    let target: Fields = vec![
        Field::new("right", DataType::Int64, false),
        Field::new("left", DataType::Int32, false),
        Field::new("missing", DataType::Int32, true),
    ].into();
    let actual = cast_column(&source, &DataType::Struct(target.clone()), &CastOptions::default())?;
    let expected = StructArray::try_new(target, vec![
        Arc::new(Int64Array::from(vec![22])), Arc::new(Int32Array::from(vec![11])),
        Arc::new(Int32Array::from(vec![None])),
    ], None)?;
    actual.to_data().validate_full()?;
    assert_eq!(actual.to_data(), expected.to_data());
    Ok(())
}

#[tokio::test]
async fn native_make_array_keeps_duplicate_widening_positions() -> TestResult {
    use arrow::array::AsArray;
    use arrow::datatypes::{Int32Type, Int64Type};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use delta_arrow_reader_spark_sql::configure_spark_sql;

    let native = SessionContext::new();
    let configured = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let sql = "SELECT make_array(named_struct('item',CAST(1 AS INT),'item',CAST(2 AS INT)),named_struct('item',CAST(3 AS BIGINT),'item',CAST(4 AS INT))) AS r";
    let expected_schema = native.sql(sql).await?.schema().clone();
    for session in [native, configured] {
        let frame = session.sql(sql).await?;
        assert_eq!(*frame.schema(), expected_schema);
        let batches = frame.collect().await?;
        assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
        for batch in batches {
            batch.column(0).to_data().validate_full()?;
            let values = batch.column(0).as_list::<i32>().values().as_struct();
            assert_eq!(values.column(0).as_primitive::<Int64Type>().values().as_ref(), &[1, 3]);
            assert_eq!(values.column(1).as_primitive::<Int32Type>().values().as_ref(), &[2, 4]);
        }
    }
    Ok(())
}
