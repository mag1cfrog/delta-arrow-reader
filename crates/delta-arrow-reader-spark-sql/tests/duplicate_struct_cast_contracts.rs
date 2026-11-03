//! Shared STRUCT casts retain duplicate positions and native name matching.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::ScalarValue;
use datafusion::common::nested_struct::{cast_column, validate_data_type_compatibility};
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::type_coercion::binary::{
    comparison_coercion, try_type_union_resolution_with_struct,
};

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
    Ok(())
}

#[test]
fn duplicate_struct_casts_reject_nulls_produced_for_required_fields() -> TestResult {
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

#[test]
fn constructor_struct_declarations_share_nullable_positional_fields() -> TestResult {
    let left = DataType::Struct(vec![
        Field::new("item", DataType::Int32, false)
            .with_metadata([("source".to_owned(), "left".to_owned())].into()),
        Field::new("item", DataType::Utf8, true),
    ].into());
    let right = DataType::Struct(vec![
        Field::new("item", DataType::Int64, true),
        Field::new("item", DataType::Utf8, false),
    ].into());
    let expected = DataType::Struct(vec![
        Field::new("item", DataType::Int64, true),
        Field::new("item", DataType::Utf8, true),
    ].into());
    assert_eq!(try_type_union_resolution_with_struct(&[left.clone(), right.clone()])?,
        vec![expected.clone(), expected.clone()]);

    let wrappers: [fn(DataType) -> DataType; 2] = [
        |value| DataType::Struct(vec![Field::new("child", value, true)].into()),
        |value| DataType::List(Arc::new(Field::new_list_field(value, true))),
    ];
    for wrap in wrappers {
        let nested_left = wrap(left.clone());
        let nested_right = wrap(right.clone());
        let nested_expected = wrap(expected.clone());
        assert_eq!(comparison_coercion(&nested_left, &nested_right), Some(nested_expected));
        let outer_left = DataType::Struct(vec![Field::new("nested", nested_left, false)].into());
        let outer_right = DataType::Struct(vec![Field::new("nested", nested_right, true)].into());
        let outer_expected = DataType::Struct(vec![Field::new("nested", wrap(expected.clone()), true)].into());
        assert_eq!(try_type_union_resolution_with_struct(&[outer_left, outer_right])?,
            vec![outer_expected.clone(), outer_expected]);
    }
    assert!(try_type_union_resolution_with_struct(&[
        DataType::Struct(vec![Field::new("left", DataType::Int32, false)].into()),
        DataType::Struct(vec![Field::new("right", DataType::Int32, false)].into()),
    ]).is_err());
    Ok(())
}

#[test]
fn constructor_struct_union_compares_complete_field_name_sequences() -> TestResult {
    let struct_type = |names: &[&str]| DataType::Struct(names.iter()
        .map(|name| Field::new(*name, DataType::Int32, false))
        .collect::<Vec<_>>().into());
    for (left_names, right_names) in [
        (vec!["a,b", "c", "a"], vec!["a", "b,c", "a"]),
        (vec!["a,b", "c"], vec!["a", "b", "c"]),
    ] {
        let left = struct_type(&left_names);
        let right = struct_type(&right_names);
        let error = try_type_union_resolution_with_struct(&[left.clone(), right])
            .expect_err("comma-separated names must not make different layouts match");
        assert!(error.to_string().contains("Expect same keys for struct type"));
        assert_eq!(try_type_union_resolution_with_struct(&[left.clone(), left.clone()])?,
            vec![left.clone(), left]);
    }
    Ok(())
}

#[test]
fn spark_array_merges_list_struct_metadata_and_validates_names() -> TestResult {
    use datafusion::logical_expr::ScalarUDFImpl;
    use sail_function::scalar::array::spark_array::SparkArray;

    let array = SparkArray::new();
    let child = |source: &str| DataType::Struct(vec![
        Field::new("x", DataType::Int32, true)
            .with_metadata([("source".to_owned(), source.to_owned())].into()),
    ].into());
    let list = |value| DataType::List(Arc::new(Field::new_list_field(value, true)));
    let left = list(child("left"));
    let right = list(child("right"));
    let expected = list(DataType::Struct(vec![Field::new("x", DataType::Int32, true)].into()));
    assert_eq!(array.coerce_types(&[left.clone(), right])?, vec![expected.clone(), expected]);
    assert_eq!(array.coerce_types(&[left.clone(), left.clone()])?, vec![left.clone(), left]);

    for (left, right) in [
        (list(DataType::Int32), list(DataType::Utf8)),
        (list(DataType::Float32), list(DataType::Int32)),
        (list(DataType::Float64), list(DataType::Decimal128(10, 2))),
        (list(DataType::Decimal128(12, 3)), list(DataType::Decimal128(10, 2))),
        (list(DataType::Struct(vec![Field::new("x", DataType::Utf8, true)].into())),
            list(DataType::Struct(vec![Field::new("x", DataType::Int32, true)].into()))),
    ] {
        let expected = comparison_coercion(&left, &right).ok_or("missing native List coercion")?;
        assert_eq!(array.coerce_types(&[left, right])?, vec![expected.clone(), expected]);
    }

    let left = DataType::Struct(vec![
        Field::new("a,b", DataType::Int32, true),
        Field::new("c", DataType::Int32, true),
        Field::new("a", DataType::Int32, false),
    ].into());
    let right = DataType::Struct(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b,c", DataType::Int32, true),
        Field::new("a", DataType::Int32, false),
    ].into());
    assert_eq!(comparison_coercion(&left, &right), None);
    let error = array.coerce_types(&[left, right])
        .expect_err("ARRAY must reject different complete STRUCT name sequences");
    assert!(error.to_string().contains("[DATATYPE_MISMATCH.DATA_DIFF_TYPES]"));
    Ok(())
}

#[tokio::test]
async fn spark_map_retains_the_struct_admission_error_cause() -> TestResult {
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build());
    let error = plan_query(&session,
        "SELECT MAP('left',NAMED_STRUCT('a,b',CAST(NULL AS INT),'c',CAST(NULL AS INT),'a',11),'right',NAMED_STRUCT('a',21,'b,c',CAST(NULL AS INT),'a',23)) AS r",
        &SparkSqlOptions::default()).await.err().ok_or("MAP must reject mismatched STRUCT values")?;
    assert!(error.to_string().contains("[DATATYPE_MISMATCH.CREATE_MAP_VALUE_DIFF_TYPES]"));
    let mut cause = std::error::Error::source(&error);
    let mut retained_cause = false;
    while let Some(current) = cause {
        retained_cause |= current.to_string().starts_with("Execution error: Expect same keys for struct type");
        cause = current.source();
    }
    assert!(retained_cause, "MAP must retain its STRUCT admission error cause: {error:?}");
    Ok(())
}

#[test]
fn constructor_admission_preserves_nested_duplicate_positions() -> TestResult {
    use datafusion::logical_expr::ScalarUDFImpl;
    use sail_function::scalar::array::spark_array::SparkArray;

    let array = SparkArray::new();
    let fields = |names: &[&str]| DataType::Struct(names.iter()
        .map(|name| Field::new(*name, DataType::Int32, false))
        .collect::<Vec<_>>().into());
    let left = fields(&["a", "a", "b"]);
    let right = fields(&["a", "b", "b"]);
    let wrappers: [fn(DataType) -> DataType; 2] = [
        |value| DataType::Struct(vec![Field::new("child", value, false)].into()),
        |value| DataType::List(Arc::new(Field::new_list_field(value, false))),
    ];
    for wrap in wrappers {
        let wrapped_left = wrap(left.clone());
        let wrapped_right = wrap(right.clone());
        for input in [vec![wrapped_left.clone(), wrapped_right.clone()],
            vec![DataType::Null, wrapped_left.clone(), wrapped_right.clone()]] {
            let error = array.coerce_types(&input)
                .expect_err("nested duplicate layouts must not remap values by name");
            assert!(error.to_string().contains("[DATATYPE_MISMATCH.DATA_DIFF_TYPES]"));
        }
        assert_eq!(array.coerce_types(&[DataType::Null, wrapped_left.clone(), wrapped_left.clone()])?,
            vec![wrapped_left; 3]);
    }
    let outer_left = DataType::Struct(vec![Field::new("child", left, false)].into());
    let outer_right = DataType::Struct(vec![Field::new("child", right, false)].into());
    assert!(try_type_union_resolution_with_struct(&[outer_left, outer_right]).is_err());
    Ok(())
}
