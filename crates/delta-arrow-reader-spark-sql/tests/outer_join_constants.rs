//! Outer joins can turn projected constants into NULL before grouping or set operations.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{JoinType, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::equivalence::{
    AcrossPartitions, EquivalenceClass, EquivalenceGroup,
};
use datafusion::physical_expr::expressions::{Column, IsNullExpr, lit};
use datafusion::physical_expr::{ConstExpr, PhysicalExprRef};
use datafusion::physical_plan::joins::CrossJoinExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[test]
fn join_equivalences_remain_valid_for_null_extended_rows() -> TestResult {
    let column =
        |index: usize| Arc::new(Column::new(&format!("c{index}"), index)) as PhysicalExprRef;
    let nonnull = lit(7_i64);
    let null = lit(ScalarValue::Int64(None));
    let computed: PhysicalExprRef = Arc::new(IsNullExpr::new(column(0)));
    let mut input = EquivalenceGroup::new([
        EquivalenceClass::new([column(0), column(1), nonnull.clone()]),
        EquivalenceClass::new([column(2), column(3), null.clone()]),
        EquivalenceClass::new([column(4), computed.clone()]),
        EquivalenceClass::new([column(5), column(6)]),
    ]);
    input.add_constant(ConstExpr::new(column(5), AcrossPartitions::Heterogeneous));
    input.add_constant(ConstExpr::new(column(7), AcrossPartitions::Uniform(None)));
    input.add_constant(ConstExpr::new(
        column(8),
        AcrossPartitions::Uniform(Some(ScalarValue::Int64(Some(9)))),
    ));
    input.add_constant(ConstExpr::new(
        column(9),
        AcrossPartitions::Uniform(Some(ScalarValue::Int64(None))),
    ));

    for (kind, sides) in [
        (JoinType::Inner, vec![(0, true), (10, true)]),
        (JoinType::Left, vec![(0, true), (10, false)]),
        (JoinType::Right, vec![(0, false), (10, true)]),
        (JoinType::Full, vec![(0, false), (10, false)]),
        (JoinType::LeftSemi, vec![(0, true)]),
        (JoinType::LeftAnti, vec![(0, true)]),
        (JoinType::LeftMark, vec![(0, true)]),
        (JoinType::RightSemi, vec![(0, true)]),
        (JoinType::RightAnti, vec![(0, true)]),
        (JoinType::RightMark, vec![(0, true)]),
    ] {
        let output = input.join(&input, &kind, 10, &[])?;
        for (offset, preserved) in sides {
            let shifted = |index: usize| {
                Arc::new(Column::new(&format!("c{index}"), index + offset)) as PhysicalExprRef
            };
            assert!(
                output.exprs_equal(&shifted(0), &shifted(1)),
                "{kind}: column equality"
            );
            assert!(
                output.exprs_equal(&shifted(2), &shifted(3)),
                "{kind}: NULL columns"
            );
            assert!(
                output.exprs_equal(&shifted(2), &null),
                "{kind}: NULL literal"
            );
            assert!(
                output.exprs_equal(&shifted(5), &shifted(6)),
                "{kind}: unknown constant columns"
            );
            assert_eq!(
                output.exprs_equal(&shifted(0), &nonnull),
                preserved,
                "{kind}: non-NULL literal"
            );
            let expression: PhysicalExprRef = Arc::new(IsNullExpr::new(shifted(0)));
            assert_eq!(
                output.exprs_equal(&shifted(4), &expression),
                preserved,
                "{kind}: non-null-preserving expression"
            );
            for index in [0, 5, 7, 8] {
                assert_eq!(
                    output.is_expr_constant(&shifted(index)).is_some(),
                    preserved,
                    "{kind}: constant {index}"
                );
            }
            for index in [2, 3, 9] {
                assert_eq!(
                    output.is_expr_constant(&shifted(index)),
                    Some(AcrossPartitions::Uniform(Some(ScalarValue::Int64(None)))),
                    "{kind}: known NULL {index}"
                );
            }
            if preserved {
                assert_eq!(
                    output.is_expr_constant(&shifted(5)),
                    Some(AcrossPartitions::Heterogeneous)
                );
                assert_eq!(
                    output.is_expr_constant(&shifted(7)),
                    Some(AcrossPartitions::Uniform(None))
                );
                assert_eq!(
                    output.is_expr_constant(&shifted(8)),
                    Some(AcrossPartitions::Uniform(Some(ScalarValue::Int64(Some(9)))))
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn cross_join_retains_both_inputs_constant_properties() -> TestResult {
    let input = Arc::new(PlaceholderRowExec::new(Arc::new(Schema::empty())));
    let left = Arc::new(ProjectionExec::try_new(
        vec![(lit(7_i64), "left_value".to_string())],
        input.clone(),
    )?);
    let right = Arc::new(ProjectionExec::try_new(
        vec![(lit(9_i64), "right_value".to_string())],
        input,
    )?);
    let joined = Arc::new(CrossJoinExec::new(left, right));
    for (index, name, value) in [(0, "left_value", 7), (1, "right_value", 9)] {
        let column: PhysicalExprRef = Arc::new(Column::new(name, index));
        assert_eq!(
            joined
                .properties()
                .equivalence_properties()
                .eq_group()
                .is_expr_constant(&column),
            Some(AcrossPartitions::Uniform(Some(ScalarValue::Int64(Some(
                value
            )))))
        );
    }
    let batches = collect(joined, SessionContext::new().task_ctx()).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    for batch in batches.iter().filter(|batch| batch.num_rows() > 0) {
        for (index, value) in [(0, 7), (1, 9)] {
            assert_eq!(
                ScalarValue::try_from_array(batch.column(index), 0)?,
                ScalarValue::Int64(Some(value))
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn outer_join_grouping_and_sets_match_spark() -> TestResult {
    let records = include_str!("fixtures/outer-join-constants-spark.jsonl")
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(records.len() - 1, 900);
    assert_eq!(records[0]["spark_version"], "4.2.0");
    assert_eq!(records[0]["timezone"], "UTC");
    let mut differences = Vec::new();
    let mut observations = 0;
    for spark in [false, true] {
        for batch_size in [1, 32] {
            for partitions in [1, 2] {
                let builder = SessionStateBuilder::new()
                    .with_default_features()
                    .with_config(
                        SessionConfig::new()
                            .with_batch_size(batch_size)
                            .with_target_partitions(partitions),
                    );
                let session = SessionContext::new_with_state(
                    if spark {
                        configure_spark_sql(builder)
                    } else {
                        builder
                    }
                    .build(),
                );
                register_label_inputs(&session, &records[0], batch_size, partitions)?;
                for case in &records[1..] {
                    if !spark {
                        // Native ALL uses semi/anti joins, not Spark's duplicate counts.
                        let sql = case["sql"].as_str().ok_or("missing SQL")?;
                        if case["ansi"] == false
                            || sql.contains("INTERSECT ALL")
                            || sql.contains("EXCEPT ALL")
                        {
                            continue;
                        }
                    }
                    observations += 1;
                    let actual = execute_join_case(&session, case, spark).await;
                    let mut expected = case["rows"].clone();
                    normalize_zero_signs_and_row_order(&mut expected, case)?;
                    if !matches!(&actual, Ok(rows) if rows == &expected) {
                        differences.push(format!("{} Spark={spark} ANSI={} batch={batch_size} partitions={partitions}: expected {expected}, got {actual:?}", case["id"],case["ansi"]));
                    }
                }
            }
        }
    }
    assert_eq!(observations, 4600);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}

fn register_label_inputs(
    session: &SessionContext,
    metadata: &Value,
    batch_size: usize,
    partition_count: usize,
) -> TestResult {
    for (name, values) in metadata["inputs"].as_object().ok_or("missing inputs")? {
        let values = values.as_array().ok_or("missing label values")?;
        let labels: ArrayRef = Arc::new(StringArray::from(
            values.iter().map(Value::as_str).collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, true)])),
            vec![labels],
        )?;
        let mut partitions = vec![Vec::new(); partition_count];
        for (index, start) in (0..batch.num_rows()).step_by(batch_size).enumerate() {
            partitions[index % partition_count]
                .push(batch.slice(start, batch_size.min(batch.num_rows() - start)));
        }
        session.register_table(
            name,
            Arc::new(MemTable::try_new(batch.schema(), partitions)?),
        )?;
    }
    Ok(())
}

async fn execute_join_case(
    session: &SessionContext,
    case: &Value,
    spark: bool,
) -> TestResult<Value> {
    let sql = case["sql"].as_str().ok_or("missing SQL")?;
    let physical = if spark {
        plan_query(
            session,
            sql,
            &SparkSqlOptions {
                ansi_mode: case["ansi"].as_bool().ok_or("missing ANSI")?,
                ..SparkSqlOptions::default()
            },
        )
        .await?
        .create_physical_plan(session)
        .await?
    } else {
        session.sql(sql).await?.create_physical_plan().await?
    };
    let schema = physical.schema();
    let fields = case["schema"]["fields"]
        .as_array()
        .ok_or("missing fields")?;
    assert_eq!(schema.fields().len(), fields.len());
    for (field, expected) in schema.fields().iter().zip(fields) {
        assert_eq!(
            field.name(),
            expected["name"].as_str().ok_or("missing field name")?
        );
        let name = match field.data_type() {
            DataType::Float32 => "float",
            DataType::Float64 => "double",
            DataType::Int64 => "long",
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => "string",
            other => return Err(format!("unexpected field type: {other}").into()),
        };
        assert_eq!(expected["type"], name);
    }
    let mut rows = Vec::new();
    for batch in collect(physical, session.task_ctx()).await? {
        for row in 0..batch.num_rows() {
            let mut values = Vec::new();
            for (index, array) in batch.columns().iter().enumerate() {
                let value = ScalarValue::try_from_array(array, row)?;
                values.push(if value.is_null() {
                    assert!(schema.field(index).is_nullable());
                    Value::Null
                } else {
                    match value {
                        ScalarValue::Float32(value) => json!(value),
                        ScalarValue::Float64(value) => json!(value),
                        ScalarValue::Int64(value) => json!(value),
                        ScalarValue::Utf8(value)
                        | ScalarValue::LargeUtf8(value)
                        | ScalarValue::Utf8View(value) => json!(value),
                        other => return Err(format!("unexpected value: {other}").into()),
                    }
                });
            }
            rows.push(json!(values));
        }
    }
    let mut rows = json!(rows);
    normalize_zero_signs_and_row_order(&mut rows, case)?;
    Ok(rows)
}

// Zero representation is a separate contract; every NULL and duplicate still counts.
fn normalize_zero_signs_and_row_order(rows: &mut Value, case: &Value) -> TestResult {
    let rows = rows.as_array_mut().ok_or("missing rows")?;
    if matches!(
        case["value_type"].as_str(),
        Some("float" | "double" | "null_double")
    ) {
        for row in rows.iter_mut() {
            if row[0].as_f64() == Some(0.0) {
                row[0] = json!(0.0);
            }
        }
    }
    rows.sort_by_key(Value::to_string);
    Ok(())
}
