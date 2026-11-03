//! Inner list views flatten like their canonical-width list controls.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, LargeListArray, LargeListViewArray,
    ListArray, ListViewArray, RecordBatch,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionConfig, SessionContext};
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use serde_json::{Value, json};

#[allow(dead_code)]
#[path = "support/spark_results.rs"]
mod spark_results;
use spark_results::TestResult;

struct Layout {
    name: &'static str,
    values: Vec<Option<i32>>,
    offsets: Vec<i32>,
    sizes: Vec<i32>,
    logical_inner: Vec<Option<Vec<Option<i32>>>>,
    outer_lengths: Vec<usize>,
    outer_validity: Option<Vec<bool>>,
    rows: Value,
}

fn layouts() -> Vec<Layout> {
    let mut layouts = Vec::new();
    // Frozen v3 ranges retain gaps/overlap; prefix/suffix rows expose sliced offsets.
    for (name, backing, offsets, flat) in [
        ("contiguous", vec![1, 2, 3, 4], vec![0, 2], vec![1, 2, 3, 4]),
        ("gapped", vec![99, 1, 2, 88, 3, 4, 77], vec![1, 4], vec![1, 2, 3, 4]),
        ("overlapping", vec![99, 1, 2, 3, 77], vec![1, 2], vec![1, 2, 2, 3]),
    ] {
        let end = backing.len() as i32 + 1;
        let mut values = vec![Some(-101)];
        values.extend(backing.into_iter().map(Some));
        values.push(Some(-102));
        layouts.push(Layout {
            name,
            values,
            offsets: vec![0, offsets[0] + 1, offsets[1] + 1, end],
            sizes: vec![1, 2, 2, 1],
            logical_inner: vec![
                Some(vec![Some(-101)]),
                Some(flat[..2].iter().copied().map(Some).collect()),
                Some(flat[2..].iter().copied().map(Some).collect()),
                Some(vec![Some(-102)]),
            ],
            outer_lengths: vec![1, 2, 1],
            outer_validity: None,
            rows: json!([[[-101]], [flat], [[-102]]]),
        });
    }
    layouts.push(Layout {
        name: "nullable",
        values: vec![Some(99), Some(1), None, Some(77), Some(4)],
        offsets: vec![1, 2, 4, 0],
        sizes: vec![2, 1, 1, 0],
        logical_inner: vec![Some(vec![Some(1), None]), None, Some(vec![Some(4)]), Some(vec![])],
        outer_lengths: vec![1, 2, 1, 0, 0],
        outer_validity: Some(vec![true, true, true, false, true]),
        rows: json!([[[1, null]], [null], [[]], [null], [[]]]),
    });
    layouts
}

fn inputs(layout: &Layout, large_inner: bool, large_outer: bool) -> TestResult<(ArrayRef, ArrayRef)> {
    let leaf_nullable = layout.logical_inner.iter().flatten().flatten().any(Option::is_none);
    let inner_nullable = layout.logical_inner.iter().any(Option::is_none);
    let leaf = Arc::new(
        Field::new("element", DataType::Int32, leaf_nullable)
            .with_metadata([("origin".into(), "inner-view-leaf".into())].into()),
    );
    let nulls = inner_nullable.then(|| {
        NullBuffer::from(layout.logical_inner.iter().map(Option::is_some).collect::<Vec<_>>())
    });
    let values: ArrayRef = Arc::new(Int32Array::from(layout.values.clone()));
    let canonical_values: ArrayRef = Arc::new(Int32Array::from(
        layout.logical_inner.iter().flatten().flatten().copied().collect::<Vec<_>>(),
    ));
    let lengths = layout.logical_inner.iter().map(|row| row.as_ref().map_or(0, Vec::len));
    let (view, canonical): (ArrayRef, ArrayRef) = if large_inner {
        (
            Arc::new(LargeListViewArray::try_new(
                leaf.clone(),
                ScalarBuffer::from(layout.offsets.iter().map(|&v| i64::from(v)).collect::<Vec<_>>()),
                ScalarBuffer::from(layout.sizes.iter().map(|&v| i64::from(v)).collect::<Vec<_>>()),
                values,
                nulls.clone(),
            )?),
            Arc::new(LargeListArray::try_new(
                leaf,
                OffsetBuffer::from_lengths(lengths),
                canonical_values,
                nulls,
            )?),
        )
    } else {
        (
            Arc::new(ListViewArray::try_new(
                leaf.clone(),
                ScalarBuffer::from(layout.offsets.clone()),
                ScalarBuffer::from(layout.sizes.clone()),
                values,
                nulls.clone(),
            )?),
            Arc::new(ListArray::try_new(
                leaf,
                OffsetBuffer::from_lengths(lengths),
                canonical_values,
                nulls,
            )?),
        )
    };
    let mut outer = Vec::new();
    for inner in [view, canonical] {
        let field = Arc::new(
            Field::new("inner", inner.data_type().clone(), inner_nullable)
                .with_metadata([("origin".into(), "inner-view-array".into())].into()),
        );
        let nulls = layout.outer_validity.clone().map(NullBuffer::from);
        let array: ArrayRef = if large_outer {
            Arc::new(LargeListArray::try_new(
                field, OffsetBuffer::from_lengths(layout.outer_lengths.clone()), inner, nulls,
            )?)
        } else {
            Arc::new(ListArray::try_new(
                field, OffsetBuffer::from_lengths(layout.outer_lengths.clone()), inner, nulls,
            )?)
        };
        array.to_data().validate_full()?;
        outer.push(array);
    }
    let [view, canonical]: [ArrayRef; 2] = outer.try_into().map_err(|_| "missing input pair")?;
    for row in 0..view.len() {
        assert_eq!(value_at(view.as_ref(), row)?, value_at(canonical.as_ref(), row)?);
    }
    Ok((view, canonical))
}

fn value_at(array: &dyn Array, row: usize) -> TestResult<Value> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    let (field, values) = match array.data_type() {
        DataType::List(field) => (field, array.as_list::<i32>().value(row)),
        DataType::LargeList(field) => (field, array.as_list::<i64>().value(row)),
        DataType::ListView(field) => (field, array.as_list_view::<i32>().value(row)),
        DataType::LargeListView(field) => (field, array.as_list_view::<i64>().value(row)),
        DataType::Int32 => return Ok(json!(array.as_primitive::<arrow::datatypes::Int32Type>().value(row))),
        other => return Err(format!("unexpected inner-view result type: {other}").into()),
    };
    if !field.is_nullable() && values.logical_null_count() != 0 {
        return Err("nonnullable list element contains NULL".into());
    }
    Ok(json!((0..values.len()).map(|i| value_at(values.as_ref(), i)).collect::<TestResult<Vec<_>>>()?))
}

#[derive(Debug, PartialEq)]
struct Observation {
    public: Fields,
    before_collect: Fields,
    executed: Fields,
    rows: Value,
}

async fn observe(session: &SessionContext, table: &str, ansi: Option<bool>) -> TestResult<Observation> {
    let sql = format!("SELECT FLATTEN(items) AS r FROM {table}");
    let public;
    let physical = if let Some(ansi_mode) = ansi {
        let query = plan_query(session, &sql, &SparkSqlOptions {
            ansi_mode, session_timezone: "UTC".into(), ..SparkSqlOptions::default()
        }).await?;
        assert_eq!(query.logical_plan.schema().fields().len(), query.column_names.len());
        public = query.logical_plan.schema().fields().iter().zip(&query.column_names)
            .map(|(field, name)| field.as_ref().clone().with_name(name.clone()))
            .collect::<Vec<_>>().into();
        query.create_physical_plan(session).await?
    } else {
        let frame = session.sql(&sql).await?;
        public = frame.schema().fields().clone();
        frame.create_physical_plan().await?
    };
    let schema = physical.schema();
    let before_collect = schema.fields().clone();
    let batches = collect(physical.clone(), session.task_ctx()).await?;
    let executed = physical.schema().fields().clone();
    let mut rows = Vec::new();
    for batch in batches {
        spark_results::validate_batch(schema.as_ref(), &batch)?;
        for row in 0..batch.num_rows() {
            rows.push(batch.columns().iter().map(|array| value_at(array.as_ref(), row))
                .collect::<TestResult<Vec<_>>>()?);
        }
    }
    Ok(Observation { public, before_collect, executed, rows: json!(rows) })
}

#[tokio::test]
async fn flatten_inner_views_match_canonical_storage() -> TestResult {
    let mut identities = BTreeSet::new();
    let mut differences = Vec::new();
    for layout in layouts() {
        for large_inner in [false, true] {
            for large_outer in [false, true] {
                let (view, canonical) = inputs(&layout, large_inner, large_outer)?;
                for (slice, offset, len) in [
                    ("full", 0, view.len()),
                    ("middle", 1, view.len() - 2),
                    ("empty", 1, 0),
                ] {
                    let expected = json!(&layout.rows.as_array().ok_or("missing expected rows")?[offset..offset + len]);
                    for batch_size in [1, 32] {
                        let config = SessionConfig::new().with_batch_size(batch_size).with_target_partitions(1);
                        let native = SessionContext::new_with_config(config.clone());
                        let configured = SessionContext::new_with_state(configure_spark_sql(
                            SessionStateBuilder::new().with_default_features().with_config(config),
                        ).build());
                        for (table, array) in [("view_input", &view), ("canonical_input", &canonical)] {
                            let schema = Arc::new(Schema::new(vec![Field::new(
                                "items", array.data_type().clone(), layout.outer_validity.is_some(),
                            )]));
                            let batch = RecordBatch::try_new(schema.clone(), vec![array.slice(offset, len)])?;
                            for session in [&native, &configured] {
                                session.register_table(table, Arc::new(MemTable::try_new(
                                    schema.clone(), vec![vec![batch.clone()]],
                                )?))?;
                            }
                            assert_eq!(observe(&native, table, None).await?, observe(&configured, table, None).await?);
                        }
                        for ansi in [true, false] {
                            let control = observe(&configured, "canonical_input", Some(ansi)).await?;
                            assert_eq!(control.rows, expected, "canonical {} {slice}", layout.name);
                            let actual = observe(&configured, "view_input", Some(ansi)).await?;
                            let matches = actual == control;
                            assert!(identities.insert((layout.name, large_inner, large_outer, slice, batch_size, ansi)));
                            eprintln!("FLATTEN_INNER_VIEWS_OBSERVATION {}", json!({
                                "layout":layout.name, "large_inner":large_inner, "large_outer":large_outer,
                                "slice":slice, "batch":batch_size, "ansi":ansi, "matches":matches,
                                "canonical_fields":format!("{:?}",control.public),
                                "actual_fields":format!("{:?}",actual.public), "rows":actual.rows,
                                "expected_rows":expected,
                            }));
                            if !matches {
                                differences.push(format!("{} inner64={large_inner} outer64={large_outer} {slice} batch={batch_size} ANSI={ansi}: expected={control:?}, actual={actual:?}", layout.name));
                            }
                        }
                    }
                }
            }
        }
    }
    assert_eq!(identities.len(), 192);
    assert!(differences.is_empty(), "{}", differences.join("\n"));
    Ok(())
}
