//! Native membership preserves dynamic values and three-valued NULL results.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::{ArrayRef, AsArray, Int32Array, NullArray, StructArray, new_null_array};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{ColumnarValue, Volatility, col, create_udf, lit};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    InListExpr, col as physical_col, lit as physical_lit,
};
use datafusion::prelude::SessionContext;

#[test]
fn volatile_list_items_are_evaluated_at_execution_not_planning() -> Result<()> {
    let session = SessionContext::new();
    let batch = RecordBatch::try_from_iter([
        ("a", Arc::new(Int32Array::from(vec![1, 2, 5])) as ArrayRef),
        ("b", Arc::new(Int32Array::from(vec![99; 3])) as ArrayRef),
    ])?;
    let schema = DFSchema::try_from(batch.schema())?;
    for (array_result, nested) in [(false, false), (false, true), (true, false), (true, true)] {
        for (with_column, negated) in [(false, false), (false, true), (true, false), (true, true)] {
            for position in 0..4 {
                let calls = Arc::new(AtomicUsize::new(0));
                let counter = Arc::clone(&calls);
                let next_value = create_udf(
                    "next_value",
                    if array_result {
                        vec![DataType::Int32]
                    } else {
                        vec![]
                    },
                    DataType::Int32,
                    Volatility::Volatile,
                    Arc::new(move |args| {
                        let value = counter.fetch_add(1, Ordering::SeqCst) + 1;
                        let scalar = ScalarValue::Int32(Some(value as i32));
                        Ok(if array_result {
                            let ColumnarValue::Array(input) = &args[0] else {
                                return exec_err!("expected the registered column");
                            };
                            ColumnarValue::Array(scalar.to_array_of_size(input.len())?)
                        } else {
                            ColumnarValue::Scalar(scalar)
                        })
                    }),
                );
                let mut dynamic =
                    next_value.call(if array_result { vec![col("a")] } else { vec![] });
                if nested {
                    dynamic = dynamic + lit(0_i32);
                }
                let mut list = vec![
                    if with_column { col("b") } else { lit(99_i32) },
                    lit(98_i32),
                    lit(97_i32),
                ];
                list.insert(position, dynamic);
                let plan =
                    session.create_physical_expr(col("a").in_list(list, negated), &schema)?;
                assert_eq!(calls.load(Ordering::SeqCst), 0, "planning: {plan}");
                for invocation in 1..=2 {
                    let result = plan.evaluate(&batch)?.into_array(3)?;
                    let expected = [1, 2, 5].map(|value| Some((value == invocation) != negated));
                    assert_eq!(
                        result.as_boolean().iter().collect::<Vec<_>>(),
                        expected,
                        "invocation {invocation}: {plan}"
                    );
                    assert_eq!(calls.load(Ordering::SeqCst), invocation as usize);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn required_volatile_list_errors_remain_execution_errors() -> Result<()> {
    let session = SessionContext::new();
    let batch =
        RecordBatch::try_from_iter([("a", Arc::new(Int32Array::from(vec![1])) as ArrayRef)])?;
    for negated in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let failure = create_udf(
            "required_failure",
            vec![],
            DataType::Int32,
            Volatility::Volatile,
            Arc::new(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                exec_err!("required list item failed")
            }),
        );
        let plan = session.create_physical_expr(
            col("a").in_list(
                vec![failure.call(vec![]), lit(2_i32), lit(3_i32), lit(4_i32)],
                negated,
            ),
            &DFSchema::try_from(batch.schema())?,
        )?;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let error = plan.evaluate(&batch).unwrap_err();
        assert!(error.to_string().contains("required list item failed"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[test]
fn untyped_null_array_items_keep_unknown_until_a_later_match() -> Result<()> {
    let lookup_values = Arc::new(Int32Array::from(vec![
        Some(99),
        Some(1),
        Some(2),
        None,
        Some(1),
    ])) as ArrayRef;
    let list_values = Arc::new(Int32Array::from(vec![
        Some(99),
        Some(1),
        Some(3),
        Some(4),
        None,
    ])) as ArrayRef;
    let field = Arc::new(Field::new("n", DataType::Int32, true));
    let struct_lookup_values = Arc::new(StructArray::from(vec![(
        field.clone(),
        lookup_values.clone(),
    )])) as ArrayRef;
    let struct_list_values =
        Arc::new(StructArray::from(vec![(field, list_values.clone())])) as ArrayRef;
    for (lookup_values, list_values) in [
        (lookup_values, list_values),
        (struct_lookup_values, struct_list_values),
    ] {
        let data_type = lookup_values.data_type().clone();
        let batch = RecordBatch::try_from_iter_with_nullable([
            ("a", lookup_values.clone(), true),
            ("b", list_values, true),
            (
                "untyped_null",
                Arc::new(NullArray::new(5)) as ArrayRef,
                true,
            ),
            ("typed_null", new_null_array(&data_type, 5), true),
        ])?
        .slice(1, 4);
        for length in [0, 4] {
            let input = batch.slice(0, length);
            let schema = input.schema();
            let lookups = [
                physical_col("a", &schema)?,
                physical_lit(ScalarValue::try_from_array(&lookup_values, 1)?),
                physical_lit(ScalarValue::try_from(&data_type)?),
            ];
            let null_items = [
                physical_col("untyped_null", &schema)?,
                physical_col("typed_null", &schema)?,
                physical_lit(ScalarValue::Null),
                physical_lit(ScalarValue::try_from(&data_type)?),
            ];
            for (lookup_index, lookup) in lookups.iter().enumerate() {
                for null_item in &null_items {
                    for (with_match, null_first) in [(false, true), (true, true), (true, false)] {
                        let mut list = vec![null_item.clone()];
                        if with_match {
                            list.insert(usize::from(null_first), physical_col("b", &schema)?);
                        }
                        for negated in [false, true] {
                            let expression = InListExpr::try_new(
                                lookup.clone(),
                                list.clone(),
                                negated,
                                &schema,
                            )?;
                            assert!(expression.nullable(&schema)?);
                            let result = expression.evaluate(&input)?.into_array(length)?;
                            let expected: Vec<_> = (0..length)
                                .map(|row| {
                                    (with_match && lookup_index != 2 && row == 0)
                                        .then_some(!negated)
                                })
                                .collect();
                            assert_eq!(
                                result.as_boolean().iter().collect::<Vec<_>>(),
                                expected,
                                "{data_type:?} {expression} length={length}"
                            );
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn untyped_null_items_preserve_type_rejection_and_required_errors() -> Result<()> {
    let batch = RecordBatch::try_from_iter_with_nullable([
        ("a", Arc::new(Int32Array::from(vec![1])) as ArrayRef, false),
        ("n", Arc::new(NullArray::new(1)) as ArrayRef, true),
    ])?;
    let schema = batch.schema();
    let incompatible = InListExpr::try_new(
        physical_col("a", &schema)?,
        vec![
            physical_col("n", &schema)?,
            physical_lit(ScalarValue::Utf8(Some("1".into()))),
        ],
        false,
        &schema,
    )
    .unwrap_err();
    assert!(
        incompatible
            .to_string()
            .contains("data type inlist should be same")
    );
    let failure = create_udf(
        "required_failure",
        vec![],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(|_| exec_err!("required item after NULL failed")),
    );
    let required = SessionContext::new()
        .create_physical_expr(failure.call(vec![]), &DFSchema::try_from(schema.clone())?)?;
    for negated in [false, true] {
        let expression = InListExpr::try_new(
            physical_col("a", &schema)?,
            vec![physical_col("n", &schema)?, required.clone()],
            negated,
            &schema,
        )?;
        let error = expression.evaluate(&batch).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("required item after NULL failed")
        );
    }
    Ok(())
}
