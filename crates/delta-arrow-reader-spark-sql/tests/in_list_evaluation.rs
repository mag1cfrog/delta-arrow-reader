//! IN-list planning preserves volatile operands for each execution.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::{ArrayRef, AsArray, Int32Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err};
use datafusion::logical_expr::{ColumnarValue, Volatility, col, create_udf, lit};
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
