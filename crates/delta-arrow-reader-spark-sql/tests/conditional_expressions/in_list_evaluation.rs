//! Native membership preserves NULL results and evaluates only needed operands.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use arrow::array::{
    ArrayRef, AsArray, BooleanArray, DictionaryArray, Int8Array, Int32Array, NullArray,
    StructArray, new_null_array,
};
use arrow::datatypes::{DataType, Field, Int32Type};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion::logical_expr::{ColumnarValue, Expr, Volatility, col, create_udf, lit};
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

fn record_operand_rows(
    name: &str,
    value: Expr,
    data_type: &DataType,
    visits: Arc<Mutex<Vec<Vec<i32>>>>,
) -> Expr {
    create_udf(
        name,
        vec![DataType::Int32, data_type.clone()],
        data_type.clone(),
        Volatility::Volatile,
        Arc::new(move |arguments| {
            let arrays = ColumnarValue::values_to_arrays(arguments)?;
            visits
                .lock()
                .map_err(|_| exec_datafusion_err!("poisoned test recorder"))?
                .push(arrays[0].as_primitive::<Int32Type>().values().to_vec());
            Ok(arguments[1].clone())
        }),
    )
    .call(vec![col("id"), value])
}

#[test]
fn native_list_operands_visit_only_unresolved_rows() -> Result<()> {
    // A NULL comparison can carry a true value bit that must not resolve a row.
    let null_comparison =
        arrow::compute::kernels::cmp::eq(&Int32Array::from(vec![0]), &Int32Array::new_null(1))?;
    assert_eq!(null_comparison.iter().next(), Some(None));
    assert!(null_comparison.values().value(0));

    let columns = [
        (
            "a",
            [
                Some(1),
                Some(2),
                None,
                Some(4),
                Some(5),
                Some(6),
                None,
                Some(8),
                Some(10),
            ],
        ),
        (
            "b",
            [
                Some(1),
                Some(0),
                Some(7),
                None,
                Some(0),
                Some(6),
                Some(0),
                None,
                Some(0),
            ],
        ),
        (
            "c",
            [
                Some(9),
                Some(2),
                Some(7),
                Some(4),
                None,
                Some(0),
                Some(0),
                None,
                Some(0),
            ],
        ),
        (
            "d",
            [
                Some(9),
                Some(9),
                Some(7),
                Some(9),
                Some(5),
                Some(9),
                Some(0),
                Some(0),
                Some(0),
            ],
        ),
    ];
    let cases = [
        (
            col("a"),
            vec![
                Some(true),
                Some(true),
                None,
                Some(true),
                Some(true),
                Some(true),
                None,
                None,
                Some(false),
            ],
            vec![
                vec![0, 1, 3, 4, 5, 7, 8],
                vec![1, 3, 4, 7, 8],
                vec![4, 7, 8],
                vec![7, 8],
            ],
        ),
        (
            col("b"),
            vec![
                Some(true),
                Some(true),
                Some(true),
                None,
                Some(true),
                Some(true),
                Some(true),
                None,
                Some(true),
            ],
            vec![vec![0, 1, 2, 4, 5, 6, 8], vec![], vec![], vec![]],
        ),
        (
            lit(1_i32),
            vec![
                Some(true),
                Some(false),
                Some(false),
                None,
                None,
                Some(false),
                Some(false),
                None,
                Some(false),
            ],
            vec![
                (0..9).collect(),
                (1..9).collect(),
                (1..9).collect(),
                (1..9).collect(),
            ],
        ),
        (
            lit(0_i32),
            vec![
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(true),
            ],
            vec![
                (0..9).collect(),
                vec![0, 2, 3, 5, 7],
                vec![0, 2, 3, 7],
                vec![0, 2, 3],
            ],
        ),
        (
            lit(99_i32),
            vec![
                Some(false),
                Some(false),
                Some(false),
                None,
                None,
                Some(false),
                Some(false),
                None,
                Some(false),
            ],
            vec![(0..9).collect(); 4],
        ),
        (
            lit(ScalarValue::Int32(None)),
            vec![None; 9],
            vec![vec![]; 4],
        ),
    ];
    for offset in [1, 63] {
        let mut arrays = vec![(
            "id",
            Arc::new(Int32Array::from_iter_values(0..(offset + 9) as i32)) as ArrayRef,
        )];
        arrays.extend(columns.iter().map(|(name, values)| {
            (
                *name,
                Arc::new(Int32Array::from_iter(
                    std::iter::repeat_n(None, offset).chain(*values),
                )) as ArrayRef,
            )
        }));
        let input = RecordBatch::try_from_iter(arrays)?.slice(offset, 9);
        for (lookup, expected, required_ids) in &cases {
            for (length, negated) in [(0, false), (0, true), (9, false), (9, true)] {
                let batch = input.slice(0, length);
                let visits: [_; 5] = std::array::from_fn(|_| Arc::new(Mutex::new(Vec::new())));
                let mut operands = [lookup.clone(), col("b"), col("c"), col("d"), lit(9_i32)]
                    .into_iter()
                    .enumerate()
                    .map(|(i, value)| {
                        record_operand_rows(
                            &format!("operand_{i}"),
                            value,
                            &DataType::Int32,
                            visits[i].clone(),
                        )
                    });
                let lookup = operands.next().expect("lookup operand");
                let plan = SessionContext::new().create_physical_expr(
                    lookup.in_list(operands.collect(), negated),
                    &DFSchema::try_from(batch.schema())?,
                )?;
                assert!(
                    plan.is::<InListExpr>(),
                    "expected native membership: {plan}"
                );
                for observed in &visits {
                    assert!(
                        observed
                            .lock()
                            .map_err(|_| exec_datafusion_err!("poisoned test recorder"))?
                            .is_empty()
                    );
                }
                let result = plan.evaluate(&batch)?.into_array(length)?;
                let expected_rows: Vec<_> = expected
                    .iter()
                    .take(length)
                    .map(|value| value.map(|value| value != negated))
                    .collect();
                assert_eq!(
                    result.as_boolean().iter().collect::<Vec<_>>(),
                    expected_rows,
                    "{plan}"
                );
                let all_ids = (0..9).collect::<Vec<_>>();
                for (i, ids) in std::iter::once(&all_ids).chain(required_ids).enumerate() {
                    let expected_visits = if length == 0 || ids.is_empty() {
                        vec![]
                    } else {
                        vec![ids.iter().map(|id| id + offset as i32).collect::<Vec<_>>()]
                    };
                    assert_eq!(
                        *visits[i]
                            .lock()
                            .map_err(|_| exec_datafusion_err!("poisoned test recorder"))?,
                        expected_visits,
                        "operand {i}: {plan}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn logical_null_lookup_rows_never_require_a_list_item() -> Result<()> {
    let dictionary = Arc::new(DictionaryArray::try_new(
        Int8Array::from(vec![0, 1, 2, 1, 0]),
        Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
    )?) as ArrayRef;
    for (values, expected) in [
        (
            dictionary,
            vec![Some(true), None, Some(true), None, Some(true)],
        ),
        (Arc::new(NullArray::new(5)) as ArrayRef, vec![None; 5]),
    ] {
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from_iter_values(0..5)) as ArrayRef,
            ),
            ("value", values),
        ])?;
        for negated in [false, true] {
            let failure = create_udf(
                "unneeded_failure",
                vec![],
                batch.column(1).data_type().clone(),
                Volatility::Volatile,
                Arc::new(|_| exec_err!("NULL lookup or prior match must skip this item")),
            );
            let plan = SessionContext::new().create_physical_expr(
                col("value").in_list(vec![col("value"), failure.call(vec![])], negated),
                &DFSchema::try_from(batch.schema())?,
            )?;
            assert!(plan.is::<InListExpr>());
            let result = plan.evaluate(&batch)?.into_array(5)?;
            assert_eq!(
                result.as_boolean().iter().collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|value| value.map(|value| value != negated))
                    .collect::<Vec<_>>()
            );
        }
    }
    Ok(())
}

#[test]
fn selected_boolean_scalar_items_keep_unselected_results() -> Result<()> {
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from_iter_values(0..4)) as ArrayRef,
        ),
        (
            "value",
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])) as ArrayRef,
        ),
    ])?;
    for negated in [false, true] {
        let visits: [_; 2] = std::array::from_fn(|_| Arc::new(Mutex::new(Vec::new())));
        let list = [true, false]
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
                record_operand_rows(
                    &format!("boolean_{i}"),
                    lit(value),
                    &DataType::Boolean,
                    visits[i].clone(),
                )
            })
            .collect();
        let plan = SessionContext::new().create_physical_expr(
            col("value").in_list(list, negated),
            &DFSchema::try_from(batch.schema())?,
        )?;
        assert!(plan.is::<InListExpr>());
        let result = plan.evaluate(&batch)?.into_array(4)?;
        assert_eq!(
            result.as_boolean().iter().collect::<Vec<_>>(),
            vec![Some(!negated), Some(!negated), None, Some(!negated)]
        );
        for (i, expected) in [vec![vec![0, 1, 3]], vec![vec![1]]].into_iter().enumerate() {
            assert_eq!(
                *visits[i]
                    .lock()
                    .map_err(|_| exec_datafusion_err!("poisoned test recorder"))?,
                expected
            );
        }
    }
    Ok(())
}

#[test]
fn native_empty_lists_keep_false_for_null_and_nonnull_lookups() -> Result<()> {
    let batch = RecordBatch::try_from_iter([(
        "value",
        Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
    )])?;
    for negated in [false, true] {
        let plan = SessionContext::new().create_physical_expr(
            col("value").in_list(vec![], negated),
            &DFSchema::try_from(batch.schema())?,
        )?;
        assert!(plan.is::<InListExpr>());
        let result = plan.evaluate(&batch)?.into_array(2)?;
        assert_eq!(
            result.as_boolean().iter().collect::<Vec<_>>(),
            vec![Some(negated); 2]
        );
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
