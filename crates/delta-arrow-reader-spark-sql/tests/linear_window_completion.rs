//! Stateful completion with the same global-row guarantee as LinearSearch.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Int32Array, Int64Array, ListArray, StructArray, UInt32Array,
};
use arrow::compute::{SortOptions, take_record_batch};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::max_udaf;
use datafusion::functions_window::nth_value::{first_value_udwf, last_value_udwf, nth_value_udwf};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::window_state::{
    PartitionBatchState, WindowFrameContext, range_bound_target,
};
use datafusion::logical_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::physical_expr::expressions::{Literal, col};
use datafusion::physical_expr::window::{PartitionBatches, PartitionWindowAggStates, WindowExpr};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::physical_plan::windows::{
    WindowAggExec, create_window_expr, get_best_fitting_window,
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn source_batch(
    rows: &[(i32, i32, i32)],
    nested: bool,
    descending: bool,
) -> TestResult<RecordBatch> {
    let keys = rows
        .iter()
        .map(|(_, _, key)| if descending { -*key } else { *key });
    let keys: ArrayRef = if nested {
        let values: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            keys.map(|key| Some(vec![Some(key), None])),
        ));
        Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("values", values.data_type().clone(), true)),
            values,
        )]))
    } else {
        Arc::new(Int32Array::from_iter_values(keys))
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from_iter_values(
            rows.iter().map(|(id, _, _)| *id),
        )),
        Arc::new(Int32Array::from_iter_values(
            rows.iter().map(|(_, partition, _)| *partition),
        )),
        keys,
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", columns[0].data_type().clone(), false),
        Field::new("p", columns[1].data_type().clone(), false),
        Field::new("k", columns[2].data_type().clone(), false),
    ]));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn count_window(
    schema: SchemaRef,
    units: WindowFrameUnits,
    end: WindowFrameBound,
    options: SortOptions,
) -> TestResult<Arc<dyn WindowExpr>> {
    Ok(create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "n".into(),
        &[col("id", &schema)?],
        &[col("p", &schema)?],
        &[PhysicalSortExpr {
            expr: col("k", &schema)?,
            options,
        }],
        Arc::new(WindowFrame::new_bounds(
            units,
            WindowFrameBound::CurrentRow,
            end,
        )),
        schema,
        false,
        false,
        None,
    )?)
}

fn date_source_batch(
    rows: &[(i32, i32, Option<i32>)],
    descending: bool,
) -> TestResult<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.0))),
        Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.1))),
        Arc::new(Date32Array::from(
            rows.iter()
                .map(|row| row.2.map(|key| if descending { -key } else { key }))
                .collect::<Vec<_>>(),
        )),
    ];
    let schema = Arc::new(Schema::new(
        ["id", "p", "k"]
            .into_iter()
            .zip(&columns)
            .map(|(name, column)| Field::new(name, column.data_type().clone(), name == "k"))
            .collect::<Vec<_>>(),
    ));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn append_linear_batch(
    window: &dyn WindowExpr,
    buffers: &mut PartitionBatches,
    states: &mut PartitionWindowAggStates,
    batch: RecordBatch,
) -> TestResult {
    let partitions = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or("partition type")?;
    for partition in [0, 1] {
        let indices = UInt32Array::from_iter_values(
            (0..batch.num_rows())
                .filter(|&row| partitions.value(row) == partition)
                .map(|row| row as u32),
        );
        if indices.is_empty() {
            continue;
        }
        let partition_batch = take_record_batch(&batch, &indices)?;
        let key = vec![ScalarValue::Int32(Some(partition))];
        if let Some(buffer) = buffers.get_mut(&key) {
            buffer.extend(&partition_batch)?;
        } else {
            buffers.insert(key, PartitionBatchState::new_with_batch(partition_batch));
        }
    }
    // LinearSearch shares the last globally ordered source row with every open partition.
    for buffer in buffers.values_mut() {
        assert!(!buffer.is_end);
        buffer.set_most_recent_row(batch.slice(batch.num_rows() - 1, 1));
    }
    window.evaluate_stateful(buffers, states)?;
    Ok(())
}

fn counts(states: &PartitionWindowAggStates, partition: i32) -> TestResult<Vec<i64>> {
    let state = &states
        .get(&vec![ScalarValue::Int32(Some(partition))])
        .ok_or("missing state")?
        .state;
    let values = state
        .out_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("count type")?;
    assert!(!state.is_end);
    Ok(values.values().to_vec())
}

fn prune_partition(
    buffers: &mut PartitionBatches,
    states: &mut PartitionWindowAggStates,
    partition: i32,
    n_prune: usize,
) -> TestResult {
    let key = vec![ScalarValue::Int32(Some(partition))];
    let state = &mut states.get_mut(&key).ok_or("missing state")?.state;
    assert_eq!(state.window_frame_range.start, n_prune);
    assert!(state.last_calculated_index >= n_prune);
    let buffer = buffers.get_mut(&key).ok_or("missing buffer")?;
    buffer.record_batch = buffer
        .record_batch
        .slice(n_prune, buffer.record_batch.num_rows() - n_prune);
    state.prune_state(n_prune);
    Ok(())
}

#[test]
fn date_range_offsets_decline_reversal_of_either_bound() -> TestResult {
    for data_type in [DataType::Date32, DataType::Int32] {
        let schema = Arc::new(Schema::new(vec![Field::new("k", data_type.clone(), false)]));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("k", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        }])
        .ok_or("empty ordering")?;
        let input: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
            schema.clone(),
            vec![],
            None,
            Some(ordering),
            false,
            None,
        )?);
        for offset in [0, 1] {
            for (start, end) in [
                (
                    WindowFrameBound::Preceding(ScalarValue::Int32(Some(offset))),
                    WindowFrameBound::CurrentRow,
                ),
                (
                    WindowFrameBound::CurrentRow,
                    WindowFrameBound::Following(ScalarValue::Int32(Some(offset))),
                ),
            ] {
                let window = create_window_expr(
                    &WindowFunctionDefinition::AggregateUDF(count_udaf()),
                    "n".into(),
                    &[col("k", &schema)?],
                    &[],
                    &[PhysicalSortExpr {
                        expr: col("k", &schema)?,
                        options: SortOptions::default(),
                    }],
                    Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Range, start, end)),
                    schema.clone(),
                    false,
                    false,
                    None,
                )?;
                let optimized = get_best_fitting_window(&[window], &input, &[])?;
                assert_eq!(
                    optimized.is_some(),
                    data_type == DataType::Int32 || offset == 0
                );
            }
        }
    }
    Ok(())
}

#[test]
fn date_range_wrap_does_not_infer_aggregate_output_ordering() -> TestResult {
    for data_type in [DataType::Date32, DataType::Int32] {
        let schema = Arc::new(Schema::new(vec![Field::new("k", data_type.clone(), false)]));
        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let ordering = LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("k", &schema)?,
            options,
        }])
        .ok_or("empty ordering")?;
        let input: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
            schema.clone(),
            vec![],
            None,
            Some(ordering),
            false,
            None,
        )?);
        let window = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(max_udaf()),
            "mx".into(),
            &[col("k", &schema)?],
            &[],
            &[PhysicalSortExpr {
                expr: col("k", &schema)?,
                options,
            }],
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::Preceding(ScalarValue::Int32(Some(i32::MAX))),
                WindowFrameBound::Preceding(ScalarValue::Int32(Some(i32::MAX - 1))),
            )),
            schema,
            false,
            false,
            None,
        )?;
        let plan = WindowAggExec::try_new(vec![window], input, false)?;
        let inferred = plan
            .properties()
            .equivalence_properties()
            .ordering_satisfy([PhysicalSortExpr {
                expr: col("mx", &plan.schema())?,
                options,
            }])?;
        assert_eq!(inferred, data_type == DataType::Int32);
    }
    Ok(())
}

#[test]
fn date_preceding_wrap_waits_for_builtin_window_values() -> TestResult {
    for (function, nth) in [(first_value_udwf(), false), (nth_value_udwf(), true)] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let first = date_source_batch(&[(0, 0, Some(-3)), (1, 1, Some(-2))], descending)?;
                let schema = first.schema();
                let mut arguments = vec![col("id", &schema)?];
                if nth {
                    arguments.push(Arc::new(Literal::new(ScalarValue::Int64(Some(2)))));
                }
                let window = create_window_expr(
                    &WindowFunctionDefinition::WindowUDF(function.clone()),
                    "value".into(),
                    &arguments,
                    &[col("p", &schema)?],
                    &[PhysicalSortExpr {
                        expr: col("k", &schema)?,
                        options: SortOptions {
                            descending,
                            nulls_first,
                        },
                    }],
                    Arc::new(WindowFrame::new_bounds(
                        WindowFrameUnits::Range,
                        WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                        WindowFrameBound::Preceding(ScalarValue::Int32(Some(i32::MAX))),
                    )),
                    schema,
                    false,
                    false,
                    None,
                )?;
                let mut buffers = PartitionBatches::default();
                let mut states = PartitionWindowAggStates::default();
                append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
                for state in states.values() {
                    assert!(state.state.out_col.is_empty());
                }
                append_linear_batch(
                    window.as_ref(),
                    &mut buffers,
                    &mut states,
                    date_source_batch(
                        &[(2, 0, Some(i32::MAX - 1)), (3, 1, Some(i32::MAX))],
                        descending,
                    )?,
                )?;
                for buffer in buffers.values_mut() {
                    buffer.is_end = true;
                }
                window.evaluate_stateful(&buffers, &mut states)?;
                for partition in [0, 1] {
                    let state = &states[&vec![ScalarValue::Int32(Some(partition))]].state;
                    let values = state
                        .out_col
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or("value type")?;
                    let expected = if nth { partition + 2 } else { partition };
                    assert_eq!(values.null_count(), 0);
                    assert_eq!(values.values().as_ref(), &[expected, expected]);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn date_range_day_bounds_complete_and_prune_sparse_partitions() -> TestResult {
    for end in [
        WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
        WindowFrameBound::Preceding(ScalarValue::Int32(Some(-1))),
    ] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let first = date_source_batch(
                    &[
                        (0, 0, Some(1)),
                        (1, 1, Some(1)),
                        (2, 0, Some(2)),
                        (3, 1, Some(3)),
                    ],
                    descending,
                )?;
                let window = count_window(
                    first.schema(),
                    WindowFrameUnits::Range,
                    end.clone(),
                    SortOptions {
                        descending,
                        nulls_first,
                    },
                )?;
                let mut buffers = PartitionBatches::default();
                let mut states = PartitionWindowAggStates::default();
                append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
                assert_eq!(counts(&states, 0)?, [2]);
                assert_eq!(counts(&states, 1)?, [1]);
                append_linear_batch(
                    window.as_ref(),
                    &mut buffers,
                    &mut states,
                    date_source_batch(&[(4, 0, Some(3)), (5, 1, Some(4))], descending)?,
                )?;
                assert_eq!(counts(&states, 0)?, [2, 2]);
                assert_eq!(counts(&states, 1)?, [1]);
                prune_partition(&mut buffers, &mut states, 0, 1)?;
                append_linear_batch(
                    window.as_ref(),
                    &mut buffers,
                    &mut states,
                    date_source_batch(&[(6, 0, Some(4)), (7, 1, Some(5))], descending)?,
                )?;
                assert_eq!(counts(&states, 0)?, [2, 2, 2]);
                assert_eq!(counts(&states, 1)?, [1, 2]);
                assert_eq!(
                    states[&vec![ScalarValue::Int32(Some(0))]]
                        .state
                        .offset_pruned_rows,
                    1
                );
            }
        }
    }
    Ok(())
}

#[test]
fn date_range_completion_uses_the_latest_rows_null_order() -> TestResult {
    for offset in [0, 1] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let keys = if nulls_first {
                    [None, Some(1)]
                } else {
                    [Some(1), None]
                };
                let first = date_source_batch(&[(0, 0, keys[0]), (1, 1, keys[1])], descending)?;
                let window = count_window(
                    first.schema(),
                    WindowFrameUnits::Range,
                    WindowFrameBound::Following(ScalarValue::Int32(Some(offset))),
                    SortOptions {
                        descending,
                        nulls_first,
                    },
                )?;
                let mut buffers = PartitionBatches::default();
                let mut states = PartitionWindowAggStates::default();
                append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
                assert_eq!(counts(&states, 0)?, [1]);
                assert!(counts(&states, 1)?.is_empty());
            }
        }
    }
    Ok(())
}

#[test]
fn nested_current_row_completes_sparse_partitions_before_input_ends() -> TestResult {
    for units in [WindowFrameUnits::Range, WindowFrameUnits::Groups] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let first = source_batch(&[(0, 0, 1), (1, 1, 1)], true, descending)?;
                let window = count_window(
                    first.schema(),
                    units,
                    WindowFrameBound::CurrentRow,
                    SortOptions {
                        descending,
                        nulls_first,
                    },
                )?;
                let mut buffers = PartitionBatches::default();
                let mut states = PartitionWindowAggStates::default();
                append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
                assert_eq!(counts(&states, 0)?, Vec::<i64>::new());
                assert_eq!(counts(&states, 1)?, Vec::<i64>::new());

                append_linear_batch(
                    window.as_ref(),
                    &mut buffers,
                    &mut states,
                    source_batch(&[(2, 0, 1), (3, 1, 2)], true, descending)?,
                )?;
                assert_eq!(counts(&states, 0)?, [2, 2]);
                assert_eq!(counts(&states, 1)?, [1]);

                append_linear_batch(
                    window.as_ref(),
                    &mut buffers,
                    &mut states,
                    source_batch(&[(4, 0, 3), (5, 1, 3)], true, descending)?,
                )?;
                assert_eq!(counts(&states, 0)?, [2, 2]);
                assert_eq!(counts(&states, 1)?, [1, 1]);
                prune_partition(&mut buffers, &mut states, 1, 1)?;

                append_linear_batch(
                    window.as_ref(),
                    &mut buffers,
                    &mut states,
                    source_batch(&[(6, 0, 3), (7, 1, 4)], true, descending)?,
                )?;
                assert_eq!(counts(&states, 0)?, [2, 2, 2, 2]);
                assert_eq!(counts(&states, 1)?, [1, 1, 1]);
                prune_partition(&mut buffers, &mut states, 0, 2)?;
                prune_partition(&mut buffers, &mut states, 1, 1)?;
                for partition in [0, 1] {
                    let key = vec![ScalarValue::Int32(Some(partition))];
                    assert_eq!(states[&key].state.offset_pruned_rows, 2);
                    assert_eq!(buffers[&key].record_batch.num_rows(), 2);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn following_completion_changes_with_row_and_group_position() -> TestResult {
    for units in [WindowFrameUnits::Range, WindowFrameUnits::Groups] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let nested = units == WindowFrameUnits::Groups;
                let first = source_batch(
                    &[(0, 0, 1), (1, 1, 1), (2, 0, 2), (3, 1, 3)],
                    nested,
                    descending,
                )?;
                let delta = if nested {
                    ScalarValue::UInt64(Some(1))
                } else {
                    ScalarValue::Int32(Some(1))
                };
                let window = count_window(
                    first.schema(),
                    units,
                    WindowFrameBound::Following(delta),
                    SortOptions {
                        descending,
                        nulls_first,
                    },
                )?;
                let mut buffers = PartitionBatches::default();
                let mut states = PartitionWindowAggStates::default();
                append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
                // The first tail range is complete; the next still needs a future row/group.
                assert_eq!(counts(&states, 0)?, [2]);
                let expected_other = if nested { vec![] } else { vec![1] };
                assert_eq!(counts(&states, 1)?, expected_other);

                append_linear_batch(
                    window.as_ref(),
                    &mut buffers,
                    &mut states,
                    source_batch(&[(4, 0, 3), (5, 1, 4)], nested, descending)?,
                )?;
                assert_eq!(counts(&states, 0)?, [2, 2]);
                prune_partition(&mut buffers, &mut states, 0, 1)?;
                let key = vec![ScalarValue::Int32(Some(0))];
                assert_eq!(states[&key].state.offset_pruned_rows, 1);
                assert_eq!(states[&key].state.last_calculated_index, 1);
                assert_eq!(buffers[&key].record_batch.num_rows(), 2);
            }
        }
    }
    Ok(())
}

fn range_count_window(
    schema: SchemaRef,
    start: WindowFrameBound,
    end: WindowFrameBound,
    options: SortOptions,
) -> TestResult<Arc<dyn WindowExpr>> {
    Ok(create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "n".into(),
        &[col("id", &schema)?],
        &[col("p", &schema)?],
        &[PhysicalSortExpr {
            expr: col("k", &schema)?,
            options,
        }],
        Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Range, start, end)),
        schema,
        false,
        false,
        None,
    )?)
}

#[test]
fn null_range_targets_preserve_native_unbounded_and_arithmetic_edges() -> TestResult {
    for data_type in [
        DataType::UInt64,
        DataType::Int32,
        DataType::Float64,
        DataType::Date32,
    ] {
        let null = ScalarValue::try_from(&data_type)?;
        for bound in [
            WindowFrameBound::Preceding(null.clone()),
            WindowFrameBound::Following(null.clone()),
        ] {
            assert!(bound.is_unbounded());
            assert!(!bound.is_null_offset());
            assert_eq!(range_bound_target(&null, &bound, false)?, None);
            assert!(bound.to_string().starts_with("UNBOUNDED "));
        }
    }
    for bound in [
        WindowFrameBound::PrecedingNullOffset(DataType::Float64),
        WindowFrameBound::FollowingNullOffset(DataType::Float64),
    ] {
        assert!(!bound.is_unbounded());
        assert!(bound.is_null_offset());
        assert!(bound.to_string().starts_with("NULL "));
        assert_eq!(
            range_bound_target(&ScalarValue::Date32(Some(1)), &bound, false)?,
            Some(ScalarValue::Date32(None))
        );
        let frame = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            bound,
            WindowFrameBound::Preceding(ScalarValue::Int32(Some(1))),
        );
        assert!(frame.has_null_offset());
        assert!(!frame.is_causal());
        assert!(!frame.is_ever_expanding());
        assert!(!frame.can_accept_multi_orderby());
        assert_eq!(frame.reverse().reverse(), frame);
        assert!(frame.regularize_order_bys(&mut vec![]).is_err());
    }
    for bound in [
        WindowFrameBound::CurrentRow,
        WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
    ] {
        assert_eq!(
            range_bound_target(&ScalarValue::Int32(None), &bound, false)?,
            Some(ScalarValue::Int32(None))
        );
    }
    assert_eq!(
        range_bound_target(
            &ScalarValue::UInt32(Some(1)),
            &WindowFrameBound::Preceding(ScalarValue::UInt32(Some(2))),
            false,
        )?,
        Some(ScalarValue::UInt32(Some(0)))
    );
    let following_one = WindowFrameBound::Following(ScalarValue::Int32(Some(1)));
    assert_eq!(
        range_bound_target(&ScalarValue::Int32(Some(i32::MAX)), &following_one, false)?,
        None
    );
    assert_eq!(
        range_bound_target(&ScalarValue::Date32(Some(i32::MAX)), &following_one, false)?,
        Some(ScalarValue::Date32(Some(i32::MIN)))
    );
    let native = WindowFrame::new_bounds(
        WindowFrameUnits::Range,
        WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
        WindowFrameBound::CurrentRow,
    );
    assert_eq!(native, WindowFrame::new(Some(false)));
    assert!(!native.has_null_offset());
    assert!(native.is_ever_expanding());
    assert!(native.can_accept_multi_orderby());
    Ok(())
}

#[test]
fn null_range_cursors_stop_at_consumed_upper_rows() -> TestResult {
    for date in [false, true] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let keys = if nulls_first {
                    vec![None, Some(1), Some(1), Some(2), Some(3)]
                } else {
                    vec![Some(1), Some(1), Some(2), Some(3), None]
                };
                let keys = keys
                    .into_iter()
                    .map(|key| key.map(|key| if descending { -key } else { key }))
                    .collect::<Vec<_>>();
                let column: ArrayRef = if date {
                    Arc::new(Date32Array::from(keys))
                } else {
                    Arc::new(Int32Array::from(keys))
                };
                for null_bound in [
                    WindowFrameBound::PrecedingNullOffset(DataType::Int32),
                    WindowFrameBound::FollowingNullOffset(DataType::Int32),
                ] {
                    let cases = if nulls_first {
                        vec![
                            (
                                WindowFrameBound::CurrentRow,
                                null_bound.clone(),
                                vec![0..1, 1..1, 1..1, 1..1, 1..1],
                            ),
                            (
                                null_bound.clone(),
                                WindowFrameBound::CurrentRow,
                                vec![0..1, 0..3, 0..3, 0..4, 0..5],
                            ),
                            (null_bound.clone(), null_bound.clone(), vec![0..1; 5]),
                        ]
                    } else {
                        vec![
                            (
                                WindowFrameBound::CurrentRow,
                                null_bound.clone(),
                                vec![0..5, 0..5, 2..5, 3..5, 4..5],
                            ),
                            (
                                null_bound.clone(),
                                WindowFrameBound::CurrentRow,
                                vec![2..2, 2..2, 3..3, 4..4, 4..5],
                            ),
                            (null_bound.clone(), null_bound.clone(), vec![4..5; 5]),
                        ]
                    };
                    for (start, end, expected) in cases {
                        let frame =
                            Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Range, start, end));
                        let mut context = WindowFrameContext::new(
                            frame,
                            vec![SortOptions {
                                descending,
                                nulls_first,
                            }],
                        );
                        let mut last = 0..0;
                        for (idx, expected) in expected.into_iter().enumerate() {
                            let range = context.calculate_range(
                                std::slice::from_ref(&column),
                                &last,
                                column.len(),
                                idx,
                            )?;
                            assert_eq!(range, expected);
                            assert!(last.start <= range.start && last.end <= range.end);
                            assert!(range.start <= range.end && range.end <= column.len());
                            last = range;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn null_range_native_constructor_rejects_invalid_units_sides_and_keys() -> TestResult {
    let schema = date_source_batch(&[(0, 0, None)], false)?.schema();
    let order_by = PhysicalSortExpr {
        expr: col("k", &schema)?,
        options: SortOptions::default(),
    };
    let null = WindowFrameBound::FollowingNullOffset(DataType::Int32);
    let frames = [
        (
            WindowFrameUnits::Rows,
            WindowFrameBound::CurrentRow,
            null.clone(),
        ),
        (
            WindowFrameUnits::Groups,
            null.clone(),
            WindowFrameBound::CurrentRow,
        ),
        (
            WindowFrameUnits::Range,
            WindowFrameBound::Following(ScalarValue::Int32(None)),
            null.clone(),
        ),
        (
            WindowFrameUnits::Range,
            null.clone(),
            WindowFrameBound::Preceding(ScalarValue::Date32(None)),
        ),
    ];
    for (function, takes_argument) in [
        (WindowFunctionDefinition::AggregateUDF(count_udaf()), true),
        (
            WindowFunctionDefinition::WindowUDF(first_value_udwf()),
            true,
        ),
        (
            WindowFunctionDefinition::WindowUDF(row_number_udwf()),
            false,
        ),
    ] {
        let args = if takes_argument {
            vec![col("id", &schema)?]
        } else {
            vec![]
        };
        for (units, start, end) in &frames {
            let frame = Arc::new(WindowFrame::new_bounds(*units, start.clone(), end.clone()));
            assert!(
                create_window_expr(
                    &function,
                    "value".into(),
                    &args,
                    &[],
                    std::slice::from_ref(&order_by),
                    frame.clone(),
                    schema.clone(),
                    false,
                    false,
                    None
                )
                .is_err()
            );
            if *units != WindowFrameUnits::Range {
                let mut context = WindowFrameContext::new(frame, vec![SortOptions::default()]);
                let column: ArrayRef = Arc::new(Date32Array::from(vec![None]));
                assert!(context.calculate_range(&[column], &(0..0), 1, 0).is_err());
            }
        }
        for keys in [vec![], vec![order_by.clone(), order_by.clone()]] {
            let frame = Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::CurrentRow,
                null.clone(),
            ));
            assert!(
                create_window_expr(
                    &function,
                    "value".into(),
                    &args,
                    &[],
                    &keys,
                    frame,
                    schema.clone(),
                    false,
                    false,
                    None
                )
                .is_err()
            );
        }
        let frame = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            null.clone(),
        ));
        let window = create_window_expr(
            &function,
            "value".into(),
            &args,
            &[],
            std::slice::from_ref(&order_by),
            frame,
            schema.clone(),
            false,
            false,
            None,
        )?;
        assert_eq!(window.uses_bounded_memory(), !takes_argument);
    }
    Ok(())
}

#[test]
fn null_upper_completes_sparse_partitions_without_claiming_bounded_retention() -> TestResult {
    for descending in [false, true] {
        for end in [
            WindowFrameBound::PrecedingNullOffset(DataType::Int32),
            WindowFrameBound::FollowingNullOffset(DataType::Int32),
        ] {
            let first = date_source_batch(&[(0, 0, None), (1, 1, None)], descending)?;
            let window = range_count_window(
                first.schema(),
                WindowFrameBound::CurrentRow,
                end,
                SortOptions {
                    descending,
                    nulls_first: true,
                },
            )?;
            assert!(!window.uses_bounded_memory());
            let mut buffers = PartitionBatches::default();
            let mut states = PartitionWindowAggStates::default();
            append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
            assert!(counts(&states, 0)?.is_empty());
            assert!(counts(&states, 1)?.is_empty());
            append_linear_batch(
                window.as_ref(),
                &mut buffers,
                &mut states,
                date_source_batch(&[(2, 1, Some(1))], descending)?,
            )?;
            assert_eq!(counts(&states, 0)?, [1]);
            assert_eq!(counts(&states, 1)?, [1, 0]);
            append_linear_batch(
                window.as_ref(),
                &mut buffers,
                &mut states,
                date_source_batch(&[(3, 0, Some(2)), (4, 1, Some(3))], descending)?,
            )?;
            assert_eq!(counts(&states, 0)?, [1, 0]);
            prune_partition(&mut buffers, &mut states, 0, 1)?;
            let key = vec![ScalarValue::Int32(Some(0))];
            assert_eq!(states[&key].state.window_frame_range, 0..0);
            assert_eq!(buffers[&key].record_batch.num_rows(), 1);
            append_linear_batch(
                window.as_ref(),
                &mut buffers,
                &mut states,
                date_source_batch(&[(5, 0, Some(4)), (6, 1, Some(5))], descending)?,
            )?;
            assert_eq!(counts(&states, 0)?, [1, 0, 0]);
            assert_eq!(counts(&states, 1)?, [1, 0, 0, 0]);
            assert_eq!(states[&key].state.window_frame_range, 0..0);
            assert_eq!(states[&key].state.offset_pruned_rows, 1);
            assert_eq!(states[&key].state.last_calculated_index, 2);
            assert_eq!(buffers[&key].record_batch.num_rows(), 2);
        }
    }
    Ok(())
}

#[test]
fn null_upper_last_and_true_unbounded_upper_wait_for_future_rows() -> TestResult {
    for (nulls_first, end) in [
        (
            false,
            WindowFrameBound::FollowingNullOffset(DataType::Int32),
        ),
        (true, WindowFrameBound::Following(ScalarValue::Int32(None))),
    ] {
        let first = date_source_batch(&[(0, 0, None), (1, 1, None)], false)?;
        let start = if nulls_first {
            WindowFrameBound::PrecedingNullOffset(DataType::Int32)
        } else {
            WindowFrameBound::CurrentRow
        };
        let window = range_count_window(
            first.schema(),
            start,
            end,
            SortOptions {
                descending: false,
                nulls_first,
            },
        )?;
        let mut buffers = PartitionBatches::default();
        let mut states = PartitionWindowAggStates::default();
        append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
        let later = if nulls_first { Some(1) } else { None };
        append_linear_batch(
            window.as_ref(),
            &mut buffers,
            &mut states,
            date_source_batch(&[(2, 0, later), (3, 1, later)], false)?,
        )?;
        assert!(counts(&states, 0)?.is_empty());
        assert!(counts(&states, 1)?.is_empty());
        for buffer in buffers.values_mut() {
            buffer.is_end = true;
        }
        window.evaluate_stateful(&buffers, &mut states)?;
        for state in states.values() {
            let values = state
                .state
                .out_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("count type")?;
            assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(2), Some(2)]);
            assert_eq!(state.state.n_row_result_missing, 0);
        }
    }
    Ok(())
}

#[test]
fn null_lower_with_finite_upper_waits_for_null_peers() -> TestResult {
    for end in [
        WindowFrameBound::Preceding(ScalarValue::Int32(Some(1))),
        WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
    ] {
        let first = date_source_batch(&[(0, 0, None), (1, 1, None)], false)?;
        let window = range_count_window(
            first.schema(),
            WindowFrameBound::PrecedingNullOffset(DataType::Int32),
            end,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )?;
        let mut buffers = PartitionBatches::default();
        let mut states = PartitionWindowAggStates::default();
        append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
        assert!(counts(&states, 0)?.is_empty());
        assert!(counts(&states, 1)?.is_empty());
        append_linear_batch(
            window.as_ref(),
            &mut buffers,
            &mut states,
            date_source_batch(&[(2, 1, Some(1))], false)?,
        )?;
        assert_eq!(counts(&states, 0)?, [1]);
    }
    Ok(())
}

#[test]
fn null_range_builtins_preserve_values_validity_and_sparse_completion() -> TestResult {
    let source = |rows: &[(i32, i32, Option<i32>)]| -> TestResult<RecordBatch> {
        let batch = date_source_batch(rows, false)?;
        let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new("v", DataType::Int32, true)));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| if row.0 == 1 { Some(11) } else { None })
                .collect::<Vec<_>>(),
        )));
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    };
    for (function, nth, expected_respect, expected_ignore) in [
        (first_value_udwf(), false, None, Some(11)),
        (last_value_udwf(), false, Some(11), Some(11)),
        (nth_value_udwf(), true, Some(11), None),
    ] {
        for ignore_nulls in [false, true] {
            let first = source(&[(0, 0, None), (1, 0, None), (2, 1, None)])?;
            let schema = first.schema();
            let mut arguments = vec![col("v", &schema)?];
            if nth {
                arguments.push(Arc::new(Literal::new(ScalarValue::Int64(Some(2)))));
            }
            let window = create_window_expr(
                &WindowFunctionDefinition::WindowUDF(function.clone()),
                "value".into(),
                &arguments,
                &[col("p", &schema)?],
                &[PhysicalSortExpr {
                    expr: col("k", &schema)?,
                    options: SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                }],
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Range,
                    WindowFrameBound::CurrentRow,
                    WindowFrameBound::FollowingNullOffset(DataType::Int32),
                )),
                schema,
                ignore_nulls,
                false,
                None,
            )?;
            assert_eq!(
                window.field()?.as_ref(),
                &Field::new("value", DataType::Int32, true)
            );
            assert!(!window.uses_bounded_memory());
            let mut buffers = PartitionBatches::default();
            let mut states = PartitionWindowAggStates::default();
            append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
            assert!(states.values().all(|state| state.state.out_col.is_empty()));
            append_linear_batch(
                window.as_ref(),
                &mut buffers,
                &mut states,
                source(&[(3, 1, Some(1))])?,
            )?;
            for partition in [0, 1] {
                let state = &states[&vec![ScalarValue::Int32(Some(partition))]].state;
                let values = state
                    .out_col
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("value type")?;
                let value = if ignore_nulls {
                    expected_ignore
                } else {
                    expected_respect
                };
                let expected = if partition == 0 {
                    vec![value, value]
                } else {
                    vec![None, None]
                };
                assert_eq!(values.iter().collect::<Vec<_>>(), expected);
                assert_eq!(
                    values.null_count(),
                    expected.iter().filter(|value| value.is_none()).count()
                );
                assert_eq!(state.n_row_result_missing, 0);
            }
        }
    }
    Ok(())
}

#[test]
fn null_range_planning_selects_final_operator_and_declines_reversal() -> TestResult {
    let schema = date_source_batch(&[(0, 0, None)], false)?.schema();
    let options = SortOptions {
        descending: false,
        nulls_first: true,
    };
    let order_by = PhysicalSortExpr {
        expr: col("k", &schema)?,
        options,
    };
    for (start, end) in [
        (
            WindowFrameBound::CurrentRow,
            WindowFrameBound::FollowingNullOffset(DataType::Int32),
        ),
        (
            WindowFrameBound::PrecedingNullOffset(DataType::Int32),
            WindowFrameBound::CurrentRow,
        ),
        (
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::FollowingNullOffset(DataType::Int32),
        ),
    ] {
        let window = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "n".into(),
            &[col("id", &schema)?],
            &[],
            std::slice::from_ref(&order_by),
            Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Range, start, end)),
            schema.clone(),
            false,
            false,
            None,
        )?;
        for reverse in [false, true] {
            let source_order = if reverse {
                order_by.reverse()
            } else {
                order_by.clone()
            };
            let ordering = LexOrdering::new(vec![source_order]).ok_or("empty ordering")?;
            let input: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
                schema.clone(),
                vec![],
                None,
                Some(ordering),
                false,
                None,
            )?);
            let plan = get_best_fitting_window(std::slice::from_ref(&window), &input, &[])?;
            if reverse {
                assert!(plan.is_none());
            } else {
                assert!(
                    plan.ok_or("matching ordering rejected")?
                        .downcast_ref::<WindowAggExec>()
                        .is_some()
                );
            }
        }
    }
    Ok(())
}

#[test]
fn null_lower_builtin_true_unbounded_upper_does_not_complete_at_null_target() -> TestResult {
    let first = date_source_batch(&[(0, 0, None), (1, 1, Some(1))], false)?;
    let schema = first.schema();
    let window = create_window_expr(
        &WindowFunctionDefinition::WindowUDF(last_value_udwf()),
        "value".into(),
        &[col("id", &schema)?],
        &[col("p", &schema)?],
        &[PhysicalSortExpr {
            expr: col("k", &schema)?,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }],
        Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::PrecedingNullOffset(DataType::Int32),
            WindowFrameBound::Following(ScalarValue::Int32(None)),
        )),
        schema,
        false,
        false,
        None,
    )?;
    let mut buffers = PartitionBatches::default();
    let mut states = PartitionWindowAggStates::default();
    append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
    assert!(states.values().all(|state| state.state.out_col.is_empty()));
    append_linear_batch(
        window.as_ref(),
        &mut buffers,
        &mut states,
        date_source_batch(&[(2, 0, Some(2)), (3, 1, Some(3))], false)?,
    )?;
    assert!(states.values().all(|state| state.state.out_col.is_empty()));
    for buffer in buffers.values_mut() {
        buffer.is_end = true;
    }
    window.evaluate_stateful(&buffers, &mut states)?;
    for partition in [0, 1] {
        let state = &states[&vec![ScalarValue::Int32(Some(partition))]].state;
        let values = state
            .out_col
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("value type")?;
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(partition + 2); 2]
        );
        assert_eq!(values.null_count(), 0);
        assert_eq!(state.n_row_result_missing, 0);
    }
    Ok(())
}

#[test]
fn null_lower_numeric_upper_does_not_cache_row_dependent_completion() -> TestResult {
    let first = source_batch(&[(0, 0, 0), (1, 0, 5), (2, 1, 11)], false, false)?;
    let window = range_count_window(
        first.schema(),
        WindowFrameBound::PrecedingNullOffset(DataType::Int32),
        WindowFrameBound::Preceding(ScalarValue::Int32(Some(-10))),
        SortOptions {
            descending: false,
            nulls_first: true,
        },
    )?;
    let mut buffers = PartitionBatches::default();
    let mut states = PartitionWindowAggStates::default();
    append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
    let key = vec![ScalarValue::Int32(Some(0))];
    // Latest key 11 passes row 0's target 10, but not row 1's target 15.
    assert_eq!(counts(&states, 0)?, [2]);
    assert_eq!(states[&key].state.last_calculated_index, 1);
    assert_eq!(states[&key].state.n_row_result_missing, 1);
    assert_eq!(states[&key].state.window_frame_range, 0..2);
    append_linear_batch(
        window.as_ref(),
        &mut buffers,
        &mut states,
        source_batch(&[(3, 0, 12), (4, 1, 16)], false, false)?,
    )?;
    assert_eq!(counts(&states, 0)?, [2, 3]);
    assert_eq!(states[&key].state.last_calculated_index, 2);
    assert_eq!(states[&key].state.n_row_result_missing, 1);
    assert_eq!(states[&key].state.window_frame_range, 0..3);
    for buffer in buffers.values_mut() {
        buffer.is_end = true;
    }
    window.evaluate_stateful(&buffers, &mut states)?;
    for (partition, expected) in [(0, vec![2, 3, 3]), (1, vec![2, 2])] {
        let state = &states[&vec![ScalarValue::Int32(Some(partition))]].state;
        let values = state
            .out_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("count type")?;
        assert_eq!(values.values().as_ref(), expected.as_slice());
        assert_eq!(values.null_count(), 0);
        assert_eq!(state.n_row_result_missing, 0);
        assert!(state.is_end);
    }
    Ok(())
}

#[test]
fn null_lower_numeric_upper_waits_for_later_null_peers() -> TestResult {
    let source = |rows: &[(i32, i32, Option<i32>)]| -> TestResult<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("p", DataType::Int32, false),
            Field::new("k", DataType::Int32, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ];
        Ok(RecordBatch::try_new(schema, columns)?)
    };
    for (end, expected_partition_one) in [
        (
            WindowFrameBound::Preceding(ScalarValue::Int32(Some(1))),
            vec![2, 2, 2],
        ),
        (
            WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
            vec![2, 2, 3],
        ),
    ] {
        let first = source(&[(0, 0, None), (1, 1, None)])?;
        let window = range_count_window(
            first.schema(),
            WindowFrameBound::PrecedingNullOffset(DataType::Int32),
            end,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )?;
        let mut buffers = PartitionBatches::default();
        let mut states = PartitionWindowAggStates::default();
        append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
        assert!(counts(&states, 0)?.is_empty());
        assert!(counts(&states, 1)?.is_empty());
        append_linear_batch(
            window.as_ref(),
            &mut buffers,
            &mut states,
            source(&[(2, 0, None), (3, 1, None)])?,
        )?;
        for partition in [0, 1] {
            assert!(counts(&states, partition)?.is_empty());
            let state = &states[&vec![ScalarValue::Int32(Some(partition))]].state;
            assert_eq!(state.last_calculated_index, 0);
            assert_eq!(state.n_row_result_missing, 2);
        }
        append_linear_batch(
            window.as_ref(),
            &mut buffers,
            &mut states,
            source(&[(4, 1, Some(1))])?,
        )?;
        assert_eq!(counts(&states, 0)?, [2, 2]);
        assert_eq!(
            states[&vec![ScalarValue::Int32(Some(0))]]
                .state
                .n_row_result_missing,
            0
        );
        for buffer in buffers.values_mut() {
            buffer.is_end = true;
        }
        window.evaluate_stateful(&buffers, &mut states)?;
        for (partition, expected) in [(0, vec![2, 2]), (1, expected_partition_one)] {
            let state = &states[&vec![ScalarValue::Int32(Some(partition))]].state;
            let values = state
                .out_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("count type")?;
            assert_eq!(values.values().as_ref(), expected.as_slice());
            assert_eq!(values.null_count(), 0);
            assert_eq!(state.n_row_result_missing, 0);
            assert!(state.is_end);
        }
    }
    Ok(())
}

#[test]
fn date_null_frame_policy_survives_clone_and_reverse_without_changing_sql() -> TestResult {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let frame = WindowFrame::new_bounds(
        WindowFrameUnits::Range,
        WindowFrameBound::Following(ScalarValue::Int32(Some(i32::MIN))),
        WindowFrameBound::PrecedingNullOffset(DataType::Int32),
    );
    assert!(!frame.checks_date32_offset_negation());
    assert!(!WindowFrame::new(None).checks_date32_offset_negation());
    assert!(!WindowFrame::new(Some(false)).checks_date32_offset_negation());
    let checked = frame.clone().with_date32_offset_negation_check();
    assert!(checked.checks_date32_offset_negation());
    assert_ne!(checked, frame);
    assert!(checked.clone().checks_date32_offset_negation());
    assert!(checked.reverse().checks_date32_offset_negation());
    assert_eq!(checked.reverse().reverse(), checked);
    assert_eq!(checked.to_string(), frame.to_string());
    assert!(!format!("{frame:?}").contains("date32_offset_negation_check"));
    assert!(format!("{checked:?}").contains("date32_offset_negation_check: true"));
    let hash = |value: &WindowFrame| {
        let mut state = DefaultHasher::new();
        value.hash(&mut state);
        state.finish()
    };
    assert_eq!(hash(&checked), hash(&checked.clone()));
    assert_eq!(hash(&checked), hash(&checked.reverse().reverse()));
    Ok(())
}

#[test]
fn date_null_frame_checks_negation_only_for_a_required_non_null_comparison() -> TestResult {
    for (keys, expected) in [
        (vec![Some(3), Some(2), Some(1)], vec![0..0, 0..0, 0..0]),
        (vec![None, None], vec![0..2, 0..2]),
    ] {
        let column: ArrayRef = Arc::new(Date32Array::from(keys));
        let frame = Arc::new(
            WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::Following(ScalarValue::Int32(Some(i32::MIN))),
                WindowFrameBound::PrecedingNullOffset(DataType::Int32),
            )
            .with_date32_offset_negation_check(),
        );
        let mut context = WindowFrameContext::new(
            frame,
            vec![SortOptions {
                descending: true,
                nulls_first: true,
            }],
        );
        let mut last = 0..0;
        for (idx, expected) in expected.into_iter().enumerate() {
            last =
                context.calculate_range(std::slice::from_ref(&column), &last, column.len(), idx)?;
            assert_eq!(last, expected);
        }
    }
    let column: ArrayRef = Arc::new(Date32Array::from(vec![None, Some(3)]));
    let frame = WindowFrame::new_bounds(
        WindowFrameUnits::Range,
        WindowFrameBound::Following(ScalarValue::Int32(Some(i32::MIN))),
        WindowFrameBound::PrecedingNullOffset(DataType::Int32),
    )
    .with_date32_offset_negation_check();
    let mut context = WindowFrameContext::new(
        Arc::new(frame.clone()),
        vec![SortOptions {
            descending: true,
            nulls_first: true,
        }],
    );
    let first = context.calculate_range(std::slice::from_ref(&column), &(0..0), 2, 0)?;
    assert_eq!(first, 0..1);
    let error = context
        .calculate_range(std::slice::from_ref(&column), &first, 2, 1)
        .expect_err("required DATE comparison must check negation");
    assert!(
        error
            .to_string()
            .contains("[ARITHMETIC_OVERFLOW] DATE RANGE offset negation")
    );
    assert!(
        error
            .to_string()
            .contains("Int32 overflow on negative(-2147483648)")
    );
    let mut reversed = WindowFrameContext::new(
        Arc::new(frame.reverse()),
        vec![SortOptions {
            descending: false,
            nulls_first: false,
        }],
    );
    let reversed_column: ArrayRef = Arc::new(Date32Array::from(vec![Some(3), None]));
    assert!(
        reversed
            .calculate_range(&[reversed_column], &(0..0), 2, 0)
            .is_err()
    );
    Ok(())
}

#[test]
fn date_null_frame_static_aggregate_and_builtin_preserve_lazy_errors_and_validity() -> TestResult {
    for (function, ordinal, expected_peer_id) in [
        (
            WindowFunctionDefinition::AggregateUDF(count_udaf()),
            None,
            None,
        ),
        (
            WindowFunctionDefinition::WindowUDF(first_value_udwf()),
            None,
            Some(0),
        ),
        (
            WindowFunctionDefinition::WindowUDF(last_value_udwf()),
            None,
            Some(1),
        ),
        (
            WindowFunctionDefinition::WindowUDF(nth_value_udwf()),
            Some(2),
            Some(1),
        ),
    ] {
        let batch = date_source_batch(&[(0, 0, Some(1)), (1, 0, Some(2))], true)?;
        let schema = batch.schema();
        let frame = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Following(ScalarValue::Int32(Some(i32::MIN))),
            WindowFrameBound::PrecedingNullOffset(DataType::Int32),
        );
        let make_window = |frame: WindowFrame| -> TestResult<Arc<dyn WindowExpr>> {
            let mut arguments = vec![col("id", &schema)?];
            if let Some(ordinal) = ordinal {
                arguments.push(Arc::new(Literal::new(ScalarValue::Int64(Some(ordinal)))));
            }
            Ok(create_window_expr(
                &function,
                "value".into(),
                &arguments,
                &[col("p", &schema)?],
                &[PhysicalSortExpr {
                    expr: col("k", &schema)?,
                    options: SortOptions {
                        descending: true,
                        nulls_first: true,
                    },
                }],
                Arc::new(frame),
                schema.clone(),
                false,
                false,
                None,
            )?)
        };
        let unchecked = make_window(frame.clone())?;
        let checked = make_window(frame.with_date32_offset_negation_check())?;
        assert!(!checked.uses_bounded_memory());
        assert_eq!(checked.field()?, unchecked.field()?);
        let values = checked.evaluate(&batch)?;
        if expected_peer_id.is_some() {
            assert_eq!(values.null_count(), 2);
            assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("value type")?
                    .iter()
                    .collect::<Vec<_>>(),
                vec![None, None]
            );
        } else {
            assert_eq!(values.null_count(), 0);
            assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("count type")?
                    .values()
                    .as_ref(),
                &[0, 0]
            );
        }
        let all_null = date_source_batch(&[(0, 0, None), (1, 0, None)], true)?;
        let values = checked.evaluate(&all_null)?;
        assert_eq!(values.null_count(), 0);
        if let Some(expected_peer_id) = expected_peer_id {
            assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("value type")?
                    .values()
                    .as_ref(),
                &[expected_peer_id, expected_peer_id]
            );
        } else {
            assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("count type")?
                    .values()
                    .as_ref(),
                &[2, 2]
            );
        }
        let mixed = date_source_batch(&[(0, 0, None), (1, 0, Some(1))], true)?;
        let error = checked
            .evaluate(&mixed)
            .expect_err("required DATE comparison must fail");
        assert!(error.to_string().contains("[ARITHMETIC_OVERFLOW]"));
        assert!(
            error
                .to_string()
                .contains("Int32 overflow on negative(-2147483648)")
        );
        assert!(unchecked.evaluate(&mixed).is_ok());
    }
    Ok(())
}

#[test]
fn date_null_frame_completion_does_not_speculate_on_checked_targets() -> TestResult {
    let first = date_source_batch(&[(0, 0, None)], true)?;
    let schema = first.schema();
    let window = create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "n".into(),
        &[col("id", &schema)?],
        &[col("p", &schema)?],
        &[PhysicalSortExpr {
            expr: col("k", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: true,
            },
        }],
        Arc::new(
            WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::PrecedingNullOffset(DataType::Int32),
                WindowFrameBound::Following(ScalarValue::Int32(Some(i32::MIN))),
            )
            .with_date32_offset_negation_check(),
        ),
        schema,
        false,
        false,
        None,
    )?;
    let key = vec![ScalarValue::Int32(Some(0))];
    let mut buffer = PartitionBatchState::new_with_batch(first);
    // A finite globally latest row in another partition must not prove a checked target safe.
    buffer.set_most_recent_row(date_source_batch(&[(1, 1, Some(1))], true)?);
    let mut buffers = PartitionBatches::default();
    buffers.insert(key.clone(), buffer);
    let mut states = PartitionWindowAggStates::default();
    window.evaluate_stateful(&buffers, &mut states)?;
    assert!(counts(&states, 0)?.is_empty());
    assert_eq!(states[&key].state.n_row_result_missing, 1);
    buffers.get_mut(&key).ok_or("missing buffer")?.is_end = true;
    window.evaluate_stateful(&buffers, &mut states)?;
    let values = states[&key]
        .state
        .out_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("count type")?;
    assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(1)]);
    Ok(())
}
