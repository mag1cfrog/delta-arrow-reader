//! Stateful completion with the same global-row guarantee as LinearSearch.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array,
    Int64Array, ListArray, StructArray, UInt32Array,
};
use arrow::compute::{SortOptions, take_record_batch};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef, i256};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::common::config::ConfigOptions;
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::min_max::max_udaf;
use datafusion::functions_window::nth_value::{first_value_udwf, last_value_udwf, nth_value_udwf};
use datafusion::functions_window::row_number::row_number_udwf;
use datafusion::logical_expr::window_frame::RangeOffsetArithmetic;
use datafusion::logical_expr::window_state::{
    PartitionBatchState, WindowFrameContext, range_bound_target,
};
use datafusion::logical_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::physical_expr::expressions::{Literal, col};
use datafusion::physical_expr::window::{PartitionBatches, PartitionWindowAggStates, WindowExpr};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_plan::execution_plan::{Boundedness, CardinalityEffect};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::windows::{
    BoundedWindowAggExec, WindowAggExec, create_window_expr, get_best_fitting_window,
};
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream,
};
use futures_util::{FutureExt, StreamExt, stream};

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
            assert_eq!(
                range_bound_target(&null, &bound, false, RangeOffsetArithmetic::DataFusion)?,
                None
            );
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
            range_bound_target(
                &ScalarValue::Date32(Some(1)),
                &bound,
                false,
                RangeOffsetArithmetic::DataFusion
            )?,
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
            range_bound_target(
                &ScalarValue::Int32(None),
                &bound,
                false,
                RangeOffsetArithmetic::DataFusion
            )?,
            Some(ScalarValue::Int32(None))
        );
    }
    assert_eq!(
        range_bound_target(
            &ScalarValue::UInt32(Some(1)),
            &WindowFrameBound::Preceding(ScalarValue::UInt32(Some(2))),
            false,
            RangeOffsetArithmetic::DataFusion
        )?,
        Some(ScalarValue::UInt32(Some(0)))
    );
    let following_one = WindowFrameBound::Following(ScalarValue::Int32(Some(1)));
    assert_eq!(
        range_bound_target(
            &ScalarValue::Int32(Some(i32::MAX)),
            &following_one,
            false,
            RangeOffsetArithmetic::DataFusion
        )?,
        None
    );
    assert_eq!(
        range_bound_target(
            &ScalarValue::Date32(Some(i32::MAX)),
            &following_one,
            false,
            RangeOffsetArithmetic::DataFusion
        )?,
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
    assert_eq!(
        frame.range_offset_arithmetic(),
        RangeOffsetArithmetic::DataFusion
    );
    assert_eq!(
        WindowFrame::new(None).range_offset_arithmetic(),
        RangeOffsetArithmetic::DataFusion
    );
    assert_eq!(
        WindowFrame::new(Some(false)).range_offset_arithmetic(),
        RangeOffsetArithmetic::DataFusion
    );
    let checked = frame
        .clone()
        .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true });
    assert_eq!(
        checked.range_offset_arithmetic(),
        RangeOffsetArithmetic::Spark { ansi_mode: true }
    );
    assert_ne!(checked, frame);
    assert_eq!(
        checked.clone().range_offset_arithmetic(),
        RangeOffsetArithmetic::Spark { ansi_mode: true }
    );
    assert_eq!(
        checked.reverse().range_offset_arithmetic(),
        RangeOffsetArithmetic::Spark { ansi_mode: true }
    );
    assert_eq!(checked.reverse().reverse(), checked);
    assert_eq!(checked.to_string(), frame.to_string());
    assert!(!format!("{frame:?}").contains("range_offset_arithmetic"));
    assert!(format!("{checked:?}").contains("range_offset_arithmetic: Spark { ansi_mode: true }"));
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
            .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true }),
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
    .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true });
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
        let checked = make_window(
            frame.with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true }),
        )?;
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
            .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true }),
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

#[test]
fn spark_integer_range_arithmetic_wraps_or_checks_each_signed_width() -> TestResult {
    for (maximum, minimum, one, zero) in [
        (
            ScalarValue::Int8(Some(i8::MAX)),
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Int8(Some(1)),
            ScalarValue::Int8(Some(0)),
        ),
        (
            ScalarValue::Int16(Some(i16::MAX)),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int16(Some(1)),
            ScalarValue::Int16(Some(0)),
        ),
        (
            ScalarValue::Int32(Some(i32::MAX)),
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int32(Some(1)),
            ScalarValue::Int32(Some(0)),
        ),
        (
            ScalarValue::Int64(Some(i64::MAX)),
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(0)),
        ),
    ] {
        let following_one = WindowFrameBound::Following(one.clone());
        assert_eq!(
            range_bound_target(
                &maximum,
                &following_one,
                false,
                RangeOffsetArithmetic::DataFusion
            )?,
            None
        );
        assert_eq!(
            range_bound_target(
                &maximum,
                &following_one,
                false,
                RangeOffsetArithmetic::Spark { ansi_mode: false }
            )?,
            Some(minimum.clone())
        );
        let error = range_bound_target(
            &maximum,
            &following_one,
            false,
            RangeOffsetArithmetic::Spark { ansi_mode: true },
        )
        .expect_err("checked addition must overflow");
        let condition = if matches!(maximum, ScalarValue::Int8(_) | ScalarValue::Int16(_)) {
            "[BINARY_ARITHMETIC_OVERFLOW]"
        } else {
            "[ARITHMETIC_OVERFLOW]"
        };
        assert!(error.to_string().contains(condition));
        let following_minimum = WindowFrameBound::Following(minimum.clone());
        // Direct checked subtraction would accept MIN-MIN; Spark first negates the offset.
        assert_eq!(
            range_bound_target(
                &minimum,
                &following_minimum,
                true,
                RangeOffsetArithmetic::DataFusion
            )?,
            Some(zero.clone())
        );
        assert_eq!(
            range_bound_target(
                &minimum,
                &following_minimum,
                true,
                RangeOffsetArithmetic::Spark { ansi_mode: false }
            )?,
            Some(zero.clone())
        );
        let error = range_bound_target(
            &minimum,
            &following_minimum,
            true,
            RangeOffsetArithmetic::Spark { ansi_mode: true },
        )
        .expect_err("DESC must check MIN negation before addition");
        assert!(
            error
                .to_string()
                .contains("[ARITHMETIC_OVERFLOW] RANGE offset negation")
        );
        assert!(error.to_string().contains("overflow on negative("));
        let preceding_minimum = WindowFrameBound::Preceding(minimum.clone());
        assert_eq!(
            range_bound_target(
                &minimum,
                &preceding_minimum,
                false,
                RangeOffsetArithmetic::Spark { ansi_mode: false }
            )?,
            Some(zero)
        );
        assert!(
            range_bound_target(
                &minimum,
                &preceding_minimum,
                false,
                RangeOffsetArithmetic::Spark { ansi_mode: true }
            )
            .is_err()
        );
        let null = ScalarValue::try_from(&maximum.data_type())?;
        assert_eq!(
            range_bound_target(
                &null,
                &following_minimum,
                true,
                RangeOffsetArithmetic::Spark { ansi_mode: true }
            )?,
            Some(null.clone())
        );
        assert_eq!(
            range_bound_target(
                &null,
                &WindowFrameBound::Following(null.clone()),
                true,
                RangeOffsetArithmetic::Spark { ansi_mode: true }
            )?,
            None
        );
        assert_eq!(
            range_bound_target(
                &maximum,
                &WindowFrameBound::Preceding(one),
                true,
                RangeOffsetArithmetic::Spark { ansi_mode: false }
            )?,
            Some(minimum)
        );
    }
    let type_error = range_bound_target(
        &ScalarValue::Int32(Some(1)),
        &WindowFrameBound::Following(ScalarValue::Float64(Some(1.0))),
        false,
        RangeOffsetArithmetic::Spark { ansi_mode: true },
    )
    .expect_err("unsupported offset types must fail without overflow relabeling");
    assert!(!type_error.to_string().contains("[ARITHMETIC_OVERFLOW]"));
    Ok(())
}

#[test]
fn spark_integer_range_targets_preserve_consumed_upper_and_empty_comparisons() -> TestResult {
    // Spark's independent minimum_int_no_null_nullp_minp_asc_nf case has count6 for this partition.
    let column: ArrayRef = Arc::new(Int32Array::from(vec![-4, 0, 1, 1, 3, 7]));
    let frame = Arc::new(
        WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::PrecedingNullOffset(DataType::Int32),
            WindowFrameBound::Preceding(ScalarValue::Int32(Some(i32::MIN))),
        )
        .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: false }),
    );
    let mut context = WindowFrameContext::new(
        frame,
        vec![SortOptions {
            descending: false,
            nulls_first: true,
        }],
    );
    let mut last = 0..0;
    for idx in 0..column.len() {
        last = context.calculate_range(std::slice::from_ref(&column), &last, column.len(), idx)?;
        assert_eq!(last, 0..6);
    }
    for (nulls_first, expect_error) in [(true, false), (false, true)] {
        let frame = Arc::new(
            WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::Following(ScalarValue::Int32(Some(i32::MIN))),
                WindowFrameBound::PrecedingNullOffset(DataType::Int32),
            )
            .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true }),
        );
        let mut context = WindowFrameContext::new(
            frame,
            vec![SortOptions {
                descending: false,
                nulls_first,
            }],
        );
        let mut last = 0..0;
        for idx in 0..column.len() {
            let result =
                context.calculate_range(std::slice::from_ref(&column), &last, column.len(), idx);
            if expect_error {
                assert!(
                    result
                        .expect_err("required addition comparison must overflow")
                        .to_string()
                        .contains("[ARITHMETIC_OVERFLOW]")
                );
                break;
            }
            last = result?;
            assert_eq!(last, 0..0);
        }
    }
    Ok(())
}

#[test]
fn spark_date_range_key_addition_wraps_in_both_ansi_modes() -> TestResult {
    let bound = WindowFrameBound::Following(ScalarValue::Int32(Some(1)));
    for ansi_mode in [false, true] {
        assert_eq!(
            range_bound_target(
                &ScalarValue::Date32(Some(i32::MAX)),
                &bound,
                false,
                RangeOffsetArithmetic::Spark { ansi_mode }
            )?,
            Some(ScalarValue::Date32(Some(i32::MIN)))
        );
    }
    Ok(())
}

#[test]
fn spark_decimal_range_targets_widen_before_sign_arithmetic() -> TestResult {
    let maximum = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128;
    let positive_sum = i256::from_string("199999999999999999999999999999999999998")
        .ok_or("positive 39-digit target")?;
    let negative_sum = i256::from_string("-199999999999999999999999999999999999998")
        .ok_or("negative 39-digit target")?;
    for scale in [0, 2] {
        let positive = ScalarValue::Decimal128(Some(maximum), 38, scale);
        let negative = ScalarValue::Decimal128(Some(-maximum), 38, scale);
        for ansi_mode in [false, true] {
            for (key, bound, descending, expected) in [
                (
                    positive.clone(),
                    WindowFrameBound::Following(positive.clone()),
                    false,
                    positive_sum,
                ),
                (
                    negative.clone(),
                    WindowFrameBound::Following(negative.clone()),
                    false,
                    negative_sum,
                ),
                (
                    negative.clone(),
                    WindowFrameBound::Following(positive.clone()),
                    true,
                    negative_sum,
                ),
                (
                    positive.clone(),
                    WindowFrameBound::Preceding(positive.clone()),
                    true,
                    positive_sum,
                ),
            ] {
                assert_eq!(
                    range_bound_target(
                        &key,
                        &bound,
                        descending,
                        RangeOffsetArithmetic::Spark { ansi_mode },
                    )?,
                    Some(ScalarValue::Decimal256(Some(expected), 76, scale))
                );
            }
            let null = ScalarValue::Decimal128(None, 38, scale);
            assert_eq!(
                range_bound_target(
                    &null,
                    &WindowFrameBound::Following(positive.clone()),
                    true,
                    RangeOffsetArithmetic::Spark { ansi_mode },
                )?,
                Some(null.clone())
            );
            assert_eq!(
                range_bound_target(
                    &positive,
                    &WindowFrameBound::FollowingNullOffset(positive.data_type()),
                    false,
                    RangeOffsetArithmetic::Spark { ansi_mode },
                )?,
                Some(null)
            );
        }
        assert_eq!(
            range_bound_target(
                &positive,
                &WindowFrameBound::Following(positive.clone()),
                false,
                RangeOffsetArithmetic::DataFusion,
            )?,
            None
        );
    }
    for offset in [
        ScalarValue::Decimal128(Some(1), 38, 1),
        ScalarValue::Float64(Some(1.0)),
        ScalarValue::Decimal256(Some(i256::ONE), 76, 0),
    ] {
        let error = range_bound_target(
            &ScalarValue::Decimal128(Some(1), 38, 0),
            &WindowFrameBound::Following(offset),
            false,
            RangeOffsetArithmetic::Spark { ansi_mode: true },
        )
        .expect_err("unadmitted pairs must not acquire arithmetic overflow labels");
        assert!(!error.to_string().contains("[ARITHMETIC_OVERFLOW]"));
    }
    Ok(())
}

#[test]
fn spark_decimal_range_membership_keeps_finite_targets_separate_from_nulls() -> TestResult {
    let maximum = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128;
    // ASC cases are the four captured decimal38_max boundary queries. DESC reflects
    // both the key sign and ordering, preserving the same finite-versus-NULL relation.
    for descending in [false, true] {
        let key = if descending { -maximum } else { maximum };
        for (nulls_first, lower_finite, expected) in [
            (false, false, vec![1..1, 1..2]),
            (true, false, vec![0..1, 0..2]),
            (false, true, vec![1..2, 1..2]),
            (true, true, vec![0..1, 1..1]),
        ] {
            let values = if nulls_first {
                vec![None, Some(key)]
            } else {
                vec![Some(key), None]
            };
            let column: ArrayRef =
                Arc::new(Decimal128Array::from(values).with_precision_and_scale(38, 0)?);
            for ansi_mode in [false, true] {
                let null = WindowFrameBound::PrecedingNullOffset(DataType::Decimal128(38, 0));
                let finite =
                    WindowFrameBound::Following(ScalarValue::Decimal128(Some(maximum), 38, 0));
                let (start, end) = if lower_finite {
                    (finite, null)
                } else {
                    (null, finite)
                };
                let frame = Arc::new(
                    WindowFrame::new_bounds(WindowFrameUnits::Range, start, end)
                        .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode }),
                );
                let mut context = WindowFrameContext::new(
                    frame,
                    vec![SortOptions {
                        descending,
                        nulls_first,
                    }],
                );
                let mut last = 0..0;
                for (idx, expected) in expected.iter().enumerate() {
                    last = context.calculate_range(
                        std::slice::from_ref(&column),
                        &last,
                        column.len(),
                        idx,
                    )?;
                    assert_eq!(&last, expected);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn spark_float_range_matches_infinity_and_negated_nan_spark_rows() -> TestResult {
    // Exact lengths from float/double_infinity_following and negated_nan_following.
    for wide in [false, true] {
        for (infinity, expected) in [(true, vec![1, 5, 5, 5, 5]), (false, vec![1, 3, 3])] {
            let (column, offset): (ArrayRef, ScalarValue) = if wide {
                let values = if infinity {
                    vec![
                        None,
                        Some(f64::NEG_INFINITY),
                        Some(0.0),
                        Some(f64::INFINITY),
                        Some(f64::NAN),
                    ]
                } else {
                    vec![None, Some(0.0), Some(f64::from_bits(0xfff8_0000_0000_0123))]
                };
                (
                    Arc::new(Float64Array::from(values)),
                    ScalarValue::Float64(Some(if infinity {
                        f64::INFINITY
                    } else {
                        f64::from_bits(0xfff8_0000_0000_4567)
                    })),
                )
            } else {
                let values = if infinity {
                    vec![
                        None,
                        Some(f32::NEG_INFINITY),
                        Some(0.0),
                        Some(f32::INFINITY),
                        Some(f32::NAN),
                    ]
                } else {
                    vec![None, Some(0.0), Some(f32::from_bits(0xffc0_0123))]
                };
                (
                    Arc::new(Float32Array::from(values)),
                    ScalarValue::Float32(Some(if infinity {
                        f32::INFINITY
                    } else {
                        f32::from_bits(0xffc0_4567)
                    })),
                )
            };
            for ansi_mode in [false, true] {
                let frame = Arc::new(
                    WindowFrame::new_bounds(
                        WindowFrameUnits::Range,
                        WindowFrameBound::PrecedingNullOffset(column.data_type().clone()),
                        WindowFrameBound::Following(offset.clone()),
                    )
                    .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode }),
                );
                let mut context = WindowFrameContext::new(
                    frame,
                    vec![SortOptions {
                        descending: false,
                        nulls_first: true,
                    }],
                );
                let mut last = 0..0;
                for (idx, expected) in expected.iter().enumerate() {
                    last = context.calculate_range(
                        std::slice::from_ref(&column),
                        &last,
                        column.len(),
                        idx,
                    )?;
                    assert_eq!(last, 0..*expected);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn spark_float_range_peers_collapse_nan_payloads_and_signed_zeros() -> TestResult {
    for wide in [false, true] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let (column, zero): (ArrayRef, ScalarValue) = if wide {
                    let mut values = vec![
                        Some(-0.0),
                        Some(0.0),
                        Some(f64::from_bits(0xfff8_0000_0000_0123)),
                        Some(f64::from_bits(0x7ff8_0000_0000_4567)),
                    ];
                    if descending {
                        values.rotate_left(2);
                    }
                    if nulls_first {
                        values.insert(0, None);
                    } else {
                        values.push(None);
                    }
                    (
                        Arc::new(Float64Array::from(values)),
                        ScalarValue::Float64(Some(0.0)),
                    )
                } else {
                    let mut values = vec![
                        Some(-0.0),
                        Some(0.0),
                        Some(f32::from_bits(0xffc0_0123)),
                        Some(f32::from_bits(0x7fc0_4567)),
                    ];
                    if descending {
                        values.rotate_left(2);
                    }
                    if nulls_first {
                        values.insert(0, None);
                    } else {
                        values.push(None);
                    }
                    (
                        Arc::new(Float32Array::from(values)),
                        ScalarValue::Float32(Some(0.0)),
                    )
                };
                let expected = if nulls_first {
                    vec![0..1, 0..3, 0..3, 0..5, 0..5]
                } else {
                    vec![2..2, 2..2, 4..4, 4..4, 4..5]
                };
                for ansi_mode in [false, true] {
                    let frame = Arc::new(
                        WindowFrame::new_bounds(
                            WindowFrameUnits::Range,
                            WindowFrameBound::PrecedingNullOffset(column.data_type().clone()),
                            WindowFrameBound::Following(zero.clone()),
                        )
                        .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode }),
                    );
                    let mut context = WindowFrameContext::new(
                        frame,
                        vec![SortOptions {
                            descending,
                            nulls_first,
                        }],
                    );
                    let mut last = 0..0;
                    for (idx, expected) in expected.iter().enumerate() {
                        last = context.calculate_range(
                            std::slice::from_ref(&column),
                            &last,
                            column.len(),
                            idx,
                        )?;
                        assert_eq!(&last, expected);
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn spark_float_range_negation_preserves_signed_zero_before_addition() -> TestResult {
    for ansi_mode in [false, true] {
        for descending in [false, true] {
            let bound32 = if descending {
                WindowFrameBound::Following(ScalarValue::Float32(Some(0.0)))
            } else {
                WindowFrameBound::Preceding(ScalarValue::Float32(Some(0.0)))
            };
            let bound64 = if descending {
                WindowFrameBound::Following(ScalarValue::Float64(Some(0.0)))
            } else {
                WindowFrameBound::Preceding(ScalarValue::Float64(Some(0.0)))
            };
            let target32 = range_bound_target(
                &ScalarValue::Float32(Some(-0.0)),
                &bound32,
                descending,
                RangeOffsetArithmetic::Spark { ansi_mode },
            )?;
            let target64 = range_bound_target(
                &ScalarValue::Float64(Some(-0.0)),
                &bound64,
                descending,
                RangeOffsetArithmetic::Spark { ansi_mode },
            )?;
            let Some(ScalarValue::Float32(Some(target32))) = target32 else {
                return Err("FLOAT comparison target type".into());
            };
            let Some(ScalarValue::Float64(Some(target64))) = target64 else {
                return Err("DOUBLE comparison target type".into());
            };
            assert_eq!(target32.to_bits(), (-0.0_f32).to_bits());
            assert_eq!(target64.to_bits(), (-0.0_f64).to_bits());
        }
    }
    Ok(())
}

#[test]
fn spark_integer_range_guards_are_derived_from_nonzero_offsets() -> TestResult {
    for one in [
        ScalarValue::Int8(Some(1)),
        ScalarValue::Int16(Some(1)),
        ScalarValue::Int32(Some(1)),
        ScalarValue::Int64(Some(1)),
    ] {
        let native = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Preceding(ScalarValue::Null),
            WindowFrameBound::Preceding(one.clone()),
        );
        assert!(native.is_causal());
        assert!(!native.has_nonzero_spark_integer_range_offset());
        for ansi_mode in [false, true] {
            let spark = native
                .clone()
                .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode });
            assert!(spark.has_nonzero_spark_integer_range_offset());
            assert!(!spark.is_causal());
            assert!(format!("{spark:?}").contains("is_causal: false"));
            assert_eq!(spark.to_string(), native.to_string());
            assert_eq!(spark.clone(), spark.reverse().reverse());
            assert_eq!(
                spark.with_range_offset_arithmetic(RangeOffsetArithmetic::DataFusion),
                native
            );
            for end in [
                WindowFrameBound::CurrentRow,
                WindowFrameBound::Following(ScalarValue::new_zero(&one.data_type())?),
                WindowFrameBound::Following(ScalarValue::Null),
            ] {
                let frame = WindowFrame::new_bounds(
                    WindowFrameUnits::Range,
                    WindowFrameBound::CurrentRow,
                    end,
                );
                let causal = frame.is_causal();
                let spark =
                    frame.with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode });
                assert!(!spark.has_nonzero_spark_integer_range_offset());
                assert_eq!(spark.is_causal(), causal);
            }
        }
    }
    let unsigned = WindowFrame::new_bounds(
        WindowFrameUnits::Range,
        WindowFrameBound::CurrentRow,
        WindowFrameBound::Following(ScalarValue::UInt32(Some(1))),
    )
    .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: false });
    assert!(!unsigned.has_nonzero_spark_integer_range_offset());
    Ok(())
}

#[test]
fn spark_finite_integer_cursors_never_retract_unconsumed_rows() -> TestResult {
    for minimum in [
        ScalarValue::Int8(Some(i8::MIN)),
        ScalarValue::Int16(Some(i16::MIN)),
        ScalarValue::Int32(Some(i32::MIN)),
        ScalarValue::Int64(Some(i64::MIN)),
    ] {
        let key_type = minimum.data_type();
        let keys = ScalarValue::iter_to_array(
            [-4, 0, 1, 1, 3, 7]
                .into_iter()
                .map(|key| ScalarValue::Int32(Some(key)).cast_to(&key_type))
                .collect::<datafusion::common::Result<Vec<_>>>()?,
        )?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("k", key_type.clone(), false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5])),
                keys.clone(),
            ],
        )?;
        let frame = Arc::new(
            WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::Preceding(minimum),
                WindowFrameBound::Following(ScalarValue::new_one(&key_type)?),
            )
            .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: false }),
        );
        let mut context = WindowFrameContext::new(frame.clone(), vec![SortOptions::default()]);
        let mut last = 0..0;
        for (idx, expected) in [1..1, 1..4, 1..4, 1..4, 1..5, 1..6].into_iter().enumerate() {
            let next = context.calculate_range(std::slice::from_ref(&keys), &last, 6, idx)?;
            assert_eq!(next, expected, "{key_type:?} row{idx}");
            assert!(last.start <= next.start && last.end <= next.end && next.start <= next.end);
            last = next;
        }
        let window = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "n".into(),
            &[col("id", &batch.schema())?],
            &[],
            &[PhysicalSortExpr {
                expr: col("k", &batch.schema())?,
                options: SortOptions::default(),
            }],
            frame,
            batch.schema(),
            false,
            false,
            None,
        )?;
        assert!(window.uses_bounded_memory());
        let values = window.evaluate(&batch)?;
        assert_eq!(values.null_count(), 0);
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("count type")?
                .values()
                .as_ref(),
            &[0, 3, 3, 3, 4, 5]
        );
    }
    Ok(())
}

#[test]
fn spark_finite_integer_sparse_completion_prunes_consumed_rows() -> TestResult {
    for ansi_mode in [false, true] {
        for descending in [false, true] {
            for (function, ordinal, is_count) in [
                (
                    WindowFunctionDefinition::AggregateUDF(count_udaf()),
                    None,
                    true,
                ),
                (
                    WindowFunctionDefinition::WindowUDF(first_value_udwf()),
                    None,
                    false,
                ),
                (
                    WindowFunctionDefinition::WindowUDF(last_value_udwf()),
                    None,
                    false,
                ),
                (
                    WindowFunctionDefinition::WindowUDF(nth_value_udwf()),
                    Some(2),
                    false,
                ),
            ] {
                let first = source_batch(&[(0, 0, 0), (1, 1, 100)], false, descending)?;
                let schema = first.schema();
                let mut args = vec![col("id", &schema)?];
                if let Some(ordinal) = ordinal {
                    args.push(Arc::new(Literal::new(ScalarValue::Int64(Some(ordinal)))));
                }
                let window = create_window_expr(
                    &function,
                    "v".into(),
                    &args,
                    &[col("p", &schema)?],
                    &[PhysicalSortExpr {
                        expr: col("k", &schema)?,
                        options: SortOptions {
                            descending,
                            nulls_first: true,
                        },
                    }],
                    Arc::new(
                        WindowFrame::new_bounds(
                            WindowFrameUnits::Range,
                            WindowFrameBound::CurrentRow,
                            WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
                        )
                        .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode }),
                    ),
                    schema,
                    false,
                    false,
                    None,
                )?;
                assert!(window.uses_bounded_memory());
                let mut buffers = PartitionBatches::default();
                let mut states = PartitionWindowAggStates::default();
                append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
                let key = vec![ScalarValue::Int32(Some(0))];
                assert_eq!(states[&key].state.out_col.len(), 1);
                for (id, a_key, b_key) in [(2, 101, 103), (4, 104, 106)] {
                    append_linear_batch(
                        window.as_ref(),
                        &mut buffers,
                        &mut states,
                        source_batch(&[(id, 0, a_key), (id + 1, 1, b_key)], false, descending)?,
                    )?;
                    prune_partition(&mut buffers, &mut states, 0, 1)?;
                    assert_eq!(buffers[&key].record_batch.num_rows(), 1);
                }
                let state = &states[&key].state;
                assert_eq!(state.offset_pruned_rows, 2);
                assert_eq!(state.out_col.len(), 3);
                assert!(!state.is_end);
                for (index, id) in [0, 2, 4].into_iter().enumerate() {
                    let expected = if is_count {
                        ScalarValue::Int64(Some(1))
                    } else if ordinal.is_some() {
                        ScalarValue::Int32(None)
                    } else {
                        ScalarValue::Int32(Some(id))
                    };
                    assert_eq!(
                        ScalarValue::try_from_array(&state.out_col, index)?,
                        expected
                    );
                }
            }
        }
    }
    Ok(())
}

fn nullable_integer_range_batch(rows: &[(i32, i32, Option<i32>)]) -> TestResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("p", DataType::Int32, false),
            Field::new("k", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )?)
}

#[test]
fn spark_finite_integer_completion_keeps_exhausted_upper_errors_lazy() -> TestResult {
    for append_candidate in [false, true] {
        let first = nullable_integer_range_batch(&[
            (0, 0, Some(i32::MAX - 2)),
            (1, 0, Some(i32::MAX - 1)),
            (2, 0, Some(i32::MAX)),
            (3, 1, None),
        ])?;
        let schema = first.schema();
        let window = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "n".into(),
            &[col("id", &schema)?],
            &[col("p", &schema)?],
            &[PhysicalSortExpr {
                expr: col("k", &schema)?,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            }],
            Arc::new(
                WindowFrame::new_bounds(
                    WindowFrameUnits::Range,
                    WindowFrameBound::Preceding(ScalarValue::Null),
                    WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
                )
                .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true }),
            ),
            schema,
            false,
            false,
            None,
        )?;
        let mut buffers = PartitionBatches::default();
        let mut states = PartitionWindowAggStates::default();
        append_linear_batch(window.as_ref(), &mut buffers, &mut states, first)?;
        assert_eq!(counts(&states, 0)?, [2, 3]);
        let key = vec![ScalarValue::Int32(Some(0))];
        assert_eq!(states[&key].state.window_frame_range.end, 3);
        assert_eq!(states[&key].state.last_calculated_index, 2);
        prune_partition(&mut buffers, &mut states, 0, 2)?;
        assert_eq!(buffers[&key].record_batch.num_rows(), 1);
        if append_candidate {
            let error = append_linear_batch(
                window.as_ref(),
                &mut buffers,
                &mut states,
                nullable_integer_range_batch(&[(4, 0, None)])?,
            )
            .expect_err("a new candidate requires the checked MAX+1 comparison");
            assert!(
                error
                    .to_string()
                    .contains("[ARITHMETIC_OVERFLOW] RANGE comparison target")
            );
        } else {
            for buffer in buffers.values_mut() {
                buffer.is_end = true;
            }
            window.evaluate_stateful(&buffers, &mut states)?;
            let values = &states[&key].state.out_col;
            assert_eq!(values.null_count(), 0);
            assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or("count type")?
                    .values()
                    .as_ref(),
                &[2, 3, 3]
            );
        }
    }
    Ok(())
}

#[derive(Debug)]
struct PendingIntegerRangeInput(RecordBatch);

impl PartitionStream for PendingIntegerRangeInput {
    fn schema(&self) -> &SchemaRef {
        self.0.schema_ref()
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batches = stream::iter([Ok(self.0.clone())]).chain(stream::pending());
        Box::pin(RecordBatchStreamAdapter::new(self.0.schema(), batches))
    }
}

#[tokio::test]
async fn spark_finite_integer_bounded_executor_emits_before_input_eof() -> TestResult {
    for linear in [false, true] {
        let batch = if linear {
            source_batch(&[(0, 0, 0), (1, 1, 100)], false, false)?
        } else {
            source_batch(&[(0, 0, 0), (1, 0, 1), (2, 1, 0)], false, false)?
        };
        let schema = batch.schema();
        let key_order = PhysicalSortExpr {
            expr: col("k", &schema)?,
            options: SortOptions::default(),
        };
        let mut ordering = if linear {
            vec![]
        } else {
            vec![PhysicalSortExpr {
                expr: col("p", &schema)?,
                options: SortOptions::default(),
            }]
        };
        ordering.push(key_order.clone());
        let input: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
            schema.clone(),
            vec![Arc::new(PendingIntegerRangeInput(batch))],
            None,
            Some(LexOrdering::new(ordering).ok_or("empty source order")?),
            true,
            None,
        )?);
        let window = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "n".into(),
            &[col("id", &schema)?],
            &[col("p", &schema)?],
            &[key_order],
            Arc::new(
                WindowFrame::new_bounds(
                    WindowFrameUnits::Range,
                    WindowFrameBound::CurrentRow,
                    WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
                )
                .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true }),
            ),
            schema,
            false,
            false,
            None,
        )?;
        let plan =
            get_best_fitting_window(&[window], &input, &[])?.ok_or("fitting window missing")?;
        assert!(plan.downcast_ref::<BoundedWindowAggExec>().is_some());
        let mut output = plan.execute(0, Arc::new(TaskContext::default()))?;
        let ready = output
            .next()
            .now_or_never()
            .ok_or("finite frame waited for physical EOF")?
            .ok_or("stream ended before its pending source")??;
        let values = ready
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("count type")?;
        assert_eq!(
            values.values().as_ref(),
            if linear { &[1][..] } else { &[2, 1][..] }
        );
        assert!(output.next().now_or_never().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn spark_finite_integer_output_fetch_preserves_following_lookahead_and_groups() -> TestResult
{
    for batch_size in [1, 32] {
        let batch = source_batch(
            &[(0, 0, 0), (1, 0, 1), (2, 1, 0), (3, 1, 1), (4, 1, i32::MAX)],
            false,
            false,
        )?;
        let schema = batch.schema();
        let key_order = PhysicalSortExpr {
            expr: col("k", &schema)?,
            options: SortOptions::default(),
        };
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: col("p", &schema)?,
                options: SortOptions::default(),
            },
            key_order.clone(),
        ])
        .ok_or("empty order")?;
        let batches = (0..batch.num_rows())
            .step_by(batch_size)
            .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
            .collect::<Vec<_>>();
        let source = Arc::new(
            TestMemoryExec::try_new(&[batches], schema.clone(), None)?
                .try_with_sort_information(vec![ordering])?,
        );
        let input: Arc<dyn ExecutionPlan> = Arc::new(TestMemoryExec::update_cache(&source));
        let mut windows = Vec::new();
        for (function, name) in [
            (WindowFunctionDefinition::AggregateUDF(count_udaf()), "n"),
            (
                WindowFunctionDefinition::WindowUDF(last_value_udwf()),
                "last_id",
            ),
        ] {
            windows.push(create_window_expr(
                &function,
                name.into(),
                &[col("id", &schema)?],
                &[col("p", &schema)?],
                std::slice::from_ref(&key_order),
                Arc::new(
                    WindowFrame::new_bounds(
                        WindowFrameUnits::Range,
                        WindowFrameBound::CurrentRow,
                        WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
                    )
                    .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true }),
                ),
                schema.clone(),
                false,
                false,
                None,
            )?);
        }
        let plan =
            get_best_fitting_window(&windows, &input, &[])?.ok_or("fitting window missing")?;
        assert_eq!(plan.fetch(), None);
        let native = count_window(
            schema.clone(),
            WindowFrameUnits::Range,
            WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
            SortOptions::default(),
        )?;
        let native_plan = get_best_fitting_window(&[native], &input, &[])?
            .ok_or("native fitting window missing")?;
        assert!(native_plan.with_fetch(Some(3)).is_none());
        let limited = plan.with_fetch(Some(3)).ok_or("output fetch unsupported")?;
        assert_eq!(limited.fetch(), Some(3));
        assert!(!limited.supports_limit_pushdown());
        let rebuilt = limited.clone().with_new_children(vec![input.clone()])?;
        assert_eq!(rebuilt.fetch(), Some(3));
        assert_eq!(
            rebuilt
                .with_fetch(None)
                .ok_or("fetch removal unsupported")?
                .fetch(),
            None
        );
        let requested: Arc<dyn ExecutionPlan> =
            Arc::new(GlobalLimitExec::new(plan.clone(), 1, Some(2)));
        let limited = LimitPushdown::new().optimize(requested, &ConfigOptions::default())?;
        assert_eq!(limited.children()[0].fetch(), Some(3));
        let batches =
            datafusion::physical_plan::collect(limited, Arc::new(TaskContext::default())).await?;
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                rows.push((
                    ScalarValue::try_from_array(batch.column(0), row)?,
                    ScalarValue::try_from_array(batch.column(3), row)?,
                    ScalarValue::try_from_array(batch.column(4), row)?,
                ));
            }
        }
        assert_eq!(
            rows,
            vec![
                (
                    ScalarValue::Int32(Some(1)),
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Int32(Some(1))
                ),
                (
                    ScalarValue::Int32(Some(2)),
                    ScalarValue::Int64(Some(2)),
                    ScalarValue::Int32(Some(3))
                ),
            ]
        );
        let sorted: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(
                LexOrdering::new(vec![PhysicalSortExpr {
                    expr: col("n", &plan.schema())?,
                    options: SortOptions {
                        descending: true,
                        nulls_first: false,
                    },
                }])
                .ok_or("empty result order")?,
                plan.clone(),
            )
            .with_fetch(Some(1)),
        );
        let sorted = LimitPushdown::new().optimize(sorted, &ConfigOptions::default())?;
        assert_eq!(sorted.children()[0].fetch(), None);
        let error = datafusion::physical_plan::collect(sorted, Arc::new(TaskContext::default()))
            .await
            .expect_err("sorting by the window result requires the later MAX+1 output");
        assert!(
            error
                .to_string()
                .contains("[ARITHMETIC_OVERFLOW] RANGE comparison target")
        );
        let error = datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default()))
            .await
            .expect_err("the unlimited last output row requires MAX+1");
        assert!(
            error
                .to_string()
                .contains("[ARITHMETIC_OVERFLOW] RANGE comparison target")
        );
    }
    Ok(())
}

#[tokio::test]
async fn spark_finite_integer_bounded_batches_preserve_empty_frames_and_retraction() -> TestResult {
    for batch_size in [1, 32] {
        let batch = source_batch(
            &[
                (0, 0, -4),
                (1, 0, 0),
                (2, 0, 1),
                (3, 0, 1),
                (4, 0, 3),
                (5, 0, 7),
            ],
            false,
            false,
        )?;
        let schema = batch.schema();
        let key_order = PhysicalSortExpr {
            expr: col("k", &schema)?,
            options: SortOptions::default(),
        };
        let ordering = LexOrdering::new(vec![key_order.clone()]).ok_or("empty order")?;
        let batches = (0..batch.num_rows())
            .step_by(batch_size)
            .map(|offset| batch.slice(offset, batch_size.min(batch.num_rows() - offset)))
            .collect::<Vec<_>>();
        let source = Arc::new(
            TestMemoryExec::try_new(&[batches], schema.clone(), None)?
                .try_with_sort_information(vec![ordering])?,
        );
        let input: Arc<dyn ExecutionPlan> = Arc::new(TestMemoryExec::update_cache(&source));
        let mut windows = Vec::new();
        for (function, name) in [
            (WindowFunctionDefinition::AggregateUDF(count_udaf()), "n"),
            (
                WindowFunctionDefinition::WindowUDF(first_value_udwf()),
                "first_id",
            ),
            (
                WindowFunctionDefinition::WindowUDF(last_value_udwf()),
                "last_id",
            ),
        ] {
            windows.push(create_window_expr(
                &function,
                name.into(),
                &[col("id", &schema)?],
                &[],
                std::slice::from_ref(&key_order),
                Arc::new(
                    WindowFrame::new_bounds(
                        WindowFrameUnits::Range,
                        WindowFrameBound::Preceding(ScalarValue::Int32(Some(i32::MIN))),
                        WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
                    )
                    .with_range_offset_arithmetic(
                        RangeOffsetArithmetic::Spark { ansi_mode: false },
                    ),
                ),
                schema.clone(),
                false,
                false,
                None,
            )?);
        }
        let plan =
            get_best_fitting_window(&windows, &input, &[])?.ok_or("fitting window missing")?;
        assert!(plan.downcast_ref::<BoundedWindowAggExec>().is_some());
        let batches =
            datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default())).await?;
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                rows.push((
                    ScalarValue::try_from_array(batch.column(3), row)?,
                    ScalarValue::try_from_array(batch.column(4), row)?,
                    ScalarValue::try_from_array(batch.column(5), row)?,
                ));
            }
        }
        assert_eq!(
            rows,
            [
                (0, None, None),
                (3, Some(1), Some(3)),
                (3, Some(1), Some(3)),
                (3, Some(1), Some(3)),
                (4, Some(1), Some(4)),
                (5, Some(1), Some(5)),
            ]
            .into_iter()
            .map(|(count, first, last)| (
                ScalarValue::Int64(Some(count)),
                ScalarValue::Int32(first),
                ScalarValue::Int32(last),
            ))
            .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[test]
fn spark_finite_integer_planning_declines_reversal_and_result_ordering() -> TestResult {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
    let key_order = PhysicalSortExpr {
        expr: col("k", &schema)?,
        options: SortOptions::default(),
    };
    for arithmetic in [
        RangeOffsetArithmetic::DataFusion,
        RangeOffsetArithmetic::Spark { ansi_mode: false },
        RangeOffsetArithmetic::Spark { ansi_mode: true },
    ] {
        let window = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(max_udaf()),
            "mx".into(),
            &[col("k", &schema)?],
            &[],
            std::slice::from_ref(&key_order),
            Arc::new(
                WindowFrame::new_bounds(
                    WindowFrameUnits::Range,
                    WindowFrameBound::Preceding(ScalarValue::Int32(Some(i32::MAX))),
                    WindowFrameBound::Preceding(ScalarValue::Int32(Some(i32::MAX - 1))),
                )
                .with_range_offset_arithmetic(arithmetic),
            ),
            schema.clone(),
            false,
            false,
            None,
        )?;
        for reverse in [false, true] {
            let source_order = if reverse {
                key_order.reverse()
            } else {
                key_order.clone()
            };
            let input: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
                schema.clone(),
                vec![],
                None,
                Some(LexOrdering::new(vec![source_order]).ok_or("empty order")?),
                false,
                None,
            )?);
            let fitting = get_best_fitting_window(std::slice::from_ref(&window), &input, &[])?;
            assert_eq!(
                fitting.is_some(),
                !reverse || arithmetic == RangeOffsetArithmetic::DataFusion
            );
            if !reverse {
                assert!(
                    fitting
                        .ok_or("matching order rejected")?
                        .downcast_ref::<BoundedWindowAggExec>()
                        .is_some()
                );
                let plan = WindowAggExec::try_new(vec![window.clone()], input, false)?;
                let inferred = plan
                    .properties()
                    .equivalence_properties()
                    .ordering_satisfy([PhysicalSortExpr {
                        expr: col("mx", &plan.schema())?,
                        options: SortOptions {
                            descending: false,
                            nulls_first: false,
                        },
                    }])?;
                assert_eq!(inferred, arithmetic == RangeOffsetArithmetic::DataFusion);
            }
        }
    }
    Ok(())
}

#[test]
fn spark_finite_integer_fetch_properties_follow_the_limit_and_current_input() -> TestResult {
    let batch = source_batch(&[(0, 0, 0), (1, 0, 1), (2, 1, 0)], false, false)?;
    let schema = batch.schema();
    let key_order = PhysicalSortExpr {
        expr: col("k", &schema)?,
        options: SortOptions::default(),
    };
    let ordering = LexOrdering::new(vec![
        PhysicalSortExpr {
            expr: col("p", &schema)?,
            options: SortOptions::default(),
        },
        key_order.clone(),
    ])
    .ok_or("empty input ordering")?;
    let unbounded: Arc<dyn ExecutionPlan> = Arc::new(StreamingTableExec::try_new(
        schema.clone(),
        vec![Arc::new(PendingIntegerRangeInput(batch.clone()))],
        None,
        Some(ordering.clone()),
        true,
        None,
    )?);
    let source = Arc::new(
        TestMemoryExec::try_new(&[vec![batch]], schema.clone(), None)?
            .try_with_sort_information(vec![ordering])?,
    );
    let bounded: Arc<dyn ExecutionPlan> = Arc::new(TestMemoryExec::update_cache(&source));
    let window = create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "n".into(),
        &[col("id", &schema)?],
        &[col("p", &schema)?],
        &[key_order],
        Arc::new(
            WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::CurrentRow,
                WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
            )
            .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true }),
        ),
        schema,
        false,
        false,
        None,
    )?;
    let plan =
        get_best_fitting_window(&[window], &unbounded, &[])?.ok_or("fitting window missing")?;
    assert_eq!(plan.fetch(), None);
    assert_eq!(plan.boundedness(), unbounded.boundedness());
    assert!(plan.boundedness().is_unbounded());
    assert!(matches!(
        plan.cardinality_effect(),
        CardinalityEffect::Equal
    ));

    let fetched = plan.with_fetch(Some(1)).ok_or("fetch unsupported")?;
    assert_eq!(fetched.boundedness(), Boundedness::Bounded);
    assert!(matches!(
        fetched.cardinality_effect(),
        CardinalityEffect::LowerEqual
    ));
    assert_eq!(fetched.output_ordering(), plan.output_ordering());
    assert_eq!(fetched.pipeline_behavior(), plan.pipeline_behavior());
    assert_eq!(fetched.clone().boundedness(), Boundedness::Bounded);
    let cleared = fetched.with_fetch(None).ok_or("clear fetch unsupported")?;
    assert_eq!(cleared.boundedness(), unbounded.boundedness());
    assert!(matches!(
        cleared.cardinality_effect(),
        CardinalityEffect::Equal
    ));

    let same = fetched.clone().with_new_children(vec![unbounded.clone()])?;
    assert_eq!(same.fetch(), Some(1));
    assert_eq!(same.boundedness(), Boundedness::Bounded);
    assert_eq!(
        same.with_fetch(None)
            .ok_or("clear same-child fetch")?
            .boundedness(),
        unbounded.boundedness()
    );

    let changed = fetched.with_new_children(vec![bounded.clone()])?;
    assert_eq!(changed.fetch(), Some(1));
    assert_eq!(changed.boundedness(), Boundedness::Bounded);
    assert_eq!(
        changed
            .with_fetch(None)
            .ok_or("clear changed-child fetch")?
            .boundedness(),
        bounded.boundedness()
    );
    let restored = changed.with_new_children(vec![unbounded.clone()])?;
    assert_eq!(restored.fetch(), Some(1));
    assert_eq!(restored.boundedness(), Boundedness::Bounded);
    assert_eq!(
        restored
            .with_fetch(None)
            .ok_or("clear restored-child fetch")?
            .boundedness(),
        unbounded.boundedness()
    );

    let default_rebuilt = plan.with_new_children(vec![bounded.clone()])?;
    assert_eq!(default_rebuilt.fetch(), None);
    assert_eq!(default_rebuilt.boundedness(), bounded.boundedness());
    assert!(matches!(
        default_rebuilt.cardinality_effect(),
        CardinalityEffect::Equal
    ));
    Ok(())
}
