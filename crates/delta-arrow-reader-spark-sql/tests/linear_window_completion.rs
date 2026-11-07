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
use datafusion::functions_window::nth_value::{first_value_udwf, nth_value_udwf};
use datafusion::logical_expr::window_state::PartitionBatchState;
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
