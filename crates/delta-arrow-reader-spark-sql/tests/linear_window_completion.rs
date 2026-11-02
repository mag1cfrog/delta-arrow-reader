//! Stateful completion with the same global-row guarantee as LinearSearch.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, ListArray, StructArray, UInt32Array};
use arrow::compute::{SortOptions, take_record_batch};
use arrow::datatypes::{Field, Int32Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::logical_expr::window_state::PartitionBatchState;
use datafusion::logical_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::window::{PartitionBatches, PartitionWindowAggStates, WindowExpr};
use datafusion::physical_plan::windows::create_window_expr;

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
