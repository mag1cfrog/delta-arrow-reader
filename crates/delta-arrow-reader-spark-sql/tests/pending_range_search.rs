//! Compare resumable Spark RANGE searches with the unchanged aggregate evaluator.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::logical_expr::window_frame::RangeOffsetArithmetic;
use datafusion::logical_expr::window_state::PartitionBatchState;
use datafusion::logical_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::window::{
    PartitionBatches, PartitionWindowAggStates, WindowExpr, reuse_pending_spark_range_search,
};
use datafusion::physical_plan::windows::create_window_expr;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct PendingRangeCase {
    name: &'static str,
    keys: &'static [Option<i32>],
    start: WindowFrameBound,
    end: WindowFrameBound,
    nulls_first: bool,
    ansi_error: bool,
}

include!("fixtures/pending-range-1099-inputs.rs");

fn batch(keys: &[Option<i32>], start: usize) -> TestResult<RecordBatch> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from_iter_values(
            (start..start + keys.len()).map(|id| id as i32),
        )),
        Arc::new(Int32Array::from(keys.to_vec())),
    ];
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("k", DataType::Int32, true),
        ])),
        columns,
    )?)
}

fn count_window(
    case: &PendingRangeCase,
    arithmetic: RangeOffsetArithmetic,
) -> TestResult<Arc<dyn WindowExpr>> {
    let schema = batch(&[], 0)?.schema();
    let key = col("k", &schema)?;
    Ok(create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(count_udaf()),
        "n".into(),
        &[col("id", &schema)?],
        &[],
        &[PhysicalSortExpr {
            expr: key,
            options: SortOptions {
                descending: false,
                nulls_first: case.nulls_first,
            },
        }],
        Arc::new(
            WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                case.start.clone(),
                case.end.clone(),
            )
            .with_range_offset_arithmetic(arithmetic),
        ),
        schema,
        false,
        false,
        None,
    )?)
}

fn compare_states(states: &[PartitionWindowAggStates; 2]) -> TestResult<Vec<i64>> {
    let left = &states[0].get(&vec![]).ok_or("missing resumed state")?.state;
    let right = &states[1]
        .get(&vec![])
        .ok_or("missing baseline state")?
        .state;
    assert_eq!(left.window_frame_range, right.window_frame_range);
    assert_eq!(left.last_calculated_index, right.last_calculated_index);
    assert_eq!(left.n_row_result_missing, right.n_row_result_missing);
    assert_eq!(left.offset_pruned_rows, right.offset_pruned_rows);
    assert_eq!(left.is_end, right.is_end);
    assert_eq!(left.out_col.to_data(), right.out_col.to_data());
    let values = left
        .out_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("count type")?;
    assert_eq!(values.null_count(), 0);
    Ok(values.values().to_vec())
}

fn compare_retries(
    case: &PendingRangeCase,
    arithmetic: RangeOffsetArithmetic,
    batch_size: usize,
    prune: bool,
) -> TestResult<bool> {
    let baseline = count_window(case, arithmetic)?;
    let windows = [
        reuse_pending_spark_range_search(Arc::clone(&baseline)),
        baseline,
    ];
    assert_eq!(windows[0].field()?, windows[1].field()?);
    let mut buffers = [PartitionBatches::default(), PartitionBatches::default()];
    let mut states = [
        PartitionWindowAggStates::default(),
        PartitionWindowAggStates::default(),
    ];
    for start in (0..case.keys.len()).step_by(batch_size) {
        let end = (start + batch_size).min(case.keys.len());
        let input = batch(&case.keys[start..end], start)?;
        for buffer in &mut buffers {
            let partition = buffer
                .entry(vec![])
                .or_insert_with(|| PartitionBatchState::new(input.schema()));
            partition.extend(&input)?;
            partition.set_most_recent_row(input.slice(input.num_rows() - 1, 1));
        }
        // Retry unchanged input too, with accumulator state still uncommitted for pending rows.
        for _ in 0..2 {
            let errors = std::array::from_fn::<_, 2, _>(|index| {
                windows[index]
                    .evaluate_stateful(&buffers[index], &mut states[index])
                    .err()
                    .map(|error| error.to_string())
            });
            assert_eq!(errors[0], errors[1], "{} at append {end}", case.name);
            if let Some(error) = &errors[0] {
                assert!(error.contains("OVERFLOW"), "{error}");
                return Ok(true);
            }
            let emitted = compare_states(&states)?;
            if case.name == "ordinary_following" {
                assert_eq!(
                    emitted,
                    if end == case.keys.len() {
                        vec![4; 4]
                    } else {
                        vec![]
                    }
                );
                assert!(!states[0][&vec![]].state.is_end);
                if end < case.keys.len() {
                    assert_eq!(states[0][&vec![]].state.window_frame_range, 0..0);
                }
            }
        }
        if prune {
            for (buffer, states) in buffers.iter_mut().zip(&mut states) {
                let state = &mut states.get_mut(&vec![]).ok_or("missing prune state")?.state;
                let count = state
                    .window_frame_range
                    .start
                    .min(state.last_calculated_index);
                if count != 0 {
                    let partition = buffer.get_mut(&vec![]).ok_or("missing prune buffer")?;
                    partition.record_batch = partition
                        .record_batch
                        .slice(count, partition.record_batch.num_rows() - count);
                    state.prune_state(count);
                }
            }
        }
    }
    for (window, (buffer, states)) in windows.iter().zip(buffers.iter_mut().zip(&mut states)) {
        buffer.get_mut(&vec![]).ok_or("missing EOF buffer")?.is_end = true;
        window.evaluate_stateful(buffer, states)?;
    }
    let emitted = compare_states(&states)?;
    assert_eq!(emitted.len(), case.keys.len());
    if case.name == "ordinary_following" {
        assert_eq!(emitted, [4, 4, 4, 4, 5]);
    }
    Ok(false)
}

#[test]
fn resumed_range_search_matches_baseline_across_appends_and_pruning() -> TestResult {
    for case in CASES {
        for ansi_mode in [false, true] {
            for batch_size in [1, 2, 3] {
                for prune in [false, true] {
                    assert_eq!(
                        compare_retries(
                            case,
                            RangeOffsetArithmetic::Spark { ansi_mode },
                            batch_size,
                            prune
                        )?,
                        ansi_mode && case.ansi_error,
                        "{} ANSI {ansi_mode} batch {batch_size} prune {prune}",
                        case.name
                    );
                }
            }
        }
        // Native target arithmetic stays on its existing route, including overflow edges.
        for batch_size in [1, 3] {
            assert!(!compare_retries(
                case,
                RangeOffsetArithmetic::DataFusion,
                batch_size,
                true
            )?);
        }
    }
    Ok(())
}
