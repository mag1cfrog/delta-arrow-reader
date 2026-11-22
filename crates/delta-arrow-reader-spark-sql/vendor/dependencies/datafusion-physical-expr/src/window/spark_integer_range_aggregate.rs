// Modified for the DAR Spark SQL runtime. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Resume deferred searches for bounded Spark signed-integer RANGE aggregates.

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{ArrayRef, new_empty_array};
use arrow::datatypes::FieldRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, internal_err};
use datafusion_expr::window_state::{PartitionBatchState, WindowAggState, WindowFrameContext};
use datafusion_expr::{Accumulator, PartitionEvaluator, WindowFrame};
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

use crate::PhysicalExpr;
use crate::window::window_expr::{
    AggregateWindowExpr, WindowFn, get_orderby_values, is_end_bound_safe,
};
use crate::window::{
    PartitionBatches, PartitionWindowAggStates, PlainAggregateWindowExpr,
    SlidingAggregateWindowExpr, WindowExpr, WindowState,
};

/// Selects resumable search for a Spark aggregate over a signed-integer ORDER BY key.
/// The planner checks the key type before calling this function.
pub fn reuse_pending_spark_range_search(expression: Arc<dyn WindowExpr>) -> Arc<dyn WindowExpr> {
    let frame = expression.get_window_frame();
    if frame.has_nonzero_spark_integer_range_offset()
        && !frame.has_null_offset()
        && expression.uses_bounded_memory()
        && (expression.as_any().is::<PlainAggregateWindowExpr>()
            || expression.as_any().is::<SlidingAggregateWindowExpr>())
    {
        Arc::new(SparkIntegerRangeAggregate { expression })
    } else {
        expression
    }
}

#[derive(Debug)]
struct SparkIntegerRangeAggregate {
    expression: Arc<dyn WindowExpr>,
}

impl WindowExpr for SparkIntegerRangeAggregate {
    fn as_any(&self) -> &dyn Any {
        // The usual aggregate ordering and limit rules still apply.
        self.expression.as_any()
    }

    fn field(&self) -> Result<FieldRef> {
        self.expression.field()
    }

    fn name(&self) -> &str {
        self.expression.name()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.expression.expressions()
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        self.expression.evaluate(batch)
    }

    fn evaluate_stateful(
        &self,
        batches: &PartitionBatches,
        states: &mut PartitionWindowAggStates,
    ) -> Result<()> {
        let expression = self.expression.as_any();
        if let Some(plain) = expression.downcast_ref::<PlainAggregateWindowExpr>() {
            evaluate_partitions(plain, batches, states)?;
            if plain.get_window_frame().start_bound.is_unbounded() {
                for key in batches.keys() {
                    let range = &mut states
                        .get_mut(key)
                        .ok_or_else(|| exec_datafusion_err!("Cannot find state"))?
                        .state
                        .window_frame_range;
                    range.start = range.end.saturating_sub(1);
                }
            }
            Ok(())
        } else if let Some(sliding) = expression.downcast_ref::<SlidingAggregateWindowExpr>() {
            evaluate_partitions(sliding, batches, states)
        } else {
            internal_err!("Spark RANGE search requires an aggregate window")
        }
    }

    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>] {
        self.expression.partition_by()
    }

    fn order_by(&self) -> &[PhysicalSortExpr] {
        self.expression.order_by()
    }

    fn get_window_frame(&self) -> &Arc<WindowFrame> {
        self.expression.get_window_frame()
    }

    fn uses_bounded_memory(&self) -> bool {
        self.expression.uses_bounded_memory()
    }

    fn get_reverse_expr(&self) -> Option<Arc<dyn WindowExpr>> {
        self.expression
            .get_reverse_expr()
            .map(reuse_pending_spark_range_search)
    }

    fn create_window_fn(&self) -> Result<WindowFn> {
        self.expression.create_window_fn()
    }

    fn with_new_expressions(
        &self,
        args: Vec<Arc<dyn PhysicalExpr>>,
        partition_by: Vec<Arc<dyn PhysicalExpr>>,
        order_by: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<dyn WindowExpr>> {
        self.expression
            .with_new_expressions(args, partition_by, order_by)
            .map(reuse_pending_spark_range_search)
    }
}

#[derive(Debug)]
struct SparkRangePartitionEvaluator {
    accumulator: Box<dyn Accumulator>,
    pending: Option<PendingRangeSearch>,
}

// The specialized expression owns this evaluator through the existing per-partition slot.
impl PartitionEvaluator for SparkRangePartitionEvaluator {}

#[derive(Debug)]
struct PendingRangeSearch {
    row: usize,
    length: usize,
    pruned_rows: usize,
    committed: Range<usize>,
    searched: Range<usize>,
}

fn evaluate_partitions(
    aggregate: &impl AggregateWindowExpr,
    batches: &PartitionBatches,
    states: &mut PartitionWindowAggStates,
) -> Result<()> {
    let field = aggregate.field()?;
    for (key, batch) in batches {
        if !states.contains_key(key) {
            let accumulator = aggregate.get_accumulator()?;
            states.insert(
                key.clone(),
                WindowState {
                    state: WindowAggState::new(field.data_type())?,
                    window_fn: WindowFn::Builtin(Box::new(SparkRangePartitionEvaluator {
                        accumulator,
                        pending: None,
                    })),
                },
            );
        }
        let window = states
            .get_mut(key)
            .ok_or_else(|| exec_datafusion_err!("Cannot find state"))?;
        let WindowFn::Builtin(evaluator) = &mut window.window_fn else {
            return internal_err!("Spark RANGE partition evaluator is missing");
        };
        let evaluator = (&mut **evaluator as &mut dyn Any)
            .downcast_mut::<SparkRangePartitionEvaluator>()
            .ok_or_else(|| exec_datafusion_err!("Incorrect Spark RANGE partition evaluator"))?;
        let state = &mut window.state;
        let result = if batch
            .output_end_index
            .is_some_and(|end| state.last_calculated_index >= end)
        {
            new_empty_array(field.data_type())
        } else {
            evaluate_pending_frames(aggregate, batch, state, evaluator)?
        };
        state.update(&result, batch)?;
    }
    Ok(())
}

fn evaluate_pending_frames(
    aggregate: &impl AggregateWindowExpr,
    batch: &PartitionBatchState,
    state: &mut WindowAggState,
    evaluator: &mut SparkRangePartitionEvaluator,
) -> Result<ArrayRef> {
    let record_batch = &batch.record_batch;
    let values = aggregate.evaluate_args(record_batch)?;
    let mask = aggregate
        .filter_expr()
        .map(|filter| {
            filter
                .evaluate(record_batch)?
                .into_array(record_batch.num_rows())
        })
        .transpose()?;
    let filter_mask = mask.as_deref().map(as_boolean_array).transpose()?;
    let order = get_orderby_values(aggregate.order_by_columns(record_batch)?);
    let most_recent_order = batch
        .most_recent_row
        .as_ref()
        .map(|row| aggregate.order_by_columns(row).map(get_orderby_values))
        .transpose()?;
    let frame = state.window_frame_ctx.get_or_insert_with(|| {
        let options = aggregate
            .order_by()
            .iter()
            .map(|order| order.options)
            .collect();
        WindowFrameContext::new(Arc::clone(aggregate.get_window_frame()), options)
    });

    let length = values[0].len();
    let output_end = batch.output_end_index.unwrap_or(length).min(length);
    let mut rows = Vec::new();
    for row in state.last_calculated_index..output_end {
        let committed = &state.window_frame_range;
        // A deferred row has already compared the immutable buffered prefix.
        // Pruning or moving to a different row invalidates the saved indices.
        let pending = evaluator.pending.take();
        let search_from = pending
            .as_ref()
            .filter(|pending| {
                pending.row == row
                    && pending.pruned_rows == state.offset_pruned_rows
                    && pending.length <= length
                    && pending.committed == *committed
            })
            .map_or(committed, |pending| &pending.searched);
        let range = frame.calculate_range(&order, search_from, length, row)?;
        if range.end == length
            && !batch.is_end
            && !is_end_bound_safe(
                frame,
                &order,
                most_recent_order.as_deref(),
                aggregate.order_by(),
                row,
            )?
        {
            evaluator.pending = Some(PendingRangeSearch {
                row,
                length,
                pruned_rows: state.offset_pruned_rows,
                committed: committed.clone(),
                searched: range,
            });
            break;
        }
        // Keep accumulator updates based on the committed range.
        rows.push(aggregate.get_aggregate_result_inside_range(
            committed,
            &range,
            &values,
            &mut evaluator.accumulator,
            filter_mask,
        )?);
        state.window_frame_range = range;
    }
    if rows.is_empty() {
        Ok(new_empty_array(aggregate.field()?.data_type()))
    } else {
        ScalarValue::iter_to_array(rows)
    }
}
