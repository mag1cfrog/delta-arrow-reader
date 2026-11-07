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

//! Structures used to hold window function state (for implementing WindowUDFs)

use std::{collections::VecDeque, fmt, ops::Range, sync::Arc};

use crate::{WindowFrame, WindowFrameBound, WindowFrameUnits};

use arrow::compute::kernels::sort::{LexicographicalComparator, SortColumn};
use arrow::{
    array::ArrayRef,
    compute::{SortOptions, concat, concat_batches},
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion_common::{
    Result, ScalarValue, exec_datafusion_err, internal_datafusion_err, internal_err,
    utils::{compare_rows, get_row_at_idx, search_in_slice},
};

/// Holds the state of evaluating a window function
#[derive(Debug, Clone)]
pub struct WindowAggState {
    /// The range that we calculate the window function
    pub window_frame_range: Range<usize>,
    pub window_frame_ctx: Option<WindowFrameContext>,
    /// The index of the last row that its result is calculated inside the partition record batch buffer.
    pub last_calculated_index: usize,
    /// The offset of the deleted row number
    pub offset_pruned_rows: usize,
    /// Stores the results calculated by window frame
    pub out_col: ArrayRef,
    /// Keeps track of how many rows should be generated to be in sync with input record_batch.
    // (For each row in the input record batch we need to generate a window result).
    pub n_row_result_missing: usize,
    /// Flag indicating whether we have received all data for this partition
    pub is_end: bool,
}

impl WindowAggState {
    pub fn prune_state(&mut self, n_prune: usize) {
        self.window_frame_range = Range {
            start: self.window_frame_range.start - n_prune,
            end: self.window_frame_range.end - n_prune,
        };
        self.last_calculated_index -= n_prune;
        self.offset_pruned_rows += n_prune;

        match self.window_frame_ctx.as_mut() {
            // Rows have no state do nothing
            Some(WindowFrameContext::Rows(_)) => {}
            Some(WindowFrameContext::Range { state, .. }) => {
                state.peer_comparator = None;
            }
            Some(WindowFrameContext::Groups { state, .. }) => {
                let mut n_group_to_del = 0;
                for (_, end_idx) in &state.group_end_indices {
                    if n_prune < *end_idx {
                        break;
                    }
                    n_group_to_del += 1;
                }
                state.group_end_indices.drain(0..n_group_to_del);
                state
                    .group_end_indices
                    .iter_mut()
                    .for_each(|(_, start_idx)| *start_idx -= n_prune);
                state.current_group_idx -= n_group_to_del;
            }
            None => {}
        };
    }

    pub fn update(
        &mut self,
        out_col: &ArrayRef,
        partition_batch_state: &PartitionBatchState,
    ) -> Result<()> {
        self.last_calculated_index += out_col.len();
        // no need to use concat if the current `out_col` is empty
        if self.out_col.is_empty() {
            self.out_col = Arc::clone(out_col);
        } else {
            self.out_col = concat(&[&self.out_col, &out_col])?;
        }
        self.n_row_result_missing =
            partition_batch_state.record_batch.num_rows() - self.last_calculated_index;
        self.is_end = partition_batch_state.is_end;
        Ok(())
    }

    pub fn new(out_type: &DataType) -> Result<Self> {
        let empty_out_col = ScalarValue::try_from(out_type)?.to_array_of_size(0)?;
        Ok(Self {
            window_frame_range: Range { start: 0, end: 0 },
            window_frame_ctx: None,
            last_calculated_index: 0,
            offset_pruned_rows: 0,
            out_col: empty_out_col,
            n_row_result_missing: 0,
            is_end: false,
        })
    }
}

/// This object stores the window frame state for use in incremental calculations.
#[derive(Debug, Clone)]
pub enum WindowFrameContext {
    /// ROWS frames are inherently stateless.
    Rows(Arc<WindowFrame>),
    /// RANGE frames are stateful, they store indices specifying where the
    /// previous search left off. This amortizes the overall cost to O(n)
    /// where n denotes the row count.
    Range {
        window_frame: Arc<WindowFrame>,
        state: WindowFrameStateRange,
    },
    /// GROUPS frames are stateful, they store group boundaries and indices
    /// specifying where the previous search left off. This amortizes the
    /// overall cost to O(n) where n denotes the row count.
    Groups {
        window_frame: Arc<WindowFrame>,
        state: WindowFrameStateGroups,
    },
}

impl WindowFrameContext {
    /// Create a new state object for the given window frame.
    pub fn new(window_frame: Arc<WindowFrame>, sort_options: Vec<SortOptions>) -> Self {
        match window_frame.units {
            WindowFrameUnits::Rows => WindowFrameContext::Rows(window_frame),
            WindowFrameUnits::Range => WindowFrameContext::Range {
                window_frame,
                state: WindowFrameStateRange::new(sort_options),
            },
            WindowFrameUnits::Groups => WindowFrameContext::Groups {
                window_frame,
                state: WindowFrameStateGroups::default(),
            },
        }
    }

    /// This function calculates beginning/ending indices for the frame of the current row.
    pub fn calculate_range(
        &mut self,
        range_columns: &[ArrayRef],
        last_range: &Range<usize>,
        length: usize,
        idx: usize,
    ) -> Result<Range<usize>> {
        match self {
            WindowFrameContext::Rows(window_frame) => {
                Self::calculate_range_rows(window_frame, length, idx)
            }
            // Sort options is used in RANGE mode calculations because the
            // ordering or position of NULLs impact range calculations and
            // comparison of rows.
            WindowFrameContext::Range {
                window_frame,
                state,
            } => state.calculate_range(
                window_frame,
                last_range,
                range_columns,
                length,
                idx,
            ),
            // Sort options is not used in GROUPS mode calculations as the
            // inequality of two rows indicates a group change, and ordering
            // or position of NULLs do not impact inequality.
            WindowFrameContext::Groups {
                window_frame,
                state,
            } => state.calculate_range(window_frame, range_columns, length, idx),
        }
    }

    /// This function calculates beginning/ending indices for the frame of the current row.
    fn calculate_range_rows(
        window_frame: &Arc<WindowFrame>,
        length: usize,
        idx: usize,
    ) -> Result<Range<usize>> {
        let start = match window_frame.start_bound {
            // UNBOUNDED PRECEDING
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)) => 0,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n))) => {
                idx.saturating_sub(n as usize)
            }
            WindowFrameBound::CurrentRow => idx,
            // UNBOUNDED FOLLOWING
            WindowFrameBound::Following(ScalarValue::UInt64(None)) => {
                return internal_err!(
                    "Frame start cannot be UNBOUNDED FOLLOWING '{window_frame:?}'"
                );
            }
            WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                std::cmp::min(idx + n as usize, length)
            }
            // ERRONEOUS FRAMES
            WindowFrameBound::Preceding(_)
            | WindowFrameBound::Following(_)
            | WindowFrameBound::PrecedingNullOffset(_)
            | WindowFrameBound::FollowingNullOffset(_) => {
                return internal_err!("Rows should be UInt64");
            }
        };
        let end = match window_frame.end_bound {
            // UNBOUNDED PRECEDING
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)) => {
                return internal_err!(
                    "Frame end cannot be UNBOUNDED PRECEDING '{window_frame:?}'"
                );
            }
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(n))) => {
                if idx >= n as usize {
                    idx - n as usize + 1
                } else {
                    0
                }
            }
            WindowFrameBound::CurrentRow => idx + 1,
            // UNBOUNDED FOLLOWING
            WindowFrameBound::Following(ScalarValue::UInt64(None)) => length,
            WindowFrameBound::Following(ScalarValue::UInt64(Some(n))) => {
                std::cmp::min(idx + n as usize + 1, length)
            }
            // ERRONEOUS FRAMES
            WindowFrameBound::Preceding(_)
            | WindowFrameBound::Following(_)
            | WindowFrameBound::PrecedingNullOffset(_)
            | WindowFrameBound::FollowingNullOffset(_) => {
                return internal_err!("Rows should be UInt64");
            }
        };
        Ok(Range { start, end })
    }
}

/// State for each unique partition determined according to PARTITION BY column(s)
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionBatchState {
    /// The record batch belonging to current partition
    pub record_batch: RecordBatch,
    /// The record batch that contains the most recent row at the input.
    /// Please note that this batch doesn't necessarily have the same partitioning
    /// with `record_batch`. Keeping track of this batch enables us to prune
    /// `record_batch` when cardinality of the partition is sparse.
    pub most_recent_row: Option<RecordBatch>,
    /// Flag indicating whether we have received all data for this partition
    pub is_end: bool,
    /// Number of rows emitted for each partition
    pub n_out_row: usize,
}

impl PartitionBatchState {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            record_batch: RecordBatch::new_empty(schema),
            most_recent_row: None,
            is_end: false,
            n_out_row: 0,
        }
    }

    pub fn new_with_batch(batch: RecordBatch) -> Self {
        Self {
            record_batch: batch,
            most_recent_row: None,
            is_end: false,
            n_out_row: 0,
        }
    }

    pub fn extend(&mut self, batch: &RecordBatch) -> Result<()> {
        self.record_batch =
            concat_batches(&self.record_batch.schema(), [&self.record_batch, batch])?;
        Ok(())
    }

    pub fn set_most_recent_row(&mut self, batch: RecordBatch) {
        // It is enough for the batch to contain only a single row (the rest
        // are not necessary).
        self.most_recent_row = Some(batch);
    }
}

/// This structure encapsulates all the state information we require as we scan
/// ranges of data while processing RANGE frames.
/// Attribute `sort_options` stores the column ordering specified by the ORDER
/// BY clause. This information is used to calculate the range.
#[derive(Default, Clone)]
pub struct WindowFrameStateRange {
    sort_options: Vec<SortOptions>,
    peer_comparator: Option<(Vec<ArrayRef>, Arc<LexicographicalComparator>)>,
}

impl fmt::Debug for WindowFrameStateRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowFrameStateRange")
            .field("sort_options", &self.sort_options)
            .finish_non_exhaustive()
    }
}

/// Whether a nonzero INT day bound can wrap a Date32 RANGE key.
pub fn date32_range_can_wrap(
    data_type: &DataType,
    units: WindowFrameUnits,
    bound: &WindowFrameBound,
) -> bool {
    data_type == &DataType::Date32
        && units == WindowFrameUnits::Range
        && matches!(
            bound,
            WindowFrameBound::Preceding(ScalarValue::Int32(Some(offset)))
                | WindowFrameBound::Following(ScalarValue::Int32(Some(offset))) if *offset != 0
        )
}

/// Calculates the comparison target for a Date32 key and signed INT day offset.
/// Spark's DateAdd wraps at the i32 boundary in both ANSI modes.
/// Returns None for other scalar pairs and preserves NULL date values.
pub fn date32_range_target(
    value: &ScalarValue,
    delta: &ScalarValue,
    add_offset: bool,
) -> Option<ScalarValue> {
    match (value, delta) {
        (ScalarValue::Date32(days), ScalarValue::Int32(Some(offset))) => {
            Some(ScalarValue::Date32(days.map(|days| {
                if add_offset {
                    days.wrapping_add(*offset)
                } else {
                    days.wrapping_sub(*offset)
                }
            })))
        }
        _ => None,
    }
}

/// Computes a RANGE comparison target using the current ORDER BY key type.
/// `None` denotes a native unbounded bound or the existing checked-arithmetic
/// overflow edge. Membership retains its directional edge; completion must wait.
pub fn range_bound_target(
    current_key: &ScalarValue,
    bound: &WindowFrameBound,
    descending: bool,
) -> Result<Option<ScalarValue>> {
    let (delta, preceding) = match bound {
        WindowFrameBound::Preceding(delta) | WindowFrameBound::Following(delta)
            if delta.is_null() =>
        {
            return Ok(None);
        }
        WindowFrameBound::PrecedingNullOffset(_)
        | WindowFrameBound::FollowingNullOffset(_) => {
            return ScalarValue::try_from(&current_key.data_type()).map(Some);
        }
        WindowFrameBound::CurrentRow => return Ok(Some(current_key.clone())),
        WindowFrameBound::Preceding(delta) => (delta, true),
        WindowFrameBound::Following(delta) => (delta, false),
    };
    if current_key.is_null() {
        return Ok(Some(current_key.clone()));
    }
    let add_offset = preceding == descending;
    if let Some(target) = date32_range_target(current_key, delta, add_offset) {
        Ok(Some(target))
    } else if add_offset {
        Ok(current_key.add_checked(delta).ok())
    } else if current_key.is_unsigned() && current_key < delta {
        current_key.sub(current_key).map(Some)
    } else {
        Ok(current_key.sub_checked(delta).ok())
    }
}

impl WindowFrameStateRange {
    /// Create a new object to store the search state.
    fn new(sort_options: Vec<SortOptions>) -> Self {
        Self {
            sort_options,
            peer_comparator: None,
        }
    }

    /// This function calculates beginning/ending indices for the frame of the current row.
    // Argument `last_range` stores the resulting indices from the previous search. Since the indices only
    // advance forward, we start from `last_range` subsequently. Thus, the overall
    // time complexity of linear search amortizes to O(n) where n denotes the total
    // row count.
    fn calculate_range(
        &mut self,
        window_frame: &Arc<WindowFrame>,
        last_range: &Range<usize>,
        range_columns: &[ArrayRef],
        length: usize,
        idx: usize,
    ) -> Result<Range<usize>> {
        let date32_wrapping_bound = range_columns.first().is_some_and(|column| {
            [&window_frame.start_bound, &window_frame.end_bound]
                .iter()
                .any(|bound| {
                    date32_range_can_wrap(column.data_type(), window_frame.units, bound)
                })
        });
        if date32_wrapping_bound || window_frame.has_null_offset() {
            let end = self.calculate_index_of_bound::<false>(
                window_frame,
                &window_frame.end_bound, range_columns, last_range, idx, length,
            )?;
            // Spark advances the lower cursor only over input consumed by the upper cursor.
            let start = self.calculate_index_of_bound::<true>(
                window_frame,
                &window_frame.start_bound, range_columns, last_range, idx, end,
            )?;
            return Ok(Range { start, end });
        }
        let start = self.calculate_index_of_bound::<true>(
            window_frame,
            &window_frame.start_bound, range_columns, last_range, idx, length,
        )?;
        let end = self.calculate_index_of_bound::<false>(
            window_frame,
            &window_frame.end_bound, range_columns, last_range, idx, length,
        )?;
        Ok(Range { start, end })
    }

    fn calculate_index_of_bound<const SIDE: bool>(
        &mut self,
        window_frame: &WindowFrame,
        bound: &WindowFrameBound,
        range_columns: &[ArrayRef],
        last_range: &Range<usize>,
        idx: usize,
        length: usize,
    ) -> Result<usize> {
        match bound {
            WindowFrameBound::Preceding(n) if SIDE && n.is_null() => Ok(0),
            WindowFrameBound::Following(n) if !SIDE && n.is_null() => Ok(length),
            WindowFrameBound::Preceding(_)
            | WindowFrameBound::PrecedingNullOffset(_) => self.calculate_index_of_row::<SIDE, true>(
                window_frame,
                range_columns,
                last_range,
                idx,
                bound,
                length,
            ),
            WindowFrameBound::Following(_)
            | WindowFrameBound::FollowingNullOffset(_) => self.calculate_index_of_row::<SIDE, false>(
                window_frame,
                range_columns,
                last_range,
                idx,
                bound,
                length,
            ),
            WindowFrameBound::CurrentRow => self.calculate_index_of_row::<SIDE, false>(
                window_frame,
                range_columns,
                last_range,
                idx,
                bound,
                length,
            ),
        }
    }

    /// This function does the heavy lifting when finding range boundaries. It is meant to be
    /// called twice, in succession, to get window frame start and end indices (with `SIDE`
    /// supplied as true and false, respectively).
    fn calculate_index_of_row<const SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        window_frame: &WindowFrame,
        range_columns: &[ArrayRef],
        last_range: &Range<usize>,
        idx: usize,
        bound: &WindowFrameBound,
        length: usize,
    ) -> Result<usize> {
        let search_start = if SIDE {
            last_range.start
        } else {
            last_range.end
        };
        if window_frame.has_null_offset() && search_start == length {
            return Ok(length);
        }
        if matches!(bound, WindowFrameBound::CurrentRow) {
            // Peer boundaries must use the same nested ordering as the sort.
            if !self.peer_comparator.as_ref().is_some_and(|(columns, _)| {
                columns.len() == range_columns.len()
                    && columns
                        .iter()
                        .zip(range_columns)
                        .all(|(old, current)| Arc::ptr_eq(old, current))
            }) {
                let columns = range_columns
                    .iter()
                    .zip(&self.sort_options)
                    .map(|(values, options)| SortColumn {
                        values: values.clone(),
                        options: Some(*options),
                    })
                    .collect::<Vec<_>>();
                let comparator = LexicographicalComparator::try_new(&columns)?;
                self.peer_comparator =
                    Some((range_columns.to_vec(), Arc::new(comparator)));
            }
            let (_, comparator) = self.peer_comparator.as_ref().ok_or_else(|| {
                internal_datafusion_err!(
                    "Peer comparator unexpectedly absent in a window frame"
                )
            })?;
            return Ok((search_start..length)
                .find(|&row| {
                    let order = comparator.compare(row, idx);
                    if SIDE { !order.is_lt() } else { !order.is_le() }
                })
                .unwrap_or(length));
        }
        let current_row_values = get_row_at_idx(range_columns, idx)?;
        let end_range = {
            let is_descending: bool = self
                .sort_options
                .first()
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Sort options unexpectedly absent in a window frame"
                    )
                })?
                .descending;

            // On overflow the boundary exceeds the type's range and is
            // effectively unbounded within the partition. Collapse to the
            // partition edge rather than feeding `search_in_slice` a
            // wrapped-around target: PRECEDING searches reach `search_start`,
            // FOLLOWING searches reach `length`.
            let unbounded_edge = if SEARCH_SIDE { search_start } else { length };
            let mut targets = Vec::with_capacity(current_row_values.len());
            for value in &current_row_values {
                if window_frame.checks_date32_offset_negation()
                    && matches!(value, ScalarValue::Date32(Some(_)))
                    && SEARCH_SIDE != is_descending
                    && let WindowFrameBound::Preceding(ScalarValue::Int32(Some(offset)))
                        | WindowFrameBound::Following(ScalarValue::Int32(Some(offset))) = bound
                {
                    offset.checked_neg().ok_or_else(|| {
                        exec_datafusion_err!("Int32 overflow on negative({offset})")
                            .context("[ARITHMETIC_OVERFLOW] DATE RANGE offset negation")
                    })?;
                }
                let Some(target) = range_bound_target(value, bound, is_descending)? else {
                    return Ok(unbounded_edge);
                };
                targets.push(target);
            }
            targets
        };
        let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| {
            let cmp = compare_rows(current, target, &self.sort_options)?;
            Ok(if SIDE { cmp.is_lt() } else { cmp.is_le() })
        };
        search_in_slice(range_columns, &end_range, compare_fn, search_start, length)
    }
}

// In GROUPS mode, rows with duplicate sorting values are grouped together.
// Therefore, there must be an ORDER BY clause in the window definition to use GROUPS mode.
// The syntax is as follows:
//     GROUPS frame_start [ frame_exclusion ]
//     GROUPS BETWEEN frame_start AND frame_end [ frame_exclusion ]
// The optional frame_exclusion specifier is not yet supported.
// The frame_start and frame_end parameters allow us to specify which rows the window
// frame starts and ends with. They accept the following values:
//    - UNBOUNDED PRECEDING: Start with the first row of the partition. Possible only in frame_start.
//    - offset PRECEDING: When used in frame_start, it refers to the first row of the group
//                        that comes "offset" groups before the current group (i.e. the group
//                        containing the current row). When used in frame_end, it refers to the
//                        last row of the group that comes "offset" groups before the current group.
//    - CURRENT ROW: When used in frame_start, it refers to the first row of the group containing
//                   the current row. When used in frame_end, it refers to the last row of the group
//                   containing the current row.
//    - offset FOLLOWING: When used in frame_start, it refers to the first row of the group
//                        that comes "offset" groups after the current group (i.e. the group
//                        containing the current row). When used in frame_end, it refers to the
//                        last row of the group that comes "offset" groups after the current group.
//    - UNBOUNDED FOLLOWING: End with the last row of the partition. Possible only in frame_end.

/// This structure encapsulates all the state information we require as we
/// scan groups of data while processing window frames.
#[derive(Debug, Default, Clone)]
pub struct WindowFrameStateGroups {
    /// A tuple containing group values and the row index where the group ends.
    /// Example: [[1, 1], [1, 1], [2, 1], [2, 1], ...] would correspond to
    ///          [([1, 1], 2), ([2, 1], 4), ...].
    pub group_end_indices: VecDeque<(Vec<ScalarValue>, usize)>,
    /// The group index to which the row index belongs.
    pub current_group_idx: usize,
}

impl WindowFrameStateGroups {
    fn calculate_range(
        &mut self,
        window_frame: &Arc<WindowFrame>,
        range_columns: &[ArrayRef],
        length: usize,
        idx: usize,
    ) -> Result<Range<usize>> {
        let start = match window_frame.start_bound {
            WindowFrameBound::Preceding(ref n) => {
                if n.is_null() {
                    // UNBOUNDED PRECEDING
                    0
                } else {
                    self.calculate_index_of_row::<true, true>(
                        range_columns,
                        idx,
                        Some(n),
                        length,
                    )?
                }
            }
            WindowFrameBound::CurrentRow => self.calculate_index_of_row::<true, true>(
                range_columns,
                idx,
                None,
                length,
            )?,
            WindowFrameBound::Following(ref n) => self
                .calculate_index_of_row::<true, false>(
                    range_columns,
                    idx,
                    Some(n),
                    length,
                )?,
            WindowFrameBound::PrecedingNullOffset(_)
            | WindowFrameBound::FollowingNullOffset(_) => {
                return internal_err!("NULL offsets require a RANGE window frame");
            }
        };
        let end = match window_frame.end_bound {
            WindowFrameBound::Preceding(ref n) => self
                .calculate_index_of_row::<false, true>(
                    range_columns,
                    idx,
                    Some(n),
                    length,
                )?,
            WindowFrameBound::CurrentRow => self.calculate_index_of_row::<false, false>(
                range_columns,
                idx,
                None,
                length,
            )?,
            WindowFrameBound::Following(ref n) => {
                if n.is_null() {
                    // UNBOUNDED FOLLOWING
                    length
                } else {
                    self.calculate_index_of_row::<false, false>(
                        range_columns,
                        idx,
                        Some(n),
                        length,
                    )?
                }
            }
            WindowFrameBound::PrecedingNullOffset(_)
            | WindowFrameBound::FollowingNullOffset(_) => {
                return internal_err!("NULL offsets require a RANGE window frame");
            }
        };
        Ok(Range { start, end })
    }

    /// This function does the heavy lifting when finding range boundaries. It is meant to be
    /// called twice, in succession, to get window frame start and end indices (with `SIDE`
    /// supplied as true and false, respectively). Generic argument `SEARCH_SIDE` determines
    /// the sign of `delta` (where true/false represents negative/positive respectively).
    fn calculate_index_of_row<const SIDE: bool, const SEARCH_SIDE: bool>(
        &mut self,
        range_columns: &[ArrayRef],
        idx: usize,
        delta: Option<&ScalarValue>,
        length: usize,
    ) -> Result<usize> {
        let delta = if let Some(delta) = delta {
            if let ScalarValue::UInt64(Some(value)) = delta {
                *value as usize
            } else {
                return internal_err!(
                    "Unexpectedly got a non-UInt64 value in a GROUPS mode window frame"
                );
            }
        } else {
            0
        };
        let mut group_start = 0;
        let last_group = self.group_end_indices.back_mut();
        if let Some((group_row, group_end)) = last_group {
            if *group_end < length {
                let new_group_row = get_row_at_idx(range_columns, *group_end)?;
                // If last/current group keys are the same, we extend the last group:
                if new_group_row.eq(group_row) {
                    // Update the end boundary of the group (search right boundary):
                    *group_end = search_in_slice(
                        range_columns,
                        group_row,
                        check_equality,
                        *group_end,
                        length,
                    )?;
                }
            }
            // Start searching from the last group boundary:
            group_start = *group_end;
        }

        // Advance groups until `idx` is inside a group:
        while idx >= group_start {
            let group_row = get_row_at_idx(range_columns, group_start)?;
            // Find end boundary of the group (search right boundary):
            let group_end = search_in_slice(
                range_columns,
                &group_row,
                check_equality,
                group_start,
                length,
            )?;
            self.group_end_indices.push_back((group_row, group_end));
            group_start = group_end;
        }

        // Update the group index `idx` belongs to:
        while self.current_group_idx < self.group_end_indices.len()
            && idx >= self.group_end_indices[self.current_group_idx].1
        {
            self.current_group_idx += 1;
        }

        // Find the group index of the frame boundary:
        let group_idx = if SEARCH_SIDE {
            self.current_group_idx.saturating_sub(delta)
        } else {
            self.current_group_idx + delta
        };

        // Extend `group_start_indices` until it includes at least `group_idx`:
        while self.group_end_indices.len() <= group_idx && group_start < length {
            let group_row = get_row_at_idx(range_columns, group_start)?;
            // Find end boundary of the group (search right boundary):
            let group_end = search_in_slice(
                range_columns,
                &group_row,
                check_equality,
                group_start,
                length,
            )?;
            self.group_end_indices.push_back((group_row, group_end));
            group_start = group_end;
        }

        // Calculate index of the group boundary:
        Ok(match (SIDE, SEARCH_SIDE) {
            // Window frame start:
            (true, _) => {
                let group_idx = std::cmp::min(group_idx, self.group_end_indices.len());
                if group_idx > 0 {
                    // Normally, start at the boundary of the previous group.
                    self.group_end_indices[group_idx - 1].1
                } else {
                    // If previous group is out of the table, start at zero.
                    0
                }
            }
            // Window frame end, PRECEDING n
            (false, true) => {
                if self.current_group_idx >= delta {
                    let group_idx = self.current_group_idx - delta;
                    self.group_end_indices[group_idx].1
                } else {
                    // Group is out of the table, therefore end at zero.
                    0
                }
            }
            // Window frame end, FOLLOWING n
            (false, false) => {
                let group_idx = std::cmp::min(
                    self.current_group_idx + delta,
                    self.group_end_indices.len() - 1,
                );
                self.group_end_indices[group_idx].1
            }
        })
    }
}

fn check_equality(current: &[ScalarValue], target: &[ScalarValue]) -> Result<bool> {
    Ok(current == target)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{
        Date32Array, DictionaryArray, Float64Array, Int32Array, ListArray, StringArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Int32Type};

    #[test]
    fn test_date_range_wrapping_offsets_keep_unconsumed_rows() -> Result<()> {
        let max_days = ScalarValue::Int32(Some(i32::MAX));
        for (days, descending, start, end, expected) in [
            (
                vec![-2, -1, 0, 1],
                false,
                WindowFrameBound::Preceding(max_days.clone()),
                WindowFrameBound::CurrentRow,
                vec![1..1, 1..2, 1..3, 1..4],
            ),
            (
                vec![-2, -1, 0, 1],
                false,
                WindowFrameBound::CurrentRow,
                WindowFrameBound::Following(max_days.clone()),
                vec![0..4, 1..4, 2..4, 3..4],
            ),
            (
                vec![2, 1, 0, -1, -2],
                true,
                WindowFrameBound::Preceding(max_days.clone()),
                WindowFrameBound::CurrentRow,
                vec![1..1, 2..2, 2..3, 2..4, 2..5],
            ),
            (
                vec![1, 2],
                false,
                WindowFrameBound::CurrentRow,
                WindowFrameBound::Following(max_days),
                vec![0..0, 0..0],
            ),
        ] {
            let column: ArrayRef = Arc::new(Date32Array::from(days));
            let frame =
                Arc::new(WindowFrame::new_bounds(WindowFrameUnits::Range, start, end));
            let mut state = WindowFrameContext::new(
                frame,
                vec![SortOptions {
                    descending,
                    nulls_first: true,
                }],
            );
            let mut last_range = 0..0;
            for (idx, expected) in expected.into_iter().enumerate() {
                last_range = state.calculate_range(
                    std::slice::from_ref(&column),
                    &last_range,
                    column.len(),
                    idx,
                )?;
                assert_eq!(last_range, expected);
            }
        }
        Ok(())
    }

    #[test]
    fn test_date_zero_offsets_preserve_null_peers() -> Result<()> {
        for (days, descending, nulls_first) in [
            (vec![None, Some(0), Some(0), Some(1)], false, true),
            (vec![Some(1), Some(0), Some(0), None], true, false),
        ] {
            let column: ArrayRef = Arc::new(Date32Array::from(days));
            let frame = Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::Preceding(ScalarValue::Int32(Some(0))),
                WindowFrameBound::Following(ScalarValue::Int32(Some(0))),
            ));
            let mut state = WindowFrameContext::new(frame, vec![SortOptions { descending, nulls_first }]);
            let mut last_range = 0..0;
            for (idx, expected) in [0..1, 1..3, 1..3, 3..4].into_iter().enumerate() {
                last_range = state.calculate_range(std::slice::from_ref(&column), &last_range, column.len(), idx)?;
                assert_eq!(last_range, expected);
            }
        }
        Ok(())
    }

    #[test]
    fn test_range_peer_comparator_reuses_and_releases_batch_arrays() -> Result<()> {
        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![0, 0, 1, 1, 2, 2]),
            Arc::new(StringArray::from(vec![None, Some("a"), Some("b")])),
        ));
        let nested: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", dictionary.data_type().clone(), true)),
            OffsetBuffer::new((0..=6).collect::<Vec<i32>>().into()),
            dictionary.clone(),
            None,
        ));
        let frame = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::CurrentRow,
            WindowFrameBound::CurrentRow,
        ));
        for column in [dictionary, nested] {
            let mut state = WindowFrameStateRange::new(vec![SortOptions::default()]);
            let mut last_range = 0..0;
            let mut cached = None;
            for idx in 0..column.len() {
                let range = state.calculate_range(
                    &frame,
                    &last_range,
                    std::slice::from_ref(&column),
                    column.len(),
                    idx,
                )?;
                assert_eq!(range, (idx / 2 * 2)..(idx / 2 * 2 + 2));
                let (_, comparator) = state
                    .peer_comparator
                    .as_ref()
                    .ok_or_else(|| internal_datafusion_err!("missing test comparator"))?;
                let current = Arc::downgrade(comparator);
                if let Some(previous) = &cached {
                    assert!(std::sync::Weak::ptr_eq(previous, &current));
                }
                cached = Some(current);
                last_range = range;
            }
            let previous = cached
                .ok_or_else(|| internal_datafusion_err!("missing test comparator"))?;
            let replacement = column.slice(0, column.len());
            state.calculate_range(&frame, &(0..0), &[replacement], column.len(), 0)?;
            assert!(previous.upgrade().is_none());
            let (_, comparator) = state
                .peer_comparator
                .as_ref()
                .ok_or_else(|| internal_datafusion_err!("missing test comparator"))?;
            let current = Arc::downgrade(comparator);
            let mut aggregate_state = WindowAggState::new(&DataType::Int64)?;
            aggregate_state.window_frame_range = 2..4;
            aggregate_state.last_calculated_index = 4;
            aggregate_state.window_frame_ctx = Some(WindowFrameContext::Range {
                window_frame: frame.clone(),
                state,
            });
            aggregate_state.prune_state(2);
            assert!(current.upgrade().is_none());
        }
        Ok(())
    }

    fn get_test_data() -> (Vec<ArrayRef>, Vec<SortOptions>) {
        let range_columns: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            5.0, 7.0, 8.0, 8.0, 9., 10., 10., 10., 11.,
        ]))];
        let sort_options = vec![SortOptions {
            descending: false,
            nulls_first: false,
        }];

        (range_columns, sort_options)
    }

    fn assert_group_ranges(
        window_frame: &Arc<WindowFrame>,
        expected_results: Vec<(Range<usize>, usize)>,
    ) -> Result<()> {
        let mut window_frame_groups = WindowFrameStateGroups::default();
        let (range_columns, _) = get_test_data();
        let n_row = range_columns[0].len();
        for (idx, (expected_range, expected_group_idx)) in
            expected_results.into_iter().enumerate()
        {
            let range = window_frame_groups.calculate_range(
                window_frame,
                &range_columns,
                n_row,
                idx,
            )?;
            assert_eq!(range, expected_range);
            assert_eq!(window_frame_groups.current_group_idx, expected_group_idx);
        }
        Ok(())
    }

    fn assert_frame_ranges(
        window_frame: &Arc<WindowFrame>,
        expected_results: Vec<Range<usize>>,
    ) -> Result<()> {
        let mut window_frame_context =
            WindowFrameContext::new(Arc::clone(window_frame), vec![]);
        let (range_columns, _) = get_test_data();
        let n_row = range_columns[0].len();
        let mut last_range = Range { start: 0, end: 0 };
        for (idx, expected_range) in expected_results.into_iter().enumerate() {
            let range = window_frame_context.calculate_range(
                &range_columns,
                &last_range,
                n_row,
                idx,
            )?;
            assert_eq!(range, expected_range);
            last_range = range;
        }
        Ok(())
    }

    #[test]
    fn test_default_window_frame_group_boundaries() -> Result<()> {
        let window_frame = Arc::new(WindowFrame::new(None));
        assert_group_ranges(
            &window_frame,
            vec![
                (Range { start: 0, end: 9 }, 0),
                (Range { start: 0, end: 9 }, 0),
                (Range { start: 0, end: 9 }, 0),
                (Range { start: 0, end: 9 }, 0),
                (Range { start: 0, end: 9 }, 0),
                (Range { start: 0, end: 9 }, 0),
                (Range { start: 0, end: 9 }, 0),
                (Range { start: 0, end: 9 }, 0),
                (Range { start: 0, end: 9 }, 0),
            ],
        )?;

        assert_frame_ranges(
            &window_frame,
            vec![
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
            ],
        )?;

        Ok(())
    }

    #[test]
    fn test_unordered_window_frame_group_boundaries() -> Result<()> {
        let window_frame = Arc::new(WindowFrame::new(Some(false)));
        assert_group_ranges(
            &window_frame,
            vec![
                (Range { start: 0, end: 1 }, 0),
                (Range { start: 0, end: 2 }, 1),
                (Range { start: 0, end: 4 }, 2),
                (Range { start: 0, end: 4 }, 2),
                (Range { start: 0, end: 5 }, 3),
                (Range { start: 0, end: 8 }, 4),
                (Range { start: 0, end: 8 }, 4),
                (Range { start: 0, end: 8 }, 4),
                (Range { start: 0, end: 9 }, 5),
            ],
        )?;

        assert_frame_ranges(
            &window_frame,
            vec![
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
                Range { start: 0, end: 9 },
            ],
        )?;

        Ok(())
    }

    #[test]
    fn test_ordered_window_frame_group_boundaries() -> Result<()> {
        let window_frame = Arc::new(WindowFrame::new(Some(true)));
        assert_group_ranges(
            &window_frame,
            vec![
                (Range { start: 0, end: 1 }, 0),
                (Range { start: 0, end: 2 }, 1),
                (Range { start: 0, end: 4 }, 2),
                (Range { start: 0, end: 4 }, 2),
                (Range { start: 0, end: 5 }, 3),
                (Range { start: 0, end: 8 }, 4),
                (Range { start: 0, end: 8 }, 4),
                (Range { start: 0, end: 8 }, 4),
                (Range { start: 0, end: 9 }, 5),
            ],
        )?;

        assert_frame_ranges(
            &window_frame,
            vec![
                Range { start: 0, end: 1 },
                Range { start: 0, end: 2 },
                Range { start: 0, end: 3 },
                Range { start: 0, end: 4 },
                Range { start: 0, end: 5 },
                Range { start: 0, end: 6 },
                Range { start: 0, end: 7 },
                Range { start: 0, end: 8 },
                Range { start: 0, end: 9 },
            ],
        )?;

        Ok(())
    }

    #[test]
    fn test_window_frame_group_boundaries() -> Result<()> {
        let window_frame = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Groups,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
            WindowFrameBound::Following(ScalarValue::UInt64(Some(1))),
        ));
        assert_group_ranges(
            &window_frame,
            vec![
                (Range { start: 0, end: 2 }, 0),
                (Range { start: 0, end: 4 }, 1),
                (Range { start: 1, end: 5 }, 2),
                (Range { start: 1, end: 5 }, 2),
                (Range { start: 2, end: 8 }, 3),
                (Range { start: 4, end: 9 }, 4),
                (Range { start: 4, end: 9 }, 4),
                (Range { start: 4, end: 9 }, 4),
                (Range { start: 5, end: 9 }, 5),
            ],
        )
    }

    #[test]
    fn test_window_frame_group_boundaries_both_following() -> Result<()> {
        let window_frame = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Groups,
            WindowFrameBound::Following(ScalarValue::UInt64(Some(1))),
            WindowFrameBound::Following(ScalarValue::UInt64(Some(2))),
        ));
        assert_group_ranges(
            &window_frame,
            vec![
                (Range::<usize> { start: 1, end: 4 }, 0),
                (Range::<usize> { start: 2, end: 5 }, 1),
                (Range::<usize> { start: 4, end: 8 }, 2),
                (Range::<usize> { start: 4, end: 8 }, 2),
                (Range::<usize> { start: 5, end: 9 }, 3),
                (Range::<usize> { start: 8, end: 9 }, 4),
                (Range::<usize> { start: 8, end: 9 }, 4),
                (Range::<usize> { start: 8, end: 9 }, 4),
                (Range::<usize> { start: 9, end: 9 }, 5),
            ],
        )
    }

    #[test]
    fn test_window_frame_group_boundaries_both_preceding() -> Result<()> {
        let window_frame = Arc::new(WindowFrame::new_bounds(
            WindowFrameUnits::Groups,
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(2))),
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
        ));
        assert_group_ranges(
            &window_frame,
            vec![
                (Range::<usize> { start: 0, end: 0 }, 0),
                (Range::<usize> { start: 0, end: 1 }, 1),
                (Range::<usize> { start: 0, end: 2 }, 2),
                (Range::<usize> { start: 0, end: 2 }, 2),
                (Range::<usize> { start: 1, end: 4 }, 3),
                (Range::<usize> { start: 2, end: 5 }, 4),
                (Range::<usize> { start: 2, end: 5 }, 4),
                (Range::<usize> { start: 2, end: 5 }, 4),
                (Range::<usize> { start: 4, end: 8 }, 5),
            ],
        )
    }
}
