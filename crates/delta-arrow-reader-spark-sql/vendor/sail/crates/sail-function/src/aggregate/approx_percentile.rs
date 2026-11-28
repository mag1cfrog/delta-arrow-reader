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

// Rust adaptation of Spark 4.2.0 QuantileSummaries rank, compression, and merge rules.
// Source and retained Apache Spark NOTICE: crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
use std::mem::size_of;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array, ListArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};

use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Literal;

use crate::aggregate::utils::{
    extract_percentile_literal, extract_percentiles_array, get_scalar_value, scalar_to_f64,
};

const PENDING_VALUES_LIMIT: usize = 50_000;
const COMPRESS_THRESHOLD: usize = 10_000;

#[derive(Debug, PartialEq, Eq, Hash)]
struct ApproxPercentile {
    signature: Signature,
}

pub fn approx_percentile_udaf() -> Arc<AggregateUDF> {
    Arc::new(AggregateUDF::from(ApproxPercentile {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
}

impl AggregateUDFImpl for ApproxPercentile {
    fn name(&self) -> &str {
        "approx_percentile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=3).contains(&types.len()) || !(types[0].is_numeric() || types[0] == DataType::Null)
        {
            return Err(DataFusionError::Plan(
                "approx_percentile requires a numeric value, percentile, and optional accuracy"
                    .into(),
            ));
        }
        let mut types = types.to_vec();
        if types[0] == DataType::Null {
            types[0] = DataType::Float64;
        }
        Ok(types)
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        if matches!(types[1], DataType::List(_)) {
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                types[0].clone(),
                false,
            ))))
        } else {
            Ok(types[0].clone())
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(make_accumulator(args)?))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Only physical literals guarantee the same configuration in each rebuilt frame.
        let fixed_configuration = args.exprs[1..]
            .iter()
            .all(|expr| expr.downcast_ref::<Literal>().is_some());
        let accumulator = make_accumulator(args)?;
        if !accumulator.returns_array
            && accumulator.value_type == DataType::Float64
            && fixed_configuration
        {
            Ok(Box::new(SlidingPercentile::new(accumulator)))
        } else {
            Ok(Box::new(accumulator))
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "count"),
                DataType::UInt64,
                false,
            )),
            Arc::new(Field::new_list(
                format_state_name(args.name, "values"),
                Field::new_list_field(DataType::Float64, false),
                false,
            )),
            Arc::new(Field::new_list(
                format_state_name(args.name, "rank_gap"),
                Field::new_list_field(DataType::UInt64, false),
                false,
            )),
            Arc::new(Field::new_list(
                format_state_name(args.name, "rank_uncertainty"),
                Field::new_list_field(DataType::UInt64, false),
                false,
            )),
        ])
    }
}

fn make_accumulator(args: AccumulatorArgs) -> Result<ApproxPercentileAccumulator> {
    let returns_array = matches!(args.expr_fields[1].data_type(), DataType::List(_));
    let percentiles = if returns_array {
        extract_percentiles_array(&args.exprs[1])?
    } else {
        vec![scalar_percentile_value(&args.exprs[1])?]
    };
    let accuracy = if args.exprs.len() == 3 {
        match get_scalar_value(&args.exprs[2])?.cast_to(&DataType::Int64)? {
            ScalarValue::Int64(Some(value)) if (1..=i32::MAX as i64).contains(&value) => value,
            _ => {
                return Err(DataFusionError::Plan(
                    "approx_percentile accuracy must be a positive integer".into(),
                ));
            }
        }
    } else {
        10_000
    };
    Ok(ApproxPercentileAccumulator {
        summary: QuantileSummary::new(1.0 / accuracy as f64),
        percentiles,
        returns_array,
        value_type: args.expr_fields[0].data_type().clone(),
    })
}

fn scalar_percentile_value(expr: &Arc<dyn PhysicalExpr>) -> Result<f64> {
    if let Some(literal) = expr.downcast_ref::<Literal>()
        && let Ok(percentile) = scalar_to_f64(literal.value())
        && (0.0..=1.0).contains(&percentile)
    {
        return Ok(percentile);
    }
    extract_percentile_literal(expr)
}

#[derive(Debug)]
struct ApproxPercentileAccumulator {
    summary: QuantileSummary,
    percentiles: Vec<f64>,
    returns_array: bool,
    value_type: DataType,
}

impl Accumulator for ApproxPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = datafusion::arrow::compute::cast(&values[0], &DataType::Float64)?;
        for value in values
            .as_primitive::<datafusion::arrow::datatypes::Float64Type>()
            .iter()
            .flatten()
        {
            self.summary.insert(value);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.summary.compress();
        if self.summary.samples.is_empty() || self.percentiles.is_empty() {
            return ScalarValue::try_from(if self.returns_array {
                DataType::List(Arc::new(Field::new("item", self.value_type.clone(), false)))
            } else {
                self.value_type.clone()
            });
        }
        let cast_percentile = |percentile| {
            ScalarValue::Float64(Some(self.summary.query(percentile))).cast_to(&self.value_type)
        };
        if self.returns_array {
            let values = self
                .percentiles
                .iter()
                .copied()
                .map(cast_percentile)
                .collect::<Result<Vec<_>>>()?;
            Ok(ScalarValue::List(ScalarValue::new_list(
                &values,
                &self.value_type,
                false,
            )))
        } else {
            cast_percentile(self.percentiles[0])
        }
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.summary.compress();
        let values: ArrayRef = Arc::new(Float64Array::from_iter_values(
            self.summary.samples.iter().map(|s| s.value),
        ));
        let ranks: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            self.summary.samples.iter().map(|s| s.rank_gap),
        ));
        let deltas: ArrayRef = Arc::new(UInt64Array::from_iter_values(
            self.summary.samples.iter().map(|s| s.rank_uncertainty),
        ));
        let mut state = vec![ScalarValue::UInt64(Some(self.summary.count))];
        for values in [values, ranks, deltas] {
            state.push(ScalarValue::List(Arc::new(
                SingleRowListArrayBuilder::new(values)
                    .with_nullable(false)
                    .build_list_array(),
            )));
        }
        Ok(state)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        if states.len() != 4 {
            return Err(DataFusionError::Internal(
                "invalid percentile state field count".into(),
            ));
        }
        let counts = states[0]
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| DataFusionError::Internal("invalid percentile count state".into()))?;
        let lists = states[1..]
            .iter()
            .map(|a| {
                a.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    DataFusionError::Internal("invalid percentile list state".into())
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if counts.null_count() != 0
            || lists
                .iter()
                .any(|list| list.len() != counts.len() || list.null_count() != 0)
        {
            return Err(DataFusionError::Internal(
                "invalid percentile state rows".into(),
            ));
        }
        for row in 0..counts.len() {
            let values = lists[0].value(row);
            let ranks = lists[1].value(row);
            let deltas = lists[2].value(row);
            let values = values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("invalid percentile value state".into())
                })?;
            let ranks = ranks
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| DataFusionError::Internal("invalid percentile rank state".into()))?;
            let deltas = deltas
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("invalid percentile delta state".into())
                })?;
            if values.len() != ranks.len()
                || values.len() != deltas.len()
                || values.null_count() != 0
                || ranks.null_count() != 0
                || deltas.null_count() != 0
            {
                return Err(DataFusionError::Internal(
                    "mismatched percentile state lengths".into(),
                ));
            }
            let samples = (0..values.len())
                .map(|i| RankSample {
                    value: values.value(i),
                    rank_gap: ranks.value(i),
                    rank_uncertainty: deltas.value(i),
                })
                .collect();
            self.summary.merge(QuantileSummary {
                samples,
                pending_values: Vec::new(),
                count: counts.value(row),
                relative_error: self.summary.relative_error,
            });
        }
        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + self.summary.samples.capacity() * size_of::<RankSample>()
            + self.summary.pending_values.capacity() * size_of::<f64>()
            + self.percentiles.capacity() * size_of::<f64>()
            + self.value_type.size()
            - size_of::<DataType>()
    }
}

const FRAME_VALUES: usize = 5;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct FrameValues {
    len: usize,
    bits: [u64; FRAME_VALUES],
}

impl FrameValues {
    fn push(&mut self, bits: u64) {
        self.bits[self.len] = bits;
        self.len += 1;
    }

    fn extend(self, entering: &Float64Array) -> Option<Self> {
        if self.len + entering.len() - entering.null_count() > FRAME_VALUES {
            return None;
        }
        let mut frame = self;
        for value in entering.iter().flatten() {
            frame.push(value.to_bits());
        }
        Some(frame)
    }
}

#[derive(Debug, Default)]
enum FrameResult {
    #[default]
    Uncached,
    Rebuilt,
    Evaluated(ScalarValue),
    Reuse(ScalarValue),
}

#[derive(Debug)]
struct SlidingPercentile {
    inner: ApproxPercentileAccumulator,
    frame: Option<FrameValues>,
    entering: Option<ArrayRef>,
    result: FrameResult,
}

impl SlidingPercentile {
    fn new(inner: ApproxPercentileAccumulator) -> Self {
        Self {
            inner,
            frame: Some(FrameValues::default()),
            entering: None,
            result: FrameResult::Uncached,
        }
    }

    // With the same frame start, keep the original incremental summary and evaluation.
    fn flush_entering(&mut self) -> Result<()> {
        if let Some(entering) = self.entering.take() {
            self.inner.update_batch(std::slice::from_ref(&entering))?;
            self.frame = self
                .frame
                .and_then(|frame| frame.extend(entering.as_primitive()));
            self.result = FrameResult::Uncached;
        }
        Ok(())
    }
}

impl Accumulator for SlidingPercentile {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.flush_entering()?;
        if self.frame.is_some() {
            // The sliding executor calls update before retract for a moving frame.
            self.entering = Some(Arc::clone(&values[0]));
            Ok(())
        } else {
            self.inner.update_batch(values)
        }
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let previous = self.frame.ok_or_else(|| {
            DataFusionError::Internal("cannot retract an untracked percentile frame".into())
        })?;
        let departing = values[0].len() - values[0].null_count();
        let retained = &previous.bits[departing.min(previous.len)..previous.len];
        let entering = self.entering.take();
        let entering = entering
            .iter()
            .flat_map(|array| {
                array
                    .as_primitive::<datafusion::arrow::datatypes::Float64Type>()
                    .iter()
            })
            .flatten()
            .skip(departing.saturating_sub(previous.len))
            .map(f64::to_bits);
        let mut rows = retained.iter().copied().chain(entering);
        let mut next = FrameValues::default();
        for value in rows.by_ref().take(FRAME_VALUES) {
            next.push(value);
        }
        let extra = rows.next();
        self.frame = extra.is_none().then_some(next);

        // The cached result came from a fresh summary over these same ordered float bits.
        if self.frame == Some(previous)
            && let FrameResult::Evaluated(value) = std::mem::take(&mut self.result)
        {
            self.result = FrameResult::Reuse(value);
            return Ok(());
        }

        self.inner.summary = QuantileSummary::new(self.inner.summary.relative_error);
        for value in next.bits[..next.len]
            .iter()
            .copied()
            .chain(extra)
            .chain(rows)
        {
            self.inner.summary.insert(f64::from_bits(value));
        }
        self.result = if self.frame.is_some() {
            FrameResult::Rebuilt
        } else {
            FrameResult::Uncached
        };
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        // When the raw frame exceeds the bound, the executor replays the next moving frame.
        self.frame.is_some()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.flush_entering()?;
        match std::mem::take(&mut self.result) {
            FrameResult::Reuse(value) => {
                self.result = FrameResult::Evaluated(value.clone());
                Ok(value)
            }
            FrameResult::Rebuilt => {
                let value = self.inner.evaluate()?;
                self.result = FrameResult::Evaluated(value.clone());
                Ok(value)
            }
            _ => self.inner.evaluate(),
        }
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.flush_entering()?;
        self.result = FrameResult::Uncached;
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.flush_entering()?;
        self.frame = None;
        self.result = FrameResult::Uncached;
        self.inner.merge_batch(states)
    }

    fn size(&self) -> usize {
        self.inner.size() + size_of::<Self>() - size_of::<ApproxPercentileAccumulator>()
    }
}

#[derive(Clone, Copy, Debug)]
struct RankSample {
    value: f64,
    rank_gap: u64,
    rank_uncertainty: u64,
}

#[derive(Debug)]
struct QuantileSummary {
    samples: Vec<RankSample>,
    pending_values: Vec<f64>,
    // Pending values enter the rank count when their buffer is inserted.
    count: u64,
    relative_error: f64,
}

impl QuantileSummary {
    fn new(relative_error: f64) -> Self {
        Self {
            samples: Vec::new(),
            pending_values: Vec::new(),
            count: 0,
            relative_error,
        }
    }

    fn insert(&mut self, value: f64) {
        self.pending_values.push(value);
        if self.pending_values.len() == PENDING_VALUES_LIMIT {
            self.flush_pending_values();
            if self.samples.len() >= COMPRESS_THRESHOLD {
                self.compress();
            }
        }
    }

    fn flush_pending_values(&mut self) {
        if self.pending_values.is_empty() {
            return;
        }
        self.pending_values.sort_by(f64::total_cmp);
        let mut existing = std::mem::take(&mut self.samples).into_iter().peekable();
        let mut samples = Vec::new();
        let last = self.pending_values.len() - 1;
        for (i, value) in self.pending_values.drain(..).enumerate() {
            while existing.peek().is_some_and(|s| s.value <= value) {
                if let Some(sample) = existing.next() {
                    samples.push(sample);
                }
            }
            self.count += 1;
            let delta = if samples.is_empty() || (existing.peek().is_none() && i == last) {
                0
            } else {
                (2.0 * self.relative_error * self.count as f64).floor() as u64
            };
            samples.push(RankSample {
                value,
                rank_gap: 1,
                rank_uncertainty: delta,
            });
        }
        samples.extend(existing);
        self.samples = samples;
    }

    fn compress(&mut self) {
        self.flush_pending_values();
        if self.samples.len() <= 1 {
            return;
        }
        let threshold = 2.0 * self.relative_error * self.count as f64;
        let first = self.samples[0];
        let len = self.samples.len();
        let mut head = self.samples[len - 1];
        let mut write = len;
        // Write survivors into the consumed suffix in ascending rank order.
        for read in (1..len - 1).rev() {
            let sample = self.samples[read];
            if ((sample.rank_gap + head.rank_gap + head.rank_uncertainty) as f64) < threshold {
                head.rank_gap += sample.rank_gap;
            } else {
                write -= 1;
                self.samples[write] = head;
                head = sample;
            }
        }
        write -= 1;
        self.samples[write] = head;
        if first.value <= head.value {
            write -= 1;
            self.samples[write] = first;
        }
        self.samples.copy_within(write.., 0);
        self.samples.truncate(len - write);
        // Retain at most the capacity a fresh, geometrically grown vector would use.
        self.samples
            .shrink_to(self.samples.len().max(4).next_power_of_two());
    }

    fn merge(&mut self, other: Self) {
        self.compress();
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other;
            return;
        }
        let self_delta = (2.0 * other.relative_error * other.count as f64).floor() as u64;
        let other_delta = (2.0 * self.relative_error * self.count as f64).floor() as u64;
        let mut samples = Vec::new();
        let (mut i, mut j) = (0, 0);
        while i < self.samples.len() && j < other.samples.len() {
            let mut sample;
            if self.samples[i].value < other.samples[j].value {
                sample = self.samples[i];
                i += 1;
                if j > 0 {
                    sample.rank_uncertainty += self_delta;
                }
            } else {
                sample = other.samples[j];
                j += 1;
                if i > 0 {
                    sample.rank_uncertainty += other_delta;
                }
            }
            samples.push(sample);
        }
        samples.extend_from_slice(&self.samples[i..]);
        samples.extend_from_slice(&other.samples[j..]);
        self.samples = samples;
        self.count += other.count;
        self.relative_error = self.relative_error.max(other.relative_error);
        self.compress();
    }

    fn query(&self, percentile: f64) -> f64 {
        if percentile <= self.relative_error {
            return self.samples[0].value;
        }
        if percentile >= 1.0 - self.relative_error {
            return self.samples[self.samples.len() - 1].value;
        }
        let target_error = self
            .samples
            .iter()
            .map(|s| s.rank_gap + s.rank_uncertainty)
            .max()
            .unwrap_or(0)
            / 2;
        let rank = (percentile * self.count as f64).ceil() as u64;
        let mut min_rank = 0;
        for sample in &self.samples {
            min_rank += sample.rank_gap;
            if (min_rank + sample.rank_uncertainty) as f64 - target_error as f64 <= rank as f64
                && rank <= min_rank + target_error
            {
                return sample.value;
            }
        }
        self.samples[self.samples.len() - 1].value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn accumulator(accuracy: f64) -> ApproxPercentileAccumulator {
        ApproxPercentileAccumulator {
            summary: QuantileSummary::new(1.0 / accuracy),
            percentiles: vec![0.0, 0.01, 0.25, 0.5, 0.51, 0.9, 1.0],
            returns_array: true,
            value_type: DataType::Float64,
        }
    }

    #[test]
    fn sliding_reuse_matches_the_original_frame_replay() -> Result<()> {
        use std::ops::Range;

        fn float_bits(value: ScalarValue) -> Result<Option<u64>> {
            let ScalarValue::Float64(value) = value else {
                return Err(DataFusionError::Internal(
                    "expected Float64 percentile".into(),
                ));
            };
            Ok(value.map(f64::to_bits))
        }

        fn compare(input: &ArrayRef, frames: &[Range<usize>], accuracy: f64) -> Result<bool> {
            let scalar_accumulator = || ApproxPercentileAccumulator {
                summary: QuantileSummary::new(1.0 / accuracy),
                percentiles: vec![0.5],
                returns_array: false,
                value_type: DataType::Float64,
            };
            let mut reference = scalar_accumulator();
            let mut candidate = SlidingPercentile::new(scalar_accumulator());
            let mut previous = 0..0;
            let mut reused = false;
            for frame in frames {
                let moved = frame.start != previous.start;
                let replay = moved && !candidate.supports_retract_batch();
                if moved {
                    reference = scalar_accumulator();
                }
                if replay {
                    candidate = SlidingPercentile::new(scalar_accumulator());
                }
                let reference_start = if moved { frame.start } else { previous.end };
                if frame.end > reference_start {
                    reference.update_batch(&[
                        input.slice(reference_start, frame.end - reference_start)
                    ])?;
                }
                let candidate_start = if replay { frame.start } else { previous.end };
                if frame.end > candidate_start {
                    candidate.update_batch(&[
                        input.slice(candidate_start, frame.end - candidate_start)
                    ])?;
                }
                if moved && !replay {
                    candidate.retract_batch(&[
                        input.slice(previous.start, frame.start - previous.start)
                    ])?;
                }
                reused |= matches!(candidate.result, FrameResult::Reuse(_));
                assert_eq!(
                    float_bits(reference.evaluate()?)?,
                    float_bits(candidate.evaluate()?)?,
                    "frame {frame:?}, accuracy {accuracy}",
                );
                previous = frame.clone();
            }
            Ok(reused)
        }

        let moving = (0..16_usize)
            .map(|row| row.saturating_sub(4)..row + 1)
            .collect::<Vec<_>>();
        let widths = [0..1, 0..4, 2..13, 5..14, 10..15, 11..16];
        let jumps = [0..1, 3..9, 13..16];
        let repeating: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.5); 16]));
        let nulls: ArrayRef = Arc::new(Float64Array::from(vec![None::<f64>; 16]));
        let mixed: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(-0.0),
            Some(0.0),
            Some(f64::from_bits(0x7ff8_0000_0000_0001)),
            Some(f64::from_bits(0xfff8_0000_0000_0002)),
            Some(8.0),
            Some(3.0),
            Some(-9.0),
            None,
            Some(7.0),
            Some(7.0),
            Some(7.0),
            Some(7.0),
            Some(7.0),
            Some(7.0),
        ]));
        for accuracy in [1.0, 10_000.0] {
            assert!(compare(&repeating, &moving, accuracy)?);
            assert!(compare(&nulls, &moving, accuracy)?);
            compare(&mixed, &moving, accuracy)?;
            compare(&mixed, &widths, accuracy)?;
            compare(&mixed, &jumps, accuracy)?;
        }
        Ok(())
    }

    #[test]
    fn scalar_percentile_literals_match_expression_evaluation() {
        use datafusion::logical_expr::Operator;
        use datafusion_physical_expr::expressions::BinaryExpr;

        let mut expressions: Vec<Arc<dyn PhysicalExpr>> = [
            ScalarValue::Float64(Some(0.5)),
            ScalarValue::Float64(Some(-0.0)),
            ScalarValue::Float64(Some(f64::NAN)),
            ScalarValue::Float64(None),
            ScalarValue::Int32(Some(0)),
            ScalarValue::Int64(Some(2)),
            ScalarValue::Decimal128(Some(51), 3, 2),
            ScalarValue::Utf8(Some("0.5".into())),
        ]
        .into_iter()
        .map(|value| Arc::new(Literal::new(value)) as Arc<dyn PhysicalExpr>)
        .collect();
        expressions.push(Arc::new(BinaryExpr::new(
            Arc::new(Literal::new(ScalarValue::Float64(Some(0.25)))),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Float64(Some(0.25)))),
        )));
        for expression in expressions {
            let expected = extract_percentile_literal(&expression)
                .map(f64::to_bits)
                .map_err(|error| error.to_string());
            let actual = scalar_percentile_value(&expression)
                .map(f64::to_bits)
                .map_err(|error| error.to_string());
            assert_eq!(actual, expected, "{expression}");
        }
    }

    #[test]
    fn scalar_percentile_result_matches_single_item_list_cast() -> Result<()> {
        for value_type in [
            DataType::Float64,
            DataType::Int64,
            DataType::Decimal128(5, 2),
        ] {
            for values in [vec![], vec![None], vec![Some(-0.0), None, Some(2.5)]] {
                let input = Arc::new(Float64Array::from(values)) as ArrayRef;
                let mut scalar = accumulator(10_000.0);
                scalar.percentiles = vec![0.5];
                scalar.returns_array = false;
                scalar.value_type = value_type.clone();
                scalar.update_batch(&[Arc::clone(&input)])?;

                let mut list = accumulator(10_000.0);
                list.percentiles = vec![0.5];
                list.value_type = value_type.clone();
                list.update_batch(&[input])?;
                let ScalarValue::List(expected) = list.evaluate()? else {
                    unreachable!("the reference has a List result");
                };
                let expected = if expected.is_null(0) {
                    ScalarValue::try_from(&value_type)?
                } else {
                    ScalarValue::try_from_array(expected.value(0).as_ref(), 0)?
                };
                assert_eq!(scalar.evaluate()?, expected);
            }
        }
        Ok(())
    }

    #[test]
    fn in_place_compression_matches_the_original_rank_walk() {
        fn reference(samples: &[RankSample], threshold: f64) -> Vec<RankSample> {
            let mut result = Vec::new();
            let mut head = samples[samples.len() - 1];
            for sample in samples[1..samples.len() - 1].iter().rev() {
                if ((sample.rank_gap + head.rank_gap + head.rank_uncertainty) as f64) < threshold {
                    head.rank_gap += sample.rank_gap;
                } else {
                    result.push(head);
                    head = *sample;
                }
            }
            result.push(head);
            if samples[0].value <= head.value {
                result.push(samples[0]);
            }
            result.reverse();
            result
        }

        fn rank_bits(sample: &RankSample) -> (u64, u64, u64) {
            (
                sample.value.to_bits(),
                sample.rank_gap,
                sample.rank_uncertainty,
            )
        }

        let values = [
            f64::from_bits(0xfff8_0000_0000_0002),
            f64::NEG_INFINITY,
            -5.0,
            -0.0,
            0.0,
            0.0,
            2.0,
            2.0,
            f64::INFINITY,
            f64::from_bits(0x7ff8_0000_0000_0001),
        ];
        for start in 0..values.len() - 1 {
            for end in start + 2..=values.len() {
                for relative_error in [0.0001, 0.05, 0.25, 0.5, 1.0] {
                    let samples = values[start..end]
                        .iter()
                        .enumerate()
                        .map(|(index, value)| RankSample {
                            value: *value,
                            rank_gap: (index % 4 + 1) as u64,
                            rank_uncertainty: (index % 3) as u64,
                        })
                        .collect::<Vec<_>>();
                    let count: u64 = samples.iter().map(|sample| sample.rank_gap).sum();
                    let expected = reference(&samples, 2.0 * relative_error * count as f64);
                    let mut summary = QuantileSummary::new(relative_error);
                    summary.samples = samples;
                    summary.count = count;
                    summary.compress();

                    assert_eq!(summary.count, count);
                    assert_eq!(
                        summary.samples.iter().map(rank_bits).collect::<Vec<_>>(),
                        expected.iter().map(rank_bits).collect::<Vec<_>>(),
                        "sample range {start}..{end}, relative error {relative_error}",
                    );
                    assert!(summary.samples.capacity() <= expected.capacity());
                }
            }
        }
    }

    #[test]
    fn compressed_state_and_merges_match_spark_partition_order() -> Result<()> {
        let source = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../tests/fixtures/approx-percentile-ranks/spark-results.jsonl"
        ));
        let cases = source
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let mut checked = 0;
        for case in cases.iter().filter(|case| {
            case["ansi"] == false
                && case["id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("compressed_p"))
        }) {
            let id = case["id"].as_str().unwrap_or_default();
            let parts = if id.starts_with("compressed_p1_") {
                1
            } else {
                2
            };
            let accuracy = id
                .rsplit_once("accuracy")
                .and_then(|(_, a)| a.parse::<f64>().ok())
                .ok_or_else(|| {
                    DataFusionError::Internal("missing accuracy in Spark reference".into())
                })?;
            let mut merged = accumulator(accuracy);
            for part in 0..parts {
                let mut partial = accumulator(accuracy);
                let values = Float64Array::from_iter_values(
                    (120001 * part / parts..120001 * (part + 1) / parts).map(|value| value as f64),
                );
                partial.update_batch(&[Arc::new(values)])?;
                let state = partial
                    .state()?
                    .iter()
                    .map(ScalarValue::to_array)
                    .collect::<Result<Vec<_>>>()?;
                merged.merge_batch(&state)?;
            }
            let result = merged.evaluate()?;
            let ScalarValue::List(result) = result else {
                return Err(DataFusionError::Internal("expected percentile list".into()));
            };
            let values = result
                .values()
                .as_primitive::<datafusion::arrow::datatypes::Float64Type>();
            for (i, value) in values.values().iter().enumerate() {
                assert_eq!(
                    format!("{:016x}", value.to_bits()),
                    case["rows"][0][0][i].as_str().unwrap_or_default(),
                    "{id} percentile index {i}"
                );
            }
            checked += 1;
        }
        assert_eq!(checked, 6);
        Ok(())
    }

    #[test]
    fn nonmonotone_three_partition_states_match_spark() -> Result<()> {
        let source = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../tests/fixtures/approx-percentile-ranks/summary-merges-spark.jsonl"
        ));
        let cases = source
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        assert_eq!(cases.len(), 2);
        for case in cases {
            assert_eq!(case["spark_version"], "4.2.0");
            assert_eq!(case["merge_order"], serde_json::json!([0, 1, 2]));
            assert_eq!(case["accuracy"], 100);
            let size = case["size"]
                .as_u64()
                .ok_or_else(|| DataFusionError::Internal("missing summary input size".into()))?;
            let parts = case["partitions"].as_u64().ok_or_else(|| {
                DataFusionError::Internal("missing summary partition count".into())
            })?;
            let mut merged = accumulator(100.0);
            for part in 0..parts {
                let mut partial = accumulator(100.0);
                let values = Float64Array::from_iter_values(
                    (size * part / parts..size * (part + 1) / parts).map(|value| {
                        if case["id"] == "reverse" {
                            (size - 1 - value) as f64
                        } else {
                            ((value * 37) % size) as f64
                        }
                    }),
                );
                partial.update_batch(&[Arc::new(values)])?;
                let state = partial
                    .state()?
                    .iter()
                    .map(ScalarValue::to_array)
                    .collect::<Result<Vec<_>>>()?;
                merged.merge_batch(&state)?;
            }
            let ScalarValue::List(result) = merged.evaluate()? else {
                return Err(DataFusionError::Internal("expected percentile list".into()));
            };
            let values = result
                .values()
                .as_primitive::<datafusion::arrow::datatypes::Float64Type>();
            for (i, value) in values.values().iter().enumerate() {
                assert_eq!(
                    format!("{:016x}", value.to_bits()),
                    case["rows"][0][0][i].as_str().unwrap_or_default(),
                    "{} percentile {i}",
                    case["id"]
                );
            }
        }
        Ok(())
    }

    #[test]
    fn compression_bounds_retained_state_and_accounts_for_buffered_memory() -> Result<()> {
        let mut compact = accumulator(100.0);
        let mut precise = accumulator(10000.0);
        for value in 0..1_000_001 {
            compact.summary.insert(value as f64);
            precise.summary.insert(value as f64);
        }
        for acc in [&mut compact, &mut precise] {
            assert!(acc.summary.pending_values.len() < PENDING_VALUES_LIMIT);
            acc.state()?;
            assert!(acc.summary.pending_values.is_empty());
            // Draining pending input keeps its allocation, which size() must still report.
            let retained_allocations = acc.summary.samples.capacity() * size_of::<RankSample>()
                + acc.summary.pending_values.capacity() * size_of::<f64>()
                + acc.percentiles.capacity() * size_of::<f64>();
            assert!(acc.size() >= size_of::<ApproxPercentileAccumulator>() + retained_allocations);
            assert_eq!(
                acc.summary
                    .samples
                    .iter()
                    .map(|sample| sample.rank_gap)
                    .sum::<u64>(),
                1_000_001
            );
            // A collect-all replacement violates the aggregate's retained-memory contract.
            assert!(acc.size() < 1_000_001 * size_of::<f64>());
            assert_eq!(acc.summary.query(0.0), 0.0);
            assert_eq!(acc.summary.query(1.0), 1_000_000.0);
            let median = acc.summary.query(0.5);
            assert!((median - 500_000.0).abs() <= 1_000_001.0 * acc.summary.relative_error);
        }
        assert!(compact.summary.samples.len() < precise.summary.samples.len());
        Ok(())
    }
}
