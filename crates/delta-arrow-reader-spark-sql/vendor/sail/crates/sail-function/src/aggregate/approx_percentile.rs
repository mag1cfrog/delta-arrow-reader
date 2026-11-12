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

use crate::aggregate::utils::{
    extract_percentile_literal, extract_percentiles_array, get_scalar_value,
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
        let returns_array = matches!(args.expr_fields[1].data_type(), DataType::List(_));
        let percentiles = if returns_array {
            extract_percentiles_array(&args.exprs[1])?
        } else {
            vec![extract_percentile_literal(&args.exprs[1])?]
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
        Ok(Box::new(ApproxPercentileAccumulator {
            summary: QuantileSummary::new(1.0 / accuracy as f64),
            percentiles,
            returns_array,
            value_type: args.expr_fields[0].data_type().clone(),
        }))
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
        let values = self
            .percentiles
            .iter()
            .map(|&p| ScalarValue::Float64(Some(self.summary.query(p))).cast_to(&self.value_type))
            .collect::<Result<Vec<_>>>()?;
        if self.returns_array {
            Ok(ScalarValue::List(ScalarValue::new_list(
                &values,
                &self.value_type,
                false,
            )))
        } else {
            Ok(values[0].clone())
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
        let mut result = Vec::new();
        let mut head = self.samples[self.samples.len() - 1];
        for sample in self.samples[1..self.samples.len() - 1].iter().rev() {
            if ((sample.rank_gap + head.rank_gap + head.rank_uncertainty) as f64) < threshold {
                head.rank_gap += sample.rank_gap;
            } else {
                result.push(head);
                head = *sample;
            }
        }
        result.push(head);
        if self.samples[0].value <= head.value {
            result.push(self.samples[0]);
        }
        result.reverse();
        self.samples = result;
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
