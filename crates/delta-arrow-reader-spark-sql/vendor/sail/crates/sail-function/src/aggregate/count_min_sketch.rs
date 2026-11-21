// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::fmt::Debug;

use arrow::array::{Array, ArrayRef, AsArray, BinaryArray};
use arrow::datatypes::{DataType, Field, FieldRef, Int8Type, Int16Type, Int32Type, Int64Type};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, exec_err};

use super::utils::get_scalar_value;
use crate::error::{invalid_arg_count_exec_err, unsupported_data_type_exec_err};
use crate::scalar::hash::utils::spark_compatible_murmur3_hash;
use crate::scalar::math::java_random::JavaRandom;

const PRIME_MODULUS: i64 = (1i64 << 31) - 1;
const COUNT_MIN_SKETCH_VERSION: i32 = 1;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CountMinSketchFunction {
    signature: Signature,
}

impl Default for CountMinSketchFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl CountMinSketchFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::Any(4)], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CountMinSketchFunction {
    fn name(&self) -> &str {
        "count_min_sketch"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_count_min_sketch_types(arg_types)?;
        Ok(DataType::Binary)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let eps = resolve_float64_literal(&acc_args, 1, "eps")?;
        let confidence = resolve_float64_literal(&acc_args, 2, "confidence")?;
        let seed = resolve_seed_literal(&acc_args, 3)?;
        Ok(Box::new(CountMinSketchAccumulator::new(
            eps, confidence, seed,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "count_min_sketch"),
                DataType::Binary,
                true,
            )
            .into(),
        ])
    }
}

#[derive(Debug)]
struct CountMinSketchAccumulator {
    sketch: SparkCountMinSketch,
}

impl CountMinSketchAccumulator {
    fn new(eps: f64, confidence: f64, seed: i32) -> Result<Self> {
        Ok(Self {
            sketch: SparkCountMinSketch::new(eps, confidence, seed)?,
        })
    }
}

impl Accumulator for CountMinSketchAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.sketch.adjust_counts(&values[0], 1)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.sketch.adjust_counts(&values[0], -1)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.serialize())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.sketch.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.serialize()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(states) = as_binary_array(&states[0], "count_min_sketch state")? else {
            return Ok(());
        };
        for row in 0..states.len() {
            if !states.is_null(row) {
                let other = SparkCountMinSketch::deserialize(states.value(row))?;
                self.sketch.merge(&other)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct SparkCountMinSketch {
    depth: usize,
    width: usize,
    total_count: i64,
    hash_a: Vec<i64>,
    table: Vec<i64>,
}

impl SparkCountMinSketch {
    fn new(eps: f64, confidence: f64, seed: i32) -> Result<Self> {
        validate_parameters(eps, confidence)?;
        let width = (2.0 / eps).ceil();
        let depth = (-(-confidence).ln_1p() / 2.0_f64.ln()).ceil();
        if !width.is_finite() || width <= 0.0 || width > i32::MAX as f64 {
            return exec_err!("count_min_sketch computed invalid width {width}");
        }
        if !depth.is_finite() || depth <= 0.0 || depth > i32::MAX as f64 {
            return exec_err!("count_min_sketch computed invalid depth {depth}");
        }
        let width = width as usize;
        let depth = depth as usize;
        let entries = depth.checked_mul(width).ok_or_else(|| {
            DataFusionError::Execution("count_min_sketch table size overflow".to_string())
        })?;
        let mut rng = JavaRandom::new(seed as i64 as u64);
        let hash_a = (0..depth)
            .map(|_| {
                rng.next_i32_bound(i32::MAX)
                    .map(|value| value as i64)
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "count_min_sketch random hash bound must be positive".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            depth,
            width,
            total_count: 0,
            hash_a,
            table: vec![0; entries],
        })
    }

    fn size(&self) -> usize {
        self.hash_a.capacity() * std::mem::size_of::<i64>()
            + self.table.capacity() * std::mem::size_of::<i64>()
    }

    fn adjust_counts(&mut self, values: &ArrayRef, count_delta: i64) -> Result<()> {
        // Downcast once outside the row loop: the data type and the concrete array
        // are loop-invariant, so dispatching per row would re-pay the match + downcast
        // on every element of the batch. `iter().flatten()` skips nulls without a
        // per-row branch.
        match values.data_type() {
            DataType::Int8 => {
                for v in values.as_primitive::<Int8Type>().iter().flatten() {
                    self.adjust_long_count(v as i64, count_delta);
                }
            }
            DataType::Int16 => {
                for v in values.as_primitive::<Int16Type>().iter().flatten() {
                    self.adjust_long_count(v as i64, count_delta);
                }
            }
            DataType::Int32 => {
                for v in values.as_primitive::<Int32Type>().iter().flatten() {
                    self.adjust_long_count(v as i64, count_delta);
                }
            }
            DataType::Int64 => {
                for v in values.as_primitive::<Int64Type>().iter().flatten() {
                    self.adjust_long_count(v, count_delta);
                }
            }
            DataType::Binary => {
                for v in values.as_binary::<i32>().iter().flatten() {
                    self.adjust_binary_count(v, count_delta);
                }
            }
            DataType::Utf8 => {
                for v in values.as_string::<i32>().iter().flatten() {
                    self.adjust_binary_count(v.as_bytes(), count_delta);
                }
            }
            DataType::LargeUtf8 => {
                for v in values.as_string::<i64>().iter().flatten() {
                    self.adjust_binary_count(v.as_bytes(), count_delta);
                }
            }
            DataType::Utf8View => {
                for v in values.as_string_view().iter().flatten() {
                    self.adjust_binary_count(v.as_bytes(), count_delta);
                }
            }
            DataType::Null => {}
            data_type => {
                return Err(unsupported_data_type_exec_err(
                    "count_min_sketch",
                    "TINYINT, SMALLINT, INT, BIGINT, STRING or BINARY",
                    data_type,
                ));
            }
        }
        Ok(())
    }

    fn adjust_long_count(&mut self, item: i64, count_delta: i64) {
        for row in 0..self.depth {
            let bucket = self.hash_long(item, row);
            self.table[row * self.width + bucket] += count_delta;
        }
        self.total_count += count_delta;
    }

    fn adjust_binary_count(&mut self, item: &[u8], count_delta: i64) {
        // Compute the two base hashes once, then derive each row's bucket inline
        // so a per-element bucket Vec is never allocated.
        let hash1 = spark_compatible_murmur3_hash(item, 0) as i32;
        let hash2 = spark_compatible_murmur3_hash(item, hash1 as u32) as i32;
        for row in 0..self.depth {
            let hash = hash1.wrapping_add((row as i32).wrapping_mul(hash2));
            let bucket = (hash % self.width as i32).wrapping_abs() as usize;
            self.table[row * self.width + bucket] += count_delta;
        }
        self.total_count += count_delta;
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        if self.depth != other.depth {
            return exec_err!("Cannot merge count_min_sketch values of different depth");
        }
        if self.width != other.width {
            return exec_err!("Cannot merge count_min_sketch values of different width");
        }
        if self.hash_a != other.hash_a {
            return exec_err!("Cannot merge count_min_sketch values of different seed");
        }
        for (left, right) in self.table.iter_mut().zip(other.table.iter()) {
            *left += right;
        }
        self.total_count += other.total_count;
        Ok(())
    }

    fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(20 + (self.depth * 8) + (self.depth * self.width * 8));
        bytes.extend_from_slice(&COUNT_MIN_SKETCH_VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.total_count.to_be_bytes());
        bytes.extend_from_slice(&(self.depth as i32).to_be_bytes());
        bytes.extend_from_slice(&(self.width as i32).to_be_bytes());
        for value in &self.hash_a {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        for value in &self.table {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        bytes
    }

    fn deserialize(bytes: &[u8]) -> Result<Self> {
        let mut offset = 0;
        let version = read_i32_be(bytes, &mut offset, "version")?;
        if version != COUNT_MIN_SKETCH_VERSION {
            return exec_err!("Unexpected Count-Min Sketch version number ({version})");
        }
        let total_count = read_i64_be(bytes, &mut offset, "total_count")?;
        let depth = read_i32_be(bytes, &mut offset, "depth")?;
        let width = read_i32_be(bytes, &mut offset, "width")?;
        if depth <= 0 || width <= 0 {
            return exec_err!("Invalid Count-Min Sketch dimensions: depth={depth}, width={width}");
        }
        let depth = depth as usize;
        let width = width as usize;
        let entries = depth.checked_mul(width).ok_or_else(|| {
            DataFusionError::Execution("count_min_sketch table size overflow".to_string())
        })?;
        let mut hash_a = Vec::with_capacity(depth);
        for _ in 0..depth {
            hash_a.push(read_i64_be(bytes, &mut offset, "hash_a")?);
        }
        let mut table = Vec::with_capacity(entries);
        for _ in 0..entries {
            table.push(read_i64_be(bytes, &mut offset, "table")?);
        }
        if offset != bytes.len() {
            return exec_err!("Count-Min Sketch has trailing bytes");
        }
        Ok(Self {
            depth,
            width,
            total_count,
            hash_a,
            table,
        })
    }

    fn hash_long(&self, item: i64, row: usize) -> usize {
        let mut hash = self.hash_a[row].wrapping_mul(item);
        hash = hash.wrapping_add(hash >> 32);
        hash &= PRIME_MODULUS;
        (hash as i32 % self.width as i32) as usize
    }
}

fn validate_parameters(eps: f64, confidence: f64) -> Result<()> {
    if !eps.is_finite() || eps <= 0.0 {
        return exec_err!("count_min_sketch requires eps to be positive, got {eps}");
    }
    if !confidence.is_finite() || confidence <= 0.0 || confidence >= 1.0 {
        return exec_err!(
            "count_min_sketch requires confidence to be in the range (0.0, 1.0), got {confidence}"
        );
    }
    Ok(())
}

fn validate_count_min_sketch_types(arg_types: &[DataType]) -> Result<()> {
    if arg_types.len() != 4 {
        return Err(invalid_arg_count_exec_err(
            "count_min_sketch",
            (4, 4),
            arg_types.len(),
        ));
    }
    match &arg_types[0] {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Binary
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Null => {}
        data_type => {
            return Err(unsupported_data_type_exec_err(
                "count_min_sketch",
                "TINYINT, SMALLINT, INT, BIGINT, STRING or BINARY",
                data_type,
            ));
        }
    }
    if !matches!(arg_types[1], DataType::Float64) {
        return exec_err!(
            "count_min_sketch requires a double eps argument, got {}",
            arg_types[1]
        );
    }
    if !matches!(arg_types[2], DataType::Float64) {
        return exec_err!(
            "count_min_sketch requires a double confidence argument, got {}",
            arg_types[2]
        );
    }
    if !matches!(arg_types[3], DataType::Int32 | DataType::Int64) {
        return exec_err!(
            "count_min_sketch requires an integer seed argument, got {}",
            arg_types[3]
        );
    }
    Ok(())
}

fn resolve_float64_literal(args: &AccumulatorArgs, index: usize, name: &str) -> Result<f64> {
    let value = args.exprs.get(index).ok_or_else(|| {
        DataFusionError::Plan(format!("count_min_sketch requires a {name} argument"))
    })?;
    let value = get_scalar_value(value).map_err(|_| {
        DataFusionError::Plan(format!(
            "count_min_sketch requires {name} to be a constant double value"
        ))
    })?;
    // `validate_count_min_sketch_types` already rejects anything but Float64 for
    // eps/confidence, so only the Float64 literal can reach here.
    match value {
        ScalarValue::Float64(Some(value)) => Ok(value),
        value => Err(DataFusionError::Plan(format!(
            "count_min_sketch requires {name} to be a non-null double literal, got {}",
            value.data_type()
        ))),
    }
}

fn resolve_seed_literal(args: &AccumulatorArgs, index: usize) -> Result<i32> {
    let value = args.exprs.get(index).ok_or_else(|| {
        DataFusionError::Plan("count_min_sketch requires a seed argument".to_string())
    })?;
    let value = get_scalar_value(value).map_err(|_| {
        DataFusionError::Plan("count_min_sketch requires seed to be a constant integer".to_string())
    })?;
    match value {
        ScalarValue::Int32(Some(value)) => Ok(value),
        ScalarValue::Int64(Some(value)) => Ok(value as i32),
        value => Err(DataFusionError::Plan(format!(
            "count_min_sketch requires seed to be a non-null integer literal, got {}",
            value.data_type()
        ))),
    }
}

fn read_i32_be(bytes: &[u8], offset: &mut usize, name: &'static str) -> Result<i32> {
    Ok(i32::from_be_bytes(read_array(bytes, offset, name)?))
}

fn read_i64_be(bytes: &[u8], offset: &mut usize, name: &'static str) -> Result<i64> {
    Ok(i64::from_be_bytes(read_array(bytes, offset, name)?))
}

fn read_array<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
    name: &'static str,
) -> Result<[u8; N]> {
    let end = offset.checked_add(N).ok_or_else(|| {
        DataFusionError::Execution(format!("Count-Min Sketch offset overflow reading {name}"))
    })?;
    let slice = bytes.get(*offset..end).ok_or_else(|| {
        DataFusionError::Execution(format!("Count-Min Sketch is truncated reading {name}"))
    })?;
    let mut out = [0; N];
    out.copy_from_slice(slice);
    *offset = end;
    Ok(out)
}

fn as_binary_array<'a>(array: &'a ArrayRef, context: &str) -> Result<Option<&'a BinaryArray>> {
    if matches!(array.data_type(), DataType::Null) {
        return Ok(None);
    }
    array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .map(Some)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{context} expected BinaryArray, got {}",
                array.data_type()
            ))
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};

    use super::*;

    #[test]
    fn retracting_sketch_rows_preserves_the_replayed_frame_bytes() -> Result<()> {
        let inputs: [ArrayRef; 3] = [
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(1),
                None,
                Some(1),
                Some(i64::MAX),
                Some(-7),
                Some(0),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("one"),
                None,
                Some("one"),
                Some("\u{1f600}"),
                Some("two"),
                Some(""),
                None,
            ])),
            Arc::new(BinaryArray::from(vec![
                Some(b"".as_slice()),
                Some(b"one".as_slice()),
                None,
                Some(b"one".as_slice()),
                Some(b"\x00\xff".as_slice()),
                Some(b"two".as_slice()),
                Some(b"".as_slice()),
                None,
            ])),
        ];
        for input in inputs {
            let mut sliding = CountMinSketchAccumulator::new(0.9, 0.9, -7)?;
            let mut last = 0..0;
            for frame in [0..0, 0..3, 1..5, 2..5, 5..7, 6..8, 8..8] {
                sliding.update_batch(&[input.slice(last.end, frame.end - last.end)])?;
                sliding.retract_batch(&[input.slice(last.start, frame.start - last.start)])?;
                let mut original = CountMinSketchAccumulator::new(0.9, 0.9, -7)?;
                original.update_batch(&[input.slice(frame.start, frame.len())])?;
                assert_eq!(
                    sliding.evaluate()?,
                    original.evaluate()?,
                    "{} {frame:?}",
                    input.data_type()
                );
                last = frame;
            }
        }
        Ok(())
    }
}
